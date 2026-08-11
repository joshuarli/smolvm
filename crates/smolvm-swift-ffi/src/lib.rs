//! Versioned C ABI used by the Swift SDK.
//!
//! This crate intentionally wraps smolvm's `EmbeddedRuntime`, rather than
//! exposing libkrun directly. `libkrun` is only a VMM/device API; image pulls,
//! overlays, agent lifecycle, readiness, and exec protocol are smolvm's Rust
//! contract. Opaque handles and JSON payloads keep Rust collection/layout
//! details out of the Swift ABI.

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use smolvm::agent::{ExecEvent, HostMount, PortMapping, VmResources};
use smolvm::config::{PublishedSocketConfig, SocketDirection};
use smolvm::embedded::{EmbeddedRuntime, MachineSpec};
use smolvm::SmolvmDb;
use smolvm_protocol::ImageInfo;
use std::collections::VecDeque;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::Duration;

const ABI_VERSION: u32 = 1;

pub struct RuntimeHandle {
    runtime: Arc<EmbeddedRuntime>,
}

pub struct StreamHandle {
    state: Arc<StreamState>,
}

struct StreamState {
    queue: Mutex<VecDeque<StreamEvent>>,
    ready: Condvar,
    finished: Mutex<bool>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeOptions {
    state_directory: Option<String>,
    lib_directory: Option<String>,
    agent_rootfs: Option<String>,
    boot_binary: Option<String>,
}

#[derive(Clone, PartialEq, Eq)]
struct ProcessPaths {
    state_directory: Option<String>,
    lib_directory: Option<String>,
    agent_rootfs: Option<String>,
    boot_binary: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MachineRequest {
    name: String,
    image: Option<String>,
    #[serde(default)]
    mounts: Vec<MountRequest>,
    #[serde(default)]
    ports: Vec<PortRequest>,
    #[serde(default)]
    published_sockets: Vec<PublishedSocketRequest>,
    /// Explicit host-side path for the dedicated Docker socket bridge. The
    /// bridge is enabled when this is present; a missing path leaves it off.
    #[serde(default)]
    docker_socket: Option<String>,
    /// Shell snippets run once after the first successful machine boot.
    /// `initCommands` is intentionally separate from image entrypoint/CMD:
    /// these commands prepare a durable service VM before its caller starts
    /// any workload.
    #[serde(default, alias = "init")]
    init_commands: Vec<String>,
    resources: Option<ResourceRequest>,
    #[serde(default = "default_persistent")]
    persistent: bool,
}

fn default_persistent() -> bool {
    true
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MountRequest {
    source: String,
    target: String,
    #[serde(default)]
    read_only: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PortRequest {
    host: u16,
    guest: u16,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PublishedSocketRequest {
    direction: PublishedSocketDirection,
    guest_path: String,
    host_path: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum PublishedSocketDirection {
    Expose,
    Mount,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResourceRequest {
    cpus: Option<u8>,
    #[serde(alias = "memoryMiB")]
    memory_mib: Option<u32>,
    network: Option<bool>,
    #[serde(alias = "storageGiB")]
    storage_gib: Option<u64>,
    #[serde(alias = "overlayGiB")]
    overlay_gib: Option<u64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExecRequest {
    command: Vec<String>,
    #[serde(default)]
    environment: Vec<EnvironmentEntry>,
    working_directory: Option<String>,
    timeout_seconds: Option<u64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EnvironmentEntry {
    key: String,
    value: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MachineStatus {
    abi_version: u32,
    state: String,
    running: bool,
    pid: Option<i32>,
}

/// Wire-stable image metadata returned to Swift. `ImageInfo` belongs to the
/// Rust agent protocol and uses Rust's snake_case field names by default;
/// keeping that type out of the C ABI prevents an incidental protocol spelling
/// change from breaking the Swift SDK's Codable contract.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ImageInfoResponse {
    reference: String,
    digest: String,
    size: u64,
    created: Option<String>,
    architecture: String,
    os: String,
    layer_count: usize,
    layers: Vec<String>,
    entrypoint: Vec<String>,
    cmd: Vec<String>,
    env: Vec<String>,
    workdir: Option<String>,
    user: Option<String>,
}

impl From<ImageInfo> for ImageInfoResponse {
    fn from(value: ImageInfo) -> Self {
        Self {
            reference: value.reference,
            digest: value.digest,
            size: value.size,
            created: value.created,
            architecture: value.architecture,
            os: value.os,
            layer_count: value.layer_count,
            layers: value.layers,
            entrypoint: value.entrypoint,
            cmd: value.cmd,
            env: value.env,
            workdir: value.workdir,
            user: value.user,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ExecResult {
    abi_version: u32,
    exit_code: i32,
    stdout_base64: String,
    stderr_base64: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StreamEvent {
    abi_version: u32,
    kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data_base64: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FFIError {
    abi_version: u32,
    message: String,
}

fn set_error(slot: *mut *mut c_char, error: impl std::fmt::Display) {
    if slot.is_null() {
        return;
    }
    let body = serde_json::to_string(&FFIError {
        abi_version: ABI_VERSION,
        message: error.to_string(),
    })
    .unwrap_or_else(|_| "{\"abiVersion\":1,\"message\":\"unknown smolvm FFI error\"}".into());
    // JSON has no NUL bytes. The fallback avoids panicking across the ABI.
    let value = CString::new(body).unwrap_or_else(|_| CString::new("{}").expect("literal"));
    unsafe { *slot = value.into_raw() };
}

unsafe fn required_string<'a>(value: *const c_char, field: &str) -> Result<&'a str, String> {
    if value.is_null() {
        return Err(format!("{field} must not be null"));
    }
    unsafe { CStr::from_ptr(value) }
        .to_str()
        .map_err(|_| format!("{field} must be valid UTF-8"))
}

unsafe fn required_runtime<'a>(value: *mut RuntimeHandle) -> Result<&'a RuntimeHandle, String> {
    unsafe { value.as_ref() }.ok_or_else(|| "runtime must not be null".into())
}

fn json_result<T: Serialize>(value: T) -> *mut c_char {
    let json = match serde_json::to_string(&value) {
        Ok(value) => value,
        Err(_) => return std::ptr::null_mut(),
    };
    CString::new(json)
        .map(CString::into_raw)
        .unwrap_or(std::ptr::null_mut())
}

fn build_machine(request: MachineRequest) -> Result<MachineSpec, String> {
    let docker_socket = request
        .docker_socket
        .map(|path| {
            if path.is_empty() || !path.starts_with('/') {
                return Err("dockerSocket must be an absolute path".to_string());
            }
            if path.contains('\0') || path.contains(';') || path.contains('|') {
                return Err("dockerSocket contains an unsupported character".to_string());
            }
            Ok(PathBuf::from(path))
        })
        .transpose()?;
    let image = request
        .image
        .map(|image| -> Result<String, String> {
            if smolvm::data::image_source::is_local_ref(&image) {
                return Ok(image);
            }
            match smolvm::data::image_source::classify(&image) {
                smolvm::data::image_source::ImageSource::Registry(_) => Ok(image),
                local => match smolvm::data::image_source::resolve(local)
                    .map_err(|error| error.to_string())?
                {
                    smolvm::data::image_source::ResolvedImage::Local { reference, .. } => {
                        Ok(reference)
                    }
                    smolvm::data::image_source::ResolvedImage::Registry(_) => Err(
                        "local image source unexpectedly resolved as a registry reference".into(),
                    ),
                },
            }
        })
        .transpose()?;
    let mounts = request
        .mounts
        .into_iter()
        .map(|mount| {
            HostMount::new(
                PathBuf::from(mount.source),
                PathBuf::from(mount.target),
                mount.read_only,
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    HostMount::ensure_unique_targets(&mounts).map_err(|error| error.to_string())?;

    let published_sockets = request
        .published_sockets
        .into_iter()
        .map(|socket| {
            if socket.guest_path.is_empty() || !socket.guest_path.starts_with('/') {
                return Err("published socket guestPath must be an absolute path".to_string());
            }
            if socket.host_path.is_empty() || !socket.host_path.starts_with('/') {
                return Err("published socket hostPath must be an absolute path".to_string());
            }
            if socket.guest_path.contains('\0')
                || socket.host_path.contains('\0')
                || socket.guest_path.contains(';')
                || socket.guest_path.contains('|')
            {
                return Err("published socket path contains an unsupported character".to_string());
            }
            Ok(PublishedSocketConfig {
                direction: match socket.direction {
                    PublishedSocketDirection::Expose => SocketDirection::Expose,
                    PublishedSocketDirection::Mount => SocketDirection::Mount,
                },
                guest_path: socket.guest_path,
                host_path: Some(socket.host_path),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if published_sockets.len() > smolvm_protocol::ports::PUBLISH_SOCKET_MAX {
        return Err(format!(
            "at most {} published sockets are supported",
            smolvm_protocol::ports::PUBLISH_SOCKET_MAX
        ));
    }

    let mut resources = VmResources::default();
    if let Some(requested) = request.resources {
        if let Some(cpus) = requested.cpus {
            resources.cpus = cpus;
        }
        if let Some(memory_mib) = requested.memory_mib {
            resources.memory_mib = memory_mib;
        }
        if let Some(network) = requested.network {
            resources.network = network;
        }
        resources.storage_gib = requested.storage_gib;
        resources.overlay_gib = requested.overlay_gib;
    }
    resources.validate().map_err(|error| error.to_string())?;

    Ok(MachineSpec {
        name: request.name,
        mounts,
        ports: request
            .ports
            .into_iter()
            .map(|port| PortMapping::new(port.host, port.guest))
            .collect(),
        published_sockets,
        docker_socket,
        init: request.init_commands,
        resources,
        image,
        persistent: request.persistent,
        // A persistent SDK machine is durable application state. It must not
        // be reclaimed merely because its embedding process died; the next
        // host process reconnects to the same VM record and storage disk.
        // Ephemeral SDK machines retain the runtime-managed orphan cleanup.
        runtime_managed: !request.persistent,
    })
}

fn configure_process_paths(options: RuntimeOptions) -> Result<Option<PathBuf>, String> {
    static PATHS: OnceLock<ProcessPaths> = OnceLock::new();
    let paths = ProcessPaths {
        state_directory: options.state_directory,
        lib_directory: options.lib_directory,
        agent_rootfs: options.agent_rootfs,
        boot_binary: options.boot_binary,
    };
    if let Some(existing) = PATHS.get() {
        if existing != &paths {
            return Err(
                "smolvm native runtime paths are process-wide and must be configured consistently"
                    .into(),
            );
        }
    } else {
        if let Some(path) = &paths.state_directory {
            std::env::set_var("SMOLVM_RUNTIME_ROOT", path);
        }
        if let Some(path) = &paths.lib_directory {
            std::env::set_var("SMOLVM_LIB_DIR", path);
        }
        if let Some(path) = &paths.agent_rootfs {
            std::env::set_var("SMOLVM_AGENT_ROOTFS", path);
        }
        if let Some(path) = &paths.boot_binary {
            std::env::set_var("SMOLVM_BOOT_BINARY", path);
        }
        PATHS
            .set(paths.clone())
            .map_err(|_| "could not set smolvm native runtime paths")?;
    }
    Ok(paths.state_directory.map(PathBuf::from))
}

fn stream_event(event: ExecEvent) -> StreamEvent {
    match event {
        ExecEvent::Stdout(data) => StreamEvent {
            abi_version: ABI_VERSION,
            kind: "stdout".into(),
            data_base64: Some(base64::engine::general_purpose::STANDARD.encode(data)),
            exit_code: None,
            message: None,
        },
        ExecEvent::Stderr(data) => StreamEvent {
            abi_version: ABI_VERSION,
            kind: "stderr".into(),
            data_base64: Some(base64::engine::general_purpose::STANDARD.encode(data)),
            exit_code: None,
            message: None,
        },
        ExecEvent::Exit(code) => StreamEvent {
            abi_version: ABI_VERSION,
            kind: "exit".into(),
            data_base64: None,
            exit_code: Some(code),
            message: None,
        },
        ExecEvent::Error(message) => StreamEvent {
            abi_version: ABI_VERSION,
            kind: "error".into(),
            data_base64: None,
            exit_code: None,
            message: Some(message),
        },
    }
}

fn push_stream_event(state: &StreamState, event: StreamEvent) {
    if let Ok(mut queue) = state.queue.lock() {
        queue.push_back(event);
        state.ready.notify_all();
    }
}

fn finish_stream(state: &StreamState) {
    if let Ok(mut finished) = state.finished.lock() {
        *finished = true;
        state.ready.notify_all();
    }
}

#[no_mangle]
pub unsafe extern "C" fn smolvm_swift_runtime_create(
    options_json: *const c_char,
    error_json: *mut *mut c_char,
) -> *mut RuntimeHandle {
    let result = (|| {
        let raw = unsafe { required_string(options_json, "options_json") }?;
        let options: RuntimeOptions =
            serde_json::from_str(raw).map_err(|error| error.to_string())?;
        // smolvm's dynamic VMM loader, storage root, and agent rootfs resolver
        // are process-wide. Configure them once before the first VM is created.
        let state_directory = configure_process_paths(options)?;
        let runtime = match state_directory {
            Some(root) => EmbeddedRuntime::with_db(
                SmolvmDb::open_at(&root.join("smolvm.db")).map_err(|error| error.to_string())?,
            ),
            None => EmbeddedRuntime::new().map_err(|error| error.to_string())?,
        };
        Ok::<_, String>(Box::into_raw(Box::new(RuntimeHandle {
            runtime: Arc::new(runtime),
        })))
    })();
    match result {
        Ok(runtime) => runtime,
        Err(error) => {
            set_error(error_json, error);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn smolvm_swift_runtime_free(runtime: *mut RuntimeHandle) {
    if !runtime.is_null() {
        unsafe { drop(Box::from_raw(runtime)) };
    }
}

#[no_mangle]
pub unsafe extern "C" fn smolvm_swift_machine_create(
    runtime: *mut RuntimeHandle,
    request_json: *const c_char,
    error_json: *mut *mut c_char,
) -> i32 {
    let result = (|| {
        let runtime = unsafe { required_runtime(runtime) }?;
        let raw = unsafe { required_string(request_json, "request_json") }?;
        let request = serde_json::from_str(raw).map_err(|error| error.to_string())?;
        runtime
            .runtime
            .open_or_create_machine(build_machine(request)?)
            .map_err(|error| error.to_string())
    })();
    match result {
        Ok(()) => 0,
        Err(error) => {
            set_error(error_json, error);
            -1
        }
    }
}

macro_rules! named_action {
    ($name:ident, $method:ident) => {
        #[no_mangle]
        pub unsafe extern "C" fn $name(
            runtime: *mut RuntimeHandle,
            name: *const c_char,
            error_json: *mut *mut c_char,
        ) -> i32 {
            let result = (|| {
                let runtime = unsafe { required_runtime(runtime) }?;
                let name = unsafe { required_string(name, "name") }?;
                runtime
                    .runtime
                    .$method(name)
                    .map_err(|error| error.to_string())
            })();
            match result {
                Ok(()) => 0,
                Err(error) => {
                    set_error(error_json, error);
                    -1
                }
            }
        }
    };
}

named_action!(smolvm_swift_machine_start, start_machine);
named_action!(smolvm_swift_machine_delete, delete_machine);
named_action!(
    smolvm_swift_machine_start_image_workload,
    start_image_workload
);

#[no_mangle]
pub unsafe extern "C" fn smolvm_swift_machine_stop(
    runtime: *mut RuntimeHandle,
    name: *const c_char,
    error_json: *mut *mut c_char,
) -> i32 {
    let result = (|| {
        let runtime = unsafe { required_runtime(runtime) }?;
        let name = unsafe { required_string(name, "name") }?;
        runtime
            .runtime
            .force_stop_machine(name)
            .map_err(|error| error.to_string())
    })();
    match result {
        Ok(()) => 0,
        Err(error) => {
            set_error(error_json, error);
            -1
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn smolvm_swift_machine_status(
    runtime: *mut RuntimeHandle,
    name: *const c_char,
    error_json: *mut *mut c_char,
) -> *mut c_char {
    let result = (|| {
        let runtime = unsafe { required_runtime(runtime) }?;
        let name = unsafe { required_string(name, "name") }?;
        Ok::<_, String>(MachineStatus {
            abi_version: ABI_VERSION,
            state: runtime.runtime.state(name),
            running: runtime.runtime.is_running(name),
            pid: runtime.runtime.pid(name),
        })
    })();
    match result {
        Ok(value) => json_result(value),
        Err(error) => {
            set_error(error_json, error);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn smolvm_swift_machine_exec(
    runtime: *mut RuntimeHandle,
    name: *const c_char,
    request_json: *const c_char,
    error_json: *mut *mut c_char,
) -> *mut c_char {
    let result = (|| {
        let runtime = unsafe { required_runtime(runtime) }?;
        let name = unsafe { required_string(name, "name") }?;
        let raw = unsafe { required_string(request_json, "request_json") }?;
        let request: ExecRequest = serde_json::from_str(raw).map_err(|error| error.to_string())?;
        if request.command.is_empty() {
            return Err("command must not be empty".into());
        }
        let timeout = request.timeout_seconds.map(Duration::from_secs);
        let environment = request
            .environment
            .into_iter()
            .map(|value| (value.key, value.value))
            .collect();
        let (exit_code, stdout, stderr) = runtime
            .runtime
            .exec(
                name,
                request.command,
                environment,
                request.working_directory,
                timeout,
            )
            .map_err(|error| error.to_string())?;
        Ok::<_, String>(ExecResult {
            abi_version: ABI_VERSION,
            exit_code,
            stdout_base64: base64::engine::general_purpose::STANDARD.encode(stdout),
            stderr_base64: base64::engine::general_purpose::STANDARD.encode(stderr),
        })
    })();
    match result {
        Ok(value) => json_result(value),
        Err(error) => {
            set_error(error_json, error);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn smolvm_swift_machine_exec_stream_start(
    runtime: *mut RuntimeHandle,
    name: *const c_char,
    request_json: *const c_char,
    error_json: *mut *mut c_char,
) -> *mut StreamHandle {
    let result = (|| {
        let runtime = unsafe { required_runtime(runtime) }?;
        let name = unsafe { required_string(name, "name") }?.to_string();
        let raw = unsafe { required_string(request_json, "request_json") }?;
        let request: ExecRequest = serde_json::from_str(raw).map_err(|error| error.to_string())?;
        if request.command.is_empty() {
            return Err("command must not be empty".into());
        }

        let state = Arc::new(StreamState {
            queue: Mutex::new(VecDeque::new()),
            ready: Condvar::new(),
            finished: Mutex::new(false),
        });
        let worker_state = Arc::clone(&state);
        let runtime = Arc::clone(&runtime.runtime);
        std::thread::Builder::new()
            .name(format!("smolvm-exec-{name}"))
            .spawn(move || {
                let timeout = request.timeout_seconds.map(Duration::from_secs);
                let environment = request
                    .environment
                    .into_iter()
                    .map(|value| (value.key, value.value))
                    .collect();
                let result = runtime.exec_streaming_with(
                    &name,
                    request.command,
                    environment,
                    request.working_directory,
                    timeout,
                    |event| push_stream_event(&worker_state, stream_event(event)),
                );
                if let Err(error) = result {
                    push_stream_event(
                        &worker_state,
                        StreamEvent {
                            abi_version: ABI_VERSION,
                            kind: "error".into(),
                            data_base64: None,
                            exit_code: None,
                            message: Some(error.to_string()),
                        },
                    );
                }
                finish_stream(&worker_state);
            })
            .map_err(|error| error.to_string())?;
        Ok::<_, String>(Box::into_raw(Box::new(StreamHandle { state })))
    })();
    match result {
        Ok(stream) => stream,
        Err(error) => {
            set_error(error_json, error);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn smolvm_swift_stream_next(
    stream: *mut StreamHandle,
    timeout_millis: u64,
    error_json: *mut *mut c_char,
) -> *mut c_char {
    let result = (|| {
        let stream =
            unsafe { stream.as_ref() }.ok_or_else(|| "stream must not be null".to_string())?;
        let mut queue = stream
            .state
            .queue
            .lock()
            .map_err(|error| error.to_string())?;
        if queue.is_empty() {
            let timeout = Duration::from_millis(timeout_millis);
            let (new_queue, _) = stream
                .state
                .ready
                .wait_timeout(queue, timeout)
                .map_err(|error| error.to_string())?;
            queue = new_queue;
        }
        if let Some(event) = queue.pop_front() {
            return Ok::<_, String>(event);
        }
        let finished = *stream
            .state
            .finished
            .lock()
            .map_err(|error| error.to_string())?;
        Ok(StreamEvent {
            abi_version: ABI_VERSION,
            kind: if finished { "finished" } else { "pending" }.into(),
            data_base64: None,
            exit_code: None,
            message: None,
        })
    })();
    match result {
        Ok(value) => json_result(value),
        Err(error) => {
            set_error(error_json, error);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn smolvm_swift_stream_free(stream: *mut StreamHandle) {
    if !stream.is_null() {
        unsafe { drop(Box::from_raw(stream)) };
    }
}

#[no_mangle]
pub unsafe extern "C" fn smolvm_swift_image_pull(
    runtime: *mut RuntimeHandle,
    name: *const c_char,
    reference: *const c_char,
    error_json: *mut *mut c_char,
) -> *mut c_char {
    let result = (|| {
        let runtime = unsafe { required_runtime(runtime) }?;
        let name = unsafe { required_string(name, "name") }?;
        let reference = unsafe { required_string(reference, "reference") }?;
        runtime
            .runtime
            .pull_image(name, reference)
            .map(ImageInfoResponse::from)
            .map_err(|error| error.to_string())
    })();
    match result {
        Ok(value) => json_result(value),
        Err(error) => {
            set_error(error_json, error);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn smolvm_swift_image_list(
    runtime: *mut RuntimeHandle,
    name: *const c_char,
    error_json: *mut *mut c_char,
) -> *mut c_char {
    let result = (|| {
        let runtime = unsafe { required_runtime(runtime) }?;
        let name = unsafe { required_string(name, "name") }?;
        runtime
            .runtime
            .list_images(name)
            .map(|images| {
                images
                    .into_iter()
                    .map(ImageInfoResponse::from)
                    .collect::<Vec<_>>()
            })
            .map_err(|error| error.to_string())
    })();
    match result {
        Ok(value) => json_result(value),
        Err(error) => {
            set_error(error_json, error);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn smolvm_swift_string_free(value: *mut c_char) {
    if !value.is_null() {
        unsafe { drop(CString::from_raw(value)) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn image_response_uses_the_sdk_camel_case_contract() {
        let response = ImageInfoResponse::from(ImageInfo {
            reference: "docker.io/library/alpine:latest".to_string(),
            digest: "sha256:abc".to_string(),
            size: 12,
            created: None,
            architecture: "arm64".to_string(),
            os: "linux".to_string(),
            layer_count: 1,
            layers: vec!["sha256:layer".to_string()],
            entrypoint: Vec::new(),
            cmd: vec!["sh".to_string()],
            env: Vec::new(),
            workdir: None,
            user: None,
        });

        let value = serde_json::to_value(response).unwrap();
        assert_eq!(value["layerCount"], 1);
        assert!(value.get("layer_count").is_none());
    }

    #[test]
    fn machine_request_preserves_docker_socket_bridge_and_init_commands() {
        let request: MachineRequest = serde_json::from_value(serde_json::json!({
            "name": "docker-host",
            "persistent": true,
            "dockerSocket": "/tmp/docker-host.sock",
            "publishedSockets": [{
                "direction": "expose",
                "guestPath": "/var/run/docker.sock",
                "hostPath": "/tmp/docker-host.sock"
            }],
            "initCommands": [
                "apk add --no-cache docker",
                "dockerd --data-root=/storage/docker &"
            ],
            "resources": {
                "network": true,
                "storageGiB": 20,
                "overlayGiB": 10,
                "memoryMiB": 1024
            }
        }))
        .unwrap();

        let spec = build_machine(request).unwrap();
        let record = spec.to_record();

        assert!(spec.persistent);
        assert!(!spec.runtime_managed);
        assert_eq!(record.storage_gb, Some(20));
        assert_eq!(record.overlay_gb, Some(10));
        assert!(record.docker_socket);
        assert_eq!(
            record.docker_socket_path.as_deref(),
            Some("/tmp/docker-host.sock")
        );
        assert_eq!(record.init.len(), 2);
        assert!(!record.init_completed);
        assert_eq!(record.published_sockets.len(), 1);
        assert_eq!(
            record.published_sockets[0].guest_path,
            "/var/run/docker.sock"
        );
        assert_eq!(
            record.published_sockets[0].host_path.as_deref(),
            Some("/tmp/docker-host.sock")
        );
    }
}
