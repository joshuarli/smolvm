//! The seam between the shim's Task-API state machine and the smolvm engine.
//!
//! The shim's correctness burden (state transitions, event ordering, exit
//! propagation — everything critest asserts) lives above this trait; the VM
//! mechanics live below it. `MockBackend` lets the whole state machine run in
//! unit tests on any host; the engine-backed implementation (Linux) boots a
//! smolvm microVM per pod sandbox and drives containers through the in-guest
//! agent over vsock.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::Arc;

/// Stdio wiring for a process: containerd-side fifo paths (empty = not wired).
#[derive(Debug, Clone, Default)]
pub struct Stdio {
    pub stdin: String,
    pub stdout: String,
    pub stderr: String,
    pub terminal: bool,
}

/// What the pod backend needs to know to create a container or exec process.
#[derive(Debug, Clone)]
pub struct ProcessSpec {
    /// OCI bundle dir on the host (config.json + rootfs mount target).
    pub bundle: String,
    /// Host rootfs path (already mounted by the shim from containerd's mounts).
    pub rootfs: String,
    pub stdio: Stdio,
    /// For exec processes: the serialized OCI process spec from containerd.
    pub exec_spec: Option<Vec<u8>>,
}

/// Exit information delivered exactly once per process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExitInfo {
    pub status: u32,
    /// Unix nanos; 0 = unknown.
    pub exited_at_ns: i64,
    /// The container's init was killed by the guest cgroup OOM killer. Drives a
    /// TaskOOM event so the CRI reports `reason=OOMKilled`. Only the init exit
    /// ever sets this.
    pub oom: bool,
}

struct ExitState {
    inner: Mutex<ExitStateInner>,
    wake: std::sync::Condvar,
}

struct ExitStateInner {
    value: Option<ExitInfo>,
    generation: u64,
    closed: bool,
}

/// Sender for an [`ExitWatch`].  Sending is idempotent in the sense that the
/// first value wins; all current waiters are woken by every send.
pub struct ExitSender {
    state: Arc<ExitState>,
}

/// A cloneable process-exit observation.  Each clone observes the same value
/// and has its own generation cursor, so concurrent Wait calls do not consume
/// one another's notification.
pub struct ExitWatch {
    state: Arc<ExitState>,
    generation: u64,
}

impl Clone for ExitWatch {
    fn clone(&self) -> Self {
        let generation = self
            .state
            .inner
            .lock()
            .map(|inner| inner.generation)
            .unwrap_or_default();
        Self {
            state: self.state.clone(),
            generation,
        }
    }
}

impl ExitSender {
    pub(crate) fn channel() -> (Self, ExitWatch) {
        let state = Arc::new(ExitState {
            inner: Mutex::new(ExitStateInner {
                value: None,
                generation: 0,
                closed: false,
            }),
            wake: std::sync::Condvar::new(),
        });
        (
            Self {
                state: state.clone(),
            },
            ExitWatch {
                state,
                generation: 0,
            },
        )
    }

    pub fn send(&self, value: Option<ExitInfo>) -> Result<(), ()> {
        {
            let mut inner = self.state.inner.lock().map_err(|_| ())?;
            if inner.closed {
                return Err(());
            }
            if inner.value.is_none() {
                inner.value = value;
            }
            inner.generation = inner.generation.wrapping_add(1);
        }
        self.state.wake.notify_all();
        Ok(())
    }

    /// Returns the currently published value without consuming it.
    pub fn borrow(&self) -> Option<ExitInfo> {
        self.state
            .inner
            .lock()
            .ok()
            .and_then(|inner| inner.value)
    }
}

impl Drop for ExitSender {
    fn drop(&mut self) {
        if let Ok(mut inner) = self
            .state
            .inner
            .lock()
        {
            inner.closed = true;
            self.state.wake.notify_all();
        }
    }
}

impl ExitWatch {
    /// Returns the most recent exit value without consuming it.
    pub fn borrow(&self) -> Option<ExitInfo> {
        self.state
            .inner
            .lock()
            .ok()
            .and_then(|inner| inner.value)
    }

    /// Blocks until a value newer than the watch's current generation is
    /// published. Clones have independent cursors, so concurrent Wait calls
    /// observe the same exit notification.
    pub fn wait(&mut self) -> Result<(), ()> {
        let mut inner = self.state.inner.lock().map_err(|_| ())?;
        while inner.generation == self.generation && !inner.closed {
            inner = self.state.wake.wait(inner).map_err(|_| ())?;
        }
        if inner.generation != self.generation {
            self.generation = inner.generation;
            Ok(())
        } else {
            Err(())
        }
    }
}

/// Backend operations for one pod (sandbox VM + its containers).
///
/// Process addressing follows the Task API: `(container_id, exec_id)`, where
/// an empty exec_id means the container's init process.
pub trait PodBackend: Send + Sync + 'static {
    /// Boot the sandbox VM for this pod. Called once, from the sandbox
    /// container's Create. Returns a nominal "pid" for the sandbox task.
    fn create_sandbox(
        &self,
        id: &str,
        bundle: &str,
        netns: Option<&str>,
    ) -> Result<u32, String>;

    /// Create (but do not start) a workload container in the sandbox VM.
    fn create_container(&self, id: &str, spec: ProcessSpec) -> Result<u32, String>;

    /// Start a previously created container init process or exec process.
    fn start(&self, id: &str, exec_id: Option<&str>) -> Result<u32, String>;

    /// Register an exec process (started later via `start`).
    fn create_exec(&self, id: &str, exec_id: &str, spec: ProcessSpec) -> Result<(), String>;

    /// Deliver a signal. `all` = process group / whole container.
    fn kill(
        &self,
        id: &str,
        exec_id: Option<&str>,
        signal: u32,
        all: bool,
    ) -> Result<(), String>;

    /// Watch for a process exit.
    fn wait_channel(&self, id: &str, exec_id: Option<&str>) -> Result<ExitWatch, String>;

    /// Resize a terminal process's PTY.
    fn resize_pty(
        &self,
        id: &str,
        exec_id: Option<&str>,
        w: u32,
        h: u32,
    ) -> Result<(), String>;

    /// Close the process's stdin.
    fn close_io(&self, id: &str, exec_id: Option<&str>) -> Result<(), String>;

    /// Remove a container's resources (after exit). Sandbox delete tears down
    /// the VM.
    fn delete(&self, id: &str, exec_id: Option<&str>) -> Result<(), String>;

    /// PIDs visible in the container (guest view).
    fn pids(&self, id: &str) -> Result<Vec<u32>, String>;

    /// Raw stats blob (cgroup metrics protobuf), if available.
    fn stats(&self, id: &str) -> Result<Option<Vec<u8>>, String>;
}

// ============================== MockBackend ==============================

/// In-process fake used by unit tests: processes are entries in a map whose
/// exits are triggered by tests (or immediately on kill).
#[derive(Default)]
pub struct MockBackend {
    inner: Mutex<HashMap<String, MockProc>>,
}

struct MockProc {
    running: bool,
    tx: ExitSender,
    rx: ExitWatch,
    next_pid: u32,
}

fn key(id: &str, exec_id: Option<&str>) -> String {
    match exec_id {
        Some(e) if !e.is_empty() => format!("{id}/{e}"),
        _ => id.to_string(),
    }
}

impl MockBackend {
    /// Test-only convenience (the production path wraps the mock in
    /// `engine::ShimBackend`, which needs it by value, not in an Arc).
    #[allow(dead_code)]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Test hook: mark a process exited.
    #[allow(dead_code)]
    pub fn finish(&self, id: &str, exec_id: Option<&str>, status: u32) {
        let map = self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(p) = map.get(&key(id, exec_id)) {
            let _ = p.tx.send(Some(ExitInfo {
                status,
                exited_at_ns: 1_700_000_000_000_000_000,
                oom: false,
            }));
        }
    }

    fn insert(&self, k: String, pid: u32) {
        let (tx, rx) = ExitSender::channel();
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                k,
                MockProc {
                    running: false,
                    tx,
                    rx,
                    next_pid: pid,
                },
            );
    }
}

impl PodBackend for MockBackend {
    fn create_sandbox(
        &self,
        id: &str,
        _bundle: &str,
        _netns: Option<&str>,
    ) -> Result<u32, String> {
        self.insert(key(id, None), 1);
        Ok(1)
    }

    fn create_container(&self, id: &str, _spec: ProcessSpec) -> Result<u32, String> {
        self.insert(key(id, None), 100);
        Ok(100)
    }

    fn start(&self, id: &str, exec_id: Option<&str>) -> Result<u32, String> {
        let mut map = self.inner.lock().map_err(|e| e.to_string())?;
        let p = map
            .get_mut(&key(id, exec_id))
            .ok_or_else(|| format!("no such process {}", key(id, exec_id)))?;
        p.running = true;
        Ok(p.next_pid)
    }

    fn create_exec(&self, id: &str, exec_id: &str, _spec: ProcessSpec) -> Result<(), String> {
        self.insert(key(id, Some(exec_id)), 200);
        Ok(())
    }

    fn kill(
        &self,
        id: &str,
        exec_id: Option<&str>,
        signal: u32,
        _all: bool,
    ) -> Result<(), String> {
        let map = self.inner.lock().map_err(|e| e.to_string())?;
        let p = map
            .get(&key(id, exec_id))
            .ok_or_else(|| "no such process".to_string())?;
        // SIGKILL/SIGTERM end the mock process immediately.
        if signal == 9 || signal == 15 {
            let _ = p.tx.send(Some(ExitInfo {
                status: 137,
                exited_at_ns: 1_700_000_000_000_000_000,
                oom: false,
            }));
        }
        Ok(())
    }

    fn wait_channel(&self, id: &str, exec_id: Option<&str>) -> Result<ExitWatch, String> {
        let map = self.inner.lock().map_err(|e| e.to_string())?;
        map.get(&key(id, exec_id))
            .map(|p| p.rx.clone())
            .ok_or_else(|| "no such process".to_string())
    }

    fn resize_pty(
        &self,
        _id: &str,
        _exec_id: Option<&str>,
        _w: u32,
        _h: u32,
    ) -> Result<(), String> {
        Ok(())
    }

    fn close_io(&self, _id: &str, _exec_id: Option<&str>) -> Result<(), String> {
        Ok(())
    }

    fn delete(&self, id: &str, exec_id: Option<&str>) -> Result<(), String> {
        self.inner
            .lock()
            .map_err(|e| e.to_string())?
            .remove(&key(id, exec_id));
        Ok(())
    }

    fn pids(&self, _id: &str) -> Result<Vec<u32>, String> {
        Ok(vec![1])
    }

    fn stats(&self, _id: &str) -> Result<Option<Vec<u8>>, String> {
        Ok(None)
    }
}
