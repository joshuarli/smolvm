//! Command execution handlers.

use h12tiny::web::{
    Event, Extension, KeepAlive, Path, Request, Response, Sse, State,
    WebSocketFrame, WebSocketOpCode, WebSocketPayload, WebSocketUpgrade,
};
use crate::api::Json;
use crate::api::Query;
use std::convert::Infallible;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::api::error::{classify_ensure_running_error, ApiError};
use crate::api::state::{ensure_running_and_persist, with_machine_client_traced, ApiState};
use crate::api::types::{
    ApiErrorResponse, EnvVar, ExecRequest, ExecResponse, LogsQuery, RunRequest,
};
use crate::api::validate_command;
use crate::api::TraceId;
use crate::data::consts::BYTES_PER_MIB;
use crate::data::storage::HostMount;
use crate::runtime::Semaphore;

/// Execute a command in a machine.
///
/// This executes directly in the VM (not in a container).
#[utoipa::path(
    post,
    path = "/api/v1/machines/{id}/exec",
    tag = "Execution",
    params(
        ("id" = String, Path, description = "Machine name")
    ),
    request_body = ExecRequest,
    responses(
        (status = 200, description = "Command executed", body = ExecResponse),
        (status = 400, description = "Invalid request", body = ApiErrorResponse),
        (status = 404, description = "Machine not found", body = ApiErrorResponse),
        (status = 500, description = "Execution failed", body = ApiErrorResponse)
    )
)]
pub async fn exec_command(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
    trace_id: Option<Extension<TraceId>>,
    Json(req): Json<ExecRequest>,
) -> Result<Json<ExecResponse>, ApiError> {
    let tid = trace_id.map(|t| t.0 .0.clone());
    validate_command(&req.command)?;

    let entry = state.get_machine(&id)?;

    // Ensure machine is running and persist state to DB
    ensure_running_and_persist(&state, &id, &entry)
        .await
        .map_err(classify_ensure_running_error)?;

    // Resolve secrets ONCE, before the background/foreground split, so a
    // detached workload gets them too (a long-lived daemon usually needs its
    // credentials more than a one-shot exec does). Env precedence (low → high):
    // req.env (caller-plaintext) → record.secret_refs (persisted by a
    // TrustedLocal actor) → req.secrets (ad-hoc, Untrusted). Validation runs
    // before resolution so structural/scope violations surface as 400 without
    // the resolution audit firing.
    crate::api::handlers::validate_request_secrets(&req.secrets)?;
    crate::api::handlers::validate_request_env(&req.env)?;
    let record_env = crate::api::handlers::record_secret_refs_env(&entry)?;
    let req_env = crate::api::handlers::resolve_request_secrets(&req.secrets)?;
    let mut env = EnvVar::to_tuples(&req.env);
    env.extend(crate::secrets::expose_into_env(record_env));
    env.extend(crate::secrets::expose_into_env(req_env));

    // Detached/background: spawn the process and return its PID immediately, so a
    // long-lived daemon (dev server, agent runner) keeps running after the
    // request returns. Image machines run it in their container (persistent
    // overlay); plain machines run it in the VM.
    if req.background {
        let command = req.command.clone();
        let workdir = req.workdir.clone();
        let machine_rec = state.lookup_vm(&id).await?;
        let machine_golden = machine_rec.as_ref().and_then(|r| r.golden.clone());
        let machine_image = machine_rec.and_then(|r| r.image);
        let pid = if let Some(image) = machine_image {
            let mounts_config = {
                let e = entry.lock();
                e.mounts
                    .iter()
                    .enumerate()
                    .map(|(i, m)| (HostMount::mount_tag(i), m.target.clone(), m.readonly))
                    .collect::<Vec<_>>()
            };
            // A fork clone's inherited overlay lives under the golden's id
            // (see `persistent_overlay_owner`).
            let overlay_id =
                crate::workload::persistent_overlay_owner(&id, machine_golden.as_deref());
            with_machine_client_traced(state.runtime()?, &entry, tid, move |c| {
                if c.query(&image)?.is_none() {
                    c.pull_with_registry_config(&image)?;
                }
                let config = crate::agent::RunConfig::new(image, command)
                    .with_env(env)
                    .with_workdir(workdir)
                    .with_mounts(mounts_config)
                    .with_persistent_overlay(Some(overlay_id));
                c.run_background(config)
            })
            .await?
        } else {
            with_machine_client_traced(state.runtime()?, &entry, tid, move |c| {
                c.vm_exec_background(command, env, workdir)
            })
            .await?
        };
        let stdout = format!("pid={pid}\n");
        return Ok(Json(ExecResponse {
            exit_code: 0,
            stdout_b64: stdout.clone().into_bytes(),
            stdout,
            stderr_b64: Vec::new(),
            stderr: String::new(),
        }));
    }

    // Secrets already resolved into `env` above (shared with the background
    // path); env precedence is req.env < record.secret_refs < req.secrets.
    let command = req.command.clone();
    let workdir = req.workdir.clone();
    let timeout = req.timeout_secs.map(Duration::from_secs);
    let stdin_data = req.stdin.clone();

    // Image-based machines exec INSIDE a container from their image, with a
    // per-machine persistent overlay so filesystem changes persist across exec
    // sessions. Without this, exec runs in the bare agent VM (no `python3`,
    // etc.) — the image is never entered. Plain machines exec in the VM
    // directly via `vm_exec`.
    let machine_rec = state.lookup_vm(&id).await?;
    let machine_golden = machine_rec.as_ref().and_then(|r| r.golden.clone());
    let machine_image = machine_rec.and_then(|r| r.image);

    let start = std::time::Instant::now();
    let (exit_code, stdout, stderr) = if let Some(image) = machine_image {
        let mounts_config = {
            let e = entry.lock();
            e.mounts
                .iter()
                .enumerate()
                .map(|(i, m)| (HostMount::mount_tag(i), m.target.clone(), m.readonly))
                .collect::<Vec<_>>()
        };
        // A fork clone's inherited overlay lives under the golden's id.
        let overlay_id = crate::workload::persistent_overlay_owner(&id, machine_golden.as_deref());
        let stdin_data = stdin_data.clone();
        with_machine_client_traced(state.runtime()?, &entry, tid, move |c| {
            // Pull only if the image isn't already present — avoids a registry
            // round-trip on every exec, and works once cached even on
            // network-restricted machines.
            if c.query(&image)?.is_none() {
                c.pull_with_registry_config(&image)?;
            }
            let config = crate::agent::RunConfig::new(image, command)
                .with_env(env)
                .with_workdir(workdir)
                .with_mounts(mounts_config)
                .with_timeout(timeout)
                .with_persistent_overlay(Some(overlay_id))
                .with_stdin(stdin_data);
            c.run_non_interactive(config)
        })
        .await?
    } else {
        with_machine_client_traced(state.runtime()?, &entry, tid, move |c| {
            c.vm_exec(command, env, workdir, timeout, stdin_data)
        })
        .await?
    };
    metrics::histogram!("smolvm_exec_seconds").record(start.elapsed().as_secs_f64());

    Ok(Json(ExecResponse {
        exit_code,
        stdout: String::from_utf8_lossy(&stdout).into_owned(),
        stderr: String::from_utf8_lossy(&stderr).into_owned(),
        stdout_b64: stdout,
        stderr_b64: stderr,
    }))
}

/// Execute a command with streaming output (Server-Sent Events).
///
/// Returns real-time stdout/stderr as SSE events. Useful for long-running
/// commands where buffering the entire output is impractical.
#[utoipa::path(
    post,
    path = "/api/v1/machines/{id}/exec/stream",
    tag = "Execution",
    params(
        ("id" = String, Path, description = "Machine name")
    ),
    request_body = ExecRequest,
    responses(
        (status = 200, description = "Streaming output (SSE)", content_type = "text/event-stream"),
        (status = 404, description = "Machine not found", body = ApiErrorResponse),
        (status = 500, description = "Execution failed", body = ApiErrorResponse)
    )
)]
pub async fn exec_stream(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
    trace_id: Option<Extension<TraceId>>,
    Json(req): Json<ExecRequest>,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let tid = trace_id.map(|t| t.0 .0.clone());
    validate_command(&req.command)?;

    let entry = state.get_machine(&id)?;
    ensure_running_and_persist(&state, &id, &entry)
        .await
        .map_err(classify_ensure_running_error)?;

    crate::api::handlers::validate_request_secrets(&req.secrets)?;
    crate::api::handlers::validate_request_env(&req.env)?;
    let record_env = crate::api::handlers::record_secret_refs_env(&entry)?;
    let req_env = crate::api::handlers::resolve_request_secrets(&req.secrets)?;

    let command = req.command.clone();
    let mut env = EnvVar::to_tuples(&req.env);
    env.extend(crate::secrets::expose_into_env(record_env));
    env.extend(crate::secrets::expose_into_env(req_env));
    let workdir = req.workdir.clone();
    let timeout = req.timeout_secs.map(Duration::from_secs);

    // Image-based machines stream from a container in their image (persistent
    // overlay keyed by machine name); plain machines stream from the VM
    // directly. Without this, streaming exec on an image machine produces no
    // output (the agent-base streaming path doesn't enter the container).
    let machine_rec = state.lookup_vm(&id).await?;
    let machine_golden = machine_rec.as_ref().and_then(|r| r.golden.clone());
    let machine_image = machine_rec.and_then(|r| r.image);

    // Bridge the blocking, synchronous vsock streaming exec to an async SSE
    // stream: a spawned blocking task runs the exec and pushes each ExecEvent
    // into an unbounded channel AS IT ARRIVES; the SSE stream below yields
    // them live. Previously this collected every event into a Vec and only
    // built the SSE stream AFTER the command completed, so the "stream"
    // delivered nothing until exit — defeating streaming exec (F-16). The
    // per-session output cap is still enforced inside the agent client's
    // streaming collector (MAX_STREAMING_EXEC_OUTPUT), which emits a
    // truncation Error event and stops relaying.
    let (tx, rx) = async_channel::unbounded::<crate::agent::ExecEvent>();
    let err_tx = tx.clone();
    let entry_exec = entry.clone();
    let start = std::time::Instant::now();
    let runtime = state.runtime()?.clone();
    let execution_runtime = runtime.clone();
    runtime
        .spawn(async move {
        let result = if let Some(image) = machine_image {
            let mounts_config = {
                let e = entry_exec.lock();
                e.mounts
                    .iter()
                    .enumerate()
                    .map(|(i, m)| (HostMount::mount_tag(i), m.target.clone(), m.readonly))
                    .collect::<Vec<_>>()
            };
            // A fork clone's inherited overlay lives under the golden's id.
            let overlay_id =
                crate::workload::persistent_overlay_owner(&id, machine_golden.as_deref());
            with_machine_client_traced(&execution_runtime, &entry_exec, tid, move |c| {
                if c.query(&image)?.is_none() {
                    c.pull_with_registry_config(&image)?;
                }
                let config = crate::agent::RunConfig::new(image, command)
                    .with_env(env)
                    .with_workdir(workdir)
                    .with_mounts(mounts_config)
                    .with_timeout(timeout)
                    .with_persistent_overlay(Some(overlay_id));
                c.run_streaming_with(config, |e| {
                    let _ = tx.send(e);
                })
            })
            .await
        } else {
            with_machine_client_traced(&execution_runtime, &entry_exec, tid, move |c| {
                c.vm_exec_streaming_with(command, env, workdir, timeout, |e| {
                    let _ = tx.send(e);
                })
            })
            .await
        };
        metrics::histogram!("smolvm_exec_seconds").record(start.elapsed().as_secs_f64());
        // Setup/transport failures (image pull, vsock) can't become an HTTP
        // status once the SSE has begun, so surface them as a terminal error
        // event instead of silently ending the stream.
        if let Err(e) = result {
            let _ = err_tx.send(crate::agent::ExecEvent::Error(format!("{e:?}")));
        }
    })
        .map_err(ApiError::internal)?
        .detach();

    // Yield each event as an SSE frame the instant it lands in the channel.
    let stream = async_stream::stream! {
        while let Ok(event) = rx.recv().await {
            let sse_event = match event {
                crate::agent::ExecEvent::Stdout(data) => Event::new()
                    .event("stdout")
                    .data(String::from_utf8_lossy(&data)),
                crate::agent::ExecEvent::Stderr(data) => Event::new()
                    .event("stderr")
                    .data(String::from_utf8_lossy(&data)),
                crate::agent::ExecEvent::Exit(code) => Event::new()
                    .event("exit")
                    .data(format!("{{\"exitCode\":{}}}", code)),
                crate::agent::ExecEvent::Error(msg) => Event::new()
                    .event("error")
                    .data(format!("{{\"message\":\"{}\"}}", msg)),
            };
            yield Ok::<_, Infallible>(sse_event);
        }
    };

    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

/// Run a command in an image.
///
/// This creates a temporary overlay from the image and runs the command.
#[utoipa::path(
    post,
    path = "/api/v1/machines/{id}/run",
    tag = "Execution",
    params(
        ("id" = String, Path, description = "Machine name")
    ),
    request_body = RunRequest,
    responses(
        (status = 200, description = "Command executed", body = ExecResponse),
        (status = 400, description = "Invalid request", body = ApiErrorResponse),
        (status = 404, description = "Machine not found", body = ApiErrorResponse),
        (status = 500, description = "Execution failed", body = ApiErrorResponse)
    )
)]
pub async fn run_command(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
    trace_id: Option<Extension<TraceId>>,
    Json(req): Json<RunRequest>,
) -> Result<Json<ExecResponse>, ApiError> {
    let tid = trace_id.map(|t| t.0 .0.clone());
    validate_command(&req.command)?;

    let entry = state.get_machine(&id)?;

    // Ensure machine is running and persist state to DB
    ensure_running_and_persist(&state, &id, &entry)
        .await
        .map_err(classify_ensure_running_error)?;

    crate::api::handlers::validate_request_secrets(&req.secrets)?;
    crate::api::handlers::validate_request_env(&req.env)?;
    let record_env = crate::api::handlers::record_secret_refs_env(&entry)?;
    let req_env = crate::api::handlers::resolve_request_secrets(&req.secrets)?;

    let image = req.image.clone();
    let command = req.command.clone();
    let mut env = EnvVar::to_tuples(&req.env);
    env.extend(crate::secrets::expose_into_env(record_env));
    env.extend(crate::secrets::expose_into_env(req_env));
    let workdir = req.workdir.clone();
    let timeout = req.timeout_secs.map(Duration::from_secs);

    // Get mounts from machine config (converted to protocol format)
    let mounts_config = {
        let entry = entry.lock();
        entry
            .mounts
            .iter()
            .enumerate()
            .map(|(i, m)| {
                let tag = HostMount::mount_tag(i);
                (tag, m.target.clone(), m.readonly)
            })
            .collect::<Vec<_>>()
    };

    let start = std::time::Instant::now();
    let (exit_code, stdout, stderr) = with_machine_client_traced(state.runtime()?, &entry, tid, move |c| {
        let config = crate::agent::RunConfig::new(image, command)
            .with_env(env)
            .with_workdir(workdir)
            .with_mounts(mounts_config)
            .with_timeout(timeout);
        c.run_non_interactive(config)
    })
    .await?;
    metrics::histogram!("smolvm_exec_seconds").record(start.elapsed().as_secs_f64());

    Ok(Json(ExecResponse {
        exit_code,
        stdout: String::from_utf8_lossy(&stdout).into_owned(),
        stderr: String::from_utf8_lossy(&stderr).into_owned(),
        stdout_b64: stdout,
        stderr_b64: stderr,
    }))
}

/// Query parameters for an interactive PTY session.
#[derive(Debug, serde::Deserialize)]
pub struct InteractiveQuery {
    /// Program to run (single argv[0]); defaults to `/bin/sh`.
    pub cmd: Option<String>,
    /// Initial terminal width in columns.
    pub cols: Option<u16>,
    /// Initial terminal height in rows.
    pub rows: Option<u16>,
}

/// Messages owned by the WebSocket writer task.
///
/// The reader and the blocking PTY session never write to the upgraded socket
/// directly. Keeping one writer preserves frame ordering and lets a closed
/// socket release both producers through the bounded channel.
enum InteractiveWebSocketOutput {
    Binary(Vec<u8>),
    Pong(Vec<u8>),
    Close(Vec<u8>),
    Exit(i32),
}

/// Interactive PTY session over a WebSocket.
///
/// The client connects a WebSocket; binary frames are forwarded to the
/// command's stdin and the PTY's output is sent back as binary frames. A JSON
/// text frame `{"type":"resize","cols":N,"rows":N}` resizes the terminal. When
/// the command exits, a final text frame `{"type":"exit","code":N}` is sent
/// before the socket closes.
///
/// Image machines run the program in their persistent-overlay container (the
/// same filesystem `exec` uses); plain machines run it directly in the VM.
pub async fn exec_interactive(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
    Query(q): Query<InteractiveQuery>,
    _trace_id: Option<Extension<TraceId>>,
    mut request: Request,
) -> Result<Response, ApiError> {
    // Keep malformed-handshake failures in smolvm's established JSON error
    // envelope rather than h12tiny's generic extractor rejection. The
    // constructor still captures the raw H1 upgrade future for the task below.
    let websocket = WebSocketUpgrade::try_from_request(&mut request)
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;

    let entry = state.get_machine(&id)?;
    ensure_running_and_persist(&state, &id, &entry)
        .await
        .map_err(classify_ensure_running_error)?;

    let machine_image = state.lookup_vm(&id).await?.and_then(|r| r.image);

    let command = vec![q.cmd.clone().unwrap_or_else(|| "/bin/sh".to_string())];
    let init_size = (q.cols.unwrap_or(80), q.rows.unwrap_or(24));

    // Snapshot mounts now (used only for image runs) so the upgrade closure
    // doesn't need to re-lock the entry.
    let mounts_config = {
        let e = entry.lock();
        e.mounts
            .iter()
            .enumerate()
            .map(|(i, m)| (HostMount::mount_tag(i), m.target.clone(), m.readonly))
            .collect::<Vec<_>>()
    };

    // h12tiny's optional WebSocket boundary has already validated RFC 6455
    // headers. It owns the 101 response and server-role framing, while this
    // handler owns the PTY protocol and every application-runtime task.
    let response = websocket.response();
    let runtime = state.runtime()?.clone();
    runtime
        .clone()
        .spawn(async move {
            let connection = match websocket.into_connection().await {
                Ok(connection) => connection,
                Err(error) => {
                    tracing::debug!(error = ?error, "pty: HTTP upgrade did not complete");
                    return;
                }
            };
            let (mut reader, writer) = connection.split();
            let (outgoing_tx, outgoing_rx) =
                async_channel::bounded::<InteractiveWebSocketOutput>(256);

            let writer_task = match runtime.spawn(async move {
                let mut writer = writer;
                while let Ok(output) = outgoing_rx.recv().await {
                    let write_result = match output {
                        InteractiveWebSocketOutput::Binary(bytes) => {
                            writer
                                .write_frame(WebSocketFrame::binary(WebSocketPayload::Owned(bytes)))
                                .await
                        }
                        InteractiveWebSocketOutput::Pong(bytes) => {
                            writer
                                .write_frame(WebSocketFrame::pong(WebSocketPayload::Owned(bytes)))
                                .await
                        }
                        InteractiveWebSocketOutput::Close(bytes) => {
                            let result = writer
                                .write_frame(WebSocketFrame::close_raw(WebSocketPayload::Owned(bytes)))
                                .await;
                            if result.is_ok() {
                                let _ = writer.flush().await;
                            }
                            break;
                        }
                        InteractiveWebSocketOutput::Exit(code) => {
                            let text = format!("{{\"type\":\"exit\",\"code\":{code}}}");
                            if writer
                                .write_frame(WebSocketFrame::text(WebSocketPayload::Owned(
                                    text.into_bytes(),
                                )))
                                .await
                                .is_ok()
                                && writer
                                    .write_frame(WebSocketFrame::close(1000, &[]))
                                    .await
                                    .is_ok()
                            {
                                let _ = writer.flush().await;
                            }
                            break;
                        }
                    };

                    if write_result.is_err() || writer.flush().await.is_err() {
                        break;
                    }
                }
            }) {
                Ok(task) => task,
                Err(error) => {
                    tracing::warn!(error = ?error, "pty: runtime stopped before WebSocket writer started");
                    return;
                }
            };

            // Input channel: WS reader → blocking session. Closing this sender
            // signals EOF to the synchronous PTY loop without blocking an async
            // executor worker.
            let (input_tx, input_rx) = std::sync::mpsc::channel::<crate::agent::InteractiveInput>();
            let _ = input_tx.send(crate::agent::InteractiveInput::Resize {
                cols: init_size.0,
                rows: init_size.1,
            });
            let reader_output = outgoing_tx.clone();
            let reader_task = match runtime.spawn(async move {
                let control_output = reader_output.clone();
                let mut send_control = move |frame: WebSocketFrame<'_>| {
                    let control_output = control_output.clone();
                    let output = match frame.opcode {
                        WebSocketOpCode::Pong => Some(InteractiveWebSocketOutput::Pong(Vec::from(frame.payload))),
                        WebSocketOpCode::Close => Some(InteractiveWebSocketOutput::Close(Vec::from(frame.payload))),
                        _ => None,
                    };
                    async move {
                        if let Some(output) = output {
                            control_output
                                .send(output)
                                .await
                                .map_err(|_| std::io::Error::from(std::io::ErrorKind::BrokenPipe))?;
                        }
                        Ok::<(), std::io::Error>(())
                    }
                };

                loop {
                    let frame = match reader.read_frame(&mut send_control).await {
                        Ok(frame) => frame,
                        Err(error) => {
                            tracing::debug!(error = ?error, "pty: invalid or disconnected WebSocket client");
                            let _ = reader_output
                                .send(InteractiveWebSocketOutput::Close(vec![0x03, 0xea]))
                                .await;
                            break;
                        }
                    };

                    match frame.opcode {
                        WebSocketOpCode::Binary => {
                            if input_tx
                                .send(crate::agent::InteractiveInput::Stdin(Vec::from(frame.payload)))
                                .is_err()
                            {
                                break;
                            }
                        }
                        WebSocketOpCode::Text => {
                            let text = Vec::from(frame.payload);
                            // FragmentCollectorRead has already enforced UTF-8
                            // for text frames. JSON controls retain the legacy
                            // interactive protocol; other text is stdin.
                            match serde_json::from_slice::<serde_json::Value>(&text) {
                                Ok(value) if value["type"] == "resize" => {
                                    let cols = value["cols"].as_u64().unwrap_or(80) as u16;
                                    let rows = value["rows"].as_u64().unwrap_or(24) as u16;
                                    if input_tx
                                        .send(crate::agent::InteractiveInput::Resize { cols, rows })
                                        .is_err()
                                    {
                                        break;
                                    }
                                }
                                Ok(value) if value["type"] == "stdin" => {
                                    if let Some(data) = value["data"].as_str() {
                                        if input_tx
                                            .send(crate::agent::InteractiveInput::Stdin(
                                                data.as_bytes().to_vec(),
                                            ))
                                            .is_err()
                                        {
                                            break;
                                        }
                                    }
                                }
                                _ if input_tx
                                    .send(crate::agent::InteractiveInput::Stdin(text))
                                    .is_err() => break,
                                _ => {}
                            }
                        }
                        WebSocketOpCode::Close => {
                            let _ = input_tx.send(crate::agent::InteractiveInput::Eof);
                            break;
                        }
                        // Ping and Pong are handled by the parser's automatic
                        // control-frame response path. Continuations are
                        // collected before this point.
                        WebSocketOpCode::Ping
                        | WebSocketOpCode::Pong
                        | WebSocketOpCode::Continuation => {}
                    }
                }
            }) {
                Ok(task) => task,
                Err(error) => {
                    tracing::warn!(error = ?error, "pty: runtime stopped before WebSocket reader started");
                    drop(outgoing_tx);
                    let _ = writer_task.await;
                    return;
                }
            };

            // Run the interactive session on a DEDICATED agent connection — NOT
            // the shared per-machine client. Holding the shared client lock for
            // a long-lived PTY would block every other operation on the machine.
            let session_entry = entry.clone();
            let session_output = outgoing_tx.clone();
            let session = runtime.spawn_blocking(move || {
                let connect = { session_entry.lock().manager.connect() };
                let mut client = match connect {
                    Ok(client) => client,
                    Err(error) => {
                        tracing::warn!(error = ?error, "pty: failed to open dedicated agent connection");
                        return -1;
                    }
                };
                let on_output = move |output| {
                    let bytes = match output {
                        crate::agent::InteractiveOutput::Stdout(bytes)
                        | crate::agent::InteractiveOutput::Stderr(bytes) => bytes,
                    };
                    // A closed browser socket drops the receiver and unblocks
                    // this synchronous callback instead of holding a PTY open.
                    let _ = session_output.send_blocking(InteractiveWebSocketOutput::Binary(bytes));
                };

                if let Some(image) = machine_image {
                    match client.query(&image) {
                        Ok(Some(_)) => {}
                        Ok(None) => {
                            if let Err(error) = client.pull_with_registry_config(&image) {
                                tracing::warn!(error = ?error, "pty: image pull failed");
                                return -1;
                            }
                        }
                        Err(error) => {
                            tracing::warn!(error = ?error, "pty: image query failed");
                            return -1;
                        }
                    }
                    let config = crate::agent::RunConfig::new(image, command)
                        .with_mounts(mounts_config)
                        .with_tty(true)
                        .with_persistent_overlay(Some(id));
                    client.run_interactive_io(config, input_rx, on_output).unwrap_or_else(|error| {
                        tracing::warn!(error = ?error, "pty: interactive run failed");
                        -1
                    })
                } else {
                    client
                        .vm_exec_interactive_io(command, Vec::new(), None, true, input_rx, on_output)
                        .unwrap_or_else(|error| {
                            tracing::warn!(error = ?error, "pty: interactive vm_exec failed");
                            -1
                        })
                }
            });

            let code = match session {
                Ok(session) => match session.await {
                    Ok(code) => code,
                    Err(error) => {
                        tracing::warn!(error = ?error, "pty: blocking session did not complete");
                        -1
                    }
                },
                Err(error) => {
                    tracing::warn!(error = ?error, "pty: runtime cannot schedule blocking session");
                    -1
                }
            };

            // FIFO ordering means all PTY output sent before the blocking task
            // returned is emitted before this terminal exit control frame.
            let _ = outgoing_tx.send(InteractiveWebSocketOutput::Exit(code)).await;
            drop(reader_task);
            drop(outgoing_tx);
            let _ = writer_task.await;
        })
        .map_err(ApiError::internal)?
        .detach();

    Ok(response)
}

/// Maximum number of concurrent log-follow SSE streams.
/// Each follower polls via `spawn_blocking` every 100ms, so capping concurrency
/// prevents blocking-pool saturation under high follower counts.
static LOG_FOLLOW_SEMAPHORE: std::sync::LazyLock<Semaphore> =
    std::sync::LazyLock::new(|| Semaphore::new(16));

/// Stream machine console logs via SSE.
#[utoipa::path(
    get,
    path = "/api/v1/machines/{id}/logs",
    tag = "Logs",
    params(
        ("id" = String, Path, description = "Machine name"),
        ("follow" = Option<bool>, Query, description = "Follow the logs (like tail -f)"),
        ("tail" = Option<usize>, Query, description = "Number of lines to show from the end")
    ),
    responses(
        (status = 200, description = "Log stream (SSE)", content_type = "text/event-stream"),
        (status = 404, description = "Machine or log file not found", body = ApiErrorResponse)
    )
)]
pub async fn stream_logs(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
    Query(query): Query<LogsQuery>,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, ApiError> {
    // get_machine only knows machines in the running map. Distinguish a real
    // typo (absent from the DB too) from a machine that exists but was never
    // started (present in the DB, no console log yet) so clients don't read a
    // created machine as "not found".
    let entry = match state.get_machine(&id) {
        Ok(e) => e,
        Err(_) => {
            let known = matches!(state.db().get_vm(&id), Ok(Some(_)));
            return Err(if known {
                ApiError::NotFound(format!(
                    "machine '{id}' has not been started yet — no logs available"
                ))
            } else {
                ApiError::NotFound(format!("machine '{id}' not found"))
            });
        }
    };

    // Get console log path
    let log_path: PathBuf = {
        let entry = entry.lock();
        entry
            .manager
            .console_log()
            .ok_or_else(|| ApiError::NotFound("console log not configured".into()))?
            .to_path_buf()
    };

    // Check if file exists (blocking check is acceptable here since it's fast)
    let path_check = log_path.clone();
    let exists = state.blocking(move || path_check.exists()).await?;

    if !exists {
        // The machine is registered (get_machine succeeded above) but has no
        // console log yet — it was created and never started, or hasn't produced
        // output. Report that plainly instead of leaking the internal host log
        // path, and give the same not-started-shaped hint as the DB-miss branch
        // (which a created machine skips, since it's in the running map).
        return Err(ApiError::NotFound(format!(
            "machine '{id}' has no logs yet — it may not have been started"
        )));
    }

    let follow = query.follow;
    let tail = query.tail;
    let json_only = query.format.as_deref() == Some("json");

    // Validate tail value upfront
    const MAX_TAIL_LINES: usize = 10_000;
    if let Some(n) = tail {
        if n > MAX_TAIL_LINES {
            return Err(ApiError::BadRequest(format!(
                "tail value {} exceeds maximum of {}",
                n, MAX_TAIL_LINES,
            )));
        }
    }

    // Acquire a follow permit if the client wants to follow. This limits
    // concurrent long-lived polling streams to prevent blocking-pool saturation.
    // The permit is moved into the stream so it's held for the stream's lifetime.
    let follow_permit = if follow {
        Some(
            LOG_FOLLOW_SEMAPHORE
                .try_acquire()
                .map_err(|_| ApiError::Conflict("too many concurrent log followers".into()))?,
        )
    } else {
        None
    };

    // For tail, read last N lines upfront using spawn_blocking with bounded memory
    let (initial_lines, start_pos) = if let Some(n) = tail {
        let path = log_path.clone();
        state
            .blocking(move || read_last_n_lines_bounded(&path, n))
            .await?
            .map_err(ApiError::internal)?
    } else {
        (Vec::new(), 0)
    };

    // Create the SSE stream
    let log_runtime = state.runtime()?.clone();
    let stream = async_stream::stream! {
        // Hold the follow permit for the stream's lifetime so it's released
        // when the client disconnects or the stream ends.
        let _permit = follow_permit;

        // Emit initial tail lines first
        for line in initial_lines {
            if json_only && serde_json::from_str::<serde_json::Value>(&line).is_err() {
                continue; // skip non-JSON lines in json mode
            }
            yield Ok(Event::new().data(line));
        }

        if tail.is_some() && !follow {
            return;
        }

        // For following or full read, poll the file for new content
        let mut pos = if tail.is_some() { start_pos } else { 0 };
        let mut partial_line = String::new();

        loop {
            // Read new content in spawn_blocking
            let path = log_path.clone();
            let current_pos = pos;

            let result = match log_runtime
                .spawn_blocking(move || read_from_position(&path, current_pos))
            {
                Ok(task) => task
                    .await
                    .map_err(std::io::Error::other)
                    .and_then(|result| result),
                Err(error) => Err(std::io::Error::other(error)),
            };

            match result {
                Ok((new_data, new_pos)) => {
                    pos = new_pos;
                    if !new_data.is_empty() {
                        partial_line.push_str(&new_data);
                        // Yield complete lines
                        while let Some(newline_pos) = partial_line.find('\n') {
                            let line = partial_line[..newline_pos].trim_end_matches('\r').to_string();
                            partial_line = partial_line[newline_pos + 1..].to_string();
                            if json_only && serde_json::from_str::<serde_json::Value>(&line).is_err() {
                                continue; // skip non-JSON lines in json mode
                            }
                            yield Ok(Event::new().data(line));
                        }
                        // Flush partial line if it exceeds the safety cap
                        if partial_line.len() > MAX_PARTIAL_LINE {
                            yield Ok(Event::new().data(partial_line.clone()));
                            partial_line.clear();
                        }
                    }
                }
                Err(e) => {
                    yield Ok(Event::new().data(format!("error: {}", e)));
                    break;
                }
            }

            if !follow {
                // Yield any remaining partial line
                if !partial_line.is_empty() {
                    yield Ok(Event::new().data(partial_line.clone()));
                }
                break;
            }

            // Wait before polling again
            crate::runtime::sleep(Duration::from_millis(100)).await;
        }
    };

    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

/// Read the last N lines from a file using a bounded ring buffer.
/// Returns (lines, file_position_at_end) for follow mode.
fn read_last_n_lines_bounded(
    path: &std::path::Path,
    n: usize,
) -> std::io::Result<(Vec<String>, u64)> {
    use std::collections::VecDeque;

    let file = std::fs::File::open(path)?;
    let metadata = file.metadata()?;
    let file_len = metadata.len();

    // n == 0 means "no tail lines" — skip reading the file entirely
    if n == 0 {
        return Ok((Vec::new(), file_len));
    }

    let reader = BufReader::new(file);

    // Use a ring buffer to keep only the last N lines in memory
    let mut ring: VecDeque<String> = VecDeque::with_capacity(n + 1);

    for line in reader.lines() {
        let line = line?;
        if ring.len() == n {
            ring.pop_front();
        }
        ring.push_back(line);
    }

    Ok((ring.into_iter().collect(), file_len))
}

/// Maximum bytes to read per poll cycle (64 KiB).
/// Bounds memory usage per follower and prevents a single large write from
/// blocking the async runtime.
const MAX_READ_CHUNK: u64 = 64 * 1024;

/// Maximum size of the partial (incomplete) line buffer (1 MiB).
/// If a log produces data without newlines beyond this limit, the partial
/// buffer is flushed as-is to prevent unbounded memory growth.
const MAX_PARTIAL_LINE: usize = BYTES_PER_MIB as usize;

/// Read new content from a file starting at a given position.
/// Reads at most `MAX_READ_CHUNK` bytes per call.
fn read_from_position(path: &std::path::Path, pos: u64) -> std::io::Result<(String, u64)> {
    use std::io::Read as _;

    let mut file = std::fs::File::open(path)?;
    let metadata = file.metadata()?;
    let file_len = metadata.len();

    if pos >= file_len {
        // No new content
        return Ok((String::new(), pos));
    }

    file.seek(SeekFrom::Start(pos))?;
    let to_read = std::cmp::min(file_len - pos, MAX_READ_CHUNK) as usize;
    let mut buf = vec![0u8; to_read];
    file.read_exact(&mut buf)?;
    let new_pos = pos + to_read as u64;

    let text = String::from_utf8_lossy(&buf).into_owned();
    Ok((text, new_pos))
}

#[cfg(test)]
mod websocket_tests {
    use super::*;
    use h12tiny::util::BodyExt;
    use h12tiny::web::{IntoResponse, StatusCode};

    #[test]
    fn malformed_websocket_handshake_keeps_the_api_json_error_envelope() {
        let directory = tempfile::tempdir().expect("create temporary database directory");
        let database = crate::db::SmolvmDb::open_at(&directory.path().join("smolvm.db"))
            .expect("open temporary database");
        let state = Arc::new(ApiState::with_db(database));
        let request = Request::builder()
            .version(h12tiny::web::Version::HTTP_11)
            .header("host", "localhost")
            .header("connection", "Upgrade")
            .header("upgrade", "websocket")
            .header("sec-websocket-version", "13")
            .body(h12tiny::util::boxed_body(h12tiny::util::empty_body()))
            .expect("the malformed handshake is valid HTTP");
        let runtime = crate::runtime::Runtime::with_workers(1).expect("start test runtime");

        let error = match runtime.block_on(exec_interactive(
            State(state),
            Path("machine".to_owned()),
            Query(InteractiveQuery {
                cmd: None,
                cols: None,
                rows: None,
            }),
            None,
            request,
        )) {
            Ok(_) => panic!("a missing Sec-WebSocket-Key must fail before VM lookup"),
            Err(error) => error,
        };

        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = runtime
            .block_on(response.into_body().collect())
            .expect("collect JSON error body")
            .to_bytes();
        let body: serde_json::Value =
            serde_json::from_slice(&body).expect("parse API error response as JSON");
        assert_eq!(body["code"], "BAD_REQUEST");
        assert!(body["error"].as_str().is_some_and(|message| {
            message.contains("Sec-WebSocket-Key")
        }));
    }
}
