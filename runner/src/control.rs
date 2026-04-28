//! Control plane — local-socket JSON RPC between runner and CLI.
//!
//! Transport: `interprocess::local_socket` — Windows named pipe on Windows,
//! Unix domain socket on Unix. **No TCP.** Address written to
//! `$session/control.addr`.
//!
//! Address file contents (single line):
//!   - Unix:     `fs:/abs/path/to/session/control.sock`
//!   - Windows:  `ns:dpe-<session_id>`
//!
//! Commands: status, pause, resume, stop, kill, progress.
//! Wire: newline-delimited JSON.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use interprocess::local_socket::{
    tokio::{prelude::*, Listener, Stream},
    GenericFilePath, GenericNamespaced, ListenerOptions, Name, ToFsName, ToNsName,
};
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;

// ═══ Wire protocol types ══════════════════════════════════════════════════

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "lowercase")]
pub enum Request {
    Status,
    Pause    { stage: Option<String> },
    Resume   { stage: Option<String> },
    Stop,
    Kill,
    Progress,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status:   Option<StatusReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress: Option<ProgressReport>,
}

impl Response {
    pub fn ok() -> Self { Self { ok: true, error: None, status: None, progress: None } }
    pub fn err(msg: impl Into<String>) -> Self {
        Self { ok: false, error: Some(msg.into()), status: None, progress: None }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StatusReport {
    pub pipeline: String,
    pub variant:  String,
    pub session:  String,
    pub state:    PipelineState,
    pub started_at: u64,
    pub stages: Vec<StageStatus>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PipelineState {
    #[default]
    Idle,
    Running,
    Paused,
    Stopping,
    Stopped,
    Failed,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StageStatus {
    pub sid: String,
    pub tool: String,
    pub state: PipelineState,
    pub rows: u64,
    pub errors: u64,
    pub replicas: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProgressReport {
    pub gates: Vec<GateProgress>,
    pub rows_total: u64,
    pub errors_total: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GateProgress {
    pub name: String,
    pub count: u64,
    pub predicate_met: bool,
}

// ═══ Shared state ═════════════════════════════════════════════════════════

/// Mutable state the runner publishes and the control server reads.
#[derive(Debug, Default)]
pub struct ControlState {
    status:   StatusReport,
    progress: ProgressReport,
    command_tx: Option<mpsc::Sender<ControlCommand>>,
}

/// One-shot command sent from the server to the runner.
#[derive(Debug, Clone)]
pub enum ControlCommand {
    Pause  { stage: Option<String> },
    Resume { stage: Option<String> },
    Stop,
    Kill,
}

#[derive(Debug, Clone, Default)]
pub struct ControlHandle {
    inner: Arc<RwLock<ControlState>>,
}

impl ControlHandle {
    pub fn new(command_tx: mpsc::Sender<ControlCommand>) -> Self {
        Self { inner: Arc::new(RwLock::new(ControlState {
            command_tx: Some(command_tx), ..Default::default()
        })) }
    }

    pub async fn set_status(&self, s: StatusReport) {
        self.inner.write().await.status = s;
    }
    pub async fn set_progress(&self, p: ProgressReport) {
        self.inner.write().await.progress = p;
    }
    pub async fn snapshot_status(&self) -> StatusReport {
        self.inner.read().await.status.clone()
    }
    pub async fn snapshot_progress(&self) -> ProgressReport {
        self.inner.read().await.progress.clone()
    }
}

// ═══ Address plumbing ═════════════════════════════════════════════════════

/// Pick a platform-appropriate socket address for this session and return
/// (1) the line to write into `control.addr`, and (2) the Name handle for
/// listener creation.
fn addr_for_session(
    #[allow(unused_variables)] session_dir: &Path,
    session_id: &str,
) -> io::Result<(String, Name<'static>)> {
    #[cfg(windows)] {
        let name_str = format!("dpe-{}", session_id);
        // to_ns_name consumes String; returned Name is 'static.
        let name = name_str.clone().to_ns_name::<GenericNamespaced>()?;
        Ok((format!("ns:{}", name_str), name))
    }
    #[cfg(unix)] {
        // sockaddr_un.sun_path caps the socket path at 108 bytes on Linux,
        // 104 on macOS. Test rigs put the session under /var/folders/.../T/
        // which alone exceeds 104. Stage the socket in a guaranteed-short
        // location instead, and let `control.addr` in the session dir carry
        // the (possibly long) reference to it.
        let path = short_socket_path(session_id);
        let path_str = path.to_string_lossy().to_string();
        // Remove stale socket file from a previous crash. Only NotFound is
        // benign — anything else we want to know about because bind will
        // fail with a misleading "address in use" error a moment later.
        if let Err(e) = std::fs::remove_file(&path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                eprintln!("[control] WARN — could not remove stale socket {:?}: {}", path, e);
            }
        }
        let name = path_str.clone().to_fs_name::<GenericFilePath>()?;
        Ok((format!("fs:{}", path_str), name))
    }
}

/// Build a short Unix-socket path that fits sockaddr_un.sun_path on every
/// supported platform (108 bytes Linux, 104 bytes macOS).
///
/// The path includes three uniqueness components: a hash of the session id,
/// the current process id, and a process-wide atomic counter. The counter
/// matters in tests where many parallel test threads in the same process
/// share a hardcoded session id ("test"), and would otherwise all derive
/// the same path → race on `bind()` → EADDRINUSE. The pid component
/// disambiguates concurrent test binaries (cargo runs each integration
/// test file as its own process).
#[cfg(unix)]
fn short_socket_path(session_id: &str) -> PathBuf {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let mut h = DefaultHasher::new();
    session_id.hash(&mut h);
    let tag = format!(
        "{:08x}-{:x}-{:x}",
        h.finish() as u32,
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed),
    );
    std::env::temp_dir().join(format!("dpe-{}.sock", tag))
}

/// Parse a control.addr file content into a Name usable for connect.
pub fn parse_addr(raw: &str) -> io::Result<Name<'static>> {
    let trimmed = raw.trim();
    let (kind, rest) = trimmed.split_once(':').ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "control.addr missing 'kind:' prefix")
    })?;
    let rest_owned = rest.to_string();
    match kind {
        "ns" => rest_owned.to_ns_name::<GenericNamespaced>(),
        "fs" => rest_owned.to_fs_name::<GenericFilePath>(),
        other => Err(io::Error::new(io::ErrorKind::InvalidData,
            format!("control.addr kind '{}' not recognised (want fs or ns)", other))),
    }
}

// ═══ Server ═══════════════════════════════════════════════════════════════

#[derive(Debug)]
pub struct ControlServer {
    pub addr_file: PathBuf,
    _handle: JoinHandle<()>,
}

impl ControlServer {
    /// Bind a local socket derived from `session_id`, write the address to
    /// `$session/control.addr`, spawn accept loop.
    pub async fn start(
        session_dir: &Path,
        session_id: &str,
        handle: ControlHandle,
    ) -> io::Result<Self> {
        fs::create_dir_all(session_dir).await?;
        let (addr_line, name) = addr_for_session(session_dir, session_id)?;
        let opts = ListenerOptions::new().name(name);
        let listener = opts.create_tokio()?;
        let addr_file = session_dir.join("control.addr");
        fs::write(&addr_file, &addr_line).await?;
        let server_handle = tokio::spawn(accept_loop(listener, handle));
        Ok(Self { addr_file, _handle: server_handle })
    }
}

async fn accept_loop(listener: Listener, handle: ControlHandle) {
    loop {
        match listener.accept().await {
            Ok(stream) => {
                let h = handle.clone();
                tokio::spawn(handle_client(stream, h));
            }
            Err(_) => return,
        }
    }
}

async fn handle_client(stream: Stream, handle: ControlHandle) {
    let (read, mut write) = stream.split();
    let mut reader = BufReader::new(read).lines();

    while let Ok(Some(line)) = reader.next_line().await {
        let line = line.trim();
        if line.is_empty() { continue; }

        let resp = match serde_json::from_str::<Request>(line) {
            Ok(req) => dispatch(req, &handle).await,
            Err(e)  => Response::err(format!("invalid request: {}", e)),
        };
        let mut out = serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into());
        out.push('\n');
        if write.write_all(out.as_bytes()).await.is_err() { break; }
    }
}

async fn dispatch(req: Request, handle: &ControlHandle) -> Response {
    match req {
        Request::Status => {
            let s = handle.snapshot_status().await;
            Response { ok: true, error: None, status: Some(s), progress: None }
        }
        Request::Progress => {
            let p = handle.snapshot_progress().await;
            Response { ok: true, error: None, status: None, progress: Some(p) }
        }
        Request::Pause { stage }  => send_cmd(handle, ControlCommand::Pause { stage }).await,
        Request::Resume { stage } => send_cmd(handle, ControlCommand::Resume { stage }).await,
        Request::Stop             => send_cmd(handle, ControlCommand::Stop).await,
        Request::Kill             => send_cmd(handle, ControlCommand::Kill).await,
    }
}

async fn send_cmd(handle: &ControlHandle, cmd: ControlCommand) -> Response {
    let state = handle.inner.read().await;
    let Some(tx) = &state.command_tx else {
        return Response::err("runner not ready to accept commands");
    };
    if tx.send(cmd).await.is_err() {
        return Response::err("runner command channel closed");
    }
    Response::ok()
}

// ═══ Client helper ════════════════════════════════════════════════════════

/// Read `$session/control.addr`, connect, send one request, return one response.
/// Used by CLI subcommands `status`/`stop`/`monitor`.
pub async fn send_request(
    session_dir: &Path,
    request: &Request,
) -> io::Result<Response> {
    let raw = fs::read_to_string(session_dir.join("control.addr")).await?;
    let name = parse_addr(&raw)?;
    let stream = Stream::connect(name).await?;
    let (read, mut write) = stream.split();
    let body = serde_json::to_string(request)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    write.write_all(body.as_bytes()).await?;
    write.write_all(b"\n").await?;
    write.flush().await?;

    let mut reader = BufReader::new(read).lines();
    let Some(line) = reader.next_line().await? else {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "no response"));
    };
    serde_json::from_str(&line)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_addr_round_trip_ns() {
        let _ = parse_addr("ns:dpe-test").expect("ns parses");
    }

    #[cfg(unix)]
    #[test]
    fn parse_addr_round_trip_fs() {
        let _ = parse_addr("fs:/tmp/dpe-test.sock").expect("fs parses");
    }

    #[test]
    fn parse_addr_rejects_unknown_kind() {
        assert!(parse_addr("tcp:127.0.0.1:1234").is_err());
    }
}
