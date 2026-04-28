//! Session-control abstraction.
//!
//! [`SessionProxy`] is the boundary between consumers (TUI monitor, CLI
//! `dpe status` / `dpe progress` / `dpe stop`) and the control transport.
//! Consumers depend only on the trait; transports implement it.
//!
//! Two implementations:
//! - [`ControlSocketProxy`] — production. Sends `Request::*` over the
//!   per-session local socket (`UDS` on Unix, named pipe on Windows) and
//!   parses the [`Response`] back.
//! - [`MockSessionProxy`] — test-only. Returns canned responses so we can
//!   drive the TUI and command handlers without spawning a real runner.
//!
//! The trait uses native `async fn` (Rust 1.75+) — no `async_trait` crate.
//! Consumers use generic dispatch (`P: SessionProxy`) rather than trait
//! objects, which keeps the surface object-safety-free and dependency-free.

use std::path::PathBuf;

use crate::control::{
    self, ProgressReport, Request, Response, StatusReport,
};

#[derive(Debug, thiserror::Error)]
pub enum ProxyError {
    #[error("control channel unreachable: {0}")]
    Unreachable(String),
    #[error("control responded with error: {0}")]
    Refused(String),
    #[error("io: {0}")]
    Io(String),
}

/// Operations exposed by a live runner session.
///
/// Implementations are responsible for translating logical operations
/// (status, stop, …) into the underlying transport's request/response
/// shape, and for surfacing transport errors as [`ProxyError`].
pub trait SessionProxy: Send {
    fn status(&mut self)   -> impl std::future::Future<Output = Result<StatusReport, ProxyError>> + Send;
    fn progress(&mut self) -> impl std::future::Future<Output = Result<ProgressReport, ProxyError>> + Send;
    fn pause(&mut self, stage: Option<String>) -> impl std::future::Future<Output = Result<(), ProxyError>> + Send;
    fn resume(&mut self, stage: Option<String>) -> impl std::future::Future<Output = Result<(), ProxyError>> + Send;
    fn stop(&mut self)     -> impl std::future::Future<Output = Result<(), ProxyError>> + Send;
    fn kill(&mut self)     -> impl std::future::Future<Output = Result<(), ProxyError>> + Send;
}

/// Production proxy: speaks the JSON-line protocol over the per-session
/// local socket. One [`ControlSocketProxy`] is bound to one session dir;
/// every method opens a fresh connection (matches today's `send_request`
/// behavior — control sockets are short-lived request/response, not a
/// persistent stream).
pub struct ControlSocketProxy {
    session_dir: PathBuf,
}

impl ControlSocketProxy {
    pub fn new(session_dir: PathBuf) -> Self {
        Self { session_dir }
    }

    async fn send(&self, req: Request) -> Result<Response, ProxyError> {
        control::send_request(&self.session_dir, &req)
            .await
            .map_err(|e| ProxyError::Unreachable(e.to_string()))
    }
}

impl SessionProxy for ControlSocketProxy {
    async fn status(&mut self) -> Result<StatusReport, ProxyError> {
        let resp = self.send(Request::Status).await?;
        if !resp.ok {
            return Err(ProxyError::Refused(resp.error.unwrap_or_default()));
        }
        resp.status.ok_or_else(|| ProxyError::Refused("response missing status payload".into()))
    }

    async fn progress(&mut self) -> Result<ProgressReport, ProxyError> {
        let resp = self.send(Request::Progress).await?;
        if !resp.ok {
            return Err(ProxyError::Refused(resp.error.unwrap_or_default()));
        }
        resp.progress.ok_or_else(|| ProxyError::Refused("response missing progress payload".into()))
    }

    async fn pause(&mut self, stage: Option<String>) -> Result<(), ProxyError> {
        ack(self.send(Request::Pause { stage }).await?)
    }

    async fn resume(&mut self, stage: Option<String>) -> Result<(), ProxyError> {
        ack(self.send(Request::Resume { stage }).await?)
    }

    async fn stop(&mut self) -> Result<(), ProxyError> {
        ack(self.send(Request::Stop).await?)
    }

    async fn kill(&mut self) -> Result<(), ProxyError> {
        ack(self.send(Request::Kill).await?)
    }
}

fn ack(resp: Response) -> Result<(), ProxyError> {
    if resp.ok { Ok(()) } else { Err(ProxyError::Refused(resp.error.unwrap_or_default())) }
}

// ═══ Test mock ═════════════════════════════════════════════════════════════

/// In-memory proxy for tests. Each method returns the next queued response
/// or — if the queue is empty — a default value. Methods that record
/// commands (pause/resume/stop/kill) push the call to a vec the test can
/// inspect.
#[cfg(test)]
pub mod mock {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Clone)]
    pub struct MockSessionProxy {
        pub status_queue:   Arc<Mutex<VecDeque<Result<StatusReport, ProxyError>>>>,
        pub progress_queue: Arc<Mutex<VecDeque<Result<ProgressReport, ProxyError>>>>,
        pub commands:       Arc<Mutex<Vec<MockCommand>>>,
        /// What ack-style commands return when the queue's not stocked.
        pub default_ack:    Result<(), ProxyError>,
    }

    impl Default for MockSessionProxy {
        fn default() -> Self {
            Self {
                status_queue:   Arc::default(),
                progress_queue: Arc::default(),
                commands:       Arc::default(),
                default_ack:    Ok(()),
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum MockCommand {
        Pause(Option<String>),
        Resume(Option<String>),
        Stop,
        Kill,
    }

    impl MockSessionProxy {
        pub fn new() -> Self { Self::default() }

        pub fn enqueue_status(self, s: StatusReport) -> Self {
            self.status_queue.lock().unwrap().push_back(Ok(s));
            self
        }
        pub fn enqueue_status_err(self, e: ProxyError) -> Self {
            self.status_queue.lock().unwrap().push_back(Err(e));
            self
        }
        pub fn enqueue_progress(self, p: ProgressReport) -> Self {
            self.progress_queue.lock().unwrap().push_back(Ok(p));
            self
        }
        pub fn commands_seen(&self) -> Vec<MockCommand> {
            self.commands.lock().unwrap().clone()
        }
    }

    impl SessionProxy for MockSessionProxy {
        async fn status(&mut self) -> Result<StatusReport, ProxyError> {
            self.status_queue.lock().unwrap().pop_front()
                .unwrap_or_else(|| Ok(StatusReport::default()))
        }
        async fn progress(&mut self) -> Result<ProgressReport, ProxyError> {
            self.progress_queue.lock().unwrap().pop_front()
                .unwrap_or_else(|| Ok(ProgressReport::default()))
        }
        async fn pause(&mut self, stage: Option<String>) -> Result<(), ProxyError> {
            self.commands.lock().unwrap().push(MockCommand::Pause(stage));
            self.default_ack.clone()
        }
        async fn resume(&mut self, stage: Option<String>) -> Result<(), ProxyError> {
            self.commands.lock().unwrap().push(MockCommand::Resume(stage));
            self.default_ack.clone()
        }
        async fn stop(&mut self) -> Result<(), ProxyError> {
            self.commands.lock().unwrap().push(MockCommand::Stop);
            self.default_ack.clone()
        }
        async fn kill(&mut self) -> Result<(), ProxyError> {
            self.commands.lock().unwrap().push(MockCommand::Kill);
            self.default_ack.clone()
        }
    }

    impl Clone for ProxyError {
        fn clone(&self) -> Self {
            match self {
                Self::Unreachable(s) => Self::Unreachable(s.clone()),
                Self::Refused(s)     => Self::Refused(s.clone()),
                Self::Io(s)          => Self::Io(s.clone()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::mock::{MockCommand, MockSessionProxy};
    use crate::control::PipelineState;

    #[tokio::test]
    async fn mock_status_returns_queued_value() {
        let report = StatusReport {
            pipeline: "p".into(),
            variant: "main".into(),
            session: "s1".into(),
            state: PipelineState::Running,
            started_at: 42,
            stages: vec![],
        };
        let mut proxy = MockSessionProxy::new().enqueue_status(report.clone());
        let got = proxy.status().await.unwrap();
        assert_eq!(got.session, "s1");
        assert_eq!(got.state, PipelineState::Running);
    }

    #[tokio::test]
    async fn mock_status_error_surfaces() {
        let mut proxy = MockSessionProxy::new()
            .enqueue_status_err(ProxyError::Unreachable("dead".into()));
        let err = proxy.status().await.unwrap_err();
        assert!(matches!(err, ProxyError::Unreachable(_)));
    }

    #[tokio::test]
    async fn mock_progress_returns_default_when_queue_empty() {
        let mut proxy = MockSessionProxy::new();
        let got = proxy.progress().await.unwrap();
        assert_eq!(got.rows_total, 0);
    }

    #[tokio::test]
    async fn mock_records_commands() {
        let mut proxy = MockSessionProxy::new();
        proxy.pause(Some("stage1".into())).await.unwrap();
        proxy.resume(None).await.unwrap();
        proxy.stop().await.unwrap();
        proxy.kill().await.unwrap();
        assert_eq!(
            proxy.commands_seen(),
            vec![
                MockCommand::Pause(Some("stage1".into())),
                MockCommand::Resume(None),
                MockCommand::Stop,
                MockCommand::Kill,
            ]
        );
    }

    #[tokio::test]
    async fn mock_default_ack_can_fail() {
        let mut proxy = MockSessionProxy {
            default_ack: Err(ProxyError::Refused("pipeline already stopped".into())),
            ..Default::default()
        };
        let err = proxy.stop().await.unwrap_err();
        assert!(matches!(err, ProxyError::Refused(_)));
    }
}
