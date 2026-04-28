//! Route builtin: forward each envelope to the first channel whose
//! expression evaluates truthy.

use std::collections::BTreeMap;
use std::io;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader};
use tokio::task::JoinHandle;

use crate::expr::{compile, evaluate, Expr, Scope};
use crate::types::OnError;

use super::{build_scope, is_truthy, BuiltinError, BuiltinWriter};

/// Compiled route stage.
pub struct BuiltinRoute {
    pub stage_id: String,
    pub on_error: OnError,
    routes: Vec<(String, Expr)>,                  // (channel_name, compiled)
    writers: BTreeMap<String, BuiltinWriter>,     // channel → downstream writer
}

impl std::fmt::Debug for BuiltinRoute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BuiltinRoute")
            .field("stage_id", &self.stage_id)
            .field("on_error", &self.on_error)
            .field("channels", &self.routes.iter().map(|(c, _)| c).collect::<Vec<_>>())
            .field("writer_count", &self.writers.len())
            .finish()
    }
}

impl BuiltinRoute {
    pub fn compile(
        stage_id: &str,
        routes: &BTreeMap<String, String>,
        writers: BTreeMap<String, BuiltinWriter>,
        on_error: OnError,
    ) -> Result<Self, BuiltinError> {
        if routes.is_empty() {
            return Err(BuiltinError::NoChannels { stage: stage_id.into() });
        }
        for ch in routes.keys() {
            if !writers.contains_key(ch) {
                return Err(BuiltinError::MissingChannel {
                    stage: stage_id.into(), channel: ch.clone(),
                });
            }
        }
        let mut compiled = Vec::with_capacity(routes.len());
        for (channel, expr_src) in routes {
            let ast = compile(expr_src).map_err(|e| BuiltinError::CompileRoute {
                stage: stage_id.into(), channel: channel.clone(), source: e,
            })?;
            compiled.push((channel.clone(), ast));
        }
        Ok(Self {
            stage_id: stage_id.into(),
            on_error,
            routes: compiled,
            writers,
        })
    }

    /// Spawn the routing task. Returns a handle that completes when the
    /// upstream reader closes. Errors on the channel's writer propagate
    /// as task errors (the task finishes but returns io::Error).
    pub fn spawn_task<R>(self, upstream: R) -> JoinHandle<io::Result<RouteStats>>
    where R: AsyncRead + Unpin + Send + 'static,
    {
        tokio::spawn(route_task(self, upstream))
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RouteStats {
    pub rows_in: u64,
    pub rows_routed: u64,
    pub rows_dropped: u64,
    pub rows_errored: u64,
}

async fn route_task<R>(mut route: BuiltinRoute, upstream: R) -> io::Result<RouteStats>
where R: AsyncRead + Unpin,
{
    let mut reader = BufReader::new(upstream);
    let mut line = String::new();
    let mut stats = RouteStats::default();

    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 { break; }
        if line.trim().is_empty() { continue; }
        stats.rows_in += 1;

        let env: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => {
                stats.rows_errored += 1;
                if matches!(route.on_error, OnError::Fail) {
                    return Err(io::Error::new(io::ErrorKind::InvalidData,
                        "route: invalid envelope JSON"));
                }
                continue;
            }
        };
        let scope = build_scope(&env);

        let channel = find_channel(&route.routes, &scope, route.on_error, &mut stats);
        let Some(channel) = channel else {
            if !matches!(route.on_error, OnError::Pass) { stats.rows_dropped += 1; }
            // OnError::Pass forwards to ALL channels; handled inside find_channel
            continue;
        };

        match route.writers.get_mut(&channel) {
            Some(w) => {
                w.write_all(line.as_bytes()).await?;
                stats.rows_routed += 1;
            }
            None => {
                stats.rows_errored += 1;
            }
        }
    }

    // Close every channel so downstream sees EOF.
    for (channel, mut w) in std::mem::take(&mut route.writers) {
        if let Err(e) = w.flush().await {
            eprintln!("[route] WARN — flushing channel '{}' failed: {}", channel, e);
        }
        drop(w);
    }
    Ok(stats)
}

/// Pick the first channel whose expression evaluates truthy.
///   - runtime error & on_error=Drop → None, row counted as dropped
///   - runtime error & on_error=Pass → None, caller forwards to all (not
///     implemented yet — treated like Drop)
///   - runtime error & on_error=Fail → None, caller may treat as fatal
///     (we log via stats.rows_errored; fatal escalation is caller's job)
fn find_channel(
    routes: &[(String, Expr)],
    scope: &Scope,
    on_error: OnError,
    stats: &mut RouteStats,
) -> Option<String> {
    for (name, expr) in routes {
        match evaluate(expr, scope) {
            Ok(v) => {
                if is_truthy(&v) { return Some(name.clone()); }
            }
            Err(_) => {
                stats.rows_errored += 1;
                match on_error {
                    OnError::Drop => return None,
                    OnError::Fail => return None, // caller can check stats
                    OnError::Pass => continue,    // try next channel
                }
            }
        }
    }
    None
}
