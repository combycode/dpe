//! Filter builtin: keep envelopes whose expression evaluates truthy,
//! drop the rest.

use std::io;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader};
use tokio::task::JoinHandle;

use crate::expr::{compile, evaluate, Expr};
use crate::types::{FilterOnFalse, OnError};

use super::{build_scope, is_truthy, BuiltinError, BuiltinWriter};

pub struct BuiltinFilter {
    pub stage_id: String,
    pub on_false: FilterOnFalse,
    pub on_error: OnError,
    expr: Expr,
    writer: BuiltinWriter,
}

impl std::fmt::Debug for BuiltinFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BuiltinFilter")
            .field("stage_id", &self.stage_id)
            .field("on_false", &self.on_false)
            .field("on_error", &self.on_error)
            .finish()
    }
}

impl BuiltinFilter {
    pub fn compile(
        stage_id: &str,
        expression: &str,
        writer: BuiltinWriter,
        on_false: FilterOnFalse,
        on_error: OnError,
    ) -> Result<Self, BuiltinError> {
        let expr = compile(expression).map_err(|e| BuiltinError::CompileFilter {
            stage: stage_id.into(), source: e,
        })?;
        Ok(Self { stage_id: stage_id.into(), on_false, on_error, expr, writer })
    }

    pub fn spawn_task<R>(self, upstream: R) -> JoinHandle<io::Result<FilterStats>>
    where R: AsyncRead + Unpin + Send + 'static,
    {
        tokio::spawn(filter_task(self, upstream))
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct FilterStats {
    pub rows_in: u64,
    pub rows_passed: u64,
    pub rows_dropped: u64,
    pub rows_errored: u64,
}

async fn filter_task<R>(mut filter: BuiltinFilter, upstream: R) -> io::Result<FilterStats>
where R: AsyncRead + Unpin,
{
    let mut reader = BufReader::new(upstream);
    let mut line = String::new();
    let mut stats = FilterStats::default();

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
                if matches!(filter.on_error, OnError::Fail) {
                    return Err(io::Error::new(io::ErrorKind::InvalidData,
                        "filter: invalid envelope JSON"));
                }
                continue;
            }
        };
        let scope = build_scope(&env);

        let keep = match evaluate(&filter.expr, &scope) {
            Ok(v) => is_truthy(&v),
            Err(_) => {
                stats.rows_errored += 1;
                match filter.on_error {
                    OnError::Drop => false,
                    OnError::Pass => true,
                    OnError::Fail => return Err(io::Error::other(
                        "filter: expression error")),
                }
            }
        };

        if keep {
            filter.writer.write_all(line.as_bytes()).await?;
            stats.rows_passed += 1;
        } else {
            stats.rows_dropped += 1;
            // on_false: drop (default) → nothing more to do
            //           emit-meta → would emit a meta envelope (deferred)
            //           emit-stderr → emit error-shaped record (deferred)
            let _ = filter.on_false; // silence unused warning until those land
        }
    }

    if let Err(e) = filter.writer.flush().await {
        eprintln!("[filter] WARN — final writer flush failed: {}", e);
    }
    drop(filter.writer);
    Ok(stats)
}
