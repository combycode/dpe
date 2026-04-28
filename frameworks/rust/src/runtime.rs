//! Runtime — main loop, queue management, signal handling.

use std::collections::HashMap;
use std::io::{self, BufRead, BufWriter, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use serde_json::Value;

use crate::accumulators::Memory;
use crate::context::{Context, QueueItem};
use crate::envelope;

/// Processor function type.
pub type ProcessorFn = fn(Value, &Value, &mut Context<'_>);

/// Tool definition — maps processor names to functions.
pub struct Tool {
    pub input_fn: ProcessorFn,
    pub queue_fns: HashMap<String, ProcessorFn>,
}

impl Tool {
    pub fn new(input_fn: ProcessorFn) -> Self {
        Self {
            input_fn,
            queue_fns: HashMap::new(),
        }
    }

    pub fn queue(mut self, name: &str, f: ProcessorFn) -> Self {
        self.queue_fns.insert(name.to_string(), f);
        self
    }
}

/// Parse settings from argv[1] JSON string.
pub fn parse_settings() -> Value {
    std::env::args()
        .nth(1)
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or(Value::Object(serde_json::Map::new()))
}

/// Main run loop.
pub fn run(tool: Tool) {
    let settings = parse_settings();
    let mut memory = Memory::new();
    let mut queue: Vec<QueueItem> = Vec::new();

    // Signal handling
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();
    let _ = ctrlc_handler(shutdown_clone);

    let stdin = io::stdin();
    let reader = stdin.lock();
    let stdout = io::stdout();
    let mut stdout_buf = BufWriter::new(stdout.lock());
    let stderr = io::stderr();
    let mut stderr_buf = BufWriter::new(stderr.lock());

    for line in reader.lines() {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };

        let envelope = match envelope::parse_envelope(&line) {
            Some(e) => e,
            None => continue,
        };

        if envelope.get("t").and_then(|t| t.as_str()) != Some("d") {
            continue;
        }

        let id = envelope.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let src = envelope.get("src").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let v = envelope.get("v").cloned().unwrap_or(Value::Object(serde_json::Map::new()));

        {
            let mut ctx = Context::new(
                id.clone(), src.clone(), &mut memory, &mut queue,
                &mut stdout_buf, &mut stderr_buf,
            );
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                (tool.input_fn)(v.clone(), &settings, &mut ctx);
            })) {
                Ok(_) => {},
                Err(_) => {
                    ctx.error(&v, "processor panicked");
                }
            }
        }

        // Auto-drain
        if !queue.is_empty() {
            drain_queue(&mut queue, &tool.queue_fns, &settings, &mut memory,
                        &mut stdout_buf, &mut stderr_buf);
        }
    }

    // Final drain after stdin EOF
    if !queue.is_empty() {
        drain_queue(&mut queue, &tool.queue_fns, &settings, &mut memory,
                    &mut stdout_buf, &mut stderr_buf);
    }
}

/// Drain all queued items until empty.
pub fn drain_queue(
    queue: &mut Vec<QueueItem>,
    processors: &HashMap<String, ProcessorFn>,
    settings: &Value,
    memory: &mut Memory,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) {
    let max_iterations = 100_000;
    let mut iterations = 0;

    while !queue.is_empty() && iterations < max_iterations {
        let (name, v, item_id, item_src) = queue.remove(0);

        let proc = match processors.get(&name) {
            Some(f) => f,
            None => {
                envelope::write_log(
                    &format!("No processor for queue '{}', dropping item", name),
                    "warn", stderr,
                );
                continue;
            }
        };

        let mut ctx = Context::new(
            item_id, item_src, memory, queue,
            stdout, stderr,
        );

        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            proc(v.clone(), settings, &mut ctx);
        })) {
            Ok(_) => {},
            Err(_) => {
                ctx.error(&v, "queue processor panicked");
            }
        }

        iterations += 1;
    }

    if iterations >= max_iterations {
        envelope::write_log("Queue drain hit safety limit", "error", stderr);
    }
}

fn ctrlc_handler(shutdown: Arc<AtomicBool>) -> Result<(), Box<dyn std::error::Error>> {
    // Set up SIGTERM/SIGINT handler
    // On Windows, only Ctrl+C is reliably caught
    ctrlc::set_handler(move || {
        shutdown.store(true, Ordering::Relaxed);
    })?;
    Ok(())
}
