//! Context object — passed to every processor invocation.

use serde_json::{Map, Value};
use std::io::Write;

use crate::accumulators::Memory;
use crate::envelope;

/// Queue item: (queue_name, v, id, src)
pub type QueueItem = (String, Value, String, String);

/// Processing context created per invocation.
pub struct Context<'a> {
    pub id: String,
    pub src: String,
    pub memory: &'a mut Memory,
    queue: &'a mut Vec<QueueItem>,
    stdout: &'a mut dyn Write,
    stderr: &'a mut dyn Write,
    /// Labels accumulated by ctx.trace(k, v). Flushed (merged) as one
    /// {type:"trace"} stderr event before each ctx.output(), then cleared.
    labels: Map<String, Value>,
}

impl<'a> Context<'a> {
    pub fn new(
        id: String,
        src: String,
        memory: &'a mut Memory,
        queue: &'a mut Vec<QueueItem>,
        stdout: &'a mut dyn Write,
        stderr: &'a mut dyn Write,
    ) -> Self {
        Self { id, src, memory, queue, stdout, stderr, labels: Map::new() }
    }

    /// Accumulate a label on this invocation's next output envelope.
    pub fn trace(&mut self, key: &str, value: Value) {
        self.labels.insert(key.to_string(), value);
    }

    /// Emit a stats event to stderr.
    pub fn stats(&mut self, data: Value) {
        if data.is_object() && !data.as_object().map(|o| o.is_empty()).unwrap_or(true) {
            envelope::write_stats(&data, &mut self.stderr);
        }
    }

    /// Emit data record to stdout.
    /// Flushes accumulated trace labels (merged) first, then the envelope.
    pub fn output(&mut self, v: Value, id: Option<&str>, src: Option<&str>) {
        let out_id = id.unwrap_or(&self.id).to_string();
        let out_src = src.unwrap_or(&self.src).to_string();
        let labels_value = Value::Object(std::mem::take(&mut self.labels));
        envelope::write_trace(&out_id, &out_src, &labels_value, &mut self.stderr);
        envelope::write_data(&v, &out_id, &out_src, &mut self.stdout);
    }

    /// Emit to internal named queue.
    pub fn emit(&mut self, queue_name: &str, v: Value, id: Option<&str>, src: Option<&str>) {
        let item_id = id.unwrap_or(&self.id).to_string();
        let item_src = src.unwrap_or(&self.src).to_string();
        self.queue.push((queue_name.to_string(), v, item_id, item_src));
    }

    /// Emit metadata record to stdout.
    pub fn meta(&mut self, v: Value) {
        envelope::write_meta(&v, &mut self.stdout);
    }

    /// Write structured log to stderr.
    pub fn log(&mut self, msg: &str, level: &str) {
        envelope::write_log(msg, level, &mut self.stderr);
    }

    /// Write error to stderr with original input preserved.
    pub fn error(&mut self, v: &Value, err: &str) {
        envelope::write_error(v, err, &self.id, &self.src, &mut self.stderr);
    }

    /// Hash a string. Returns 16-char hex.
    pub fn hash(&self, key: &str) -> String {
        envelope::hash_string(key)
    }

    /// Hash file content in chunks. Returns hex string or None on error.
    pub fn hash_file(&self, filepath: &str) -> Option<String> {
        envelope::hash_file(filepath, 65536)
    }
}
