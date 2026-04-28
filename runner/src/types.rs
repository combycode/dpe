//! Shared types for runner: pipeline, variant, stage definitions.
//!
//! Schema matches `dpe/schemas/pipeline.schema.json` and SPEC.md §4.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// A pipeline variant as loaded from a YAML/JSON file, BEFORE inheritance
/// resolution. `stages` may be partial if `extends` is set.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VariantFile {
    pub pipeline: String,
    pub variant: String,
    #[serde(default)]
    pub extends: Option<String>,
    #[serde(default)]
    pub overrides: BTreeMap<String, Value>,
    #[serde(default)]
    pub settings: Option<PipelineSettings>,
    #[serde(default)]
    pub stages: BTreeMap<String, Stage>,
}

/// A pipeline variant after inheritance resolution — fully self-contained.
#[derive(Debug, Clone, Serialize)]
pub struct ResolvedVariant {
    pub pipeline: String,
    pub variant: String,
    pub settings: PipelineSettings,
    pub stages: BTreeMap<String, Stage>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct PipelineSettings {
    #[serde(default)]
    pub trace_buffer: Option<TraceBufferSettings>,
    #[serde(default)]
    pub trace: Option<bool>,
    #[serde(default)]
    pub cache_default_mode: Option<CacheMode>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TraceBufferSettings {
    #[serde(default)]
    pub max_events: Option<u64>,
    #[serde(default)]
    pub flush_ms: Option<u64>,
    #[serde(default)]
    pub max_segment_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum CacheMode {
    #[default]
    Use,
    Refresh,
    Bypass,
    Off,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Stage {
    pub tool: String,
    #[serde(default)]
    pub settings: Option<Value>,
    #[serde(default)]
    pub settings_file: Option<String>,
    #[serde(default)]
    pub input: Option<Input>,
    #[serde(default = "default_replicas")]
    pub replicas: u32,
    #[serde(default = "default_replicas_routing")]
    pub replicas_routing: ReplicasRouting,
    #[serde(default = "default_true")]
    pub trace: bool,
    #[serde(default)]
    pub cache: Option<CacheMode>,
    #[serde(default = "default_on_error")]
    pub on_error: OnError,
    // Built-in specific
    #[serde(default)]
    pub routes: Option<BTreeMap<String, String>>,
    #[serde(default)]
    pub expression: Option<String>,
    #[serde(default)]
    pub on_false: Option<FilterOnFalse>,
    #[serde(default)]
    pub dedup: Option<DedupCfg>,
    #[serde(default)]
    pub group_by: Option<GroupByCfg>,
}

/// Settings block for the built-in `dedup` stage.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DedupCfg {
    /// Path expressions resolved per envelope; joined with '|' then hashed.
    /// Empty list → use canonical JSON of v as the key.
    #[serde(default)]
    pub key: Vec<String>,
    #[serde(default = "default_dedup_hash_algo")]
    pub hash_algo: DedupHashAlgo,
    /// Index file name suffix. If `path` is not set, the full path is
    /// `<session>/index-<index_name>.bin`. When `path` is set, `index_name`
    /// is used only for trace/log labels.
    pub index_name: String,
    /// Explicit index file path. Supports `$session/...` (default) or
    /// `$storage/...` for cross-session persistence. The runner's path
    /// resolver substitutes prefixes before the value reaches this struct.
    /// When None, falls back to `<session>/index-<index_name>.bin`.
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default = "default_true")]
    pub load_existing: bool,
    #[serde(default = "default_on_duplicate")]
    pub on_duplicate: OnDuplicate,
}

/// Settings block for the built-in `group-by` stage.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GroupByCfg {
    /// Path expression for grouping key (e.g. "v.day"). Values stringified.
    pub key: String,
    /// Path expression for the label of the sub-bucket this row contributes to
    /// (e.g. "v.source_file" or "v.table_kind").
    pub bucket_key_from: String,
    /// Optional path expression for the value to place under that sub-bucket.
    /// Defaults to the whole envelope `v` when absent.
    #[serde(default)]
    pub value_from: Option<String>,
    /// Absolute path (starting with "v.") where the merged object is placed
    /// on the emitted envelope. e.g. "v.buckets" → v.buckets = {A:{...}, B:{...}}.
    pub target: String,
    /// Trigger: emit this group once it has accumulated all listed labels.
    /// When set, overrides count_threshold.
    #[serde(default)]
    pub expected_sources: Option<Vec<String>>,
    /// Trigger: emit once N distinct labels accumulated.
    #[serde(default)]
    pub count_threshold: Option<usize>,
    /// On upstream EOF, emit remaining partial groups with v._partial = true.
    #[serde(default = "default_true")]
    pub emit_partial_on_eof: bool,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DedupHashAlgo {
    Xxh64,
    Xxh128,
    Blake2b,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum OnDuplicate {
    /// Silently skip the duplicate.
    Drop,
    /// Emit a trace event with `dedup:dropped` label; no envelope output.
    Trace,
    /// Emit a meta envelope `{t:"m", v:{kind:"dedup_drop", k, id, src}}` downstream.
    Meta,
    /// Append `{type:"error",...}` to `<session>/logs/<stage>_errors.log`.
    Error,
}

fn default_dedup_hash_algo() -> DedupHashAlgo { DedupHashAlgo::Xxh64 }
fn default_on_duplicate() -> OnDuplicate { OnDuplicate::Drop }

/// Input reference. Either a single upstream (stage name or `stage.channel` or `$input`)
/// or an array for fan-in.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Input {
    One(String),
    Many(Vec<String>),
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ReplicasRouting {
    RoundRobin,
    HashId,
    LeastBusy,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum OnError {
    Drop,
    Pass,
    Fail,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum FilterOnFalse {
    Drop,
    EmitMeta,
    EmitStderr,
}

fn default_replicas() -> u32 { 1 }
fn default_replicas_routing() -> ReplicasRouting { ReplicasRouting::RoundRobin }
fn default_true() -> bool { true }
fn default_on_error() -> OnError { OnError::Drop }
