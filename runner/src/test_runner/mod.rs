//! `dpe test` — per-stage isolated snapshot tests.
//!
//! Phase 1: single-case isolated spawn.
//! Phase 2: `--update` / `--update-if-missing` snapshot regeneration.
//! Phase 3: bulk runs (`dpe test <pipeline>` / `<pipeline>:<variant>`)
//! with skip-list (`toggle` / `gate` / `checkpoint` / `dedup`) and
//! `test_exclusive` filter.
//!
//! Per the proposal's core principle: a test case spawns ONE stage in
//! isolation, NOT the variant's DAG. seed.ndjson is piped to the tool's
//! stdin; stdout is split by `t` value into `data.ndjson` / `meta.ndjson`,
//! diffed against the `expected/` directory under the case folder.
//!
//! The on-disk layout this runner recognises:
//!
//! ```text
//! <pipeline>/tests/<variant>/<stage>/<case>/
//!   ├── test.yaml                  (optional; settings_override / env / compare)
//!   ├── input/seed.ndjson          (one envelope per line; piped to stdin)
//!   ├── expected/data.ndjson       (canonical expected stdout)
//!   └── .run/                      (per-test ephemerals; gitignored)
//!       ├── temp/                  ← $temp during this run
//!       ├── session/               ← $session
//!       ├── storage/               ← $storage
//!       └── output/                ← $output
//! ```
//!
//! Target syntax (parts separated by `:`):
//!   `<pipeline>`                                  — bulk pipeline (every variant × every stage × every case)
//!   `<pipeline>:<variant>`                        — bulk variant
//!   `<pipeline>:<variant>:<stage>`                — bulk stage (every case for that stage)
//!   `<pipeline>:<variant>:<stage>:<case>`         — single case
//!
//! Empty pipeline (leading `:`) means the current working directory.
//! Bulk modes (1- and 2-part targets) apply the **skip-list** and the
//! per-tool `test_exclusive` flag. Stage-explicit modes (3- and 4-part)
//! bypass both filters — the user has explicitly asked for that stage,
//! so they own any required environment setup.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

use crate::bom::strip_bom;
use crate::config::RunnerConfig;
use crate::env::SessionContext;
use crate::env_interp::{interpolate_in_value, MapEnv, ProcessEnv};
use crate::paths::PathResolver;
use crate::pipeline::{deep_merge_json, load_variant};
use crate::spawn::spawn;
use crate::tools::{self, Invocation};
use crate::types::{CacheMode, ResolvedVariant};

// Phase B-E modules — landed independently; Phase F wires them into
// `run_one_case`. Until then they're unused-but-compiled and tested
// in isolation.
pub mod channels;
pub mod compare;
pub mod fs_diff;
pub mod script;
pub mod builtin_driver;

/// Tools whose stages are silently skipped in bulk `dpe test` runs and
/// don't count against coverage. Curated; ships with the runner. The
/// rule is "control-flow plumbing isn't worth a snapshot test on its
/// own — the variants that USE these stages get tested as part of the
/// surrounding settings flow."
const SKIP_TOOLS: &[&str] = &["toggle", "gate", "checkpoint", "dedup"];

/// What the user asked for. `variant` / `stage` / `case` are filled in
/// left-to-right as the colon-separated target gains parts; `None` means
/// "all of them at this level".
///
///   `<pipeline>`                            → variant=stage=case=None
///   `<pipeline>:<variant>`                  → stage=case=None
///   `<pipeline>:<variant>:<stage>`          → case=None
///   `<pipeline>:<variant>:<stage>:<case>`   → all set
#[derive(Debug, Clone)]
pub struct TestTarget {
    pub pipeline_dir:  PathBuf,
    pub pipeline_name: String,
    pub variant:       Option<String>,
    pub stage:         Option<String>,
    pub case:          Option<String>,
}

impl TestTarget {
    /// True when the user explicitly named a stage. In that case the
    /// skip-list and `test_exclusive` filter are BYPASSED — the user
    /// has asked for this stage, the runner respects that.
    pub fn explicit_stage(&self) -> bool {
        self.stage.is_some()
    }
}

/// A fully-resolved single case — what the runner actually iterates.
#[derive(Debug, Clone)]
pub struct CaseRef {
    pub variant: String,
    pub stage:   String,
    pub case:    String,
}

/// Parsed `test.yaml`. All fields optional — a missing file or empty
/// file yields `TestSpec::default()` (= "inherit variant settings,
/// default compare rules"). The full schema is documented in
/// `docs/testing.md`.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct TestSpec {
    pub settings_override: Option<Value>,
    pub env:               Option<BTreeMap<String, String>>,
    /// Per-case cache override. Wins over the `Use` default; loses to a
    /// CLI-level `--cache` flag. Useful for stability tests that must
    /// hit the real tool every run (`cache: bypass`).
    pub cache:             Option<crate::types::CacheMode>,
    pub compare:           Option<CompareSpec>,
    pub assert:            Option<script::AssertYaml>,
    /// Wall-clock cap for the spawned tool (external path) or the
    /// in-process builtin task. Default `60_000` ms (preserves the
    /// previous hardcoded limit). Bumped per-case for long-running
    /// stages — e.g. live LLM calls in doc-converter that exceed 60s
    /// under normal API latency.
    pub timeout_ms:        Option<u64>,
    /// Multi-phase test cases share `.run/` state across phases (output,
    /// temp, storage, session). `.run/actual/` is wiped between phases;
    /// the rest persists so phase 2 can observe what phase 1 wrote
    /// (cache files, batch state, accumulated outputs).
    pub phases:            Option<Vec<PhaseSpec>>,
    /// Exit code(s) the spawned tool is allowed to return. Default `0`.
    /// Accept either a single `int` or a `[int, int, ...]` array — the
    /// latter is for tools whose contract permits multiple success codes
    /// (e.g. `0` for success, `1` for "validated as invalid input — test
    /// that the failure path emits the expected error envelope"). Any
    /// exit code OUTSIDE the allowed set fails the test with the tool's
    /// stderr as detail.
    pub expected_exit_code: Option<ExpectedExitCode>,
}

/// One or many allowed exit codes. Single-int and list-of-ints YAML
/// shapes are both accepted via untagged-enum deserialisation.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ExpectedExitCode {
    One(i32),
    Many(Vec<i32>),
}

impl ExpectedExitCode {
    pub fn allows(&self, code: i32) -> bool {
        match self {
            Self::One(n)   => *n == code,
            Self::Many(xs) => xs.contains(&code),
        }
    }

    /// Human-readable form for error messages, e.g. `0` or `[0, 1, 2]`.
    pub fn describe(&self) -> String {
        match self {
            Self::One(n)   => n.to_string(),
            Self::Many(xs) => {
                let inner = xs.iter().map(i32::to_string).collect::<Vec<_>>().join(", ");
                format!("[{inner}]")
            }
        }
    }
}

/// Compare engine spec. All fields optional.
///
/// `channels` (None = auto-detect from expected file presence; Some(list)
/// = exactly these channels must match). `global` is merged into every
/// per-channel block as the base layer (per-channel overrides scalars,
/// lists concatenate). `fs_check` / `fs_ignore` / `files` control the
/// filesystem tree comparison.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct CompareSpec {
    pub channels:  Option<Vec<String>>,
    pub global:    Option<compare::ChannelRulesYaml>,
    pub data:      Option<compare::ChannelRulesYaml>,
    pub meta:      Option<compare::ChannelRulesYaml>,
    pub errors:    Option<compare::ChannelRulesYaml>,
    pub logs:      Option<compare::ChannelRulesYaml>,
    pub trace:     Option<compare::ChannelRulesYaml>,
    pub stats:     Option<compare::ChannelRulesYaml>,
    pub input:     Option<compare::ChannelRulesYaml>,
    pub fs_check:  Option<Vec<String>>,
    pub fs_ignore: Option<Vec<String>>,
    pub files:     Option<Vec<fs_diff::FileOverrideYaml>>,
}

/// One phase of a multi-phase case. `name` is the only required field;
/// every other field overrides the case-level value when present.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhaseSpec {
    pub name:              String,
    #[serde(default)]
    pub settings_override: Option<Value>,
    #[serde(default)]
    pub env:               Option<BTreeMap<String, String>>,
    #[serde(default)]
    pub cache:             Option<crate::types::CacheMode>,
    #[serde(default)]
    pub compare:           Option<CompareSpec>,
    #[serde(default)]
    pub assert:            Option<script::AssertYaml>,
    /// Per-phase wall-clock cap. Overrides the case-level `timeout_ms`.
    /// Useful for cache-flow tests where one phase makes live API
    /// calls and another is a fast cache-hit verification.
    #[serde(default)]
    pub timeout_ms:        Option<u64>,
    /// Path to expected dir, relative to the case dir. Default:
    /// `expected/<phase.name>` for multi-phase cases.
    #[serde(default)]
    pub expected:          Option<String>,
    /// Per-phase override of the case-level `expected_exit_code`. Lets a
    /// multi-phase case test the "exit 0" success path AND the "exit 1
    /// invalid input" path without two separate cases.
    #[serde(default)]
    pub expected_exit_code: Option<ExpectedExitCode>,
}

/// Snapshot regeneration policy. `None` (default) runs the normal
/// PASS/FAIL diff path. `Always` rewrites `expected/` from the
/// captured actual on every run. `IfMissing` only writes when the
/// expected file doesn't already exist — safer for shared/CI use,
/// since it can't silently rewrite a snapshot a reviewer is about
/// to look at.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum UpdateMode {
    #[default]
    None,
    Always,
    IfMissing,
}

/// Why a case was skipped in bulk mode.
#[derive(Debug, Clone)]
pub enum SkipReason {
    /// Tool name is in the hard-coded `SKIP_TOOLS` list (control layer + dedup).
    SkipList(String),
    /// `meta.json` declared `test_exclusive: true` and we're in bulk mode.
    TestExclusive,
    /// `meta.json` declared `test_skipped: true` — pure I/O tool, no logic
    /// worth snapshot-testing; excluded from bulk runs and coverage denominator.
    TestSkipped,
    /// Case folder exists but the variant doesn't have that stage anymore
    /// (left over from a rename or deletion). Phase 7 (`dpe test-cleanup`)
    /// surfaces these as cleanup candidates; in bulk runs we just skip.
    StageNotInVariant,
}

impl SkipReason {
    fn label(&self) -> String {
        match self {
            SkipReason::SkipList(tool)    => format!("skip-list: {tool}"),
            SkipReason::TestExclusive     => "test_exclusive".to_string(),
            SkipReason::TestSkipped       => "test-skipped".to_string(),
            SkipReason::StageNotInVariant => "stage not in variant".to_string(),
        }
    }
}

/// Outcome of one test case.
#[derive(Debug, Clone)]
pub enum ResultKind {
    Pass,
    Fail { diff: String },
    Updated { wrote: Vec<PathBuf> },
    Skipped { reason: SkipReason },
    Error { msg: String },
}

/// Single-case result with timing.
#[derive(Debug, Clone)]
pub struct CaseResult {
    pub case: CaseRef,
    pub kind: ResultKind,
    pub duration_ms: u128,
}

/// Aggregate report over a bulk run (or a single-case run, in which
/// case `results` has length 1).
#[derive(Debug, Clone, Default)]
pub struct RunReport {
    pub results:    Vec<CaseResult>,
    pub passed:     usize,
    pub failed:     usize,
    pub updated:    usize,
    pub skipped:    usize,
    pub errored:    usize,
    pub total_ms:   u128,
}

impl RunReport {
    fn record(&mut self, r: CaseResult) {
        self.total_ms = self.total_ms.saturating_add(r.duration_ms);
        match &r.kind {
            ResultKind::Pass        => self.passed  += 1,
            ResultKind::Fail { .. } => self.failed  += 1,
            ResultKind::Updated{..} => self.updated += 1,
            ResultKind::Skipped{..} => self.skipped += 1,
            ResultKind::Error{..}   => self.errored += 1,
        }
        self.results.push(r);
    }
    /// Process exit code: 0 if no failures or errors, 1 if any FAIL,
    /// 2 if any ERROR (and no FAIL — invocation problems take priority
    /// over snapshot mismatches when both happen, so callers see error
    /// 2 first and can re-run after fixing the harness).
    pub fn exit_code(&self) -> i32 {
        if self.errored > 0 { 2 } else if self.failed > 0 { 1 } else { 0 }
    }
}

// ─── Public entry points ─────────────────────────────────────────────

/// Run every case the target selects. Single-case targets produce a
/// report with one entry; bulk targets enumerate everything under
/// `<pipeline>/tests/`. Skip-list + `test_exclusive` apply when the
/// target does NOT explicitly name a stage (Phase 3 rule).
pub async fn run_many(
    target: &TestTarget,
    config: &RunnerConfig,
    update: UpdateMode,
    cli_cache: Option<crate::types::CacheMode>,
) -> Result<RunReport> {
    let cases = discover(target)?;
    let bypass_filter = target.explicit_stage();

    // Cache loaded variants — same variant typically has many stages,
    // and load_variant parses YAML + resolves inheritance, so calling
    // it once per case wastes work.
    let mut variant_cache: HashMap<String, ResolvedVariant> = HashMap::new();
    let mut report = RunReport::default();

    for case in cases {
        let started = Instant::now();
        let variant = match variant_cache.get(&case.variant) {
            Some(v) => v.clone(),
            None => {
                let loaded = load_variant(&target.pipeline_dir, &target.pipeline_name, &case.variant);
                match loaded {
                    Ok(v) => {
                        variant_cache.insert(case.variant.clone(), v.clone());
                        v
                    }
                    Err(e) => {
                        report.record(CaseResult {
                            case: case.clone(),
                            kind: ResultKind::Error { msg: format!("load variant: {e}") },
                            duration_ms: started.elapsed().as_millis(),
                        });
                        continue;
                    }
                }
            }
        };

        // Stage absent? Skip in any mode (even explicit single-case),
        // but in single-case mode bubble it up as an Error so the user
        // notices the typo. Bulk mode stays silent — see SkipReason::
        // StageNotInVariant doc.
        let stage_def = variant.stages.get(&case.stage).cloned();
        if stage_def.is_none() {
            if bypass_filter {
                report.record(CaseResult {
                    case: case.clone(),
                    kind: ResultKind::Error {
                        msg: format!(
                            "stage '{}' not in variant '{}' (have: {})",
                            case.stage, case.variant,
                            variant.stages.keys().cloned().collect::<Vec<_>>().join(", ")
                        ),
                    },
                    duration_ms: started.elapsed().as_millis(),
                });
            } else {
                report.record(CaseResult {
                    case: case.clone(),
                    kind: ResultKind::Skipped { reason: SkipReason::StageNotInVariant },
                    duration_ms: started.elapsed().as_millis(),
                });
            }
            continue;
        }
        let stage_def = stage_def.unwrap();

        // Apply skip-list + test_skipped + test_exclusive in bulk modes only.
        if !bypass_filter {
            if SKIP_TOOLS.contains(&stage_def.tool.as_str()) {
                report.record(CaseResult {
                    case: case.clone(),
                    kind: ResultKind::Skipped {
                        reason: SkipReason::SkipList(stage_def.tool.clone()),
                    },
                    duration_ms: started.elapsed().as_millis(),
                });
                continue;
            }
            // Tool resolution can fail (tool not on disk yet); we
            // surface that as Error rather than silent skip so the
            // user knows their pipeline references a missing tool.
            match tools::resolve(&stage_def.tool, &target.pipeline_dir, config) {
                Ok(t) if t.meta.test_skipped => {
                    report.record(CaseResult {
                        case: case.clone(),
                        kind: ResultKind::Skipped { reason: SkipReason::TestSkipped },
                        duration_ms: started.elapsed().as_millis(),
                    });
                    continue;
                }
                Ok(t) if t.meta.test_exclusive => {
                    report.record(CaseResult {
                        case: case.clone(),
                        kind: ResultKind::Skipped { reason: SkipReason::TestExclusive },
                        duration_ms: started.elapsed().as_millis(),
                    });
                    continue;
                }
                Ok(_)  => {}
                Err(e) => {
                    report.record(CaseResult {
                        case: case.clone(),
                        kind: ResultKind::Error { msg: format!("resolve tool: {e}") },
                        duration_ms: started.elapsed().as_millis(),
                    });
                    continue;
                }
            }
        }

        // Run the case end-to-end.
        let outcome = run_one_case(
            &target.pipeline_dir, &target.pipeline_name,
            &case, &variant, config, update, cli_cache,
        ).await;
        let duration_ms = started.elapsed().as_millis();
        let kind = match outcome {
            Ok(k)  => k,
            Err(e) => ResultKind::Error { msg: format!("{e:#}") },
        };
        report.record(CaseResult { case, kind, duration_ms });
    }

    Ok(report)
}

/// Phase 1/2 single-case entry retained for back-compat / direct
/// callers. Internally this just calls `run_many` with a fully-
/// specified target. CLI cache override defaults to None — callers
/// using `run_one` directly inherit the `test.yaml` cache: field
/// (or the Use default).
pub async fn run_one(
    target: &TestTarget,
    config: &RunnerConfig,
    update: UpdateMode,
) -> Result<TestOutcome> {
    let report = run_many(target, config, update, None).await?;
    let r = report.results.into_iter().next()
        .ok_or_else(|| anyhow!("no case ran (target matched zero cases)"))?;
    Ok(match r.kind {
        ResultKind::Pass               => TestOutcome::Pass,
        ResultKind::Fail { diff }      => TestOutcome::Fail { diff },
        ResultKind::Updated { wrote }  => TestOutcome::Updated { wrote },
        ResultKind::Skipped { reason } => TestOutcome::Fail {
            diff: format!("skipped: {}\n", reason.label()),
        },
        ResultKind::Error { msg }      => return Err(anyhow!(msg)),
    })
}

/// Legacy single-case outcome (Phase 1/2 callers expect this shape).
#[derive(Debug)]
pub enum TestOutcome {
    Pass,
    Fail { diff: String },
    Updated { wrote: Vec<PathBuf> },
}

// ─── Discovery ───────────────────────────────────────────────────────

/// Walk `<pipeline>/tests/<variant>/<stage>/<case>/` and return every
/// case the target selects. Validation: only directories that contain
/// `input/seed.ndjson` count as cases. A case folder without seed gets
/// silently ignored (treated as "not a real test case yet").
pub fn discover(target: &TestTarget) -> Result<Vec<CaseRef>> {
    let tests_root = target.pipeline_dir.join("tests");
    if !tests_root.is_dir() {
        return Ok(vec![]);
    }
    let mut out: Vec<CaseRef> = Vec::new();
    let variants = list_subdirs(&tests_root)?;
    let target_variants: BTreeSet<&str> = match &target.variant {
        Some(v) => std::iter::once(v.as_str()).collect(),
        None    => variants.iter().map(String::as_str).collect(),
    };
    for variant in &variants {
        if !target_variants.contains(variant.as_str()) { continue; }
        let stages_dir = tests_root.join(variant);
        let stages = list_subdirs(&stages_dir)?;
        let target_stages: BTreeSet<&str> = match &target.stage {
            Some(s) => std::iter::once(s.as_str()).collect(),
            None    => stages.iter().map(String::as_str).collect(),
        };
        for stage in &stages {
            if !target_stages.contains(stage.as_str()) { continue; }
            let cases_dir = stages_dir.join(stage);
            let cases = list_subdirs(&cases_dir)?;
            let target_cases: BTreeSet<&str> = match &target.case {
                Some(c) => std::iter::once(c.as_str()).collect(),
                None    => cases.iter().map(String::as_str).collect(),
            };
            for case in &cases {
                if !target_cases.contains(case.as_str()) { continue; }
                let seed = cases_dir.join(case).join("input").join("seed.ndjson");
                if !seed.is_file() { continue; }
                out.push(CaseRef {
                    variant: variant.clone(),
                    stage:   stage.clone(),
                    case:    case.clone(),
                });
            }
        }
    }
    // Sorted for stable bulk-run output.
    out.sort_by(|a, b| {
        a.variant.cmp(&b.variant)
            .then_with(|| a.stage.cmp(&b.stage))
            .then_with(|| a.case.cmp(&b.case))
    });
    Ok(out)
}

fn list_subdirs(dir: &Path) -> Result<Vec<String>> {
    if !dir.is_dir() { return Ok(vec![]); }
    let mut names = Vec::new();
    for entry in std::fs::read_dir(dir).with_context(|| format!("read_dir {}", dir.display()))? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            if let Some(name) = entry.file_name().to_str() {
                names.push(name.to_string());
            }
        }
    }
    names.sort();
    Ok(names)
}

// ─── Per-case runner ─────────────────────────────────────────────────

async fn run_one_case(
    pipeline_dir:  &Path,
    pipeline_name: &str,
    case_ref:      &CaseRef,
    variant:       &ResolvedVariant,
    config:        &RunnerConfig,
    update:        UpdateMode,
    cli_cache:     Option<crate::types::CacheMode>,
) -> Result<ResultKind> {
    let case_dir = pipeline_dir
        .join("tests")
        .join(&case_ref.variant)
        .join(&case_ref.stage)
        .join(&case_ref.case);
    if !case_dir.is_dir() {
        return Err(anyhow!("case directory not found: {}", case_dir.display()));
    }

    let stage_def = variant.stages.get(&case_ref.stage)
        .ok_or_else(|| anyhow!(
            "stage '{}' not in variant '{}'", case_ref.stage, case_ref.variant
        ))?
        .clone();
    let test_spec = read_test_spec(&case_dir).context("reading test.yaml")?;

    let tool = tools::resolve(&stage_def.tool, pipeline_dir, config)
        .map_err(|e| anyhow!("resolving tool '{}': {}", stage_def.tool, e))?;
    // Builtins that have no logic worth testing (spread, toggle) carry
    // `test_skipped: true` on their synthetic meta. Bulk runs filter
    // them out in `run_many` before reaching here, but explicit-stage
    // targets (`dpe test pipe:variant:my-spread`) bypass that filter —
    // catch them here so the user sees a consistent SKIP rather than an
    // error or an attempt to spawn a non-existent binary.
    if let Invocation::Builtin(kind) = &tool.invocation {
        if matches!(kind, tools::BuiltinKind::Spread | tools::BuiltinKind::Toggle) {
            return Ok(ResultKind::Skipped { reason: SkipReason::TestSkipped });
        }
    }

    // Wipe `.run/` ONCE per case. State subdirs (output/temp/storage/
    // session) persist across phases — that's the multi-phase contract
    // (phase 2 sees what phase 1 wrote: cache, batch state, files).
    // Only `.run/actual/` is wiped between phases (inside run_phase).
    let run_dir = case_dir.join(".run");
    if run_dir.exists() {
        std::fs::remove_dir_all(&run_dir)
            .with_context(|| format!("wipe {} before test run", run_dir.display()))?;
    }
    for p in [
        run_dir.join("temp"),
        run_dir.join("session"),
        run_dir.join("storage"),
        run_dir.join("output"),
        run_dir.join("actual"),
    ] {
        std::fs::create_dir_all(&p)
            .with_context(|| format!("mkdir {}", p.display()))?;
    }
    let input_dir = case_dir.join("input");
    let seed_file = input_dir.join("seed.ndjson");
    if !seed_file.is_file() {
        return Err(anyhow!("missing input/seed.ndjson at {}", seed_file.display()));
    }

    // Build phase plan. No `phases:` block → single implicit phase
    // named "" with case-level everything and expected dir at
    // `expected/`. Multi-phase → one entry per `phases[]`.
    let phase_plans = build_phase_plan(&test_spec, &case_dir);

    // Per-phase outcomes accumulate; the final ResultKind aggregates.
    let mut phase_reports: Vec<PhaseReport> = Vec::with_capacity(phase_plans.len());
    let mut updated_files: Vec<PathBuf> = Vec::new();

    for plan in &phase_plans {
        let report = run_phase(
            RunPhaseInputs {
                pipeline_dir,
                pipeline_name,
                case_ref,
                case_dir: &case_dir,
                run_dir:  &run_dir,
                input_dir: &input_dir,
                seed_file: &seed_file,
                tool:      &tool,
                stage_def: &stage_def,
                test_spec: &test_spec,
                plan,
                cli_cache,
                update,
                wrote: &mut updated_files,
            }
        ).await;
        phase_reports.push(report);
    }

    // Aggregate. Update mode wins if anything was written. Otherwise
    // any phase failure → Fail; any phase error → Error; else Pass.
    if !updated_files.is_empty() && matches!(update, UpdateMode::Always | UpdateMode::IfMissing) {
        return Ok(ResultKind::Updated { wrote: updated_files });
    }
    let mut had_error = None;
    let mut diff_text = String::new();
    let mut had_failure = false;
    for r in &phase_reports {
        if let Some(msg) = &r.error {
            if had_error.is_none() { had_error = Some(msg.clone()); }
        }
        if !r.passed && r.error.is_none() {
            had_failure = true;
        }
        let detail = r.detail_text();
        if !detail.is_empty() {
            if !diff_text.is_empty() { diff_text.push('\n'); }
            if phase_plans.len() > 1 || !r.name.is_empty() {
                diff_text.push_str(&format!("── phase {:?} ──\n", r.name));
            }
            diff_text.push_str(&detail);
        }
    }
    if let Some(msg) = had_error {
        return Ok(ResultKind::Error { msg });
    }
    if had_failure {
        return Ok(ResultKind::Fail { diff: diff_text });
    }
    Ok(ResultKind::Pass)
}

// ─── Phase planning + execution (Phase F) ────────────────────────────

/// One concrete phase to run. For single-phase cases the plan list
/// has one entry with name="" and `expected_dir = case_dir/expected`.
struct PhasePlan<'a> {
    /// Phase name (empty string for the implicit single phase).
    name: String,
    /// Reference to the parsed PhaseSpec (None for single-phase fallback).
    phase: Option<&'a PhaseSpec>,
    /// Absolute path to the per-phase expected dir.
    expected_dir: PathBuf,
}

fn build_phase_plan<'a>(spec: &'a TestSpec, case_dir: &Path) -> Vec<PhasePlan<'a>> {
    match spec.phases.as_ref() {
        None => vec![PhasePlan {
            name: String::new(),
            phase: None,
            expected_dir: case_dir.join("expected"),
        }],
        Some(list) => list.iter().map(|p| {
            let expected_rel = p.expected.clone()
                .unwrap_or_else(|| format!("expected/{}", p.name));
            PhasePlan {
                name: p.name.clone(),
                phase: Some(p),
                expected_dir: case_dir.join(&expected_rel),
            }
        }).collect(),
    }
}

/// Per-phase outcome — the four step results plus a single
/// `error` slot for infrastructure failures (spawn / non-zero exit /
/// timeout). Each `Some` step that didn't pass contributes failure
/// detail to `detail_text`.
struct PhaseReport {
    name: String,
    passed: bool,
    error: Option<String>,                  // infrastructure error (errored, not failed)
    step1: Vec<channels::ChannelMismatch>,  // shape mismatches
    step2: Vec<fs_diff::FileFailure>,       // fs tree failures
    step3: BTreeMap<channels::ChannelKey, compare::ChannelDiffReport>,
    step4: Option<script::AssertOutcome>,
}

impl PhaseReport {
    fn detail_text(&self) -> String {
        let mut out = String::new();
        if let Some(e) = &self.error {
            out.push_str(&format!("ERROR: {e}\n"));
            return out;
        }
        if !self.step1.is_empty() {
            out.push_str("Step 1 — channel shape:\n");
            for m in &self.step1 {
                out.push_str(&format!("  • {}\n", m.message()));
            }
        }
        if !self.step2.is_empty() {
            out.push_str("Step 2 — filesystem tree:\n");
            for f in &self.step2 {
                out.push_str(&format!("  • {}: ", f.path));
                match &f.kind {
                    fs_diff::FileFailureKind::MissingInActual =>
                        out.push_str("missing in actual (regression)\n"),
                    fs_diff::FileFailureKind::UnexpectedInActual =>
                        out.push_str("unexpected in actual\n"),
                    fs_diff::FileFailureKind::ModeMismatch { mode, detail } =>
                        out.push_str(&format!("[{mode}] {detail}\n")),
                    fs_diff::FileFailureKind::Io(s) =>
                        out.push_str(&format!("io: {s}\n")),
                }
            }
        }
        let mut step3_failed: Vec<&channels::ChannelKey> = Vec::new();
        for (k, r) in &self.step3 {
            if !r.passed() { step3_failed.push(k); }
        }
        if !step3_failed.is_empty() {
            out.push_str("Step 3 — channel diff:\n");
            for k in step3_failed {
                let r = self.step3.get(k).unwrap();
                out.push_str(&format!("  channel '{}':\n", k.as_str()));
                for mf in &r.matcher_failures {
                    out.push_str(&format!("    line {}: {}\n", mf.line_idx + 1, mf.message));
                }
                if let Some(d) = &r.diff_unified {
                    for line in d.lines() {
                        out.push_str("    ");
                        out.push_str(line);
                        out.push('\n');
                    }
                }
            }
        }
        match &self.step4 {
            Some(script::AssertOutcome::Failed { stderr, exit_code }) => {
                out.push_str(&format!(
                    "Step 4 — assert script (exit {exit_code}):\n"
                ));
                for line in stderr.lines() {
                    out.push_str("  ");
                    out.push_str(line);
                    out.push('\n');
                }
            }
            Some(script::AssertOutcome::Errored { reason, stderr }) => {
                out.push_str(&format!("Step 4 — assert script ERRORED: {reason}\n"));
                if let Some(s) = stderr {
                    for line in s.lines() {
                        out.push_str("  ");
                        out.push_str(line);
                        out.push('\n');
                    }
                }
            }
            _ => {}
        }
        out
    }
}

#[allow(clippy::too_many_arguments)]
struct RunPhaseInputs<'a> {
    pipeline_dir:  &'a Path,
    pipeline_name: &'a str,
    case_ref:      &'a CaseRef,
    case_dir:      &'a Path,
    run_dir:       &'a Path,
    input_dir:     &'a Path,
    seed_file:     &'a Path,
    tool:          &'a tools::ResolvedTool,
    stage_def:     &'a crate::types::Stage,
    test_spec:     &'a TestSpec,
    plan:          &'a PhasePlan<'a>,
    cli_cache:     Option<crate::types::CacheMode>,
    update:        UpdateMode,
    wrote:         &'a mut Vec<PathBuf>,
}

async fn run_phase(inp: RunPhaseInputs<'_>) -> PhaseReport {
    let mut report = PhaseReport {
        name: inp.plan.name.clone(),
        passed: false,
        error: None,
        step1: Vec::new(),
        step2: Vec::new(),
        step3: BTreeMap::new(),
        step4: None,
    };

    // ── Wipe .run/actual/ between phases (state subdirs persist) ──
    let actual_dir = inp.run_dir.join("actual");
    if actual_dir.exists() {
        if let Err(e) = std::fs::remove_dir_all(&actual_dir) {
            report.error = Some(format!("wipe .run/actual/: {e}"));
            return report;
        }
    }
    if let Err(e) = std::fs::create_dir_all(&actual_dir) {
        report.error = Some(format!("mkdir .run/actual/: {e}"));
        return report;
    }

    // ── Compose effective per-phase config ────────────────────────
    let phase = inp.plan.phase;

    // settings_override: case ⊎ phase (deep-merge, phase wins)
    let case_settings = inp.test_spec.settings_override.clone()
        .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
    let phase_settings = phase
        .and_then(|p| p.settings_override.clone())
        .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
    let merged_override = deep_merge_json(case_settings, phase_settings);

    // env: case ⊎ phase (phase wins on key collision)
    let mut effective_env: BTreeMap<String, String> = BTreeMap::new();
    if let Some(e) = &inp.test_spec.env {
        for (k, v) in e { effective_env.insert(k.clone(), v.clone()); }
    }
    if let Some(p) = phase {
        if let Some(e) = &p.env {
            for (k, v) in e { effective_env.insert(k.clone(), v.clone()); }
        }
    }

    // cache: CLI > phase > case > Use
    let effective_cache = inp.cli_cache
        .or_else(|| phase.and_then(|p| p.cache))
        .or(inp.test_spec.cache)
        .unwrap_or(CacheMode::Use);

    // assert: phase wins, else case
    let effective_assert: Option<&script::AssertYaml> = phase
        .and_then(|p| p.assert.as_ref())
        .or(inp.test_spec.assert.as_ref());

    // timeout: phase wins, else case, else 60_000ms default. Applied
    // to both spawn and (future) builtin paths — long-running stages
    // (live LLM calls) need a per-case override.
    let effective_timeout_ms: u64 = phase
        .and_then(|p| p.timeout_ms)
        .or(inp.test_spec.timeout_ms)
        .unwrap_or(60_000);

    // settings: stage_def.settings ⊎ merged_override → env interp → path resolve
    let mut effective = inp.stage_def.settings.clone()
        .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
    effective = deep_merge_json(effective, merged_override);

    let interp_result = if effective_env.is_empty() {
        interpolate_in_value(&effective, &ProcessEnv)
    } else {
        let mut env_map: BTreeMap<String, String> = BTreeMap::new();
        for (k, v) in std::env::vars() { env_map.insert(k, v); }
        for (k, v) in &effective_env { env_map.insert(k.clone(), v.clone()); }
        interpolate_in_value(&effective, &MapEnv(env_map))
    };
    effective = match interp_result {
        Ok(v)  => v,
        Err(e) => {
            report.error = Some(format!("env interpolation in settings: {e}"));
            return report;
        }
    };
    let resolver = PathResolver::default()
        .with("input",    inp.input_dir)
        .with("output",   inp.run_dir.join("output"))
        .with("temp",     inp.run_dir.join("temp"))
        .with("session",  inp.run_dir.join("session"))
        .with("storage",  inp.run_dir.join("storage"))
        .with("configs",  inp.pipeline_dir.join("configs"))
        .with("pipeline", inp.pipeline_dir);
    let resolved_settings = match resolver.resolve_in_value(&effective) {
        Ok(v)  => v,
        Err(e) => {
            report.error = Some(format!("resolving $-prefixes: {e}"));
            return report;
        }
    };

    // ── Spawn tool, capture stdout/stderr, write channel files ────
    let session_id = if inp.plan.name.is_empty() {
        format!("test-{}-{}", inp.case_ref.stage, inp.case_ref.case)
    } else {
        format!("test-{}-{}-{}", inp.case_ref.stage, inp.case_ref.case, inp.plan.name)
    };
    let session_ctx = SessionContext {
        pipeline_dir:     inp.pipeline_dir.to_path_buf(),
        pipeline_name:    inp.pipeline_name.to_string(),
        variant:          inp.case_ref.variant.clone(),
        session_id,
        input:            inp.input_dir.to_path_buf(),
        output:           inp.run_dir.join("output"),
        cache_mode:       effective_cache,
        temp_override:    Some(inp.run_dir.join("temp")),
        storage_override: Some(inp.run_dir.join("storage")),
    };
    if let Err(e) = std::fs::create_dir_all(session_ctx.session_dir()) {
        report.error = Some(format!("mkdir {}: {e}", session_ctx.session_dir().display()));
        return report;
    }

    let seed_bytes = match std::fs::read(inp.seed_file) {
        Ok(b)  => b,
        Err(e) => { report.error = Some(format!("read seed.ndjson: {e}")); return report; }
    };

    let (stdout_buf, stderr_buf) = match &inp.tool.invocation {
        // ── Builtin path: run the in-process task driver ─────────
        Invocation::Builtin(kind) => {
            // Builtins read their config from the structured stage_def
            // fields (`expression`, `routes`, `dedup`, `group_by`),
            // not from the JSON `settings` blob. We still need
            // env_interp + path-resolve applied to those fields so
            // tests can use `${VAR}` and `$input` inside expressions
            // — same surface area `dpe run` exposes.
            let env_lookup = if effective_env.is_empty() {
                None
            } else {
                let mut env_map: BTreeMap<String, String> = BTreeMap::new();
                for (k, v) in std::env::vars() { env_map.insert(k, v); }
                for (k, v) in &effective_env { env_map.insert(k.clone(), v.clone()); }
                Some(env_map)
            };
            let interp_result: Result<crate::types::Stage> = if let Some(map) = env_lookup {
                crate::dag::plan::interpolate_stage_config(
                    &inp.case_ref.stage, inp.stage_def, &MapEnv(map),
                ).map_err(|e| anyhow!("env interp on builtin stage: {e}"))
            } else {
                crate::dag::plan::interpolate_stage_config(
                    &inp.case_ref.stage, inp.stage_def, &ProcessEnv,
                ).map_err(|e| anyhow!("env interp on builtin stage: {e}"))
            };
            let interpolated = match interp_result {
                Ok(s) => s,
                Err(e) => { report.error = Some(e.to_string()); return report; }
            };
            let resolved = match crate::dag::plan::resolve_stage_expressions(
                &interpolated, &resolver,
            ) {
                Ok(s) => s,
                Err(e) => {
                    report.error = Some(format!("resolve $-prefixes on builtin stage: {e}"));
                    return report;
                }
            };
            // Settings field unused for filter/route/group_by/dedup but
            // some configs land there for forward-compat — keep the
            // already-resolved settings in case a future builtin adds
            // a settings shape (parity with run path).
            let _ = &resolved_settings;
            match builtin_driver::run_builtin_test(
                *kind, &resolved, &inp.case_ref.stage,
                &seed_bytes, &session_ctx.session_dir(),
            ).await {
                Ok(cap) => (cap.stdout, cap.stderr),
                Err(e) => {
                    report.error = Some(format!("builtin '{}': {e}", inp.stage_def.tool));
                    return report;
                }
            }
        }
        // ── External tool path: spawn child process ─────────────
        _ => {
            let env_for_spawn = if effective_env.is_empty() { None } else { Some(&effective_env) };
            let spawned = match spawn(
                inp.tool, &resolved_settings, &session_ctx, &inp.case_ref.stage, 0,
                Some(effective_cache), env_for_spawn,
            ) {
                Ok(s)  => s,
                Err(e) => { report.error = Some(format!("spawn: {e}")); return report; }
            };
            let mut child = spawned.child;
            let mut child_stdin  = match spawned.stdin {
                Some(s) => s,
                None    => { report.error = Some("missing child stdin".into()); return report; }
            };
            let child_stdout = match spawned.stdout {
                Some(s) => s,
                None    => { report.error = Some("missing child stdout".into()); return report; }
            };
            let child_stderr = match spawned.stderr {
                Some(s) => s,
                None    => { report.error = Some("missing child stderr".into()); return report; }
            };
            let seed_for_stdin = seed_bytes.clone();
            let stdin_task = tokio::spawn(async move {
                let _ = child_stdin.write_all(&seed_for_stdin).await;
                let _ = child_stdin.shutdown().await;
                drop(child_stdin);
            });
            let stdout_task = tokio::spawn(drain_to_vec(child_stdout));
            let stderr_task = tokio::spawn(drain_to_vec(child_stderr));
            let exit = match timeout(Duration::from_millis(effective_timeout_ms), child.wait()).await {
                Ok(Ok(s))  => s,
                Ok(Err(e)) => { report.error = Some(format!("waiting on tool: {e}")); return report; }
                Err(_)     => {
                    report.error = Some(format!(
                        "test exceeded {}ms timeout (set test.yaml `timeout_ms:` to raise)",
                        effective_timeout_ms,
                    ));
                    return report;
                }
            };
            let _ = stdin_task.await;
            let so = stdout_task.await.unwrap_or_default();
            let se = stderr_task.await.unwrap_or_default();
            // Exit-code policy: phase override beats case-level. Default
            // is "0 only" — historical behaviour. To assert a negative
            // path (tool must reject malformed input with code 1), set
            // `expected_exit_code: 1` or `expected_exit_code: [0, 1]`.
            let allowed = phase.and_then(|p| p.expected_exit_code.as_ref())
                .or(inp.test_spec.expected_exit_code.as_ref());
            let actual_code = exit.code().unwrap_or(-1);
            let ok = match &allowed {
                Some(spec) => spec.allows(actual_code),
                None       => exit.success(),
            };
            if !ok {
                let err_text = String::from_utf8_lossy(&se).to_string();
                let wanted = allowed.map(|s| s.describe())
                    .unwrap_or_else(|| "0".to_string());
                report.error = Some(format!(
                    "tool exit code {actual_code} not in expected {wanted}: {err_text}",
                ));
                return report;
            }
            (so, se)
        }
    };

    let stdout_anomalies = match write_stdout_channels(&stdout_buf, &actual_dir) {
        Ok(a)  => a,
        Err(e) => { report.error = Some(format!("split stdout: {e}")); return report; }
    };
    let stderr_anomalies = match write_stderr_channels(&stderr_buf, &actual_dir) {
        Ok(a)  => a,
        Err(e) => { report.error = Some(format!("split stderr: {e}")); return report; }
    };
    // Stop the run if the tool emitted a line whose `t` / `type` is set
    // but holds an unknown value (typo of a known channel key). Silent
    // drops here hide real tool bugs; surfacing them keeps the test
    // honest. We report up to 3 examples; the rest are summarised.
    let all_anom: Vec<&ClassifyAnomaly> = stdout_anomalies.iter()
        .chain(stderr_anomalies.iter()).collect();
    if !all_anom.is_empty() {
        let mut msg = format!(
            "{} line(s) with unrecognised discriminator value:",
            all_anom.len(),
        );
        for a in all_anom.iter().take(3) {
            msg.push_str(&format!(
                "\n  {}={:?}: {}",
                a.discriminator, a.bad_value, a.line_preview,
            ));
        }
        if all_anom.len() > 3 {
            msg.push_str(&format!("\n  ... and {} more", all_anom.len() - 3));
        }
        report.error = Some(msg);
        return report;
    }

    // ── --update mode: write canonicalised actual channels back ───
    if let Some(written) = maybe_update_expected(
        inp.update, &inp.plan.expected_dir, &actual_dir,
        inp.test_spec.compare.as_ref().or(phase.and_then(|p| p.compare.as_ref())),
        phase.and_then(|p| p.compare.as_ref()),
        inp.case_dir, inp.run_dir,
    ) {
        match written {
            Ok(paths) => {
                inp.wrote.extend(paths);
                report.passed = true;
                return report;
            }
            Err(e) => {
                report.error = Some(format!("--update write: {e}"));
                return report;
            }
        }
    }

    // ── Compose effective compare config ──────────────────────────
    let tokens = compare::ScrubTokens {
        case_dir: Some(inp.case_dir.to_string_lossy().to_string()),
        run_dir:  Some(inp.run_dir.to_string_lossy().to_string()),
        cwd:      std::env::current_dir().ok().map(|p| p.to_string_lossy().to_string()),
        home:     std::env::var("HOME").or_else(|_| std::env::var("USERPROFILE")).ok(),
    };
    let effective_compare = match compose_compare(
        inp.test_spec.compare.as_ref(),
        phase.and_then(|p| p.compare.as_ref()),
        inp.case_dir,
        &tokens,
    ) {
        Ok(c)  => c,
        Err(e) => {
            report.error = Some(format!("compose compare rules: {e}"));
            return report;
        }
    };

    // ── Step 1: channel shape ─────────────────────────────────────
    let declared_keys: Option<Vec<channels::ChannelKey>> = effective_compare.channels.clone();
    let declared_slice: Option<&[channels::ChannelKey]> = declared_keys.as_deref();
    report.step1 = channels::check_channel_shape(&channels::ShapeCheckInputs {
        expected_dir: &inp.plan.expected_dir,
        actual_dir:   &actual_dir,
        declared:     declared_slice,
    });

    // ── Step 2: fs tree compare ───────────────────────────────────
    match fs_diff::diff_tree(
        &inp.plan.expected_dir,
        inp.run_dir,
        effective_compare.fs_check.as_deref(),
        &effective_compare.fs_ignore,
        &effective_compare.file_overrides,
        &effective_compare.global_scrub_paths,
    ) {
        Ok(failures) => report.step2 = failures,
        Err(e) => {
            report.error = Some(format!("fs tree compare: {e}"));
            return report;
        }
    }

    // ── Step 3: per-channel envelope diff ─────────────────────────
    let channels_to_diff: Vec<channels::ChannelKey> = match &effective_compare.channels {
        Some(list) => list.clone(),
        None => channels::ChannelKey::all().iter()
            .copied()
            .filter(|k| inp.plan.expected_dir.join(format!("{}.ndjson", k.as_str())).is_file())
            .collect(),
    };
    for k in channels_to_diff {
        let exp_path = inp.plan.expected_dir.join(format!("{}.ndjson", k.as_str()));
        let act_path = actual_dir.join(format!("{}.ndjson", k.as_str()));
        let exp_body = std::fs::read_to_string(&exp_path).unwrap_or_default();
        let act_body = std::fs::read_to_string(&act_path).unwrap_or_default();
        let rules = match effective_compare.per_channel.get(&k) {
            Some(r) => r,
            None    => continue,  // shouldn't happen — compose_compare populates every declared k
        };
        match compare::diff_channel(&exp_body, &act_body, rules) {
            Ok(rep) => { report.step3.insert(k, rep); }
            Err(e) => {
                report.error = Some(format!("diff channel {}: {e}", k.as_str()));
                return report;
            }
        }
    }

    // ── Did declarative compare pass? ─────────────────────────────
    let declarative_passed = report.step1.is_empty()
        && report.step2.is_empty()
        && report.step3.values().all(|r| r.passed());

    // ── Step 4: assert script (only if declarative passed) ────────
    if declarative_passed {
        if let Some(yaml) = effective_assert {
            let cfg = match script::build_cfg(
                yaml,
                inp.case_dir,
                inp.run_dir,
                &inp.plan.expected_dir,
                &inp.case_ref.variant,
                &inp.case_ref.stage,
                &inp.case_ref.case,
                if inp.plan.name.is_empty() { None } else { Some(inp.plan.name.as_str()) },
            ) {
                Ok(c)  => c,
                Err(e) => {
                    report.error = Some(format!("assert: {e}"));
                    return report;
                }
            };
            let outcome = script::run(&cfg).await;
            report.step4 = Some(outcome);
        }
    }

    // ── Aggregate phase pass ──────────────────────────────────────
    let step4_ok = match &report.step4 {
        None => true,
        Some(script::AssertOutcome::Pass) => true,
        Some(script::AssertOutcome::Failed { .. }) => false,
        Some(script::AssertOutcome::Errored { reason, stderr }) => {
            // Errored at step 4 = infrastructure problem; surface as
            // ResultKind::Error rather than Fail.
            report.error = Some(format!(
                "assert script: {reason}{}",
                stderr.as_deref().map(|s| format!("\n{s}")).unwrap_or_default()
            ));
            return report;
        }
    };
    report.passed = declarative_passed && step4_ok;
    report
}

// ─── Compare composition ────────────────────────────────────────────

struct ResolvedCompare {
    /// Channels declared in `compare.channels`, parsed. None = auto-detect.
    channels: Option<Vec<channels::ChannelKey>>,
    /// Per-channel effective rules. Populated for every channel that
    /// participates in step 3 (declared channels OR auto-detected).
    per_channel: BTreeMap<channels::ChannelKey, compare::EffectiveChannelRules>,
    /// Compiled scrub_paths from `global` only — applied to fs tree
    /// text-mode comparisons. (Per-channel scrub_paths apply only to
    /// the corresponding channel's envelope diff.)
    global_scrub_paths: Vec<(regex::Regex, String)>,
    fs_check:       Option<Vec<String>>,
    fs_ignore:      Vec<String>,
    file_overrides: Vec<fs_diff::FileOverride>,
}

fn compose_compare(
    case_compare:  Option<&CompareSpec>,
    phase_compare: Option<&CompareSpec>,
    case_dir:      &Path,
    tokens:        &compare::ScrubTokens,
) -> Result<ResolvedCompare> {
    // channels: phase wins, else case, else None (auto-detect)
    let chan_strings = phase_compare.and_then(|c| c.channels.as_ref())
        .or_else(|| case_compare.and_then(|c| c.channels.as_ref()));
    let channels: Option<Vec<channels::ChannelKey>> = match chan_strings {
        None => None,
        Some(list) => {
            let mut out = Vec::with_capacity(list.len());
            for s in list {
                let k = channels::ChannelKey::parse(s)
                    .ok_or_else(|| anyhow!("compare.channels: unknown channel '{}'", s))?;
                out.push(k);
            }
            Some(out)
        }
    };

    // global rules — phase global merged on top of case global. Lists
    // concat, scalars phase-overrides-case (use compose_rules pattern
    // but applied to `global` slot specifically).
    // Simpler: build a synthesised "global" YAML by concatenating
    // case.global + phase.global as if they were two layers.
    let case_global = case_compare.and_then(|c| c.global.as_ref());
    let phase_global = phase_compare.and_then(|c| c.global.as_ref());

    // Per-channel: for each channel in {data, meta, errors, logs, trace, stats},
    // build EffectiveChannelRules = compose([case.global, phase.global, case.<chan>, phase.<chan>]).
    // compose_rules takes (global, channel) but it iterates over
    // [global, channel] in order. We have FOUR layers. Trick: call
    // compose_rules in two passes — first global merge, then full.
    // Simpler: extend compose_rules to N layers via a helper.
    fn pick_chan(
        c: Option<&CompareSpec>,
        k: channels::ChannelKey,
    ) -> Option<&compare::ChannelRulesYaml> {
        c.and_then(|c| match k {
            channels::ChannelKey::Data   => c.data.as_ref(),
            channels::ChannelKey::Meta   => c.meta.as_ref(),
            channels::ChannelKey::Errors => c.errors.as_ref(),
            channels::ChannelKey::Logs   => c.logs.as_ref(),
            channels::ChannelKey::Trace  => c.trace.as_ref(),
            channels::ChannelKey::Stats  => c.stats.as_ref(),
            channels::ChannelKey::Input  => c.input.as_ref(),
        })
    }

    let mut per_channel: BTreeMap<channels::ChannelKey, compare::EffectiveChannelRules> = BTreeMap::new();
    let keys_to_compose: Vec<channels::ChannelKey> = match &channels {
        Some(list) => list.clone(),
        // auto-detect: compose for all channels, the orchestrator
        // filters by file presence at step-3 time.
        None => channels::ChannelKey::all().to_vec(),
    };
    for k in keys_to_compose {
        let layers: Vec<&compare::ChannelRulesYaml> = [
            case_global, phase_global,
            pick_chan(case_compare, k), pick_chan(phase_compare, k),
        ].iter().filter_map(|x| *x).collect();
        let rules = compose_n_layers(&layers, tokens)
            .with_context(|| format!("compose rules for channel '{}'", k.as_str()))?;
        per_channel.insert(k, rules);
    }

    // Global scrub_paths only — compiled via compose_rules so token
    // expansion applies. Lists concat case→phase.
    let global_layers: Vec<&compare::ChannelRulesYaml> = [case_global, phase_global]
        .iter().filter_map(|x| *x).collect();
    let global_rules = compose_n_layers(&global_layers, tokens)
        .context("compose global scrub")?;
    let global_scrub_paths = global_rules.scrub_paths;

    // fs_check / fs_ignore: phase wins for fs_check (replace), fs_ignore concats
    let fs_check = phase_compare.and_then(|c| c.fs_check.clone())
        .or_else(|| case_compare.and_then(|c| c.fs_check.clone()));
    let mut fs_ignore: Vec<String> = Vec::new();
    if let Some(c) = case_compare { if let Some(v) = &c.fs_ignore { fs_ignore.extend(v.iter().cloned()); } }
    if let Some(c) = phase_compare { if let Some(v) = &c.fs_ignore { fs_ignore.extend(v.iter().cloned()); } }

    // files: concat (case overrides come first; phase appended; later
    // entries win when paths collide via fs_diff::diff_tree's `find`).
    // We swap the order — phase first, case second — so phase entries
    // win when paths collide.
    let mut files_yaml: Vec<fs_diff::FileOverrideYaml> = Vec::new();
    if let Some(c) = phase_compare { if let Some(v) = &c.files { files_yaml.extend(v.iter().cloned()); } }
    if let Some(c) = case_compare  { if let Some(v) = &c.files { files_yaml.extend(v.iter().cloned()); } }
    let file_overrides = fs_diff::parse_overrides(&files_yaml, case_dir)
        .context("parse file overrides")?;

    Ok(ResolvedCompare {
        channels, per_channel, global_scrub_paths, fs_check, fs_ignore, file_overrides,
    })
}

/// Compose channel-rule layers in order. Thin wrapper that delegates to
/// `compare::compose_rules_layered` so the merge semantics live in one
/// place (lists concatenate, scalars last-write-wins, regex compilation
/// reports the originating layer).
fn compose_n_layers(
    layers: &[&compare::ChannelRulesYaml],
    tokens: &compare::ScrubTokens,
) -> Result<compare::EffectiveChannelRules> {
    compare::compose_rules_layered(layers, tokens)
}

// ─── --update mode: write canonicalised actuals back to expected ───

/// If `update` is Always (or IfMissing and channel file is absent),
/// canonicalise each non-empty actual channel and write to the
/// corresponding `expected/` path. Returns Some(Result<paths>) if
/// update is active, None otherwise (caller continues to compare).
fn maybe_update_expected(
    update: UpdateMode,
    expected_dir: &Path,
    actual_dir:   &Path,
    case_compare: Option<&CompareSpec>,
    phase_compare: Option<&CompareSpec>,
    case_dir: &Path,
    run_dir:  &Path,
) -> Option<Result<Vec<PathBuf>>> {
    if matches!(update, UpdateMode::None) { return None; }
    let tokens = compare::ScrubTokens {
        case_dir: Some(case_dir.to_string_lossy().to_string()),
        run_dir:  Some(run_dir.to_string_lossy().to_string()),
        cwd:      std::env::current_dir().ok().map(|p| p.to_string_lossy().to_string()),
        home:     std::env::var("HOME").or_else(|_| std::env::var("USERPROFILE")).ok(),
    };
    let resolved = match compose_compare(case_compare, phase_compare, case_dir, &tokens) {
        Ok(r) => r, Err(e) => return Some(Err(e)),
    };
    let mut wrote = Vec::new();
    if let Err(e) = std::fs::create_dir_all(expected_dir) {
        return Some(Err(anyhow!("mkdir {}: {e}", expected_dir.display())));
    }
    // Channel scope for --update: never auto-snapshot framework noise
    // (logs/trace/stats) — those are non-deterministic by nature
    // (timestamps, durations) and would defeat the test on the second
    // run. User opts them in explicitly via `compare.channels`.
    let auto_scope: &[channels::ChannelKey] = &[
        channels::ChannelKey::Data,
        channels::ChannelKey::Meta,
        channels::ChannelKey::Errors,
    ];
    let scope: Vec<channels::ChannelKey> = match &resolved.channels {
        Some(list) => list.clone(),
        None       => auto_scope.to_vec(),
    };
    for k in &scope {
        let act = actual_dir.join(format!("{}.ndjson", k.as_str()));
        let exp = expected_dir.join(format!("{}.ndjson", k.as_str()));
        let actual_body = std::fs::read_to_string(&act).unwrap_or_default();
        let actual_non_empty = !actual_body.trim().is_empty();
        let exp_exists = exp.is_file();
        let should_write = match update {
            UpdateMode::Always    => actual_non_empty || exp_exists,
            UpdateMode::IfMissing => actual_non_empty && !exp_exists,
            UpdateMode::None      => unreachable!(),
        };
        if !should_write { continue; }
        let rules = match resolved.per_channel.get(k) {
            Some(r) => r,
            None    => continue,
        };
        let mut canon_lines: Vec<String> = Vec::new();
        for (idx, line) in actual_body.lines().enumerate() {
            let t = line.trim();
            if t.is_empty() { continue; }
            match compare::canonicalise_line(idx, t, rules) {
                Ok(s)  => canon_lines.push(s),
                Err(e) => return Some(Err(anyhow!(
                    "canonicalise {}.ndjson line {}: {}", k.as_str(), idx + 1, e.message
                ))),
            }
        }
        let mut body = canon_lines.join("\n");
        if !body.is_empty() { body.push('\n'); }
        if let Err(e) = atomic_write(&exp, body.as_bytes()) {
            return Some(Err(anyhow!("write {}: {e}", exp.display())));
        }
        wrote.push(exp);
    }
    Some(Ok(wrote))
}

// ─── Phase A — per-channel stream capture ──────────────────────────
//
// All channels written to `.run/actual/` so subsequent comparison
// steps (channel-shape check, per-channel diff, fs-tree compare) can
// read from disk. Every channel file is created — empty file means
// "channel produced no events." Lines that don't parse as JSON are
// dropped silently; the protocol mandates structured envelopes, so
// any non-JSON line is a tool bug we surface in stderr-as-tool-error
// rather than miscategorising as a fake channel event.

const STDOUT_CHANNELS: &[(&str, &str)] = &[
    // (envelope `t` value, output filename without extension)
    ("d", "data"),
    ("m", "meta"),
];

const STDERR_CHANNELS: &[(&str, &str)] = &[
    // (event `type` value, output filename without extension)
    ("error", "errors"),
    ("log",   "logs"),
    ("trace", "trace"),
    ("stats", "stats"),
    // Framework-emitted per-envelope-read marker. Opt-in for tests
    // (same as trace/stats): silently routed to `input.ndjson` and
    // only asserted on when the case declares it. See inbox 0039.
    ("input", "input"),
];

/// One anomalous classify-time event the caller may want to surface as a
/// test error rather than silently dropping. Currently used for lines
/// whose discriminator field IS present but holds an unrecognised value
/// (e.g. `"t":"meta"` — a likely typo of `"m"` — which would otherwise
/// vanish into a "no matching channel" silent drop).
#[derive(Debug, Clone)]
pub(crate) struct ClassifyAnomaly {
    pub discriminator: String,
    pub bad_value:     String,
    pub line_preview:  String,
}

fn write_stdout_channels(buf: &[u8], actual_dir: &Path) -> Result<Vec<ClassifyAnomaly>> {
    let mut bins: BTreeMap<&str, Vec<&[u8]>> = BTreeMap::new();
    for (key, _) in STDOUT_CHANNELS {
        bins.insert(*key, Vec::new());
    }
    let anomalies = classify_lines(buf, "t", &mut bins);
    write_channel_files(STDOUT_CHANNELS, &bins, actual_dir)?;
    Ok(anomalies)
}

fn write_stderr_channels(buf: &[u8], actual_dir: &Path) -> Result<Vec<ClassifyAnomaly>> {
    let mut bins: BTreeMap<&str, Vec<&[u8]>> = BTreeMap::new();
    for (key, _) in STDERR_CHANNELS {
        bins.insert(*key, Vec::new());
    }
    let anomalies = classify_lines(buf, "type", &mut bins);
    write_channel_files(STDERR_CHANNELS, &bins, actual_dir)?;
    Ok(anomalies)
}

/// Walk newline-delimited bytes, parse each line as JSON, route by
/// `discriminator` field's string value into `bins`.
///
/// Returned anomalies cover the case where the discriminator IS present
/// but holds a value that's not a known channel key (e.g. a typo like
/// `"t":"meta"` instead of `"m"`). Without this signal those lines would
/// silently vanish — the test would pass while hiding a real tool bug.
///
/// Lines that fail to parse as JSON, or are empty/whitespace-only, or
/// are missing the discriminator entirely (stdout: routes to `data` for
/// backwards compat; stderr: dropped — non-JSON stderr output is
/// expected) do NOT count as anomalies.
fn classify_lines<'a>(
    buf: &'a [u8],
    discriminator: &str,
    bins: &mut BTreeMap<&str, Vec<&'a [u8]>>,
) -> Vec<ClassifyAnomaly> {
    let mut anomalies: Vec<ClassifyAnomaly> = Vec::new();
    let text = match std::str::from_utf8(buf) {
        Ok(t) => t,
        Err(_) => return anomalies,
    };
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() { continue; }
        let v: Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let (key_owned, was_explicit) = match v.get(discriminator).and_then(|d| d.as_str()) {
            Some(k) => (k.to_string(), true),
            None    => {
                // stdout: missing `t` defaults to "d" (matches today's
                // canonicalize_ndjson behaviour). stderr: missing
                // `type` is unclassifiable — drop. Neither counts as
                // an anomaly: bare envelopes on stdout are valid; plain
                // text on stderr is valid.
                if discriminator == "t" { ("d".to_string(), false) } else { continue }
            }
        };
        // bins is keyed on &'static str; only route when we know the key.
        if let Some((static_key, _)) = STDOUT_CHANNELS.iter()
            .chain(STDERR_CHANNELS.iter())
            .find(|(k, _)| *k == key_owned.as_str())
        {
            if let Some(vec) = bins.get_mut(*static_key) {
                vec.push(line.as_bytes());
            }
        } else if was_explicit {
            // Discriminator was set but doesn't match any known channel.
            // Likely a typo (e.g. `"t":"meta"` vs `"m"`). Surface it so
            // the test fails loudly instead of dropping the line.
            anomalies.push(ClassifyAnomaly {
                discriminator: discriminator.to_string(),
                bad_value:     key_owned,
                line_preview:  preview_line(line),
            });
        }
    }
    anomalies
}

/// Truncate a line for inclusion in an error message. Keeps the first
/// 120 characters and adds an ellipsis if the line was longer.
fn preview_line(line: &str) -> String {
    const MAX: usize = 120;
    if line.chars().count() <= MAX {
        line.to_string()
    } else {
        let mut out: String = line.chars().take(MAX).collect();
        out.push('…');
        out
    }
}

fn write_channel_files(
    channels: &[(&str, &str)],
    bins: &BTreeMap<&str, Vec<&[u8]>>,
    actual_dir: &Path,
) -> Result<()> {
    for (key, name) in channels {
        let path = actual_dir.join(format!("{name}.ndjson"));
        let mut body: Vec<u8> = Vec::new();
        if let Some(lines) = bins.get(*key) {
            for line in lines {
                body.extend_from_slice(line);
                body.push(b'\n');
            }
        }
        std::fs::write(&path, &body)
            .with_context(|| format!("write {}", path.display()))?;
    }
    Ok(())
}

async fn drain_to_vec<R: tokio::io::AsyncRead + Unpin>(mut r: R) -> Vec<u8> {
    let mut buf = Vec::new();
    let _ = r.read_to_end(&mut buf).await;
    buf
}

// ─── Helpers ─────────────────────────────────────────────────────────

fn read_test_spec(case_dir: &Path) -> Result<TestSpec> {
    let path = case_dir.join("test.yaml");
    if !path.is_file() {
        return Ok(TestSpec::default());
    }
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("reading {}", path.display()))?;
    let body = strip_bom(&raw);
    if body.trim().is_empty() {
        return Ok(TestSpec::default());
    }
    let opts = serde_saphyr::options!(
        strict_booleans:      true,
        no_schema:            true,
        legacy_octal_numbers: false,
    );
    serde_saphyr::from_str_with_options(body, opts)
        .map_err(|e| anyhow!("parsing test.yaml: {}", e))
}

fn atomic_write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;
    let tmp = path.with_extension(format!(
        "{}.tmp",
        path.extension().and_then(|e| e.to_str()).unwrap_or("ndjson")
    ));
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)
}

// ─── Phase 4: Coverage ───────────────────────────────────────────────

/// Which bucket a stage falls into for coverage purposes.
///
/// Numerator (counts as covered):   Covered, ExclusiveCovered
/// Denominator (counts against %):  Covered, ExclusiveCovered, ExclusiveUncovered, Uncovered
/// Excluded entirely from %:        SkipList, TestSkipped
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum CoverageBucket {
    Covered,
    /// Hard-coded SKIP_TOOLS list (control layer: gate, checkpoint, toggle, dedup).
    SkipList { tool: String },
    /// `meta.json` `test_skipped: true` — pure I/O tool, no snapshot tests needed.
    TestSkipped,
    ExclusiveCovered,
    ExclusiveUncovered,
    Uncovered,
}

impl CoverageBucket {
    pub fn symbol(&self) -> &'static str {
        match self {
            Self::Covered            => "\u{2713}", // ✓
            Self::SkipList { .. }    => "\u{2298}", // ⊘
            Self::TestSkipped        => "\u{2298}", // ⊘
            Self::ExclusiveCovered   => "\u{25D0}", // ◐
            Self::ExclusiveUncovered => "\u{25D4}", // ◔
            Self::Uncovered          => "\u{2717}", // ✗
        }
    }
    pub fn counts_covered(&self) -> bool {
        matches!(self, Self::Covered | Self::ExclusiveCovered)
    }
    pub fn counts_total(&self) -> bool {
        !matches!(self, Self::SkipList { .. } | Self::TestSkipped)
    }
}

/// Coverage row for one stage within a variant.
#[derive(Debug, Clone, Serialize)]
pub struct StageCoverage {
    pub stage:      String,
    pub tool:       String,
    pub bucket:     CoverageBucket,
    pub case_count: usize,
}

/// Coverage summary for one variant.
#[derive(Debug, Clone, Serialize)]
pub struct VariantCoverage {
    pub variant: String,
    pub stages:  Vec<StageCoverage>,
    /// Number of stages that count as covered (✓ + ◐).
    pub covered: usize,
    /// Total stages that count against the denominator (✓ + ◐ + ◔ + ✗).
    pub total:   usize,
    /// `covered / total * 100`, or 100.0 when total == 0.
    pub pct:     f64,
}

/// Compute coverage for a pipeline. `target.variant = None` → all variants;
/// `Some(name)` → one variant. `target.stage` and `target.case` are ignored.
pub fn coverage(target: &TestTarget, config: &RunnerConfig) -> Result<Vec<VariantCoverage>> {
    let variants_dir = target.pipeline_dir.join("variants");
    if !variants_dir.is_dir() {
        return Err(anyhow!(
            "no variants/ directory at {}",
            target.pipeline_dir.display()
        ));
    }
    let mut variant_names = list_variant_names(&variants_dir)?;
    if let Some(ref v) = target.variant {
        if !variant_names.contains(v) {
            return Err(anyhow!(
                "variant '{}' not found (available: {})",
                v,
                variant_names.join(", ")
            ));
        }
        variant_names = vec![v.clone()];
    }
    let mut out = Vec::new();
    for vname in variant_names {
        out.push(coverage_for_variant(target, config, &vname)?);
    }
    Ok(out)
}

fn list_variant_names(variants_dir: &Path) -> Result<Vec<String>> {
    let mut names = Vec::new();
    for entry in std::fs::read_dir(variants_dir)
        .with_context(|| format!("read_dir {}", variants_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("yaml") { continue; }
        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
            names.push(stem.to_string());
        }
    }
    names.sort();
    Ok(names)
}

fn coverage_for_variant(
    target: &TestTarget,
    config: &RunnerConfig,
    variant_name: &str,
) -> Result<VariantCoverage> {
    let variant = load_variant(&target.pipeline_dir, &target.pipeline_name, variant_name)
        .with_context(|| format!("loading variant '{variant_name}'"))?;

    let tests_root = target.pipeline_dir.join("tests").join(variant_name);
    let mut stages  = Vec::new();
    let mut covered = 0usize;
    let mut total   = 0usize;

    for (stage_id, stage_def) in &variant.stages {
        let case_count = count_cases(&tests_root.join(stage_id));
        let bucket = if SKIP_TOOLS.contains(&stage_def.tool.as_str()) {
            CoverageBucket::SkipList { tool: stage_def.tool.clone() }
        } else {
            match tools::resolve(&stage_def.tool, &target.pipeline_dir, config) {
                Ok(t) if t.meta.test_skipped  => CoverageBucket::TestSkipped,
                Ok(t) if t.meta.test_exclusive => {
                    if case_count > 0 { CoverageBucket::ExclusiveCovered }
                    else              { CoverageBucket::ExclusiveUncovered }
                }
                Ok(_) => {
                    if case_count > 0 { CoverageBucket::Covered }
                    else              { CoverageBucket::Uncovered }
                }
                Err(_) => {
                    // Unresolved tool: treat same as uncovered so it
                    // shows in the report and the user notices.
                    if case_count > 0 { CoverageBucket::Covered }
                    else              { CoverageBucket::Uncovered }
                }
            }
        };
        if bucket.counts_covered() { covered += 1; }
        if bucket.counts_total()   { total   += 1; }
        stages.push(StageCoverage {
            stage: stage_id.clone(),
            tool:  stage_def.tool.clone(),
            bucket,
            case_count,
        });
    }

    let pct = if total == 0 { 100.0 } else { 100.0 * covered as f64 / total as f64 };
    Ok(VariantCoverage { variant: variant_name.to_string(), stages, covered, total, pct })
}

/// Count valid test cases (dirs with `input/seed.ndjson`) under a stage dir.
fn count_cases(stage_dir: &Path) -> usize {
    if !stage_dir.is_dir() { return 0; }
    let Ok(rd) = std::fs::read_dir(stage_dir) else { return 0; };
    rd.filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .filter(|e| e.path().join("input").join("seed.ndjson").is_file())
        .count()
}

// ─── Target parser ───────────────────────────────────────────────────

/// Parse `[<pipeline>][[:<variant>][:<stage>][:<case>]]` into a TestTarget.
///
/// Empty / `.` pipeline means cwd. With colons, parts fill in
/// left-to-right (1 → just pipeline, 2 → +variant, 3 → +stage,
/// 4 → +case). Empty parts inside (`pipe::stage`) are not allowed.
pub fn parse_target(target: &str, cwd: &Path) -> Result<TestTarget> {
    let parts: Vec<&str> = target.split(':').collect();
    if parts.is_empty() || parts.len() > 4 {
        return Err(anyhow!(
            "target '{target}': expected `[<pipeline>][:<variant>[:<stage>[:<case>]]]` (1-4 parts), got {} parts",
            parts.len()
        ));
    }

    let pipeline_dir = if parts[0].is_empty() || parts[0] == "." {
        cwd.to_path_buf()
    } else {
        let candidate = cwd.join(parts[0]);
        if candidate.is_dir() { candidate } else { PathBuf::from(parts[0]) }
    };
    if !pipeline_dir.is_dir() {
        return Err(anyhow!("pipeline dir not found: {}", pipeline_dir.display()));
    }
    let pipeline_name = pipeline_dir.file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("pipeline")
        .to_string();

    // None when the part isn't supplied; Some(non-empty) when supplied.
    // Empty inner part (e.g. `pipe::stage`) is rejected up-front so the
    // user gets a clear error instead of mysterious "no cases" output.
    let variant = parts.get(1).map(|s| s.to_string()).filter(|s| !s.is_empty());
    let stage   = parts.get(2).map(|s| s.to_string()).filter(|s| !s.is_empty());
    let case    = parts.get(3).map(|s| s.to_string()).filter(|s| !s.is_empty());

    if parts.len() >= 2 && variant.is_none() {
        return Err(anyhow!("target '{target}': variant cannot be empty"));
    }
    if parts.len() >= 3 && stage.is_none() {
        return Err(anyhow!("target '{target}': stage cannot be empty"));
    }
    if parts.len() >= 4 && case.is_none() {
        return Err(anyhow!("target '{target}': case cannot be empty"));
    }
    Ok(TestTarget {
        pipeline_dir, pipeline_name,
        variant, stage, case,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_target_full() {
        let cwd = std::env::current_dir().unwrap();
        let t = parse_target(".:v:s:c", &cwd).unwrap();
        assert_eq!(t.variant.as_deref(), Some("v"));
        assert_eq!(t.stage.as_deref(),   Some("s"));
        assert_eq!(t.case.as_deref(),    Some("c"));
        assert!(t.explicit_stage());
    }

    #[test]
    fn parse_target_three_parts_no_default_case() {
        // Phase 3 change: 3 parts means "all cases under that stage",
        // NOT "case-baseline" as Phase 1 had it. Explicit users spell
        // case out as the 4th part.
        let cwd = std::env::current_dir().unwrap();
        let t = parse_target(".:v:s", &cwd).unwrap();
        assert_eq!(t.case, None);
        assert!(t.explicit_stage());
    }

    #[test]
    fn parse_target_two_parts_bulk_variant() {
        let cwd = std::env::current_dir().unwrap();
        let t = parse_target(".:v", &cwd).unwrap();
        assert_eq!(t.variant.as_deref(), Some("v"));
        assert_eq!(t.stage, None);
        assert_eq!(t.case,  None);
        assert!(!t.explicit_stage());
    }

    #[test]
    fn parse_target_one_part_bulk_pipeline() {
        let cwd = std::env::current_dir().unwrap();
        let t = parse_target(".", &cwd).unwrap();
        assert_eq!(t.variant, None);
        assert_eq!(t.stage,   None);
        assert_eq!(t.case,    None);
        assert!(!t.explicit_stage());
    }

    #[test]
    fn parse_target_rejects_empty_parts() {
        let cwd = std::env::current_dir().unwrap();
        assert!(parse_target(".:", &cwd).is_err());          // empty variant
        assert!(parse_target(".:v:", &cwd).is_err());        // empty stage
        assert!(parse_target(".:v:s:", &cwd).is_err());      // empty case
    }


    #[test]
    fn atomic_write_creates_file_and_overwrites() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("nested/expected/data.ndjson");
        atomic_write(&p, b"first\n").unwrap();
        assert_eq!(std::fs::read_to_string(&p).unwrap(), "first\n");
        atomic_write(&p, b"second\n").unwrap();
        assert_eq!(std::fs::read_to_string(&p).unwrap(), "second\n");
        let parent = p.parent().unwrap();
        for entry in std::fs::read_dir(parent).unwrap() {
            let name = entry.unwrap().file_name();
            let s = name.to_string_lossy();
            assert!(!s.ends_with(".tmp"), "leftover tmp file: {s}");
        }
    }

    #[test]
    fn skip_list_constants() {
        // Pin the curated list — accidental drift would silently change
        // bulk-test behavior across all pipelines using dpe test.
        assert_eq!(SKIP_TOOLS, &["toggle", "gate", "checkpoint", "dedup"]);
    }

    #[test]
    fn discover_finds_baseline() {
        // Build a fake tests/ tree and prove `discover` enumerates it.
        let dir = tempfile::tempdir().unwrap();
        let pipeline_dir = dir.path();
        let cd = pipeline_dir.join("tests/v1/stage-a/case-baseline");
        std::fs::create_dir_all(cd.join("input")).unwrap();
        std::fs::write(cd.join("input/seed.ndjson"), b"{}\n").unwrap();
        // A second case under the same stage.
        let cd2 = pipeline_dir.join("tests/v1/stage-a/case-edge");
        std::fs::create_dir_all(cd2.join("input")).unwrap();
        std::fs::write(cd2.join("input/seed.ndjson"), b"{}\n").unwrap();
        // A case folder without seed → must be ignored.
        std::fs::create_dir_all(pipeline_dir.join("tests/v1/stage-a/case-empty")).unwrap();

        let target = TestTarget {
            pipeline_dir:  pipeline_dir.to_path_buf(),
            pipeline_name: "p".into(),
            variant: None, stage: None, case: None,
        };
        let cases = discover(&target).unwrap();
        let ids: Vec<String> = cases.iter()
            .map(|c| format!("{}:{}:{}", c.variant, c.stage, c.case))
            .collect();
        assert_eq!(ids, vec![
            "v1:stage-a:case-baseline".to_string(),
            "v1:stage-a:case-edge".to_string(),
        ]);
    }

    #[test]
    fn coverage_bucket_symbols_and_flags() {
        use super::{CoverageBucket};
        let covered     = CoverageBucket::Covered;
        let skip        = CoverageBucket::SkipList { tool: "gate".into() };
        let test_skipped = CoverageBucket::TestSkipped;
        let exc_cov     = CoverageBucket::ExclusiveCovered;
        let exc_uncov   = CoverageBucket::ExclusiveUncovered;
        let uncov       = CoverageBucket::Uncovered;

        // Numerator membership
        assert!( covered.counts_covered());
        assert!(!skip.counts_covered());
        assert!(!test_skipped.counts_covered());
        assert!( exc_cov.counts_covered());
        assert!(!exc_uncov.counts_covered());
        assert!(!uncov.counts_covered());

        // Denominator membership — both skip-list AND test-skipped are excluded
        assert!( covered.counts_total());
        assert!(!skip.counts_total());
        assert!(!test_skipped.counts_total());
        assert!( exc_cov.counts_total());
        assert!( exc_uncov.counts_total());
        assert!( uncov.counts_total());

        // All symbols non-empty; SkipList and TestSkipped share ⊘ intentionally
        for b in [&covered, &skip, &test_skipped, &exc_cov, &exc_uncov, &uncov] {
            assert!(!b.symbol().is_empty());
        }
        // Covered, ExclusiveCovered, ExclusiveUncovered, Uncovered are all distinct
        let logic_syms = [covered.symbol(), exc_cov.symbol(), exc_uncov.symbol(), uncov.symbol()];
        let mut seen = std::collections::HashSet::new();
        for s in logic_syms { assert!(seen.insert(s), "duplicate symbol: {s}"); }
    }

    #[test]
    fn count_cases_counts_only_valid_seed_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path();

        // Valid case: has input/seed.ndjson
        let c1 = base.join("case-good");
        std::fs::create_dir_all(c1.join("input")).unwrap();
        std::fs::write(c1.join("input/seed.ndjson"), b"{}").unwrap();

        // Invalid: directory exists but no seed
        std::fs::create_dir_all(base.join("case-noseed")).unwrap();

        // Invalid: seed is a directory, not a file
        let c3 = base.join("case-dir-seed");
        std::fs::create_dir_all(c3.join("input").join("seed.ndjson")).unwrap();

        assert_eq!(super::count_cases(base), 1);
    }

    #[test]
    fn list_variant_names_filters_yaml_only() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path();
        std::fs::write(base.join("main.yaml"),  b"").unwrap();
        std::fs::write(base.join("dev.yaml"),   b"").unwrap();
        std::fs::write(base.join("notes.txt"),  b"").unwrap();
        std::fs::create_dir(base.join("subdir")).unwrap();

        let names = super::list_variant_names(base).unwrap();
        assert_eq!(names, vec!["dev".to_string(), "main".to_string()]);
    }

    #[test]
    fn discover_filters_by_target() {
        let dir = tempfile::tempdir().unwrap();
        let pipeline_dir = dir.path();
        for v in &["v1", "v2"] {
            for s in &["stage-a", "stage-b"] {
                {
                    let c = "case-baseline";
                    let cd = pipeline_dir.join(format!("tests/{v}/{s}/{c}"));
                    std::fs::create_dir_all(cd.join("input")).unwrap();
                    std::fs::write(cd.join("input/seed.ndjson"), b"{}\n").unwrap();
                }
            }
        }
        let bulk_v1 = TestTarget {
            pipeline_dir: pipeline_dir.to_path_buf(),
            pipeline_name: "p".into(),
            variant: Some("v1".into()), stage: None, case: None,
        };
        let cases = discover(&bulk_v1).unwrap();
        assert_eq!(cases.len(), 2);
        assert!(cases.iter().all(|c| c.variant == "v1"));

        let bulk_stage = TestTarget {
            pipeline_dir: pipeline_dir.to_path_buf(),
            pipeline_name: "p".into(),
            variant: Some("v1".into()),
            stage:   Some("stage-a".into()),
            case:    None,
        };
        let cases = discover(&bulk_stage).unwrap();
        assert_eq!(cases.len(), 1);
        assert_eq!(cases[0].stage, "stage-a");
    }

    // ─── Track C: env + settings_override ───────────────────────────────────

    #[test]
    fn test_spec_env_parsed_from_yaml() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = "env:\n  MY_VAR: hello\n  OTHER: world\n";
        std::fs::write(dir.path().join("test.yaml"), yaml).unwrap();
        let spec = read_test_spec(dir.path()).unwrap();
        let env = spec.env.unwrap();
        assert_eq!(env.get("MY_VAR").map(String::as_str), Some("hello"));
        assert_eq!(env.get("OTHER").map(String::as_str),  Some("world"));
    }

    #[test]
    fn test_spec_settings_override_with_env_ref_resolved_from_test_env() {
        // settings_override contains ${MY_MODEL}; test_spec.env sets MY_MODEL.
        // The same logic run_one_case uses: build MapEnv from process+spec, then
        // call interpolate_in_value.
        let spec_env: BTreeMap<String, String> = [
            ("MY_MODEL".to_string(), "claude-opus-4-7".to_string()),
        ].into_iter().collect();

        let settings = serde_json::json!({ "model": "${MY_MODEL}", "n": 10 });

        let mut env_map: BTreeMap<String, String> = std::env::vars().collect();
        for (k, v) in &spec_env { env_map.insert(k.clone(), v.clone()); }

        let resolved = interpolate_in_value(&settings, &MapEnv(env_map)).unwrap();
        assert_eq!(resolved["model"], "claude-opus-4-7");
        assert_eq!(resolved["n"], 10);
    }

    #[test]
    fn test_spec_settings_override_with_env_ref_resolved_from_process_env() {
        // When test_spec.env is None, interpolate_in_value uses ProcessEnv (host env).
        let key = "DPE_TEST_RUNNER_TRACK_C_VAR_X1";
        std::env::set_var(key, "process-value");
        let settings = serde_json::json!({ "val": format!("${{{key}}}") });
        let resolved = interpolate_in_value(&settings, &ProcessEnv).unwrap();
        std::env::remove_var(key);
        assert_eq!(resolved["val"], "process-value");
    }

    #[test]
    fn test_spec_env_overrides_process_env_for_interp() {
        // When the same var appears in both process env and test_spec.env,
        // test_spec.env wins (it's inserted after process env into the map).
        let key = "DPE_TEST_RUNNER_TRACK_C_OVERRIDE_X2";
        std::env::set_var(key, "process-value");
        let spec_env: BTreeMap<String, String> = [
            (key.to_string(), "spec-value".to_string()),
        ].into_iter().collect();

        let settings = serde_json::json!({ "val": format!("${{{key}}}") });
        let mut env_map: BTreeMap<String, String> = std::env::vars().collect();
        for (k, v) in &spec_env { env_map.insert(k.clone(), v.clone()); }
        let resolved = interpolate_in_value(&settings, &MapEnv(env_map)).unwrap();
        std::env::remove_var(key);
        assert_eq!(resolved["val"], "spec-value");
    }

    // ─── Cache mode (--cache flag + test.yaml cache: field) ───────────────

    #[test]
    fn test_spec_cache_parsed_from_yaml() {
        use crate::types::CacheMode;
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("test.yaml"), "cache: bypass\n").unwrap();
        let spec = read_test_spec(dir.path()).unwrap();
        assert_eq!(spec.cache, Some(CacheMode::Bypass));

        std::fs::write(dir.path().join("test.yaml"), "cache: refresh\n").unwrap();
        let spec = read_test_spec(dir.path()).unwrap();
        assert_eq!(spec.cache, Some(CacheMode::Refresh));

        // No cache key present → None (caller falls through to default).
        std::fs::write(dir.path().join("test.yaml"), "env:\n  X: \"hello\"\n").unwrap();
        let spec = read_test_spec(dir.path()).unwrap();
        assert_eq!(spec.cache, None);
    }

    #[test]
    fn test_spec_cache_unknown_value_rejected() {
        // serde's lowercase rename means anything not in the enum fails.
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("test.yaml"), "cache: turbo\n").unwrap();
        let res = read_test_spec(dir.path());
        assert!(res.is_err(), "unknown cache mode should reject");
    }

    #[test]
    fn cache_precedence_cli_wins() {
        // Logic mirrors run_one_case:
        //   effective = cli_cache.or(test_spec.cache).unwrap_or(Use)
        use crate::types::CacheMode;
        let cli = Some(CacheMode::Bypass);
        let spec = Some(CacheMode::Refresh);
        let eff = cli.or(spec).unwrap_or(CacheMode::Use);
        assert_eq!(eff, CacheMode::Bypass);
    }

    #[test]
    fn cache_precedence_spec_wins_when_no_cli() {
        use crate::types::CacheMode;
        let cli: Option<CacheMode> = None;
        let spec = Some(CacheMode::Bypass);
        let eff = cli.or(spec).unwrap_or(CacheMode::Use);
        assert_eq!(eff, CacheMode::Bypass);
    }

    #[test]
    fn cache_precedence_default_use_when_neither() {
        use crate::types::CacheMode;
        let cli: Option<CacheMode> = None;
        let spec: Option<CacheMode> = None;
        let eff = cli.or(spec).unwrap_or(CacheMode::Use);
        assert_eq!(eff, CacheMode::Use);
    }

    // ─── timeout_ms — case-level + phase-level + default ────────────────

    #[test]
    fn test_spec_timeout_ms_parsed_from_yaml() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("test.yaml"), "timeout_ms: 600000\n").unwrap();
        let spec = read_test_spec(dir.path()).unwrap();
        assert_eq!(spec.timeout_ms, Some(600_000));

        // Absent → None (caller falls back to 60_000).
        std::fs::write(dir.path().join("test.yaml"), "env:\n  X: \"hi\"\n").unwrap();
        let spec = read_test_spec(dir.path()).unwrap();
        assert_eq!(spec.timeout_ms, None);
    }

    #[test]
    fn timeout_precedence_phase_then_case_then_default() {
        // Mirrors run_phase logic:
        //   effective = phase.timeout_ms.or(case.timeout_ms).unwrap_or(60_000)
        let phase: Option<u64> = Some(5_000);
        let case:  Option<u64> = Some(120_000);
        assert_eq!(phase.or(case).unwrap_or(60_000), 5_000);

        let phase: Option<u64> = None;
        let case:  Option<u64> = Some(120_000);
        assert_eq!(phase.or(case).unwrap_or(60_000), 120_000);

        let phase: Option<u64> = None;
        let case:  Option<u64> = None;
        assert_eq!(phase.or(case).unwrap_or(60_000), 60_000);
    }

    #[test]
    fn phase_spec_timeout_ms_parsed_from_yaml() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = r#"timeout_ms: 30000
phases:
  - name: cold
    timeout_ms: 600000
  - name: warm
"#;
        std::fs::write(dir.path().join("test.yaml"), yaml).unwrap();
        let spec = read_test_spec(dir.path()).unwrap();
        assert_eq!(spec.timeout_ms, Some(30_000));
        let phases = spec.phases.unwrap();
        assert_eq!(phases.len(), 2);
        assert_eq!(phases[0].name, "cold");
        assert_eq!(phases[0].timeout_ms, Some(600_000));
        assert_eq!(phases[1].name, "warm");
        assert_eq!(phases[1].timeout_ms, None);
    }

    // ─── --env-file (dotenvy) — "existing process env wins" guarantee ─────

    #[test]
    fn dotenvy_does_not_override_already_set_vars() {
        // Hard requirement of our --env-file design: a checked-in `.env`
        // never silently shadows a CI / shell-exported secret. dotenvy's
        // `from_path` is the API that gives us this — verify the guarantee
        // here so a future dep upgrade doesn't quietly change behavior.
        let key = "DPE_TEST_DOTENVY_NOOVERRIDE_VAR";
        std::env::set_var(key, "process-wins");

        let dir = tempfile::tempdir().unwrap();
        let env_file = dir.path().join(".env");
        std::fs::write(&env_file, format!("{key}=file-value\n")).unwrap();

        // dotenvy's from_path returns Ok even when keys are already set;
        // it just skips them. Documented behavior we depend on.
        let _ = dotenvy::from_path(&env_file);

        let v = std::env::var(key).unwrap();
        std::env::remove_var(key);
        assert_eq!(v, "process-wins",
            "dotenvy must not override a pre-set process env var");
    }

    #[test]
    fn dotenvy_loads_unset_vars() {
        // Companion to the above: vars NOT already set should land
        // in the process env after from_path.
        let key = "DPE_TEST_DOTENVY_LOADS_FRESH_VAR";
        std::env::remove_var(key);

        let dir = tempfile::tempdir().unwrap();
        let env_file = dir.path().join(".env");
        std::fs::write(&env_file, format!("{key}=loaded-from-file\n")).unwrap();

        let r = dotenvy::from_path(&env_file);
        assert!(r.is_ok(), "from_path should succeed: {r:?}");

        let v = std::env::var(key).unwrap();
        std::env::remove_var(key);
        assert_eq!(v, "loaded-from-file");
    }

    #[test]
    fn dotenvy_missing_file_errors() {
        // Our --env-file design treats a missing path as a hard error
        // (no silent CWD pickup). Verify dotenvy gives us a real error
        // for that path so main.rs can wrap it cleanly.
        let r = dotenvy::from_path("D:/this/path/should/not/exist/.env");
        assert!(r.is_err());
    }
}
