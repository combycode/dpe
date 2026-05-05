//! dpe — CLI entry for the DPE runner.

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "dpe", version, about = "DPE pipeline runner CLI")]
struct Cli {
    /// Runner config file (default: ~/.dpe/config.toml)
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
// One-shot CLI parse — boxing variants to chase 280 bytes vs 88 buys
// nothing real (the enum is constructed once per invocation and dropped).
#[allow(clippy::large_enum_variant)]
enum Command {
    /// Initialise a new pipeline directory with a ready-to-run skeleton
    Init {
        /// Pipeline name (directory name)
        name: String,
        /// Output parent directory (default: current directory)
        #[arg(short, long, default_value = ".")]
        out: PathBuf,
    },
    /// Validate a pipeline variant (parse + resolve + compile expressions)
    Check {
        /// pipeline[:variant]
        target: String,
        /// Check all variants for the pipeline
        #[arg(long)]
        all: bool,
        /// After validation, compile + print the ExecutionPlan as JSON.
        /// Useful for inspecting tool resolution, settings expansion, and
        /// wiring without actually running. `$session` paths stay literal —
        /// they are only resolved at run time.
        #[arg(long)]
        plan: bool,
        /// Treat unset `${VAR}` references as empty string instead of
        /// erroring at validation. Used by editor-time validation where
        /// the runtime env isn't known yet. `dpe run` always requires
        /// every referenced env var to be set.
        #[arg(long)]
        allow_undefined_env: bool,
    },
    /// Run a pipeline variant
    Run {
        /// pipeline[:variant]
        target: String,
        /// Input directory ($input)
        #[arg(short, long)]
        input: PathBuf,
        /// Output directory ($output)
        #[arg(short, long)]
        output: PathBuf,
        /// What to clear before starting (session|temp|storage|all)
        #[arg(long)]
        clear: Option<String>,
        /// Cache behavior (use|refresh|bypass)
        #[arg(long)]
        cache: Option<String>,
        /// Override `$temp`. Default: <pipeline>/temp. Use this to
        /// isolate scratch state between concurrent runs of the same
        /// pipeline (which would otherwise collide on checkpoint spool
        /// dirs etc.). Added in v2.0.1.
        #[arg(long)]
        temp_dir: Option<PathBuf>,
        /// Override `$storage`. Default: <pipeline>/storage. Use this
        /// to isolate persistent state (dedup index, write-stream
        /// hashed indexes, etc.) between concurrent runs. Added in v2.0.1.
        #[arg(long)]
        storage_dir: Option<PathBuf>,
        /// Inject a single envelope as the run's first input — raw NDJSON
        /// line (no surrounding quotes; one envelope per `--seed`). Mutually
        /// exclusive with `--seed-file`. Added in v2.0.1.
        #[arg(long, conflicts_with = "seed_file")]
        seed: Option<String>,
        /// Inject a file of NDJSON envelopes as the run's input. Mutually
        /// exclusive with `--seed`. Pipelines that start with a stdin-
        /// consuming tool (read-file-stream, normalize, etc.) receive these
        /// envelopes directly; the upstream is wired transparently — no
        /// pipeline YAML changes required. Added in v2.0.1.
        #[arg(long, conflicts_with = "seed")]
        seed_file: Option<PathBuf>,
        /// Emit machine-readable NDJSON to stdout instead of human text.
        /// Two events: `{"event":"started", ...}` at run start (with
        /// sessionId, sessionDir, controlAddr, pid) and
        /// `{"event":"summary", ...}` at completion. Used by editors and
        /// other tooling consumers to spawn `dpe run` and parse session
        /// metadata without text-scraping. Added in v2.0.1.
        #[arg(long)]
        json: bool,
        /// Emit periodic per-stage counter snapshots:
        ///   --stats        — every 500 ms (default)
        ///   --stats 50     — every 50 ms
        /// JSON mode: `{"event":"stats","stages":{"<stage>":[rowsOut,errors],...},"t":...}`.
        /// Text mode: `[stats t=...] <stage>: rowsOut/errors; ...`.
        /// Without the flag, no stats lines are emitted. Added in v2.0.1.
        #[arg(long, value_name = "MS", num_args = 0..=1, default_missing_value = "500")]
        stats: Option<u64>,
        /// Capture each terminal stage's stdout to `<session>/debug/<stage>.ndjson`.
        ///   --debug-capture                    — all terminals
        ///   --debug-capture stage1,stage2      — only the listed stages
        /// Without the flag, terminal stdout is silently discarded
        /// (default). Useful when a stage emits envelopes you forgot to
        /// route — captured files let you inspect what flowed past.
        /// Tools that write their own files (write-file-stream,
        /// mongo-upsert, etc.) don't need this — their output goes
        /// where their settings say. Added in v2.0.1.
        #[arg(long, value_name = "STAGES", num_args = 0..=1, default_missing_value = "")]
        debug_capture: Option<String>,
    },
    /// Rebuild journal.json for a session by scanning artefacts on disk.
    /// Use after an abnormal termination (kill, crash) where the runner
    /// did not get to finalize the journal itself.
    Journal {
        /// Path to the session directory (e.g. pipeline/sessions/<id>_<variant>)
        session: PathBuf,
    },
    /// Query a running session's live status via its control socket.
    Status {
        /// Session directory
        session: PathBuf,
    },
    /// Query progress (gates, totals) from a running session.
    Progress {
        session: PathBuf,
    },
    /// Request a running session to stop gracefully.
    Stop {
        session: PathBuf,
    },
    /// Tail $session/log.ndjson, formatting each line as `[stage] level: msg`.
    Logs {
        session: PathBuf,
        /// Follow as new lines append (like tail -f). Default: print what's there and exit.
        #[arg(short, long)]
        follow: bool,
    },
    /// Stream / tail logs and errors for a session. Per-stage filterable.
    ///
    /// Sources:
    ///   - `<session>/log.ndjson`            (logs from ctx.log() across all stages)
    ///   - `<session>/logs/<stage>_errors.log` (errors from ctx.error() per stage)
    ///
    /// Default mode (no flags): emit the last N entries time-merged, exit.
    /// `--follow`: live-tail until run ends. Auto-degrades to default mode
    /// if the session is already terminal at start.
    /// `--stage`: filter to one stage. Without it: every stage's events.
    /// `--error` / `--log`: source filter (mutually exclusive). Without
    /// either: both sources merged.
    /// `--tail N`: override the default count from `[log_sink].tail_default`.
    ///
    /// Output is NDJSON with `kind:"log"|"error"` discriminator. Stats and
    /// state transitions are NOT emitted here — those are `dpe run --stats`.
    Log {
        /// Session directory.
        session: PathBuf,
        /// Filter to a single stage id (matches `sid` in events).
        #[arg(long)]
        stage: Option<String>,
        /// Live-tail; without it, prints last N and exits. Mutex with --search.
        #[arg(long, short = 'f', conflicts_with = "search")]
        follow: bool,
        /// Last N entries (or last N matches when --search is set).
        /// Default: `[log_sink].tail_default` (50).
        #[arg(long)]
        tail: Option<usize>,
        /// Errors only. Mutex with `--log`.
        #[arg(long, conflicts_with = "log_only")]
        error: bool,
        /// Logs only. Mutex with `--error`.
        #[arg(long = "log", conflicts_with = "error")]
        log_only: bool,
        /// Filter entries to those whose `msg` (logs) or `error` (errors)
        /// field matches the pattern. Default: case-insensitive substring.
        /// With `--regex`: full Rust regex syntax.
        #[arg(long)]
        search: Option<String>,
        /// Treat `--search` pattern as a regex instead of substring.
        #[arg(long, requires = "search")]
        regex: bool,
    },
    /// Live TUI dashboard for a session (tabs: Stages / Pipeline / Logs).
    Monitor {
        session: PathBuf,
    },
    /// Show tool catalogue + installed state.
    Tools {
        #[command(subcommand)]
        sub: ToolsCmd,
    },
    /// Install a tool from the catalogue into ~/.dpe/tools/<name>/.
    Install {
        /// Tool name (must be in the catalogue; see `dpe tools list`).
        name: String,
        /// Overwrite existing installation.
        #[arg(long)]
        force: bool,
    },
    /// Inspect / edit runner config (~/.dpe/config.toml).
    Config {
        #[command(subcommand)]
        sub: ConfigCmd,
    },
}

#[derive(Debug, Subcommand)]
enum ToolsCmd {
    /// List tools: name, runtime, status, version.
    List {
        /// Emit JSON to stdout instead of a human-readable table.
        /// Includes catalog metadata + path-discovered tools
        /// (no catalog entry needed) + spec.yaml settings schema
        /// when available. Used by editors and other tooling consumers.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
enum ConfigCmd {
    /// Print the resolved config (after defaults + file loads).
    Show,
    /// Create `~/.dpe/config.toml` (and `~/.dpe/tools/`, `~/.dpe/registries/`)
    /// with sensible defaults if missing. Idempotent: existing config is
    /// not touched. Useful on a fresh install before `dpe install`.
    Init {
        /// Overwrite an existing ~/.dpe/config.toml if present.
        #[arg(long)]
        force: bool,
    },
    /// Append a directory to [tools] paths[] in ~/.dpe/config.toml.
    AddPath { path: PathBuf },
    /// Print the resolved config file path (or what it would be).
    Path,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let cfg = dpe::load_config(cli.config.as_deref())?;

    match cli.command {
        Command::Init { name, out } => cmd_init(&name, &out),
        Command::Check { target, all, plan, allow_undefined_env } =>
            cmd_check(target, all, plan, allow_undefined_env, &cfg),
        Command::Run {
            target, input, output, clear: _, cache,
            temp_dir, storage_dir, seed, seed_file, json, stats, debug_capture,
        } => cmd_run(target, input, output, cache, temp_dir, storage_dir, seed, seed_file, json, stats, debug_capture, &cfg),
        Command::Journal { session } => cmd_journal(session),
        Command::Status { session }   => cmd_status(session),
        Command::Progress { session } => cmd_progress(session),
        Command::Stop { session }     => cmd_stop(session),
        Command::Logs { session, follow } => cmd_logs(session, follow),
        Command::Log  { session, stage, follow, tail, error, log_only, search, regex }
            => cmd_log(session, stage, follow, tail, error, log_only, search, regex, &cfg),
        Command::Monitor { session }      => dpe::monitor::run(session),
        Command::Tools { sub } => match sub {
            ToolsCmd::List { json } => cmd_tools_list(&cfg, json),
        },
        Command::Install { name, force } => cmd_install(&cfg, &name, force),
        Command::Config { sub } => match sub {
            ConfigCmd::Show        => cmd_config_show(&cfg),
            ConfigCmd::Init { force } => cmd_config_init(force),
            ConfigCmd::AddPath { path } => cmd_config_add_path(&path),
            ConfigCmd::Path        => cmd_config_path(),
        },
    }
}

fn cmd_init(name: &str, out: &std::path::Path) -> anyhow::Result<()> {
    let dir = dpe::init::init(name, out)?;
    println!("[OK] created pipeline '{}' at {}", name, dir.display());
    println!("     next: cd {} && dpe run .:main -i data/input -o data/output",
        dir.display());
    Ok(())
}

fn cmd_status(session: PathBuf) -> anyhow::Result<()> {
    use dpe::session_proxy::SessionProxy;
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let mut proxy = dpe::session_proxy::ControlSocketProxy::new(session);
    let report = rt.block_on(proxy.status())?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn cmd_progress(session: PathBuf) -> anyhow::Result<()> {
    use dpe::session_proxy::SessionProxy;
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let mut proxy = dpe::session_proxy::ControlSocketProxy::new(session);
    let report = rt.block_on(proxy.progress())?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn cmd_stop(session: PathBuf) -> anyhow::Result<()> {
    use dpe::session_proxy::SessionProxy;
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let mut proxy = dpe::session_proxy::ControlSocketProxy::new(session);
    rt.block_on(proxy.stop())?;
    println!("[OK] stop signaled");
    Ok(())
}

fn cmd_logs(session: PathBuf, follow: bool) -> anyhow::Result<()> {
    use std::io::{BufRead, BufReader, Seek, SeekFrom};
    let path = session.join("log.ndjson");
    let mut file = std::fs::File::open(&path)
        .map_err(|e| anyhow::anyhow!("cannot open {}: {}", path.display(), e))?;

    let mut pos = 0u64;
    loop {
        file.seek(SeekFrom::Start(pos))?;
        let reader = BufReader::new(&file);
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() { continue; }
            match serde_json::from_str::<serde_json::Value>(&line) {
                Ok(v) => {
                    let sid = v.get("sid").and_then(|x| x.as_str()).unwrap_or("?");
                    let lvl = v.get("level").and_then(|x| x.as_str()).unwrap_or("info");
                    let msg = v.get("msg").and_then(|x| x.as_str()).unwrap_or("");
                    println!("[{}] {}: {}", sid, lvl, msg);
                }
                Err(_) => println!("{}", line),
            }
        }
        pos = file.metadata()?.len();
        if !follow { return Ok(()); }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
}

// ═══ dpe log <session> — per-stage log+error stream / tail ═══════════════
//
// Reads two on-disk sources:
//   - <session>/log.ndjson                 (one line per ctx.log() call,
//                                           all stages mixed, has `t`)
//   - <session>/logs/<stage>_errors.log    (one line per ctx.error() call,
//                                           one file per stage, has `t` + `sid`)
//
// Output is NDJSON, time-merged, with a `kind` discriminator:
//   {"t":...,"sid":"X","kind":"log","level":"info","msg":"...",...extras}
//   {"t":...,"sid":"X","kind":"error","error":"...","input":...,"id":...,"src":...}
//
// Stats and state transitions are NOT emitted here — consumers wanting
// those subscribe to `dpe run --stats` instead.
//
// Modes (mutually exclusive on `--follow` axis):
//   default      : merge backlog, keep last `--tail N` (or [log_sink].tail_default), exit
//   --follow / -f: same backlog cap, then live-tail until session terminal
//
// Source filter:
//   none      : both log.ndjson AND <sid>_errors.log
//   --error   : errors only (from <sid>_errors.log files)
//   --log     : logs only   (from log.ndjson)
//
// Stage filter:
//   none           : every stage; errors from every <sid>_errors.log file
//   --stage <name> : only that sid in log.ndjson + only that <stage>_errors.log
//
// `--follow` auto-degrades to default mode when the session is already
// terminal at start (journal.json state ∈ {succeeded,partial,failed,killed}).

#[allow(clippy::too_many_arguments)]
fn cmd_log(
    session: PathBuf,
    stage: Option<String>,
    follow: bool,
    tail: Option<usize>,
    error_only: bool,
    log_only: bool,
    search: Option<String>,
    use_regex: bool,
    cfg: &dpe::RunnerConfig,
) -> anyhow::Result<()> {
    use std::time::Duration;

    if !session.is_dir() {
        anyhow::bail!("session directory not found: {}", session.display());
    }
    let include_log = !error_only;
    let include_err = !log_only;
    let tail_n = tail.unwrap_or_else(|| cfg.log_sink.effective_tail_default());

    // Compile the search filter once. With --regex we honour Rust's
    // regex syntax verbatim; without it, escape user input and wrap in
    // (?i) so plain words match case-insensitively. clap already enforces
    // --search ⊥ --follow so we don't need to handle the streaming case.
    let search_re: Option<regex::Regex> = match search.as_deref() {
        None => None,
        Some(p) if use_regex => Some(regex::Regex::new(p)
            .map_err(|e| anyhow::anyhow!("invalid --search regex: {e}"))?),
        Some(p) => {
            let escaped = regex::escape(p);
            Some(regex::Regex::new(&format!("(?i){escaped}"))
                .expect("substring pattern always compiles"))
        }
    };

    let log_path   = session.join("log.ndjson");
    let errors_dir = session.join("logs");

    // Poll cadence for follow mode. Reuses runtime.monitor_poll_ms — same
    // knob already governs other "tail-and-watch" loops.
    let poll_ms = cfg.runtime.effective_monitor_poll_ms();

    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    rt.block_on(async move {
        // Auto-degrade --follow if session is already terminal: there's
        // nothing to follow, so collapse to "last N + exit" semantics.
        let effective_follow = follow && !is_session_terminal(&session).await;

        // ─── Backlog: collect every on-disk entry, time-sort, tail to N ─────
        let mut cursor = LogCursor::default();
        let mut entries = Vec::new();
        if include_log {
            cursor.log_pos = scan_log_file(
                &log_path, 0, stage.as_deref(), &mut entries,
            ).await;
        }
        if include_err {
            scan_errors_dir(
                &errors_dir, stage.as_deref(),
                &mut cursor.err_pos, &mut entries,
            ).await;
        }
        // Apply search filter BEFORE sort+tail. For 1M-envelope runs we
        // care about "the most recent 50 mentions of 'zephyr'", not "50
        // newest entries that incidentally contain it" — so filter, then
        // sort, then take the last N.
        if let Some(re) = &search_re {
            entries.retain(|v| match_entry(v, re));
        }
        entries.sort_by_key(entry_t);

        // Apply --tail window. For --follow runs, this caps the initial
        // backlog dump too — without that, opening the modal late in a
        // long run would dump every line before tailing began.
        let start = entries.len().saturating_sub(tail_n);
        for v in &entries[start..] {
            emit_event(v.clone());
        }

        if !effective_follow {
            return Ok::<(), anyhow::Error>(());
        }

        // ─── Live tail: poll cycle until session goes terminal ──────────────
        // We give the BufWriter behind log.ndjson one extra cycle after
        // detecting termination so a final flush doesn't get clipped.
        let mut consecutive_terminal_polls: u32 = 0;
        loop {
            tokio::time::sleep(Duration::from_millis(poll_ms)).await;

            let mut new_entries = Vec::new();
            if include_log {
                cursor.log_pos = scan_log_file(
                    &log_path, cursor.log_pos, stage.as_deref(), &mut new_entries,
                ).await;
            }
            if include_err {
                scan_errors_dir(
                    &errors_dir, stage.as_deref(),
                    &mut cursor.err_pos, &mut new_entries,
                ).await;
            }
            new_entries.sort_by_key(entry_t);
            for v in new_entries {
                emit_event(v);
            }

            if is_session_terminal(&session).await {
                consecutive_terminal_polls += 1;
                // Two terminal polls in a row → safe to exit. The first
                // poll may have raced the runner's final journal write +
                // log.ndjson flush; the second guarantees we drained any
                // tail-end bytes the writer was still buffering.
                if consecutive_terminal_polls >= 2 {
                    return Ok(());
                }
            } else {
                consecutive_terminal_polls = 0;
            }
        }
    })
}

/// Position cursors for follow-mode tailing. Tracks log.ndjson byte
/// offset + per-error-file offset. New stages whose errors file appears
/// AFTER cmd_log starts are picked up automatically by `scan_errors_dir`
/// (it discovers files on each call, not just on init).
#[derive(Default)]
struct LogCursor {
    log_pos: u64,
    err_pos: std::collections::HashMap<std::path::PathBuf, u64>,
}

/// Pull the `t` field for sort. Entries without `t` sort to the front (0).
fn entry_t(v: &serde_json::Value) -> u64 {
    v.get("t").and_then(|x| x.as_u64()).unwrap_or(0)
}

/// Test whether `v` matches the search regex.
///
/// For log entries (`kind:"log"`): probe `msg` only.
/// For error entries (`kind:"error"`): probe `error` AND the stringified
/// `input` payload. Including `input` lets users search for "zephyr"
/// against errors whose ctx.error preserved a message containing that
/// word — the common case in pipelines that don't ctx.log every envelope.
///
/// Envelope id/src are NEVER searched: they're hex hashes and matching
/// against them is always a false positive.
fn match_entry(v: &serde_json::Value, re: &regex::Regex) -> bool {
    if let Some(s) = v.get("msg").and_then(|x| x.as_str()) {
        if re.is_match(s) { return true; }
    }
    if let Some(s) = v.get("error").and_then(|x| x.as_str()) {
        if re.is_match(s) { return true; }
    }
    if v.get("kind").and_then(|x| x.as_str()) == Some("error") {
        if let Some(input) = v.get("input") {
            // Cheap canonical render — we only check is_match, no other
            // use of the rendered string. Skips re-emitting the value.
            let s = serde_json::to_string(input).unwrap_or_default();
            if re.is_match(&s) { return true; }
        }
    }
    false
}

/// Read new bytes from `<session>/log.ndjson` starting at `pos`. Filter
/// to `stage` if provided. Append normalized `{kind:"log", ...}` entries
/// to `out`. Returns the new cursor position (or `pos` if file missing).
async fn scan_log_file(
    path: &std::path::Path,
    pos: u64,
    stage: Option<&str>,
    out: &mut Vec<serde_json::Value>,
) -> u64 {
    use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
    let Ok(mut f) = tokio::fs::File::open(path).await else { return pos };
    let Ok(meta) = f.metadata().await else { return pos };
    let len = meta.len();
    let start = if len < pos { 0 } else { pos };
    if len == start { return len; }
    if f.seek(SeekFrom::Start(start)).await.is_err() { return pos; }
    let mut buf = String::new();
    let _ = f.read_to_string(&mut buf).await;
    for line in buf.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() { continue; }
        let Ok(parsed) = serde_json::from_str::<serde_json::Value>(trimmed) else { continue };
        let sid = parsed.get("sid").and_then(|x| x.as_str()).unwrap_or("");
        if let Some(s) = stage {
            if sid != s { continue; }
        }
        // Reshape: keep all original fields, drop "type" (not used in this stream),
        // add kind:"log".
        let mut obj = match parsed {
            serde_json::Value::Object(m) => m,
            _ => continue,
        };
        obj.remove("type");
        obj.insert("kind".into(), serde_json::Value::String("log".into()));
        out.push(serde_json::Value::Object(obj));
    }
    len
}

/// Walk `<session>/logs/` for `<stage>_errors.log` files, read newly-appended
/// bytes, parse each line, append normalized `{kind:"error", ...}` entries to
/// `out`. Picks up new files that appeared since the last call (so a stage
/// emitting its first error mid-follow gets surfaced). Returns nothing — the
/// `cursors` map is updated in-place.
async fn scan_errors_dir(
    dir: &std::path::Path,
    stage: Option<&str>,
    cursors: &mut std::collections::HashMap<std::path::PathBuf, u64>,
    out: &mut Vec<serde_json::Value>,
) {
    use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};

    // Decide which files to inspect.
    let candidates: Vec<std::path::PathBuf> = if let Some(s) = stage {
        let p = dir.join(format!("{s}_errors.log"));
        if p.is_file() { vec![p] } else { vec![] }
    } else {
        let mut v = Vec::new();
        let Ok(mut rd) = tokio::fs::read_dir(dir).await else { return };
        while let Ok(Some(entry)) = rd.next_entry().await {
            let p = entry.path();
            let name = match p.file_name().and_then(|s| s.to_str()) {
                Some(n) => n,
                None => continue,
            };
            if name.ends_with("_errors.log") { v.push(p); }
        }
        v
    };

    for path in candidates {
        let pos = cursors.get(&path).copied().unwrap_or(0);
        let Ok(mut f) = tokio::fs::File::open(&path).await else { continue };
        let Ok(meta) = f.metadata().await else { continue };
        let len = meta.len();
        let start = if len < pos { 0 } else { pos };
        if len == start {
            cursors.insert(path, len);
            continue;
        }
        if f.seek(SeekFrom::Start(start)).await.is_err() { continue }
        let mut buf = String::new();
        let _ = f.read_to_string(&mut buf).await;
        for line in buf.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() { continue; }
            let Ok(parsed) = serde_json::from_str::<serde_json::Value>(trimmed) else { continue };
            let mut obj = match parsed {
                serde_json::Value::Object(m) => m,
                _ => continue,
            };
            // Drop the on-disk `type:"error"` discriminator and replace
            // with `kind:"error"` for output stream consistency.
            obj.remove("type");
            obj.insert("kind".into(), serde_json::Value::String("error".into()));
            out.push(serde_json::Value::Object(obj));
        }
        cursors.insert(path, len);
    }
}

/// True when the session has finalized journal.json with a non-Running
/// state. Used to (a) decide whether `--follow` should auto-degrade at
/// start, and (b) detect end-of-run during follow loops. Returns false
/// for missing/malformed journals — treat as still-running.
async fn is_session_terminal(session: &std::path::Path) -> bool {
    let path = session.join("journal.json");
    let Ok(text) = tokio::fs::read_to_string(&path).await else { return false };
    let Ok(j) = serde_json::from_str::<dpe::journal::Journal>(&text) else { return false };
    use dpe::journal::JournalState as S;
    matches!(j.state, S::Succeeded | S::Partial | S::Failed | S::Killed)
}

fn emit_event(value: serde_json::Value) {
    if let Ok(s) = serde_json::to_string(&value) {
        // println! is line-buffered when stdout is a terminal, fully
        // buffered when piped. Editors reading our stdout depend on
        // line-buffering; force a flush on every event.
        println!("{}", s);
        let _ = std::io::Write::flush(&mut std::io::stdout());
    }
}

fn cmd_journal(session: PathBuf) -> anyhow::Result<()> {
    let journal = dpe::journal::rebuild_from_disk(&session)?;
    let body = serde_json::to_vec_pretty(&journal)?;
    let out = session.join("journal.json");
    std::fs::write(&out, body)?;
    println!("[OK] rebuilt {}", out.display());
    println!("  state={:?}  stages={}  envelopes={}  errors={}",
        journal.state,
        journal.stages.len(),
        journal.totals.envelopes_observed,
        journal.totals.errors);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn cmd_run(
    target: String,
    input: PathBuf,
    output: PathBuf,
    cache: Option<String>,
    temp_dir: Option<PathBuf>,
    storage_dir: Option<PathBuf>,
    seed: Option<String>,
    seed_file: Option<PathBuf>,
    json: bool,
    stats_poll_ms: Option<u64>,
    debug_capture: Option<String>,
    cfg: &dpe::RunnerConfig,
) -> anyhow::Result<()> {
    use dpe::types::CacheMode;

    // Reject CLI args that start with `$` — they are almost certainly
    // a misused DPE path-prefix. Path prefixes (`$temp`, `$storage`,
    // ...) substitute INSIDE settings strings only; on the CLI they
    // arrive as literal text. A literal `$temp` then cascades into
    // DPE_TEMP/DPE_STORAGE for spawned tools, downstream tools resolve
    // it as a relative path against their own CWD, and extracted
    // files land in completely wrong places. Loud reject + hint.
    reject_prefix_arg("--temp-dir", temp_dir.as_deref())?;
    reject_prefix_arg("--storage-dir", storage_dir.as_deref())?;

    let (pipeline_path, variant_name) = parse_target(&target)?;
    let (pipeline_dir, pipeline_name) = resolve_pipeline_dir_and_name(&pipeline_path)?;

    let resolved = dpe::load_variant(&pipeline_dir, &pipeline_name, &variant_name)?;
    if let Err(errs) = dpe::validate::validate(&resolved, &pipeline_dir, cfg) {
        for e in &errs { eprintln!("[ERR] {}", e); }
        anyhow::bail!("{} validation error(s)", errs.len());
    }

    let cache_mode = match cache.as_deref() {
        Some("use") | None => CacheMode::Use,
        Some("refresh") => CacheMode::Refresh,
        Some("bypass") => CacheMode::Bypass,
        Some("off") => CacheMode::Off,
        Some(other) => anyhow::bail!("unknown --cache mode: {}", other),
    };

    let session_id = dpe::env::new_session_id();
    let ctx = dpe::env::SessionContext {
        pipeline_dir: pipeline_dir.clone(),
        pipeline_name,
        variant: variant_name.clone(),
        session_id: session_id.clone(),
        input: input.clone(),
        output: output.clone(),
        cache_mode,
        temp_override: temp_dir,
        storage_override: storage_dir,
    };
    std::fs::create_dir_all(&output)?;

    // ─── Emit `started` event (machine-readable mode) or text banner ──
    // We do this here, AFTER ctx + output dir are settled but BEFORE
    // any stage spawn, so callers (editors, automation) see session
    // info immediately. The control socket address is deterministic
    // from session_id + platform, computed by `control_addr_for`.
    //
    // Strip the Windows `\\?\` verbatim prefix from session_dir; it's
    // technically correct but trips downstream consumers that don't
    // expect it. Internal code keeps the canonical form via ctx.
    let session_dir = strip_unc_prefix(ctx.session_dir());
    let control_addr = control_addr_for(&session_dir, &session_id);
    if json {
        let started = serde_json::json!({
            "event":         "started",
            "sessionId":     session_id,
            "sessionDir":    session_dir.to_string_lossy(),
            "controlAddr":   control_addr,
            "pid":           std::process::id(),
            "pipeline":      resolved.pipeline,
            "variant":       resolved.variant,
        });
        println!("{}", serde_json::to_string(&started)?);
    } else {
        println!("sessionId:     {session_id}");
        println!("sessionDir:    {}", session_dir.display());
        println!("controlAddr:   {control_addr}");
        println!("pid:           {}", std::process::id());
        println!("pipeline:      {}", resolved.pipeline);
        println!("variant:       {}", resolved.variant);
        println!();
    }

    // Resolve the seed input. Priority:
    //   1. --seed <json>          (v2.0.1) — JSON object treated as the
    //                              `v` field of a single envelope. dpe
    //                              wraps it as `{t:"d", id:<hash>, src:"seed", v:<input>}`
    //                              and writes to <session>/_seed.ndjson.
    //                              Power users can pass an already-wrapped
    //                              envelope (`{"t":"d","v":{...},...}`); we
    //                              detect the `t` field and pass through
    //                              unchanged.
    //   2. --seed-file <path>     (v2.0.1) — file with one JSON object
    //                              per line. Same per-line wrap logic as
    //                              --seed. Output is staged to
    //                              <session>/_seed.ndjson so dpe owns the
    //                              read order.
    //   3. <input>/_seed.ndjson   (legacy convention) — feed the seed file.
    //   4. <input> if it's a file — feed it directly.
    //   5. Empty                  — pipelines that need no seed (scan-fs etc.).
    // Build the path resolver once — used to expand $input / $output /
    // $session / $temp / $storage / $configs in seed envelopes' v.path
    // and other fields. Lets seed files travel between hosts without
    // hardcoding absolute paths.
    let seed_resolver = dpe::paths::PathResolver::from_map(ctx.prefix_map());

    let input_source = if let Some(seed_line) = seed {
        let session_dir = ctx.session_dir();
        std::fs::create_dir_all(&session_dir)?;
        let seed_path = session_dir.join("_seed.ndjson");
        let line = wrap_seed_line(&seed_line, &seed_resolver)
            .map_err(|e| anyhow::anyhow!("--seed: {e}"))?;
        std::fs::write(&seed_path, line)?;
        dpe::dag::InputSource::File(seed_path)
    } else if let Some(seed_path) = seed_file {
        if !seed_path.is_file() {
            anyhow::bail!("--seed-file does not point to a regular file: {}",
                seed_path.display());
        }
        // Read each line, wrap if needed, write to <session>/_seed.ndjson.
        // We don't feed the user's file directly — that way dpe owns the
        // canonical seed location and downstream tools can find it
        // without knowing where the user staged the original.
        let session_dir = ctx.session_dir();
        std::fs::create_dir_all(&session_dir)?;
        let dest = session_dir.join("_seed.ndjson");
        let raw = std::fs::read_to_string(&seed_path)
            .map_err(|e| anyhow::anyhow!("read --seed-file {}: {e}", seed_path.display()))?;
        let stripped = dpe::bom::strip_bom(&raw);
        let mut wrapped = String::new();
        for (i, line) in stripped.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.is_empty() { continue; }
            let wrapped_line = wrap_seed_line(trimmed, &seed_resolver)
                .map_err(|e| anyhow::anyhow!("--seed-file line {}: {e}", i + 1))?;
            wrapped.push_str(&wrapped_line);
        }
        std::fs::write(&dest, wrapped)?;
        dpe::dag::InputSource::File(dest)
    } else if input.is_file() {
        dpe::dag::InputSource::File(input.clone())
    } else if input.is_dir() {
        let seed = input.join("_seed.ndjson");
        if seed.is_file() {
            dpe::dag::InputSource::File(seed)
        } else {
            dpe::dag::InputSource::Empty
        }
    } else {
        dpe::dag::InputSource::Empty
    };

    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    let report = rt.block_on(async {
        // StatsCollector + StateCollector — ALWAYS created (regardless of
        // --stats) so cmd_run can emit a start snapshot (all stages
        // Pending) and a terminal snapshot (final states) on every run.
        // Editor consumers depend on per-stage events to drive the graph
        // state machine; without start+end snapshots a fast pipeline
        // would finish before any periodic tick fires and the editor
        // would never see stage transitions. --stats only controls the
        // PERIODIC mid-run snapshots; start+end are unconditional.
        let stats_coll = dpe::stderr::StatsCollector::new();
        let state_coll = dpe::state::StateCollector::new();

        // Stage list in topological order — needed by the emitter so the
        // wire snapshot has a consistent set of keys, even for stages
        // that never see any envelope (Pending throughout).
        let all_sids: Vec<String> = resolved.stages.keys().cloned().collect();

        // Initial snapshot. Every stage = pending, all counters zero.
        // This lets the editor populate the graph topology and node
        // initial states before envelopes start flowing.
        emit_stats_snapshot(&all_sids, &stats_coll.snapshot(), &state_coll.snapshot(), json);

        // Periodic emitter — only when --stats interval was passed. The
        // editor's RunModal default is 250ms; CLI users who didn't ask
        // for periodic stats still get the start + end snapshots above
        // and below.
        let emitter = stats_poll_ms.map(|ms| {
            let stats = stats_coll.clone();
            let state = state_coll.clone();
            let sids = all_sids.clone();
            let interval = std::time::Duration::from_millis(ms.max(10));
            let json_mode = json;
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                // Skip the immediate first tick — first tick is at t=0,
                // before any counters change; re-tick gives us real data.
                ticker.tick().await;
                loop {
                    ticker.tick().await;
                    emit_stats_snapshot(&sids, &stats.snapshot(), &state.snapshot(), json_mode);
                }
            })
        });

        // Default: silently discard terminal stages' stdout. Tools that
        // need to deliver output write their own files via settings.
        // `--debug-capture[=stage1,stage2]` opts into writing those
        // streams to `<session>/debug/<stage>.ndjson` for inspection
        // when a stage emits envelopes that aren't otherwise consumed.
        let output_sink = match debug_capture.as_deref() {
            None => dpe::dag::OutputSink::Discard,
            Some("") => dpe::dag::OutputSink::DebugDir {
                dir: ctx.session_dir().join("debug"),
                stages: None,
            },
            Some(list) => {
                let stages: std::collections::HashSet<String> = list
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                dpe::dag::OutputSink::DebugDir {
                    dir: ctx.session_dir().join("debug"),
                    stages: if stages.is_empty() { None } else { Some(stages) },
                }
            }
        };
        // `output` PathBuf is intentionally NOT consumed here — it stays
        // wired into SessionContext above so tools resolve `$output/...`
        // to the user-supplied dir for their own writes.
        let _ = &output;

        let report = dpe::dag::run_variant_with_stats(
            &resolved,
            &pipeline_dir,
            &ctx,
            cfg,
            input_source,
            output_sink,
            Some(stats_coll.clone()),
            Some(state_coll.clone()),
        ).await;

        // Final terminal snapshot — ALWAYS emitted, regardless of --stats.
        // run_variant_with_stats's end-of-run reconciliation has already
        // collapsed any non-terminal stages to Succeeded/Failed at this
        // point, so this snapshot is the authoritative final state.
        if let Some(h) = emitter { h.abort(); }
        emit_stats_snapshot(&all_sids, &stats_coll.snapshot(), &state_coll.snapshot(), json);

        report
    }).map_err(|e| anyhow::anyhow!("dag: {}", e))?;

    if json {
        let summary = serde_json::json!({
            "event":            "summary",
            "sessionId":        session_id,
            "sessionDir":       session_dir.to_string_lossy(),
            "pipeline":         resolved.pipeline,
            "variant":          resolved.variant,
            "stagesRun":        report.stages_run,
            "stagesSucceeded":  report.stages_succeeded,
            "stagesFailed":     report.stages_failed,
            "durationMs":       report.duration_ms,
        });
        println!("{}", serde_json::to_string(&summary)?);
    } else {
        println!("[OK] {}:{} — {} stage(s), {} succeeded, {} failed, {}ms",
            resolved.pipeline, resolved.variant,
            report.stages_run, report.stages_succeeded, report.stages_failed,
            report.duration_ms);
    }
    Ok(())
}

/// Print one per-stage counter + state snapshot. Format depends on `--json`.
///
/// JSON shape (v2.0.2):
///   `{"event":"stats","t":<ms>,"stages":{
///       "<sid>":["<state>",rows_in,rows_out,meta,errors],
///       ...
///   }}`
///
/// `state` is one of: pending / running / succeeded / failed / cancelled.
/// Pending and running are derived from rows activity if no terminal
/// transition has been recorded yet.
///
/// Text shape: `[stats t=<ms>] scan: pending 0→0 e0; …`
///
/// stdout flushes per-line so consumers reading the pipe see snapshots
/// promptly (tokio's default buffered stdout would otherwise hold them).
///
/// `all_sids` ensures every topologically-known stage appears in the
/// payload, even if it has zero activity — keeps the wire schema stable
/// from the editor's perspective.
fn emit_stats_snapshot(
    all_sids: &[String],
    counters: &std::collections::BTreeMap<String, dpe::stderr::StageCounters>,
    states:   &std::collections::BTreeMap<String, dpe::state::StageState>,
    json: bool,
) {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    // Resolve state per stage: explicit (terminal) wins; otherwise
    // derive Pending/Running from any non-zero counter.
    let resolve = |sid: &str, c: &dpe::stderr::StageCounters| -> dpe::state::StageState {
        match states.get(sid).copied() {
            Some(s) => s,
            None if c.rows_in > 0 || c.rows_out > 0 || c.errors > 0
                => dpe::state::StageState::Running,
            None => dpe::state::StageState::Pending,
        }
    };

    if json {
        let mut stages = serde_json::Map::new();
        for sid in all_sids {
            let c = counters.get(sid).cloned().unwrap_or_default();
            let st = resolve(sid, &c);
            stages.insert(
                sid.clone(),
                serde_json::json!([
                    st.as_str(),
                    c.rows_in, c.rows_out, c.meta, c.errors,
                ]),
            );
        }
        let line = serde_json::json!({
            "event":  "stats",
            "t":      t,
            "stages": stages,
        });
        println!("{line}");
    } else {
        let mut s = format!("[stats t={t}]");
        let mut first = true;
        for sid in all_sids {
            let c = counters.get(sid).cloned().unwrap_or_default();
            let st = resolve(sid, &c);
            if first { s.push(' '); first = false; }
            s.push_str(&format!(
                "{sid}: {} {}→{} m{} e{}; ",
                st.as_str(), c.rows_in, c.rows_out, c.meta, c.errors,
            ));
        }
        println!("{s}");
    }
    // Force flush — child PIPEs are fully-buffered by default.
    let _ = std::io::Write::flush(&mut std::io::stdout());
}

/// Compute the runner ↔ CLI control socket address for a session. This
/// matches the address the runner writes to `<session>/control.addr` at
/// startup, but we derive it deterministically here so the `started`
/// event can include it before the runner has booted.
///
/// Windows: named pipe `\\.\pipe\dpe-<session_id>` (interprocess crate
/// convention). Unix: UDS at `<session>/control.sock`.
fn control_addr_for(session_dir: &std::path::Path, session_id: &str) -> String {
    if cfg!(windows) {
        format!(r"\\.\pipe\dpe-{session_id}")
    } else {
        session_dir.join("control.sock").to_string_lossy().into_owned()
    }
}

/// Wrap a user-provided JSON object as a full dpe envelope, unless it
/// already has a `t` field (in which case the user supplied a complete
/// envelope and we pass through unchanged).
///
/// For simple use the user just provides the `v` part:
///     --seed '{"path":"data/in/file.txt"}'
/// → envelope: `{"t":"d","id":"<hash>","src":"seed","v":{"path":"data/in/file.txt"}}`
///
/// Power users who want to control id/src can supply the full envelope:
///     --seed '{"t":"d","id":"e1","src":"seed","v":{"path":"..."}}'
/// → emitted as-is.
///
/// Returns the wrapped envelope as a serde_json Value. Callers serialize
/// (after optional `$prefix` expansion via PathResolver — see
/// `expand_seed_prefixes`).
fn wrap_seed_value(line: &str) -> anyhow::Result<serde_json::Value> {
    use serde_json::Value;
    let parsed: Value = serde_json::from_str(line)
        .map_err(|e| anyhow::anyhow!("not valid JSON: {e}"))?;
    let obj = parsed.as_object().ok_or_else(||
        anyhow::anyhow!("seed JSON must be an object, got: {parsed}"))?;

    if obj.contains_key("t") {
        // Already-wrapped envelope. Pass through unchanged — power users
        // who include `t` explicitly are taking responsibility for the
        // full shape (id, src, etc.).
        Ok(parsed)
    } else {
        // Wrap as data envelope. id is a deterministic 16-char blake2b
        // of the v JSON so re-runs of the same seed produce the same id —
        // useful for cache lookups and trace stability.
        use blake2::{Blake2b, Digest, digest::consts::U8};
        let v_canonical = serde_json::to_string(&parsed)?;
        let mut hasher: Blake2b<U8> = Blake2b::new();
        hasher.update(v_canonical.as_bytes());
        let hash = hasher.finalize();
        let id = hex::encode(hash);
        Ok(serde_json::json!({
            "t":   "d",
            "id":  id,
            "src": "seed",
            "v":   parsed,
        }))
    }
}

/// Resolve `$prefix` paths inside a seed envelope's `v` field. Tools
/// like scan-fs interpret `v.path` literally and don't expand prefixes
/// themselves — dpe pre-expands here so seed files can be authored
/// portably (`{"path":"$input/some/dir"}`) instead of hardcoding host-
/// specific absolute paths.
fn expand_seed_prefixes(
    mut envelope: serde_json::Value,
    resolver: &dpe::paths::PathResolver,
) -> anyhow::Result<serde_json::Value> {
    if let Some(obj) = envelope.as_object_mut() {
        if let Some(v) = obj.get_mut("v") {
            *v = resolver.resolve_in_value(v)
                .map_err(|e| anyhow::anyhow!("resolving $prefix in seed: {e}"))?;
        }
    }
    Ok(envelope)
}

/// Convenience: wrap → expand → serialize → append newline. The full
/// chain that the cmd_run seed-writing step needs.
fn wrap_seed_line(
    line: &str,
    resolver: &dpe::paths::PathResolver,
) -> anyhow::Result<String> {
    let envelope = wrap_seed_value(line)?;
    let expanded = expand_seed_prefixes(envelope, resolver)?;
    let mut out = serde_json::to_string(&expanded)?;
    out.push('\n');
    Ok(out)
}

fn cmd_check(
    target: String,
    all: bool,
    plan: bool,
    allow_undefined_env: bool,
    cfg: &dpe::RunnerConfig,
) -> anyhow::Result<()> {
    let (pipeline_path, variant_name) = parse_target(&target)?;
    let (resolved_dir, pipeline_name) = resolve_pipeline_dir_and_name(&pipeline_path)?;
    let pipeline_dir = resolved_dir.as_path();

    if all {
        if plan {
            anyhow::bail!("--plan and --all are mutually exclusive; pick one variant");
        }
        check_all_variants(pipeline_dir, &pipeline_name, allow_undefined_env, cfg)
    } else {
        check_one(pipeline_dir, &pipeline_name, &variant_name, plan, allow_undefined_env, cfg)
    }
}

fn env_for_check(allow_undefined_env: bool) -> Box<dyn dpe::env_interp::EnvLookup> {
    if allow_undefined_env {
        Box::new(dpe::env_interp::AllowUndefinedEnv)
    } else {
        Box::new(dpe::env_interp::ProcessEnv)
    }
}

fn check_one(
    dir: &std::path::Path,
    pipeline: &str,
    variant: &str,
    print_plan: bool,
    allow_undefined_env: bool,
    cfg: &dpe::RunnerConfig,
) -> anyhow::Result<()> {
    let resolved = dpe::load_variant(dir, pipeline, variant)?;
    let env = env_for_check(allow_undefined_env);

    match dpe::validate::validate_with_env(&resolved, dir, cfg, env.as_ref()) {
        Ok(()) => {
            // With --plan, stdout is the JSON plan and nothing else — so
            // `dpe check :v --plan | jq` just works. Without --plan we keep
            // the human-readable [OK] banner. The OK status line still goes
            // to stderr in --plan mode so terminals show success.
            if print_plan {
                eprintln!("[OK] {}:{} — {} stage(s) resolved + validated",
                    pipeline, variant, resolved.stages.len());
                print_execution_plan(&resolved, dir, cfg, env.as_ref())?;
            } else {
                println!("[OK] {}:{} — {} stage(s) resolved + validated",
                    pipeline, variant, resolved.stages.len());
            }
            Ok(())
        }
        Err(errs) => {
            for e in &errs { eprintln!("[ERR] {}", e); }
            anyhow::bail!("{} validation error(s)", errs.len())
        }
    }
}

/// Compile and print the ExecutionPlan as pretty JSON. `$session` paths
/// remain literal — they are bound only at run time. `$input/$output/etc.`
/// stay literal too unless overridden by future flags.
fn print_execution_plan(
    resolved:     &dpe::ResolvedVariant,
    pipeline_dir: &std::path::Path,
    cfg:          &dpe::RunnerConfig,
    env:          &dyn dpe::env_interp::EnvLookup,
) -> anyhow::Result<()> {
    let static_resolver = dpe::paths::PathResolver::default();
    let plan = dpe::dag::plan::compile_with_env(resolved, pipeline_dir, cfg, &static_resolver, env)
        .map_err(|e| anyhow::anyhow!("plan compile: {}", e))?;
    let json = serde_json::to_string_pretty(&plan)?;
    println!("{}", json);
    Ok(())
}

fn check_all_variants(
    dir: &std::path::Path,
    pipeline: &str,
    allow_undefined_env: bool,
    cfg: &dpe::RunnerConfig,
) -> anyhow::Result<()> {
    let variants_dir = dir.join("variants");
    if !variants_dir.exists() {
        anyhow::bail!("variants directory not found: {}", variants_dir.display());
    }
    let mut checked = 0;
    let mut errors = Vec::new();
    for entry in std::fs::read_dir(&variants_dir)? {
        let p = entry?.path();
        let ext = p.extension().and_then(|s| s.to_str()).unwrap_or("");
        if !["yaml", "yml", "json"].contains(&ext) { continue; }
        let name = p.file_stem().and_then(|s| s.to_str()).unwrap_or("").to_string();
        match check_one(dir, pipeline, &name, false, allow_undefined_env, cfg) {
            Ok(()) => checked += 1,
            Err(e) => { errors.push(format!("{}: {}", name, e)); }
        }
    }
    if errors.is_empty() {
        println!("[OK] {} variant(s) checked", checked);
        Ok(())
    } else {
        for e in &errors { eprintln!("{}", e); }
        anyhow::bail!("{} variant(s) failed validation", errors.len());
    }
}

/// Resolve a user-supplied pipeline path into (canonical absolute path, name).
///
/// Canonicalising is what makes `dpe run .:main` work from inside a pipeline
/// directory — `.` becomes the absolute CWD and `file_name()` then yields the
/// directory's basename. Bare relative names like `test-1` (where the dir
/// exists in CWD) also resolve, returning the absolute path so downstream
/// `pipeline.exists()` checks aren't ambiguous about where they're looking.
///
/// On failure (path doesn't resolve), we fall back to the raw input so the
/// caller's existing "pipeline folder not found" error path still fires with
/// a sensible message.
fn resolve_pipeline_dir_and_name(pipeline_path: &str) -> anyhow::Result<(PathBuf, String)> {
    let raw = PathBuf::from(pipeline_path);
    // Strip the `\\?\` UNC prefix that std::fs::canonicalize introduces
    // on Windows. Without this strip, the prefix cascades through
    // PathResolver into every settings path the tool sees — e.g.
    // classify's `$configs/.../rules.js` arrives as `\\?\D:\...js`,
    // and Bun/Node `import()` chokes on that prefix. See inbox 0005.
    let canonical = strip_unc_prefix(
        std::fs::canonicalize(&raw).unwrap_or_else(|_| raw.clone()),
    );
    let name = canonical
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow::anyhow!(
            "cannot derive pipeline name from path: {}", pipeline_path
        ))?
        .to_string();
    Ok((canonical, name))
}

fn parse_target(s: &str) -> anyhow::Result<(String, String)> {
    // Split on LAST ':' so Windows drive letters (C:\path:variant) work.
    match s.rsplit_once(':') {
        Some((p, v)) if !v.contains(['/', '\\']) && !v.is_empty() => Ok((p.into(), v.into())),
        _ => Ok((s.into(), "main".into())),
    }
}

// ═══ install / tools list / config ═════════════════════════════════════════

fn cmd_install(cfg: &dpe::RunnerConfig, name: &str, force: bool) -> anyhow::Result<()> {
    let dir = dpe::install::install(cfg, name, force)?;
    println!("[OK] installed {} at {}", name, dir.display());
    Ok(())
}

/// One entry in the tool listing. Tier values:
///   - `standard` — from a catalog entry that didn't override tier.
///   - `pipeline-local` — discovered under a `tools_paths` entry whose
///     parent directory contains `pipeline.toml`.
///   - `external` — discovered under any other `tools_paths` entry.
struct ToolListEntry {
    name:               String,
    tier:               String,
    runtime:            String,
    version:            Option<String>,
    description:        Option<String>,
    source:             Option<String>,
    installed:          bool,
    settings_schema:    Option<serde_json::Value>,
    output_description: Option<String>,
}

struct BuiltinEntry {
    name:        &'static str,
    description: &'static str,
}

/// The complete set of in-process builtins recognised by the runner.
/// Kept sorted by frequency-of-use, not alphabetical, so the most
/// common ones are top-of-list in the text output.
const BUILTINS: &[BuiltinEntry] = &[
    BuiltinEntry { name: "route",
        description: "Fan envelopes out to named channels by expression — first-truthy-wins" },
    BuiltinEntry { name: "spread",
        description: "Broadcast every envelope to ALL downstream consumers (1→N tee)" },
    BuiltinEntry { name: "filter",
        description: "Drop or divert envelopes that fail a boolean expression" },
    BuiltinEntry { name: "dedup",
        description: "Drop envelopes whose key has been seen before (persistent index)" },
    BuiltinEntry { name: "group_by",
        description: "Group envelopes by key and emit aggregates on trigger" },
    BuiltinEntry { name: "toggle",
        description: "Env-gated 1→1 passthrough; pass-all or drop-all per env match" },
];

/// Tier detection for a discovered tool directory: a tools_path whose
/// parent contains `pipeline.toml` is "pipeline-local"; otherwise
/// "external". Catalog entries override via their own `tier` field.
fn tier_for_discovered_path(tools_path: &std::path::Path) -> &'static str {
    let parent = tools_path.parent();
    if parent.map(|p| p.join("pipeline.toml").is_file()).unwrap_or(false) {
        "pipeline-local"
    } else {
        "external"
    }
}

/// Build the unified tool listing used by both text and JSON output.
/// Pass 1 walks the catalog (preserving its declared tier); pass 2
/// discovers tools_paths-only entries and assigns tier from their
/// parent's marker file. First match wins on name conflict (catalog
/// beats discovery).
fn gather_tool_listing(
    catalog:         &dpe::catalog::Catalog,
    effective_paths: &[std::path::PathBuf],
) -> (Vec<ToolListEntry>, Vec<std::path::PathBuf>) {
    use serde_json::Value;

    let resolved_paths: Vec<std::path::PathBuf> = effective_paths.iter()
        .map(|p| std::fs::canonicalize(p).unwrap_or_else(|_| p.clone()))
        .map(strip_unc_prefix)
        .collect();

    let mut tools = Vec::new();
    let mut seen = std::collections::BTreeSet::<String>::new();

    // Pass 1 — catalog entries.
    for (name, entry) in &catalog.tools {
        let (source, schema, output_desc) = inspect_tool_dir(name, &resolved_paths);
        let installed = source.is_some();
        tools.push(ToolListEntry {
            name:               name.clone(),
            tier:               entry.tier.clone().unwrap_or_else(|| "standard".into()),
            runtime:            entry.runtime.clone(),
            version:            entry.version.clone(),
            description:        Some(entry.description.clone()).filter(|s| !s.is_empty()),
            source,
            installed,
            settings_schema:    schema,
            output_description: output_desc,
        });
        seen.insert(name.clone());
    }

    // Pass 2 — path-discovered tools missing from every catalog.
    for path in &resolved_paths {
        let Ok(entries) = std::fs::read_dir(path) else { continue };
        for entry in entries.flatten() {
            let p = entry.path();
            if !p.is_dir() { continue; }
            let meta_path = p.join("meta.json");
            let Ok(raw) = std::fs::read_to_string(&meta_path) else { continue };
            let stripped = dpe::bom::strip_bom(&raw);
            let Ok(meta_v): Result<Value, _> = serde_json::from_str(stripped) else { continue };
            let Some(name) = meta_v.get("name").and_then(|v| v.as_str()) else { continue };
            if seen.contains(name) { continue; }
            let runtime = meta_v.get("runtime").and_then(|v| v.as_str())
                .unwrap_or("unknown").to_string();
            let version = meta_v.get("version").and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let description = meta_v.get("description").and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let (schema, output_desc) = read_spec_yaml(&p);
            tools.push(ToolListEntry {
                name:               name.to_string(),
                tier:               tier_for_discovered_path(path).to_string(),
                runtime,
                version,
                description,
                source:             Some(p.to_string_lossy().into_owned()),
                installed:          true,
                settings_schema:    schema,
                output_description: output_desc,
            });
            seen.insert(name.to_string());
        }
    }

    (tools, resolved_paths)
}

/// Sort priority for tier sections in text output: standard first
/// (most common), then pipeline-local, then external, then unknown.
fn tier_order(t: &str) -> u8 {
    match t {
        "standard"       => 0,
        "pipeline-local" => 1,
        "external"       => 2,
        _                => 3,
    }
}

fn cmd_tools_list(cfg: &dpe::RunnerConfig, json: bool) -> anyhow::Result<()> {
    let registries = dpe::catalog::resolve_registries(cfg);
    let catalog = dpe::catalog::Catalog::load_from_files(&registries);
    let layout = dpe::home::Layout::resolve().ok();
    let effective_paths: Vec<std::path::PathBuf> = cfg.tools_paths.iter()
        .map(|p| std::path::PathBuf::from(expand_home(p)))
        .chain(layout.iter().map(|l| l.tools.clone()))
        .collect();

    let (mut tools, resolved_paths) = gather_tool_listing(&catalog, &effective_paths);

    if json {
        return print_tools_list_json(&tools, &registries, &effective_paths, &resolved_paths);
    }

    // Text: tier-sorted, then alphabetical within tier.
    tools.sort_by(|a, b| tier_order(&a.tier).cmp(&tier_order(&b.tier))
        .then_with(|| a.name.cmp(&b.name)));

    println!("{:24} {:8} {:14} {:8} STATUS",
        "NAME", "RUNTIME", "TIER", "VERSION");
    println!("{}", "─".repeat(80));

    if tools.is_empty() {
        println!("(no tools — registry empty AND no meta.json found in tools_paths)");
    }
    for t in &tools {
        let status = if t.installed { "installed" } else { "available (run: dpe install <name>)" };
        println!("{:24} {:8} {:14} {:8} {}",
            truncate(&t.name, 24),
            t.runtime,
            t.tier,
            t.version.as_deref().unwrap_or(""),
            status);
    }

    // Built-in stages — always available, no install needed.
    println!();
    println!("Built-in stages (in-process; no install needed):");
    for b in BUILTINS {
        println!("  {:10} {}", b.name, b.description);
    }

    println!();
    println!("tools_paths searched:");
    for p in &effective_paths { println!("  {}", p.display()); }
    println!();
    println!("tool registries:");
    if registries.is_empty() {
        println!("  (none — set tools_registries in config.toml or place catalog.json next to dpe)");
    } else {
        for r in &registries {
            let mark = if r.exists() { " " } else { "✗" };
            println!("  {} {}", mark, r.display());
        }
    }
    Ok(())
}

/// JSON output for `dpe tools list --json`. The schema is:
///
/// ```text
/// {
///   "version":   "2.0.1",                    // dpe version (single value)
///   "registries": ["...catalog.json", ...],   // resolved & loaded
///   "tools_paths": ["...", ...],              // resolved (after ~ expansion)
///   "builtins":  [{name, description}, ...],  // route, spread, filter, dedup, group_by, toggle
///   "tools": [
///     {
///       "name":        "scan-fs",
///       "tier":        "standard"  | "external"  | "pipeline-local",
///       "runtime":     "rust" | "bun" | "python" | ...,
///       "version":     "2.0.1" | null,
///       "description": "...",
///       "source":      "/abs/path/to/<name>"  // dir holding meta.json+spec.yaml
///                                             // (null when not found on disk)
///       "settings_schema": { ... } | null,    // parsed spec.yaml `settings:`
///       "output_description": "..." | null,   // parsed spec.yaml `output.description`
///       "installed":   true|false             // meta.json reachable in tools_paths?
///     }
///   ]
/// }
/// ```
///
/// Tool sources merge in this priority order; first wins:
///   1. Catalog entries (tier from catalog, default "standard").
///   2. Path-discovered tools — directories under any `tools_paths` entry
///      with a `meta.json`/`spec.yaml` pair, but NOT in any catalog. Tier
///      = "pipeline-local" if the parent dir has `pipeline.toml`, else
///      "external". Lets pipelines pull in tool packs that don't ship a
///      catalog (e.g. dpe-tools) and surface their own local tools too.
///
/// Builtins are listed separately; pipelines consume them transparently
/// — same set as the text output.
fn print_tools_list_json(
    tools:           &[ToolListEntry],
    registries:      &[std::path::PathBuf],
    effective_paths: &[std::path::PathBuf],
    resolved_paths:  &[std::path::PathBuf],
) -> anyhow::Result<()> {
    use serde_json::{json, Value};

    let tools_json: Vec<Value> = tools.iter().map(|t| json!({
        "name":               t.name,
        "tier":               t.tier,
        "runtime":            t.runtime,
        "version":            t.version,
        "description":        t.description,
        "source":             t.source,
        "settings_schema":    t.settings_schema,
        "output_description": t.output_description,
        "installed":          t.installed,
    })).collect();

    let registries_arr: Vec<String> = registries.iter()
        .map(|r| r.to_string_lossy().into_owned()).collect();
    // Echo the resolved (absolute) tools_paths so consumers see the
    // same paths embedded in tool sources.
    let paths_arr: Vec<String> = if resolved_paths.is_empty() {
        effective_paths.iter().map(|p| p.to_string_lossy().into_owned()).collect()
    } else {
        resolved_paths.iter().map(|p| p.to_string_lossy().into_owned()).collect()
    };

    let builtins_json: Vec<Value> = BUILTINS.iter().map(|b| json!({
        "name":        b.name,
        "description": b.description,
    })).collect();

    let payload = json!({
        "version":     env!("CARGO_PKG_VERSION"),
        "registries":  registries_arr,
        "tools_paths": paths_arr,
        "builtins":    builtins_json,
        "tools":       tools_json,
    });

    let s = serde_json::to_string_pretty(&payload)?;
    println!("{}", s);
    Ok(())
}

/// Reject CLI path args that start with `$`. DPE path prefixes
/// (`$temp`, `$storage`, ...) substitute inside SETTINGS strings only,
/// not at the CLI. A literal `$temp` passed via `--temp-dir`
/// propagates as DPE_TEMP="$temp" to spawned tools, and any tool that
/// resolves a settings path against that env var ends up writing to
/// `$temp/...` (a literal directory under its own cwd). See
/// .agents/inbox/0011 for a real-world incident where this corrupted
/// extraction.
fn reject_prefix_arg(flag: &str, value: Option<&std::path::Path>) -> anyhow::Result<()> {
    if let Some(p) = value {
        let s = p.to_string_lossy();
        if s.starts_with('$') {
            anyhow::bail!(
                "{} value '{}' starts with '$'.\n\
                 \n\
                 DPE path prefixes (`$temp`, `$storage`, `$session`, ...) substitute\n\
                 inside SETTINGS strings only — at the CLI they pass through literally.\n\
                 \n\
                 Pass an actual filesystem path, OR omit the flag and let the runner\n\
                 use the default (<pipeline>/temp, <pipeline>/storage).\n\
                 \n\
                 Examples:\n\
                 \x20\x20WRONG: dpe run my:main --temp-dir '$temp'\n\
                 \x20\x20OK:    dpe run my:main                          # use default\n\
                 \x20\x20OK:    dpe run my:main --temp-dir D:/scratch    # explicit path",
                flag, s,
            );
        }
    }
    Ok(())
}

/// `std::fs::canonicalize` returns `\\?\…` UNC paths on Windows. The
/// verbatim prefix is technically correct but ugly in JSON output and
/// trips downstream consumers that don't expect it. Strip when present.
fn strip_unc_prefix(path: std::path::PathBuf) -> std::path::PathBuf {
    if cfg!(windows) {
        let s = path.to_string_lossy();
        if let Some(stripped) = s.strip_prefix(r"\\?\") {
            return std::path::PathBuf::from(stripped);
        }
    }
    path
}

/// Find the on-disk dir for `name` under any `tools_paths` entry and read
/// its meta.json+spec.yaml. Returns (source_dir, settings_schema, output_description).
fn inspect_tool_dir(
    name:  &str,
    paths: &[std::path::PathBuf],
) -> (Option<String>, Option<serde_json::Value>, Option<String>) {
    for p in paths {
        let dir = p.join(name);
        if dir.join("meta.json").is_file() {
            let (schema, output_desc) = read_spec_yaml(&dir);
            return (Some(dir.to_string_lossy().into_owned()), schema, output_desc);
        }
    }
    (None, None, None)
}

/// Read `<dir>/spec.yaml` and extract `settings` (JSON Schema) +
/// `output.description`. Returns (None, None) if the file is missing or
/// fails to parse — never errors.
fn read_spec_yaml(dir: &std::path::Path) -> (Option<serde_json::Value>, Option<String>) {
    let path = dir.join("spec.yaml");
    let Ok(raw) = std::fs::read_to_string(&path) else { return (None, None) };
    let opts = serde_saphyr::options!(
        strict_booleans:      true,
        no_schema:            true,
        legacy_octal_numbers: false,
    );
    let parsed: serde_json::Value = match serde_saphyr::from_str_with_options(
        dpe::bom::strip_bom(&raw), opts,
    ) {
        Ok(v) => v,
        Err(_) => return (None, None),
    };
    let schema = parsed.get("settings").cloned();
    let output_desc = parsed.get("output")
        .and_then(|o| o.get("description"))
        .and_then(|d| d.as_str())
        .map(|s| s.to_string());
    (schema, output_desc)
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max { s.to_string() } else { format!("{}…", &s[..max-1]) }
}

fn expand_home(p: &str) -> String {
    if let Some(rest) = p.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(rest).to_string_lossy().into_owned();
        }
    }
    p.to_string()
}

fn cmd_config_show(cfg: &dpe::RunnerConfig) -> anyhow::Result<()> {
    let s = toml::to_string_pretty(cfg)?;
    println!("{}", s);
    Ok(())
}

fn cmd_config_path() -> anyhow::Result<()> {
    match dpe::config::default_config_path() {
        Some(p) => println!("{}", p.display()),
        None    => println!("(no config path resolvable)"),
    }
    Ok(())
}

/// `dpe config init` — bootstrap ~/.dpe/config.toml + tools/ + registries/.
/// Refuses to overwrite an existing config unless `--force` is given. The
/// generated config is intentionally empty (default values for everything)
/// so subsequent `dpe install <name>` and `dpe tools list` work without
/// manual editing.
fn cmd_config_init(force: bool) -> anyhow::Result<()> {
    let cfg_path = dpe::config::home_config_path()
        .ok_or_else(|| anyhow::anyhow!("cannot determine home config path"))?;
    let dpe_dir = cfg_path.parent()
        .ok_or_else(|| anyhow::anyhow!("home config path has no parent"))?
        .to_path_buf();

    std::fs::create_dir_all(&dpe_dir)?;
    std::fs::create_dir_all(dpe_dir.join("tools"))?;
    std::fs::create_dir_all(dpe_dir.join("registries"))?;

    if cfg_path.exists() && !force {
        println!("[config] already exists: {} (pass --force to overwrite)",
            cfg_path.display());
        return Ok(());
    }

    // Default config — empty arrays, all-defaults sub-tables. The runner
    // resolves `<binary-dir>/catalog.json` automatically when registries
    // is empty, so `dpe tools list` works on a fresh install without
    // additional steps.
    let cfg = dpe::RunnerConfig::default();
    dpe::config::save(&cfg_path, &cfg)?;
    println!("[config] wrote: {}", cfg_path.display());
    println!("[config]   ↳ {}", dpe_dir.join("tools").display());
    println!("[config]   ↳ {}", dpe_dir.join("registries").display());
    println!("[config] try: dpe install <name>   |   dpe tools list");
    Ok(())
}

fn cmd_config_add_path(path: &std::path::Path) -> anyhow::Result<()> {
    let abs = if path.is_absolute() { path.to_path_buf() }
              else { std::env::current_dir()?.join(path) };

    let cfg_path = dpe::config::home_config_path()
        .ok_or_else(|| anyhow::anyhow!("cannot determine home config path"))?;

    // Ensure parent exists
    if let Some(parent) = cfg_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut cfg = if cfg_path.exists() {
        dpe::config::load(Some(&cfg_path))?
    } else {
        dpe::RunnerConfig::default()
    };

    let canon = abs.to_string_lossy().replace('\\', "/");
    if cfg.tools_paths.iter().any(|p| p == &canon) {
        println!("[config] path already registered: {}", canon);
        return Ok(());
    }
    cfg.tools_paths.push(canon.clone());
    dpe::config::save(&cfg_path, &cfg)?;
    println!("[config] added: {}", canon);
    println!("[config] wrote: {}", cfg_path.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{check_one, reject_prefix_arg, strip_unc_prefix, wrap_seed_line};
    use dpe::paths::PathResolver;
    use serde_json::Value;
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    fn empty_resolver() -> PathResolver {
        PathResolver::from_map(BTreeMap::new())
    }

    fn input_resolver(input: &str) -> PathResolver {
        let mut m = BTreeMap::new();
        m.insert("input".into(), PathBuf::from(input));
        PathResolver::from_map(m)
    }

    #[test]
    fn wraps_v_only_object_into_data_envelope() {
        let r = empty_resolver();
        let line = wrap_seed_line(r#"{"path":"data/in/file.txt"}"#, &r).unwrap();
        let env: Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(env["t"], "d");
        assert_eq!(env["src"], "seed");
        assert_eq!(env["v"]["path"], "data/in/file.txt");
        assert!(env["id"].as_str().unwrap().len() >= 8);
    }

    #[test]
    fn passes_through_envelope_with_t_field() {
        let r = empty_resolver();
        let raw = r#"{"t":"d","id":"e1","src":"upstream","v":{"a":1}}"#;
        let line = wrap_seed_line(raw, &r).unwrap();
        let env: Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(env["t"], "d");
        assert_eq!(env["id"], "e1");
        assert_eq!(env["src"], "upstream");
    }

    #[test]
    fn rejects_non_object_seed() {
        let r = empty_resolver();
        assert!(wrap_seed_line("42", &r).is_err());
        assert!(wrap_seed_line(r#""string""#, &r).is_err());
        assert!(wrap_seed_line("[1,2]", &r).is_err());
    }

    #[test]
    fn rejects_invalid_json() {
        let r = empty_resolver();
        assert!(wrap_seed_line("not json", &r).is_err());
        assert!(wrap_seed_line("{unclosed", &r).is_err());
    }

    #[test]
    fn deterministic_id_for_same_v() {
        let r = empty_resolver();
        let a = wrap_seed_line(r#"{"x":1}"#, &r).unwrap();
        let b = wrap_seed_line(r#"{"x":1}"#, &r).unwrap();
        let ea: Value = serde_json::from_str(a.trim()).unwrap();
        let eb: Value = serde_json::from_str(b.trim()).unwrap();
        assert_eq!(ea["id"], eb["id"]);
    }

    #[test]
    fn expands_input_prefix_in_v_path() {
        // Regression: scan-fs reads v.path literally. dpe must pre-expand
        // $input so seed files can be authored portably.
        let r = input_resolver("/abs/data/in");
        let line = wrap_seed_line(r#"{"path":"$input"}"#, &r).unwrap();
        let env: Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(env["v"]["path"], "/abs/data/in");
    }

    #[test]
    fn expands_input_prefix_with_subpath() {
        let r = input_resolver("/abs/data/in");
        let line = wrap_seed_line(r#"{"path":"$input/customers.csv"}"#, &r).unwrap();
        let env: Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(env["v"]["path"], "/abs/data/in/customers.csv");
    }

    #[test]
    fn unknown_prefix_passes_through_unresolved() {
        // PathResolver returns Ok(None) for unknown $xxx so Mongo
        // operators like $set in v don't fail expansion.
        let r = input_resolver("/abs");
        let line = wrap_seed_line(r#"{"$set":{"a":1}}"#, &r).unwrap();
        let env: Value = serde_json::from_str(line.trim()).unwrap();
        assert!(env["v"]["$set"].is_object());
    }

    // ─── check_one threads --config through (regression: inbox 0001) ─────
    //
    // Pre-fix: `check_one` called `dpe::load_config(None)` internally,
    // ignoring the user's `--config` flag. Pipelines whose tools live
    // outside the global `~/.dpe/config.toml` would fail validation
    // even though `dpe tools list` and `dpe run` worked.
    //
    // The test below scaffolds a pipeline whose tool resolution requires
    // the cfg-supplied tools_paths. Pre-fix this would fail with
    // "tool unresolved"; post-fix it succeeds.

    fn fixtures_tools_dir() -> String {
        let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        p.push("tests");
        p.push("fixtures");
        p.push("tools");
        p.to_string_lossy().into_owned()
    }

    fn write_minimal_pipeline(dir: &std::path::Path, tool_name: &str) {
        std::fs::create_dir_all(dir.join("variants")).unwrap();
        let yaml = format!(
            "pipeline: test-pipe\nvariant: main\nstages:\n  s:\n    tool: {tool}\n    settings: {{}}\n    input: $input\n",
            tool = tool_name,
        );
        std::fs::write(dir.join("variants/main.yaml"), yaml).unwrap();
    }

    #[test]
    fn check_one_uses_supplied_cfg_tools_paths() {
        // Scaffold pipeline that references mock-tool.
        let tmp = tempfile::tempdir().unwrap();
        let pipe_dir = tmp.path().join("test-pipe");
        write_minimal_pipeline(&pipe_dir, "mock-tool");

        // Build cfg pointing at the fixture tools dir.
        let cfg = dpe::RunnerConfig {
            tools_paths: vec![fixtures_tools_dir()],
            ..Default::default()
        };

        // Pre-fix this would fail because check_one ignored the cfg
        // and reloaded from None (which has no tools_paths).
        let r = check_one(&pipe_dir, "test-pipe", "main", false, false, &cfg);
        assert!(r.is_ok(), "expected ok, got: {:?}", r);
    }

    // ─── UNC prefix strip (regression: inbox 0005) ─────────────────────
    //
    // std::fs::canonicalize on Windows produces `\\?\D:\…` extended-
    // length paths. The runner now strips that prefix at the point
    // where the pipeline directory is resolved, so downstream
    // PathResolver-built paths don't carry the prefix into tool
    // settings. Bun/Node `import()` chokes on `\\?\` regardless of
    // whether you wrap it in pathToFileURL — stripping at source is
    // the broad fix.

    #[test]
    fn strip_unc_prefix_removes_windows_extended_prefix() {
        let p = strip_unc_prefix(PathBuf::from(r"\\?\D:\some\path"));
        if cfg!(windows) {
            assert_eq!(p, PathBuf::from(r"D:\some\path"));
        } else {
            // On non-Windows the function is a no-op.
            assert_eq!(p, PathBuf::from(r"\\?\D:\some\path"));
        }
    }

    #[test]
    fn strip_unc_prefix_passes_through_non_unc_paths() {
        let p = strip_unc_prefix(PathBuf::from(r"D:\some\path"));
        assert_eq!(p, PathBuf::from(r"D:\some\path"));
        let q = strip_unc_prefix(PathBuf::from("/usr/local/bin"));
        assert_eq!(q, PathBuf::from("/usr/local/bin"));
    }

    // ─── reject_prefix_arg (regression: 0011 — Bug 1) ──────────────────
    //
    // CLI flags that take filesystem paths must reject a `$`-prefix
    // value. Path prefixes (`$temp`, `$storage`, `$session`, ...)
    // substitute INSIDE settings only; at the CLI they pass through
    // literally and corrupt downstream tool paths. Real-world: led
    // to xlsx writing 4376 hash dirs under
    // `<dpe-tools>/tools/xlsx/$temp/batch_$/raw/` instead of the
    // pipeline temp dir.

    #[test]
    fn reject_prefix_arg_blocks_dollar_temp() {
        let r = reject_prefix_arg("--temp-dir", Some(&PathBuf::from("$temp")));
        assert!(r.is_err());
        let msg = r.unwrap_err().to_string();
        assert!(msg.contains("--temp-dir"), "msg should name the flag: {}", msg);
        assert!(msg.contains("$temp"),       "msg should echo the value: {}", msg);
        // The hint must explain the prefix-vs-CLI distinction. Word the
        // check loosely so cosmetic message tweaks (line breaks, etc)
        // don't break the test.
        assert!(
            msg.contains("substitute") && msg.contains("SETTINGS"),
            "msg should explain prefixes substitute in settings only: {}", msg,
        );
    }

    #[test]
    fn reject_prefix_arg_blocks_dollar_storage_too() {
        let r = reject_prefix_arg("--storage-dir", Some(&PathBuf::from("$storage/sub")));
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().contains("--storage-dir"));
    }

    #[test]
    fn reject_prefix_arg_allows_real_paths() {
        // Absolute Windows
        assert!(reject_prefix_arg("--temp-dir", Some(&PathBuf::from(r"D:\scratch"))).is_ok());
        // Absolute POSIX
        assert!(reject_prefix_arg("--temp-dir", Some(&PathBuf::from("/tmp/scratch"))).is_ok());
        // Relative path (resolves against pipeline cwd; not a $-prefix)
        assert!(reject_prefix_arg("--temp-dir", Some(&PathBuf::from("scratch"))).is_ok());
    }

    #[test]
    fn reject_prefix_arg_no_op_when_unset() {
        // Most users don't pass --temp-dir at all; None is the common case.
        assert!(reject_prefix_arg("--temp-dir", None).is_ok());
    }

    #[test]
    fn check_one_fails_when_tool_not_in_cfg_paths() {
        // Counterpart: with empty cfg.tools_paths, the same pipeline
        // should fail to resolve mock-tool. Confirms the cfg is what
        // controls resolution (not some hidden global).
        let tmp = tempfile::tempdir().unwrap();
        let pipe_dir = tmp.path().join("test-pipe");
        write_minimal_pipeline(&pipe_dir, "mock-tool");

        let cfg = dpe::RunnerConfig::default();  // no tools_paths
        let r = check_one(&pipe_dir, "test-pipe", "main", false, false, &cfg);
        assert!(r.is_err(), "expected err with empty cfg, got Ok");
        let msg = r.unwrap_err().to_string();
        assert!(msg.contains("validation error"), "unexpected msg: {}", msg);
    }
}
