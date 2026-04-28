//! dpe — CLI entry for the DPE runner.

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "dpe", about = "DPE pipeline runner CLI")]
struct Cli {
    /// Runner config file (default: ~/.dpe/config.toml)
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
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
    List,
}

#[derive(Debug, Subcommand)]
enum ConfigCmd {
    /// Print the resolved config (after defaults + file loads).
    Show,
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
        Command::Check { target, all, plan } => cmd_check(target, all, plan, &cfg),
        Command::Run { target, input, output, clear: _, cache } =>
            cmd_run(target, input, output, cache, &cfg),
        Command::Journal { session } => cmd_journal(session),
        Command::Status { session }   => cmd_status(session),
        Command::Progress { session } => cmd_progress(session),
        Command::Stop { session }     => cmd_stop(session),
        Command::Logs { session, follow } => cmd_logs(session, follow),
        Command::Monitor { session }      => dpe::monitor::run(session),
        Command::Tools { sub } => match sub {
            ToolsCmd::List => cmd_tools_list(&cfg),
        },
        Command::Install { name, force } => cmd_install(&cfg, &name, force),
        Command::Config { sub } => match sub {
            ConfigCmd::Show        => cmd_config_show(&cfg),
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

fn cmd_run(
    target: String,
    input: PathBuf,
    output: PathBuf,
    cache: Option<String>,
    cfg: &dpe::RunnerConfig,
) -> anyhow::Result<()> {
    use dpe::types::CacheMode;

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
        session_id,
        input: input.clone(),
        output: output.clone(),
        cache_mode,
    };
    std::fs::create_dir_all(&output)?;

    // Resolve the seed input. Convention: if --input is a file, use it
    // directly; if it's a directory, look for `_seed.ndjson`; else Empty.
    let input_source = if input.is_file() {
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
    let report = rt.block_on(dpe::dag::run_variant(
        &resolved,
        &pipeline_dir,
        &ctx,
        cfg,
        input_source,
        dpe::dag::OutputSink::Directory(output),
    )).map_err(|e| anyhow::anyhow!("dag: {}", e))?;

    println!("[OK] {}:{} — {} stage(s), {} succeeded, {} failed, {}ms",
        resolved.pipeline, resolved.variant,
        report.stages_run, report.stages_succeeded, report.stages_failed,
        report.duration_ms);
    Ok(())
}

fn cmd_check(target: String, all: bool, plan: bool, _cfg: &dpe::RunnerConfig) -> anyhow::Result<()> {
    let (pipeline_path, variant_name) = parse_target(&target)?;
    let (resolved_dir, pipeline_name) = resolve_pipeline_dir_and_name(&pipeline_path)?;
    let pipeline_dir = resolved_dir.as_path();

    if all {
        if plan {
            anyhow::bail!("--plan and --all are mutually exclusive; pick one variant");
        }
        check_all_variants(pipeline_dir, &pipeline_name)
    } else {
        check_one(pipeline_dir, &pipeline_name, &variant_name, plan)
    }
}

fn check_one(dir: &std::path::Path, pipeline: &str, variant: &str, print_plan: bool) -> anyhow::Result<()> {
    let resolved = dpe::load_variant(dir, pipeline, variant)?;
    let cfg = dpe::load_config(None)?;

    match dpe::validate::validate(&resolved, dir, &cfg) {
        Ok(()) => {
            println!("[OK] {}:{} — {} stage(s) resolved + validated",
                pipeline, variant, resolved.stages.len());
            if print_plan {
                print_execution_plan(&resolved, dir, &cfg)?;
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
) -> anyhow::Result<()> {
    let static_resolver = dpe::paths::PathResolver::default();
    let plan = dpe::dag::plan::compile(resolved, pipeline_dir, cfg, &static_resolver)
        .map_err(|e| anyhow::anyhow!("plan compile: {}", e))?;
    let json = serde_json::to_string_pretty(&plan)?;
    println!("{}", json);
    Ok(())
}

fn check_all_variants(dir: &std::path::Path, pipeline: &str) -> anyhow::Result<()> {
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
        match check_one(dir, pipeline, &name, false) {
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
    let canonical = std::fs::canonicalize(&raw).unwrap_or_else(|_| raw.clone());
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

fn cmd_tools_list(cfg: &dpe::RunnerConfig) -> anyhow::Result<()> {
    let registries = dpe::catalog::resolve_registries(cfg);
    let catalog = dpe::catalog::Catalog::load_from_files(&registries);
    let layout = dpe::home::Layout::resolve().ok();
    let effective_paths: Vec<std::path::PathBuf> = cfg.tools_paths.iter()
        .map(|p| std::path::PathBuf::from(expand_home(p)))
        .chain(layout.iter().map(|l| l.tools.clone()))
        .collect();

    println!("{:24} {:8} {:10} {:8} STATUS",
        "NAME", "RUNTIME", "TIER", "VERSION");
    println!("{}", "─".repeat(80));

    if catalog.tools.is_empty() {
        println!("(no tools — registry list is empty or all files failed to load)");
    }
    for (name, entry) in &catalog.tools {
        let status = resolve_status(name, &effective_paths);
        println!("{:24} {:8} {:10} {:8} {}",
            truncate(name, 24),
            entry.runtime,
            entry.tier.as_deref().unwrap_or(""),
            entry.version.as_deref().unwrap_or(""),
            status);
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

fn resolve_status(name: &str, paths: &[std::path::PathBuf]) -> &'static str {
    for p in paths {
        let candidate = p.join(name).join("meta.json");
        if candidate.exists() { return "installed"; }
    }
    "available (run: dpe install <name>)"
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
