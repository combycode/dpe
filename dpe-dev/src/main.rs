//! dpe-dev — tool development CLI.

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

mod embedded;
mod scaffold;
mod setup;
mod verify;

#[derive(Parser)]
#[command(name = "dpe-dev", version, about = "Scaffold, build, test, and verify DPE tools")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Scaffold a new tool from the matching framework's `template/` directory.
    Scaffold {
        /// Kebab-case tool name (becomes crate/package name).
        #[arg(long)]
        name: String,
        /// Runtime: rust | bun | python.
        #[arg(long)]
        runtime: Runtime,
        /// Output directory — created if missing. Must not already contain files.
        #[arg(long)]
        out: PathBuf,
        /// One-line description (defaults to "TODO").
        #[arg(long, default_value = "TODO")]
        description: String,
        /// Override auto-discovery of the frameworks root.
        #[arg(long)]
        frameworks_dir: Option<PathBuf>,
    },
    /// Build the tool (runtime-aware). Rust: cargo build --release. Bun: bun install.
    /// Python: pip install -e . (tries uv first).
    Build {
        dir: PathBuf,
        /// Future: run additional steps (bundle / wheel / sign).
        #[arg(long)]
        full: bool,
    },
    /// Run the tool's unit tests.
    Test {
        dir: PathBuf,
    },
    /// Run the verify cases (spawn the built tool, feed input, diff stdout).
    Verify {
        dir: PathBuf,
    },
    /// Static checks — meta.json valid, spec.yaml parses, binary entry exists.
    Check {
        dir: PathBuf,
    },
    /// Bootstrap a dev-workspace: extracts the skill pack + fixtures, registers
    /// the path in ~/.dpe/config.toml. Default path: ~/.dpe/dev-workspace.
    Setup {
        /// Workspace directory. Defaults to ~/.dpe/dev-workspace.
        path: Option<PathBuf>,
        /// Overwrite existing files in the workspace (skill, fixtures, README).
        #[arg(long)]
        force: bool,
    },
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum Runtime {
    Rust,
    Bun,
    Python,
}

impl Runtime {
    fn framework_dir_name(self) -> &'static str {
        match self {
            Runtime::Rust   => "frameworks/rust",
            Runtime::Bun    => "frameworks/ts",
            Runtime::Python => "frameworks/python",
        }
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Scaffold { name, runtime, out, description, frameworks_dir } => {
            scaffold::scaffold(&name, runtime, &out, &description, frameworks_dir.as_deref())
        }
        Cmd::Build { dir, full } => build(&dir, full),
        Cmd::Test  { dir }       => test(&dir),
        Cmd::Verify{ dir }       => verify::verify(&dir),
        Cmd::Setup { path, force } => {
            let ws = setup::setup(path, force)?;
            println!("[OK] workspace ready: {}", ws.display());
            Ok(())
        }
        Cmd::Check { dir }       => check(&dir),
    }
}

// ═══ build ═══════════════════════════════════════════════════════════════════

fn read_runtime(dir: &Path) -> Result<String> {
    let meta_path = dir.join("meta.json");
    let raw = std::fs::read_to_string(&meta_path)
        .with_context(|| format!("read {:?}", meta_path))?;
    let j: serde_json::Value = serde_json::from_str(&raw)?;
    Ok(j.get("runtime").and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("meta.json missing 'runtime'"))?
        .to_string())
}

fn build(dir: &Path, _full: bool) -> Result<()> {
    let runtime = read_runtime(dir)?;
    match runtime.as_str() {
        "rust"   => run_cmd(dir, "cargo",  &["build", "--release"]),
        "bun"    => run_cmd(dir, "bun",    &["install"]),
        "python" => python_build(dir),
        other => bail!("unknown runtime '{}' in meta.json", other),
    }
}

fn test(dir: &Path) -> Result<()> {
    let runtime = read_runtime(dir)?;
    match runtime.as_str() {
        "rust"   => run_cmd(dir, "cargo", &["test"]),
        "bun"    => run_cmd(dir, "bun",   &["test"]),
        "python" => python_test(dir),
        other => bail!("unknown runtime '{}' in meta.json", other),
    }
}

/// Python tools build into a per-tool `.venv/`. This works on every modern
/// Python distribution including Debian (where PEP 668 blocks system pip)
/// and macOS Homebrew. The `dev` extra pulls in pytest for test().
fn python_build(dir: &Path) -> Result<()> {
    let venv = dir.join(".venv");
    if !venv.exists() {
        // Prefer uv when installed (much faster). `--seed` is required so
        // pip gets installed inside the venv — without it `uv venv` produces
        // a venv with only python, no pip, breaking the editable install.
        // Fall back to stdlib venv when uv is unavailable.
        if run_cmd(dir, "uv", &["venv", "--seed", ".venv"]).is_err() {
            run_cmd(dir, "python3", &["-m", "venv", ".venv"])?;
        }
    }
    let pip = python_venv_bin(&venv, "pip");
    let pip_path = pip.to_str().ok_or_else(|| anyhow!("non-utf8 venv path"))?;
    run_cmd(dir, pip_path, &["install", "--upgrade", "--quiet", "pip"])?;
    run_cmd(dir, pip_path, &["install", "--quiet", "-e", ".[dev]"])
}

fn python_test(dir: &Path) -> Result<()> {
    let venv = dir.join(".venv");
    if !venv.exists() {
        bail!("missing {:?} — run `dpe-dev build` first", venv);
    }
    let pytest = python_venv_bin(&venv, "pytest");
    run_cmd(dir, pytest.to_str().unwrap_or("pytest"), &[])
}

/// Path to a binary inside a Python venv. `<venv>/bin/<name>` on Unix,
/// `<venv>\Scripts\<name>.exe` on Windows.
fn python_venv_bin(venv: &Path, name: &str) -> PathBuf {
    #[cfg(windows)]
    { venv.join("Scripts").join(format!("{}.exe", name)) }
    #[cfg(not(windows))]
    { venv.join("bin").join(name) }
}

fn check(dir: &Path) -> Result<()> {
    let meta_path = dir.join("meta.json");
    if !meta_path.exists() { bail!("meta.json not found in {:?}", dir); }
    let meta_raw = std::fs::read_to_string(&meta_path)?;
    let meta: serde_json::Value = serde_json::from_str(&meta_raw)
        .with_context(|| format!("parse meta.json in {:?}", dir))?;
    for field in &["name", "version", "runtime"] {
        if meta.get(field).is_none() {
            bail!("meta.json missing required field '{}'", field);
        }
    }
    let spec_path = dir.join("spec.yaml");
    if spec_path.exists() {
        let spec_raw = std::fs::read_to_string(&spec_path)?;
        let opts = serde_saphyr::options!(
            strict_booleans: true,
            no_schema: true,
            legacy_octal_numbers: false,
        );
        serde_saphyr::from_str_with_options::<serde_json::Value>(&spec_raw, opts)
            .with_context(|| "parse spec.yaml")?;
    }
    println!("[check] {:?} OK (name={}, runtime={})",
        dir,
        meta.get("name").and_then(|v| v.as_str()).unwrap_or("?"),
        meta.get("runtime").and_then(|v| v.as_str()).unwrap_or("?"));
    Ok(())
}

// ═══ helpers ═════════════════════════════════════════════════════════════════

fn run_cmd(dir: &Path, program: &str, args: &[&str]) -> Result<()> {
    println!("[dpe-dev] $ {} {} (in {:?})", program, args.join(" "), dir);
    let status = Command::new(program)
        .args(args)
        .current_dir(dir)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .with_context(|| format!("spawn {} {:?}", program, args))?;
    if !status.success() {
        bail!("{} {:?} failed with {}", program, args, status);
    }
    Ok(())
}
