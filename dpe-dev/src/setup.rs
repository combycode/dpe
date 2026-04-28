//! `dpe-dev setup [path]` — materialise a dev-workspace from the embedded
//! template and register it in `~/.dpe/config.toml`.
//!
//! Behaviour:
//!   - If `path` given, use it. Else default to `~/.dpe/dev-workspace`.
//!   - Create the dir if missing.
//!   - Extract embedded dev-workspace-template into it (skips existing files).
//!   - Append or update `[dev] workspace = "<path>"` in `~/.dpe/config.toml`.

use anyhow::{anyhow, Context, Result};
use std::path::{Path, PathBuf};

use crate::embedded;

pub(crate) fn setup(path: Option<PathBuf>, force: bool) -> Result<PathBuf> {
    let workspace = resolve_target(path)?;
    let abs = canonical_or_given(&workspace);

    eprintln!("[setup] target workspace: {}", abs.display());

    std::fs::create_dir_all(&abs)
        .with_context(|| format!("create {:?}", abs))?;
    let n = embedded::extract(&embedded::WORKSPACE_TEMPLATE, &abs, force)
        .with_context(|| format!("extract template into {:?}", abs))?;
    eprintln!("[setup] extracted {} template file(s)", n);

    // Write a marker file so dpe-dev can recognise its own workspaces later.
    let marker = abs.join(".dpe-workspace.toml");
    if !marker.exists() {
        let contents = format!(
            "# marker file — dpe-dev setup created this workspace\nversion = \"2.0.0\"\ncreated = \"{}\"\n",
            chrono_rfc3339()
        );
        std::fs::write(&marker, contents)?;
    }

    // Register in ~/.dpe/config.toml (best-effort)
    if let Err(e) = register_in_config(&abs) {
        eprintln!("[setup] WARN — config registration failed: {}", e);
    }

    eprintln!("[setup] done. Try:");
    eprintln!("   cd {}", abs.display());
    eprintln!("   dpe-dev scaffold --name hello --runtime bun --out tools/hello");
    Ok(abs)
}

fn resolve_target(path: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(p) = path { return Ok(p); }
    let home = dirs::home_dir()
        .ok_or_else(|| anyhow!("cannot determine user home dir"))?;
    Ok(home.join(".dpe").join("dev-workspace"))
}

fn canonical_or_given(p: &Path) -> PathBuf {
    // Try to canonicalize the parent + append the filename, so we don't fail
    // when `p` doesn't yet exist.
    if p.exists() {
        p.canonicalize().unwrap_or_else(|_| p.to_path_buf())
    } else {
        match p.parent() {
            Some(parent) if parent.exists() => {
                let canon_parent = parent.canonicalize().unwrap_or_else(|_| parent.to_path_buf());
                if let Some(name) = p.file_name() { canon_parent.join(name) } else { p.to_path_buf() }
            }
            _ => p.to_path_buf(),
        }
    }
}

fn register_in_config(workspace: &Path) -> Result<()> {
    let home = dirs::home_dir()
        .ok_or_else(|| anyhow!("no home dir"))?;
    let config_path = home.join(".dpe").join("config.toml");

    std::fs::create_dir_all(config_path.parent().unwrap())?;

    // Read existing config (or start empty)
    let mut doc: toml::Value = if config_path.exists() {
        let raw = std::fs::read_to_string(&config_path)?;
        toml::from_str(&raw).unwrap_or_else(|_| toml::Value::Table(toml::map::Map::new()))
    } else {
        toml::Value::Table(toml::map::Map::new())
    };

    // Ensure [dev] table, set workspace
    let table = doc.as_table_mut()
        .ok_or_else(|| anyhow!("config is not a table"))?;
    let dev_entry = table.entry("dev".to_string())
        .or_insert_with(|| toml::Value::Table(toml::map::Map::new()));
    let dev = dev_entry.as_table_mut()
        .ok_or_else(|| anyhow!("[dev] is not a table"))?;
    dev.insert("workspace".into(),
        toml::Value::String(workspace.to_string_lossy().replace('\\', "/")));

    // Write back
    let out = toml::to_string_pretty(&doc)?;
    std::fs::write(&config_path, out)?;
    eprintln!("[setup] registered workspace in {}", config_path.display());
    Ok(())
}

/// Return a compact RFC3339-ish timestamp without pulling the chrono crate.
/// Acceptable for a marker-file note.
fn chrono_rfc3339() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    format!("unix:{}", now.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn setup_creates_workspace_files() {
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("ws");
        let result = setup(Some(target.clone()), false).unwrap();
        assert!(result.exists());
        assert!(target.join(".claude/skills/dpe-tool/SKILL.md").exists());
        assert!(target.join("fixtures/uppercase-text.yaml").exists());
        assert!(target.join(".dpe-workspace.toml").exists());
    }

    #[test] fn setup_is_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("ws");
        setup(Some(target.clone()), false).unwrap();
        // Second run must succeed and not clobber
        let marker = target.join(".dpe-workspace.toml");
        let before = std::fs::read_to_string(&marker).unwrap();
        setup(Some(target.clone()), false).unwrap();
        let after = std::fs::read_to_string(&marker).unwrap();
        assert_eq!(before, after, "marker should not be overwritten on second setup");
    }

    #[test] fn resolve_target_defaults_to_home_dpe_dev_workspace() {
        // Can't reliably check home dir in CI but the function must return Ok
        let r = resolve_target(None).unwrap();
        assert!(r.ends_with("dev-workspace"));
    }
}
