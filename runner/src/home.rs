//! `~/.dpe/` layout: canonical user-level install directory.
//!
//! Layout:
//!   ~/.dpe/
//!     bin/                 dpe + dpe-dev binaries
//!     tools/               shipped + installed tools, one dir per tool
//!     frameworks/          lazy-populated by dpe-dev
//!     dev-workspace/       default dpe-dev setup location (if user didn't pick one)
//!     config.toml          user config
//!
//! The bootstrap function ensures the directories exist (idempotent). Call
//! it once at CLI startup; subsequent invocations are cheap.

use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct Layout {
    pub root:             PathBuf,    // ~/.dpe
    pub bin:              PathBuf,    // ~/.dpe/bin
    pub tools:            PathBuf,    // ~/.dpe/tools
    pub frameworks:       PathBuf,    // ~/.dpe/frameworks
    pub dev_workspace:    PathBuf,    // ~/.dpe/dev-workspace
    pub config:           PathBuf,    // ~/.dpe/config.toml
}

impl Layout {
    /// Compute the canonical layout paths. Root is `$HOME/.dpe` on Unix and
    /// `%USERPROFILE%/.dpe` on Windows.
    pub fn resolve() -> Result<Self, LayoutError> {
        let home = dirs::home_dir()
            .ok_or(LayoutError::NoHomeDir)?;
        let root = home.join(".dpe");
        Ok(Self {
            bin:           root.join("bin"),
            tools:         root.join("tools"),
            frameworks:    root.join("frameworks"),
            dev_workspace: root.join("dev-workspace"),
            config:        root.join("config.toml"),
            root,
        })
    }

    /// Create every directory in the layout. Idempotent.
    pub fn ensure(&self) -> Result<(), LayoutError> {
        for d in [&self.root, &self.bin, &self.tools, &self.frameworks] {
            std::fs::create_dir_all(d).map_err(|e| LayoutError::Io(d.clone(), e.to_string()))?;
        }
        Ok(())
    }

    /// True if the layout root exists.
    pub fn exists(&self) -> bool { self.root.is_dir() }
}

#[derive(Debug, thiserror::Error)]
pub enum LayoutError {
    #[error("cannot determine user home directory")]
    NoHomeDir,
    #[error("filesystem error at {0}: {1}")]
    Io(PathBuf, String),
}

/// Resolve a tool path inside `~/.dpe/tools/<name>/`. Does NOT create it.
pub fn tool_dir(layout: &Layout, name: &str) -> PathBuf {
    layout.tools.join(name)
}

/// Resolve a framework cache dir `~/.dpe/frameworks/<runtime>/<version>/`.
pub fn framework_dir(layout: &Layout, runtime: &str, version: &str) -> PathBuf {
    layout.frameworks.join(runtime).join(version)
}

/// Get a `Path` for `~/.dpe/` itself. Useful in tests + CLI.
pub fn resolve_root() -> Result<PathBuf, LayoutError> {
    Layout::resolve().map(|l| l.root)
}

/// Best-effort check if `<layout.bin>` is on PATH.
pub fn bin_on_path(layout: &Layout) -> bool {
    let Some(path_var) = std::env::var_os("PATH") else { return false; };
    let target = normalize(&layout.bin);
    std::env::split_paths(&path_var)
        .map(|p| normalize(&p))
        .any(|p| p == target)
}

fn normalize(p: &Path) -> PathBuf {
    p.canonicalize().unwrap_or_else(|_| p.to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn resolve_yields_paths_under_dpe_dir() {
        let l = Layout::resolve().unwrap();
        assert!(l.root.ends_with(".dpe"));
        assert!(l.bin.ends_with("bin"));
        assert!(l.tools.ends_with("tools"));
        assert!(l.frameworks.ends_with("frameworks"));
        assert!(l.dev_workspace.ends_with("dev-workspace"));
        assert!(l.config.ends_with("config.toml"));
    }

    #[test] fn tool_dir_appends_name() {
        let l = Layout::resolve().unwrap();
        let p = tool_dir(&l, "scan-fs");
        assert!(p.ends_with("tools/scan-fs") || p.ends_with("tools\\scan-fs"));
    }

    #[test] fn framework_dir_formatted() {
        let l = Layout::resolve().unwrap();
        let p = framework_dir(&l, "rust", "2.0.0");
        assert!(p.to_string_lossy().contains("frameworks"));
        assert!(p.to_string_lossy().contains("rust"));
        assert!(p.to_string_lossy().contains("2.0.0"));
    }

    #[test] fn ensure_is_idempotent_in_tempdir() {
        let tmp = tempfile::tempdir().unwrap();
        let layout = Layout {
            root:          tmp.path().to_path_buf(),
            bin:           tmp.path().join("bin"),
            tools:         tmp.path().join("tools"),
            frameworks:    tmp.path().join("frameworks"),
            dev_workspace: tmp.path().join("dev-workspace"),
            config:        tmp.path().join("config.toml"),
        };
        layout.ensure().unwrap();
        layout.ensure().unwrap();  // twice must not error
        assert!(layout.bin.is_dir());
        assert!(layout.tools.is_dir());
        assert!(layout.frameworks.is_dir());
    }
}
