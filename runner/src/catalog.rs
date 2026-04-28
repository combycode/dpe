//! Curated tool catalogue.
//!
//! Catalogs live on disk as JSON files. The runner loads one or more files
//! listed in `config.toml::tools_registries` (or, when that is empty, the
//! single file `<binary_dir>/catalog.json` if present). Multiple files are
//! merged with **first-match-wins** semantics on the tool name.
//!
//! Mental model: registries are searched in declaration order. A user-private
//! registry listed first shadows a company-wide one; both shadow the default
//! that ships next to the binary.
//!
//! Missing files are warnings on stderr — never errors. A misconfigured or
//! stale path should not break `dpe install` for tools served by other
//! registries in the list.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct Catalog {
    pub version:    String,
    #[serde(default)]
    pub updated_at: Option<String>,
    #[serde(default)]
    pub comment:    Option<serde_json::Value>,
    #[serde(default)]
    pub tools:      BTreeMap<String, CatalogEntry>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CatalogEntry {
    pub description: String,
    pub runtime:     String,
    #[serde(default)]
    pub tier:        Option<String>,
    #[serde(default)]
    pub version:     Option<String>,
    #[serde(default)]
    pub binary:      BinaryInfo,
    /// Registry file this entry came from. Set by the loader; never serialized.
    #[serde(skip)]
    pub source:      Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct BinaryInfo {
    /// Binary name, typically same as the tool's kebab name.
    #[serde(default)]
    pub name:         Option<String>,
    /// Template with {os} / {arch} / {version} placeholders. Empty = no
    /// download URL yet (install prints a hint instead of fetching).
    #[serde(default)]
    pub url_template: Option<String>,
    /// Per-platform sha256 hashes. Empty for unreleased tools.
    #[serde(default)]
    pub sha256:       BTreeMap<String, String>,
}

impl Catalog {
    /// Load a single catalog file.
    pub fn load(path: &Path) -> Result<Self, CatalogError> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| CatalogError::Io(path.to_path_buf(), e.to_string()))?;
        let mut cat: Self = serde_json::from_str(&raw)
            .map_err(|e| CatalogError::Parse(path.to_path_buf(), e.to_string()))?;
        // Tag every entry with its source file so `dpe tools list` can show
        // provenance later if we want.
        for entry in cat.tools.values_mut() {
            entry.source = Some(path.to_path_buf());
        }
        Ok(cat)
    }

    /// Load and merge a list of registry files with first-match-wins on tool
    /// name. Missing or malformed files emit a warning on stderr and are
    /// skipped — they never abort the load. The returned catalog's `version`
    /// is taken from the first file that loaded successfully; an empty list
    /// (or a list where every file failed) yields a catalog with empty tools
    /// and a synthetic version of "empty".
    pub fn load_from_files(paths: &[PathBuf]) -> Self {
        let mut merged = Self {
            version: String::new(),
            updated_at: None,
            comment: None,
            tools: BTreeMap::new(),
        };
        for path in paths {
            match Self::load(path) {
                Ok(cat) => {
                    if merged.version.is_empty() {
                        merged.version = cat.version;
                        merged.updated_at = cat.updated_at;
                        merged.comment = cat.comment;
                    }
                    for (name, entry) in cat.tools {
                        // first-match-wins
                        merged.tools.entry(name).or_insert(entry);
                    }
                }
                Err(e) => {
                    eprintln!("[catalog] WARN — skipping {}: {}", path.display(), e);
                }
            }
        }
        if merged.version.is_empty() {
            merged.version = "empty".into();
        }
        merged
    }

    /// `<binary_dir>/catalog.json`, when the binary location is known.
    pub fn default_adjacent_path() -> Option<PathBuf> {
        std::env::current_exe().ok()
            .and_then(|p| p.parent().map(|d| d.join("catalog.json")))
    }
}

/// Resolve which registry files to load given a runner config.
/// If `cfg.tools_registries` is non-empty, use those (after `~` expansion).
/// Otherwise fall back to `<binary_dir>/catalog.json` — but only if it exists.
pub fn resolve_registries(cfg: &crate::config::RunnerConfig) -> Vec<PathBuf> {
    if !cfg.tools_registries.is_empty() {
        return cfg.tools_registries.iter()
            .map(|s| PathBuf::from(expand_home(s)))
            .collect();
    }
    Catalog::default_adjacent_path()
        .filter(|p| p.exists())
        .map(|p| vec![p])
        .unwrap_or_default()
}

fn expand_home(s: &str) -> String {
    if let Some(rest) = s.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(rest).to_string_lossy().into_owned();
        }
    }
    s.to_string()
}

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("read {0}: {1}")]
    Io(PathBuf, String),
    #[error("parse {0}: {1}")]
    Parse(PathBuf, String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_json(dir: &Path, name: &str, body: &str) -> PathBuf {
        let p = dir.join(name);
        let mut f = std::fs::File::create(&p).unwrap();
        f.write_all(body.as_bytes()).unwrap();
        p
    }

    #[test] fn load_single_file() {
        let tmp = tempfile::tempdir().unwrap();
        let p = write_json(tmp.path(), "c.json", r#"{
            "version":"1",
            "tools":{
                "a":{"description":"A","runtime":"rust"}
            }
        }"#);
        let cat = Catalog::load(&p).unwrap();
        assert_eq!(cat.version, "1");
        assert!(cat.tools.contains_key("a"));
        assert_eq!(cat.tools["a"].source.as_ref().unwrap(), &p);
    }

    #[test] fn load_io_error_reports_path() {
        let p = PathBuf::from("/no/such/file.json");
        let err = Catalog::load(&p).unwrap_err();
        match err {
            CatalogError::Io(got, _) => assert_eq!(got, p),
            _ => panic!("expected Io"),
        }
    }

    #[test] fn load_parse_error_reports_path() {
        let tmp = tempfile::tempdir().unwrap();
        let p = write_json(tmp.path(), "bad.json", "{not json");
        let err = Catalog::load(&p).unwrap_err();
        match err {
            CatalogError::Parse(got, _) => assert_eq!(got, p),
            _ => panic!("expected Parse"),
        }
    }

    #[test] fn first_match_wins_on_merge() {
        let tmp = tempfile::tempdir().unwrap();
        let p1 = write_json(tmp.path(), "first.json", r#"{
            "version":"first",
            "tools":{
                "shared":{"description":"from first","runtime":"rust"}
            }
        }"#);
        let p2 = write_json(tmp.path(), "second.json", r#"{
            "version":"second",
            "tools":{
                "shared":{"description":"from second","runtime":"rust"},
                "second-only":{"description":"S","runtime":"bun"}
            }
        }"#);
        let cat = Catalog::load_from_files(&[p1.clone(), p2]);
        assert_eq!(cat.version, "first");
        assert_eq!(cat.tools["shared"].description, "from first");
        assert!(cat.tools.contains_key("second-only"));
        assert_eq!(cat.tools["shared"].source.as_ref().unwrap(), &p1);
    }

    #[test] fn missing_files_emit_warnings_not_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let real = write_json(tmp.path(), "real.json", r#"{
            "version":"1",
            "tools":{"a":{"description":"A","runtime":"rust"}}
        }"#);
        let missing = tmp.path().join("missing.json");
        let cat = Catalog::load_from_files(&[missing, real]);
        assert!(cat.tools.contains_key("a"));
    }

    #[test] fn empty_input_yields_empty_catalog() {
        let cat = Catalog::load_from_files(&[]);
        assert_eq!(cat.version, "empty");
        assert!(cat.tools.is_empty());
    }

    #[test] fn all_files_failing_yields_empty_catalog() {
        let cat = Catalog::load_from_files(&[
            PathBuf::from("/none/1"),
            PathBuf::from("/none/2"),
        ]);
        assert_eq!(cat.version, "empty");
        assert!(cat.tools.is_empty());
    }

    #[test] fn resolve_registries_uses_config_when_set() {
        let cfg = crate::config::RunnerConfig {
            tools_registries: vec!["/etc/dpe/a.json".into(), "/etc/dpe/b.json".into()],
            ..Default::default()
        };
        let r = resolve_registries(&cfg);
        assert_eq!(r, vec![PathBuf::from("/etc/dpe/a.json"), PathBuf::from("/etc/dpe/b.json")]);
    }

    #[test] fn resolve_registries_expands_tilde() {
        let cfg = crate::config::RunnerConfig {
            tools_registries: vec!["~/.dpe/my.json".into()],
            ..Default::default()
        };
        let r = resolve_registries(&cfg);
        assert_eq!(r.len(), 1);
        // Tilde must be expanded — no literal ~ in the result.
        assert!(!r[0].to_string_lossy().starts_with('~'));
    }
}
