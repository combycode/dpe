//! Phase D — filesystem tree comparison.
//!
//! Step 2 of the per-phase run: walk every subdir under
//! `expected/<phase>/` (filtered by `compare.fs_check`), compare each
//! file pairwise against the matching path under `.run/`. Files
//! present in expected but missing in actual = regression. Present
//! in actual but not expected = unexpected output. Per-file mode
//! (auto by extension or `compare.files[].mode`) controls how the
//! comparison is done.
//!
//! Modes:
//! - Diff (default text): line-by-line equality after global
//!   scrub_paths applied.
//! - Exact (default binary): byte-for-byte.
//! - Schema: JSON Schema validation against a side file. Schema is
//!   the contract; value varies.
//! - Contains: every regex pattern must match somewhere in the actual
//!   content. For logs / receipts.
//! - Fuzzy: line-diff but tolerate up to threshold_pct % mismatching
//!   lines. For LLM output.
//! - Exists: file must exist; content unchecked.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use globset::{Glob, GlobSet, GlobSetBuilder};
use regex::Regex;
use serde::Deserialize;

// ─── YAML schema (parsed from test.yaml) ────────────────────────────

/// `compare.files[]` entry — per-file mode override.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileOverrideYaml {
    /// Path relative to expected root (e.g. "output/report.json").
    pub path: String,
    pub mode: String,
    /// Used by `schema` mode: path to the JSON Schema file (relative to
    /// case root or absolute).
    pub schema: Option<String>,
    /// Used by `contains` mode: list of regex patterns that must all
    /// match somewhere in the actual content.
    pub patterns: Option<Vec<String>>,
    /// Used by `fuzzy` mode: max % mismatching lines.
    pub threshold_pct: Option<f64>,
}

// ─── Effective per-file rule (post-merge with defaults) ─────────────

#[derive(Debug, Clone)]
pub enum FileMode {
    Diff,
    Exact,
    Schema { schema_path: PathBuf },
    Contains { patterns: Vec<Regex> },
    Fuzzy { threshold_pct: f64 },
    Exists,
}

impl FileMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            FileMode::Diff      => "diff",
            FileMode::Exact     => "exact",
            FileMode::Schema {..} => "schema",
            FileMode::Contains {..} => "contains",
            FileMode::Fuzzy {..} => "fuzzy",
            FileMode::Exists    => "exists",
        }
    }
}

#[derive(Debug, Clone)]
pub struct FileOverride {
    pub path: String,        // canonicalised relative form using `/` separator
    pub mode: FileMode,
}

/// Parse a list of YAML overrides into compiled overrides. Resolves
/// `schema:` paths against `case_dir`. Compiles regex patterns.
pub fn parse_overrides(
    yaml: &[FileOverrideYaml],
    case_dir: &Path,
) -> Result<Vec<FileOverride>> {
    let mut out = Vec::with_capacity(yaml.len());
    for o in yaml {
        let mode = match o.mode.as_str() {
            "diff"   => FileMode::Diff,
            "exact"  => FileMode::Exact,
            "exists" => FileMode::Exists,
            "schema" => {
                let s = o.schema.as_deref().ok_or_else(|| anyhow::anyhow!(
                    "files[].path={} mode=schema requires a `schema:` field",
                    o.path,
                ))?;
                let schema_path = if Path::new(s).is_absolute() {
                    PathBuf::from(s)
                } else {
                    case_dir.join(s)
                };
                FileMode::Schema { schema_path }
            }
            "contains" => {
                let pats = o.patterns.as_deref().ok_or_else(|| anyhow::anyhow!(
                    "files[].path={} mode=contains requires `patterns:` array",
                    o.path,
                ))?;
                let mut compiled = Vec::with_capacity(pats.len());
                for p in pats {
                    compiled.push(Regex::new(p)
                        .with_context(|| format!("compile contains pattern {p:?}"))?);
                }
                FileMode::Contains { patterns: compiled }
            }
            "fuzzy" => {
                let pct = o.threshold_pct.unwrap_or(5.0);
                if !(0.0..=100.0).contains(&pct) {
                    anyhow::bail!("files[].path={} threshold_pct={} out of range",
                        o.path, pct);
                }
                FileMode::Fuzzy { threshold_pct: pct }
            }
            other => anyhow::bail!(
                "files[].path={} unknown mode '{}' (use diff|exact|schema|contains|fuzzy|exists)",
                o.path, other,
            ),
        };
        out.push(FileOverride {
            path: normalise_rel_path(&o.path),
            mode,
        });
    }
    Ok(out)
}

/// Normalise a YAML-supplied path: convert `\` → `/`, strip leading
/// `./`, drop empty segments. The result is the canonical key used to
/// match `walk_files` outputs.
fn normalise_rel_path(s: &str) -> String {
    let s = s.replace('\\', "/");
    let s = s.strip_prefix("./").unwrap_or(&s).to_string();
    let mut parts: Vec<&str> = s.split('/').filter(|p| !p.is_empty() && *p != ".").collect();
    let mut clean: Vec<&str> = Vec::with_capacity(parts.len());
    while let Some(p) = parts.first().copied() {
        parts.remove(0);
        clean.push(p);
    }
    clean.join("/")
}

/// Default mode by file extension. Text extensions get `Diff`;
/// everything else gets `Exact`.
pub fn default_mode_for(path: &Path) -> FileMode {
    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("").to_ascii_lowercase();
    let text = matches!(ext.as_str(),
        "md" | "txt" | "json" | "ndjson" | "csv" | "tsv" | "html" | "htm"
        | "yaml" | "yml" | "toml" | "log" | "xml" | "tpl"
    );
    if text { FileMode::Diff } else { FileMode::Exact }
}

// ─── File comparator ────────────────────────────────────────────────

/// One per-file comparison failure.
#[derive(Debug, Clone)]
pub struct FileFailure {
    /// Path relative to the expected/actual root.
    pub path: String,
    pub kind: FileFailureKind,
}

#[derive(Debug, Clone)]
pub enum FileFailureKind {
    /// File listed in expected but not in actual.
    MissingInActual,
    /// File listed in actual but not in expected.
    UnexpectedInActual,
    /// Mode comparison rejected the content.
    ModeMismatch { mode: &'static str, detail: String },
    /// I/O or schema-load problem.
    Io(String),
}

/// Walk both trees + diff each pair. Returns the empty vec on success.
pub fn diff_tree(
    expected_root: &Path,
    actual_root:   &Path,
    fs_check:      Option<&[String]>,           // subdirs to walk; None = all subdirs in expected
    fs_ignore:     &[String],
    file_overrides: &[FileOverride],
    line_scrub:    &[(Regex, String)],          // applied to text-mode comparisons
) -> Result<Vec<FileFailure>> {
    let mut failures = Vec::new();

    // 1. Resolve which subdirs to walk.
    let subdirs: Vec<PathBuf> = match fs_check {
        Some(list) => list.iter().map(|s| expected_root.join(s)).collect(),
        None => list_subdirs(expected_root)?
            .into_iter().map(|d| expected_root.join(d)).collect(),
    };

    // 2. Build the ignore set.
    let ignore_set = build_globset(fs_ignore)
        .with_context(|| format!("compiling fs_ignore globs: {:?}", fs_ignore))?;

    // 3. For each subdir, gather expected file list + actual file list,
    //    pair them up, compare.
    for exp_subdir in &subdirs {
        if !exp_subdir.is_dir() { continue; }
        // Compute the relative subdir name (e.g. "output").
        let subdir_rel = exp_subdir.strip_prefix(expected_root)
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|_| exp_subdir.file_name().map(PathBuf::from).unwrap_or_default());
        let act_subdir = actual_root.join(&subdir_rel);

        let exp_files = walk_files(exp_subdir, &ignore_set, expected_root)?;
        let act_files = walk_files(&act_subdir, &ignore_set, actual_root)?;

        let exp_set: BTreeSet<&str> = exp_files.iter().map(|s| s.as_str()).collect();
        let act_set: BTreeSet<&str> = act_files.iter().map(|s| s.as_str()).collect();

        // Missing-in-actual
        for rel in exp_set.difference(&act_set) {
            failures.push(FileFailure {
                path: (*rel).to_string(),
                kind: FileFailureKind::MissingInActual,
            });
        }
        // Unexpected-in-actual
        for rel in act_set.difference(&exp_set) {
            failures.push(FileFailure {
                path: (*rel).to_string(),
                kind: FileFailureKind::UnexpectedInActual,
            });
        }
        // Both-present: compare.
        for rel in exp_set.intersection(&act_set) {
            let exp_path = expected_root.join(rel);
            let act_path = actual_root.join(rel);
            let mode = file_overrides.iter()
                .find(|o| o.path == *rel)
                .map(|o| o.mode.clone())
                .unwrap_or_else(|| default_mode_for(&exp_path));
            if let Some(failure) = compare_one(rel, &exp_path, &act_path, &mode, line_scrub) {
                failures.push(failure);
            }
        }
    }

    Ok(failures)
}

fn list_subdirs(root: &Path) -> Result<Vec<String>> {
    if !root.is_dir() { return Ok(Vec::new()); }
    let mut out = Vec::new();
    for entry in std::fs::read_dir(root)
        .with_context(|| format!("readdir {}", root.display()))?
    {
        let e = entry?;
        if e.file_type()?.is_dir() {
            if let Some(s) = e.file_name().to_str() {
                out.push(s.to_string());
            }
        }
    }
    out.sort();
    Ok(out)
}

/// Walk `root` recursively, return paths relative to `relative_to`,
/// with `/` separators, sorted. Files matching ignore globs are skipped.
fn walk_files(root: &Path, ignore: &GlobSet, relative_to: &Path) -> Result<Vec<String>> {
    let mut out = Vec::new();
    if !root.is_dir() { return Ok(out); }
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir)
            .with_context(|| format!("readdir {}", dir.display()))?
        {
            let e = entry?;
            let path = e.path();
            let ft = e.file_type()?;
            if ft.is_dir() {
                stack.push(path);
            } else if ft.is_file() {
                let rel = path.strip_prefix(relative_to)
                    .map(|p| p.to_string_lossy().replace('\\', "/"))
                    .unwrap_or_default();
                if ignore.is_match(&rel) { continue; }
                out.push(rel);
            }
        }
    }
    out.sort();
    Ok(out)
}

fn build_globset(patterns: &[String]) -> Result<GlobSet> {
    let mut b = GlobSetBuilder::new();
    for p in patterns {
        b.add(Glob::new(p).with_context(|| format!("glob {p:?}"))?);
    }
    Ok(b.build()?)
}

fn compare_one(
    rel:       &str,
    exp_path:  &Path,
    act_path:  &Path,
    mode:      &FileMode,
    line_scrub: &[(Regex, String)],
) -> Option<FileFailure> {
    match mode {
        FileMode::Exists => {
            // Existence already verified by the caller.
            None
        }
        FileMode::Exact => {
            let exp = match std::fs::read(exp_path) { Ok(v) => v,
                Err(e) => return Some(io_fail(rel, e)) };
            let act = match std::fs::read(act_path) { Ok(v) => v,
                Err(e) => return Some(io_fail(rel, e)) };
            if exp == act { None } else {
                Some(mode_fail(rel, "exact",
                    format!("byte mismatch ({} vs {} bytes)", exp.len(), act.len())))
            }
        }
        FileMode::Diff => {
            text_diff(rel, exp_path, act_path, line_scrub, None)
        }
        FileMode::Fuzzy { threshold_pct } => {
            text_diff(rel, exp_path, act_path, line_scrub, Some(*threshold_pct))
        }
        FileMode::Contains { patterns } => {
            let act = match std::fs::read_to_string(act_path) { Ok(v) => v,
                Err(e) => return Some(io_fail(rel, e)) };
            let scrubbed = apply_scrub(&act, line_scrub);
            for p in patterns {
                if !p.is_match(&scrubbed) {
                    return Some(mode_fail(rel, "contains",
                        format!("pattern {:?} did not match", p.as_str())));
                }
            }
            None
        }
        FileMode::Schema { schema_path } => {
            let schema_text = match std::fs::read_to_string(schema_path) {
                Ok(v) => v,
                Err(e) => return Some(io_fail(rel, e)),
            };
            let schema_json: serde_json::Value = match serde_json::from_str(&schema_text) {
                Ok(v) => v,
                Err(e) => return Some(mode_fail(rel, "schema",
                    format!("schema not valid JSON: {e}"))),
            };
            let validator = match jsonschema::validator_for(&schema_json) {
                Ok(c) => c,
                Err(e) => return Some(mode_fail(rel, "schema",
                    format!("schema compile: {e}"))),
            };
            let act = match std::fs::read_to_string(act_path) { Ok(v) => v,
                Err(e) => return Some(io_fail(rel, e)) };
            let act_json: serde_json::Value = match serde_json::from_str(&act) {
                Ok(v) => v,
                Err(e) => return Some(mode_fail(rel, "schema",
                    format!("actual not valid JSON: {e}"))),
            };
            let errs: Vec<_> = validator.iter_errors(&act_json).collect();
            if errs.is_empty() { None }
            else {
                let detail = errs.iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join("; ");
                Some(mode_fail(rel, "schema", detail))
            }
        }
    }
}

fn text_diff(
    rel:        &str,
    exp_path:   &Path,
    act_path:   &Path,
    line_scrub: &[(Regex, String)],
    fuzzy_pct:  Option<f64>,
) -> Option<FileFailure> {
    let exp = match std::fs::read_to_string(exp_path) { Ok(v) => v,
        Err(e) => return Some(io_fail(rel, e)) };
    let act = match std::fs::read_to_string(act_path) { Ok(v) => v,
        Err(e) => return Some(io_fail(rel, e)) };
    let exp_s = apply_scrub(&exp, line_scrub);
    let act_s = apply_scrub(&act, line_scrub);
    let exp_lines: Vec<&str> = exp_s.lines().collect();
    let act_lines: Vec<&str> = act_s.lines().collect();
    let max_len = exp_lines.len().max(act_lines.len());
    let mut mismatches = 0usize;
    let mut sample = String::new();
    for i in 0..max_len {
        let e = exp_lines.get(i).copied().unwrap_or("");
        let a = act_lines.get(i).copied().unwrap_or("");
        if e != a {
            mismatches += 1;
            if sample.lines().count() < 20 {
                sample.push_str(&format!("@@ line {} @@\n-{}\n+{}\n", i + 1, e, a));
            }
        }
    }
    if mismatches == 0 { return None; }
    if let Some(pct) = fuzzy_pct {
        let total = max_len.max(1);
        let pct_actual = (mismatches as f64 / total as f64) * 100.0;
        if pct_actual <= pct {
            return None;
        }
        return Some(mode_fail(rel, "fuzzy", format!(
            "{:.1}% mismatching lines (threshold: {:.1}%); first diffs:\n{}",
            pct_actual, pct, sample,
        )));
    }
    Some(mode_fail(rel, "diff", sample))
}

fn apply_scrub(s: &str, scrub: &[(Regex, String)]) -> String {
    let mut out = s.to_string();
    for (re, repl) in scrub {
        out = re.replace_all(&out, repl.as_str()).into_owned();
    }
    out
}

fn io_fail(rel: &str, e: std::io::Error) -> FileFailure {
    FileFailure { path: rel.to_string(), kind: FileFailureKind::Io(e.to_string()) }
}

fn mode_fail(rel: &str, mode: &'static str, detail: String) -> FileFailure {
    FileFailure {
        path: rel.to_string(),
        kind: FileFailureKind::ModeMismatch { mode, detail },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn touch(p: &Path, body: &[u8]) {
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, body).unwrap();
    }

    fn make_dirs() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let d = tempdir().unwrap();
        let exp = d.path().join("expected");
        let act = d.path().join("actual");
        std::fs::create_dir_all(&exp).unwrap();
        std::fs::create_dir_all(&act).unwrap();
        (d, exp, act)
    }

    // ─── default mode by extension ────────────────────────────────

    #[test]
    fn default_mode_text_extensions_get_diff() {
        for ext in ["md","txt","json","ndjson","csv","html","yaml","toml","log"] {
            let p = PathBuf::from(format!("x.{ext}"));
            assert!(matches!(default_mode_for(&p), FileMode::Diff),
                "ext .{ext} should default to Diff");
        }
    }

    #[test]
    fn default_mode_unknown_gets_exact() {
        for ext in ["pdf","png","jpg","bin",""] {
            let p = PathBuf::from(format!("x.{ext}"));
            assert!(matches!(default_mode_for(&p), FileMode::Exact),
                "ext .{ext} should default to Exact");
        }
    }

    // ─── Exact mode ──────────────────────────────────────────────

    #[test]
    fn exact_mode_passes_when_bytes_match() {
        let (_d, exp, act) = make_dirs();
        touch(&exp.join("output/a.bin"), b"\x00\x01\xff");
        touch(&act.join("output/a.bin"), b"\x00\x01\xff");
        let f = diff_tree(&exp, &act, None, &[], &[], &[]).unwrap();
        assert!(f.is_empty(), "{f:?}");
    }

    #[test]
    fn exact_mode_fails_on_byte_mismatch() {
        let (_d, exp, act) = make_dirs();
        touch(&exp.join("output/a.bin"), b"\x00\x01\xff");
        touch(&act.join("output/a.bin"), b"\x00\x02\xff");
        let f = diff_tree(&exp, &act, None, &[], &[], &[]).unwrap();
        assert_eq!(f.len(), 1);
        assert!(matches!(f[0].kind, FileFailureKind::ModeMismatch { mode: "exact", .. }));
    }

    // ─── Diff mode ───────────────────────────────────────────────

    #[test]
    fn diff_mode_passes_after_scrub() {
        let (_d, exp, act) = make_dirs();
        touch(&exp.join("output/x.md"), b"id=<ID>\nbody");
        touch(&act.join("output/x.md"), b"id=msgbatch_01HX9ZX\nbody");
        let scrub = vec![(
            Regex::new(r"msgbatch_[A-Za-z0-9]+").unwrap(),
            "<ID>".to_string(),
        )];
        let f = diff_tree(&exp, &act, None, &[], &[], &scrub).unwrap();
        assert!(f.is_empty(), "{f:?}");
    }

    #[test]
    fn diff_mode_reports_first_mismatching_line() {
        let (_d, exp, act) = make_dirs();
        touch(&exp.join("output/x.md"), b"line1\nline2\nline3\n");
        touch(&act.join("output/x.md"), b"line1\nLINE2\nline3\n");
        let f = diff_tree(&exp, &act, None, &[], &[], &[]).unwrap();
        assert_eq!(f.len(), 1);
        match &f[0].kind {
            FileFailureKind::ModeMismatch { mode, detail } => {
                assert_eq!(*mode, "diff");
                assert!(detail.contains("@@ line 2"), "got: {detail}");
            }
            other => panic!("expected ModeMismatch, got {other:?}"),
        }
    }

    // ─── Missing / unexpected ────────────────────────────────────

    #[test]
    fn missing_in_actual_is_failure() {
        let (_d, exp, act) = make_dirs();
        touch(&exp.join("output/a.md"), b"x");
        std::fs::create_dir_all(act.join("output")).unwrap();
        let f = diff_tree(&exp, &act, None, &[], &[], &[]).unwrap();
        assert_eq!(f.len(), 1);
        assert!(matches!(f[0].kind, FileFailureKind::MissingInActual));
    }

    #[test]
    fn unexpected_in_actual_is_failure() {
        let (_d, exp, act) = make_dirs();
        std::fs::create_dir_all(exp.join("output")).unwrap();
        touch(&act.join("output/extra.md"), b"x");
        let f = diff_tree(&exp, &act, None, &[], &[], &[]).unwrap();
        assert_eq!(f.len(), 1);
        assert!(matches!(f[0].kind, FileFailureKind::UnexpectedInActual));
    }

    #[test]
    fn fs_ignore_glob_skips_matching_files() {
        let (_d, exp, act) = make_dirs();
        std::fs::create_dir_all(exp.join("output")).unwrap();
        touch(&act.join("output/junk.lock"), b"x");
        // Without ignore, this would be UnexpectedInActual.
        let f = diff_tree(&exp, &act, None, &["**/*.lock".to_string()], &[], &[]).unwrap();
        assert!(f.is_empty(), "{f:?}");
    }

    // ─── Schema mode ─────────────────────────────────────────────

    #[test]
    fn schema_mode_passes_when_actual_matches() {
        let (_d, exp, act) = make_dirs();
        let case_dir = exp.parent().unwrap();
        touch(&case_dir.join("schemas/r.json"), br#"{"type":"object","required":["a"]}"#);
        touch(&exp.join("output/r.json"), b"{}");                // expected file presence only
        touch(&act.join("output/r.json"), br#"{"a":1}"#);
        let overrides = parse_overrides(&[FileOverrideYaml {
            path: "output/r.json".into(),
            mode: "schema".into(),
            schema: Some("schemas/r.json".into()),
            patterns: None, threshold_pct: None,
        }], case_dir).unwrap();
        let f = diff_tree(&exp, &act, None, &[], &overrides, &[]).unwrap();
        assert!(f.is_empty(), "{f:?}");
    }

    #[test]
    fn schema_mode_fails_when_actual_violates() {
        let (_d, exp, act) = make_dirs();
        let case_dir = exp.parent().unwrap();
        touch(&case_dir.join("schemas/r.json"),
            br#"{"type":"object","required":["a"]}"#);
        touch(&exp.join("output/r.json"), b"{}");
        touch(&act.join("output/r.json"), br#"{"b":1}"#);   // missing required "a"
        let overrides = parse_overrides(&[FileOverrideYaml {
            path: "output/r.json".into(),
            mode: "schema".into(),
            schema: Some("schemas/r.json".into()),
            patterns: None, threshold_pct: None,
        }], case_dir).unwrap();
        let f = diff_tree(&exp, &act, None, &[], &overrides, &[]).unwrap();
        assert_eq!(f.len(), 1);
        match &f[0].kind {
            FileFailureKind::ModeMismatch { mode, .. } => assert_eq!(*mode, "schema"),
            other => panic!("expected ModeMismatch, got {other:?}"),
        }
    }

    // ─── Contains mode ───────────────────────────────────────────

    #[test]
    fn contains_mode_passes_when_all_patterns_match() {
        let (_d, exp, act) = make_dirs();
        touch(&exp.join("output/x.txt"), b"placeholder");
        touch(&act.join("output/x.txt"), b"start\nprocessed 42 rows\ncompleted in 100ms\nend");
        let overrides = parse_overrides(&[FileOverrideYaml {
            path: "output/x.txt".into(),
            mode: "contains".into(),
            patterns: Some(vec![
                r"processed \d+ rows".into(),
                r"completed in \d+ms".into(),
            ]),
            schema: None, threshold_pct: None,
        }], exp.parent().unwrap()).unwrap();
        let f = diff_tree(&exp, &act, None, &[], &overrides, &[]).unwrap();
        assert!(f.is_empty(), "{f:?}");
    }

    #[test]
    fn contains_mode_fails_when_any_pattern_missing() {
        let (_d, exp, act) = make_dirs();
        touch(&exp.join("output/x.txt"), b"placeholder");
        touch(&act.join("output/x.txt"), b"start\nprocessed 42 rows\nend");
        let overrides = parse_overrides(&[FileOverrideYaml {
            path: "output/x.txt".into(),
            mode: "contains".into(),
            patterns: Some(vec![
                r"processed \d+ rows".into(),
                r"completed in \d+ms".into(),  // not in actual
            ]),
            schema: None, threshold_pct: None,
        }], exp.parent().unwrap()).unwrap();
        let f = diff_tree(&exp, &act, None, &[], &overrides, &[]).unwrap();
        assert_eq!(f.len(), 1);
    }

    // ─── Fuzzy mode ──────────────────────────────────────────────

    #[test]
    fn fuzzy_mode_passes_when_under_threshold() {
        let (_d, exp, act) = make_dirs();
        // 10 lines; 1 differs (10%); threshold 20% → pass.
        let exp_body: Vec<u8> = (0..10).map(|i| format!("line{i}\n")).collect::<String>().into_bytes();
        let mut act_body: Vec<u8> = exp_body.clone();
        // Mutate line 5
        let act_str = String::from_utf8(act_body).unwrap()
            .replace("line5", "different5");
        act_body = act_str.into_bytes();
        touch(&exp.join("output/x.md"), &exp_body);
        touch(&act.join("output/x.md"), &act_body);
        let overrides = parse_overrides(&[FileOverrideYaml {
            path: "output/x.md".into(),
            mode: "fuzzy".into(),
            threshold_pct: Some(20.0),
            patterns: None, schema: None,
        }], exp.parent().unwrap()).unwrap();
        let f = diff_tree(&exp, &act, None, &[], &overrides, &[]).unwrap();
        assert!(f.is_empty(), "{f:?}");
    }

    #[test]
    fn fuzzy_mode_fails_when_over_threshold() {
        let (_d, exp, act) = make_dirs();
        let exp_body: Vec<u8> = (0..10).map(|i| format!("line{i}\n")).collect::<String>().into_bytes();
        // 5/10 = 50% mismatch; threshold 20% → fail.
        let act_body = (0..10).map(|i| {
            if i < 5 { format!("DIFFERENT{i}\n") } else { format!("line{i}\n") }
        }).collect::<String>().into_bytes();
        touch(&exp.join("output/x.md"), &exp_body);
        touch(&act.join("output/x.md"), &act_body);
        let overrides = parse_overrides(&[FileOverrideYaml {
            path: "output/x.md".into(),
            mode: "fuzzy".into(),
            threshold_pct: Some(20.0),
            patterns: None, schema: None,
        }], exp.parent().unwrap()).unwrap();
        let f = diff_tree(&exp, &act, None, &[], &overrides, &[]).unwrap();
        assert_eq!(f.len(), 1);
    }

    // ─── Exists mode ─────────────────────────────────────────────

    #[test]
    fn exists_mode_passes_with_different_content() {
        let (_d, exp, act) = make_dirs();
        touch(&exp.join("output/x.bin"), b"placeholder");
        touch(&act.join("output/x.bin"), b"completely different bytes");
        let overrides = parse_overrides(&[FileOverrideYaml {
            path: "output/x.bin".into(),
            mode: "exists".into(),
            patterns: None, schema: None, threshold_pct: None,
        }], exp.parent().unwrap()).unwrap();
        let f = diff_tree(&exp, &act, None, &[], &overrides, &[]).unwrap();
        assert!(f.is_empty(), "{f:?}");
    }

    // ─── fs_check filter ─────────────────────────────────────────

    #[test]
    fn fs_check_limits_walked_subdirs() {
        let (_d, exp, act) = make_dirs();
        touch(&exp.join("output/a.md"), b"x");
        touch(&exp.join("temp/b.md"),   b"x");
        touch(&act.join("output/a.md"), b"x");
        // temp/ NOT mirrored in actual.
        // Without filter, "missing in actual" for temp/b.md.
        let f_no_filter = diff_tree(&exp, &act, None, &[], &[], &[]).unwrap();
        assert_eq!(f_no_filter.len(), 1);
        // With fs_check=["output"], temp is skipped → pass.
        let f_filtered = diff_tree(&exp, &act,
            Some(&["output".to_string()]), &[], &[], &[]).unwrap();
        assert!(f_filtered.is_empty(), "{f_filtered:?}");
    }
}
