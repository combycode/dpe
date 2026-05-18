//! Phase B — channel shape check.
//!
//! Step 1 of the per-phase run: verify the run produced exactly the
//! channels the test declared. A channel is "declared" when:
//!   - it appears in `compare.channels` (explicit), OR
//!   - its `expected/<channel>.ndjson` file exists (implicit — the
//!     user committed the expected file, so it counts as declared).
//!
//! For each declared channel: `expected/<channel>.ndjson` must exist
//! AND `.run/actual/<channel>.ndjson` must exist (Phase A always
//! creates the actual file, so this is mainly checking expected).
//!
//! For each non-empty actual channel NOT in the declared set:
//! "unexpected output." Surfaces as a fail so silent leaks (a tool
//! that suddenly starts emitting `meta` envelopes) get caught.
//!
//! Empty actual + declared but expected absent → fail (config error).
//! Empty actual + declared + empty expected → pass (channel is
//! deliberately silent for this case).

use std::path::Path;

/// One channel known to the test runner. Maps 1:1 to a filename
/// `<key>.ndjson` in both `expected/` and `.run/actual/`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ChannelKey {
    Data,
    Meta,
    Errors,
    Logs,
    Trace,
    Stats,
    /// Framework-emitted `{"type":"input"}` events on stderr — one per
    /// envelope read from stdin. Feeds the runner's `rows_in` journal
    /// counter. Opt-in for tests (same as `trace`/`stats`).
    Input,
}

impl ChannelKey {
    pub fn as_str(self) -> &'static str {
        match self {
            ChannelKey::Data   => "data",
            ChannelKey::Meta   => "meta",
            ChannelKey::Errors => "errors",
            ChannelKey::Logs   => "logs",
            ChannelKey::Trace  => "trace",
            ChannelKey::Stats  => "stats",
            ChannelKey::Input  => "input",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "data"   => Some(ChannelKey::Data),
            "meta"   => Some(ChannelKey::Meta),
            "errors" => Some(ChannelKey::Errors),
            "logs"   => Some(ChannelKey::Logs),
            "trace"  => Some(ChannelKey::Trace),
            "stats"  => Some(ChannelKey::Stats),
            "input"  => Some(ChannelKey::Input),
            _ => None,
        }
    }

    /// All channels in canonical order (matches the file write order
    /// in Phase A's writer).
    pub fn all() -> &'static [ChannelKey] {
        &[
            ChannelKey::Data, ChannelKey::Meta, ChannelKey::Errors,
            ChannelKey::Logs, ChannelKey::Trace, ChannelKey::Stats,
            ChannelKey::Input,
        ]
    }
}

/// One mismatch surfaced by the channel-shape check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelMismatch {
    /// User declared the channel (in `compare.channels`) but no
    /// `expected/<channel>.ndjson` is on disk.
    DeclaredButExpectedMissing(ChannelKey),
    /// `.run/actual/<channel>.ndjson` is non-empty but the channel
    /// is not in the declared set AND `expected/<channel>.ndjson`
    /// doesn't exist. Tool produced output the test isn't asserting
    /// against — silent regression risk; surface as a fail.
    ProducedButNotDeclared(ChannelKey),
}

impl ChannelMismatch {
    pub fn message(&self) -> String {
        match self {
            ChannelMismatch::DeclaredButExpectedMissing(k) => format!(
                "channel '{}' declared in compare.channels but \
                 expected/{}.ndjson is missing",
                k.as_str(), k.as_str(),
            ),
            ChannelMismatch::ProducedButNotDeclared(k) => format!(
                "tool emitted on '{}' channel but the test did not \
                 declare it (no compare.channels entry, no \
                 expected/{}.ndjson). Add the channel to test.yaml's \
                 compare.channels or commit an expected file.",
                k.as_str(), k.as_str(),
            ),
        }
    }
}

/// Inputs to the shape check. Kept as a small struct so the caller
/// (Phase F orchestrator) builds it once per phase.
pub struct ShapeCheckInputs<'a> {
    /// Absolute path to `expected/` (per-phase subdir if multi-phase).
    pub expected_dir: &'a Path,
    /// Absolute path to `.run/actual/`.
    pub actual_dir:   &'a Path,
    /// `compare.channels` from the effective per-phase config.
    /// `None` means "auto-detect: use channels with expected files
    /// present." `Some(empty)` means "assert no channels at all
    /// (everything declared empty)."
    pub declared:     Option<&'a [ChannelKey]>,
}

/// Run the shape check. Returns the empty vec on success.
pub fn check_channel_shape(inputs: &ShapeCheckInputs<'_>) -> Vec<ChannelMismatch> {
    let mut mismatches = Vec::new();

    // 1. Resolve the effective declared set.
    let declared_set: Vec<ChannelKey> = match inputs.declared {
        Some(list) => list.to_vec(),
        None => ChannelKey::all().iter().copied()
            .filter(|k| expected_file(inputs.expected_dir, *k).is_file())
            .collect(),
    };

    // 2. Every declared channel must have an expected file. Phase A
    //    always creates the actual file, so we only check expected.
    for k in &declared_set {
        if !expected_file(inputs.expected_dir, *k).is_file() {
            mismatches.push(ChannelMismatch::DeclaredButExpectedMissing(*k));
        }
    }

    // 3. Every channel with NON-EMPTY actual content must be either
    //    declared or have an expected file. (Empty actual + not
    //    declared = silent channel; tool produced nothing, no contract
    //    violation.)
    //
    //    Asymmetry: only `data` / `meta` / `errors` trigger this check.
    //    Framework-noise channels (`logs`, `trace`, `stats`) are
    //    OPT-IN per the proposal's non-goals — emitting them is
    //    expected baseline behaviour; they only count as test contract
    //    when the user explicitly declares them OR commits an expected
    //    file.
    for k in ChannelKey::all() {
        if !is_strict_channel(*k) { continue; }
        let actual = actual_file(inputs.actual_dir, *k);
        if !actual.is_file() { continue; }
        let len = std::fs::metadata(&actual).map(|m| m.len()).unwrap_or(0);
        if len == 0 { continue; }
        let in_declared = declared_set.contains(k);
        let has_expected = expected_file(inputs.expected_dir, *k).is_file();
        if !in_declared && !has_expected {
            mismatches.push(ChannelMismatch::ProducedButNotDeclared(*k));
        }
    }

    mismatches
}

/// Channels that count as "stage output" and trigger silent-leak
/// detection. `logs` / `trace` / `stats` are framework noise — they
/// pass silently unless the user explicitly opts in.
fn is_strict_channel(k: ChannelKey) -> bool {
    matches!(k, ChannelKey::Data | ChannelKey::Meta | ChannelKey::Errors)
}

fn expected_file(dir: &Path, k: ChannelKey) -> std::path::PathBuf {
    dir.join(format!("{}.ndjson", k.as_str()))
}

fn actual_file(dir: &Path, k: ChannelKey) -> std::path::PathBuf {
    dir.join(format!("{}.ndjson", k.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn touch(p: &Path, body: &str) {
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, body).unwrap();
    }

    #[test]
    fn parse_round_trip() {
        for k in ChannelKey::all() {
            assert_eq!(ChannelKey::parse(k.as_str()), Some(*k));
        }
        assert_eq!(ChannelKey::parse("logs"), Some(ChannelKey::Logs));
        assert_eq!(ChannelKey::parse("nope"), None);
    }

    #[test]
    fn auto_detect_mode_passes_when_only_data_present() {
        let d = tempdir().unwrap();
        let exp = d.path().join("expected"); std::fs::create_dir_all(&exp).unwrap();
        let act = d.path().join(".run/actual"); std::fs::create_dir_all(&act).unwrap();
        // Tool produced data + meta. Test declared neither channel
        // explicitly. Only data has expected/. → meta is "produced
        // but not declared" → fail.
        touch(&exp.join("data.ndjson"), "{}\n");
        touch(&act.join("data.ndjson"), "{}\n");
        touch(&act.join("meta.ndjson"), "{}\n");
        // (other channels: empty actuals from Phase A)
        for c in ["errors","logs","trace","stats","input"] {
            touch(&act.join(format!("{c}.ndjson")), "");
        }
        let mm = check_channel_shape(&ShapeCheckInputs {
            expected_dir: &exp, actual_dir: &act, declared: None,
        });
        assert_eq!(mm, vec![ChannelMismatch::ProducedButNotDeclared(ChannelKey::Meta)]);
    }

    #[test]
    fn explicit_channels_with_matching_expected_files_passes() {
        let d = tempdir().unwrap();
        let exp = d.path().join("expected"); std::fs::create_dir_all(&exp).unwrap();
        let act = d.path().join(".run/actual"); std::fs::create_dir_all(&act).unwrap();
        touch(&exp.join("data.ndjson"), "");
        touch(&exp.join("meta.ndjson"), "");
        for c in ChannelKey::all() {
            touch(&act.join(format!("{}.ndjson", c.as_str())), "");
        }
        // Both declared, both have expected files (empty), both
        // actuals empty. Pass.
        let mm = check_channel_shape(&ShapeCheckInputs {
            expected_dir: &exp, actual_dir: &act,
            declared: Some(&[ChannelKey::Data, ChannelKey::Meta]),
        });
        assert!(mm.is_empty(), "expected pass; got {mm:?}");
    }

    #[test]
    fn declared_but_expected_missing_fails() {
        let d = tempdir().unwrap();
        let exp = d.path().join("expected"); std::fs::create_dir_all(&exp).unwrap();
        let act = d.path().join(".run/actual"); std::fs::create_dir_all(&act).unwrap();
        for c in ChannelKey::all() {
            touch(&act.join(format!("{}.ndjson", c.as_str())), "");
        }
        // Declared `errors` but no expected/errors.ndjson on disk.
        let mm = check_channel_shape(&ShapeCheckInputs {
            expected_dir: &exp, actual_dir: &act,
            declared: Some(&[ChannelKey::Errors]),
        });
        assert_eq!(mm, vec![ChannelMismatch::DeclaredButExpectedMissing(ChannelKey::Errors)]);
    }

    #[test]
    fn empty_actual_not_declared_no_expected_passes() {
        // Tool didn't produce on a channel; not declared; no expected.
        // No contract violation — silent channel is fine.
        let d = tempdir().unwrap();
        let exp = d.path().join("expected"); std::fs::create_dir_all(&exp).unwrap();
        let act = d.path().join(".run/actual"); std::fs::create_dir_all(&act).unwrap();
        touch(&exp.join("data.ndjson"), "");
        for c in ChannelKey::all() {
            touch(&act.join(format!("{}.ndjson", c.as_str())), "");
        }
        let mm = check_channel_shape(&ShapeCheckInputs {
            expected_dir: &exp, actual_dir: &act,
            declared: Some(&[ChannelKey::Data]),
        });
        assert!(mm.is_empty(), "got {mm:?}");
    }

    #[test]
    fn empty_actual_declared_with_expected_passes() {
        // "Tool MUST produce no errors on this case" — declared errors,
        // committed empty expected/errors.ndjson, actual is empty. Pass.
        let d = tempdir().unwrap();
        let exp = d.path().join("expected"); std::fs::create_dir_all(&exp).unwrap();
        let act = d.path().join(".run/actual"); std::fs::create_dir_all(&act).unwrap();
        touch(&exp.join("errors.ndjson"), "");
        for c in ChannelKey::all() {
            touch(&act.join(format!("{}.ndjson", c.as_str())), "");
        }
        let mm = check_channel_shape(&ShapeCheckInputs {
            expected_dir: &exp, actual_dir: &act,
            declared: Some(&[ChannelKey::Errors]),
        });
        assert!(mm.is_empty(), "got {mm:?}");
    }

    #[test]
    fn produced_but_expected_present_passes() {
        // Auto-detect mode: tool produced data; expected/data.ndjson
        // exists → channel is implicitly declared, pass.
        let d = tempdir().unwrap();
        let exp = d.path().join("expected"); std::fs::create_dir_all(&exp).unwrap();
        let act = d.path().join(".run/actual"); std::fs::create_dir_all(&act).unwrap();
        touch(&exp.join("data.ndjson"), "{}\n");
        touch(&act.join("data.ndjson"), "{}\n");
        for c in ["meta","errors","logs","trace","stats","input"] {
            touch(&act.join(format!("{c}.ndjson")), "");
        }
        let mm = check_channel_shape(&ShapeCheckInputs {
            expected_dir: &exp, actual_dir: &act, declared: None,
        });
        assert!(mm.is_empty(), "got {mm:?}");
    }

    #[test]
    fn multiple_mismatches_all_returned() {
        // Both kinds of mismatch in one run — the runner shows the
        // user EVERY problem, not the first one.
        let d = tempdir().unwrap();
        let exp = d.path().join("expected"); std::fs::create_dir_all(&exp).unwrap();
        let act = d.path().join(".run/actual"); std::fs::create_dir_all(&act).unwrap();
        // Declared meta but no expected.
        // Tool also produced errors which is undeclared (errors is
        // a strict channel — silent leak should fail).
        touch(&exp.join("data.ndjson"), "");
        touch(&act.join("data.ndjson"), "");
        touch(&act.join("errors.ndjson"), "{}\n");
        for c in ["meta","logs","trace","stats","input"] {
            touch(&act.join(format!("{c}.ndjson")), "");
        }
        let mm = check_channel_shape(&ShapeCheckInputs {
            expected_dir: &exp, actual_dir: &act,
            declared: Some(&[ChannelKey::Data, ChannelKey::Meta]),
        });
        assert_eq!(mm.len(), 2, "got {mm:?}");
        assert!(mm.contains(&ChannelMismatch::DeclaredButExpectedMissing(ChannelKey::Meta)));
        assert!(mm.contains(&ChannelMismatch::ProducedButNotDeclared(ChannelKey::Errors)));
    }

    #[test]
    fn framework_noise_channels_pass_silently() {
        // logs/trace/stats/input are opt-in: produced but not declared
        // is OK. `input` covers the framework's per-envelope-read event
        // (`{"type":"input"}`) which fires for every non-source tool.
        let d = tempdir().unwrap();
        let exp = d.path().join("expected"); std::fs::create_dir_all(&exp).unwrap();
        let act = d.path().join(".run/actual"); std::fs::create_dir_all(&act).unwrap();
        touch(&exp.join("data.ndjson"), "{}\n");
        touch(&act.join("data.ndjson"), "{}\n");
        touch(&act.join("trace.ndjson"), "{\"type\":\"trace\"}\n");
        touch(&act.join("logs.ndjson"),  "{\"type\":\"log\"}\n");
        touch(&act.join("stats.ndjson"), "{\"type\":\"stats\"}\n");
        touch(&act.join("input.ndjson"), "{\"type\":\"input\",\"id\":\"\",\"src\":\"\"}\n");
        for c in ["meta", "errors"] {
            touch(&act.join(format!("{c}.ndjson")), "");
        }
        let mm = check_channel_shape(&ShapeCheckInputs {
            expected_dir: &exp, actual_dir: &act, declared: None,
        });
        assert!(mm.is_empty(), "framework channels should pass; got {mm:?}");
    }
}
