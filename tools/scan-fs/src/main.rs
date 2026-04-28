//! scan-fs binary — DPE tool entry.
//!
//! Reads envelopes off stdin. Each envelope's `v.path` is interpreted per
//! settings.mode:
//!   - "full": treat as directory to walk; emit one envelope per matching entry.
//!   - "diff": treat the whole `v` as a previous file record; check current
//!     state on disk; emit only changed (action=modified) or removed
//!     (action=removed) entries.

use std::path::PathBuf;

use combycode_dpe::prelude::*;
use combycode_dpe::dpe_run;

use combycode_dpe_tool_scan_fs::{
    diff::{diff_one, DiffOutcome},
    scan::{scan_root, ScanEvent},
    settings::{Mode, Settings},
};

fn parse_settings(raw: &Value) -> Result<Settings, String> {
    serde_json::from_value(raw.clone())
        .map_err(|e| format!("bad settings: {}", e))
}

fn process_input(v: Value, raw_settings: &Value, ctx: &mut Context<'_>) {
    let settings = match parse_settings(raw_settings) {
        Ok(s)  => s,
        Err(e) => { ctx.error(&v, &e); return; }
    };
    match settings.mode {
        Mode::Full => handle_full(v, &settings, ctx),
        Mode::Diff => handle_diff(v, &settings, ctx),
    }
}

fn handle_full(v: Value, settings: &Settings, ctx: &mut Context<'_>) {
    let path = match v.get("path").and_then(|p| p.as_str()) {
        Some(p) => PathBuf::from(p),
        None    => { ctx.error(&v, "missing v.path"); return; }
    };
    let result = scan_root(&path, settings, |ev| match ev {
        ScanEvent::Entry(out_v)            => ctx.output(out_v, None, None),
        ScanEvent::Error { path, error }   => {
            ctx.error(&serde_json::json!({"path": path.to_string_lossy()}), &error);
        }
    });
    if let Err(e) = result {
        ctx.error(&v, &format!("scan: {}", e));
    }
}

fn handle_diff(v: Value, settings: &Settings, ctx: &mut Context<'_>) {
    match diff_one(&v, settings) {
        DiffOutcome::Unchanged                => {}             // silent drop
        DiffOutcome::Modified(out)            => ctx.output(out, None, None),
        DiffOutcome::Removed(out)             => ctx.output(out, None, None),
        DiffOutcome::BadInput(msg)            => ctx.error(&v, &msg),
        DiffOutcome::StatError(msg)           => ctx.error(&v, &msg),
    }
}

fn main() {
    dpe_run! { input: process_input };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_settings_invalid_returns_err() {
        let raw = serde_json::json!({"mode": "garbage"});
        assert!(parse_settings(&raw).is_err());
    }

    #[test]
    fn parse_settings_default_ok() {
        let raw = serde_json::json!({});
        let s = parse_settings(&raw).unwrap();
        assert!(matches!(s.mode, Mode::Full));
    }
}
