//! Per-session context + DPE_* environment variable computation.
//!
//! Per SPEC §2.3 + §10: runner injects a fixed set of env vars into every
//! spawned tool so the framework can resolve `$prefix/...` paths.
//!
//! Session lifecycle details (session id format, cleanup) live in session.rs.
//! This module is just the data carrier + env-var mapper.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::types::CacheMode;

/// Per-run context that translates 1:1 into `DPE_*` env vars plus the
/// `PathResolver` prefix map.
#[derive(Debug, Clone)]
pub struct SessionContext {
    pub pipeline_dir: PathBuf,
    pub pipeline_name: String,
    pub variant: String,
    pub session_id: String,
    pub input: PathBuf,
    pub output: PathBuf,
    pub cache_mode: CacheMode,
}

impl SessionContext {
    /// Derive the session dir under `<pipeline_dir>/sessions/<id>_<variant>/`.
    pub fn session_dir(&self) -> PathBuf {
        self.pipeline_dir.join("sessions")
            .join(format!("{}_{}", self.session_id, self.variant))
    }
    pub fn configs_dir(&self) -> PathBuf { self.pipeline_dir.join("configs") }
    pub fn storage_dir(&self) -> PathBuf { self.pipeline_dir.join("storage") }
    pub fn temp_dir(&self)    -> PathBuf { self.pipeline_dir.join("temp") }

    /// Assemble the full DPE_* env-var set for a given stage instance.
    /// Existing env vars from the runner process are NOT included — caller
    /// merges with `std::env::vars()` when desired.
    pub fn env_for_stage(&self, stage_id: &str, instance_idx: u32) -> BTreeMap<String, String> {
        let mut m = BTreeMap::new();
        m.insert("DPE_PIPELINE_DIR".into(),   to_str(&self.pipeline_dir));
        m.insert("DPE_PIPELINE_NAME".into(),  self.pipeline_name.clone());
        m.insert("DPE_VARIANT".into(),        self.variant.clone());
        m.insert("DPE_SESSION_ID".into(),     self.session_id.clone());
        m.insert("DPE_STAGE_ID".into(),       stage_id.to_string());
        m.insert("DPE_STAGE_INSTANCE".into(), instance_idx.to_string());
        m.insert("DPE_INPUT".into(),          to_str(&self.input));
        m.insert("DPE_OUTPUT".into(),         to_str(&self.output));
        m.insert("DPE_CONFIGS".into(),        to_str(&self.configs_dir()));
        m.insert("DPE_STORAGE".into(),        to_str(&self.storage_dir()));
        m.insert("DPE_TEMP".into(),           to_str(&self.temp_dir()));
        m.insert("DPE_SESSION".into(),        to_str(&self.session_dir()));
        m.insert("DPE_CACHE_MODE".into(),     cache_mode_str(self.cache_mode).into());
        m
    }

    /// Prefix map used by PathResolver to resolve `$prefix/...` in settings.
    pub fn prefix_map(&self) -> BTreeMap<String, PathBuf> {
        let mut m = BTreeMap::new();
        m.insert("input".into(),   self.input.clone());
        m.insert("output".into(),  self.output.clone());
        m.insert("configs".into(), self.configs_dir());
        m.insert("storage".into(), self.storage_dir());
        m.insert("temp".into(),    self.temp_dir());
        m.insert("session".into(), self.session_dir());
        m
    }
}

fn to_str(p: &Path) -> String {
    p.to_string_lossy().to_string()
}

fn cache_mode_str(m: CacheMode) -> &'static str {
    match m {
        CacheMode::Use     => "use",
        CacheMode::Refresh => "refresh",
        CacheMode::Bypass  => "bypass",
        CacheMode::Off     => "off",
    }
}

/// Generate a session id: YYYYMMDD-HHMMSS-xxxx (hex).
pub fn new_session_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = now.as_secs();
    let nanos = now.subsec_nanos();
    // 4-hex-char suffix for collision-safety within the same second
    let suffix = xxhash_rust::xxh3::xxh3_64(&nanos.to_le_bytes()) & 0xFFFF;
    format!("{}-{:04x}", format_unix_time(secs), suffix)
}

fn format_unix_time(epoch_secs: u64) -> String {
    // Unix-time to YYYYMMDD-HHMMSS without chrono — manual conversion is ~25 lines.
    const SECS_PER_DAY: u64 = 86_400;
    let days_since_epoch = epoch_secs / SECS_PER_DAY;
    let sod = epoch_secs % SECS_PER_DAY;
    let h = sod / 3600;
    let m = (sod % 3600) / 60;
    let s = sod % 60;

    // Days-to-YMD via direct algorithm (Howard Hinnant 2010):
    let z = days_since_epoch as i64 + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;                 // day of era [0, 146096]
    let yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365; // year of era [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365*yoe + yoe/4 - yoe/100);          // day of year [0, 365]
    let mp = (5*doy + 2) / 153;                           // month of year [0, 11]
    let d = doy - (153*mp + 2)/5 + 1;                     // day of month [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { y + 1 } else { y };

    format!("{:04}{:02}{:02}-{:02}{:02}{:02}", year, month, d, h, m, s)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_ctx() -> SessionContext {
        SessionContext {
            pipeline_dir:  PathBuf::from("/pipes/my"),
            pipeline_name: "my".into(),
            variant:       "main".into(),
            session_id:    "20260420-153012-af01".into(),
            input:         PathBuf::from("/data/in"),
            output:        PathBuf::from("/data/out"),
            cache_mode:    CacheMode::Use,
        }
    }

    #[test] fn session_dir_derived() {
        assert_eq!(sample_ctx().session_dir(),
            PathBuf::from("/pipes/my/sessions/20260420-153012-af01_main"));
    }

    #[test] fn env_for_stage_includes_all_prefixes() {
        let env = sample_ctx().env_for_stage("scan-001", 0);
        for k in ["DPE_PIPELINE_DIR","DPE_PIPELINE_NAME","DPE_VARIANT",
                  "DPE_SESSION_ID","DPE_STAGE_ID","DPE_STAGE_INSTANCE",
                  "DPE_INPUT","DPE_OUTPUT","DPE_CONFIGS","DPE_STORAGE",
                  "DPE_TEMP","DPE_SESSION","DPE_CACHE_MODE"] {
            assert!(env.contains_key(k), "missing {}", k);
        }
    }

    #[test] fn env_stage_id_varies_per_stage() {
        let c = sample_ctx();
        let a = c.env_for_stage("a-001", 0);
        let b = c.env_for_stage("b-002", 3);
        assert_eq!(a["DPE_STAGE_ID"], "a-001");
        assert_eq!(b["DPE_STAGE_ID"], "b-002");
        assert_eq!(a["DPE_STAGE_INSTANCE"], "0");
        assert_eq!(b["DPE_STAGE_INSTANCE"], "3");
    }

    #[test] fn cache_mode_renders_correctly() {
        let mut c = sample_ctx();
        for (mode, expected) in [
            (CacheMode::Use, "use"), (CacheMode::Refresh, "refresh"),
            (CacheMode::Bypass, "bypass"), (CacheMode::Off, "off"),
        ] {
            c.cache_mode = mode;
            assert_eq!(c.env_for_stage("x", 0)["DPE_CACHE_MODE"], expected);
        }
    }

    #[test] fn prefix_map_matches_env() {
        let c = sample_ctx();
        let env = c.env_for_stage("x", 0);
        let pm  = c.prefix_map();
        assert_eq!(to_str(&pm["input"]),   env["DPE_INPUT"]);
        assert_eq!(to_str(&pm["output"]),  env["DPE_OUTPUT"]);
        assert_eq!(to_str(&pm["session"]), env["DPE_SESSION"]);
        assert_eq!(to_str(&pm["storage"]), env["DPE_STORAGE"]);
        assert_eq!(to_str(&pm["temp"]),    env["DPE_TEMP"]);
        assert_eq!(to_str(&pm["configs"]), env["DPE_CONFIGS"]);
    }

    #[test] fn session_id_format_matches_spec() {
        let id = new_session_id();
        assert_eq!(id.len(), 20, "format: YYYYMMDD-HHMMSS-xxxx -> 20 chars, got {}", id);
        let parts: Vec<&str> = id.split('-').collect();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0].len(), 8); // date
        assert_eq!(parts[1].len(), 6); // time
        assert_eq!(parts[2].len(), 4); // hex suffix
        assert!(parts[0].chars().all(|c| c.is_ascii_digit()));
        assert!(parts[1].chars().all(|c| c.is_ascii_digit()));
        assert!(parts[2].chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test] fn session_ids_unique_across_calls() {
        let mut ids = std::collections::HashSet::new();
        for _ in 0..100 {
            ids.insert(new_session_id());
            // brief spin to get different nanos
            for _ in 0..1000 { std::hint::black_box(()); }
        }
        // Most should be unique (same-second collisions handled by suffix)
        assert!(ids.len() > 50, "expected diverse ids, got {} unique out of 100", ids.len());
    }

    #[test] fn format_unix_time_epoch_zero() {
        assert_eq!(format_unix_time(0), "19700101-000000");
    }

    #[test] fn format_unix_time_known_value() {
        // 2020-03-15 00:00:00 UTC = 1584230400
        assert_eq!(format_unix_time(1_584_230_400), "20200315-000000");
    }
}
