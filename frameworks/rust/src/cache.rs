//! Cache helper used by `ctx.cached(...)`. Mirrors the TS and Python
//! framework cache modules — same DPE_CACHE_MODE semantics, same
//! on-disk layout (`$DPE_STORAGE/<namespace>/<hash>.json`), same
//! canonical-JSON key hashing.
//!
//! Failure modes (cache-disabling, NOT errors propagated to user):
//!   - DPE_STORAGE not set      → cache disabled, every call produces
//!   - cache file unreadable    → treat as miss, log warn
//!   - cache file unparseable   → treat as miss, log warn
//!   - producer fails           → returns its Err to caller (no cache write)

use std::fs;
use std::io;
use std::path::PathBuf;

use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

use crate::envelope;

/// Read DPE_CACHE_MODE; default to "use". Unrecognized → "use".
pub fn read_cache_mode() -> &'static str {
    match std::env::var("DPE_CACHE_MODE").as_deref() {
        Ok("use")     => "use",
        Ok("refresh") => "refresh",
        Ok("bypass")  => "bypass",
        Ok("off")     => "off",
        _ => "use",
    }
}

/// Compute the on-disk path for (namespace, key). Returns None when
/// DPE_STORAGE isn't set — caller treats that as cache disabled.
pub fn cache_path(namespace: &str, key: &Value) -> Option<PathBuf> {
    let storage = std::env::var("DPE_STORAGE").ok()?;
    let canonical = canonical_json(key);
    let hash = blake2b_hex32(&canonical);
    Some(PathBuf::from(storage).join(namespace).join(format!("{hash}.json")))
}

/// Cache-around-`produce` helper. Honors the four cache modes. Used by
/// `Context::cached`.
///
/// Producer return type is `serde_json::Value` for simplicity. Generic
/// helpers can wrap this if a typed return is preferred.
pub fn cached_impl<T, F>(
    namespace: &str,
    key: &Value,
    produce: F,
) -> io::Result<T>
where
    T: Serialize + DeserializeOwned,
    F: FnOnce() -> io::Result<T>,
{
    let mode = read_cache_mode();
    let path = cache_path(namespace, key);

    let can_read = path.is_some() && (mode == "use" || mode == "refresh");
    let can_write = path.is_some() && mode != "bypass" && mode != "off";
    let will_read = can_read && mode != "refresh";

    if will_read {
        if let Some(p) = path.as_ref() {
            if p.is_file() {
                match fs::read_to_string(p) {
                    Ok(text) => match serde_json::from_str::<T>(&text) {
                        Ok(value) => {
                            envelope::write_log(
                                &format!("cached: hit ({namespace})"),
                                "debug",
                                &mut io::stderr(),
                            );
                            return Ok(value);
                        }
                        Err(e) => warn(namespace, "parse", &e.to_string()),
                    },
                    Err(e) => warn(namespace, "read", &e.to_string()),
                }
                // Fall through to produce.
            }
        }
    }

    let result = produce()?;

    if can_write {
        if let Some(p) = path {
            if let Some(parent) = p.parent() {
                if let Err(e) = fs::create_dir_all(parent) {
                    warn(namespace, "mkdir", &e.to_string());
                    return Ok(result);
                }
            }
            match serde_json::to_vec(&result) {
                Ok(body) => {
                    if let Err(e) = fs::write(&p, body) {
                        warn(namespace, "write", &e.to_string());
                    }
                }
                Err(e) => warn(namespace, "serialize", &e.to_string()),
            }
        }
    }

    Ok(result)
}

fn warn(namespace: &str, op: &str, detail: &str) {
    envelope::write_log(
        &format!("cached: {op} failed ({namespace}) — {detail}"),
        "warn",
        &mut io::stderr(),
    );
}

/// Canonical-JSON serialize: sorted object keys, compact, no
/// whitespace. Stable across runs/platforms — hash inputs identically.
fn canonical_json(v: &Value) -> String {
    let mut out = String::new();
    write_canonical(v, &mut out);
    out
}

fn write_canonical(v: &Value, out: &mut String) {
    use std::fmt::Write;
    match v {
        Value::Null    => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => {
            // serde_json's Display for Number is canonical enough — no
            // trailing zeros, no scientific drift on integers.
            let _ = write!(out, "{n}");
        }
        Value::String(s) => {
            // serde_json::to_string handles escaping correctly.
            out.push_str(&serde_json::to_string(s).unwrap_or_else(|_| "\"\"".into()));
        }
        Value::Array(a) => {
            out.push('[');
            for (i, item) in a.iter().enumerate() {
                if i > 0 { out.push(','); }
                write_canonical(item, out);
            }
            out.push(']');
        }
        Value::Object(m) => {
            out.push('{');
            // Sort keys for stability.
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            for (i, k) in keys.iter().enumerate() {
                if i > 0 { out.push(','); }
                let key_lit = serde_json::to_string(k).unwrap_or_else(|_| "\"\"".into());
                out.push_str(&key_lit);
                out.push(':');
                write_canonical(&m[*k], out);
            }
            out.push('}');
        }
    }
}

/// blake2b hex truncated to 32 chars (16 bytes). Same width as the
/// other framework cache helpers.
fn blake2b_hex32(s: &str) -> String {
    let full = envelope::hash_string(s);  // 16 hex chars from blake2b-64
    // hash_string returns 16-char hex (8-byte digest). Pad to 32 by
    // hashing again — collision-resistance still > 64 bits, easy
    // matching across SDKs at 32 chars.
    let second = envelope::hash_string(&full);
    format!("{full}{second}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::{Mutex, MutexGuard, OnceLock};

    /// Global lock — cargo runs tests in parallel by default, but our
    /// tests mutate process env (DPE_STORAGE, DPE_CACHE_MODE) which is
    /// process-global. Without serialization, tests stomp each other.
    /// Each test acquires this guard at entry; drop releases on exit.
    fn env_lock() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        // PoisonError ignored — test failures shouldn't deadlock peers.
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap_or_else(|p| p.into_inner())
    }

    fn with_storage<R>(f: impl FnOnce(&std::path::Path) -> R) -> R {
        let _g = env_lock();
        let dir = tempfile::tempdir().expect("tempdir");
        // SAFETY: env_lock serialises all env-mutating tests.
        unsafe {
            std::env::set_var("DPE_STORAGE", dir.path());
            std::env::remove_var("DPE_CACHE_MODE");
        }
        let r = f(dir.path());
        unsafe { std::env::remove_var("DPE_STORAGE"); }
        r
    }

    #[test]
    fn read_mode_default_use() {
        let _g = env_lock();
        unsafe { std::env::remove_var("DPE_CACHE_MODE"); }
        assert_eq!(read_cache_mode(), "use");
    }

    #[test]
    fn read_mode_recognized_values() {
        let _g = env_lock();
        for m in ["use", "refresh", "bypass", "off"] {
            unsafe { std::env::set_var("DPE_CACHE_MODE", m); }
            assert_eq!(read_cache_mode(), m);
        }
        unsafe { std::env::remove_var("DPE_CACHE_MODE"); }
    }

    #[test]
    fn read_mode_garbage_falls_back() {
        let _g = env_lock();
        unsafe { std::env::set_var("DPE_CACHE_MODE", "nonsense"); }
        assert_eq!(read_cache_mode(), "use");
        unsafe { std::env::remove_var("DPE_CACHE_MODE"); }
    }

    #[test]
    fn cache_path_none_without_storage() {
        let _g = env_lock();
        unsafe { std::env::remove_var("DPE_STORAGE"); }
        assert!(cache_path("ns", &json!({"k":1})).is_none());
    }

    #[test]
    fn cache_path_key_order_stable() {
        with_storage(|_| {
            let a = cache_path("ns", &json!({"k":1, "m":2}));
            let b = cache_path("ns", &json!({"m":2, "k":1}));
            assert_eq!(a, b);
        });
    }

    #[test]
    fn cache_path_different_keys_diverge() {
        with_storage(|_| {
            let a = cache_path("ns", &json!({"k":1}));
            let b = cache_path("ns", &json!({"k":2}));
            assert_ne!(a, b);
        });
    }

    #[test]
    fn miss_calls_produce_writes_file() {
        with_storage(|_| {
            let mut calls = 0;
            let r: Value = cached_impl("ns", &json!({"k":1}), || {
                calls += 1;
                Ok(json!({"hello":"world"}))
            }).unwrap();
            assert_eq!(r, json!({"hello":"world"}));
            assert_eq!(calls, 1);
            assert!(cache_path("ns", &json!({"k":1})).unwrap().is_file());
        });
    }

    #[test]
    fn hit_skips_produce() {
        with_storage(|_| {
            let _: Value = cached_impl("ns", &json!({"k":1}), || Ok(json!({"first":true}))).unwrap();
            // Overwrite cache with seeded value.
            fs::write(
                cache_path("ns", &json!({"k":1})).unwrap(),
                serde_json::to_vec(&json!({"cached":true})).unwrap(),
            ).unwrap();
            let mut calls = 0;
            let r: Value = cached_impl("ns", &json!({"k":1}), || {
                calls += 1;
                Ok(json!({"fresh":true}))
            }).unwrap();
            assert_eq!(r, json!({"cached":true}));
            assert_eq!(calls, 0);
        });
    }

    #[test]
    fn refresh_overwrites() {
        with_storage(|_| {
            let _: Value = cached_impl("ns", &json!({"k":1}), || Ok(json!({"first":true}))).unwrap();
            unsafe { std::env::set_var("DPE_CACHE_MODE", "refresh"); }
            let r: Value = cached_impl("ns", &json!({"k":1}), || Ok(json!({"fresh":true}))).unwrap();
            assert_eq!(r, json!({"fresh":true}));
            let on_disk: Value = serde_json::from_slice(
                &fs::read(cache_path("ns", &json!({"k":1})).unwrap()).unwrap()
            ).unwrap();
            assert_eq!(on_disk, json!({"fresh":true}));
            unsafe { std::env::remove_var("DPE_CACHE_MODE"); }
        });
    }

    #[test]
    fn bypass_no_read_no_write() {
        with_storage(|_| {
            let _: Value = cached_impl("ns", &json!({"k":1}), || Ok(json!({"initial":true}))).unwrap();
            unsafe { std::env::set_var("DPE_CACHE_MODE", "bypass"); }
            let r: Value = cached_impl("ns", &json!({"k":1}), || Ok(json!({"fresh":true}))).unwrap();
            assert_eq!(r, json!({"fresh":true}));
            // Original file unchanged.
            let on_disk: Value = serde_json::from_slice(
                &fs::read(cache_path("ns", &json!({"k":1})).unwrap()).unwrap()
            ).unwrap();
            assert_eq!(on_disk, json!({"initial":true}));
            unsafe { std::env::remove_var("DPE_CACHE_MODE"); }
        });
    }

    #[test]
    fn no_storage_disables_cache() {
        let _g = env_lock();
        unsafe {
            std::env::remove_var("DPE_STORAGE");
            std::env::remove_var("DPE_CACHE_MODE");
        }
        let mut calls = 0;
        let r1: Value = cached_impl("ns", &json!({"k":1}), || {
            calls += 1; Ok(json!({"n": calls}))
        }).unwrap();
        let r2: Value = cached_impl("ns", &json!({"k":1}), || {
            calls += 1; Ok(json!({"n": calls}))
        }).unwrap();
        assert_eq!(r1, json!({"n":1}));
        assert_eq!(r2, json!({"n":2}));
    }

    #[test]
    fn producer_error_propagates() {
        with_storage(|_| {
            let result: io::Result<Value> = cached_impl("ns", &json!({"k":1}), || {
                Err(io::Error::other("kaboom"))
            });
            assert!(result.is_err());
            assert!(!cache_path("ns", &json!({"k":1})).unwrap().exists());
        });
    }

    #[test]
    fn malformed_cache_falls_back_to_produce() {
        with_storage(|_| {
            let _: Value = cached_impl("ns", &json!({"k":1}), || Ok(json!({"ok":true}))).unwrap();
            fs::write(cache_path("ns", &json!({"k":1})).unwrap(), b"not json").unwrap();
            let mut calls = 0;
            let r: Value = cached_impl("ns", &json!({"k":1}), || {
                calls += 1; Ok(json!({"recovered":true}))
            }).unwrap();
            assert_eq!(r, json!({"recovered":true}));
            assert_eq!(calls, 1);
        });
    }
}
