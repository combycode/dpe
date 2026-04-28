//! Verify subcommand: spawn the tool, feed settings+input, diff stdout.
//!
//! Case layout:
//!   verify/<case-name>/
//!     settings.json     — argv[1]
//!     input.ndjson      — piped to stdin
//!     expected.ndjson   — expected stdout (line-by-line)
//!
//! Diff: canonical JSON per line (order-preserving, ignores whitespace). If
//! `expected.ndjson` is missing, runs the tool and prints its output (useful
//! for bootstrapping expected).

use anyhow::{anyhow, bail, Context, Result};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

pub(crate) fn verify(dir: &Path) -> Result<()> {
    let meta_path = dir.join("meta.json");
    let raw = std::fs::read_to_string(&meta_path)
        .with_context(|| format!("read {:?}", meta_path))?;
    let meta: serde_json::Value = serde_json::from_str(&raw)?;

    let runtime = meta.get("runtime").and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("meta.json missing 'runtime'"))?;
    let name = meta.get("name").and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("meta.json missing 'name'"))?;

    let verify_root = dir.join("verify");
    if !verify_root.is_dir() {
        bail!("no verify/ directory in {:?}", dir);
    }

    let mut cases: Vec<PathBuf> = std::fs::read_dir(&verify_root)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .map(|e| e.path())
        .collect();
    cases.sort();

    if cases.is_empty() {
        bail!("no verify cases under {:?}", verify_root);
    }

    let mut failed = 0;
    for case in &cases {
        let case_name = case.file_name().unwrap().to_string_lossy().to_string();
        let res = run_case(dir, runtime, name, case);
        match res {
            Ok(()) => println!("[verify] {:-<40} PASS", case_name),
            Err(e) => {
                println!("[verify] {:-<40} FAIL", case_name);
                println!("         {:#}", e);
                failed += 1;
            }
        }
    }

    if failed > 0 {
        bail!("{}/{} verify cases failed", failed, cases.len());
    }
    println!("[verify] all {} case(s) passed", cases.len());
    Ok(())
}

fn run_case(dir: &Path, runtime: &str, name: &str, case: &Path) -> Result<()> {
    let settings_path = case.join("settings.json");
    let input_path = case.join("input.ndjson");
    let expected_path = case.join("expected.ndjson");
    if !settings_path.exists() { bail!("missing settings.json in {:?}", case); }
    if !input_path.exists()    { bail!("missing input.ndjson in {:?}", case); }

    let settings_raw = std::fs::read_to_string(&settings_path)?;
    // Normalise settings JSON to a single-line argv value.
    let settings_value: serde_json::Value = serde_json::from_str(&settings_raw)
        .with_context(|| "parse settings.json")?;
    let settings_str = serde_json::to_string(&settings_value)?;

    let input_bytes = std::fs::read(&input_path)?;

    let (program, args) = tool_invocation(dir, runtime, name)?;

    let mut cmd = Command::new(&program);
    cmd.args(&args).arg(&settings_str)
       .current_dir(dir)
       .stdin(Stdio::piped())
       .stdout(Stdio::piped())
       .stderr(Stdio::piped());
    let mut child = cmd.spawn()
        .with_context(|| format!("spawn {} for {:?}", program, case))?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(&input_bytes)?;
        // Drop stdin to signal EOF
    }
    let output = child.wait_with_output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("tool exited {}: {}", output.status, stderr.trim());
    }

    let got = String::from_utf8_lossy(&output.stdout).to_string();

    if !expected_path.exists() {
        println!("[verify] {:?} — expected.ndjson missing; actual output:", case);
        println!("{}", got);
        bail!("bootstrap mode — save this output as {:?}", expected_path);
    }

    let expected = std::fs::read_to_string(&expected_path)?;
    diff_ndjson(&expected, &got).with_context(|| format!("case {:?}", case))
}

/// Line-by-line canonical JSON comparison. Ignores whitespace-only differences.
fn diff_ndjson(expected: &str, got: &str) -> Result<()> {
    let exp_lines: Vec<String> = expected.lines().map(canon_json).collect();
    let got_lines: Vec<String> = got.lines().map(canon_json).collect();

    if exp_lines.len() != got_lines.len() {
        bail!(
            "line count mismatch — expected {} lines, got {}\n--- expected ---\n{}\n--- got ---\n{}",
            exp_lines.len(), got_lines.len(),
            exp_lines.join("\n"), got_lines.join("\n")
        );
    }
    for (i, (e, g)) in exp_lines.iter().zip(&got_lines).enumerate() {
        if e != g {
            bail!("line {} differs:\n  expected: {}\n  got:      {}", i + 1, e, g);
        }
    }
    Ok(())
}

fn canon_json(line: &str) -> String {
    let trimmed = line.trim();
    if trimmed.is_empty() { return String::new(); }
    match serde_json::from_str::<serde_json::Value>(trimmed) {
        Ok(v) => serde_json::to_string(&sort_keys(v)).unwrap_or_else(|_| trimmed.to_string()),
        Err(_) => trimmed.to_string(),
    }
}

/// Recursively sort object keys so that envelope key order (`{"t","id","src","v"}`
/// vs alphabetical) doesn't fail diffs. Workspace builds with serde_json's
/// `preserve_order` ON, so we re-insert keys in sorted order to make the
/// resulting Map iterate sorted on serialise.
fn sort_keys(v: serde_json::Value) -> serde_json::Value {
    use serde_json::{Map, Value};
    match v {
        Value::Array(arr) => Value::Array(arr.into_iter().map(sort_keys).collect()),
        Value::Object(obj) => {
            let mut entries: Vec<(String, Value)> = obj.into_iter().collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            let mut out = Map::new();
            for (k, child) in entries {
                out.insert(k, sort_keys(child));
            }
            Value::Object(out)
        }
        other => other,
    }
}

fn tool_invocation(dir: &Path, runtime: &str, name: &str) -> Result<(String, Vec<String>)> {
    match runtime {
        "rust" => {
            // Require the release binary built
            let bin = dir.join("target").join("release").join(format!("{}.exe", name));
            let bin = if bin.exists() { bin } else {
                dir.join("target").join("release").join(name)
            };
            if !bin.exists() {
                bail!("binary not found at {:?} — run `dpe-dev build .` first", bin);
            }
            Ok((bin.to_string_lossy().into_owned(), vec![]))
        }
        "bun" => {
            // Invoke as `bun src/main.ts`
            Ok(("bun".into(), vec!["src/main.ts".into()]))
        }
        "python" => {
            // Prefer the per-tool venv (created by `dpe-dev build` via
            // `uv venv --seed` or `python3 -m venv`). The venv has the
            // framework installed editable; system python does not.
            let pkg = name.replace('-', "_");
            let venv = dir.join(".venv");
            let venv_py = if cfg!(windows) {
                venv.join("Scripts").join("python.exe")
            } else {
                venv.join("bin").join("python")
            };
            let prog = if venv_py.exists() {
                venv_py.to_string_lossy().into_owned()
            } else {
                "python".into()
            };
            Ok((prog, vec![format!("src/{}/main.py", pkg)]))
        }
        other => bail!("unknown runtime '{}'", other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn canon_json_normalises() {
        // canon_json sorts object keys alphabetically so envelope key order
        // (e.g. spec `{"t","id","src","v"}` vs framework's serde alphabetical)
        // doesn't fail diffs. Whitespace is also stripped.
        assert_eq!(canon_json(r#"  {"b":1,"a":2}  "#), r#"{"a":2,"b":1}"#);
        assert_eq!(canon_json(r#"{"t":"d","id":"1","src":"s","v":{"x":1}}"#),
                   r#"{"id":"1","src":"s","t":"d","v":{"x":1}}"#);
        assert_eq!(canon_json("not json"), "not json");
        assert_eq!(canon_json(""), "");
    }
    #[test] fn diff_equal_passes() {
        let s = "{\"a\":1}\n{\"b\":2}\n";
        assert!(diff_ndjson(s, s).is_ok());
    }
    #[test] fn diff_different_fails() {
        let a = "{\"a\":1}\n";
        let b = "{\"a\":2}\n";
        assert!(diff_ndjson(a, b).is_err());
    }
    #[test] fn diff_with_whitespace_differences_still_passes() {
        let a = r#"{"a":1,"b":2}"#;
        let b = r#"{ "a": 1, "b": 2 }"#;
        assert!(diff_ndjson(a, b).is_ok());
    }
    #[test] fn diff_length_mismatch_fails() {
        assert!(diff_ndjson("{\"a\":1}\n", "{\"a\":1}\n{\"b\":2}\n").is_err());
    }
}
