//! Scaffold a new tool: copy template + substitute placeholders + rewrite framework path.
//!
//! Frameworks discovery order:
//!   1. `--frameworks-dir` override
//!   2. `DPE_FRAMEWORKS_DIR` env var
//!   3. Walk upward from cwd / binary dir looking for `frameworks/{rust,ts,python}` (monorepo dev mode)
//!   4. Extract embedded frameworks to `~/.dpe/frameworks/<ver>/` and use that (production mode)

use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

use crate::embedded;
use crate::Runtime;

const FRAMEWORK_VERSION: &str = "2.0.0";

pub(crate) fn find_frameworks_dir(override_: Option<&Path>) -> Result<PathBuf> {
    if let Some(p) = override_ {
        if !p.is_dir() { bail!("frameworks-dir {:?} doesn't exist", p); }
        return Ok(p.to_path_buf());
    }
    if let Ok(env_val) = std::env::var("DPE_FRAMEWORKS_DIR") {
        let p = PathBuf::from(env_val);
        if !p.is_dir() { bail!("DPE_FRAMEWORKS_DIR {:?} doesn't exist", p); }
        return Ok(p);
    }
    let cwd = std::env::current_dir()?;
    if let Some(found) = walk_up_looking_for_frameworks(&cwd) { return Ok(found); }

    // Walk up from binary's location (supports installed-adjacent layouts).
    if let Ok(exe) = std::env::current_exe() {
        if let Some(parent) = exe.parent() {
            if let Some(found) = walk_up_looking_for_frameworks(parent) {
                return Ok(found);
            }
        }
    }

    // Last resort: extract embedded frameworks to ~/.dpe/frameworks/<ver>/
    // and use that. Idempotent — reuses existing extract.
    if let Some(home) = dirs::home_dir() {
        let target = home.join(".dpe").join("frameworks").join(FRAMEWORK_VERSION);
        extract_embedded_frameworks(&target)
            .with_context(|| format!("extract embedded frameworks to {:?}", target))?;
        return Ok(target);
    }

    bail!(
        "no frameworks root found. Searched from {:?} and binary location. \
         Set DPE_FRAMEWORKS_DIR or pass --frameworks-dir.",
        cwd)
}

/// Look for monorepo layout: parent dir containing `frameworks/{rust,ts,python}`.
fn walk_up_looking_for_frameworks(start: &Path) -> Option<PathBuf> {
    let mut d: &Path = start;
    loop {
        let fwdir = d.join("frameworks");
        if fwdir.is_dir()
           && fwdir.join("rust").is_dir()
           && fwdir.join("ts").is_dir()
           && fwdir.join("python").is_dir() {
            return Some(d.to_path_buf());
        }
        match d.parent() {
            Some(p) => d = p,
            None => return None,
        }
    }
}

fn extract_embedded_frameworks(target_root: &Path) -> Result<()> {
    // target_root is treated as a monorepo-shaped dir: frameworks/{rust,ts,python}
    let rust_dir   = target_root.join("frameworks/rust");
    let ts_dir     = target_root.join("frameworks/ts");
    let python_dir = target_root.join("frameworks/python");
    let already_extracted = rust_dir.join("Cargo.toml").exists()
        && ts_dir.join("package.json").exists()
        && python_dir.join("pyproject.toml").exists();
    if already_extracted { return Ok(()); }
    std::fs::create_dir_all(&rust_dir)?;
    std::fs::create_dir_all(&ts_dir)?;
    std::fs::create_dir_all(&python_dir)?;
    embedded::extract(&embedded::FRAMEWORK_RUST,   &rust_dir,   false)?;
    embedded::extract(&embedded::FRAMEWORK_TS,     &ts_dir,     false)?;
    embedded::extract(&embedded::FRAMEWORK_PYTHON, &python_dir, false)?;
    Ok(())
}

pub(crate) fn scaffold(
    name: &str,
    runtime: Runtime,
    out: &Path,
    description: &str,
    frameworks_dir_override: Option<&Path>,
) -> Result<()> {
    let name_kebab = to_kebab(name);
    let name_snake = to_snake(name);

    let fw_root = find_frameworks_dir(frameworks_dir_override)?;
    let framework_dir = fw_root.join(runtime.framework_dir_name());
    let template_dir = framework_dir.join("template");
    if !template_dir.is_dir() {
        bail!("template not found: {:?}", template_dir);
    }

    // Refuse to clobber an existing non-empty directory
    if out.exists() {
        let has_files = std::fs::read_dir(out)?.next().is_some();
        if has_files { bail!("output dir {:?} already contains files — refusing to overwrite", out); }
    } else {
        std::fs::create_dir_all(out)?;
    }

    // Compute relative framework path (from out/ to framework_dir) for Cargo
    // and Bun which accept relative paths cleanly. Python's file:// URL form
    // is fragile with relative paths on Windows, so we also expose an absolute
    // form via {{framework_abs_path}} (forward slashes, no drive-letter backslash).
    let out_abs = out.canonicalize().unwrap_or_else(|_| out.to_path_buf());
    let framework_abs = framework_dir.canonicalize().unwrap_or_else(|_| framework_dir.clone());
    let framework_rel = pathdiff::diff_paths(&framework_abs, &out_abs)
        .map(|p| p.to_string_lossy().replace('\\', "/"))
        .unwrap_or_else(|| framework_abs.to_string_lossy().replace('\\', "/"));
    // Absolute, forward-slash, strip any `\\?\` verbatim prefix if present.
    let framework_abs_str = framework_abs.to_string_lossy().replace('\\', "/");
    let framework_abs_str = framework_abs_str.trim_start_matches("//?/").to_string();

    let subs: Vec<(&str, String)> = vec![
        ("{{tool_name_kebab}}",    name_kebab.clone()),
        ("{{tool_name_snake}}",    name_snake.clone()),
        ("{{tool_name}}",          name_kebab.clone()),
        ("{{description}}",        description.to_string()),
        ("{{framework_path}}",     framework_rel.clone()),
        ("{{framework_abs_path}}", framework_abs_str.clone()),
    ];

    let mut file_count = 0;
    for entry in WalkDir::new(&template_dir).min_depth(1) {
        let entry = entry?;
        let src_path = entry.path();
        let rel = src_path.strip_prefix(&template_dir).unwrap();

        // Rename __PKG__ in the destination path (Python package dir placeholder)
        let rel_renamed = PathBuf::from(
            rel.to_string_lossy().replace("__PKG__", &name_snake)
        );
        let dst_path = out.join(&rel_renamed);

        if entry.file_type().is_dir() {
            std::fs::create_dir_all(&dst_path)?;
            continue;
        }

        // Read, substitute, write
        let mut content = std::fs::read_to_string(src_path)
            .with_context(|| format!("read {:?}", src_path))?;
        for (k, v) in &subs {
            content = content.replace(k, v);
        }
        if let Some(parent) = dst_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&dst_path, &content)
            .with_context(|| format!("write {:?}", dst_path))?;
        file_count += 1;
    }

    println!("[scaffold] wrote {} files to {:?}", file_count, out);
    println!("[scaffold] framework: {:?}", framework_dir);
    println!("[scaffold] next:");
    match runtime {
        Runtime::Rust   => println!("  cd {:?} && dpe-dev build . && dpe-dev test . && dpe-dev verify .", out),
        Runtime::Bun    => println!("  cd {:?} && dpe-dev build . && dpe-dev test . && dpe-dev verify .", out),
        Runtime::Python => println!("  cd {:?} && dpe-dev build . && dpe-dev test . && dpe-dev verify .", out),
    }
    let _ = runtime;
    Ok(())
}

fn to_kebab(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'A'..='Z' => c.to_ascii_lowercase(),
            '_' | ' ' => '-',
            _ => c,
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string()
}

fn to_snake(s: &str) -> String {
    to_kebab(s).replace('-', "_")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn kebab_from_various() {
        assert_eq!(to_kebab("Foo Bar"), "foo-bar");
        assert_eq!(to_kebab("foo_bar"), "foo-bar");
        assert_eq!(to_kebab("FooBar"), "foobar");  // lowercasing only, no camel split
        assert_eq!(to_kebab("upload-pdf"), "upload-pdf");
    }
    #[test] fn snake_from_kebab() {
        assert_eq!(to_snake("foo-bar"), "foo_bar");
        assert_eq!(to_snake("Foo Bar"), "foo_bar");
    }
    #[test] fn find_fails_gracefully_with_bad_override() {
        let bad = Path::new("/does/not/exist");
        assert!(find_frameworks_dir(Some(bad)).is_err());
    }
}
