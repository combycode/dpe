//! Embedded assets: dev-workspace-template + per-runtime framework templates.
//!
//! These are baked into the dpe-dev binary at compile time via `include_dir!`.
//! At runtime they're extracted to the user's workspace (skill pack) or
//! framework cache (`~/.dpe/frameworks/<runtime>/<version>/`).
//!
//! Keeps everything self-contained — no network needed for Phase 1.

use include_dir::{include_dir, Dir};
use std::path::Path;

/// The dev-workspace skeleton (`.claude/`, fixtures/, README.md).
/// Extracted by `dpe-dev setup`.
pub(crate) static WORKSPACE_TEMPLATE: Dir<'_> =
    include_dir!("$CARGO_MANIFEST_DIR/../dev-workspace-template");

/// Rust framework source + template/.
/// Extracted to `~/.dpe/frameworks/rust/<version>/` on first scaffold.
pub(crate) static FRAMEWORK_RUST: Dir<'_> =
    include_dir!("$CARGO_MANIFEST_DIR/../frameworks/rust");

/// TS framework.
pub(crate) static FRAMEWORK_TS: Dir<'_> =
    include_dir!("$CARGO_MANIFEST_DIR/../frameworks/ts");

/// Python framework.
pub(crate) static FRAMEWORK_PYTHON: Dir<'_> =
    include_dir!("$CARGO_MANIFEST_DIR/../frameworks/python");

/// Extract a `Dir` to a filesystem path. Creates directories as needed.
/// Refuses to overwrite existing files unless `force` is true.
pub(crate) fn extract(dir: &Dir<'_>, dest: &Path, force: bool) -> std::io::Result<usize> {
    std::fs::create_dir_all(dest)?;
    let mut count = 0;
    extract_recursive(dir, dest, force, &mut count)?;
    Ok(count)
}

fn extract_recursive(
    dir: &Dir<'_>,
    dest: &Path,
    force: bool,
    count: &mut usize,
) -> std::io::Result<()> {
    for entry in dir.entries() {
        match entry {
            include_dir::DirEntry::Dir(sub) => {
                let name = sub.path().file_name()
                    .ok_or_else(|| std::io::Error::other("unnamed dir"))?;
                let sub_dest = dest.join(name);
                std::fs::create_dir_all(&sub_dest)?;
                extract_recursive(sub, &sub_dest, force, count)?;
            }
            include_dir::DirEntry::File(f) => {
                let name = f.path().file_name()
                    .ok_or_else(|| std::io::Error::other("unnamed file"))?;
                let dest_path = dest.join(name);
                if dest_path.exists() && !force { continue; }
                std::fs::write(&dest_path, f.contents())?;
                *count += 1;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn workspace_template_has_skill_files() {
        // SKILL.md lives under .claude/skills/dpe-tool/
        let skill = WORKSPACE_TEMPLATE.get_file(".claude/skills/dpe-tool/SKILL.md");
        assert!(skill.is_some(), "SKILL.md not found in embedded workspace template");
    }

    #[test] fn workspace_template_has_fixture() {
        let fx = WORKSPACE_TEMPLATE.get_file("fixtures/uppercase-text.yaml");
        assert!(fx.is_some(), "uppercase-text.yaml fixture not embedded");
    }

    #[test] fn framework_rust_has_cargo_toml() {
        assert!(FRAMEWORK_RUST.get_file("Cargo.toml").is_some());
        assert!(FRAMEWORK_RUST.get_file("src/lib.rs").is_some());
    }

    #[test] fn framework_rust_has_template() {
        assert!(FRAMEWORK_RUST.get_file("template/Cargo.toml").is_some());
    }

    #[test] fn framework_ts_has_package_json() {
        assert!(FRAMEWORK_TS.get_file("package.json").is_some());
        assert!(FRAMEWORK_TS.get_file("template/package.json").is_some());
    }

    #[test] fn framework_python_has_pyproject() {
        assert!(FRAMEWORK_PYTHON.get_file("pyproject.toml").is_some());
        assert!(FRAMEWORK_PYTHON.get_file("template/pyproject.toml").is_some());
    }

    #[test] fn extract_workspace_to_tempdir() {
        let tmp = tempfile::tempdir().unwrap();
        let n = extract(&WORKSPACE_TEMPLATE, tmp.path(), false).unwrap();
        assert!(n > 0);
        assert!(tmp.path().join(".claude/skills/dpe-tool/SKILL.md").exists());
        assert!(tmp.path().join("fixtures/uppercase-text.yaml").exists());
    }

    #[test] fn extract_skips_existing_files_without_force() {
        let tmp = tempfile::tempdir().unwrap();
        let sentinel = tmp.path().join(".claude/skills/dpe-tool/SKILL.md");
        std::fs::create_dir_all(sentinel.parent().unwrap()).unwrap();
        std::fs::write(&sentinel, "USER CONTENT").unwrap();
        extract(&WORKSPACE_TEMPLATE, tmp.path(), false).unwrap();
        let contents = std::fs::read_to_string(&sentinel).unwrap();
        assert_eq!(contents, "USER CONTENT", "existing file should not be overwritten");
    }

    #[test] fn extract_overwrites_with_force() {
        let tmp = tempfile::tempdir().unwrap();
        let sentinel = tmp.path().join(".claude/skills/dpe-tool/SKILL.md");
        std::fs::create_dir_all(sentinel.parent().unwrap()).unwrap();
        std::fs::write(&sentinel, "OLD").unwrap();
        extract(&WORKSPACE_TEMPLATE, tmp.path(), true).unwrap();
        let contents = std::fs::read_to_string(&sentinel).unwrap();
        assert_ne!(contents, "OLD");
    }
}
