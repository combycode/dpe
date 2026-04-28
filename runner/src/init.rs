//! `dpe init <name>` — bootstrap a new pipeline directory.
//!
//! Creates a ready-to-run skeleton:
//!   <name>/
//!     tools/              (pipeline-local tool proxies)
//!     configs/
//!     variants/
//!       main.yaml          (trivial: scan → write-file)
//!     data/
//!       input/.gitkeep
//!       output/.gitkeep
//!     pipeline.toml        (metadata)
//!     README.md
//!     .gitignore           (excludes sessions/, data/output/, temp/)
//!
//! Refuses to overwrite an existing non-empty directory.

use std::path::{Path, PathBuf};

pub fn init(name: &str, out: &Path) -> Result<PathBuf, InitError> {
    if name.is_empty() || name.contains(char::is_whitespace) {
        return Err(InitError::BadName(name.into()));
    }

    let dir = out.join(name);
    if dir.exists() {
        let has_files = std::fs::read_dir(&dir)
            .map_err(|e| InitError::Io(dir.clone(), e.to_string()))?
            .next().is_some();
        if has_files {
            return Err(InitError::NotEmpty(dir));
        }
    } else {
        std::fs::create_dir_all(&dir)
            .map_err(|e| InitError::Io(dir.clone(), e.to_string()))?;
    }

    // Directory layout
    for sub in &["tools", "configs", "variants", "data/input", "data/output"] {
        std::fs::create_dir_all(dir.join(sub))
            .map_err(|e| InitError::Io(dir.join(sub), e.to_string()))?;
    }

    // .gitkeep so empty dirs survive git (and get committed)
    std::fs::write(dir.join("data/input/.gitkeep"), "")
        .map_err(|e| InitError::Io(dir.join("data/input/.gitkeep"), e.to_string()))?;
    std::fs::write(dir.join("data/output/.gitkeep"), "")
        .map_err(|e| InitError::Io(dir.join("data/output/.gitkeep"), e.to_string()))?;

    // pipeline.toml
    let pipeline_toml = format!(
        "name = \"{}\"\nversion = \"0.1.0\"\ndescription = \"New DPE pipeline\"\n",
        name
    );
    std::fs::write(dir.join("pipeline.toml"), pipeline_toml)
        .map_err(|e| InitError::Io(dir.join("pipeline.toml"), e.to_string()))?;

    // variants/main.yaml — trivial scan → write-file example
    let main_yaml = format!(
        r#"pipeline: {}
variant: main
stages:
  scan:
    tool: scan-fs
    settings:
      include: "*"
      hash: none
    input: $input

  sink:
    tool: write-file-stream
    settings:
      default_file: "$output/scanned.ndjson"
      format: ndjson
    input: scan
"#,
        name);
    std::fs::write(dir.join("variants/main.yaml"), main_yaml)
        .map_err(|e| InitError::Io(dir.join("variants/main.yaml"), e.to_string()))?;

    // .gitignore — excludes runtime state
    let gitignore = "sessions/\ntemp/\ndata/output/*\n!data/output/.gitkeep\n";
    std::fs::write(dir.join(".gitignore"), gitignore)
        .map_err(|e| InitError::Io(dir.join(".gitignore"), e.to_string()))?;

    // README.md
    let readme = format!(
        r#"# {name}

A DPE pipeline — scaffolded by `dpe init`.

## Run it

```bash
# Place seed envelopes at data/input/_seed.ndjson, OR point -i at any dir
echo '{{"t":"d","id":"1","src":"seed","v":{{"path":"data/input"}}}}' > data/input/_seed.ndjson

dpe run {name}:main -i data/input -o data/output
```

## Layout

```
{name}/
├── pipeline.toml        # metadata
├── variants/
│   └── main.yaml        # the default variant
├── configs/             # prompts, dicts, rulebook files (any tool can read)
├── tools/               # pipeline-local tool overrides (proxy meta.json)
├── data/
│   ├── input/           # $input — feed the pipeline here
│   └── output/          # $output — results land here
├── sessions/            # (runtime) per-run session artefacts
└── temp/                # (runtime) scratch
```

## Next steps

- Edit `variants/main.yaml` to match your data flow
- `dpe check {name}:main` validates the variant before running
- `dpe tools list` shows available tools
- `dpe init <other>` creates more pipelines
"#,
        name = name);
    std::fs::write(dir.join("README.md"), readme)
        .map_err(|e| InitError::Io(dir.join("README.md"), e.to_string()))?;

    Ok(dir)
}

#[derive(Debug, thiserror::Error)]
pub enum InitError {
    #[error("invalid pipeline name '{0}' (no whitespace, non-empty)")]
    BadName(String),
    #[error("directory {0} already contains files — refusing to overwrite")]
    NotEmpty(PathBuf),
    #[error("io at {0}: {1}")]
    Io(PathBuf, String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn init_creates_full_layout() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = init("my-pipe", tmp.path()).unwrap();
        assert!(dir.join("pipeline.toml").exists());
        assert!(dir.join("variants/main.yaml").exists());
        assert!(dir.join("README.md").exists());
        assert!(dir.join(".gitignore").exists());
        assert!(dir.join("data/input/.gitkeep").exists());
        assert!(dir.join("data/output/.gitkeep").exists());
        assert!(dir.join("tools").is_dir());
        assert!(dir.join("configs").is_dir());
    }

    #[test] fn init_refuses_non_empty_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("exists");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("oops.txt"), "x").unwrap();
        let err = init("exists", tmp.path()).unwrap_err();
        assert!(matches!(err, InitError::NotEmpty(_)));
    }

    #[test] fn init_allows_existing_empty_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("empty");
        std::fs::create_dir_all(&dir).unwrap();
        let r = init("empty", tmp.path()).unwrap();
        assert_eq!(r, dir);
    }

    #[test] fn init_rejects_bad_names() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(init("", tmp.path()).is_err());
        assert!(init("has spaces", tmp.path()).is_err());
    }

    #[test] fn variant_yaml_references_the_name() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = init("foo", tmp.path()).unwrap();
        let yaml = std::fs::read_to_string(dir.join("variants/main.yaml")).unwrap();
        assert!(yaml.contains("pipeline: foo"));
        assert!(yaml.contains("variant: main"));
        assert!(yaml.contains("scan-fs"));
        assert!(yaml.contains("write-file-stream"));
    }
}
