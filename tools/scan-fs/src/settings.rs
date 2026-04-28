//! Settings schema for scan-fs.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    #[serde(default)]
    pub mode: Mode,
    #[serde(default)]
    pub r#return: ReturnKind,

    /// Include patterns. Accepts a single string (semicolon-separated for
    /// legacy-Python parity) OR an array of patterns. Empty = match all.
    #[serde(default)]
    pub include: Patterns,

    /// Exclude patterns. Same shape as `include`. Empty = exclude none.
    #[serde(default)]
    pub exclude: Patterns,

    /// Max walk depth (None = unlimited).
    #[serde(default)]
    pub depth: Option<usize>,

    /// Include hidden entries (names starting with `.`).
    #[serde(default)]
    pub hidden: bool,

    /// Follow symbolic links.
    #[serde(default)]
    pub follow_symlinks: bool,

    /// Hashing algorithm (or `none`).
    #[serde(default)]
    pub hash: HashAlgo,

    /// Min file size in bytes (filter applied to files only).
    #[serde(default)]
    pub min_size: Option<u64>,

    /// Max file size in bytes.
    #[serde(default)]
    pub max_size: Option<u64>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    #[default]
    Full,
    Diff,
    // Watch deferred — needs `notify` crate + persistent task
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ReturnKind {
    #[default]
    Files,
    Dirs,
    Both,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum HashAlgo {
    #[default]
    Xxhash,
    Blake2b,
    None,
}

/// Glob pattern list — accepts string or array.
#[derive(Debug, Clone, Default)]
pub struct Patterns(pub Vec<String>);

impl<'de> Deserialize<'de> for Patterns {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw { Str(String), Vec(Vec<String>) }

        let raw = Option::<Raw>::deserialize(deserializer)?;
        let v = match raw {
            None | Some(Raw::Str(_)) if matches!(&raw, Some(Raw::Str(s)) if s.is_empty()) =>
                Vec::new(),
            None => Vec::new(),
            Some(Raw::Str(s)) => s.split(';').map(|p| p.trim().to_string())
                .filter(|p| !p.is_empty()).collect(),
            Some(Raw::Vec(v)) => v.into_iter()
                .map(|p| p.trim().to_string())
                .filter(|p| !p.is_empty()).collect(),
        };
        Ok(Patterns(v))
    }
}

impl Patterns {
    pub fn is_empty(&self) -> bool { self.0.is_empty() }
    pub fn iter(&self) -> std::slice::Iter<'_, String> { self.0.iter() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_when_empty() {
        let s: Settings = serde_json::from_str("{}").unwrap();
        assert_eq!(s.mode, Mode::Full);
        assert_eq!(s.r#return, ReturnKind::Files);
        assert_eq!(s.hash, HashAlgo::Xxhash);
        assert!(!s.hidden);
        assert!(!s.follow_symlinks);
        assert!(s.include.is_empty());
        assert!(s.exclude.is_empty());
        assert_eq!(s.depth, None);
    }

    #[test]
    fn full_settings() {
        let s: Settings = serde_json::from_str(r#"{
            "mode": "diff",
            "return": "both",
            "include": ["*.pdf", "*.docx"],
            "exclude": [".git/**"],
            "depth": 5,
            "hidden": true,
            "follow_symlinks": true,
            "hash": "blake2b",
            "min_size": 100,
            "max_size": 1000000
        }"#).unwrap();
        assert_eq!(s.mode, Mode::Diff);
        assert_eq!(s.r#return, ReturnKind::Both);
        assert_eq!(s.include.0, vec!["*.pdf", "*.docx"]);
        assert_eq!(s.exclude.0, vec![".git/**"]);
        assert_eq!(s.depth, Some(5));
        assert_eq!(s.hash, HashAlgo::Blake2b);
        assert_eq!(s.min_size, Some(100));
        assert_eq!(s.max_size, Some(1000000));
    }

    #[test]
    fn include_string_legacy_semicolon() {
        let s: Settings = serde_json::from_str(r#"{"include": "*.pdf;*.docx;*.png"}"#).unwrap();
        assert_eq!(s.include.0, vec!["*.pdf", "*.docx", "*.png"]);
    }

    #[test]
    fn include_string_with_whitespace_trimmed() {
        let s: Settings = serde_json::from_str(r#"{"include": "*.pdf ; *.docx"}"#).unwrap();
        assert_eq!(s.include.0, vec!["*.pdf", "*.docx"]);
    }

    #[test]
    fn include_array_with_empty_filtered() {
        let s: Settings = serde_json::from_str(r#"{"include": ["*.pdf", "", "*.docx"]}"#).unwrap();
        assert_eq!(s.include.0, vec!["*.pdf", "*.docx"]);
    }

    #[test]
    fn include_empty_string_yields_empty() {
        let s: Settings = serde_json::from_str(r#"{"include": ""}"#).unwrap();
        assert!(s.include.is_empty());
    }

    #[test]
    fn unknown_field_rejected() {
        assert!(serde_json::from_str::<Settings>(r#"{"bogus": 1}"#).is_err());
    }

    #[test]
    fn hash_modes() {
        for (s, expected) in [
            (r#"{"hash":"xxhash"}"#,  HashAlgo::Xxhash),
            (r#"{"hash":"blake2b"}"#, HashAlgo::Blake2b),
            (r#"{"hash":"none"}"#,    HashAlgo::None),
        ] {
            let p: Settings = serde_json::from_str(s).unwrap();
            assert_eq!(p.hash, expected);
        }
    }

    #[test]
    fn return_kinds() {
        for (s, expected) in [
            (r#"{"return":"files"}"#, ReturnKind::Files),
            (r#"{"return":"dirs"}"#,  ReturnKind::Dirs),
            (r#"{"return":"both"}"#,  ReturnKind::Both),
        ] {
            let p: Settings = serde_json::from_str(s).unwrap();
            assert_eq!(p.r#return, expected);
        }
    }
}
