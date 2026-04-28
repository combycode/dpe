//! Glob include / exclude matching against relative paths.
//!
//! Patterns use gitignore semantics via the `globset` crate.
//!
//! Empty include set = match all. Empty exclude set = exclude none.
//! Matching is done against forward-slash paths relative to the scan root.

use globset::{Glob, GlobSet, GlobSetBuilder};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum PatternError {
    #[error("invalid glob '{glob}': {reason}")]
    BadGlob { glob: String, reason: String },
    #[error("pattern build: {0}")]
    Build(String),
}

#[derive(Debug, Clone)]
pub struct Matcher {
    include: Option<GlobSet>,
    exclude: Option<GlobSet>,
}

impl Matcher {
    pub fn new(include: &[String], exclude: &[String]) -> Result<Self, PatternError> {
        Ok(Self {
            include: build_set(include)?,
            exclude: build_set(exclude)?,
        })
    }

    /// True iff path is matched by include (or include is empty) AND NOT matched by exclude.
    pub fn matches(&self, rel_path: &str) -> bool {
        let included = match &self.include {
            None => true,
            Some(set) => set.is_match(rel_path),
        };
        if !included { return false; }
        match &self.exclude {
            None => true,
            Some(set) => !set.is_match(rel_path),
        }
    }
}

fn build_set(patterns: &[String]) -> Result<Option<GlobSet>, PatternError> {
    if patterns.is_empty() { return Ok(None); }
    let mut b = GlobSetBuilder::new();
    for p in patterns {
        let g = Glob::new(p).map_err(|e| PatternError::BadGlob {
            glob: p.clone(), reason: e.to_string(),
        })?;
        b.add(g);
    }
    Ok(Some(b.build().map_err(|e| PatternError::Build(e.to_string()))?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(inc: &[&str], exc: &[&str]) -> Matcher {
        let inc: Vec<String> = inc.iter().map(|s| s.to_string()).collect();
        let exc: Vec<String> = exc.iter().map(|s| s.to_string()).collect();
        Matcher::new(&inc, &exc).unwrap()
    }

    #[test]
    fn empty_include_matches_all() {
        let mat = m(&[], &[]);
        assert!(mat.matches("a.txt"));
        assert!(mat.matches("sub/b.pdf"));
    }

    #[test]
    fn include_extension_glob() {
        let mat = m(&["*.pdf"], &[]);
        assert!(mat.matches("a.pdf"));
        assert!(!mat.matches("a.txt"));
    }

    #[test]
    fn include_multiple_patterns_union() {
        let mat = m(&["*.pdf", "*.docx"], &[]);
        assert!(mat.matches("a.pdf"));
        assert!(mat.matches("b.docx"));
        assert!(!mat.matches("c.txt"));
    }

    #[test]
    fn exclude_pattern() {
        let mat = m(&[], &["*.tmp"]);
        assert!(mat.matches("a.txt"));
        assert!(!mat.matches("a.tmp"));
    }

    #[test]
    fn exclude_overrides_include() {
        let mat = m(&["*.pdf"], &["draft_*"]);
        assert!(mat.matches("final.pdf"));
        assert!(!mat.matches("draft_v1.pdf"));
    }

    #[test]
    fn directory_glob_double_star() {
        let mat = m(&["**/*.md"], &[]);
        assert!(mat.matches("readme.md"));
        assert!(mat.matches("docs/x.md"));
        assert!(mat.matches("a/b/c/x.md"));
        assert!(!mat.matches("readme.txt"));
    }

    #[test]
    fn exclude_directory_subtree() {
        let mat = m(&[], &[".git/**"]);
        assert!(mat.matches("src/main.rs"));
        assert!(!mat.matches(".git/HEAD"));
        assert!(!mat.matches(".git/objects/12/abc"));
    }

    #[test]
    fn empty_pattern_string_is_filtered_in_settings_layer() {
        // (sanity: empty list is fine in matcher; the settings layer filters
        //  empty strings before they reach us)
        let mat = m(&[], &[]);
        assert!(mat.matches("anything"));
    }

    #[test]
    fn bad_glob_errors() {
        let bad = vec!["[unclosed".to_string()];
        let err = Matcher::new(&bad, &[]).unwrap_err();
        assert!(matches!(err, PatternError::BadGlob { .. }));
    }
}
