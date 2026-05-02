//! Tiny utility — strip a leading UTF-8 BOM (`EF BB BF`) from a string read
//! from disk. Windows editors (Notepad, older PowerShell `Out-File` without
//! `-Encoding utf8NoBOM`, some IDEs) prepend it silently to "UTF-8" files.
//!
//! `serde_json::from_str` and (depending on version) other parsers reject
//! BOM-prefixed input as "expected value at line 1 column 1". Tools-on-disk
//! files (meta.json, catalog.json, variant.yaml) should tolerate the BOM
//! because we don't control how users save them.
//!
//! Use at every disk-read parse site — it's a borrow, not a copy.

pub fn strip_bom(raw: &str) -> &str {
    raw.strip_prefix('\u{feff}').unwrap_or(raw)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_leading_bom() {
        assert_eq!(strip_bom("\u{feff}{}"), "{}");
    }

    #[test]
    fn passes_through_when_absent() {
        assert_eq!(strip_bom("{}"), "{}");
        assert_eq!(strip_bom(""), "");
    }

    #[test]
    fn does_not_strip_internal_bom() {
        // Internal U+FEFF (zero-width no-break space) is part of valid YAML
        // strings; we must only touch the leading byte sequence.
        assert_eq!(strip_bom("a\u{feff}b"), "a\u{feff}b");
    }
}
