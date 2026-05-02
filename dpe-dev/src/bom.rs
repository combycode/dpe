//! Strip a leading UTF-8 BOM from disk-read text. See
//! `runner/src/bom.rs` for the rationale — this is the same helper for
//! dpe-dev, which parses the same meta.json + spec.yaml files.

pub(crate) fn strip_bom(raw: &str) -> &str {
    raw.strip_prefix('\u{feff}').unwrap_or(raw)
}
