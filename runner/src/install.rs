//! `dpe install <name>` — download + verify + extract a tool package.
//!
//! Source: catalog entry for the tool name.
//!   url_template: http(s):// or file:// URL with {os}/{arch}/{version} placeholders
//!   sha256:       per-platform digest table
//!
//! Destination: ~/.dpe/tools/<name>/ (contains meta.json + binary + spec + README)
//!
//! Phase 1: empty url_template → prints install hint and exits.
//! Phase 2 (CI-released): real URLs populated → full download + verify flow.

use anyhow::{anyhow, bail, Context, Result};
use sha2::{Digest, Sha256};
use std::io::Read;
use std::path::{Path, PathBuf};

use crate::catalog::{Catalog, CatalogEntry};
use crate::config::RunnerConfig;
use crate::home::Layout;

/// Run installation. Returns the install dir on success.
///
/// The catalog is resolved from the runner config's `tools_registries`,
/// falling back to `<binary_dir>/catalog.json` when that list is empty.
pub fn install(cfg: &RunnerConfig, name: &str, force: bool) -> Result<PathBuf> {
    let registries = crate::catalog::resolve_registries(cfg);
    if registries.is_empty() {
        bail!("no tool registry configured. Set `tools_registries` in config.toml \
               or place a `catalog.json` next to the dpe binary.");
    }
    let catalog = Catalog::load_from_files(&registries);
    let entry = catalog.tools.get(name)
        .ok_or_else(|| anyhow!("unknown tool '{}' in {} registry/registries — run `dpe tools list`",
                               name, registries.len()))?;

    let layout = Layout::resolve().context("resolve ~/.dpe layout")?;
    layout.ensure().context("create ~/.dpe dirs")?;

    // Destination: cfg.default_install_path overrides the ~/.dpe/tools default.
    let tools_root: PathBuf = match cfg.default_install_path.as_deref() {
        Some(p) if !p.is_empty() => {
            let d = expand_home(p);
            std::fs::create_dir_all(&d).with_context(|| format!("create {:?}", d))?;
            d
        }
        _ => layout.tools.clone(),
    };
    let target = tools_root.join(name);

    if target.exists() && !force {
        let has_files = std::fs::read_dir(&target)
            .map(|mut d| d.next().is_some())
            .unwrap_or(false);
        if has_files {
            bail!("already installed at {}. Use --force to reinstall", target.display());
        }
    }

    // Resolve platform + URL
    let plat = current_platform_tag();
    let url = resolve_url(entry, &plat)?;
    let expected_sha = entry.binary.sha256.get(&plat).cloned();

    eprintln!("[install] {} v{} ({})",
        name, entry.version.as_deref().unwrap_or("?"), plat);
    eprintln!("[install] src: {}", url);

    // Download to a sibling of the install root so tests using
    // `default_install_path` get isolated staging — and so a custom install
    // root doesn't write tmp files to ~/.dpe.
    let tmp_path = tools_root.join(".install-tmp").join(format!("{}-{}.tar.gz", name, plat));
    if let Some(parent) = tmp_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    download(&url, &tmp_path, cfg.runtime.effective_http_timeout_secs())
        .with_context(|| format!("download {}", url))?;

    // Verify sha256
    if let Some(expected) = &expected_sha {
        let actual = sha256_hex(&tmp_path)?;
        if &actual != expected {
            bail!("sha256 mismatch — expected {}, got {}", expected, actual);
        }
        eprintln!("[install] sha256 ok: {}", &actual[..16]);
    } else {
        eprintln!("[install] WARN — no sha256 in catalog; skipping verification");
    }

    // Extract tar.gz → target
    std::fs::create_dir_all(&target)?;
    let count = extract_tar_gz(&tmp_path, &target)?;
    eprintln!("[install] extracted {} file(s) → {}", count, target.display());

    // Cleanup temp
    let _ = std::fs::remove_file(&tmp_path);

    Ok(target)
}

fn expand_home(s: &str) -> PathBuf {
    if let Some(rest) = s.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(rest);
        }
    }
    PathBuf::from(s)
}

fn resolve_url(entry: &CatalogEntry, platform: &str) -> Result<String> {
    let tmpl = entry.binary.url_template.as_deref().unwrap_or("");
    if tmpl.is_empty() {
        bail!("no download URL for this tool yet.\n\
               Try building from source, OR wait for a CI-published release.\n\
               (url_template empty in catalog.json)");
    }
    let (os, arch) = split_platform(platform);
    let version = entry.version.as_deref().unwrap_or("0.0.0");
    Ok(tmpl
        .replace("{os}", os)
        .replace("{arch}", arch)
        .replace("{version}", version))
}

fn current_platform_tag() -> String {
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    format!("{}-{}", normalize_os(os), normalize_arch(arch))
}

fn normalize_os(os: &str) -> &str {
    match os {
        "macos" => "darwin",
        other   => other,
    }
}

fn normalize_arch(arch: &str) -> &str {
    match arch {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        other    => other,
    }
}

fn split_platform(tag: &str) -> (&str, &str) {
    tag.split_once('-').unwrap_or(("unknown", "unknown"))
}

fn download(url: &str, dest: &Path, timeout_secs: u64) -> Result<()> {
    if let Some(rest) = url.strip_prefix("file://") {
        let src = PathBuf::from(parse_file_url_path(rest));
        std::fs::copy(&src, dest)
            .with_context(|| format!("copy {:?} -> {:?}", src, dest))?;
        return Ok(());
    }
    if url.starts_with("http://") || url.starts_with("https://") {
        return http_download(url, dest, timeout_secs);
    }
    bail!("unsupported URL scheme: {}", url);
}

/// Convert the part of a `file://...` URL after the scheme into an OS path.
///
/// Handles four shapes seen in the wild:
///   `localhost/path`  — RFC 8089 authority form, strip the host
///   `/C:/win/path`    — Windows drive: drop the leading `/`
///   `/unix/path`      — Unix absolute, keep leading `/`
///   `//double/slash`  — caller built the URL with one extra `/`
///                       (e.g. `format!("file:///{}", "/tmp/x")`); collapse.
fn parse_file_url_path(rest: &str) -> &str {
    let rest = rest.strip_prefix("localhost").unwrap_or(rest);
    let bytes = rest.as_bytes();
    // `/C:/...` — Windows drive letter form
    if cfg!(windows) && bytes.len() >= 3 && bytes[0] == b'/' && bytes[2] == b':' {
        return &rest[1..];
    }
    // `//something` — accidental extra slash from `file:///` + absolute path;
    // collapse to a single leading slash.
    if rest.starts_with("//") {
        return &rest[1..];
    }
    rest
}

/// Streaming HTTP GET → write body to `dest` chunk-by-chunk. Never buffers
/// the response into memory, so a multi-GB tarball is fine.
fn http_download(url: &str, dest: &Path, timeout_secs: u64) -> Result<()> {
    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(std::time::Duration::from_secs(timeout_secs)))
        .build()
        .new_agent();

    let mut response = agent.get(url).call()
        .with_context(|| format!("GET {}", url))?;

    let status = response.status();
    if !status.is_success() {
        bail!("GET {} returned HTTP {}", url, status.as_u16());
    }

    let mut file = std::fs::File::create(dest)
        .with_context(|| format!("create {:?}", dest))?;
    let mut reader = response.body_mut().as_reader();
    std::io::copy(&mut reader, &mut file)
        .with_context(|| format!("stream body to {:?}", dest))?;
    Ok(())
}

fn sha256_hex(path: &Path) -> Result<String> {
    let mut f = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 { break; }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn extract_tar_gz(archive: &Path, dest: &Path) -> Result<usize> {
    // Entry::unpack_in canonicalizes `dest`, which fails if it doesn't exist.
    std::fs::create_dir_all(dest)?;
    let f = std::fs::File::open(archive)?;
    let gz = flate2::read::GzDecoder::new(f);
    let mut ar = tar::Archive::new(gz);
    // Honor the mode bits the tarball recorded (so binaries stay executable
    // after install). preserve_permissions(true) is the tar crate's high-level
    // switch; set_overwrite(true) lets us replace files on a forced reinstall.
    ar.set_preserve_permissions(true);
    ar.set_overwrite(true);
    let mut count = 0;
    for entry in ar.entries()? {
        let mut e = entry?;
        if e.header().entry_type().is_dir() {
            let rel = e.path()?.into_owned();
            std::fs::create_dir_all(dest.join(&rel))?;
            continue;
        }
        // Entry::unpack_in handles path safety (no `..` traversal), parent-
        // dir creation, and applies the mode + mtime from the tar header.
        e.unpack_in(dest)?;
        count += 1;
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn platform_tag_looks_sensible() {
        let t = current_platform_tag();
        assert!(t.contains('-'));
        let (os, arch) = split_platform(&t);
        assert!(!os.is_empty());
        assert!(!arch.is_empty());
    }

    #[test] fn sha256_of_known_content() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("f.txt");
        std::fs::write(&p, b"hello").unwrap();
        let h = sha256_hex(&p).unwrap();
        assert_eq!(h, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824");
    }

    fn cfg_with_registry(path: &Path) -> RunnerConfig {
        RunnerConfig {
            tools_registries: vec![path.to_string_lossy().into_owned()],
            ..Default::default()
        }
    }

    #[test] fn install_with_no_registry_configured_errors() {
        // Empty list AND no adjacent catalog.json (current_exe is the test
        // binary; no catalog.json sits next to it).
        let cfg = RunnerConfig::default();
        let err = install(&cfg, "anything", false).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("no tool registry"), "unexpected: {}", msg);
    }

    #[test] fn install_unknown_tool_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let cat = tmp.path().join("c.json");
        std::fs::write(&cat, r#"{"version":"1","tools":{
            "known":{"description":"K","runtime":"rust"}
        }}"#).unwrap();
        let cfg = cfg_with_registry(&cat);
        let err = install(&cfg, "bogus-never-exists", false).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("unknown tool"), "unexpected error: {}", msg);
    }

    #[test] fn install_with_empty_url_errors_cleanly() {
        let tmp = tempfile::tempdir().unwrap();
        let cat = tmp.path().join("c.json");
        std::fs::write(&cat, r#"{"version":"1","tools":{
            "normalize":{"description":"N","runtime":"rust","version":"2.0.0"}
        }}"#).unwrap();
        let cfg = cfg_with_registry(&cat);
        let err = install(&cfg, "normalize", true).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("no download URL") || msg.contains("url_template empty"),
                "unexpected: {}", msg);
    }

    /// Build a tar.gz at `archive` containing one file `name` with `contents`.
    /// Returns the sha256 of the archive (as the catalog would record it).
    /// The writer is scoped so the gzip trailer is flushed before we hash.
    fn make_archive(archive: &Path, name: &str, contents: &[u8]) -> String {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        {
            let f = std::fs::File::create(archive).unwrap();
            let gz = GzEncoder::new(f, Compression::default());
            let mut tar_w = tar::Builder::new(gz);
            let mut hdr = tar::Header::new_gnu();
            hdr.set_size(contents.len() as u64);
            hdr.set_mode(0o644);
            hdr.set_cksum();
            tar_w.append_data(&mut hdr, name, contents).unwrap();
            tar_w.finish().unwrap();
        }
        sha256_hex(archive).unwrap()
    }

    /// Build a runner config that points at a single registry file and
    /// overrides the install destination so tests don't pollute ~/.dpe.
    fn cfg_isolated(registry: &Path, install_dir: &Path) -> RunnerConfig {
        RunnerConfig {
            tools_registries: vec![registry.to_string_lossy().into_owned()],
            default_install_path: Some(install_dir.to_string_lossy().into_owned()),
            ..Default::default()
        }
    }

    fn make_catalog(path: &Path, name: &str, url: &str, sha: Option<&str>) {
        let plat = current_platform_tag();
        let sha_block = match sha {
            Some(s) => format!("\"sha256\":{{\"{}\":\"{}\"}}", plat, s),
            None    => String::from("\"sha256\":{}"),
        };
        let body = format!(
            r#"{{"version":"1","tools":{{
                "{name}":{{
                    "description":"D","runtime":"rust","version":"2.0.0",
                    "binary":{{"url_template":"{url}",{sha}}}
                }}
            }}}}"#,
            name = name, url = url, sha = sha_block,
        );
        std::fs::write(path, body).unwrap();
    }

    #[test] fn install_with_correct_sha256_succeeds() {
        let tmp = tempfile::tempdir().unwrap();
        let archive = tmp.path().join("pkg.tar.gz");
        let real_sha = make_archive(&archive, "binary", b"contents");
        let cat_path = tmp.path().join("c.json");
        let url = format!("file:///{}", archive.to_string_lossy().replace('\\', "/"));
        make_catalog(&cat_path, "demo", &url, Some(&real_sha));

        let install_dir = tmp.path().join("installed");
        let cfg = cfg_isolated(&cat_path, &install_dir);
        let target = install(&cfg, "demo", false).unwrap();
        assert!(target.join("binary").exists(),
            "expected installed binary at {:?}", target.join("binary"));
    }

    #[test] fn install_with_wrong_sha256_aborts_before_extract() {
        let tmp = tempfile::tempdir().unwrap();
        let archive = tmp.path().join("pkg.tar.gz");
        make_archive(&archive, "binary", b"contents");
        let cat_path = tmp.path().join("c.json");
        let url = format!("file:///{}", archive.to_string_lossy().replace('\\', "/"));
        // Lie about the sha
        let bad_sha = "0000000000000000000000000000000000000000000000000000000000000000";
        make_catalog(&cat_path, "demo", &url, Some(bad_sha));

        let install_dir = tmp.path().join("installed");
        let cfg = cfg_isolated(&cat_path, &install_dir);
        let err = install(&cfg, "demo", false).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("sha256 mismatch"), "unexpected: {}", msg);
        // The install dir should NOT contain the binary — extraction never ran.
        assert!(!install_dir.join("demo").join("binary").exists(),
            "binary leaked despite sha mismatch");
    }

    #[test] fn install_with_corrupt_archive_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let archive = tmp.path().join("pkg.tar.gz");
        // Garbage that looks vaguely gzip-y but isn't a valid archive.
        std::fs::write(&archive, b"\x1f\x8b\x08\x00not-a-real-archive-just-noise").unwrap();
        let cat_path = tmp.path().join("c.json");
        let url = format!("file:///{}", archive.to_string_lossy().replace('\\', "/"));
        make_catalog(&cat_path, "demo", &url, None);  // skip sha to reach extract

        let install_dir = tmp.path().join("installed");
        let cfg = cfg_isolated(&cat_path, &install_dir);
        let err = install(&cfg, "demo", false).unwrap_err();
        // Error should bubble up from tar/gzip — exact message varies by version,
        // so just assert it's not the success path.
        let msg = format!("{:#}", err);
        assert!(!msg.is_empty(), "expected an error chain, got empty");
    }

    #[test] fn install_when_already_installed_without_force_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let archive = tmp.path().join("pkg.tar.gz");
        let real_sha = make_archive(&archive, "binary", b"contents");
        let cat_path = tmp.path().join("c.json");
        let url = format!("file:///{}", archive.to_string_lossy().replace('\\', "/"));
        make_catalog(&cat_path, "demo", &url, Some(&real_sha));

        let install_dir = tmp.path().join("installed");
        let cfg = cfg_isolated(&cat_path, &install_dir);
        // First install: succeeds.
        install(&cfg, "demo", false).unwrap();
        // Second install without --force: refuses.
        let err = install(&cfg, "demo", false).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("already installed"), "unexpected: {}", msg);
    }

    #[test] fn install_with_force_overwrites_existing() {
        let tmp = tempfile::tempdir().unwrap();
        let archive = tmp.path().join("pkg.tar.gz");
        let real_sha = make_archive(&archive, "binary", b"v1-contents");
        let cat_path = tmp.path().join("c.json");
        let url = format!("file:///{}", archive.to_string_lossy().replace('\\', "/"));
        make_catalog(&cat_path, "demo", &url, Some(&real_sha));

        let install_dir = tmp.path().join("installed");
        let cfg = cfg_isolated(&cat_path, &install_dir);
        install(&cfg, "demo", false).unwrap();
        // Rebuild archive with new contents and updated sha.
        let new_sha = make_archive(&archive, "binary", b"v2-contents");
        make_catalog(&cat_path, "demo", &url, Some(&new_sha));

        let target = install(&cfg, "demo", true).unwrap();
        let body = std::fs::read(target.join("binary")).unwrap();
        assert_eq!(body, b"v2-contents");
    }

    #[test] fn install_with_unsupported_url_scheme_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let cat_path = tmp.path().join("c.json");
        make_catalog(&cat_path, "demo", "ftp://example.com/pkg.tar.gz", None);

        let install_dir = tmp.path().join("installed");
        let cfg = cfg_isolated(&cat_path, &install_dir);
        let err = install(&cfg, "demo", false).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(msg.contains("unsupported URL scheme") || msg.contains("ftp"),
            "unexpected: {}", msg);
    }

    // HTTP path coverage (200 OK streaming / 404 / sha mismatch over the wire)
    // is exercised end-to-end by `scripts/validate-linux.sh`, which spins up a
    // real `python -m http.server` and runs `dpe install` against it. Keeping
    // unit-level mock-server tests here would re-cover the same code paths
    // through a different (heavier) stack — not worth the dep weight.

    #[test] fn install_with_missing_source_file_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let cat_path = tmp.path().join("c.json");
        let bogus_url = format!("file:///{}/does-not-exist.tar.gz", tmp.path().to_string_lossy().replace('\\', "/"));
        make_catalog(&cat_path, "demo", &bogus_url, None);

        let install_dir = tmp.path().join("installed");
        let cfg = cfg_isolated(&cat_path, &install_dir);
        let err = install(&cfg, "demo", false).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(msg.contains("download") || msg.contains("copy"), "unexpected: {}", msg);
    }

    #[test] fn extract_tar_gz_roundtrip() {
        use flate2::write::GzEncoder;
        use flate2::Compression;

        let tmp = tempfile::tempdir().unwrap();
        let archive = tmp.path().join("test.tar.gz");
        // Build a tar.gz with 2 files
        {
            let f = std::fs::File::create(&archive).unwrap();
            let gz = GzEncoder::new(f, Compression::default());
            let mut tar_w = tar::Builder::new(gz);
            let mut hdr = tar::Header::new_gnu();
            hdr.set_size(5);
            hdr.set_mode(0o644);
            hdr.set_cksum();
            tar_w.append_data(&mut hdr.clone(), "a.txt", &b"hello"[..]).unwrap();
            let mut hdr2 = tar::Header::new_gnu();
            hdr2.set_size(3);
            hdr2.set_mode(0o644);
            hdr2.set_cksum();
            tar_w.append_data(&mut hdr2, "sub/b.txt", &b"bye"[..]).unwrap();
            tar_w.finish().unwrap();
        }

        let dest = tmp.path().join("out");
        let n = extract_tar_gz(&archive, &dest).unwrap();
        assert_eq!(n, 2);
        assert_eq!(std::fs::read_to_string(dest.join("a.txt")).unwrap(), "hello");
        assert_eq!(std::fs::read_to_string(dest.join("sub/b.txt")).unwrap(), "bye");
    }
}
