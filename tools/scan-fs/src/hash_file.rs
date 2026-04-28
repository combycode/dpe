//! Streamed file hashing — xxhash-3-64 or blake2b-128.

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use blake2::{Blake2b, Digest};
use blake2::digest::consts::U16;
use xxhash_rust::xxh3::Xxh3;

use crate::settings::HashAlgo;

const CHUNK: usize = 64 * 1024;

/// Hash a file by streaming its content in 64 KB chunks.
/// Returns hex string (16 chars for xxh3, 32 for blake2b-128) or None when algo=None.
pub fn hash_file(path: &Path, algo: HashAlgo) -> std::io::Result<Option<String>> {
    match algo {
        HashAlgo::None    => Ok(None),
        HashAlgo::Xxhash  => hash_xxh3(path).map(Some),
        HashAlgo::Blake2b => hash_blake2b(path).map(Some),
    }
}

fn hash_xxh3(path: &Path) -> std::io::Result<String> {
    let f = File::open(path)?;
    let mut r = BufReader::with_capacity(CHUNK, f);
    let mut h = Xxh3::new();
    let mut buf = vec![0u8; CHUNK];
    loop {
        let n = r.read(&mut buf)?;
        if n == 0 { break; }
        h.update(&buf[..n]);
    }
    Ok(format!("{:016x}", h.digest()))
}

fn hash_blake2b(path: &Path) -> std::io::Result<String> {
    let f = File::open(path)?;
    let mut r = BufReader::with_capacity(CHUNK, f);
    let mut h: Blake2b<U16> = Blake2b::new();
    let mut buf = vec![0u8; CHUNK];
    loop {
        let n = r.read(&mut buf)?;
        if n == 0 { break; }
        Digest::update(&mut h, &buf[..n]);
    }
    let result = h.finalize();
    Ok(hex::encode(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_file(dir: &Path, name: &str, body: &[u8]) -> std::path::PathBuf {
        let p = dir.join(name);
        let mut f = std::fs::File::create(&p).unwrap();
        f.write_all(body).unwrap();
        p
    }

    #[test]
    fn algo_none_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        let p = write_file(tmp.path(), "x", b"hello");
        assert_eq!(hash_file(&p, HashAlgo::None).unwrap(), None);
    }

    #[test]
    fn xxh3_deterministic_16_hex_chars() {
        let tmp = tempfile::tempdir().unwrap();
        let p = write_file(tmp.path(), "x", b"the quick brown fox");
        let h1 = hash_file(&p, HashAlgo::Xxhash).unwrap().unwrap();
        let h2 = hash_file(&p, HashAlgo::Xxhash).unwrap().unwrap();
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 16);
        assert!(h1.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn blake2b_deterministic_32_hex_chars() {
        let tmp = tempfile::tempdir().unwrap();
        let p = write_file(tmp.path(), "x", b"the quick brown fox");
        let h1 = hash_file(&p, HashAlgo::Blake2b).unwrap().unwrap();
        let h2 = hash_file(&p, HashAlgo::Blake2b).unwrap().unwrap();
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 32);
        assert!(h1.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn different_content_different_hash() {
        let tmp = tempfile::tempdir().unwrap();
        let a = write_file(tmp.path(), "a", b"hello");
        let b = write_file(tmp.path(), "b", b"world");
        assert_ne!(
            hash_file(&a, HashAlgo::Xxhash).unwrap(),
            hash_file(&b, HashAlgo::Xxhash).unwrap()
        );
    }

    #[test]
    fn empty_file_hashes_ok() {
        let tmp = tempfile::tempdir().unwrap();
        let p = write_file(tmp.path(), "empty", b"");
        let h = hash_file(&p, HashAlgo::Xxhash).unwrap().unwrap();
        assert_eq!(h.len(), 16);
        let h2 = hash_file(&p, HashAlgo::Blake2b).unwrap().unwrap();
        assert_eq!(h2.len(), 32);
    }

    #[test]
    fn large_file_streams_correctly() {
        let tmp = tempfile::tempdir().unwrap();
        // 256 KB of content > one CHUNK
        let body: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
        let p = write_file(tmp.path(), "big", &body);
        let h1 = hash_file(&p, HashAlgo::Xxhash).unwrap().unwrap();
        let h2 = hash_file(&p, HashAlgo::Xxhash).unwrap().unwrap();
        assert_eq!(h1, h2);
    }

    #[test]
    fn missing_file_returns_io_error() {
        let p = std::path::Path::new("/nonexistent/file/path/xyz");
        assert!(hash_file(p, HashAlgo::Xxhash).is_err());
    }
}
