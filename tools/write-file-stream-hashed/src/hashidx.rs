//! Sidecar hash-index format.
//!
//! Layout:
//!   [ magic "DPHI" (4B) | version (1B) | algo (1B) | row_count (8B LE)
//!   | content_size (8B LE) | hashes (row_count * 8B LE) ]
//!
//! The header carries `row_count` and `content_size` as of the last flush.
//! During normal operation, hashes may be appended past `row_count` between
//! flushes — the actual count is inferred from file size.
//!
//! On open, we rebuild from content whenever:
//! - sidecar missing / corrupt / wrong algo
//! - content_size_header != content_size_on_disk (external modification)
//! - actual_hash_count != newline_count (inconsistency)
//!
//! This is the explicit "rebuild-on-mismatch" guarantee.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write, BufReader, BufRead};
use std::path::Path;

pub(crate) const MAGIC: [u8; 4] = *b"DPHI";
pub(crate) const VERSION: u8 = 1;
pub(crate) const HEADER_LEN: u64 = 4 + 1 + 1 + 8 + 8;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum HashAlgo { XxHash64, Blake2b64 }

impl HashAlgo {
    pub(crate) fn from_str(s: &str) -> Option<Self> {
        match s {
            "xxhash" | "xxh3" | "xxhash64" => Some(Self::XxHash64),
            "blake2" | "blake2b" | "blake2b64" => Some(Self::Blake2b64),
            _ => None,
        }
    }
    pub(crate) fn code(self) -> u8 {
        match self { Self::XxHash64 => 0, Self::Blake2b64 => 1 }
    }
    pub(crate) fn from_code(c: u8) -> Option<Self> {
        match c { 0 => Some(Self::XxHash64), 1 => Some(Self::Blake2b64), _ => None }
    }
    pub(crate) fn hash(self, bytes: &[u8]) -> u64 {
        match self {
            Self::XxHash64 => xxhash_rust::xxh3::xxh3_64(bytes),
            Self::Blake2b64 => {
                use blake2::{Blake2b, Digest};
                use blake2::digest::consts::U8;
                let mut hasher = Blake2b::<U8>::new();
                hasher.update(bytes);
                let out = hasher.finalize();
                u64::from_le_bytes(out.into())
            }
        }
    }
}

pub(crate) struct SidecarHeader {
    pub(crate) algo: HashAlgo,
    pub(crate) row_count: u64,
    pub(crate) content_size: u64,
}

/// Write a fresh header (used when creating a new sidecar).
pub(crate) fn write_header(f: &mut File, hdr: &SidecarHeader) -> std::io::Result<()> {
    f.seek(SeekFrom::Start(0))?;
    f.write_all(&MAGIC)?;
    f.write_all(&[VERSION])?;
    f.write_all(&[hdr.algo.code()])?;
    f.write_all(&hdr.row_count.to_le_bytes())?;
    f.write_all(&hdr.content_size.to_le_bytes())?;
    Ok(())
}

/// Update an existing sidecar's header (row_count / content_size) without
/// touching the hashes that follow.
pub(crate) fn update_header(f: &mut File, hdr: &SidecarHeader) -> std::io::Result<()> {
    // Overwrite just the header area; hash data follows at HEADER_LEN
    f.seek(SeekFrom::Start(4 + 1 + 1))?;
    f.write_all(&hdr.row_count.to_le_bytes())?;
    f.write_all(&hdr.content_size.to_le_bytes())?;
    Ok(())
}

pub(crate) fn read_header(f: &mut File) -> std::io::Result<Option<SidecarHeader>> {
    f.seek(SeekFrom::Start(0))?;
    let mut buf = [0u8; HEADER_LEN as usize];
    if f.read_exact(&mut buf).is_err() { return Ok(None); }
    if buf[0..4] != MAGIC { return Ok(None); }
    if buf[4] != VERSION { return Ok(None); }
    let algo = match HashAlgo::from_code(buf[5]) {
        Some(a) => a, None => return Ok(None),
    };
    let row_count    = u64::from_le_bytes(buf[6..14].try_into().unwrap());
    let content_size = u64::from_le_bytes(buf[14..22].try_into().unwrap());
    Ok(Some(SidecarHeader { algo, row_count, content_size }))
}

/// Read all hashes currently in the sidecar (based on file size, not header).
pub(crate) fn read_all_hashes(f: &mut File) -> std::io::Result<Vec<u64>> {
    let file_len = f.metadata()?.len();
    if file_len < HEADER_LEN { return Ok(vec![]); }
    let body_len = file_len - HEADER_LEN;
    let n = (body_len / 8) as usize;
    let mut out = Vec::with_capacity(n);
    f.seek(SeekFrom::Start(HEADER_LEN))?;
    let mut buf = [0u8; 8];
    for _ in 0..n {
        f.read_exact(&mut buf)?;
        out.push(u64::from_le_bytes(buf));
    }
    Ok(out)
}

/// Scan content file by lines and hash each, returning (hashes, content_size).
/// Newline is stripped before hashing (matches how we write rows).
pub(crate) fn rebuild_from_content(
    content_path: &Path, algo: HashAlgo,
) -> std::io::Result<(Vec<u64>, u64)> {
    if !content_path.exists() {
        return Ok((vec![], 0));
    }
    let f = File::open(content_path)?;
    let content_size = f.metadata()?.len();
    let rdr = BufReader::new(f);
    let mut hashes = Vec::new();
    for line in rdr.lines() {
        let line = line?;
        // `lines()` already strips the trailing newline
        hashes.push(algo.hash(line.as_bytes()));
    }
    Ok((hashes, content_size))
}

pub(crate) fn open_or_create_sidecar(path: &Path) -> std::io::Result<File> {
    OpenOptions::new().create(true).read(true).write(true).truncate(false).open(path)
}

pub(crate) fn sidecar_path(content_path: &str) -> String {
    format!("{}.hashidx", content_path)
}
