//! The chunk index: the in-RAM map of where every file's pieces
//! live, plus the pure functions that make it REBUILDABLE BY SCAN.
//!
//! On disk, a file `key` is:
//!   - chunk files `chunks/<key>/<ordinal>` (zero or more, ordered),
//!     each self-labeled by its path, spread across disks;
//!   - exactly one metadata file `meta/<key>.json` (mime, size,
//!     filename, keep, expiry). Its WRITE is the commit point of a
//!     put; a key without a meta file does not exist (its chunks are
//!     crash garbage and get collected at scan).
//!
//! The index never lies: it is rebuilt from a full scan at boot, and
//! every read validates the chunk lengths against the metadata's
//! size, failing LOUDLY on mismatch (no best-effort reads).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use weft_core::storage::StoredFileMeta;

/// Where one chunk lives. Ordinal = position in `FileEntry::chunks`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkLoc {
    pub disk: String,
    pub len: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FileEntry {
    pub meta: StoredFileMeta,
    pub chunks: Vec<ChunkLoc>,
    /// Disk holding `meta/<key>.json`.
    pub meta_disk: String,
}

impl FileEntry {
    /// Total bytes on disk according to the chunk files. Reads
    /// require this to equal `meta.size_bytes`; a mismatch is
    /// corruption and fails loud.
    pub fn chunk_total(&self) -> u64 {
        self.chunks.iter().map(|c| c.len).sum()
    }
}

/// The persisted shape of `meta/<key>.json`. The key is NOT stored
/// inside (the path is the label); everything else mirrors
/// `StoredFileMeta`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaFile {
    #[serde(rename = "mimeType")]
    pub mime_type: String,
    #[serde(rename = "sizeBytes")]
    pub size_bytes: u64,
    pub filename: String,
    pub keep: bool,
    #[serde(rename = "expiresAtUnix")]
    pub expires_at_unix: Option<i64>,
    #[serde(rename = "keepTtlSecs")]
    pub keep_ttl_secs: Option<u64>,
    #[serde(rename = "createdAtUnix")]
    pub created_at_unix: i64,
}

impl MetaFile {
    pub fn from_meta(m: &StoredFileMeta) -> Self {
        Self {
            mime_type: m.mime_type.clone(),
            size_bytes: m.size_bytes,
            filename: m.filename.clone(),
            keep: m.keep,
            expires_at_unix: m.expires_at_unix,
            keep_ttl_secs: m.keep_ttl_secs,
            created_at_unix: m.created_at_unix,
        }
    }

    pub fn into_meta(self, key: &str) -> StoredFileMeta {
        StoredFileMeta {
            key: key.to_string(),
            mime_type: self.mime_type,
            size_bytes: self.size_bytes,
            filename: self.filename,
            keep: self.keep,
            expires_at_unix: self.expires_at_unix,
            keep_ttl_secs: self.keep_ttl_secs,
            created_at_unix: self.created_at_unix,
        }
    }
}

#[derive(Debug, Default)]
pub struct Index {
    pub files: BTreeMap<String, FileEntry>,
}

// ---------- on-disk paths (the self-labeling scheme) ----------

pub fn chunk_path(key: &str, ordinal: u32) -> String {
    format!("chunks/{key}/{ordinal:08}")
}

pub fn meta_path(key: &str) -> String {
    format!("meta/{key}.json")
}

/// Marker file at a disk's root flagging it as DRAINING (being
/// evacuated for release). Placement never targets a draining disk;
/// a boot-time scan that finds the marker resumes the evacuation.
pub const DRAINING_MARKER: &str = "draining";

/// Prefix of the replicated box-state files (capability secret,
/// shared grants, last-resize stamp).
pub const BOXSTATE_PATH: &str = "boxstate/state.json";

/// Invert `chunk_path`: `chunks/<key>/<ordinal>` -> (key, ordinal).
pub fn parse_chunk_path(path: &str) -> Option<(String, u32)> {
    let rest = path.strip_prefix("chunks/")?;
    let (key, ordinal) = rest.rsplit_once('/')?;
    let ordinal: u32 = ordinal.parse().ok()?;
    if key.is_empty() {
        return None;
    }
    Some((key.to_string(), ordinal))
}

/// Invert `meta_path`: `meta/<key>.json` -> key.
pub fn parse_meta_path(path: &str) -> Option<String> {
    let rest = path.strip_prefix("meta/")?;
    let key = rest.strip_suffix(".json")?;
    if key.is_empty() {
        return None;
    }
    Some(key.to_string())
}

// ---------- range planning (pure) ----------

/// Map a byte range onto chunk sub-ranges:
/// `(chunk_ordinal, start_within_chunk, end_within_chunk)` with the
/// usual inclusive-start / exclusive-end convention. `end` must be
/// pre-clamped to the file size by the caller.
pub fn plan_range(chunk_lens: &[u64], start: u64, end: u64) -> Vec<(usize, u64, u64)> {
    let mut out = Vec::new();
    let mut offset = 0u64;
    for (i, &len) in chunk_lens.iter().enumerate() {
        let chunk_start = offset;
        let chunk_end = offset + len;
        offset = chunk_end;
        if chunk_end <= start {
            continue;
        }
        if chunk_start >= end {
            break;
        }
        let s = start.saturating_sub(chunk_start);
        let e = end.min(chunk_end) - chunk_start;
        if s < e {
            out.push((i, s, e));
        }
    }
    out
}

// ---------- scan merge (pure) ----------

/// One disk's raw listing, as the scan reads it.
#[derive(Debug, Clone, Default)]
pub struct DiskScan {
    pub disk: String,
    pub draining: bool,
    /// `(path, len)` for everything under `chunks/`.
    pub chunk_files: Vec<(String, u64)>,
    /// `(path, parsed MetaFile)` for everything under `meta/`.
    pub meta_files: Vec<(String, MetaFile)>,
}

/// What the merge decided. `garbage` is junk to DELETE (chunks with
/// no committing meta file = crashed puts; exact duplicates left on
/// a draining disk by an interrupted evacuation copy).
#[derive(Debug, Default)]
pub struct ScanOutcome {
    pub index: Index,
    pub garbage: Vec<(String, String)>,
}

/// Merge per-disk scans into the index. Loud failure on anything
/// that cannot be explained by a crash of our own write protocols:
/// duplicate chunk/meta where NEITHER copy is on a draining disk
/// (evacuation is the only sanctioned duplicator), or unparseable
/// self-labels. Never guesses.
pub fn merge_scans(scans: Vec<DiskScan>) -> Result<ScanOutcome, String> {
    let draining: std::collections::BTreeSet<&str> = scans
        .iter()
        .filter(|s| s.draining)
        .map(|s| s.disk.as_str())
        .collect();

    // key -> ordinal -> (disk, len), resolving duplicates.
    let mut chunks: BTreeMap<String, BTreeMap<u32, (String, u64)>> = BTreeMap::new();
    let mut metas: BTreeMap<String, (String, MetaFile)> = BTreeMap::new();
    let mut garbage: Vec<(String, String)> = Vec::new();

    for scan in &scans {
        for (path, len) in &scan.chunk_files {
            let (key, ordinal) = parse_chunk_path(path).ok_or_else(|| {
                format!("unparseable chunk path '{path}' on disk '{}'", scan.disk)
            })?;
            let slot = chunks.entry(key.clone()).or_default();
            match slot.get(&ordinal) {
                None => {
                    slot.insert(ordinal, (scan.disk.clone(), *len));
                }
                Some((other_disk, other_len)) => {
                    // A duplicate is legitimate ONLY mid-evacuation:
                    // copy landed, source not yet deleted. Keep the
                    // non-draining copy, collect the draining one.
                    let this_draining = draining.contains(scan.disk.as_str());
                    let other_draining = draining.contains(other_disk.as_str());
                    match (this_draining, other_draining) {
                        (true, false) => {
                            if len != other_len {
                                return Err(format!(
                                    "chunk '{path}' duplicated with DIFFERENT lengths \
                                     ({other_len} on '{other_disk}', {len} on '{}'); \
                                     refusing to guess",
                                    scan.disk
                                ));
                            }
                            garbage.push((scan.disk.clone(), path.clone()));
                        }
                        (false, true) => {
                            if len != other_len {
                                return Err(format!(
                                    "chunk '{path}' duplicated with DIFFERENT lengths \
                                     ({other_len} on '{other_disk}', {len} on '{}'); \
                                     refusing to guess",
                                    scan.disk
                                ));
                            }
                            garbage.push((other_disk.clone(), path.clone()));
                            slot.insert(ordinal, (scan.disk.clone(), *len));
                        }
                        _ => {
                            return Err(format!(
                                "chunk '{path}' exists on BOTH '{other_disk}' and '{}' and \
                                 neither disk is draining; this cannot result from our write \
                                 protocol. Refusing to guess; inspect the disks",
                                scan.disk
                            ));
                        }
                    }
                }
            }
        }
        for (path, meta) in &scan.meta_files {
            let key = parse_meta_path(path).ok_or_else(|| {
                format!("unparseable meta path '{path}' on disk '{}'", scan.disk)
            })?;
            match metas.get(&key) {
                None => {
                    metas.insert(key, (scan.disk.clone(), meta.clone()));
                }
                Some((other_disk, _)) => {
                    let this_draining = draining.contains(scan.disk.as_str());
                    let other_draining = draining.contains(other_disk.as_str());
                    match (this_draining, other_draining) {
                        (true, false) => garbage.push((scan.disk.clone(), path.clone())),
                        (false, true) => {
                            garbage.push((other_disk.clone(), path.clone()));
                            metas.insert(key, (scan.disk.clone(), meta.clone()));
                        }
                        _ => {
                            return Err(format!(
                                "meta '{path}' exists on BOTH '{other_disk}' and '{}' and \
                                 neither disk is draining; refusing to guess",
                                scan.disk
                            ));
                        }
                    }
                }
            }
        }
    }

    // Assemble: a file exists iff its meta exists. Chunks without
    // meta are crashed-put garbage. Gaps in ordinals (0,2 without 1)
    // are corruption and fail loud at assembly, not at read time.
    let mut index = Index::default();
    for (key, (meta_disk, meta_file)) in metas {
        let chunk_map = chunks.remove(&key).unwrap_or_default();
        let mut locs = Vec::with_capacity(chunk_map.len());
        for (expected, (ordinal, (disk, len))) in chunk_map.into_iter().enumerate() {
            if ordinal as usize != expected {
                return Err(format!(
                    "file '{key}': chunk ordinals are not contiguous (missing #{expected}); \
                     the file is corrupt. Refusing to guess"
                ));
            }
            locs.push(ChunkLoc { disk, len });
        }
        let meta = meta_file.into_meta(&key);
        index.files.insert(key, FileEntry { meta, chunks: locs, meta_disk });
    }
    for (key, ordinals) in chunks {
        for (ordinal, (disk, _)) in ordinals {
            tracing::warn!(
                target: "weft_storage::scan",
                key = %key,
                ordinal,
                disk = %disk,
                "chunk without committing meta file (crashed put); collecting"
            );
            garbage.push((disk, chunk_path(&key, ordinal)));
        }
    }
    Ok(ScanOutcome { index, garbage })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(size: u64) -> MetaFile {
        MetaFile {
            mime_type: "application/octet-stream".into(),
            size_bytes: size,
            filename: "f".into(),
            keep: false,
            expires_at_unix: None,
            keep_ttl_secs: None,
            created_at_unix: 0,
        }
    }

    #[test]
    fn paths_round_trip() {
        let p = chunk_path("exec/c1/f1", 3);
        assert_eq!(p, "chunks/exec/c1/f1/00000003");
        assert_eq!(parse_chunk_path(&p).unwrap(), ("exec/c1/f1".into(), 3));
        let m = meta_path("project/p1/f2");
        assert_eq!(m, "meta/project/p1/f2.json");
        assert_eq!(parse_meta_path(&m).unwrap(), "project/p1/f2");
        assert!(parse_chunk_path("chunks/x").is_none());
        assert!(parse_meta_path("meta/.json").is_none());
    }

    #[test]
    fn plan_range_spans_chunk_boundaries() {
        // Chunks: [0..10), [10..15), [15..40)
        let lens = [10, 5, 25];
        assert_eq!(plan_range(&lens, 0, 40), vec![(0, 0, 10), (1, 0, 5), (2, 0, 25)]);
        // Cross the first boundary exactly.
        assert_eq!(plan_range(&lens, 8, 12), vec![(0, 8, 10), (1, 0, 2)]);
        // Inside one chunk.
        assert_eq!(plan_range(&lens, 16, 20), vec![(2, 1, 5)]);
        // Boundary-aligned start.
        assert_eq!(plan_range(&lens, 10, 15), vec![(1, 0, 5)]);
        // Empty range.
        assert_eq!(plan_range(&lens, 12, 12), vec![]);
    }

    #[test]
    fn merge_rebuilds_simple_layout() {
        let scans = vec![
            DiskScan {
                disk: "disk-0".into(),
                draining: false,
                chunk_files: vec![(chunk_path("exec/c/f", 0), 10)],
                meta_files: vec![(meta_path("exec/c/f"), meta(15))],
            },
            DiskScan {
                disk: "disk-1".into(),
                draining: false,
                chunk_files: vec![(chunk_path("exec/c/f", 1), 5)],
                meta_files: vec![],
            },
        ];
        let out = merge_scans(scans).unwrap();
        let entry = &out.index.files["exec/c/f"];
        assert_eq!(entry.chunks.len(), 2);
        assert_eq!(entry.chunks[0], ChunkLoc { disk: "disk-0".into(), len: 10 });
        assert_eq!(entry.chunks[1], ChunkLoc { disk: "disk-1".into(), len: 5 });
        assert_eq!(entry.meta_disk, "disk-0");
        assert!(out.garbage.is_empty());
    }

    #[test]
    fn merge_collects_chunks_without_meta_as_garbage() {
        let scans = vec![DiskScan {
            disk: "disk-0".into(),
            draining: false,
            chunk_files: vec![(chunk_path("exec/c/f", 0), 10)],
            meta_files: vec![],
        }];
        let out = merge_scans(scans).unwrap();
        assert!(out.index.files.is_empty());
        assert_eq!(out.garbage, vec![("disk-0".into(), chunk_path("exec/c/f", 0))]);
    }

    #[test]
    fn merge_prefers_non_draining_duplicate_and_collects_the_other() {
        // Interrupted evacuation: chunk copied to disk-1, source on
        // draining disk-0 not yet deleted.
        let scans = vec![
            DiskScan {
                disk: "disk-0".into(),
                draining: true,
                chunk_files: vec![(chunk_path("k/a/b", 0), 10)],
                meta_files: vec![(meta_path("k/a/b"), meta(10))],
            },
            DiskScan {
                disk: "disk-1".into(),
                draining: false,
                chunk_files: vec![(chunk_path("k/a/b", 0), 10)],
                meta_files: vec![(meta_path("k/a/b"), meta(10))],
            },
        ];
        let out = merge_scans(scans).unwrap();
        let entry = &out.index.files["k/a/b"];
        assert_eq!(entry.chunks[0].disk, "disk-1");
        assert_eq!(entry.meta_disk, "disk-1");
        assert!(out.garbage.contains(&("disk-0".into(), chunk_path("k/a/b", 0))));
        assert!(out.garbage.contains(&("disk-0".into(), meta_path("k/a/b"))));
    }

    #[test]
    fn merge_fails_loud_on_unexplainable_duplicates() {
        let scans = vec![
            DiskScan {
                disk: "disk-0".into(),
                draining: false,
                chunk_files: vec![(chunk_path("k/a/b", 0), 10)],
                meta_files: vec![],
            },
            DiskScan {
                disk: "disk-1".into(),
                draining: false,
                chunk_files: vec![(chunk_path("k/a/b", 0), 10)],
                meta_files: vec![],
            },
        ];
        let err = merge_scans(scans).unwrap_err();
        assert!(err.contains("neither disk is draining"), "{err}");
    }

    #[test]
    fn merge_fails_loud_on_ordinal_gap() {
        let scans = vec![DiskScan {
            disk: "disk-0".into(),
            draining: false,
            chunk_files: vec![
                (chunk_path("k/a/b", 0), 10),
                (chunk_path("k/a/b", 2), 10),
            ],
            meta_files: vec![(meta_path("k/a/b"), meta(20))],
        }];
        let err = merge_scans(scans).unwrap_err();
        assert!(err.contains("not contiguous"), "{err}");
    }

    #[test]
    fn merge_allows_zero_chunk_empty_file() {
        let scans = vec![DiskScan {
            disk: "disk-0".into(),
            draining: false,
            chunk_files: vec![],
            meta_files: vec![(meta_path("k/a/empty"), meta(0))],
        }];
        let out = merge_scans(scans).unwrap();
        assert_eq!(out.index.files["k/a/empty"].chunks.len(), 0);
    }
}
