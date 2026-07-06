use super::*;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

// ── MapFileReader (Layer 1, pure: no filesystem) ────────────────────────────

fn map(entries: &[(&str, &str)]) -> MapFileReader {
    let mut files = BTreeMap::new();
    for (k, v) in entries {
        files.insert(PathBuf::from(k), (*v).to_string());
    }
    MapFileReader::new(files)
}

#[test]
fn map_reads_content_and_identity() {
    let reader = map(&[("proj/prompts/system.txt", "be helpful")]);
    let resolved = reader
        .resolve_and_read(Path::new("proj/prompts"), Path::new("system.txt"))
        .unwrap();
    assert_eq!(resolved.content, "be helpful");
    // Identity is the normalized join, the key downstream uses for cycle
    // detection and for deriving the included file's directory.
    assert_eq!(resolved.identity, PathBuf::from("proj/prompts/system.txt"));
}

#[test]
fn map_resolves_dot_and_parent_within_root() {
    let reader = map(&[("proj/a.txt", "A")]);
    // `sub/../a.txt` from base `proj` normalizes to `proj/a.txt`: a `..` that
    // stays within the path is fine (it cancels a real component).
    let resolved = reader
        .resolve_and_read(Path::new("proj"), Path::new("sub/../a.txt"))
        .unwrap();
    assert_eq!(resolved.identity, PathBuf::from("proj/a.txt"));
    assert_eq!(resolved.content, "A");
}

#[test]
fn map_missing_file_errors() {
    let reader = map(&[("proj/a.txt", "A")]);
    let err = reader
        .resolve_and_read(Path::new("proj"), Path::new("b.txt"))
        .unwrap_err();
    assert!(err.contains("not found"), "got: {err}");
}

#[test]
fn map_rejects_escape_above_root() {
    // `../secret.txt` from base `proj` climbs above `proj`, the lexical
    // equivalent of the disk backing's canonicalize-then-prefix-check escape.
    let reader = map(&[("secret.txt", "leak")]);
    let err = reader
        .resolve_and_read(Path::new("proj"), Path::new("../secret.txt"))
        .unwrap_err();
    assert!(err.contains("escapes"), "got: {err}");
}

// ── normalize_lexical (Layer 1, pure) ───────────────────────────────────────

#[test]
fn normalize_collapses_cur_and_parent() {
    assert_eq!(
        normalize_lexical(Path::new("a/./b/../c")),
        Some(PathBuf::from("a/c"))
    );
}

#[test]
fn normalize_rejects_climb_above_relative_root() {
    assert_eq!(normalize_lexical(Path::new("../x")), None);
    assert_eq!(normalize_lexical(Path::new("a/../../x")), None);
}

// ── Disk / map parity on escape (Layer 3 for disk, Layer 1 for map) ──────────

#[test]
fn disk_and_map_reject_the_same_escape() {
    // Both backings reject a `../` that leaves the root, so a `@file("../x")`
    // fails identically whether the project is on disk or in an in-memory map.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("project");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(dir.path().join("secret.txt"), "leak").unwrap();

    let disk_err = DiskFileReader
        .resolve_and_read(&root, Path::new("../secret.txt"))
        .unwrap_err();
    assert!(disk_err.contains("escapes"), "disk: {disk_err}");

    let map_err = map(&[("secret.txt", "leak")])
        .resolve_and_read(Path::new("project"), Path::new("../secret.txt"))
        .unwrap_err();
    assert!(map_err.contains("escapes"), "map: {map_err}");
}
