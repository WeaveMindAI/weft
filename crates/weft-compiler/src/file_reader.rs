//! The filesystem view the compiler reads `@file`/`@include` content through.
//!
//! `@file("path")` and `@include("path")` name external content relative to the
//! referencing file's directory. Resolving them is two steps: turn `(base,
//! relative_path)` into a canonical identity (with a containment check), then
//! read that identity's bytes. Today there are two backings for those bytes:
//!
//!   - disk (the CLI on a laptop; a project folder unpacked to a temp dir and
//!     compiled from disk the same way),
//!   - in-memory map (the browser WASM parse path, and tests).
//!
//! The compile pipeline (parse, flatten, enrich, validate) is otherwise pure, so
//! the only thing that varies across the two is HOW a referenced path becomes
//! content. `FileReader` is that one seam; `CompileFs` bundles a reader with the
//! current resolution anchor (`base`) and is what the compiler threads, so the
//! reader and the anchor (which are never independent: a reader can't resolve a
//! relative path without an anchor, an anchor can't read without a reader) travel
//! as one concept.

use std::path::{Component, Path, PathBuf};

/// A resolved external reference: its canonical identity plus its content.
///
/// `identity` is the backing-agnostic key for the resolved path. The compiler
/// uses it for two things, so every backing must produce one that supports both:
///   - `@include` cycle detection (is this identity already on the resolve
///     stack?), which needs identities to compare equal iff they name the same
///     file,
///   - deriving the base for content referenced INSIDE the resolved file
///     (`identity.parent()`), so a nested `@file`/`@include` resolves against the
///     included file's own directory, not the parent's.
#[derive(Debug)]
pub struct ResolvedFile {
    pub identity: PathBuf,
    pub content: String,
}

/// Resolve and read the content named by `relative` against `base`.
///
/// `base` is the directory the reference is relative to (the referencing file's
/// own directory). The error is a human-facing string the caller wraps in a
/// `CompileError` at the field's span: no error type, because every backing's
/// failure ("escapes the root", "not found", "is not valid UTF-8") is just a
/// message pointed at a source line.
pub trait FileReader {
    fn resolve_and_read(&self, base: &Path, relative: &Path) -> Result<ResolvedFile, String>;
}

/// The on-disk backing: today's behavior. Joins `relative` onto `base`,
/// canonicalizes (resolving `..` and symlinks), rejects anything that escapes
/// `base`, then reads the bytes.
///
/// Threat model (unchanged from the original `@file` resolver): the project tree
/// is TRUSTED (this runs at build time on the user's own project; referenced
/// files are part of the project). The containment check guards an accidental
/// `../` typo leaking a host file into the build, not a hostile tree. It is
/// robust for that: `canonicalize` resolves `..` and follows symlinks before the
/// prefix check, so any path (including via a symlink) that lands outside `base`
/// is rejected. The only residual gap is the canonicalize then read TOCTOU
/// window, irrelevant for a trusted local tree. If disk content ever becomes
/// UNTRUSTED, switch to O_NOFOLLOW / per-component symlink rejection.
pub struct DiskFileReader;

impl FileReader for DiskFileReader {
    fn resolve_and_read(&self, base: &Path, relative: &Path) -> Result<ResolvedFile, String> {
        let joined = base.join(relative);
        let canonical_base = base
            .canonicalize()
            .map_err(|e| format!("project root {base:?} is unreadable: {e}"))?;
        let identity = joined
            .canonicalize()
            .map_err(|e| format!("path {relative:?} cannot be read: {e}"))?;
        if !identity.starts_with(&canonical_base) {
            return Err(format!("path {relative:?} escapes the project root"));
        }
        let content = std::fs::read_to_string(&identity)
            .map_err(|e| format!("path {relative:?} cannot be read: {e}"))?;
        Ok(ResolvedFile { identity, content })
    }
}

/// An in-memory backing: a path-keyed map of content. Used by the browser WASM
/// parse path (the editor feeds it the project's open files) and by tests that
/// exercise `@file`/`@include` without touching a real filesystem.
///
/// Resolution is LEXICAL (no filesystem): `relative` is joined onto `base` and
/// normalized (`.` and `..` collapsed), and that normalized path is both the
/// identity and the map key. Containment is enforced the same way the disk
/// reader enforces it: a normalized path that climbs above `base` (a leading
/// `..` survives normalization) escapes and is rejected, so a `@file("../x")`
/// fails identically on both backings. Keys in the map must be the normalized
/// form the compiler resolves to (callers build the map from the same paths the
/// source references).
pub struct MapFileReader {
    files: std::collections::BTreeMap<PathBuf, String>,
}

impl MapFileReader {
    pub fn new(files: std::collections::BTreeMap<PathBuf, String>) -> Self {
        Self { files }
    }
}

impl FileReader for MapFileReader {
    fn resolve_and_read(&self, base: &Path, relative: &Path) -> Result<ResolvedFile, String> {
        let escapes = || format!("path {relative:?} escapes the project root");
        // Normalize the base and the joined path, then enforce the SAME
        // containment the disk reader enforces (`starts_with(base)`). A `..` that
        // merely balances out a base component (e.g. `proj/../secret.txt`)
        // survives the per-path climb check yet leaves the base, so checking
        // "climbs above its own root" is not enough: the normalized join must
        // still sit under the normalized base.
        let canonical_base = normalize_lexical(base).ok_or_else(escapes)?;
        let identity = normalize_lexical(&base.join(relative)).ok_or_else(escapes)?;
        if !identity.starts_with(&canonical_base) {
            return Err(escapes());
        }
        match self.files.get(&identity) {
            Some(content) => Ok(ResolvedFile {
                identity,
                content: content.clone(),
            }),
            None => Err(format!("path {relative:?} not found")),
        }
    }
}

/// Lexically normalize a path: collapse `.` and resolve `..` against the
/// preceding component, WITHOUT touching the filesystem. Returns `None` if a
/// `..` would climb above the path's root (the lexical equivalent of escaping
/// the containment root), so a virtual backing rejects the same escapes the disk
/// backing's canonicalize-then-prefix-check rejects.
fn normalize_lexical(path: &Path) -> Option<PathBuf> {
    let mut out: Vec<Component> = Vec::new();
    for comp in path.components() {
        match comp {
            Component::CurDir => {}
            Component::ParentDir => match out.last() {
                Some(Component::Normal(_)) => {
                    out.pop();
                }
                // `..` at the start, or after a root/prefix, escapes upward.
                _ => return None,
            },
            other => out.push(other),
        }
    }
    Some(out.iter().collect())
}

/// The filesystem view threaded through the compile pipeline: a reader plus the
/// current resolution anchor. Replaces the bare `base_dir: Option<&Path>` the
/// pipeline used to thread, unifying "where do I resolve relative paths" and
/// "how do I read them" into one value (they are never independent).
///
/// `base` is `None` when compiling outside any project (an unsaved buffer, a
/// bare snippet): a `@file`/`@include` then has no anchor and is a compile error,
/// exactly as before. Descending into an included file produces a fresh
/// `CompileFs` (`descend`) carrying the SAME reader and the included file's own
/// directory as the new anchor.
#[derive(Clone, Copy)]
pub struct CompileFs<'a> {
    pub reader: &'a dyn FileReader,
    pub base: Option<&'a Path>,
}

/// The default reader for callers that don't inject one: read from disk. A
/// `'static` so `CompileFs::disk`/`none` can borrow it without the caller
/// holding an instance.
static DISK_READER: DiskFileReader = DiskFileReader;

impl<'a> CompileFs<'a> {
    /// Disk-backed view anchored at `base` (the project root / file directory).
    pub fn disk(base: &'a Path) -> Self {
        Self {
            reader: &DISK_READER,
            base: Some(base),
        }
    }

    /// Disk-backed view with NO anchor: compiling outside a project. Any
    /// `@file`/`@include` is a compile error (nothing to resolve against).
    pub fn none() -> Self {
        Self {
            reader: &DISK_READER,
            base: None,
        }
    }

    /// View with an explicit reader (the in-memory map the browser parse path
    /// uses) anchored at `base`.
    pub fn with_reader(reader: &'a dyn FileReader, base: Option<&'a Path>) -> Self {
        Self { reader, base }
    }

    /// The view for content referenced INSIDE a resolved file: same reader, the
    /// included file's own directory as the new anchor.
    pub fn descend(&self, base: Option<&'a Path>) -> Self {
        Self {
            reader: self.reader,
            base,
        }
    }
}

#[cfg(test)]
#[path = "tests/file_reader_tests.rs"]
mod tests;
