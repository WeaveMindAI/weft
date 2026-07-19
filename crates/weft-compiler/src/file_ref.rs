//! `@file("path", Type)` / `@asset("path", Type)` value injection.
//!
//! Both markers make a config field's value come from somewhere else; they
//! differ in the EDIT CONTRACT. `@file` is bidirectional: the referenced
//! file's text content is the value, and editing the field writes back to
//! the file; it requires a type that `supports_bidirectional_edit`. `@asset`
//! is pull-only: nothing ever writes back. A file-typed `@asset` (Image,
//! Audio, ...) defers to the build's asset resolution; a text-typed one is
//! read + cast at parse exactly like `@file`, just rendered read-only.
//! `Type` defaults to `String` and is drawn from the existing `WeftType`
//! vocabulary, so the same type names used for ports work here.
//!
//! Resolution is a single post-parse pass (`resolve_node_file_refs` per node,
//! walked over top-level nodes and group descendants) over every
//! node's config map: the four text-parse sites already store the marker
//! verbatim as a string, so none of them need to know about this feature.
//! The pass finds the markers, reads the file relative to the project root,
//! casts the bytes via `WeftType::cast_text`, and replaces the value. A bad
//! path, a malformed marker, or a failed cast becomes a `CompileError`
//! pointing at the field's source line.

use weft_core::project::{ConfigFieldSpan, FileMarker, FileRef, Span};
use weft_core::WeftType;

use crate::file_reader::CompileFs;
use crate::weft_compiler::CompileError;

/// Recognize and parse a `@file("path"[, Type])` or `@asset("path"[, Type])`
/// marker from a raw value string. Returns `None` if the string is neither
/// marker (so the caller leaves ordinary values untouched), `Some(Err(..))`
/// if it looks like one but is malformed.
///
/// The path must be a double-quoted string. The optional second argument is
/// a type expression parsed with the same `WeftType::parse` that port
/// declarations use; when omitted the type is `String`.
pub fn parse_marker(raw: &str) -> Option<Result<FileRef, String>> {
    let trimmed = raw.trim();
    // Exactly the `@file` / `@asset` directive (not `@filesystem`); the marker
    // module is the single home for directive/arg extraction and tolerates a
    // space before `(`.
    let marker = match crate::cst::marker::directive(trimmed) {
        "file" => FileMarker::File,
        "asset" => FileMarker::Asset,
        _ => return None,
    };
    let name = match marker {
        FileMarker::File => "@file",
        FileMarker::Asset => "@asset",
    };
    let Some(inner) = crate::cst::marker::args_raw(trimmed) else {
        return Some(Err(format!("{name} must be followed by (\"path\")")));
    };

    // Split into the quoted path and an optional type, on the first
    // top-level comma (the path is quoted so a comma inside it can't occur
    // unescaped, and types never contain commas at the top level: Dict's
    // comma is inside brackets).
    let (path_part, type_part) = match split_path_and_type(inner, name) {
        Ok(parts) => parts,
        Err(e) => return Some(Err(e)),
    };

    let path = match unquote(path_part.trim()) {
        Some(p) => p,
        None => {
            return Some(Err(format!(
                "{name} path must be a quoted string, got {path_part:?}"
            )))
        }
    };
    if path.is_empty() {
        return Some(Err(format!("{name} path is empty")));
    }

    let ty = match type_part {
        None => WeftType::Primitive(weft_core::WeftPrimitive::String),
        Some(t) => match WeftType::parse(t.trim()) {
            Some(ty) => ty,
            None => return Some(Err(format!("{name}: invalid type {:?}", t.trim()))),
        },
    };

    Some(Ok(FileRef { path, ty, marker }))
}

/// Split `"path"` or `"path", Type` into the path part and optional type
/// part, on the first comma that is not inside brackets. The path is
/// quoted, so the first top-level comma after the closing quote separates
/// the two arguments.
fn split_path_and_type<'a>(inner: &'a str, name: &str) -> Result<(&'a str, Option<&'a str>), String> {
    let mut depth = 0i32;
    let mut in_quote = false;
    for (i, c) in inner.char_indices() {
        match c {
            '"' => in_quote = !in_quote,
            '[' if !in_quote => depth += 1,
            ']' if !in_quote => depth -= 1,
            ',' if !in_quote && depth == 0 => {
                let path = &inner[..i];
                let ty = inner[i + 1..].trim();
                if ty.is_empty() {
                    return Err(format!("{name}: trailing comma with no type"));
                }
                return Ok((path, Some(ty)));
            }
            _ => {}
        }
    }
    Ok((inner, None))
}

fn unquote(s: &str) -> Option<String> {
    if s.len() >= 2 && s.starts_with('"') && s.ends_with('"') {
        Some(s[1..s.len() - 1].to_string())
    } else {
        None
    }
}

/// Does this ref DEFER to the build's asset resolution: an `@asset` whose
/// type is a stored-file reference, so its bytes never ride the compile.
/// Every other ref (both markers with text types) reads + casts inline at
/// parse.
pub fn is_asset_ref(file_ref: &FileRef) -> bool {
    file_ref.marker == FileMarker::Asset && file_ref.ty.references_file()
}

/// Is this ref's source an external URL (resolved inline to a url-form file
/// value; never synced, never uploaded) rather than a project/disk path?
pub fn is_url_ref(file_ref: &FileRef) -> bool {
    file_ref.path.starts_with("http://") || file_ref.path.starts_with("https://")
}

/// Is this ref's source a TENANT-LESS runtime storage key
/// (`project/<id>/<file>` etc.): a file that exists only in runtime storage
/// (picked from the project's stored files), never on disk. Nothing to sync
/// or upload; the build resolves it by looking the file up through the
/// storage listing and re-anchoring the tenant.
pub fn is_runtime_key_ref(file_ref: &FileRef) -> bool {
    weft_core::storage::key::is_scope_key(&file_ref.path)
}

/// Collect every MEDIA ref whose source is a RUNTIME STORAGE KEY (see
/// [`is_runtime_key_ref`]), deduplicated. The build driver resolves each
/// through the storage listing into the same `path -> value` map the sync's
/// file refs use, so [`apply_asset_resolutions`] treats both identically.
pub fn collect_runtime_key_refs(
    project: &weft_core::project::ProjectDefinition,
) -> Vec<FileRef> {
    let mut seen = std::collections::BTreeSet::new();
    let mut refs = Vec::new();
    for node in &project.nodes {
        for value in node.config.as_object().map(|o| o.values()).into_iter().flatten() {
            let Some(raw) = value.as_str() else { continue };
            let Some(Ok(file_ref)) = parse_marker(raw) else { continue };
            if is_asset_ref(&file_ref)
                && is_runtime_key_ref(&file_ref)
                && seen.insert(file_ref.path.clone())
            {
                refs.push(file_ref);
            }
        }
    }
    refs
}

/// Resolve runtime-key refs (from [`collect_runtime_key_refs`]) against a
/// stored-file `listing` into `map` (the same `path -> value` map the
/// sync's file refs use). The listing's keys are tenant-anchored; each ref
/// is the tenant-less scope key, so the match strips the tenant segment. A
/// key with no listed file stays unmapped and fails loud in
/// [`apply_asset_resolutions`] with the stored-file message. Pure (the
/// driver supplies the listing), so every build driver resolves
/// identically; only the listing fetch is theirs.
pub fn resolve_runtime_key_refs(
    refs: &[FileRef],
    listing: &[weft_core::storage::StoredFileMeta],
    map: &mut std::collections::BTreeMap<String, serde_json::Value>,
) {
    for r in refs {
        let Some(meta) = listing
            .iter()
            .find(|f| f.key.split_once('/').is_some_and(|(_, scope_key)| scope_key == r.path))
        else {
            continue;
        };
        let file = weft_core::storage::StoredFile {
            key: meta.key.clone(),
            mime_type: meta.mime_type.clone(),
            size_bytes: meta.size_bytes,
            filename: meta.filename.clone(),
        };
        map.insert(r.path.clone(), weft_core::storage::typed_file_value(&file, &r.ty));
    }
}

/// What resolving one `@file` marker produced.
#[derive(Debug, PartialEq)]
enum Resolved {
    /// The value to substitute into config.
    Value(serde_json::Value),
    /// Editor/lenient defer: keep the raw `@file(...)` string, but record the
    /// ref so the UI knows the field is file-backed.
    Deferred,
}

/// Resolve a single `@file` marker.
///
/// TEXT refs (`String`, `Number`, JSON shapes) read the referenced content
/// through `fs.reader` (disk, in-memory map, DB rows) and cast it to the
/// declared type; path resolution + the trusted-tree containment guard live in
/// the reader.
///
/// File-typed `@asset` refs (`Image`, `Audio`, `File`, ...) ALWAYS defer
/// here: their bytes never ride a compile. Every parse (editor and build
/// alike) keeps the raw marker string in config and records the ref; a BUILD
/// then resolves them in one explicit post-pass, [`apply_asset_resolutions`],
/// after the pre-build asset sync produced the map.
///
/// A file-typed `@file` is a hard error: those values cannot be written back
/// as text, which is `@file`'s whole contract (each type declares its side
/// via `supports_bidirectional_edit`). A text-typed `@asset` reads inline
/// like `@file` (the editor renders it read-only), but only from a project
/// file: a URL or stored-file source has no text to read at parse.
fn resolve(file_ref: &FileRef, fs: &CompileFs) -> Result<Resolved, String> {
    if is_asset_ref(file_ref) {
        return Ok(Resolved::Deferred);
    }
    if file_ref.marker == FileMarker::File && !file_ref.ty.supports_bidirectional_edit() {
        return Err(format!(
            "@file cannot carry a {} value (it cannot be edited back as text); \
             use @asset({:?}, {})",
            file_ref.ty, file_ref.path, file_ref.ty
        ));
    }
    // From here the ref reads text inline (both markers). A URL source only
    // exists in the deferred file-typed `@asset` form: whichever marker
    // carries it here, there is no text to read at parse (and no file to
    // edit back), so name the correct directive instead of failing the disk
    // read on an `https://...` path.
    if is_url_ref(file_ref) {
        return Err(format!(
            "a {}-typed {} cannot read from a URL; a URL source needs a \
             file-typed @asset (Image, Audio, ..., File)",
            file_ref.ty,
            file_ref.marker.directive()
        ));
    }
    // A stored-file key is likewise only meaningful as a deferred file-typed
    // `@asset`. Only `@asset` rejects the key form: `@file` paths keep pure
    // disk semantics, because a project may legitimately contain a directory
    // named after a scope tag (`project/...`) and its files must stay
    // readable through `@file`.
    if file_ref.marker == FileMarker::Asset && is_runtime_key_ref(file_ref) {
        return Err(format!(
            "a {}-typed @asset must reference a project file; a stored \
             file needs a file type (Image, Audio, ..., File)",
            file_ref.ty
        ));
    }
    let Some(base) = fs.base else {
        return Err(format!(
            "@file({:?}) cannot be resolved outside a project",
            file_ref.path
        ));
    };
    let resolved = fs
        .reader
        .resolve_and_read(base, std::path::Path::new(&file_ref.path))
        .map_err(|e| format!("@file {e}"))?;
    file_ref.ty.cast_text(&resolved.content).map(Resolved::Value)
}

/// Resolve every `@file(...)` marker in one node's config map in place.
/// Errors (malformed marker, unreadable file, failed cast) are collected
/// against the field's source line (falling back to `node_span`). When `fs` has
/// no anchor (`base == None`, parsing outside a project), a `@file` marker is an
/// error: there is no directory to resolve it against (the message comes from
/// `resolve`). Resolved references are recorded in `file_refs` so the editor
/// knows the field is file-backed.
///
/// Called per node by the parser's recursive walk (top-level nodes and every
/// group descendant) so `@file` inside a group body resolves too.
pub(crate) fn resolve_node_file_refs(
    config: &mut serde_json::Map<String, serde_json::Value>,
    config_spans: &std::collections::BTreeMap<String, ConfigFieldSpan>,
    file_refs: &mut std::collections::BTreeMap<String, FileRef>,
    node_span: Span,
    fs: &CompileFs,
    errors: &mut Vec<CompileError>,
) {
    for (key, value) in config.iter_mut() {
        let raw = match value.as_str() {
            Some(s) => s,
            None => continue,
        };
        let marker = match parse_marker(raw) {
            Some(m) => m,
            None => continue, // ordinary value, leave it
        };
        // The culprit is the field that carries the marker; fall back to the
        // node's declaration span only if the field span is missing.
        let span = config_spans.get(key).map(|s| s.span).unwrap_or(node_span);
        let file_ref = match marker {
            Ok(fr) => fr,
            Err(msg) => {
                errors.push(CompileError::at(span, msg));
                continue;
            }
        };
        match resolve(&file_ref, fs) {
            Ok(Resolved::Value(resolved)) => {
                *value = resolved;
                file_refs.insert(key.clone(), file_ref);
            }
            // A deferred media ref (editor/lenient parse): the raw `@file`
            // string stays in config for the UI to render, and the ref is
            // recorded so the field is known to be file-backed.
            Ok(Resolved::Deferred) => {
                file_refs.insert(key.clone(), file_ref);
            }
            Err(msg) => errors.push(CompileError::at(span, msg)),
        }
    }
}

/// Resolve every deferred `@asset` ref in a compiled definition, in
/// place: URL refs become url-form file values inline; path refs substitute
/// the pre-build asset sync's stored-file value from `map` (keyed by the raw
/// path as written). The ONE build-side resolution step, run by every build
/// driver between the sync and the hash/stage, so the definition that gets
/// hashed and shipped carries resolved values and never a raw marker.
///
/// Errors name every unresolved path ref at once ("the sync did not resolve
/// it": the file vanished between collect and apply, or the driver skipped the
/// sync). Text refs were already resolved at parse and never appear here.
pub fn apply_asset_resolutions(
    project: &mut weft_core::project::ProjectDefinition,
    map: &std::collections::BTreeMap<String, serde_json::Value>,
) -> Result<(), Vec<String>> {
    let mut missing = Vec::new();
    for node in &mut project.nodes {
        let Some(config) = node.config.as_object_mut() else { continue };
        for value in config.values_mut() {
            let Some(raw) = value.as_str() else { continue };
            let Some(Ok(file_ref)) = parse_marker(raw) else { continue };
            if !is_asset_ref(&file_ref) {
                continue;
            }
            if is_url_ref(&file_ref) {
                *value = weft_core::storage::url_file_value(&file_ref.path, &file_ref.ty);
                continue;
            }
            match map.get(&file_ref.path) {
                Some(resolved) => *value = resolved.clone(),
                None if is_runtime_key_ref(&file_ref) => missing.push(format!(
                    "@asset({:?}, {}) names a stored file that does not exist (deleted, or \
                     picked from another project)",
                    file_ref.path, file_ref.ty
                )),
                None => missing.push(format!(
                    "@asset({:?}, {}) is not a synced asset (missing file, or the build ran \
                     without the asset sync)",
                    file_ref.path, file_ref.ty
                )),
            }
        }
    }
    if missing.is_empty() {
        Ok(())
    } else {
        Err(missing)
    }
}

/// Collect every deferred `@asset` ref in a project definition whose source
/// is a PATH (URL refs resolve inline and never sync). The pre-build asset
/// sync's input: run over a DEFERRED parse (asset refs still hold their raw
/// `@asset` strings in config). Deduplicated by path, first declared type
/// wins (the type only picks the marker kind; the bytes are the identity).
pub fn collect_asset_refs(project: &weft_core::project::ProjectDefinition) -> Vec<FileRef> {
    let mut seen = std::collections::BTreeSet::new();
    let mut refs = Vec::new();
    for node in &project.nodes {
        for value in node.config.as_object().map(|o| o.values()).into_iter().flatten() {
            let Some(raw) = value.as_str() else { continue };
            let Some(Ok(file_ref)) = parse_marker(raw) else { continue };
            if is_asset_ref(&file_ref)
                && !is_url_ref(&file_ref)
                && !is_runtime_key_ref(&file_ref)
                && seen.insert(file_ref.path.clone())
            {
                refs.push(file_ref);
            }
        }
    }
    refs
}

#[cfg(test)]
#[path = "tests/file_ref_tests.rs"]
mod tests;
