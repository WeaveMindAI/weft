//! `@file("path", Type)` value injection.
//!
//! A config value of the form `@file("path")` or `@file("path", Type)`
//! injects the content of an external file as a typed value at compile
//! time. `Type` defaults to `String` (verbatim text, the prompt/document
//! case) and is drawn from the existing `WeftType` vocabulary, so the
//! same type names used for ports work here.
//!
//! Resolution is a single post-parse pass (`resolve_node_file_refs` per node,
//! walked over top-level nodes and group descendants) over every
//! node's config map: the four text-parse sites already store `@file(...)`
//! verbatim as a string, so none of them need to know about this feature.
//! The pass finds the markers, reads the file relative to the project root,
//! casts the bytes via `WeftType::cast_text`, and replaces the value. A bad
//! path, a malformed marker, or a failed cast becomes a `CompileError`
//! pointing at the field's source line.

use std::path::Path;

use weft_core::project::{ConfigFieldSpan, FileRef, Span};
use weft_core::WeftType;

use crate::weft_compiler::CompileError;

/// Recognize and parse a `@file("path"[, Type])` marker from a raw value
/// string. Returns `None` if the string is not a `@file` marker at all (so
/// the caller leaves ordinary values untouched), `Some(Err(..))` if it
/// looks like one but is malformed.
///
/// The path must be a double-quoted string. The optional second argument is
/// a type expression parsed with the same `WeftType::parse` that port
/// declarations use; when omitted the type is `String`.
pub fn parse_marker(raw: &str) -> Option<Result<FileRef, String>> {
    let trimmed = raw.trim();
    // Exactly the `@file` directive (not `@filesystem`); the marker module is the
    // single home for directive/arg extraction and tolerates a space before `(`.
    if crate::cst::marker::directive(trimmed) != "file" {
        return None;
    }
    let Some(inner) = crate::cst::marker::args_raw(trimmed) else {
        return Some(Err("@file must be followed by (\"path\")".into()));
    };

    // Split into the quoted path and an optional type, on the first
    // top-level comma (the path is quoted so a comma inside it can't occur
    // unescaped, and types never contain commas at the top level: Dict's
    // comma is inside brackets).
    let (path_part, type_part) = match split_path_and_type(inner) {
        Ok(parts) => parts,
        Err(e) => return Some(Err(e)),
    };

    let path = match unquote(path_part.trim()) {
        Some(p) => p,
        None => {
            return Some(Err(format!(
                "@file path must be a quoted string, got {path_part:?}"
            )))
        }
    };
    if path.is_empty() {
        return Some(Err("@file path is empty".into()));
    }

    let ty = match type_part {
        None => WeftType::Primitive(weft_core::WeftPrimitive::String),
        Some(t) => match WeftType::parse(t.trim()) {
            Some(ty) => ty,
            None => return Some(Err(format!("@file: invalid type {:?}", t.trim()))),
        },
    };

    Some(Ok(FileRef { path, ty }))
}

/// Split `"path"` or `"path", Type` into the path part and optional type
/// part, on the first comma that is not inside brackets. The path is
/// quoted, so the first top-level comma after the closing quote separates
/// the two arguments.
fn split_path_and_type(inner: &str) -> Result<(&str, Option<&str>), String> {
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
                    return Err("@file: trailing comma with no type".into());
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

/// Resolve a single `@file` marker against the project root: read the file
/// and cast its content to the declared type. The path is resolved relative
/// to `project_root` and must stay inside it.
///
/// Threat model: the project tree is TRUSTED (this runs at build time on the
/// user's own project; referenced files are part of the project, like infra
/// specs). The containment check guards against an accidental `../` typo leaking
/// a host file into the build, not a hostile tree. It is robust for that:
/// `canonicalize` resolves `..` and follows symlinks before the prefix check, so
/// any path (including via a symlink) that lands outside the root is rejected.
/// The only residual gap is the canonicalize→read TOCTOU window, which is
/// irrelevant for a trusted local tree. If `@file` ever resolves UNTRUSTED input,
/// switch to O_NOFOLLOW / per-component symlink rejection to close that window.
pub fn resolve(file_ref: &FileRef, project_root: &Path) -> Result<serde_json::Value, String> {
    let joined = project_root.join(&file_ref.path);

    // Reject path escape: the resolved canonical path must live under the
    // project root (canonicalize resolves `..` and symlinks, so an escaping
    // target fails the prefix check). Trusted-tree typo guard, see fn doc.
    let canonical_root = project_root
        .canonicalize()
        .map_err(|e| format!("project root {project_root:?} is unreadable: {e}"))?;
    let canonical = joined
        .canonicalize()
        .map_err(|e| format!("@file path {:?} cannot be read: {e}", file_ref.path))?;
    if !canonical.starts_with(&canonical_root) {
        return Err(format!(
            "@file path {:?} escapes the project root",
            file_ref.path
        ));
    }

    let text = std::fs::read_to_string(&canonical)
        .map_err(|e| format!("@file path {:?} cannot be read: {e}", file_ref.path))?;
    file_ref.ty.cast_text(&text)
}

/// Resolve every `@file(...)` marker in one node's config map in place.
/// Errors (malformed marker, unreadable file, failed cast) are collected
/// against the field's source line (falling back to `node_line`). When
/// `base_dir` is `None` (parsing outside a project), a `@file` marker is an
/// error: there is no directory to resolve it against. Resolved references
/// are recorded in `file_refs` so the editor knows the field is file-backed.
///
/// Called per node by the parser's recursive walk (top-level nodes and every
/// group descendant) so `@file` inside a group body resolves too.
pub(crate) fn resolve_node_file_refs(
    config: &mut serde_json::Map<String, serde_json::Value>,
    config_spans: &std::collections::BTreeMap<String, ConfigFieldSpan>,
    file_refs: &mut std::collections::BTreeMap<String, FileRef>,
    node_span: Span,
    base_dir: Option<&Path>,
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
        let root = match base_dir {
            Some(r) => r,
            None => {
                errors.push(CompileError::at(span, format!("@file({:?}) cannot be resolved outside a project", file_ref.path)));
                continue;
            }
        };
        match resolve(&file_ref, root) {
            Ok(resolved) => {
                *value = resolved;
                file_refs.insert(key.clone(), file_ref);
            }
            Err(msg) => errors.push(CompileError::at(span, msg)),
        }
    }
}

#[cfg(test)]
#[path = "tests/file_ref_tests.rs"]
mod tests;
