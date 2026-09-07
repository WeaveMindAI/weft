//! `@file("path", Type)` / `@asset("path", Type)` value injection.
//!
//! Both markers make a config field's value come from somewhere else; they
//! differ in the EDIT CONTRACT. `@file` is bidirectional: the referenced
//! file's text content is the value, and editing the field writes back to
//! the file; it requires a type that `supports_bidirectional_edit`. `@asset`
//! is pull-only: nothing ever writes back. A file-typed `@asset` (Image,
//! Audio, ...) defers to the build's asset resolution; a text-typed one
//! from a project file is read + cast at parse exactly like `@file`, just
//! rendered read-only, and one from a URL or a stored-file key is fetched
//! at build. `@file`'s type defaults to `String`; `@asset` always names its
//! type (a value carries exactly one concrete marker, and the compiler
//! never guesses a kind from a name or from bytes), so it is `Image`,
//! `Video`, `Audio`, `Blob`, or a text type. Types are drawn from the
//! existing `WeftType` vocabulary, so the same names used for ports work
//! here.
//!
//! Type names are resolved when the literal is parsed in its source scope.
//! File contents resolve once on the flattened graph, where ordinary inputs,
//! group inputs and include arguments all have an owning node and field span.
//! The pass reads each file relative to the source that wrote its reference,
//! casts the bytes via `WeftType::cast_text`, and replaces the value. A bad
//! path, a malformed marker, or a failed cast becomes a `CompileError`
//! pointing at the field's source line.

use weft_core::project::{FileMarker, FileRef};
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
    // module is the single home for directive/arg extraction, and its arg
    // list must directly abut the name (a spaced paren, or text after the
    // close, is prose, never a marker).
    let marker = match crate::cst::marker::directive(trimmed) {
        "file" => FileMarker::File,
        "asset" => FileMarker::Asset,
        _ => return None,
    };
    let name = match marker {
        FileMarker::File => "@file",
        FileMarker::Asset => "@asset",
    };
    // Each malformation gets its own actionable message (mirroring
    // @require_one_of's split); the shape comes from the one balanced
    // scan, never re-derived by string search (a `)` inside a quoted
    // path once made "unclosed" read as "trailing text").
    let inner = match crate::cst::marker::args(trimmed) {
        crate::cst::marker::MarkerArgs::Args(body) => body,
        crate::cst::marker::MarkerArgs::NoList => {
            return Some(Err(format!(
                "{name} must be followed by (\"path\") directly after the name \
                 (no space before `(`)"
            )));
        }
        crate::cst::marker::MarkerArgs::Unclosed => {
            return Some(Err(format!("{name} is missing its closing parenthesis")));
        }
        crate::cst::marker::MarkerArgs::TrailingText => {
            return Some(Err(format!(
                "{name}(...) must be the whole value: text after the closing \
                 parenthesis would be silently replaced by the file's contents"
            )));
        }
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

    let ty = match (type_part, marker) {
        (None, FileMarker::File) => WeftType::Primitive(weft_core::WeftPrimitive::String),
        // No default for an asset: the type is what picks the marker the
        // value carries, and nothing else may (not the name, not the
        // bytes).
        (None, FileMarker::Asset) => {
            return Some(Err(format!(
                "@asset({path:?}) needs its type as a second argument, e.g. \
                 @asset({path:?}, Image); the kind is never guessed from the name. \
                 Image, Video, Audio, Blob for a file, or a text type such as String"
            )));
        }
        (Some(t), _) => match WeftType::parse(t.trim()) {
            Some(ty) => ty,
            None => return Some(Err(format!("{name}: invalid type {:?}", t.trim()))),
        },
    };
    // A file type on an asset names ONE kind: `File`/`Media` would leave
    // the marker to a guess.
    if marker == FileMarker::Asset && ty.references_file() && ty.concrete_file_kind().is_none() {
        return Some(Err(format!(
            "@asset({path:?}, {ty}) must name one kind of file: Image, Video, Audio, or \
             Blob (a value carries exactly one marker, and {ty} leaves it open)"
        )));
    }

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
    serde_json::from_str(s).ok()
}

/// Freeze scoped type names into their portable wire spelling before leaving
/// the source scope. The source text and its edit spans remain untouched.
pub(crate) fn resolve_marker_types(value: &mut serde_json::Value) {
    each_value_mut(value, &mut |leaf| {
        let Some(raw) = leaf.as_str() else { return };
        let Some(Ok(file_ref)) = parse_marker(raw) else { return };
        *leaf = serde_json::Value::String(marker_text(&file_ref));
    });
}

fn marker_text(file_ref: &FileRef) -> String {
    let name = match file_ref.marker { FileMarker::File => "file", FileMarker::Asset => "asset" };
    format!("@{name}({}, {})", serde_json::to_string(&file_ref.path).expect("a path serializes"), file_ref.ty.wire_string())
}

/// Does this ref DEFER to the build: an `@asset` whose type is a stored
/// file (its bytes never ride the compile), or whose source is a URL or a
/// stored-file key (there is nothing on disk to read at parse, whatever
/// the type). Every other ref (both markers, text-typed, from a project
/// file) reads + casts inline at parse.
pub fn is_deferred_ref(file_ref: &FileRef) -> bool {
    file_ref.marker == FileMarker::Asset
        && (file_ref.ty.references_file() || is_url_ref(file_ref) || is_runtime_key_ref(file_ref))
}

/// The type a written constant has, for the type check and the cast: a
/// `@file`/`@asset` marker is the type it declares (the value it stands
/// for, never the marker text), a list is a list of its elements' types,
/// and anything else is what `WeftType::infer` reads off the JSON.
pub fn literal_type(value: &serde_json::Value) -> WeftType {
    match value {
        serde_json::Value::String(s) => match parse_marker(s) {
            Some(Ok(file_ref)) => file_ref.ty,
            _ => WeftType::infer(value),
        },
        serde_json::Value::Array(items) if !items.is_empty() => {
            let elements: Vec<WeftType> = items.iter().map(literal_type).collect();
            WeftType::List(Box::new(WeftType::unify_types(&elements)))
        }
        _ => WeftType::infer(value),
    }
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

/// Every string a node's written values hold, however deep. A marker is a
/// value, so it sits at the top (`file: @asset(...)`) or inside a list
/// (`attachments: [@asset(...), @asset(...)]`), and every pass that reads
/// or rewrites markers walks the same way.
fn each_string(value: &serde_json::Value, visit: &mut impl FnMut(&str)) {
    match value {
        serde_json::Value::String(s) => visit(s),
        serde_json::Value::Array(items) => items.iter().for_each(|v| each_string(v, visit)),
        serde_json::Value::Object(map) => map.values().for_each(|v| each_string(v, visit)),
        _ => {}
    }
}

/// The same walk, for a pass that REPLACES a marker with what it resolved
/// to (a stored-file value). Visits every leaf, string or not, so the
/// caller decides what to do with it.
fn each_value_mut(value: &mut serde_json::Value, visit: &mut impl FnMut(&mut serde_json::Value)) {
    match value {
        serde_json::Value::Array(items) => {
            items.iter_mut().for_each(|v| each_value_mut(v, visit))
        }
        serde_json::Value::Object(map) => {
            map.values_mut().for_each(|v| each_value_mut(v, visit))
        }
        leaf => visit(leaf),
    }
}

/// The one walk behind both public collectors: every `@asset` ref in
/// every node's config and port literals, deduplicated by path (first
/// declared type wins), filtered by `keep`.
fn collect_refs(
    project: &weft_core::project::ProjectDefinition,
    keep: impl Fn(&FileRef) -> bool,
) -> Vec<FileRef> {
    let mut seen = std::collections::BTreeSet::new();
    let mut refs = Vec::new();
    for node in &project.nodes {
        for value in node
            .config
            .as_object()
            .map(|o| o.values())
            .into_iter()
            .flatten()
            .chain(node.port_literals.values())
        {
            for file_ref in refs_in_value(value) {
                if is_deferred_ref(&file_ref)
                    && keep(&file_ref)
                    && seen.insert(file_ref.resolution_key())
                {
                    refs.push(file_ref);
                }
            }
        }
    }
    refs
}

pub(crate) fn refs_in_value(value: &serde_json::Value) -> Vec<FileRef> {
    let mut refs = Vec::new();
    each_string(value, &mut |text| {
        if let Some(Ok(reference)) = parse_marker(text) { refs.push(reference); }
    });
    refs
}

/// Collect every FILE-typed ref whose source is a RUNTIME STORAGE KEY (see
/// [`is_runtime_key_ref`]), deduplicated. The build driver resolves each
/// through the storage listing into the same `path -> value` map the sync's
/// file refs use, so [`apply_asset_resolutions`] treats both identically.
pub fn collect_runtime_key_refs(
    project: &weft_core::project::ProjectDefinition,
) -> Vec<FileRef> {
    collect_refs(project, |r| is_runtime_key_ref(r) && r.ty.references_file())
}

/// Collect every TEXT-typed `@asset` whose source is a URL or a stored-file
/// key: nothing on disk to read at parse, so the build driver fetches the
/// bytes, casts them to the declared type through [`resolve_text_bytes`],
/// and puts the value into the same map. Deduplicated by path.
pub fn collect_remote_text_refs(
    project: &weft_core::project::ProjectDefinition,
) -> Vec<FileRef> {
    collect_refs(project, |r| !r.ty.references_file())
}

/// Turn the bytes a build driver fetched for a text-typed remote `@asset`
/// into the value the program reads: the declared type's own cast of the
/// text (the same cast a project-file `@file` gets at parse), so `Number`
/// and JSON shapes work from a URL exactly as from disk. The error names
/// the source.
pub fn resolve_text_bytes(file_ref: &FileRef, bytes: &[u8]) -> Result<serde_json::Value, String> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| format!("@asset({:?}, {}): the content is not UTF-8 text: {e}", file_ref.path, file_ref.ty))?;
    file_ref
        .ty
        .cast_text(text)
        .map_err(|e| format!("@asset({:?}, {}): {e}", file_ref.path, file_ref.ty))
}

/// Resolve runtime-key refs (from [`collect_runtime_key_refs`]) against a
/// stored-file `listing` into `map` (the same `resolution key -> value`
/// map the sync's file refs use). The listing's keys are tenant-anchored; each ref
/// is the tenant-less scope key, so the match strips the tenant segment. A
/// key with no listed file stays unmapped and fails loud in
/// [`apply_asset_resolutions`] with the stored-file message. A listed file
/// whose stored kind (from the mime the upload recorded) is not the kind
/// the ref declared is returned as an error naming both, the way the sync
/// holds a disk file to its declaration; `Blob` accepts anything. Pure
/// (the driver supplies the listing), so every build driver resolves
/// identically; only the listing fetch is theirs.
pub fn resolve_runtime_key_refs(
    refs: &[FileRef],
    listing: &[weft_core::storage::StoredFileMeta],
    map: &mut std::collections::BTreeMap<String, serde_json::Value>,
) -> Result<(), Vec<String>> {
    let mut mismatched = Vec::new();
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
        if let Some(declared) = r.ty.concrete_file_kind() {
            let stored = file.kind();
            if declared != weft_core::weft_type::FileKind::Blob && stored != declared {
                mismatched.push(format!(
                    "@asset({:?}, {}): the stored file is {} ({}), not {}",
                    r.path,
                    r.ty,
                    stored.primitive(),
                    meta.mime_type,
                    declared.primitive()
                ));
                continue;
            }
        }
        map.insert(r.resolution_key(), weft_core::storage::typed_file_value(&file, &r.ty));
    }
    if mismatched.is_empty() { Ok(()) } else { Err(mismatched) }
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
/// Deferred refs (see [`is_deferred_ref`]: a file-typed `@asset`, or an
/// `@asset` from a URL or a stored-file key) ALWAYS defer here: their bytes
/// never ride a compile. Every parse (editor and build alike) keeps the raw
/// marker string and records the ref; a BUILD then resolves them in one
/// explicit post-pass, [`apply_asset_resolutions`], after the pre-build
/// asset sync and the remote fetches produced the map.
///
/// A file-typed `@file` is a hard error: those values cannot be written back
/// as text, which is `@file`'s whole contract (each type declares its side
/// via `supports_bidirectional_edit`). So is a `@file` from a URL: there is
/// no file to write back to. `@file` paths keep pure disk semantics
/// otherwise, because a project may legitimately contain a directory named
/// after a scope tag (`project/...`) and its files must stay readable.
fn resolve(file_ref: &FileRef, fs: &CompileFs) -> Result<Resolved, String> {
    if is_deferred_ref(file_ref) {
        return Ok(Resolved::Deferred);
    }
    if file_ref.marker == FileMarker::File && !file_ref.ty.supports_bidirectional_edit() {
        return Err(format!(
            "@file cannot carry a {} value (it cannot be edited back as text); \
             use @asset({:?}, {})",
            file_ref.ty, file_ref.path, file_ref.ty
        ));
    }
    if is_url_ref(file_ref) {
        return Err(format!(
            "@file cannot read from a URL (there is no file to write edits back to); \
             use @asset({:?}, {}), which is fetched at build",
            file_ref.path, file_ref.ty
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

/// Re-anchor a disk ref written in an included file onto the compiled
/// file's directory (the project root on a build, where the compiled
/// file is the project's `main.weft`): `@file("x")` inside
/// `components/box.weft` names `components/x`. Every consumer of a
/// ref's path (the editor's file watcher and save, the asset sync, the
/// build's resolution map) resolves against that one anchor, so a ref
/// only ever leaves the compiler in one spelling whichever file the
/// text was typed in. A URL or a stored-file key names nothing on disk
/// and is left alone; so is a ref from the compiled file itself.
///
/// A path that lands OUTSIDE the anchor keeps its absolute spelling
/// rather than being refused: naming a file where it already sits is
/// allowed (`weft-cli`'s asset source opens an absolute path as given),
/// and it is allowed identically from the compiled file, where this
/// function does not run at all. Refusing it here only made the same
/// text legal or illegal depending on which file it was typed in.
fn anchor_on_root(
    mut file_ref: FileRef,
    source_file: Option<&str>,
    root: &Result<std::path::PathBuf, String>,
) -> Result<FileRef, String> {
    let Some(source_file) = source_file else { return Ok(file_ref) };
    if is_url_ref(&file_ref) || is_runtime_key_ref(&file_ref) {
        return Ok(file_ref);
    }
    let root = root.as_ref().map_err(|e| e.clone())?;
    let dir = std::path::Path::new(source_file).parent().unwrap_or(std::path::Path::new(""));
    let joined = crate::file_reader::normalize_lexical(&dir.join(&file_ref.path)).ok_or_else(|| {
        format!(
            "{}({:?}) climbs above the filesystem root",
            file_ref.marker.directive(),
            file_ref.path
        )
    })?;
    // A path is text on the wire, and a definition hash is computed over
    // it, so a lossy conversion would make the same project hash
    // differently on two machines. Refuse rather than replace bytes.
    let anchored = match joined.strip_prefix(root) {
        Ok(relative) => relative,
        Err(_) => joined.as_path(),
    };
    file_ref.path = anchored
        .to_str()
        .ok_or_else(|| {
            format!(
                "{}({:?}) resolves to a path that is not text",
                file_ref.marker.directive(),
                file_ref.path
            )
        })?
        .to_string();
    Ok(file_ref)
}

/// Resolve all file-backed values through one pass after includes and groups
/// have been flattened. A ref written in an included file is first spelled
/// relative to the project root (see [`anchor_on_root`]), so everything
/// resolves against one anchor and the ref's path means the same thing to
/// every consumer.
pub(crate) fn resolve_project_file_refs(
    project: &mut weft_core::project::ProjectDefinition,
    root_fs: &CompileFs,
    errors: &mut Vec<CompileError>,
) {
    // Computed once: an include-sourced ref needs the root's identity to be
    // spelled under it. Compiling outside a project (no anchor) only fails
    // when such a ref actually turns up.
    let root = match root_fs.base {
        Some(base) => root_fs.reader.identity(base),
        None => Err("a file reference cannot be resolved outside a project".into()),
    };
    for node in &mut project.nodes {
      let config_spans = &node.config_spans;
      let literal_spans = &node.port_literal_spans;
      for (key, value, field) in node.config.as_object_mut().into_iter().flat_map(|map| map.iter_mut())
          .map(|(key, value)| (key, value, config_spans.get(key)))
          .chain(node.port_literals.iter_mut().map(|(key, value)| (key, value, literal_spans.get(key)))) {
        // The culprit is the field that carries the marker; fall back to the
        // node's declaration span only if the field span is missing.
        let span = field.map(|s| s.span).unwrap_or(node.span.unwrap_or_default());
        // A present field span with no file names the main source, even
        // when its owner is a group that came from an included file.
        let source_file = field.map(|s| s.source_file.as_deref()).unwrap_or(node.source_file.as_deref());
        // A marker sits at the top of a field (`systemPrompt: @file(...)`)
        // or inside a list (`attachments: [@asset(...), @asset(...)]`), and
        // every one of them resolves. Only a field that is ONE marker is
        // recorded as file-backed: that record drives editing the file
        // through the field, which is a thing you do to one file.
        let single = value.as_str().is_some();
        each_value_mut(value, &mut |leaf| {
            let Some(raw) = leaf.as_str() else { return };
            let Some(marker) = parse_marker(raw) else { return }; // ordinary value
            let file_ref = match marker {
                Ok(fr) => fr,
                Err(msg) => {
                    // A NESTED string that merely starts with the
                    // directive word and never OPENS an argument list
                    // ("@file the report please" inside a prompt array)
                    // is user prose: the abutting paren is what states
                    // intent. A string that DID open one is always an
                    // attempted marker, however malformed (an unclosed
                    // paren split across lines, trailing text), and
                    // stays loud; so does a field that IS the string.
                    if !single && !crate::cst::marker::has_abutting_args(raw.trim()) {
                        return;
                    }
                    errors.push(CompileError::at(span, msg).in_file(source_file));
                    return;
                }
            };
            let file_ref = match anchor_on_root(file_ref, source_file, &root) {
                Ok(fr) => fr,
                Err(msg) => {
                    errors.push(CompileError::at(span, msg).in_file(source_file));
                    return;
                }
            };
            match resolve(&file_ref, root_fs) {
                Ok(Resolved::Value(resolved)) => {
                    *leaf = resolved;
                    if single {
                        node.file_refs.insert(key.clone(), file_ref);
                    }
                }
                // A deferred media ref: the marker stays in config (the
                // editor renders it, the build resolves it), respelled
                // under the root so the sync and the resolution map find
                // the same file, and the ref is recorded so the field is
                // known to be file-backed.
                Ok(Resolved::Deferred) => {
                    *leaf = serde_json::Value::String(marker_text(&file_ref));
                    if single {
                        node.file_refs.insert(key.clone(), file_ref);
                    }
                }
                Err(msg) => errors.push(CompileError::at(span, msg).in_file(source_file)),
            }
        });
      }
    }
}

/// Resolve every deferred `@asset` ref in a compiled definition, in
/// place: file-typed URL refs become url-form file values inline; every
/// other deferred ref substitutes the value the driver put in `map` (keyed
/// by the raw path as written): the pre-build asset sync's stored-file
/// value for a disk path, the storage listing's for a stored key, the
/// fetched-and-cast text for a text-typed URL or key. The ONE build-side
/// resolution step, run by every build driver between the sync and the
/// hash/stage, so the definition that gets hashed and shipped carries
/// resolved values and never a raw marker.
///
/// Errors name every unresolved ref at once ("the sync did not resolve
/// it": the file vanished between collect and apply, or the driver skipped
/// a step). Project-file text refs were already resolved at parse and never
/// appear here.
pub fn apply_asset_resolutions(
    project: &mut weft_core::project::ProjectDefinition,
    map: &std::collections::BTreeMap<String, serde_json::Value>,
) -> Result<(), Vec<String>> {
    let mut missing = Vec::new();
    // An `@asset` may sit on a config field (`node.config`) or on a port
    // (`node.port_literals`, where the enrich normalization homes
    // port-driven body values); resolve both.
    fn resolve_value(
        value: &mut serde_json::Value,
        map: &std::collections::BTreeMap<String, serde_json::Value>,
        missing: &mut Vec<String>,
    ) {
        let Some(raw) = value.as_str() else { return };
        let Some(Ok(file_ref)) = parse_marker(raw) else { return };
        if !is_deferred_ref(&file_ref) {
            return;
        }
        if is_url_ref(&file_ref) && file_ref.ty.references_file() {
            *value = weft_core::storage::url_file_value(&file_ref.path, &file_ref.ty);
            return;
        }
        match map.get(&file_ref.resolution_key()) {
            Some(resolved) => *value = resolved.clone(),
            None if is_url_ref(&file_ref) => missing.push(format!(
                "@asset({:?}, {}) is a text value from a URL the build did not fetch",
                file_ref.path, file_ref.ty
            )),
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
    for node in &mut project.nodes {
        if let Some(config) = node.config.as_object_mut() {
            for value in config.values_mut() {
                each_value_mut(value, &mut |leaf| resolve_value(leaf, map, &mut missing));
            }
        }
        for value in node.port_literals.values_mut() {
            each_value_mut(value, &mut |leaf| resolve_value(leaf, map, &mut missing));
        }
    }
    if missing.is_empty() {
        Ok(())
    } else {
        Err(missing)
    }
}

/// Collect every deferred `@asset` ref in a project definition whose source
/// is a DISK PATH (URL and stored-key refs never sync; a text-typed one
/// from disk resolved at parse). The pre-build asset sync's input: run over
/// a DEFERRED parse (asset refs still hold their raw `@asset` strings).
/// Deduplicated by path, first declared type wins (the type only picks the
/// marker kind; the bytes are the identity, and the sync checks them
/// against that kind).
pub fn collect_asset_refs(project: &weft_core::project::ProjectDefinition) -> Vec<FileRef> {
    collect_refs(project, |r| !is_url_ref(r) && !is_runtime_key_ref(r))
}

#[cfg(test)]
#[path = "tests/file_ref_tests.rs"]
mod tests;
