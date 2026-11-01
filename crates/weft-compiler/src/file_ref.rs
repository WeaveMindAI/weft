//! `@file("path", Type)` / `@asset("path", Type)` value injection.
//!
//! Both markers make a config field's value come from somewhere else; they
//! differ in the EDIT CONTRACT. `@file` is bidirectional: the referenced
//! file's text content is the value, and editing the field writes back to
//! the file; it requires a type that `supports_bidirectional_edit`. `@asset`
//! is pull-only: nothing ever writes back, and every `@asset` defers to the
//! build (the parse's reader is walled to the project, and an asset may sit
//! anywhere on the machine, at a URL, or under a stored-file key). A
//! file-typed one (Image, Audio, ...) goes through the asset sync; a
//! text-typed one is read or fetched by the build driver ([`read_texts`])
//! and named by its content (`weft_core::content`).
//! `@file`'s type defaults to `String`; `@asset` always names its
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
//! path, a malformed marker, or a failed cast becomes a `SourceError`
//! pointing at the field's source line.

use weft_core::project::{FileMarker, FileRef};
use weft_core::WeftType;

use crate::file_reader::CompileFs;
use crate::weft_compiler::SourceError;

/// Recognize and parse a `@file("path"[, Type])` or `@asset("path"[, Type])`
/// marker from a raw value string. Returns `None` if the string is neither
/// marker (so the caller leaves ordinary values untouched), `Some(Err(..))`
/// if it looks like one but is malformed.
///
/// The path must be a double-quoted string. The second argument is a type
/// expression parsed with the same `WeftType::parse` that port declarations
/// use: optional on `@file` (it defaults to `String`), required on `@asset`
/// (a missing one is an error naming the choices).
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
    let name = marker.directive();
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
        (Some(t), _) => match WeftType::try_parse(t.trim()) {
            Ok(ty) => ty,
            Err(why) => return Some(Err(format!(
                "{name}: invalid type {:?} ({why}); a file kind is Image, Video, Audio or Blob, a \
                 text kind is String, Number, Boolean, JsonDict, a List or a record shape",
                t.trim()
            ))),
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

/// Whether a written field is ONE file marker, recorded as file-backed (a
/// node's field or a group's port alike): a marker at the top
/// (`systemPrompt: @file(...)`), or the fallback of an
/// `@instance_filled(@file(...))`, which the editor puts back inside the
/// marker. A list of markers resolves each one and is recorded as none:
/// editing through a field is a thing you do to one file.
fn is_one_marker(value: &serde_json::Value) -> bool {
    value.as_str().is_some()
        || weft_core::instance::as_instance_filled(value).and_then(|filled| filled.fallback).is_some_and(|f| f.is_string())
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
    format!("{}({}, {})", file_ref.marker.directive(), serde_json::to_string(&file_ref.path).expect("a path serializes"), file_ref.ty.wire_string())
}

/// Does this ref DEFER to the build: every `@asset`, whatever its type or
/// source. Only `@file` reads + casts inline at parse.
pub fn is_deferred_ref(file_ref: &FileRef) -> bool {
    // An asset may sit anywhere on the machine, and the parse's reader is
    // walled to the project. The build driver reads it (a file kind
    // through the asset sync, a text kind straight from its source) and
    // substitutes the value.
    file_ref.marker == FileMarker::Asset
}

/// The type a written constant has, for the type check and the cast: a
/// `@file`/`@asset` marker, and the content a build named one by
/// (`weft_core::content`), is the type it declares (the value it stands
/// for, never the marker), a list is a list of its elements' types,
/// and anything else is what `WeftType::infer` reads off the JSON.
pub fn literal_type(value: &serde_json::Value) -> WeftType {
    if let Some(content) = weft_core::content::ContentRef::from_value(value) {
        return content.ty().clone();
    }
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
    each_leaf_at(value, &mut String::new(), &mut |leaf, _| visit(leaf));
}

/// [`each_value_mut`], handing each leaf its JSON pointer from `value`
/// (`/0/name`; empty for `value` itself).
fn each_leaf_at(value: &mut serde_json::Value, at: &mut String, visit: &mut impl FnMut(&mut serde_json::Value, &str)) {
    let len = at.len();
    match value {
        serde_json::Value::Array(items) => {
            for (index, item) in items.iter_mut().enumerate() {
                at.push('/');
                at.push_str(&index.to_string());
                each_leaf_at(item, at, visit);
                at.truncate(len);
            }
        }
        serde_json::Value::Object(map) => {
            for (key, item) in map.iter_mut() {
                at.push('/');
                at.push_str(&key.replace('~', "~0").replace('/', "~1"));
                each_leaf_at(item, at, visit);
                at.truncate(len);
            }
        }
        leaf => visit(leaf, at),
    }
}

/// Somewhere written values live that may hold `@file` / `@asset`
/// markers: a compiled program's node config and port literals, or the
/// values a run hands in (`weft run --emit/--from/--group/--fire`, and the
/// examples saved from them). The collectors and the resolution step walk
/// either the same way, so a value written in a run and one written in
/// source resolve to the same thing.
pub trait MarkedValues {
    fn marked_values(&self) -> Vec<&serde_json::Value>;
    fn marked_values_mut(&mut self) -> Vec<&mut serde_json::Value>;
}

impl MarkedValues for weft_core::project::ProjectDefinition {
    fn marked_values(&self) -> Vec<&serde_json::Value> {
        self.written_values().collect()
    }

    fn marked_values_mut(&mut self) -> Vec<&mut serde_json::Value> {
        self.written_values_mut().collect()
    }
}

/// A run's handed values: every `--from` and `--emit` port, the
/// `--group` boundary inputs, and the `--fire` payload.
impl MarkedValues for weft_core::run_spec::RunSpec {
    fn marked_values(&self) -> Vec<&serde_json::Value> {
        self.from
            .values()
            .chain(self.emit.values())
            .chain(self.group.iter().map(|(_, ports)| ports))
            .flat_map(|ports| ports.values())
            .chain(self.fire.iter().map(|(_, payload)| payload))
            .collect()
    }

    fn marked_values_mut(&mut self) -> Vec<&mut serde_json::Value> {
        self.from
            .values_mut()
            .chain(self.emit.values_mut())
            .chain(self.group.iter_mut().map(|(_, ports)| ports))
            .flat_map(|ports| ports.values_mut())
            .chain(self.fire.iter_mut().map(|(_, payload)| payload))
            .collect()
    }
}

/// The one walk behind the public collectors: every deferred `@asset`
/// ref in `target`'s values, filtered by `keep`, deduplicated by
/// `FileRef::resolution_key` (path AND declared type: one path declared
/// with two types is two refs, each checked and resolved on its own).
fn collect_refs(target: &impl MarkedValues, keep: impl Fn(&FileRef) -> bool) -> Vec<FileRef> {
    let mut seen = std::collections::BTreeSet::new();
    let mut refs = Vec::new();
    for value in target.marked_values() {
        for file_ref in refs_in_value(value) {
            if is_deferred_ref(&file_ref) && keep(&file_ref) && seen.insert(file_ref.resolution_key()) {
                refs.push(file_ref);
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
pub fn collect_runtime_key_refs(target: &impl MarkedValues) -> Vec<FileRef> {
    collect_refs(target, |r| is_runtime_key_ref(r) && r.ty.references_file())
}

/// Collect every TEXT-typed `@asset`: a URL, a stored-file key, or a disk
/// path (in the project or anywhere on the machine). The parse reads none
/// of them; the build driver reads them through [`read_texts`] and puts
/// each value into the same map. Deduplicated like every collector (see
/// `collect_refs`).
pub fn collect_text_refs(target: &impl MarkedValues) -> Vec<FileRef> {
    collect_refs(target, |r| !r.ty.references_file())
}

/// How a resolution step spells the value of a text-typed `@asset`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextValues {
    /// Named by its content (`weft_core::content`): a program's, which a
    /// worker puts back when it reads the program. The driver stores the
    /// bytes among the tenant's assets.
    Named,
    /// The value itself: a run's handed values, which no worker puts
    /// back.
    Inline,
}

/// Where a build driver reads the bytes of a text-typed `@asset`, by the
/// kind of place its ref names. Only the transport is the driver's; which
/// one a ref takes, the cast and the errors are [`read_texts`]'s.
pub trait TextSource {
    /// The body of a GET of `url`.
    fn url(&self, url: &str) -> impl std::future::Future<Output = Result<Vec<u8>, String>> + Send;
    /// The stored file under the tenant-less key `key`.
    fn stored(&self, key: &str) -> impl std::future::Future<Output = Result<Vec<u8>, String>> + Send;
    /// The file at `path`, spelled from the project root or absolute.
    fn path(&self, path: &str) -> impl std::future::Future<Output = Result<Vec<u8>, String>> + Send;
}

/// One text `@asset` [`read_texts`] read: its ref, the bytes, the content
/// they are and the value they were cast to.
#[derive(Debug)]
pub struct TextRead {
    pub file_ref: FileRef,
    pub bytes: Vec<u8>,
    pub content: weft_core::content::ContentRef,
    pub value: serde_json::Value,
}

impl TextRead {
    /// The value the ref resolves to, spelled as `spelled` says.
    pub fn resolution(&self, spelled: TextValues) -> serde_json::Value {
        match spelled {
            TextValues::Named => self.content.to_value(),
            TextValues::Inline => self.value.clone(),
        }
    }
}

/// Read every text `@asset` `target` names through `source`, once per
/// place (one path cast to two types is read once, so both casts are of
/// the same bytes), and cast it to its declared type (the same cast a
/// project-file `@file` gets at parse, so `Number` and JSON shapes work
/// from a URL exactly as from disk). Every ref that cannot be read or cast
/// is named at once.
pub async fn read_texts(target: &impl MarkedValues, source: &impl TextSource) -> Result<Vec<TextRead>, Vec<String>> {
    let mut read = Vec::new();
    let mut failed = Vec::new();
    let mut fetched: std::collections::BTreeMap<String, Result<Vec<u8>, String>> = std::collections::BTreeMap::new();
    for file_ref in collect_text_refs(target) {
        if !fetched.contains_key(&file_ref.path) {
            let bytes = if is_url_ref(&file_ref) {
                source.url(&file_ref.path).await
            } else if is_runtime_key_ref(&file_ref) {
                source.stored(&file_ref.path).await
            } else {
                source.path(&file_ref.path).await
            };
            fetched.insert(file_ref.path.clone(), bytes);
        }
        let bytes = fetched[&file_ref.path].clone();
        let cast = bytes.and_then(|bytes| {
            let text = std::str::from_utf8(&bytes).map_err(|e| format!("the content is not UTF-8 text: {e}"))?;
            let (content, value) = weft_core::content::ContentRef::of_text(text, &file_ref.ty, &file_ref.path)?;
            Ok((content, value, bytes))
        });
        match cast {
            Ok((content, value, bytes)) => read.push(TextRead { file_ref, bytes, content, value }),
            Err(why) => failed.push(format!("@asset({:?}, {}): {why}", file_ref.path, file_ref.ty)),
        }
    }
    if failed.is_empty() { Ok(read) } else { Err(failed) }
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
    map: &mut weft_core::project::AssetResolutions,
) -> Result<(), Vec<String>> {
    let mut mismatched = Vec::new();
    for r in refs {
        let Some(meta) = listing
            .iter()
            .find(|f| f.key.split_once('/').is_some_and(|(_, scope_key)| scope_key == r.path))
        else {
            continue;
        };
        let file = weft_core::storage::StoredFile::from(meta);
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
    /// The value to substitute into config, and the content it was cast
    /// from, which the build names it by ([`name_contents`]).
    Read(weft_core::content::ContentRef, serde_json::Value),
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
    // The ref reaches here already spelled from the project root (see
    // `anchor_on_root`), so the root is both the wall and the anchor.
    let Some(root) = fs.root else {
        return Err(format!(
            "@file({:?}) cannot be resolved outside a project",
            file_ref.path
        ));
    };
    let resolved = fs
        .reader
        .resolve_and_read(root, root, std::path::Path::new(&file_ref.path))
        .map_err(|e| format!("{} {e}", file_ref.marker.directive()))?;
    weft_core::content::ContentRef::of_text(&resolved.content, &file_ref.ty, &file_ref.path).map(|(content, value)| Resolved::Read(content, value))
}

/// Put a disk ref into its one spelling. A relative path is relative to
/// the PROJECT ROOT wherever it was typed: `@file("assets/prompts/x.md")`
/// means the same file in `src/main.weft`, in `src/billing/charge.weft`
/// and in a file included from either, so every consumer of a ref's path
/// (the editor's file watcher and save, the asset sync, the build's
/// resolution map) resolves against that one anchor and a ref only ever
/// leaves the compiler in one spelling. It is normalized (`.` and a
/// balancing `..` collapse); one that climbs above the root is refused,
/// there being nothing above the root it could mean. A path starting
/// `__catalog/` names a file under the project's `nodes/` and is spelled
/// as that (`nodes/...`), so no consumer has to know the prefix.
///
/// A file outside the project is named where it sits, by an absolute
/// path or one under `~` (expanded to the home directory here, so the
/// wire and the hash carry the real path): allowed for local runs, and
/// the compiler-READ refs are still walled by the reader. A URL or a
/// stored-file key names nothing on disk and is left alone.
fn spell_from_root(mut file_ref: FileRef, home: Option<&std::path::Path>) -> Result<FileRef, String> {
    if is_url_ref(&file_ref) || is_runtime_key_ref(&file_ref) {
        return Ok(file_ref);
    }
    let written = std::path::Path::new(&file_ref.path);
    let spelled = if let Ok(under_home) = written.strip_prefix("~") {
        let home = home.ok_or_else(|| {
            format!(
                "{}({:?}) names a file under `~`, and no home directory is known here",
                file_ref.marker.directive(),
                file_ref.path
            )
        })?;
        home.join(under_home)
    } else if written.is_absolute() {
        written.to_path_buf()
    } else if let Some(in_nodes) = crate::file_reader::catalog_path(written) {
        // `__catalog/x` is `nodes/x`, spelled so here, before the
        // normalization below could fold a `..` out of the prefix.
        in_nodes.map_err(|why| format!("{}({:?}) {why}", file_ref.marker.directive(), file_ref.path))?
    } else {
        crate::file_reader::normalize_lexical(written).ok_or_else(|| {
            format!(
                "{}({:?}) escapes the project root: a path is relative to the project, wherever it is written",
                file_ref.marker.directive(),
                file_ref.path
            )
        })?
    };
    // A path is text on the wire, and a definition hash is computed over
    // it, so a lossy conversion would make the same project hash
    // differently on two machines. Refuse rather than replace bytes.
    file_ref.path = spelled
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
/// have been flattened. Every ref is first put into its one spelling (see
/// [`spell_from_root`]), so everything resolves against the project root
/// and the ref's path means the same thing to every consumer.
///
/// `reads`, when given, gets every `@file` read into a node's or a
/// group's field, for the build to name by its content once every check
/// has read the value ([`name_contents`]).
pub(crate) fn resolve_project_file_refs(
    project: &mut weft_core::project::ProjectDefinition,
    root_fs: &CompileFs,
    errors: &mut Vec<SourceError>,
    mut reads: Option<&mut Vec<ContentRead>>,
) {
    let home = std::env::var_os("HOME").map(std::path::PathBuf::from);
    for node in &mut project.nodes {
      let part = node.part().to_string();
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
        // Every marker resolves, at the top of a field or inside a list;
        // only a field that is one marker is recorded ([`is_one_marker`]).
        if let Some(why) = written_content(value) {
            errors.push(SourceError::at(span, why).in_file(source_file).about(&part));
            continue;
        }
        let single = is_one_marker(value);
        each_leaf_at(value, &mut String::new(), &mut |leaf, at| match resolve_leaf(leaf, root_fs, home.as_deref(), single) {
            Ok(Some(resolved)) => {
                if let (Some(reads), Some(content)) = (reads.as_deref_mut(), resolved.content) {
                    reads.push(ContentRead {
                        owner: ReadOwner::Node(node.id.clone()),
                        field: key.clone(),
                        at: at.to_string(),
                        content,
                        value: leaf.clone(),
                    });
                }
                if single {
                    node.file_refs.insert(key.clone(), resolved.file_ref);
                }
            }
            Ok(None) => {}
            // A file that is not there breaks the node that reads it, and
            // only that node.
            Err(msg) => errors.push(SourceError::at(span, msg).in_file(source_file).about(&part)),
        });
      }
    }
    // A literal on a group's own port (`g.x = @file(...)`, or
    // `alias.x = ...` on an include site) resolves the same way: the
    // group sends it to the members that read the port. A loop holds
    // none: its sit on its `LoopIn`, resolved with the nodes above.
    for group in &mut project.groups {
        let spans = &group.port_literal_spans;
        for (key, value) in group.port_literals.iter_mut() {
            let field = spans.get(key);
            let span = field.map(|s| s.span).unwrap_or(group.header_span.or(group.span).unwrap_or_default());
            let source_file = field.map(|s| s.source_file.as_deref()).unwrap_or(group.source_file.as_deref());
            if let Some(why) = written_content(value) {
                errors.push(SourceError::at(span, why).in_file(source_file).about(&group.id));
                continue;
            }
            let single = is_one_marker(value);
            each_leaf_at(value, &mut String::new(), &mut |leaf, at| match resolve_leaf(leaf, root_fs, home.as_deref(), single) {
                Ok(Some(resolved)) => {
                    if let (Some(reads), Some(content)) = (reads.as_deref_mut(), resolved.content) {
                        reads.push(ContentRead {
                            owner: ReadOwner::Group(group.id.clone()),
                            field: key.clone(),
                            at: at.to_string(),
                            content,
                            value: leaf.clone(),
                        });
                    }
                    if single {
                        group.file_refs.insert(key.clone(), resolved.file_ref);
                    }
                }
                Ok(None) => {}
                Err(msg) => errors.push(SourceError::at(span, msg).in_file(source_file).about(&group.id)),
            });
        }
    }
    // A metadata port's value (`self.tool = @file("tool.json", LlmTool)`)
    // resolves in place too: what a metadata wire writes into an input is
    // the content, as if typed there. It stays the content in the built
    // program too (it is never named by its content): the compiler reads
    // it, a key path into it (`search.tool.name`) or a spread of it
    // (`[...screen.tools]`) included. An include's opaque node carries its
    // file's, resolved the same way. A call site carries a copy of its
    // body's: the body's is read once, then copied, so a file is read (and
    // a missing one reported) once whatever the number of sites.
    let containers = project.groups.iter_mut()
        .filter(|group| !matches!(group.kind, weft_core::project::GroupKind::Call { .. }))
        .map(|group| (&mut group.metadata_ports, group.header_span.or(group.span), group.source_file.as_deref(), group.id.as_str()))
        .chain(project.nodes.iter_mut()
            .map(|node| (&mut node.metadata_ports, node.header_span.or(node.span), node.source_file.as_deref(), node.id.as_str())));
    for (ports, owner_span, source_file, owner) in containers {
        for port in ports.iter_mut() {
            let span = port.span.or(owner_span).unwrap_or_default();
            let Some(value) = port.value.as_mut() else { continue };
            if let Some(why) = written_content(value) {
                errors.push(SourceError::at(span, why).in_file(source_file).about(owner));
                continue;
            }
            let single = is_one_marker(value);
            each_value_mut(value, &mut |leaf| {
                if let Err(msg) = resolve_leaf(leaf, root_fs, home.as_deref(), single) {
                    errors.push(SourceError::at(span, msg).in_file(source_file).about(owner));
                }
            });
        }
    }
    let bodies: std::collections::HashMap<String, Vec<weft_core::project::MetadataPort>> = project.groups.iter()
        .filter(|group| matches!(group.kind, weft_core::project::GroupKind::Body))
        .map(|group| (group.id.clone(), group.metadata_ports.clone()))
        .collect();
    for group in project.groups.iter_mut() {
        let weft_core::project::GroupKind::Call { body, .. } = &group.kind else { continue };
        let Some(read) = bodies.get(body) else { continue };
        for port in group.metadata_ports.iter_mut() {
            if let Some(from_body) = read.iter().find(|m| m.name == port.name) {
                port.value = from_body.value.clone();
            }
        }
    }
}

/// Why a written value cannot hold what it does: a marker naming stored
/// content (`weft_core::content`) is the build's, made from a `@file` or
/// an `@asset` it read, and one written by hand would name content
/// nothing of this program stored.
fn written_content(value: &serde_json::Value) -> Option<String> {
    weft_core::content::ContentRef::claimed_in(value).then(|| {
        format!(
            "this value holds `{}`, which only a build writes, for the content of a file it read; \
             name the file instead: @file(\"path\") or @asset(\"path\", Type)",
            weft_core::content::CONTENT_MARKER_KEY
        )
    })
}

/// One `@file` a build's compile read into a field: whose field, where
/// in it (a JSON pointer from the field's value, empty for the field
/// itself), the content, and the value it was cast to.
pub(crate) struct ContentRead {
    owner: ReadOwner,
    field: String,
    at: String,
    content: weft_core::content::ContentRef,
    value: serde_json::Value,
}

/// Whose field a [`ContentRead`] is in, by id.
pub(crate) enum ReadOwner {
    Node(String),
    Group(String),
}

/// Name every `@file` the build's compile read by its content: where it
/// was read, the value cast from the file becomes the marker naming that
/// content (`weft_core::content`), so the program that is hashed, cut,
/// stored and sent to workers carries a hash-sized value where the text
/// was. Run once every check has read the values, so the checks read the
/// content itself. Hands back the value of each content named, which a
/// later check of the program puts back ([`crate::unsettled`]).
///
/// A value inside an `@instance_filled` stays the content: the install
/// reads an instance's fallback (to show it, to check what an instance
/// sends against it), and no worker is there to put it back. A value a
/// later pass rewrote (a cast to the port's type) is the pass's value, no
/// longer the file's text, and stays as the pass left it.
pub(crate) fn name_contents(
    project: &mut weft_core::project::ProjectDefinition,
    reads: Vec<ContentRead>,
) -> weft_core::content::ContentValues {
    /// The value at `at` in `field`, unless the way there crosses an
    /// `@instance_filled`.
    fn read_at<'a>(field: &'a mut serde_json::Value, at: &str) -> Option<&'a mut serde_json::Value> {
        let mut value = field;
        for token in at.split('/').skip(1) {
            if weft_core::instance::as_instance_filled(value).is_some() {
                return None;
            }
            let token = token.replace("~1", "/").replace("~0", "~");
            value = match value {
                serde_json::Value::Object(fields) => fields.get_mut(&token)?,
                serde_json::Value::Array(items) => items.get_mut(token.parse::<usize>().ok()?)?,
                _ => return None,
            };
        }
        Some(value)
    }
    let mut named = weft_core::content::ContentValues::new();
    for read in reads {
        let field = match &read.owner {
            ReadOwner::Node(id) => project.nodes.iter_mut().find(|node| &node.id == id).and_then(|node| node.written_value_mut(&read.field)),
            ReadOwner::Group(id) => {
                project.groups.iter_mut().find(|group| &group.id == id).and_then(|group| group.port_literals.get_mut(&read.field))
            }
        };
        let Some(value) = field.and_then(|field| read_at(field, &read.at)).filter(|value| **value == read.value) else { continue };
        *value = read.content.to_value();
        named.insert(read.content.id(), std::sync::Arc::new(read.value));
    }
    named
}

/// What [`resolve_leaf`] found in a value that was a marker.
struct ResolvedLeaf {
    file_ref: FileRef,
    /// For a `@file`, the content its value was cast from.
    content: Option<weft_core::content::ContentRef>,
}

/// Resolve the marker one written value holds, in place, the way the
/// parse does: a `@file` is read and cast to its type, a deferred
/// `@asset` stays a marker respelled under the root (so the asset sync
/// and the resolution map find the same file). Hands back the ref when
/// the value was one, `None` for an ordinary value.
///
/// `whole` says the value is a whole field rather than one string inside
/// a list or an object: there, a string that merely starts with the
/// directive word and never OPENS an argument list ("@file the report
/// please" inside a prompt array) is prose, the abutting paren being what
/// states intent. A string that did open one is always an attempted
/// marker, however malformed, and stays loud; so does a whole field.
fn resolve_leaf(
    leaf: &mut serde_json::Value,
    fs: &CompileFs,
    home: Option<&std::path::Path>,
    whole: bool,
) -> Result<Option<ResolvedLeaf>, String> {
    let Some(raw) = leaf.as_str() else { return Ok(None) };
    let file_ref = match parse_marker(raw) {
        None => return Ok(None),
        Some(Ok(file_ref)) => file_ref,
        Some(Err(_)) if !whole && !crate::cst::marker::has_abutting_args(raw.trim()) => return Ok(None),
        Some(Err(msg)) => return Err(msg),
    };
    let file_ref = spell_from_root(file_ref, home)?;
    let content = match resolve(&file_ref, fs)? {
        Resolved::Read(content, resolved) => {
            *leaf = resolved;
            Some(content)
        }
        Resolved::Deferred => {
            *leaf = serde_json::Value::String(marker_text(&file_ref));
            None
        }
    };
    Ok(Some(ResolvedLeaf { file_ref, content }))
}

/// Resolve every `@file` in values written outside a source file (a
/// run's handed values), exactly as the parse resolves one written in a
/// `.weft`: read from the project, cast to its declared type. Each
/// `@asset` is left a marker, respelled under the root, for the build
/// driver's asset step ([`collect_asset_refs`] and friends, then
/// [`apply_asset_resolutions`]). Every failure is named at once.
pub fn resolve_file_markers(target: &mut impl MarkedValues, fs: &CompileFs) -> Result<(), Vec<String>> {
    let home = std::env::var_os("HOME").map(std::path::PathBuf::from);
    let mut errors = Vec::new();
    for value in target.marked_values_mut() {
        let whole = value.is_string();
        each_value_mut(value, &mut |leaf| {
            if let Err(msg) = resolve_leaf(leaf, fs, home.as_deref(), whole) {
                errors.push(msg);
            }
        });
    }
    if errors.is_empty() { Ok(()) } else { Err(errors) }
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
    target: &mut impl MarkedValues,
    map: &weft_core::project::AssetResolutions,
) -> Result<(), Vec<String>> {
    let mut missing = Vec::new();
    // An `@asset` may sit on a config field (`node.config`) or on a port
    // (`node.port_literals`, where the enrich normalization homes
    // port-driven body values); resolve both.
    fn resolve_value(
        value: &mut serde_json::Value,
        map: &weft_core::project::AssetResolutions,
        missing: &mut Vec<String>,
    ) {
        let Some(raw) = value.as_str() else { return };
        let Some(Ok(file_ref)) = parse_marker(raw) else { return };
        if !is_deferred_ref(&file_ref) {
            return;
        }
        if is_url_ref(&file_ref) && file_ref.ty.references_file() {
            match file_ref.ty.concrete_file_kind() {
                Some(kind) => *value = weft_core::storage::url_file_value(&file_ref.path, kind),
                None => missing.push(format!("@asset({:?}, {}) must name one kind of file: Image, Video, Audio, or Blob", file_ref.path, file_ref.ty)),
            }
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
                "@asset({:?}, {}) was not read by the build (missing file, or the build ran \
                 without the asset step)",
                file_ref.path, file_ref.ty
            )),
        }
    }
    for value in target.marked_values_mut() {
        each_value_mut(value, &mut |leaf| resolve_value(leaf, map, &mut missing));
    }
    if missing.is_empty() {
        Ok(())
    } else {
        Err(missing)
    }
}

/// The file-kind `@asset`s whose source is a DISK PATH: what the pre-build
/// asset sync hashes and uploads (URL and stored-key refs never sync, and a
/// text kind is read and inlined instead, see `collect_text_refs`). Run over
/// a DEFERRED parse, where asset refs still hold their raw `@asset` strings.
/// Deduplicated like every collector (see `collect_refs`).
pub fn collect_asset_refs(target: &impl MarkedValues) -> Vec<FileRef> {
    collect_refs(target, |r| !is_url_ref(r) && !is_runtime_key_ref(r) && r.ty.references_file())
}

#[cfg(test)]
#[path = "tests/file_ref_tests.rs"]
mod tests;
