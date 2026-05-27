//! `weft parse` / `weft validate`: editor feedback surface.
//!
//! Both read weft source from stdin and run the compiler against the
//! project's `nodes/` catalog (the same catalog the build uses), then
//! print JSON to stdout. This is where the editor's live graph and
//! Problems-panel feedback comes from. It runs locally, on the CLI,
//! because the catalog lives in the project's `nodes/` folder: the
//! dispatcher (a remote pod) has no access to it.
//!
//! `parse` is lenient (unknown node types become placeholders so the
//! graph keeps rendering mid-edit); `validate` is the full strict
//! compile + enrich + validate pipeline.

use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, Read, Write};
use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use weft_catalog::FsCatalog;
use weft_compiler::build::build_project_catalog;
use weft_compiler::project::Project;
use weft_compiler::validate::ValidationMode;
use weft_compiler::Diagnostic;
use weft_core::{MetadataCatalog, ProjectDefinition};

use super::node_catalog::NodeCatalogEntry;
use super::Ctx;

#[derive(Debug, Serialize)]
struct ParseResponse {
    project: ProjectDefinition,
    /// Per-node-type catalog entries, keyed by `NodeDefinition.nodeType`,
    /// scoped to the node types the project references.
    catalog: BTreeMap<String, NodeCatalogEntry>,
    diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Serialize)]
struct ValidateResponse {
    diagnostics: Vec<Diagnostic>,
}

fn read_stdin() -> Result<String> {
    let mut source = String::new();
    std::io::stdin()
        .read_to_string(&mut source)
        .context("read source from stdin")?;
    Ok(source)
}

pub async fn parse(ctx: Ctx, file: Option<std::path::PathBuf>) -> Result<()> {
    let source = read_stdin()?;
    // Parse is lenient: it must keep rendering the graph mid-edit, even
    // outside a project. With a project, resolve id + catalog from its
    // real root (one discovery). Without one, the id is nil and the
    // catalog is genuinely empty (every type is an unknown placeholder)
    // rather than a misdirected cwd-relative guess.
    let (id, catalog) = match ctx.project() {
        Ok(project) => {
            let catalog = build_project_catalog(&project.root)
                .map_err(|e| anyhow::anyhow!("catalog: {e}"))?;
            (project.id(), catalog)
        }
        Err(_) => (uuid::Uuid::nil(), FsCatalog::empty()),
    };
    let base = resolution_base(&ctx, file.as_deref());
    let resp = do_parse(&source, id, base.as_deref(), &catalog, file.as_deref());
    println!("{}", serde_json::to_string(&resp).context("serialize parse response")?);
    Ok(())
}

/// The pure parse pipeline: lenient compile + per-type catalog collection.
/// Shared by the one-shot `weft parse` command and the parse-server, so both
/// produce byte-identical responses. `id`, `catalog` and `base` are resolved
/// by the caller (from `Ctx` for the one-shot command, per-request for the
/// server). `file` drives the anonymous-component name derivation only.
fn do_parse(
    source: &str,
    id: uuid::Uuid,
    base: Option<&std::path::Path>,
    catalog: &FsCatalog,
    file: Option<&std::path::Path>,
) -> ParseResponse {
    // An anonymous component opened standalone gets a readable id/label derived
    // from its filename; the compiler applies it (and no-ops for a normal
    // project). The CLI owns only the filename -> name derivation.
    let name = component_name_from(file);
    let (project, diagnostics) = weft_compiler::parse_only(
        source,
        id,
        base,
        catalog,
        name.as_ref().map(|(i, l)| (i.as_str(), l.as_str())),
    );
    let catalog_map = collect_catalog(&project, catalog);
    ParseResponse { project, catalog: catalog_map, diagnostics }
}

// ── Parse server ────────────────────────────────────────────────────────────
//
// A long-lived process the VS Code extension spawns once and keeps warm. It
// reads one JSON request per line on stdin and writes one JSON response per
// line on stdout. Holding the per-project node catalog in memory makes each
// request ~parse-cost instead of paying catalog-discovery (a full `nodes/`
// walk) on every keystroke as the one-shot `weft parse` would. This is the
// editor's hot path; the one-shot commands stay for tests and other callers.

/// One editor request. `kind` selects the pipeline; `source` is the buffer
/// text (NOT read from disk, so an unsaved buffer parses); `file` gives the
/// `@file`/`@include` base + the project to resolve. `reload_catalog` drops
/// the warm catalog for this project first (the host sends it when its
/// `nodes/` watcher fired, so the server never needs its own watcher).
#[derive(Debug, Deserialize)]
struct ServerRequest {
    id: u64,
    kind: ServerRequestKind,
    source: String,
    #[serde(default)]
    file: Option<PathBuf>,
    #[serde(default, rename = "reloadCatalog")]
    reload_catalog: bool,
    /// Edit ops (only for `kind: "edit"`). Applied to `source` in order; the
    /// edited source is then parsed so the UI re-renders in one round-trip.
    #[serde(default)]
    ops: Vec<weft_compiler::edit::EditOp>,
    /// A raw text edit to replay (only for `kind: "applyEdit"`, the undo/redo
    /// path). Applied to `source`, then parsed, like `edit`.
    #[serde(default, rename = "textEdit")]
    text_edit: Option<weft_compiler::edit::TextEdit>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
enum ServerRequestKind {
    Parse,
    Validate,
    Edit,
    /// Replay a `TextEdit` (undo/redo): apply it to `source`, parse the result.
    ApplyEdit,
}

/// Response payload for an `edit`/`applyEdit` request: the new source, the parse
/// of it, and the INVERSE text edit (apply it to `source` to restore the prior
/// source). The frontend writes `source`, re-renders from `parse`, and stores
/// `inverse` as the action's undo. One round-trip.
#[derive(Debug, Serialize)]
struct EditResponse {
    source: String,
    parse: ParseResponse,
    inverse: weft_compiler::edit::TextEdit,
}

/// Response envelope. `id` echoes the request so the host can match it. The
/// payload is the same `ParseResponse`/`ValidateResponse` the one-shot
/// commands print; on failure (e.g. validate with no project) `error` carries
/// the reason and `payload` is null. Fail loud: the host surfaces `error`.
#[derive(Debug, Serialize)]
struct ServerResponse {
    id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Run the parse server loop. One JSON request per stdin line, one JSON
/// response per stdout line. Blocks until stdin closes (the host killed us on
/// deactivate). A malformed or failing request answers with an `error`
/// envelope and the loop continues: one bad request must not take the server
/// down and stop all editor feedback.
pub async fn serve(_ctx: Ctx) -> Result<()> {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    // Per-project-root warm catalog. The catalog is immutable during an
    // editing session except when the user edits `nodes/`, which the host
    // signals via `reload_catalog`.
    let mut catalogs: HashMap<PathBuf, FsCatalog> = HashMap::new();

    for line in stdin.lock().lines() {
        let line = line.context("read request line")?;
        if line.trim().is_empty() {
            continue;
        }
        let resp = match serde_json::from_str::<ServerRequest>(&line) {
            Ok(req) => handle_request(req, &mut catalogs),
            // A request we can't even parse has no id to echo; answer id 0 so
            // the host sees the failure rather than hanging on a lost reply.
            Err(e) => ServerResponse { id: 0, payload: None, error: Some(format!("bad request: {e}")) },
        };
        let mut out = stdout.lock();
        serde_json::to_writer(&mut out, &resp).context("serialize server response")?;
        out.write_all(b"\n").context("write response newline")?;
        out.flush().context("flush response")?;
    }
    Ok(())
}

/// Resolve project + warm catalog from a request and run the selected
/// pipeline. Project discovery walks up from the file's directory (the server
/// has no fixed cwd project, unlike the one-shot commands). Parse is lenient
/// and works without a project (nil id, empty catalog); validate requires one.
fn handle_request(req: ServerRequest, catalogs: &mut HashMap<PathBuf, FsCatalog>) -> ServerResponse {
    let id = req.id;
    let base = req.file.as_deref().and_then(|f| file_dir(Some(f)));
    let project = req.file.as_deref().and_then(|f| f.parent()).and_then(|d| Project::discover(d).ok());

    match req.kind {
        ServerRequestKind::Parse => {
            match parse_source(&req.source, &project, base.as_deref(), req.file.as_deref(), catalogs, req.reload_catalog) {
                Ok(parse_resp) => envelope(id, &parse_resp),
                Err(e) => ServerResponse { id, payload: None, error: Some(e) },
            }
        }
        ServerRequestKind::Validate => {
            // Strict: validating outside a project is meaningless. Fail loud.
            let project = match project {
                Some(p) => p,
                None => return ServerResponse { id, payload: None, error: Some("validate requires a project (no weft.toml found from the file's directory)".into()) },
            };
            match warm_catalog(catalogs, &project.root, req.reload_catalog) {
                Ok(cat) => {
                    let v = do_validate(&req.source, project.id(), base.as_deref(), cat, req.file.as_deref());
                    envelope(id, &v)
                }
                Err(e) => ServerResponse { id, payload: None, error: Some(e) },
            }
        }
        ServerRequestKind::Edit => {
            // Apply the edit batch -> new source + the inverse text edit (undo).
            // Parse the result so the UI re-renders in one round-trip. Edit
            // failure is loud (the frontend keeps the pre-edit source).
            let (new_source, inverse) = match weft_compiler::edit::apply_edits(&req.source, base.as_deref(), &req.ops) {
                Ok(r) => r,
                Err(e) => return ServerResponse { id, payload: None, error: Some(format!("edit: {e}")) },
            };
            edit_envelope(id, new_source, inverse, &project, &base, &req, catalogs)
        }
        ServerRequestKind::ApplyEdit => {
            // Undo/redo: replay a raw text edit, parse the result. The returned
            // `inverse` is the edit that undoes THIS replay (so undo<->redo
            // round-trips without recomputing).
            let Some(text_edit) = &req.text_edit else {
                return ServerResponse { id, payload: None, error: Some("applyEdit requires a textEdit".into()) };
            };
            let new_source = match weft_compiler::edit::apply_text_edit(&req.source, text_edit) {
                Ok(s) => s,
                Err(e) => return ServerResponse { id, payload: None, error: Some(format!("applyEdit: {e}")) },
            };
            let inverse = weft_compiler::edit::invert_text_edit(&req.source, &new_source);
            edit_envelope(id, new_source, inverse, &project, &base, &req, catalogs)
        }
    }
}

/// Build the `{source, parse, inverse}` response shared by `edit` and
/// `applyEdit`: parse the new source (lenient, like a normal parse), or surface
/// a catalog error loudly.
fn edit_envelope(
    id: u64,
    new_source: String,
    inverse: weft_compiler::edit::TextEdit,
    project: &Option<Project>,
    base: &Option<PathBuf>,
    req: &ServerRequest,
    catalogs: &mut HashMap<PathBuf, FsCatalog>,
) -> ServerResponse {
    match parse_source(&new_source, project, base.as_deref(), req.file.as_deref(), catalogs, req.reload_catalog) {
        Ok(parse) => envelope(id, &EditResponse { source: new_source, parse, inverse }),
        Err(e) => ServerResponse { id, payload: None, error: Some(e) },
    }
}

/// Lenient parse of `source` with the project's id + warm catalog. No project
/// => nil id + empty catalog (every type is an unknown placeholder), so editing
/// outside a project still renders. The single parse path shared by the `parse`
/// and `edit` request kinds.
fn parse_source(
    source: &str,
    project: &Option<Project>,
    base: Option<&std::path::Path>,
    file: Option<&std::path::Path>,
    catalogs: &mut HashMap<PathBuf, FsCatalog>,
    reload_catalog: bool,
) -> std::result::Result<ParseResponse, String> {
    match project {
        Some(p) => {
            let cat = warm_catalog(catalogs, &p.root, reload_catalog)?;
            Ok(do_parse(source, p.id(), base, cat, file))
        }
        None => Ok(do_parse(source, uuid::Uuid::nil(), base, &FsCatalog::empty(), file)),
    }
}

/// Fetch the warm catalog for a root, building (and caching) it on first use
/// or when `reload` forces a rebuild. Errors are stringified for the envelope.
fn warm_catalog<'a>(
    catalogs: &'a mut HashMap<PathBuf, FsCatalog>,
    root: &std::path::Path,
    reload: bool,
) -> std::result::Result<&'a FsCatalog, String> {
    if reload {
        catalogs.remove(root);
    }
    if !catalogs.contains_key(root) {
        let cat = build_project_catalog(root).map_err(|e| format!("catalog: {e}"))?;
        catalogs.insert(root.to_path_buf(), cat);
    }
    Ok(catalogs.get(root).expect("just inserted"))
}

/// Wrap a serializable payload in a success envelope.
fn envelope<T: Serialize>(id: u64, payload: &T) -> ServerResponse {
    match serde_json::to_value(payload) {
        Ok(v) => ServerResponse { id, payload: Some(v), error: None },
        Err(e) => ServerResponse { id, payload: None, error: Some(format!("serialize payload: {e}")) },
    }
}

/// Derive an anonymous component's (id, label) from its filename:
/// `my-cleaner.weft` -> (`MyCleaner`, "My Cleaner"). `None` only when there's no
/// `--file`. When the stem can't yield a valid bare identifier (empty, all
/// separators, leading digit, non-ASCII), fall back to a generic `Component`
/// id so the internal sentinel never surfaces in the rendered graph; the label
/// still humanizes the stem when it has any displayable content.
fn component_name_from(file: Option<&std::path::Path>) -> Option<(String, String)> {
    let stem = file?.file_stem().and_then(|s| s.to_str()).unwrap_or("");
    let id = pascal_case(stem).unwrap_or_else(|| "Component".to_string());
    let label = humanize(stem);
    let label = if label.is_empty() { id.clone() } else { label };
    Some((id, label))
}

/// `my-cleaner` / `my_cleaner` -> "My Cleaner" (display label).
fn humanize(stem: &str) -> String {
    capitalized_words(stem).join(" ")
}

/// `my-cleaner` -> "MyCleaner" (bare identifier for the group/node ids).
/// `None` when the stem can't yield a valid bare identifier: empty, all
/// separators, or a leading digit (an id must start with a letter/underscore
/// since it prefixes node ids and SvelteFlow handles).
fn pascal_case(stem: &str) -> Option<String> {
    let id = capitalized_words(stem).join("");
    // The result must be a valid weft bare identifier (it becomes the group id
    // and prefixes node ids / SvelteFlow handles). `capitalized_words` only
    // splits on `-`/`_`/space, so any other char a filename can carry (`.`,
    // non-ASCII, ...) survives into `id`; validate against the language's
    // actual rule (`[A-Za-z_][A-Za-z0-9_]*`) and return None otherwise (the
    // caller falls back to a generic id).
    let mut chars = id.chars();
    let valid_first = matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_');
    if valid_first && chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
        Some(id)
    } else {
        None
    }
}

fn capitalized_words(stem: &str) -> Vec<String> {
    stem.split(|c| c == '-' || c == '_' || c == ' ')
        .filter(|w| !w.is_empty())
        .map(|w| {
            let mut chars = w.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

/// The base directory for `@file`/`@include` resolution: the source file's
/// own directory when known (`--file`), so relative paths resolve against the
/// file's location, not the project root. Falls back to the project root for
/// a detached buffer (no `--file`).
fn resolution_base(ctx: &Ctx, file: Option<&std::path::Path>) -> Option<std::path::PathBuf> {
    file_dir(file).or_else(|| ctx.project().ok().map(|p| p.root.clone()))
}

/// The source file's own directory, or `None` for no `--file` OR a bare
/// filename. `Path::parent()` of `"foo.weft"` is `Some("")` (the empty path),
/// not `None`; an empty base would silently resolve `@file`/`@include` against
/// the process CWD, so an empty parent is treated as absent (caller falls back
/// to the project root). Pure, so the bare-filename branch is unit-tested.
fn file_dir(file: Option<&std::path::Path>) -> Option<std::path::PathBuf> {
    file.and_then(|f| f.parent())
        .filter(|p| !p.as_os_str().is_empty())
        .map(|p| p.to_path_buf())
}

pub async fn validate(ctx: Ctx, file: Option<std::path::PathBuf>) -> Result<()> {
    let source = read_stdin()?;
    // Validate is the strict pipeline: it must run against the real
    // catalog. Validating outside a project is meaningless (every node
    // type would be unknown), so a missing project is a hard error, not
    // a silent empty-catalog pass.
    let project = ctx.project()?;
    let catalog = build_project_catalog(&project.root)
        .map_err(|e| anyhow::anyhow!("catalog: {e}"))?;
    let base = resolution_base(&ctx, file.as_deref());
    let resp = do_validate(&source, project.id(), base.as_deref(), &catalog, file.as_deref());
    println!("{}", serde_json::to_string(&resp).context("serialize validate response")?);
    Ok(())
}

/// The pure strict-validate pipeline. Shared by the one-shot `weft validate`
/// command and the parse-server. The compile -> enrich -> validate pipeline
/// (and its error-to-diagnostic mapping) lives in `compile_strict`; this only
/// picks the mode. `Runtime` mode = the complete check the Problems panel
/// wants (graph shape plus runtime rules like missing credentials).
fn do_validate(
    source: &str,
    id: uuid::Uuid,
    base: Option<&std::path::Path>,
    catalog: &FsCatalog,
    file: Option<&std::path::Path>,
) -> ValidateResponse {
    // Same anonymous-component naming as parse, so a standalone component's
    // diagnostics never embed the internal sentinel id.
    let name = component_name_from(file);
    let (_, diagnostics) = weft_compiler::compile_strict(
        source,
        id,
        base,
        catalog,
        ValidationMode::Runtime,
        name.as_ref().map(|(i, l)| (i.as_str(), l.as_str())),
    );
    ValidateResponse { diagnostics }
}

/// For each node type in the project, pull its catalog entry. Hidden
/// types (Passthrough) are omitted; unknown types are skipped (parse
/// is lenient, the editor renders them as placeholders).
fn collect_catalog(
    project: &ProjectDefinition,
    catalog: &FsCatalog,
) -> BTreeMap<String, NodeCatalogEntry> {
    let mut out = BTreeMap::new();
    for node in &project.nodes {
        if out.contains_key(&node.node_type) {
            continue;
        }
        if let Some(meta) = catalog.lookup(&node.node_type) {
            if meta.features.hidden {
                continue;
            }
            let specs = catalog.form_field_specs(&node.node_type).to_vec();
            out.insert(
                node.node_type.clone(),
                NodeCatalogEntry { metadata: meta.clone(), form_field_specs: specs },
            );
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{component_name_from, file_dir, humanize, pascal_case};
    use std::path::Path;

    #[test]
    fn component_name_falls_back_to_generic_id_never_sentinel() {
        // A filename that can't yield a valid id must still produce a usable
        // (id, label), never leak the internal `__include_root__` sentinel.
        let (id, _label) = component_name_from(Some(Path::new("/x/123-bad.weft"))).unwrap();
        assert_eq!(id, "Component");
        let (id, label) = component_name_from(Some(Path::new("/x/my-cleaner.weft"))).unwrap();
        assert_eq!(id, "MyCleaner");
        assert_eq!(label, "My Cleaner");
        assert!(component_name_from(None).is_none());
    }

    #[test]
    fn pascal_case_normal() {
        assert_eq!(pascal_case("cleaner").as_deref(), Some("Cleaner"));
        assert_eq!(pascal_case("my-cleaner").as_deref(), Some("MyCleaner"));
        assert_eq!(pascal_case("my_cleaner").as_deref(), Some("MyCleaner"));
        // Separators (incl. a leading one) split words; "_internal" -> one
        // word "internal" -> "Internal".
        assert_eq!(pascal_case("_internal").as_deref(), Some("Internal"));
    }

    #[test]
    fn pascal_case_rejects_invalid_ids() {
        // Empty / all-separator / leading-digit stems can't yield a valid bare
        // identifier; None so the caller falls back to a generic id rather than
        // renaming to an empty/invalid id (broken graph).
        assert_eq!(pascal_case(""), None);
        assert_eq!(pascal_case("---"), None);
        assert_eq!(pascal_case("___"), None);
        assert_eq!(pascal_case("123-cleaner"), None);
        // A `.` in the stem (e.g. file_stem of "my.helper.weft") is the scope
        // separator; a non-ASCII letter isn't a valid weft id char. Both must
        // be rejected, not emitted as ids the compiler can't use.
        assert_eq!(pascal_case("my.helper"), None);
        assert_eq!(pascal_case("résumé"), None);
    }

    #[test]
    fn humanize_words() {
        assert_eq!(humanize("my-cleaner"), "My Cleaner");
        assert_eq!(humanize("my_cleaner"), "My Cleaner");
        assert_eq!(humanize("cleaner"), "Cleaner");
    }

    #[test]
    fn file_dir_treats_bare_filename_as_absent() {
        // A bare filename has an empty parent; file_dir must return None (so
        // resolution_base falls back to the project root) rather than Some("")
        // which would resolve @file against the CWD.
        assert_eq!(file_dir(None), None);
        assert_eq!(file_dir(Some(Path::new("foo.weft"))), None);
        assert_eq!(file_dir(Some(Path::new("a/b/foo.weft"))), Some(std::path::PathBuf::from("a/b")));
    }
}
