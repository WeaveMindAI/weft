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

pub async fn parse(file: Option<std::path::PathBuf>) -> Result<()> {
    let source = read_stdin()?;
    // Parse is lenient: it must keep rendering the graph mid-edit even outside a
    // project. `Project::find` is the THREE-way discovery: a project (id +
    // catalog from its root), NO project (nil id, empty catalog, every type an
    // unknown placeholder), or a MALFORMED `weft.toml` (a loud `Err`, NOT a
    // silent degrade to no-project, which would hide the broken manifest). It
    // does NOT take `Ctx`: `ctx.project()` is discover's abort-on-anything, which
    // would collapse "no project" and "broken manifest" into one silent lenient
    // render (the bug). The server's `parse` and one-shot `validate` agree.
    // Discover ONCE; derive id, catalog, AND the @file base from the one result.
    let project = Project::find(&std::env::current_dir()?)?;
    let (id, catalog) = match &project {
        Some(p) => {
            let catalog = build_project_catalog(&p.root)
                .map_err(|e| anyhow::anyhow!("catalog: {e}"))?;
            (p.id(), catalog)
        }
        None => (uuid::Uuid::nil(), FsCatalog::empty()),
    };
    let base = base_dir_for(file.as_deref(), project.as_ref().map(|p| p.root.as_path()));
    let resp = do_parse(&source, id, base.as_deref(), &catalog, file.as_deref());
    println!("{}", serde_json::to_string(&resp).context("serialize parse response")?);
    Ok(())
}

/// The pure parse pipeline: lenient compile + per-type catalog collection.
/// Shared by the one-shot `weft parse` command and the parse-server, so both
/// produce byte-identical responses. `id`, `catalog` and `base` are resolved
/// by the caller (from `Ctx` for the one-shot command, per-request for the
/// server). `file` drives the anonymous top-level group's id derivation only.
fn do_parse(
    source: &str,
    id: uuid::Uuid,
    base: Option<&std::path::Path>,
    catalog: &FsCatalog,
    file: Option<&std::path::Path>,
) -> ParseResponse {
    // An anonymous top-level group takes its id from the filename (PascalCase),
    // so the file's root carries the same id at parse, edit, and render. The
    // compiler derives it; a normal project (named main group) ignores it.
    let source_id = weft_compiler::source_name::derive_id(file);
    // The CLI always reads `@file`/`@include` content from disk; a `base` is the
    // project root to resolve against, its absence means "outside a project."
    let fs = match base {
        Some(b) => weft_compiler::CompileFs::disk(b),
        None => weft_compiler::CompileFs::none(),
    };
    let (project, diagnostics) = weft_compiler::parse_only(source, id, fs, catalog, Some(&source_id));
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
    // Three-way: no project (lenient), a project, or a BROKEN manifest. A broken
    // `weft.toml` must surface loudly on every kind, not silently degrade to the
    // no-project path (which would render every node as an unknown placeholder).
    let project = match req.file.as_deref().and_then(|f| f.parent()) {
        Some(d) => match Project::find(d) {
            Ok(p) => p,
            Err(e) => return ServerResponse { id, payload: None, error: Some(format!("weft.toml: {e}")) },
        },
        None => None,
    };
    // `@file`/`@include` base: the file's own dir, else the project root (so a
    // bare `--file foo.weft` inside a project resolves relative paths against the
    // root). SAME shared rule as the one-shot commands.
    let base = base_dir_for(req.file.as_deref(), project.as_ref().map(|p| p.root.as_path()));

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
            let source_id = weft_compiler::source_name::derive_id(req.file.as_deref());
            let (new_source, inverse) = match weft_compiler::edit::apply_edits(&req.source, base.as_deref(), &source_id, &req.ops) {
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

/// The base directory for `@file`/`@include` resolution: the source file's own
/// directory when known, else the project root. The single rule, shared by the
/// one-shot commands (`parse`, `validate`) and the parse-server, all of which
/// discover the project ONCE and pass its root here, so they can't drift on
/// where a bare-filename's relative paths resolve.
fn base_dir_for(file: Option<&std::path::Path>, project_root: Option<&std::path::Path>) -> Option<std::path::PathBuf> {
    file_dir(file).or_else(|| project_root.map(|r| r.to_path_buf()))
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
    // Same single-discovery shape as `parse`: derive the `@file`/`@include` base
    // from the project we already resolved, not a second `ctx.project()` call.
    let base = base_dir_for(file.as_deref(), Some(project.root.as_path()));
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
    // Same anonymous-group id derivation as parse, so a standalone file's
    // diagnostics reference the same filename-derived id the editor renders.
    let source_id = weft_compiler::source_name::derive_id(file);
    let fs = match base {
        Some(b) => weft_compiler::CompileFs::disk(b),
        None => weft_compiler::CompileFs::none(),
    };
    let (_, diagnostics) = weft_compiler::compile_strict(
        source,
        id,
        fs,
        catalog,
        ValidationMode::Runtime,
        Some(&source_id),
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
    use super::file_dir;
    use std::path::Path;

    #[test]
    fn file_dir_treats_bare_filename_as_absent() {
        // A bare filename has an empty parent; file_dir must return None (so
        // base_dir_for falls back to the project root) rather than Some("")
        // which would resolve @file against the CWD.
        assert_eq!(file_dir(None), None);
        assert_eq!(file_dir(Some(Path::new("foo.weft"))), None);
        assert_eq!(file_dir(Some(Path::new("a/b/foo.weft"))), Some(std::path::PathBuf::from("a/b")));
    }
}
