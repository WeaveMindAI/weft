//! `weft parse` / `weft validate`: editor feedback surface.
//!
//! Both read weft source from stdin and run the compiler against the
//! project's node catalog (the same catalog the build uses), then print
//! JSON to stdout. This is where the editor's live graph and
//! Problems-panel feedback comes from. It runs locally, on the CLI,
//! because the catalog lives in the project's folders (`nodes/`, and the
//! nodes beside the code under `src/`): the dispatcher (a remote process)
//! has no access to them.
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
use weft_core::node::NodeMetadata;
use weft_core::{MetadataCatalog, ProjectDefinition};

use super::Ctx;

#[derive(Debug, Serialize)]
struct ParseResponse {
    project: ProjectDefinition,
    /// Per-node-type catalog entries (the metadata IS the entry; form-field
    /// specs included), keyed by `NodeDefinition.nodeType`, scoped to the
    /// node types the project references.
    catalog: BTreeMap<String, NodeMetadata>,
    diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Serialize)]
struct ValidateResponse {
    diagnostics: Vec<Diagnostic>,
    /// The catalog's warnings, the same lines `weft describe-nodes` warns
    /// with: one per folder left out and why (a broken `metadata.json`, a
    /// node with no code yet, an empty package), and one per name a node
    /// spells off the weft way (that node still loads). Never a failure
    /// on their own: a program naming a left-out node gets its own
    /// diagnostic.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    warnings: Vec<weft_catalog::CatalogWarning>,
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
    let anchor = anchor_for(file.as_deref(), project.as_ref().map(|p| p.root.as_path()));
    let resp = do_parse(&source, id, &anchor, &catalog, file.as_deref()).map_err(anyhow::Error::msg)?;
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
    anchor: &Anchor,
    catalog: &FsCatalog,
    file: Option<&std::path::Path>,
) -> std::result::Result<ParseResponse, String> {
    // An anonymous top-level group takes the file's body id (its path from
    // the project root), so the file's root carries the same id at parse,
    // edit, render and in the journal. The entry file gets none: it may
    // not hold an anonymous group, and the parse says so.
    let source_id = weft_compiler::source_name::file_id(anchor.root.as_deref(), file)?;
    let (project, diagnostics) = weft_compiler::parse_only(source, id, anchor.fs(), catalog, source_id.as_deref());
    let catalog_map = collect_catalog(&project, catalog);
    Ok(ParseResponse { project, catalog: catalog_map, diagnostics })
}

// ── Parse server ────────────────────────────────────────────────────────────
//
// A long-lived process the VS Code extension spawns once and keeps warm. It
// reads one JSON request per line on stdin and writes one JSON response per
// line on stdout. Holding the per-project node catalog in memory makes each
// request ~parse-cost instead of paying catalog-discovery (a full walk of
// both node roots) on every keystroke as the one-shot `weft parse` would. This is the
// editor's hot path; the one-shot commands stay for tests and other callers.

/// One editor request. `kind` selects the pipeline; `source` is the buffer
/// text (NOT read from disk, so an unsaved buffer parses); `file` gives the
/// `@file`/`@include` base + the project to resolve. `reload_catalog` drops
/// the warm catalog for this project first (the host sends it when its
/// node watcher fired, over `nodes/` and `src/` alike, so the server never
/// needs its own watcher).
// SYNC: ServerRequest <-> extension-vscode/src/parseServer.ts ParseServerRequest
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
    /// A raw text edit to replay (only for `kind: "apply_edit"`, the undo/redo
    /// path). Applied to `source`, then parsed, like `edit`.
    #[serde(default, rename = "textEdit")]
    text_edit: Option<weft_compiler::edit::TextEdit>,
    /// Which validation tier to run (only for `kind: "validate"`).
    /// `structural` = graph shape only (the Problems panel), `runtime` =
    /// additionally the rules flagged `level: runtime` such as missing
    /// credentials (the editor's pre-flight gate before Run). Required for
    /// validate requests: the server refuses to guess which tier a caller
    /// meant.
    #[serde(default)]
    mode: Option<ValidationMode>,
    /// The place the editor shows `file` at: the include sites walked
    /// into from the entry file, outermost first; empty for the entry
    /// file itself or a file opened on its own (no place). Only for the
    /// kinds that parse (`parse`, `edit`, `apply_edit`): a file included
    /// at several sites can be per instance at one and shared at another,
    /// so its nodes are told the answer at this place
    /// ([`per_instance_at_place`]).
    #[serde(default, rename = "callPath")]
    call_path: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ServerRequestKind {
    Parse,
    Validate,
    Edit,
    /// Replay a `TextEdit` (undo/redo): apply it to `source`, parse the result.
    ApplyEdit,
}

/// Response payload for an `edit`/`apply_edit` request: the new source, the parse
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
// SYNC: ServerResponse <-> extension-vscode/src/parseServer.ts ServerResponseEnvelope
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
pub fn serve() -> Result<()> {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    // Per-project-root warm catalog. The catalog is immutable during an
    // editing session except when the user edits a node, which the host
    // signals via `reload_catalog`.
    let mut catalogs: HashMap<PathBuf, FsCatalog> = HashMap::new();

    for line in stdin.lock().lines() {
        let line = line.context("read request line")?;
        if line.trim().is_empty() {
            continue;
        }
        let resp = match serde_json::from_str::<ServerRequest>(&line) {
            Ok(req) => handle_request(req, &mut catalogs),
            // A request we can't parse into the typed shape may still be
            // valid JSON carrying an `id` (a stale editor talking to a
            // newer server): recover it so the host matches the reply to
            // its pending request and shows the real reason instead of
            // idling into its timeout. Only truly unparseable input
            // falls back to id 0.
            Err(e) => {
                let id = serde_json::from_str::<serde_json::Value>(&line)
                    .ok()
                    .and_then(|v| v.get("id").and_then(|i| i.as_u64()))
                    .unwrap_or(0);
                ServerResponse { id, payload: None, error: Some(format!("bad request: {e}")) }
            }
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
    // `@file`/`@include` anchor: the file's own dir, else the program's `src/`
    // (so a bare `--file foo.weft` inside a project resolves relative paths as
    // the program would). SAME shared rule as the one-shot commands.
    let anchor = anchor_for(req.file.as_deref(), project.as_ref().map(|p| p.root.as_path()));

    match req.kind {
        ServerRequestKind::Parse => {
            match parse_source(&req.source, &project, &anchor, req.file.as_deref(), &req.call_path, catalogs, req.reload_catalog) {
                Ok(parse_resp) => envelope(id, &parse_resp),
                Err(e) => ServerResponse { id, payload: None, error: Some(e) },
            }
        }
        ServerRequestKind::Validate => {
            // The caller states which tier it wants; a request without one is
            // a caller bug, not something to paper over with a default.
            // Answered before the validation itself runs, so a caller
            // sending no mode hears about the mode, not about whatever
            // the project happens to contain.
            let mode = match req.mode {
                Some(m) => m,
                None => return ServerResponse { id, payload: None, error: Some("validate requires a mode (\"structural\" or \"runtime\")".into()) },
            };
            // Strict: validating outside a project is meaningless. Fail loud.
            let project = match project {
                Some(p) => p,
                None => return ServerResponse { id, payload: None, error: Some("validate requires a project (no weft.toml found from the file's directory)".into()) },
            };
            match warm_catalog(catalogs, &project.root, req.reload_catalog) {
                Ok(cat) => match do_validate(&req.source, project.id(), &anchor, cat, req.file.as_deref(), mode) {
                    Ok(v) => envelope(id, &v),
                    Err(e) => ServerResponse { id, payload: None, error: Some(e) },
                },
                Err(e) => ServerResponse { id, payload: None, error: Some(e) },
            }
        }
        ServerRequestKind::Edit => {
            // Apply the edit batch -> new source + the inverse text edit (undo).
            // Parse the result so the UI re-renders in one round-trip. Edit
            // failure is loud (the frontend keeps the pre-edit source).
            let source_id = match weft_compiler::source_name::file_id(anchor.root.as_deref(), req.file.as_deref()) {
                Ok(source_id) => source_id,
                Err(e) => return ServerResponse { id, payload: None, error: Some(e) },
            };
            // Edit ops validate written TYPE strings, so the project's
            // registry must be active (a declared name in a port
            // override is valid exactly when the catalog knows it).
            let registry = match &project {
                Some(p) => match warm_catalog(catalogs, &p.root, req.reload_catalog) {
                    Ok(cat) => cat.type_registry(),
                    Err(e) => return ServerResponse { id, payload: None, error: Some(e) },
                },
                None => FsCatalog::empty().type_registry(),
            };
            let (new_source, inverse) = match weft_compiler::edit::apply_edits(
                &req.source,
                source_id.as_deref(),
                &req.ops,
                registry,
            ) {
                Ok(r) => r,
                // The message travels verbatim: the host knows which request
                // kind it sent, so no kind prefix is smuggled into the text
                // (it would end up in a user-facing toast).
                Err(e) => return ServerResponse { id, payload: None, error: Some(e.to_string()) },
            };
            edit_envelope(id, new_source, inverse, &project, &anchor, &req, catalogs)
        }
        ServerRequestKind::ApplyEdit => {
            // Undo/redo: replay a raw text edit, parse the result. The returned
            // `inverse` is the edit that undoes THIS replay (so undo<->redo
            // round-trips without recomputing).
            let Some(text_edit) = &req.text_edit else {
                return ServerResponse { id, payload: None, error: Some("apply_edit requires a textEdit".into()) };
            };
            let new_source = match weft_compiler::edit::apply_text_edit(&req.source, text_edit) {
                Ok(s) => s,
                // Verbatim, same as the edit arm: no kind prefix in the text.
                Err(e) => return ServerResponse { id, payload: None, error: Some(e.to_string()) },
            };
            let inverse = weft_compiler::edit::invert_text_edit(&req.source, &new_source);
            edit_envelope(id, new_source, inverse, &project, &anchor, &req, catalogs)
        }
    }
}

/// Build the `{source, parse, inverse}` response shared by `edit` and
/// `apply_edit`: parse the new source (lenient, like a normal parse), or surface
/// a catalog error loudly.
fn edit_envelope(
    id: u64,
    new_source: String,
    inverse: weft_compiler::edit::TextEdit,
    project: &Option<Project>,
    anchor: &Anchor,
    req: &ServerRequest,
    catalogs: &mut HashMap<PathBuf, FsCatalog>,
) -> ServerResponse {
    match parse_source(&new_source, project, anchor, req.file.as_deref(), &req.call_path, catalogs, req.reload_catalog) {
        Ok(parse) => envelope(id, &EditResponse { source: new_source, parse, inverse }),
        Err(e) => ServerResponse { id, payload: None, error: Some(e) },
    }
}

/// Lenient canvas parsing keeps includes as navigable interfaces. At a
/// place (`call_path`, see `ServerRequest::call_path`), each node is told
/// whether it runs per instance there.
fn parse_source(
    source: &str,
    project: &Option<Project>,
    anchor: &Anchor,
    file: Option<&std::path::Path>,
    call_path: &[String],
    catalogs: &mut HashMap<PathBuf, FsCatalog>,
    reload_catalog: bool,
) -> std::result::Result<ParseResponse, String> {
    match project {
        Some(p) => {
            let cat = warm_catalog(catalogs, &p.root, reload_catalog)?;
            let mut parsed = do_parse(source, p.id(), anchor, cat, file)?;
            if !call_path.is_empty() {
                per_instance_at_place(&mut parsed, p, cat, call_path);
            }
            Ok(parsed)
        }
        None => do_parse(source, uuid::Uuid::nil(), anchor, &FsCatalog::empty(), file),
    }
}

/// Tell the nodes of an included file, parsed on its own, whether each
/// runs per instance at the place `call_path` reaches it from the entry
/// file. The file alone cannot say: a file included at several sites is
/// per instance where an instance's value reaches it and shared where none
/// does, which only the whole program knows. Its nodes carry the body ids
/// the whole program gives them, so each is matched by id and given the
/// program's answer at this place. When the program around the file does
/// not compile, the answer is not known, and a warning says so (the nodes
/// keep what the file alone says).
fn per_instance_at_place(parsed: &mut ParseResponse, project: &Project, catalog: &FsCatalog, call_path: &[String]) {
    let entry = project.main_weft();
    let program = std::fs::read_to_string(&entry)
        .map_err(|e| format!("read {}: {e}", entry.display()))
        .and_then(|source| {
            let anchor = anchor_for(Some(&entry), Some(&project.root));
            weft_compiler::compile_program(&source, project.id(), anchor.fs(), catalog)
                .map_err(|diagnostics| weft_compiler::render_diagnostics(&diagnostics))
        });
    match program {
        Ok(program) => stamp_per_instance(&mut parsed.project, &program, call_path),
        Err(why) => parsed.diagnostics.push(Diagnostic::at(
            Default::default(),
            weft_compiler::Severity::Warning,
            "per-instance-unknown",
            format!(
                "the program around this file does not compile, so whether its nodes run once per instance \
                 here is not known; what is shown is what this file says alone: {why}"
            ),
        )),
    }
}

/// [`per_instance_at_place`]'s answer, once the whole program is in hand:
/// each node of `view` the program has is given its mark at `call_path`.
fn stamp_per_instance(view: &mut ProjectDefinition, program: &ProjectDefinition, call_path: &[String]) {
    for node in view.nodes.iter_mut() {
        if let Some(whole) = program.nodes.iter().find(|n| n.id == node.id) {
            node.per_instance = whole.per_instance_at(call_path);
            node.per_instance_paths.clear();
        }
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
/// Where a source's markers resolve. `root` is the project (the wall a
/// path may not leave, and what every path is spelled from); `base` is
/// what a relative path is joined onto: the file's own directory, else the
/// program's `src/`. Outside a project the file's directory is both, and a
/// bare snippet has neither (a marker is then a compile error).
struct Anchor {
    root: Option<PathBuf>,
    base: Option<PathBuf>,
}

impl Anchor {
    /// The CLI always reads `@file`/`@include` content from disk.
    fn fs(&self) -> weft_compiler::CompileFs<'_> {
        match (&self.root, &self.base) {
            (Some(root), Some(base)) => weft_compiler::CompileFs::disk(root).anchored_at(Some(base)),
            _ => weft_compiler::CompileFs::none(),
        }
    }
}

fn anchor_for(file: Option<&std::path::Path>, project_root: Option<&std::path::Path>) -> Anchor {
    let dir = file_dir(file);
    match project_root {
        Some(root) => Anchor {
            root: Some(root.to_path_buf()),
            base: Some(dir.unwrap_or_else(|| root.join(weft_compiler::project::SRC_DIR))),
        },
        None => Anchor { root: dir.clone(), base: dir },
    }
}

/// The source file's own directory, or `None` for no `--file` OR a bare
/// filename. `Path::parent()` of `"foo.weft"` is `Some("")` (the empty path),
/// not `None`; an empty base would silently resolve `@file`/`@include` against
/// the process CWD, so an empty parent is treated as absent (caller falls back
/// to the program's `src/`). Pure, so the bare-filename branch is unit-tested.
fn file_dir(file: Option<&std::path::Path>) -> Option<std::path::PathBuf> {
    file.and_then(|f| f.parent())
        .filter(|p| !p.as_os_str().is_empty())
        .map(|p| p.to_path_buf())
}

pub async fn validate(ctx: Ctx, file: Option<std::path::PathBuf>) -> Result<()> {
    let source = read_stdin()?;
    // Nothing piped in is a loud error, never a clean bill. The source
    // comes from stdin (the editor's parse server pipes it), so a person
    // who types `weft validate` at a terminal and pipes nothing would
    // otherwise get an empty program validated and `{"diagnostics":[]}`
    // back, which reads as "your project is fine" when nothing was
    // checked at all.
    if source.trim().is_empty() {
        anyhow::bail!(
            "weft validate reads the program from stdin and nothing arrived, so nothing was \
             checked. Pipe the file in: `weft validate < src/main.weft`"
        );
    }
    // Validate is the strict pipeline: it must run against the real
    // catalog. Validating outside a project is meaningless (every node
    // type would be unknown), so a missing project is a hard error, not
    // a silent empty-catalog pass.
    let project = ctx.project()?;
    let catalog = build_project_catalog(&project.root)
        .map_err(|e| anyhow::anyhow!("catalog: {e}"))?;
    // Same single-discovery shape as `parse`: derive the `@file`/`@include` base
    // from the project we already resolved, not a second `ctx.project()` call.
    let anchor = anchor_for(file.as_deref(), Some(project.root.as_path()));
    // Runtime: the terminal command is the complete pre-flight check (graph
    // shape plus runtime rules like a provider with no model named); callers that want
    // the structural tier go through the parse-server's mode field.
    let resp = do_validate(&source, project.id(), &anchor, &catalog, file.as_deref(), ValidationMode::Runtime)
        .map_err(anyhow::Error::msg)?;
    // The catalog's warnings (left-out folders, misspelled names) go
    // to stderr either way, as `weft describe-nodes` does; the JSON on
    // stdout carries them too.
    for warning in &resp.warnings {
        eprintln!("warning: {warning}");
    }
    println!("{}", serde_json::to_string(&resp).context("serialize validate response")?);
    Ok(())
}

/// The pure strict-validate pipeline. Shared by the one-shot `weft validate`
/// command and the parse-server. The compile -> enrich -> validate pipeline
/// (and its error-to-diagnostic mapping) lives in `compile_strict`; the caller
/// picks the mode (`Structural` for the editor's Problems panel, `Runtime`
/// for pre-flight checks that must also catch the runtime rules, like a provider with no model named).
fn do_validate(
    source: &str,
    id: uuid::Uuid,
    anchor: &Anchor,
    catalog: &FsCatalog,
    file: Option<&std::path::Path>,
    mode: ValidationMode,
) -> std::result::Result<ValidateResponse, String> {
    // Same anonymous-group id derivation as parse, so a standalone file's
    // diagnostics reference the same id the editor renders.
    let source_id = weft_compiler::source_name::file_id(anchor.root.as_deref(), file)?;
    let (_, diagnostics) = weft_compiler::compile_strict(
        source,
        id,
        anchor.fs(),
        catalog,
        mode,
        source_id.as_deref(),
    );
    Ok(ValidateResponse { diagnostics, warnings: catalog.warnings().to_vec() })
}

/// For each node type in the project, pull its catalog entry. Hidden
/// types are omitted; unknown types are skipped (parse is lenient, the
/// editor renders them as placeholders).
fn collect_catalog(
    project: &ProjectDefinition,
    catalog: &FsCatalog,
) -> BTreeMap<String, NodeMetadata> {
    let mut out = BTreeMap::new();
    for node in &project.nodes {
        if out.contains_key(&node.node_type) {
            continue;
        }
        if let Some(meta) = catalog.lookup(&node.node_type) {
            if meta.features.hidden {
                continue;
            }
            // Ship RESOLVED metadata: every input's accepts + widget
            // filled with its effective value, so the editor never
            // re-derives either.
            out.insert(node.node_type.clone(), meta.resolved());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{file_dir, handle_request, ServerRequest, ServerRequestKind};
    use std::path::Path;

    /// A broken catalog folder is a warning on the answer, never a
    /// failure of a program that does not name it.
    #[test]
    fn validate_lists_the_catalog_folders_left_out_without_failing() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("nodes/broken")).unwrap();
        std::fs::write(dir.path().join("nodes/broken/metadata.json"), "{ not json").unwrap();
        let catalog = weft_compiler::build::build_project_catalog(dir.path()).unwrap();
        let anchor = super::anchor_for(None, Some(dir.path()));
        let resp = super::do_validate(
            "",
            uuid::Uuid::nil(),
            &anchor,
            &catalog,
            None,
            weft_compiler::validate::ValidationMode::Runtime,
        )
        .unwrap();
        assert!(resp.diagnostics.is_empty(), "{:?}", resp.diagnostics);
        assert!(resp.warnings.iter().any(|w| w.message.contains("broken")), "{:?}", resp.warnings);
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["warnings"].as_array().map(|w| w.len()), Some(resp.warnings.len()));
        assert_eq!(json["warnings"][0]["kind"], "left_out");
    }

    /// A file included both where an instance's value reaches it and where
    /// none does, opened in the graph at each of its two places: its node
    /// shows per instance at the first and shared at the second, from the
    /// whole program, whatever the file says alone.
    #[test]
    fn an_included_file_shows_per_instance_at_each_place_as_the_program_runs_it() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join("src/lib")).unwrap();
        let carry = "Group(text: String) -> (out: String) {\n  carry = ExecPython(text: String) -> (out: String) { code: \"x\" }\n  \
                     carry.text = self.text\n  self.out = carry.out\n}\n";
        let main = "tick = Cron { cron: \"0 0 3 * * *\" }\nshared = @include(\"lib/carry.weft\")\nshared.text = tick.scheduledTime\n\
                    bridge = BaileyBridge {\n  @per_instance\n}\nreceive = BaileyReceive\nreceive.bridge = bridge.bridge\n\
                    mine = @include(\"lib/carry.weft\")\nmine.text = receive.content\n";
        let (main_path, carry_path) = (root.join("src/main.weft"), root.join("src/lib/carry.weft"));
        std::fs::write(&main_path, main).unwrap();
        std::fs::write(&carry_path, carry).unwrap();
        let catalog = weft_catalog::FsCatalog::discover(&weft_catalog::stdlib_root().unwrap()).unwrap();
        let program = weft_compiler::compile_program(main, uuid::Uuid::nil(), super::anchor_for(Some(&main_path), Some(root)).fs(), &catalog)
            .expect("the program compiles");
        let view = |site: &str| {
            let mut parsed = super::do_parse(carry, uuid::Uuid::nil(), &super::anchor_for(Some(&carry_path), Some(root)), &catalog, Some(&carry_path))
                .expect("the file parses");
            super::stamp_per_instance(&mut parsed.project, &program, &[site.to_string()]);
            parsed.project.nodes.into_iter().find(|n| n.id.ends_with(".carry")).expect("the file's node")
        };
        let at_mine = view("mine");
        assert_eq!(at_mine.per_instance, Some(weft_core::instance::PerInstance::Derived), "{:?}", at_mine.id);
        assert!(at_mine.per_instance_paths.is_empty());
        let at_shared = view("shared");
        assert_eq!(at_shared.per_instance, None);
        assert!(at_shared.per_instance_paths.is_empty());
    }

    #[test]
    fn server_request_carries_the_validation_mode() {
        // The extension sends `mode` on validate requests; absent stays
        // None (and the validate handler then refuses the request).
        let req: ServerRequest = serde_json::from_str(
            r#"{"id":1,"kind":"validate","source":"","mode":"structural"}"#,
        )
        .unwrap();
        assert!(matches!(req.kind, ServerRequestKind::Validate));
        assert_eq!(req.mode, Some(weft_compiler::validate::ValidationMode::Structural));
        let req: ServerRequest =
            serde_json::from_str(r#"{"id":2,"kind":"validate","source":""}"#).unwrap();
        assert_eq!(req.mode, None);
        // And the handler refuses it before touching any project state.
        let resp = handle_request(req, &mut Default::default());
        assert_eq!(
            resp.error.as_deref(),
            Some("validate requires a mode (\"structural\" or \"runtime\")")
        );
    }

    #[test]
    fn file_dir_treats_bare_filename_as_absent() {
        // A bare filename has an empty parent; file_dir must return None (so
        // anchor_for falls back to the program's `src/`) rather than Some("")
        // which would resolve @file against the CWD.
        assert_eq!(file_dir(None), None);
        assert_eq!(file_dir(Some(Path::new("foo.weft"))), None);
        assert_eq!(file_dir(Some(Path::new("a/b/foo.weft"))), Some(std::path::PathBuf::from("a/b")));
    }
}
