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

use std::collections::BTreeMap;
use std::io::Read;

use anyhow::{Context, Result};
use serde::Serialize;

use weft_catalog::FsCatalog;
use weft_compiler::build::build_project_catalog;
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

pub async fn parse(ctx: Ctx) -> Result<()> {
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
    let (project, diagnostics) = weft_compiler::parse_only(&source, id, &catalog);
    let catalog_map = collect_catalog(&project, &catalog);
    let resp = ParseResponse { project, catalog: catalog_map, diagnostics };
    println!("{}", serde_json::to_string(&resp).context("serialize parse response")?);
    Ok(())
}

pub async fn validate(ctx: Ctx) -> Result<()> {
    let source = read_stdin()?;
    // Validate is the strict pipeline: it must run against the real
    // catalog. Validating outside a project is meaningless (every node
    // type would be unknown), so a missing project is a hard error, not
    // a silent empty-catalog pass. The compile -> enrich -> validate
    // pipeline (and its error-to-diagnostic mapping) lives in
    // `compile_strict`; this command only picks the project and the
    // mode. `Runtime` mode = the complete check the Problems panel
    // wants (graph shape plus runtime rules like missing credentials).
    let project = ctx.project()?;
    let catalog = build_project_catalog(&project.root)
        .map_err(|e| anyhow::anyhow!("catalog: {e}"))?;
    let (_, diagnostics) =
        weft_compiler::compile_strict(&source, project.id(), &catalog, ValidationMode::Runtime);
    let resp = ValidateResponse { diagnostics };
    println!("{}", serde_json::to_string(&resp).context("serialize validate response")?);
    Ok(())
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
