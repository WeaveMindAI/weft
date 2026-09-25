//! The weft compiler. Turns a project directory (`weft.toml`,
//! `src/main.weft`, `nodes/`) into a flat `ProjectDefinition` and the
//! docker build contexts of the images that run it.
//!
//! Pipeline, as `weft run` / `weft build` drive it:
//! 1. `Project::load` reads `weft.toml` (and refuses a program still at
//!    the project root instead of `src/`).
//! 2. `weft_compiler::compile` parses `src/main.weft` into the lossless
//!    CST and lowers it, resolves `@include` (the build inlines the
//!    included group's body; the editor's `Interface` mode leaves one
//!    opaque node), then flattens: a group becomes two `Passthrough`
//!    boundary nodes and a loop a `LoopIn` / `LoopOut` pair, so what
//!    comes out is one flat `ProjectDefinition` of nodes and edges.
//!    `@file` / `@asset` values resolve last, on that flat graph; a
//!    file-typed `@asset` stays a marker for the build's asset sync.
//!    (`compile_lenient` is the same pipeline for the editor, collecting
//!    errors instead of aborting.)
//! 3. `enrich` fills each node's ports and features from its catalog
//!    metadata, materializes config-derived and custom ports, and
//!    resolves TypeVars.
//! 4. `validate` checks the graph (ids, scopes, port types, required
//!    ports and `@require_one_of`, loops, trigger and infra placement,
//!    each node's declarative rules). `Structural` gates the build;
//!    `Runtime` adds the rules a sketch may leave open (credentials).
//! 5. The CLI resolves file-typed `@asset` markers into stored files
//!    (`weft-assets`), then `build_plan::plan_build_from` computes the
//!    three hashes (`hash`: binary, definition, infra) and stages every
//!    image: `build::build_project` validates the resolved definition,
//!    `codegen::emit` writes the worker cargo crate (one crate per
//!    referenced node package, plus the registry and `main.rs`; the
//!    definition itself is never baked in, a worker fetches it from the
//!    broker by `definition_hash`), and `worker_image::emit` writes its
//!    Dockerfile; `image_set` lists each infra node's own images.
//! 6. The CLI builds every planned image not already present with
//!    docker. The worker's `cargo build` runs inside its docker build,
//!    never on the host.

pub mod project;
pub mod source_name;
pub mod weft_compiler;
pub mod cst;
pub mod edit;
pub mod file_reader;
pub mod file_ref;
pub mod enrich;
pub mod validate;
// Project source / drift hashing. `compute_definition_hash` is pure
// (no filesystem) and always available
// (the browser WASM build computes `definition_hash` for live preview);
// the filesystem-bound hashes (binary / infra / image / builder-base,
// the workspace walks, and the enriched-project loader) are gated behind
// `build` INSIDE the module.
pub mod hash;
// The image-build path. Gated behind the `build` feature (default on) because it
// pulls weft-core's runtime types + native filetime; the browser WASM parse
// build turns it off.
#[cfg(feature = "build")]
pub mod codegen;
#[cfg(feature = "build")]
pub mod worker_image;
#[cfg(feature = "build")]
pub mod build;
// The infra-image set a project needs (worker + per-node infra images). Reads the
// FsCatalog + hashes image source dirs, so it rides the `build` feature.
#[cfg(feature = "build")]
pub mod image_set;
// The shared build-freshness brain: compile -> 3 hashes -> enumerate + stage every
// image -> mint refs -> report the stale set. One path regardless of build
// strategy; only the ref/tag naming + existence probe differ.
#[cfg(feature = "build")]
pub mod build_plan;
pub mod error;

pub use error::{CompileError as ProjectError, CompileResult};
pub use weft_compiler::{compile as compile_source, CompileError as SourceError};

use uuid::Uuid;
use weft_core::{MetadataCatalog, ProjectDefinition};

pub use file_reader::{CompileFs, DiskFileReader, FileReader, MapFileReader, ResolvedFile};

// Re-export weft_core's Diagnostic/Severity so downstream callers
// keep using weft_compiler::Diagnostic without touching node impls.
pub use weft_core::node::{Diagnostic, Severity};

/// Fast-path parse for interactive editing (IDE, live preview). Runs
/// lex + parse + flatten + lenient enrich. Does NOT run validation:
/// the slow-path `validate()` does that on a longer debounce.
///
/// Unknown node types, missing catalog entries, and malformed partial
/// programs produce diagnostics but don't abort; the returned project
/// is always usable for rendering.
pub fn parse_only(
    source: &str,
    project_id: Uuid,
    fs: CompileFs,
    catalog: &dyn MetadataCatalog,
    source_name: Option<&str>,
) -> (ProjectDefinition, Vec<Diagnostic>) {
    // The catalog's type registry is active for the whole pipeline, so
    // a declared type name in source (a port override, a record) parses
    // against the same table the metadata was loaded under.
    catalog.type_registry().scoped(|| parse_only_inner(source, project_id, fs, catalog, source_name))
}

/// Lex + parse + flatten with every `@include` inlined, LENIENT, and
/// nothing after it: no enrich, no validation. The whole program's
/// node list, for a caller that reads it before the program is wired
/// up (a missing input or a mis-typed literal leaves the list whole).
/// A declared type name in source (a group port typed `LlmProvider`)
/// resolves against the catalog's registry, the table every other
/// entry here parses under.
pub fn flatten_lenient(
    source: &str,
    project_id: Uuid,
    fs: CompileFs,
    catalog: &dyn MetadataCatalog,
) -> (ProjectDefinition, Vec<weft_compiler::CompileError>) {
    // The build compiles the entry file with no source id, so this does
    // too: the ids it hands out are the ones the build keys nodes by.
    catalog.type_registry().scoped(|| {
        weft_compiler::compile_lenient(source, project_id, fs, weft_compiler::IncludeMode::Full, None)
    })
}

fn parse_only_inner(
    source: &str,
    project_id: Uuid,
    fs: CompileFs,
    catalog: &dyn MetadataCatalog,
    source_name: Option<&str>,
) -> (ProjectDefinition, Vec<Diagnostic>) {
    let mut diagnostics = Vec::new();

    // Stages 1+2: lex + parse + flatten, LENIENT. A bad line becomes a
    // diagnostic; every valid node/edge around it still renders (the editor
    // must keep showing the graph mid-edit, never blank out on one typo).
    // Interface mode for @include: opaque nodes the editor navigates into.
    // `source_name` is the file's identity (e.g. `MyCleaner`); an anonymous
    // top-level group takes it as its id, so the file's root carries the same
    // id at parse, edit, and render with no sentinel to rename later.
    let (mut project, parse_errors) = weft_compiler::compile_lenient(
        source,
        project_id,
        fs,
        weft_compiler::IncludeMode::Interface,
        source_name,
    );
    for e in parse_errors {
        diagnostics
            .push(Diagnostic::at(e.span, Severity::Error, "parse", e.message).in_file(e.file.as_deref()));
    }

    // Stage 3: enrich (lenient). Unknown types / catalog misses become
    // empty-port placeholders, not aborts. The failures that SURVIVE lenient
    // mode (a bad-type port, a custom port on a node that forbids them) are real
    // authoring errors the build would reject, so surface each as an ERROR
    // diagnostic at the offending node's line, not a single span-less warning.
    for e in enrich::enrich_collecting(&mut project, catalog, enrich::EnrichPolicy::Lenient) {
        diagnostics.push(Diagnostic::at(e.span, Severity::Error, "enrich", e.message).in_file(e.file.as_deref()));
    }

    // Surface unknown node types as warnings so the IDE can paint a
    // squiggly on the header line even without calling /validate.
    for node in &project.nodes {
        // Opaque `@include` interface nodes carry no catalog entry by design
        // (their ports come from the included file's Group header). Don't
        // flag them as unknown types.
        if node.include_path.is_some() {
            continue;
        }
        // Compiler-synthesized boundary nodes (group/loop lowering) have no
        // catalog entry by design; flagging them painted phantom line-0
        // "unknown node type 'Passthrough'" warnings on every project with
        // a group.
        if enrich::is_lowering_builtin(&node.node_type) {
            continue;
        }
        if catalog.lookup(&node.node_type).is_none() {
            diagnostics.push(Diagnostic::at(
                node.header_span_or_default(),
                Severity::Warning,
                "unknown-type",
                enrich::unknown_type_message(catalog, &node.node_type),
            ));
        }
    }

    // Structural validate so the IDE gets inline feedback for
    // graph-shape problems (graph-cycle, scope-reachability, duplicate
    // ids, etc.) directly from /parse. Runtime-only rules
    // still only fire from the dedicated /validate endpoint.
    diagnostics.extend(validate::validate_with_mode(
        &project,
        catalog,
        validate::ValidationMode::Structural,
    ));

    (project, diagnostics)
}

/// Strict sibling of `parse_only`: the full pipeline (lex + parse +
/// flatten, strict enrich, validate) collecting structured diagnostics
/// instead of aborting. This is the single home for the
/// error-to-`Diagnostic` mapping; every strict caller (the editor's
/// `weft validate`, and `compile_checked` below for build/hash) goes
/// through it, so the four paths can't drift.
///
/// `mode` selects how much validation runs: `Structural` (graph shape)
/// or `Runtime` (also missing-credential style rules). The editor's
/// Problems panel wants `Runtime`; the build gate wants `Structural`
/// (a project may legitimately build without every secret filled).
///
/// Never aborts: a parse failure returns an empty project plus the
/// parse diagnostics, mirroring `parse_only`, so a caller that only
/// wants diagnostics (the editor) gets them uniformly. Callers that
/// must abort on errors use `compile_checked`.
pub fn compile_strict(
    source: &str,
    project_id: Uuid,
    fs: CompileFs,
    catalog: &dyn MetadataCatalog,
    mode: validate::ValidationMode,
    source_name: Option<&str>,
) -> (ProjectDefinition, Vec<Diagnostic>) {
    // Same registry activation as `parse_only`: one table for source
    // parsing, enrichment, and validation.
    catalog.type_registry().scoped(|| {
        let (project, mut diagnostics) =
            compile_and_enrich(source, project_id, fs, catalog, source_name);
        diagnostics.extend(validate::validate_with_mode(&project, catalog, mode));
        (project, diagnostics)
    })
}

/// Compile + strict enrich + validate, aborting if any `Error`-severity
/// diagnostic fires. The shape the build path wants: a clean validated
/// `ProjectDefinition` or one loud error. Layered on `compile_strict`
/// so there is exactly one pipeline; this only adds "errors abort".
pub fn compile_checked(
    source: &str,
    project_id: Uuid,
    fs: CompileFs,
    catalog: &dyn MetadataCatalog,
    mode: validate::ValidationMode,
) -> CompileResult<ProjectDefinition> {
    // Build path: the ENTRY file, which may hold no anonymous root (`None`
    // is what makes the compiler refuse one there).
    let (project, diagnostics) = compile_strict(source, project_id, fs, catalog, mode, None);
    bail_on_errors(diagnostics)?;
    Ok(project)
}

/// Compile + strict enrich, no validation, returning the full
/// diagnostic list on failure. For callers that need the enriched
/// topology (infra-closure walk, hashing) but not the full validation
/// gate (which the build path owns) and want to surface structured
/// per-error info to the user.
pub fn compile_enriched_with_diagnostics(
    source: &str,
    project_id: Uuid,
    fs: CompileFs,
    catalog: &dyn MetadataCatalog,
) -> Result<ProjectDefinition, Vec<Diagnostic>> {
    let (project, diagnostics) = compile_and_enrich(source, project_id, fs, catalog, None);
    let any_errors = diagnostics
        .iter()
        .any(|d| matches!(d.severity, Severity::Error));
    if any_errors {
        Err(diagnostics)
    } else {
        Ok(project)
    }
}

/// The shared front half of every strict pipeline: lex + parse +
/// flatten, then strict enrich, collecting failures as `Error`
/// diagnostics rather than aborting. The single home for the
/// parse/enrich error-to-`Diagnostic` mapping. A parse failure yields
/// an empty project (mirrors `parse_only`) so the shape is uniform;
/// callers decide whether to abort (`bail_on_errors`) or surface.
fn compile_and_enrich(
    source: &str,
    project_id: Uuid,
    fs: CompileFs,
    catalog: &dyn MetadataCatalog,
    source_name: Option<&str>,
) -> (ProjectDefinition, Vec<Diagnostic>) {
    // Parsing and enrichment both resolve declared type names, so the
    // catalog's registry is activated HERE, at the shared front half,
    // rather than trusting every public entry to remember the wrap
    // (`compile_enriched_with_diagnostics`, the build/hash path, once
    // forgot it and custom types failed only at build time). The scope
    // is a re-entrant stack, so `compile_strict`'s outer scope (which
    // also covers validation) nests harmlessly.
    catalog.type_registry().scoped(|| compile_and_enrich_inner(source, project_id, fs, catalog, source_name))
}

fn compile_and_enrich_inner(
    source: &str,
    project_id: Uuid,
    fs: CompileFs,
    catalog: &dyn MetadataCatalog,
    source_name: Option<&str>,
) -> (ProjectDefinition, Vec<Diagnostic>) {
    let mut diagnostics = Vec::new();
    let mut project = match weft_compiler::compile_with_mode(
        source,
        project_id,
        fs,
        weft_compiler::IncludeMode::Full,
        source_name,
    ) {
        Ok(p) => p,
        Err(errors) => {
            for e in errors {
                diagnostics
                    .push(Diagnostic::at(e.span, Severity::Error, "parse", e.message).in_file(e.file.as_deref()));
            }
            return (empty_project(project_id), diagnostics);
        }
    };
    // Strict enrich: one Error diagnostic PER failure, at the offending node's
    // span (so the editor squiggles the exact line), not a single span-less blob.
    for e in enrich::enrich_collecting(&mut project, catalog, enrich::EnrichPolicy::Strict) {
        diagnostics.push(Diagnostic::at(e.span, Severity::Error, "enrich", e.message).in_file(e.file.as_deref()));
    }
    (project, diagnostics)
}

/// Render a diagnostic list as one `line:column message` per line.
/// Prefers Error-severity lines; if the compile failed with only
/// warnings/hints, renders those instead of an empty string that
/// would read as "failed for no stated reason". The single
/// human-facing diagnostic rendering: `bail_on_errors` here and the
/// CLI's TTY compile-failure path both call it so the two can't drift.
pub fn render_diagnostics(diagnostics: &[Diagnostic]) -> String {
    let render = |only_errors: bool| -> String {
        diagnostics
            .iter()
            .filter(|d| !only_errors || matches!(d.severity, Severity::Error))
            // A finding in an @include's file names that file; without
            // the prefix its line number reads as the compiled source's.
            .map(|d| {
                let file = d.file.as_deref().map(|f| format!("{f}:")).unwrap_or_default();
                format!("{file}{}:{} {}", d.line, d.column, d.message)
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    let errors = render(true);
    if errors.is_empty() { render(false) } else { errors }
}

/// Turn an `Error`-severity diagnostic set into a single loud
/// `CompileError`; `Ok(())` when only warnings (or nothing) remain.
/// One place so every aborting entry formats failures identically.
pub(crate) fn bail_on_errors(diagnostics: Vec<Diagnostic>) -> CompileResult<()> {
    if diagnostics.iter().any(|d| matches!(d.severity, Severity::Error)) {
        Err(error::CompileError::Validate(render_diagnostics(&diagnostics)))
    } else {
        Ok(())
    }
}

fn empty_project(project_id: Uuid) -> ProjectDefinition {
    ProjectDefinition {
        id: project_id,
        nodes: Vec::new(),
        edges: Vec::new(),
        groups: Vec::new(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    }
}
