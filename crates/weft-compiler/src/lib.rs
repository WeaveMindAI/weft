//! The weft compiler. Turns a project directory (`weft.toml`,
//! `src/main.weft`, the nodes under `nodes/` and `src/`) into a flat `ProjectDefinition` and the
//! staged builds of what runs it: its worker (a program compiled on the
//! machine on a local install, an image on the cloud) and its infra images.
//!
//! Pipeline, as the dispatcher drives it when it builds a version
//! (`weft_dispatcher::build`, from the project files the CLI uploaded):
//! 1. `Project::load` reads `weft.toml` (and refuses a program still at
//!    the project root instead of `src/`).
//! 2. `weft_compiler::compile_lenient` parses `src/main.weft` into the
//!    lossless CST and lowers it, resolves `@include` (the build compiles
//!    each included file once, as one shared body group, and each use of
//!    it is a call site group naming that body; the editor's `Interface`
//!    mode leaves one opaque node), then flattens: a group's members join
//!    the program with the group in their scope, the lines through its
//!    ports are joined into wires end to end (`wires`), and a loop gets a
//!    `LoopIn` / `LoopOut` pair, so what comes out is one flat
//!    `ProjectDefinition` of nodes, wires and the groups as scopes.
//!    `@file` / `@asset` values resolve last, on that flat graph; a
//!    file-typed `@asset` stays a marker for the build's asset sync.
//!    Every error is collected, never an abort: the build's
//!    `compile_program` turns the ones about some parts into broken
//!    parts, the editor's `parse_only` renders around them.
//! 3. `enrich` first writes the value of every metadata wire into the
//!    input it points at and takes the wire out of the program's wires
//!    (`metadata`), then fills each node's ports and features from its
//!    catalog metadata, materializes config-derived and custom ports, and
//!    resolves TypeVars.
//! 4. `validate` checks the graph (ids, scopes, port types, required
//!    ports and `@require_one_of`, loops, trigger and infra placement,
//!    each node's declarative rules). `Structural` gates the build;
//!    `Runtime` adds the rules a sketch may leave open (a provider with
//!    no model named yet).
//! 5. The dispatcher resolves file-typed `@asset` markers into stored
//!    files, then `build_plan::plan_build_from` computes the three hashes
//!    (binary and infra in `hash`; the definition hash is weft-core's
//!    `compute_definition_hash`, re-exported there) and stages what it
//!    needs: `build::build_project` validates the resolved definition,
//!    `codegen::emit` writes the worker cargo crate (one crate per
//!    referenced node package, plus the registry and `main.rs`; the
//!    definition itself is never baked in, a worker fetches it from the
//!    broker by `definition_hash`), staged as a host build for a local
//!    install or with the Dockerfile `worker_image::emit` writes for the
//!    cloud; `image_set` lists each infra node's own images.
//! 6. The platform's builder builds everything planned not already
//!    present: a local install compiles the worker on the machine and
//!    builds infra images with `docker build`, GCP runs Cloud Build. The
//!    worker's `cargo build` runs through `weft-isolate`
//!    (`worker_isolate`): a node package that does not compile is left
//!    out of the worker and named in a record kept with it, instead of
//!    failing the build.

pub mod project;
pub mod source_name;
pub mod weft_compiler;
pub(crate) mod wires;
pub(crate) mod metadata;
pub mod cst;
pub mod edit;
pub mod file_reader;
pub mod file_ref;
pub mod enrich;
pub mod validate;
// Project source / drift hashing. `compute_definition_hash` (weft-core's,
// re-exported here) is pure (no filesystem) and always available
// (the browser WASM build computes `definition_hash` for live preview);
// the filesystem-bound hashes (binary / infra / image / builder-base,
// the workspace walks, and the enriched-project loader) are gated behind
// `build` INSIDE the module.
pub mod hash;
// The build path. Gated behind the `build` feature (default on) because it
// pulls weft-core's runtime types + native filetime; the browser WASM parse
// build turns it off.
#[cfg(feature = "build")]
pub mod codegen;
#[cfg(feature = "build")]
pub mod worker_image;
// The `weft-isolate` program every worker build runs (codegen emits its
// source as a package of its own). Compiled here for the candidate type
// codegen writes and for its tests; its `main` and the logic only `main`
// reaches run in the build, never in this crate.
#[cfg(feature = "build")]
pub(crate) mod worker_isolate;
#[cfg(feature = "build")]
pub mod build;
// The infra-image set a project needs (worker + per-node infra images). Reads the
// FsCatalog + hashes image source dirs, so it rides the `build` feature.
#[cfg(feature = "build")]
pub mod image_set;
// The shared build-freshness brain: compile -> 3 hashes -> enumerate + stage every
// artifact -> mint refs -> report the stale set. One path whatever the
// platform; only the packaging, the ref naming and the existence probe differ.
#[cfg(feature = "build")]
pub mod build_plan;
pub mod error;

use uuid::Uuid;
use weft_core::{MetadataCatalog, ProjectDefinition};

pub use file_reader::{CompileFs, DiskFileReader, FileReader, MapFileReader, PlacedFile, ResolvedFile};

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
) -> (ProjectDefinition, Vec<weft_compiler::SourceError>) {
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
        diagnostics.push(e.diagnostic("parse"));
    }
    included_metadata_values(&mut project, fs, catalog);

    // Stage 3: enrich (lenient). Unknown types / catalog misses become
    // empty-port placeholders, not aborts. The failures that SURVIVE lenient
    // mode (a bad-type port, a custom port on a node that forbids them) are real
    // authoring errors the build would reject, so surface each as an ERROR
    // diagnostic at the offending node's line, not a single span-less warning.
    for e in enrich::enrich_collecting(&mut project, catalog, enrich::EnrichPolicy::Lenient) {
        diagnostics.push(e.diagnostic());
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
        // A loop's two boundary nodes are the compiler's, with no catalog
        // entry by design; flagging them painted phantom line-0 "unknown
        // node type 'LoopIn'" warnings on every project with a loop.
        if weft_core::project::boundary_types::is_boundary(&node.node_type) {
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

/// The metadata values an `@include`'s opaque node cannot know from its
/// file's header: the ones the file gathers from its own members
/// (`self.tools = [...screen.tools]`). Each such file is compiled whole, as
/// the build compiles it, once however many `@include`s name it, and its
/// values are worked out there, so a wire from the include in the editor
/// writes what the build writes. A file that does not read, or a value it
/// cannot work out, leaves the value unknown here, and the inputs wired
/// from it are not reported as missing a driver: what is wrong is said
/// where it is written, in that file (open it, or build, to see it), as
/// for anything else inside an include.
fn included_metadata_values(project: &mut ProjectDefinition, fs: CompileFs, catalog: &dyn MetadataCatalog) {
    let (Some(root), Some(base)) = (fs.root, fs.base) else { return };
    let mut worked_out: std::collections::HashMap<String, Vec<weft_core::project::MetadataPort>> = std::collections::HashMap::new();
    for node in project.nodes.iter_mut() {
        let Some(path) = &node.include_path else { continue };
        if node.metadata_ports.iter().all(|port| port.value.is_some()) {
            continue;
        }
        if !worked_out.contains_key(path) {
            let Ok(file) = fs.reader.resolve_and_read(root, base, std::path::Path::new(path)) else { continue };
            let body_id = source_name::body_id(&file.file.from_root);
            let (mut whole, _) = weft_compiler::compile_lenient(
                file.content.as_str(),
                project.id,
                fs.anchored_at(file.file.identity.parent()),
                weft_compiler::IncludeMode::Full,
                Some(&body_id),
            );
            metadata::resolve(&mut whole, catalog);
            let ports = whole.groups.into_iter().find(|group| group.id == body_id).map(|group| group.metadata_ports).unwrap_or_default();
            worked_out.insert(path.clone(), ports);
        }
        let ports = &worked_out[path];
        for port in node.metadata_ports.iter_mut().filter(|port| port.value.is_none()) {
            port.value = ports.iter().find(|p| p.name == port.name).and_then(|p| p.value.clone());
        }
    }
}

/// Strict sibling of `parse_only`: the full pipeline (lex + parse +
/// flatten, strict enrich, validate) collecting structured diagnostics
/// instead of aborting. Every strict caller (the editor's `weft
/// validate`, the e2e fixture check) goes through it.
///
/// `mode` selects how much validation runs: `Structural` (graph shape)
/// or `Runtime` (also the rules a sketch may leave open, like a provider
/// with no model named). The editor's Problems panel and the build gate
/// want `Structural` (a project may legitimately build half-filled); the
/// editor's pre-flight before Run, Activate and Resync and `weft
/// validate` want `Runtime`.
///
/// Never aborts: a parse failure returns an empty project plus the
/// parse diagnostics, so a caller that only wants diagnostics (the
/// editor) gets them uniformly.
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
        let (mut project, parse_errors) =
            weft_compiler::compile_lenient(source, project_id, fs, weft_compiler::IncludeMode::Full, source_name);
        // A source that does not parse leaves a partial program, and the
        // later stages would only add findings about the gap.
        if !parse_errors.is_empty() {
            return (empty_project(project_id), parse_errors.iter().map(|e| e.diagnostic("parse")).collect());
        }
        // Strict enrich: one Error diagnostic PER failure, at the offending
        // node's span (so the editor squiggles the exact line).
        let mut diagnostics: Vec<Diagnostic> = enrich::enrich_collecting(&mut project, catalog, enrich::EnrichPolicy::Strict)
            .into_iter()
            .map(|e| e.diagnostic())
            .collect();
        diagnostics.extend(validate::validate_with_mode(&project, catalog, mode));
        (project, diagnostics)
    })
}

/// The program a command builds and runs from: the whole pipeline (lex +
/// parse + flatten with every `@include` inlined, strict enrich,
/// `Structural` validation), where an error about some parts of the
/// program breaks only those parts (`ProjectDefinition::broken`) instead
/// of the program. An included file that does not compile is a
/// placeholder (`weft_compiler::broken_include`), a node whose type did
/// not load is broken where it stands, a wire between two ports that
/// cannot meet breaks its two ends. Every command cuts what it touches,
/// and the cut refuses a broken part it needs (`weft_core::project::cut::cut`),
/// so another person's broken include stops nobody who does not run it.
///
/// `Err` holds every diagnostic when one is about the program as a whole
/// (the entry file does not parse, a check no cut can avoid): nothing can
/// be cut from a program that does not stand.
pub fn compile_program(
    source: &str,
    project_id: Uuid,
    fs: CompileFs,
    catalog: &dyn MetadataCatalog,
) -> Result<ProjectDefinition, Vec<Diagnostic>> {
    // A broken part names its file from the project root: the install
    // compiles in a directory of its own, whose path means nothing to
    // whoever reads the refusal.
    let root = root_identity(&fs)?;
    catalog.type_registry().scoped(|| {
        let (mut project, parse_errors) =
            weft_compiler::compile_lenient(source, project_id, fs, weft_compiler::IncludeMode::Full, None);
        let mut diagnostics: Vec<Diagnostic> = parse_errors.iter().map(|e| e.diagnostic("parse")).collect();
        // An entry file that does not parse leaves a partial program, and
        // the later stages would only add findings about the gap.
        if diagnostics.iter().any(is_whole_program_error) {
            return Err(diagnostics);
        }
        for e in enrich::enrich_collecting(&mut project, catalog, enrich::EnrichPolicy::Strict) {
            diagnostics.push(e.diagnostic());
        }
        diagnostics.extend(validate::validate_with_mode(&project, catalog, validate::ValidationMode::Structural));
        settle(&mut project, catalog, diagnostics, root.as_deref())?;
        Ok(project)
    })
}

/// The project root as `fs`'s reader names it, the identity every file
/// a compile through `fs` reads is held to ([`file_reader::path_from_root`]).
/// `None` when compiling outside any project; a root the reader cannot
/// resolve is a whole-program error.
fn root_identity(fs: &CompileFs) -> Result<Option<std::path::PathBuf>, Vec<Diagnostic>> {
    fs.root
        .map(|root| fs.reader.identity(root))
        .transpose()
        .map_err(|why| vec![Diagnostic::at(Default::default(), Severity::Error, "project-root", why)])
}

/// The errors `project` has that it does not already carry as broken
/// parts: none for a program [`compile_program`] settled, as long as
/// nothing changed it in a way that breaks something new. The build's own
/// gate, so a definition changed after its check can never build with an
/// error nobody recorded. `fs` is the view of the project it was compiled
/// through ([`compile_program`]'s), whose root the files of broken parts
/// are spelled from, however this caller spells that root.
pub fn unsettled(project: &ProjectDefinition, catalog: &dyn MetadataCatalog, fs: CompileFs) -> Vec<Diagnostic> {
    let root = match root_identity(&fs) {
        Ok(root) => root,
        Err(diagnostics) => return diagnostics,
    };
    let hiding = hiding_nodes(project, catalog);
    validate::validate_with_mode(project, catalog, validate::ValidationMode::Structural)
        .into_iter()
        .filter(|d| matches!(d.severity, Severity::Error))
        .filter_map(|d| {
            if d.subjects.is_empty() {
                return Some(d);
            }
            match broken_part(&d, root.as_deref(), &hiding) {
                Ok(part) => (!project.broken.contains(&part)).then_some(d),
                Err(unplaced) => Some(unplaced),
            }
        })
        .collect()
}

/// Fold `diagnostics` into `project`: each error about some parts of the
/// program becomes a broken part (once, however many passes found it);
/// `Err` with every diagnostic when one is about the program as a whole.
fn settle(
    project: &mut ProjectDefinition,
    catalog: &dyn MetadataCatalog,
    mut diagnostics: Vec<Diagnostic>,
    root: Option<&std::path::Path>,
) -> Result<(), Vec<Diagnostic>> {
    if diagnostics.iter().any(is_whole_program_error) {
        return Err(diagnostics);
    }
    let hiding = hiding_nodes(project, catalog);
    let mut parts = Vec::new();
    let mut unplaced = Vec::new();
    for d in diagnostics.iter().filter(|d| matches!(d.severity, Severity::Error)) {
        match broken_part(d, root, &hiding) {
            Ok(part) => parts.push(part),
            Err(why) => unplaced.push(why),
        }
    }
    if !unplaced.is_empty() {
        diagnostics.extend(unplaced);
        return Err(diagnostics);
    }
    for part in parts {
        if !project.broken.contains(&part) {
            project.broken.push(part);
        }
    }
    Ok(())
}

/// An error no cut can avoid: it names no part of the program.
fn is_whole_program_error(d: &Diagnostic) -> bool {
    matches!(d.severity, Severity::Error) && d.subjects.is_empty()
}

/// The nodes that may stand for others the program cannot see
/// (`weft_core::project::Broken::hides`): an included file's placeholder,
/// which holds none of its nodes, and a node whose type the catalog does
/// not serve, so nothing says whether it is a trigger or infra.
fn hiding_nodes(project: &ProjectDefinition, catalog: &dyn MetadataCatalog) -> std::collections::BTreeSet<String> {
    project
        .nodes
        .iter()
        .filter(|node| {
            node.include_path.is_some()
                || (!weft_core::project::own_types::is_own(&node.node_type) && catalog.lookup(&node.node_type).is_none())
        })
        .map(|node| node.id.clone())
        .collect()
}

/// The broken part an error with subjects is: its parts, the error as one
/// rendered line, its file's path from the project root (a finding in the
/// entry file names none, and reads as `src/main.weft`'s), and whether one
/// of its parts is among the `hiding` nodes ([`hiding_nodes`]).
///
/// `root` is the root's identity, the one the reader held every file it
/// read to, so a finding's file (the identity its read gave) is always
/// inside it. One that is not was stamped by something other than a
/// read, and is refused as the compiler bug it is (the `Err`, a
/// whole-program error), never named by its absolute path: that would
/// differ between two compiles of one program and read as a new error.
fn broken_part(
    d: &Diagnostic,
    root: Option<&std::path::Path>,
    hiding: &std::collections::BTreeSet<String>,
) -> Result<weft_core::project::Broken, Diagnostic> {
    let file = match d.file.as_deref() {
        Some(file) => root
            .and_then(|root| file_reader::path_from_root(root, std::path::Path::new(file)))
            .map(|rel| file_reader::slashed(&rel))
            .ok_or_else(|| {
                Diagnostic::at(
                    Default::default(),
                    Severity::Error,
                    "unplaced-finding",
                    format!(
                        "a finding names {file}, which is not a file of the project at {}; this is a bug in weft ({})",
                        root.map_or("<no project>".to_string(), |root| root.display().to_string()),
                        render_diagnostic(d)
                    ),
                )
            })?,
        None => format!("{}/{}", project::SRC_DIR, project::ENTRY_FILE),
    };
    Ok(weft_core::project::Broken {
        parts: d.subjects.clone(),
        places: d.places.clone(),
        why: format!("{file}:{}:{} {}", d.line, d.column, d.message),
        hides: d.subjects.iter().any(|part| hiding.contains(part)),
    })
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
            .map(render_diagnostic)
            .collect::<Vec<_>>()
            .join("\n")
    };
    let errors = render(true);
    if errors.is_empty() { render(false) } else { errors }
}

/// One diagnostic as one `[file:]line:column message` line. A finding in
/// an @include's file names that file; without the prefix its line number
/// reads as the compiled source's.
pub fn render_diagnostic(d: &Diagnostic) -> String {
    let file = d.file.as_deref().map(|f| format!("{f}:")).unwrap_or_default();
    format!("{file}{}:{} {}", d.line, d.column, d.message)
}

/// Turn an `Error`-severity diagnostic set into a single loud
/// [`error::BuildError::Validate`]; `Ok(())` when only warnings (or
/// nothing) remain. The build's gate (`build::build_project`) aborts on it.
#[cfg(feature = "build")]
pub(crate) fn bail_on_errors(diagnostics: Vec<Diagnostic>) -> error::BuildResult<()> {
    if diagnostics.iter().any(|d| matches!(d.severity, Severity::Error)) {
        Err(error::BuildError::Validate(render_diagnostics(&diagnostics)))
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
        defaults: Default::default(),
        given: Default::default(),
        broken: Vec::new(),
        metadata_wires: Vec::new(),
    }
}
