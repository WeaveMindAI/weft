//! The weft compiler. Turns a project directory (`main.weft`, `nodes/`,
//! `weft.toml`) into a compiled rust binary.
//!
//! Pipeline:
//! 1. `project::load` reads the project manifest and the graph source.
//! 2. `parser::parse_weft` turns the weft source into a graph AST.
//! 3. `enrich::enrich` resolves TypeVars, dynamic ports, and form-
//!    derived ports (ported from v1 in phase A2).
//! 4. `validate::validate` checks callback isolation, entry-point
//!    detection, required-port coverage.
//! 5. `codegen::emit` produces rust source files that link the graph +
//!    every referenced node (all from the project's `nodes/`).
//! 6. `build::invoke_cargo` runs cargo to produce the binary.

pub mod project;
pub mod weft_compiler;
pub mod enrich;
pub mod validate;
pub mod codegen;
pub mod worker_image;
pub mod build;
pub mod error;

pub use error::{CompileError as ProjectError, CompileResult};
pub use weft_compiler::{compile as compile_source, CompileError as SourceError};

use uuid::Uuid;
use weft_core::{MetadataCatalog, ProjectDefinition};

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
    catalog: &dyn MetadataCatalog,
) -> (ProjectDefinition, Vec<Diagnostic>) {
    let mut diagnostics = Vec::new();

    // Stages 1+2: lex + parse + flatten.
    let mut project = match weft_compiler::compile(source, project_id) {
        Ok(p) => p,
        Err(errors) => {
            // Parse failed: surface the diagnostics and return an
            // empty-but-valid project so the UI keeps rendering a
            // stable last-known-good state rather than breaking.
            for e in errors {
                diagnostics.push(Diagnostic {
                    line: e.line,
                    column: 0,
                    severity: Severity::Error,
                    message: e.message,
                    code: Some("parse".into()),
                });
            }
            return (empty_project(project_id), diagnostics);
        }
    };

    // Stage 3: enrich (lenient). Unknown types / catalog misses become
    // empty-port placeholders, not aborts.
    if let Err(e) = enrich::enrich_with_policy(&mut project, catalog, enrich::EnrichPolicy::Lenient) {
        diagnostics.push(Diagnostic {
            line: 0,
            column: 0,
            severity: Severity::Warning,
            message: format!("{e}"),
            code: Some("enrich".into()),
        });
    }

    // Surface unknown node types as warnings so the IDE can paint a
    // squiggly on the header line even without calling /validate.
    for node in &project.nodes {
        if catalog.lookup(&node.node_type).is_none() {
            let line = node
                .header_span
                .map(|s| s.start_line)
                .unwrap_or(0);
            diagnostics.push(Diagnostic {
                line,
                column: 0,
                severity: Severity::Warning,
                message: format!("unknown node type '{}'", node.node_type),
                code: Some("unknown-type".into()),
            });
        }
    }

    // Structural validate so the IDE gets inline feedback for
    // graph-shape problems (no-output-node, unreachable-from-output,
    // duplicate ids, etc.) directly from /parse. Runtime-only rules
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
    catalog: &dyn MetadataCatalog,
    mode: validate::ValidationMode,
) -> (ProjectDefinition, Vec<Diagnostic>) {
    let (project, mut diagnostics) = compile_and_enrich(source, project_id, catalog);
    diagnostics.extend(validate::validate_with_mode(&project, catalog, mode));
    (project, diagnostics)
}

/// Compile + strict enrich + validate, aborting if any `Error`-severity
/// diagnostic fires. The shape the build path wants: a clean validated
/// `ProjectDefinition` or one loud error. Layered on `compile_strict`
/// so there is exactly one pipeline; this only adds "errors abort".
pub fn compile_checked(
    source: &str,
    project_id: Uuid,
    catalog: &dyn MetadataCatalog,
    mode: validate::ValidationMode,
) -> CompileResult<ProjectDefinition> {
    let (project, diagnostics) = compile_strict(source, project_id, catalog, mode);
    bail_on_errors(diagnostics)?;
    Ok(project)
}

/// Compile + strict enrich, no validation, aborting if any
/// `Error`-severity diagnostic fires. For callers that need the
/// enriched topology (infra-closure walk, hashing) but not the full
/// validation gate, which the build path owns. Shares the same
/// compile+enrich core and abort logic as the entries above.
pub fn compile_enriched(
    source: &str,
    project_id: Uuid,
    catalog: &dyn MetadataCatalog,
) -> CompileResult<ProjectDefinition> {
    let (project, diagnostics) = compile_and_enrich(source, project_id, catalog);
    bail_on_errors(diagnostics)?;
    Ok(project)
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
    catalog: &dyn MetadataCatalog,
) -> (ProjectDefinition, Vec<Diagnostic>) {
    let mut diagnostics = Vec::new();
    let mut project = match weft_compiler::compile(source, project_id) {
        Ok(p) => p,
        Err(errors) => {
            for e in errors {
                diagnostics.push(Diagnostic {
                    line: e.line,
                    column: 0,
                    severity: Severity::Error,
                    message: e.message,
                    code: Some("parse".into()),
                });
            }
            return (empty_project(project_id), diagnostics);
        }
    };
    if let Err(e) = enrich::enrich(&mut project, catalog) {
        diagnostics.push(Diagnostic {
            line: 0,
            column: 0,
            severity: Severity::Error,
            message: format!("{e}"),
            code: Some("enrich".into()),
        });
    }
    (project, diagnostics)
}

/// Turn an `Error`-severity diagnostic set into a single loud
/// `CompileError`; `Ok(())` when only warnings (or nothing) remain.
/// One place so every aborting entry formats failures identically.
fn bail_on_errors(diagnostics: Vec<Diagnostic>) -> CompileResult<()> {
    let msg = diagnostics
        .iter()
        .filter(|d| matches!(d.severity, Severity::Error))
        .map(|d| format!("{}:{} {}", d.line, d.column, d.message))
        .collect::<Vec<_>>()
        .join("\n");
    if msg.is_empty() {
        Ok(())
    } else {
        Err(error::CompileError::Validate(msg))
    }
}

fn empty_project(project_id: Uuid) -> ProjectDefinition {
    ProjectDefinition {
        id: project_id,
        name: String::new(),
        description: None,
        nodes: Vec::new(),
        edges: Vec::new(),
        groups: Vec::new(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    }
}
