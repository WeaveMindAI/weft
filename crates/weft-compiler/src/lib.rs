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
//!    every referenced node (from stdlib + user `nodes/` + vendor).
//! 6. `build::invoke_cargo` runs cargo to produce the binary.
//!
//! Phase A1 (scaffold) stubs these out. Phase A2 ports real logic from
//! `crates-v1/weft-core/src/weft_compiler.rs` and
//! `crates-v1/weft-nodes/src/enrich.rs`.

pub mod project;
pub mod weft_compiler;
pub mod enrich;
pub mod validate;
pub mod codegen;
pub mod build;
pub mod describe;
pub mod error;

pub use error::{CompileError as ProjectError, CompileResult};
pub use weft_compiler::{compile as compile_source, CompileError as SourceError};

use uuid::Uuid;
use weft_core::{NodeCatalog, ProjectDefinition};

/// Diagnostic emitted during parse or validate. Structured for
/// consumption by VS Code's Problems panel: line + severity + message
/// + optional code (for click-through to docs).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Diagnostic {
    pub line: usize,
    pub column: usize,
    pub severity: Severity,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Error,
    Warning,
    Info,
    Hint,
}

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
    catalog: &dyn NodeCatalog,
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

    (project, diagnostics)
}

fn empty_project(project_id: Uuid) -> ProjectDefinition {
    ProjectDefinition {
        id: project_id,
        name: String::new(),
        description: None,
        nodes: Vec::new(),
        edges: Vec::new(),
        status: Default::default(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    }
}
