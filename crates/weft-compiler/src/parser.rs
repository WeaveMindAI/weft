//! Weft source parser. Turns `main.weft` text into a graph AST.
//!
//! Phase A2: port from `dashboard-v1/src/lib/ai/weft-parser.ts` and
//! aligned logic in v1 rust. The frontend today owns parse logic; v2
//! moves parsing to the backend (single source of truth per project
//! note: project_parser_dedup memory).

use crate::error::CompileResult;
use weft_core::ProjectDefinition;

pub fn parse_weft(_source: &str) -> CompileResult<ProjectDefinition> {
    unimplemented!("parser::parse_weft is a phase A2 port target")
}
