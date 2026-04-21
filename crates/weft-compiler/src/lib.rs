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
pub mod parser;
pub mod enrich;
pub mod validate;
pub mod codegen;
pub mod build;
pub mod describe;
pub mod error;

pub use error::{CompileError, CompileResult};
