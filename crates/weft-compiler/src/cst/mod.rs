//! Lossless concrete syntax tree for `.weft` source.
//!
//! The design in short:
//!
//! - `kind` defines the untyped `SyntaxKind` tag set + the `rowan::Language`
//!   binding. Every node and token is one of these tags.
//! - `lexer` turns source into a lossless token stream: every byte is a token,
//!   so concatenation reproduces the source exactly.
//! - `parser` feeds those tokens into rowan's `GreenNodeBuilder` top-down,
//!   producing a tree whose `to_string()` round-trips byte-for-byte.
//! - the typed-view layer (`nodes`) and the edit ops (in `crate::edit`) build on
//!   top.
//!
//! Edits are structural tree mutations re-serialized from the tree, never offset
//! splices: only the changed subtree is re-emitted, every other byte is carried
//! verbatim, so an edit cannot land in the wrong place.

pub mod kind;
pub mod lexer;
pub mod marker;
pub mod nodes;
pub mod parser;

pub use kind::{SyntaxElement, SyntaxKind, SyntaxNode, SyntaxToken, WeftLanguage};
pub use parser::{parse, parse_green};
