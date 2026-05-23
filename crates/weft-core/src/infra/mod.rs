//! Infra typed surface + pure helpers shared by every party that
//! needs to reason about an `InfraSpec`:
//!
//! - **engine** compiles + hashes a freshly-provisioned spec so it
//!   can decide skip / fresh / replace before enqueuing a lifecycle
//!   command;
//! - **supervisor** compiles + applies (kubectl) when it claims an
//!   `Apply` lifecycle command;
//! - **tests** round-trip specs through compile to pin manifest
//!   shapes.
//!
//! The dispatcher does NOT call into the compile or apply path. It
//! routes lifecycle commands and writes their outcomes; supervisor
//! does the actual kubectl work.

mod compile;
mod hash;
pub mod types;

pub use compile::{compile, CompileContext, CompileError};
pub use hash::hash_spec;
pub use types::*;
