//! What the install's image endpoints answer and the CLI reads: one Rust
//! type per answer, so the two ends cannot drift.

use serde::{Deserialize, Serialize};

/// `POST /images/prune`: which images to prune. No project is every
/// project's; a project is only that one's.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PruneRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<uuid::Uuid>,
}

/// `POST /images/prune`: what a prune did. Every field is required, so an
/// answer missing one is a dispatcher the CLI does not match, never an
/// empty list.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PruneReport {
    pub removed: Vec<String>,
    /// Images a container still runs from (a removed project's unit
    /// whose teardown has not finished), or a build in progress is about
    /// to register; the next prune takes them.
    pub in_use: Vec<String>,
    /// Images that could not be deleted, with why; the next prune tries
    /// them again.
    pub failed: Vec<(String, String)>,
}
