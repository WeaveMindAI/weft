//! Why an execution was cancelled. Wire-pure: the journal's
//! `ExecutionCancelled` carries it, the dispatcher's SSE event mirrors
//! it, and the `cancel_execution` task hands it to the worker that owns
//! the run, so the worker's own terminal write says the same thing the
//! dispatcher's would have.
//!
//! One structured value instead of a free-text reason, because the
//! inspector has to tell a DECISION apart from a CONSEQUENCE the same way
//! it does for skips: "you pressed Stop", "a sibling run with your tag
//! killed you", and "the worker pod was shutting down" all end in
//! `ExecutionCancelled`, and a person reading the run needs to know
//! which without parsing a sentence. The sentence still exists
//! ([`CancelCause`]'s `Display`), for the places that render text.

use serde::{Deserialize, Serialize};

use crate::Color;

/// The cause behind an execution's cancellation.
// SYNC: CancelCause <-> packages/weft-graph/src/protocol.ts CancelCause
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CancelCause {
    /// A person asked: `weft stop`, the editor's Stop button, a
    /// deactivate or wipe of the project, a cancel through a signal
    /// token.
    User,
    /// Another execution of the same project stopped this one through
    /// `ctx.stop_tagged`: `by` is the execution that asked, `tag` is
    /// the tag that matched.
    Execution { by: Color, tag: String },
    /// The live caller this run was tied to dropped its connection, so
    /// the run had nobody left to answer.
    CallerGone,
    /// The runtime itself, for a reason no person chose: a worker pod
    /// shutting down mid-run, a build superseding the image a queued
    /// run needed, an orphaned setup run swept away. `detail` says
    /// which, in words for the inspector.
    Runtime { detail: String },
}

impl std::fmt::Display for CancelCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::User => write!(f, "Cancelled by user"),
            Self::Execution { by, tag } => {
                write!(f, "Stopped by execution {by} (tag {tag})")
            }
            Self::CallerGone => write!(f, "Caller disconnected"),
            Self::Runtime { detail } => write!(f, "{detail}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The cause is a cross-process wire value (journal row, SSE, task
    /// payload): every variant round-trips unchanged, and the display
    /// text a person reads is fixed per variant.
    #[test]
    fn every_cause_round_trips_and_reads() {
        let by: Color = "9d3f8f4e-9a1a-4a9b-8c1d-2f3e4a5b6c7d".parse().unwrap();
        let cases = [
            (CancelCause::User, "Cancelled by user".to_string()),
            (
                CancelCause::Execution { by, tag: "user_42".into() },
                format!("Stopped by execution {by} (tag user_42)"),
            ),
            (CancelCause::CallerGone, "Caller disconnected".to_string()),
            (
                CancelCause::Runtime { detail: "worker pod shutting down".into() },
                "worker pod shutting down".to_string(),
            ),
        ];
        for (cause, text) in cases {
            let json = serde_json::to_string(&cause).unwrap();
            let back: CancelCause = serde_json::from_str(&json).unwrap();
            assert_eq!(back, cause, "round trip changed the cause: {json}");
            assert_eq!(cause.to_string(), text);
        }
    }

    /// The wire tag is the snake_case kind, the shape every other
    /// tagged wire enum in the journal uses.
    #[test]
    fn wire_shape_is_kind_tagged() {
        let json = serde_json::to_value(CancelCause::CallerGone).unwrap();
        assert_eq!(json, serde_json::json!({ "kind": "caller_gone" }));
    }
}
