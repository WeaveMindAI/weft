//! The project-teardown guard: clean up on a PASSING test, keep + warn on a
//! FAILING one. [`crate::project::Project`] embeds one. The policy:
//!
//!   - a passing test does its removal (remove the project + its temp copy,
//!     awaited so a failure is loud) and then calls [`Teardown::complete`],
//!     which marks the guard done so `Drop` stays silent.
//!   - a test that panics / returns early never reaches that, so [`Drop`] fires
//!     instead and is ONLY a safety net: it removes NOTHING (Drop cannot await,
//!     and a detached spawn would race process exit and orphan projects
//!     unpredictably) and warns, printing the exact recovery commands so the
//!     kept-for-post-mortem state is always cleanable by hand.
//!
//! The guard owns the id, the finished bookkeeping and the Drop warning; the
//! removal itself and the recovery hint come from the project.

use uuid::Uuid;

/// Owns the clean-on-pass / keep-and-warn-on-fail teardown policy for one
/// project. The project's `finish` does the removal then calls
/// [`Teardown::complete`]; the project's `Drop` is this guard's `Drop`.
pub struct Teardown {
    /// The project id (for messages + the recovery hint).
    id: Uuid,
    /// A short label for messages (the fixture name), so a kept-on-fail
    /// warning says WHICH project.
    label: String,
    /// Set once `complete` has run, so `Drop` neither warns nor double-removes.
    finished: bool,
    /// The exact commands to recover the kept state by hand, printed by `Drop`
    /// when a test ends early (`weft rm` and the temp dir to delete).
    recovery_hint: String,
}

impl Teardown {
    /// Build a guard for project `id` labelled `label`, with the injected
    /// `recovery_hint` (the by-hand cleanup commands the `Drop` warning prints
    /// when a test ends early).
    pub fn new(id: Uuid, label: impl Into<String>, recovery_hint: impl Into<String>) -> Self {
        let label = label.into();
        // Said the moment the project exists, not only when the guard
        // drops: a test the runner stops for running too long dies without
        // dropping anything, and the post-mortem finds its projects here.
        // SYNC: this line <-> scripts/run-e2e.sh (post_mortem)
        eprintln!("weft-e2e: made project '{label}' ({id})");
        Self {
            id,
            label,
            finished: false,
            recovery_hint: recovery_hint.into(),
        }
    }

    /// The project id.
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Mark teardown DONE: the project and its temp copy are removed on a
    /// passing test, so `Drop` must stay silent. Call as the last step of
    /// `finish`, AFTER the removal succeeded, so a removal failure (which
    /// returns early before this) still keeps + warns.
    pub fn complete(&mut self) {
        self.finished = true;
    }
}

impl Drop for Teardown {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // Test ended without `complete` (panic / early return / forgot, OR a
        // removal that failed before reaching `complete`). Keep the project
        // and its temp copy for post-mortem and print the exact recovery
        // commands. No removal here: Drop cannot await.
        // Printed, not logged: a test binary runs no log subscriber, and this
        // line is what the reader of a failed file's log is looking for.
        eprintln!(
            "weft-e2e: project '{}' ({}) NOT finished (test ended early); keeping it for \
             inspection. Recover with: {}",
            self.label,
            self.id,
            self.recovery_hint,
        );
    }
}
