//! The shared project-teardown guard: clean up on a PASSING test, keep + warn
//! on a FAILING one.
//!
//! This is the one place the "what teardown means" policy lives, so the CLI
//! and HTTP e2e suites cannot drift on it. The policy:
//!
//!   - a passing test does its removal (remove the remote project + any local
//!     artifacts, awaited so a failure is loud) and then calls
//!     [`Teardown::complete`], which marks the guard done so `Drop` stays silent.
//!   - a test that panics / returns early never reaches that, so [`Drop`] fires
//!     instead and is ONLY a safety net: it removes NOTHING (Drop cannot await,
//!     and a detached spawn would race process exit and orphan projects
//!     unpredictably) and warns, printing the exact recovery commands so the
//!     kept-for-post-mortem state is always cleanable by hand.
//!
//! The guard is backing-agnostic: it owns the id, the registered/finished
//! bookkeeping, and the Drop warning, but knows nothing about HOW a project is
//! removed (CLI `weft rm`, or an HTTP `DELETE` from an API-driven harness). Each
//! suite writes its own small removal body and supplies the recovery-hint string;
//! the guard guarantees every suite keeps-and-warns identically on failure.

use uuid::Uuid;

/// Owns the clean-on-pass / keep-and-warn-on-fail teardown policy for one
/// project, independent of how the project is created or removed. Both e2e
/// suites embed one of these: their `finish` does the suite-specific removal
/// then calls [`Teardown::complete`], and their fixture's `Drop` is just this
/// guard's `Drop`, so the keep-and-warn behaviour has a single definition.
pub struct Teardown {
    /// The project id (for messages + the recovery hint).
    id: Uuid,
    /// A short label for messages (the fixture name, or the project name in an
    /// API-driven harness) so a kept-on-fail warning says WHICH project.
    label: String,
    /// Whether the project was registered on the dispatcher, so a suite's
    /// `finish` knows whether a remote remove is even needed (an unregistered
    /// project has no remote state to drop, only local artifacts).
    registered: bool,
    /// Set once `complete` has run, so `Drop` neither warns nor double-removes.
    finished: bool,
    /// The exact commands to recover the kept state by hand, printed by `Drop`
    /// when a test ends early. Suite-specific (the local suite points at a temp
    /// dir + `weft rm`; an API-driven suite points at `weft rm --remote` / a
    /// DELETE), so the guard does not synthesize it.
    recovery_hint: String,
}

impl Teardown {
    /// Build a guard for project `id` labelled `label`, with the injected
    /// `recovery_hint` (the by-hand cleanup commands the `Drop` warning prints
    /// when a test ends early).
    pub fn new(id: Uuid, label: impl Into<String>, recovery_hint: impl Into<String>) -> Self {
        Self {
            id,
            label: label.into(),
            registered: false,
            finished: false,
            recovery_hint: recovery_hint.into(),
        }
    }

    /// The project id.
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Whether the project is marked registered on the dispatcher (so a suite's
    /// `finish` knows to issue the remote remove).
    pub fn registered(&self) -> bool {
        self.registered
    }

    /// Mark the project registered on the dispatcher, so teardown removes it.
    pub fn mark_registered(&mut self) {
        self.registered = true;
    }

    /// Mark teardown DONE: the suite has removed the project (and local
    /// artifacts) on a passing test, so `Drop` must stay silent. Call as the last
    /// step of a suite's `finish`, AFTER the removal succeeded, so a removal
    /// failure (which returns early before this) still keeps + warns.
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
        // removal that failed before reaching `complete`). Keep the remote
        // project + any local artifacts for post-mortem and print the exact
        // recovery commands. No remote teardown here: Drop cannot await.
        tracing::warn!(
            "weft-e2e: project '{}' ({}) NOT finished (test ended early); keeping it for \
             inspection. Recover with: {}",
            self.label,
            self.id,
            self.recovery_hint,
        );
    }
}
