//! Whether a run could start: everything the dispatcher refuses a run for
//! before it starts, judged from the program and the spec alone, so
//! whoever builds a run (`weft run`, `weft run --check`, a program's
//! `ctx.run()`, a run from a saved example) refuses it the same way.

use crate::project::cut::{self, SelectionBounds};
use crate::project::graph::ProjectGraph;
use crate::project::ProjectDefinition;

use super::{refuse_instanceless, resolve_spec, unfed, Resolved, RunSpec, Unfed};

/// What stands between a run and its start, sorted by what a reader does
/// with it ([`judge`]).
#[derive(Debug, Default)]
pub struct Verdict {
    /// The spec's resolution: `None` when there is no spec (the run is the
    /// whole program) or when it could not be resolved (`refused` says why).
    pub resolved: Option<Resolved>,
    /// The inputs something in the run needs that get nothing.
    pub unfed: Vec<Unfed>,
    /// Every other reason it cannot start.
    pub refused: Vec<String>,
}

impl Verdict {
    /// Everything that stops the run, as the run itself refuses it.
    pub fn refusal(&self) -> Option<String> {
        let reasons: Vec<&str> = self.unfed.iter().map(|unfed| unfed.message.as_str()).chain(self.refused.iter().map(String::as_str)).collect();
        (!reasons.is_empty()).then(|| format!("the run cannot start:\n{}", reasons.join("\n")))
    }

    /// The spec's resolution when nothing stops the run (`None` when there
    /// is no spec: the run is the whole program), else the refusal.
    pub fn into_resolved(self) -> Result<Option<Resolved>, String> {
        match self.refusal() {
            Some(refusal) => Err(refusal),
            None => Ok(self.resolved),
        }
    }
}

/// Judge a run of `definition` shaped by `spec` the way the dispatcher
/// refuses one before it starts. `seeded` says whether the run reuses
/// another run's history (`--seed`); `seed_until` and `seed_before` bound
/// what it reuses.
pub fn judge(definition: &ProjectDefinition, spec: Option<&RunSpec>, seeded: bool, seed_until: &[String], seed_before: &[String]) -> Verdict {
    let mut verdict = Verdict::default();
    match spec {
        Some(spec) => match resolve_spec(spec, definition, &Default::default()) {
            Err(refusal) => verdict.refused.extend(refusal.errors),
            Ok(resolved) => {
                // A seeded run can feed a crossing from history, which only
                // the dispatcher reads; it refuses an unfed input after that.
                // Which instance the run is for it refuses either way.
                if !seeded {
                    verdict.unfed = unfed(definition, &resolved, spec);
                }
                if let Err(refusal) = refuse_instanceless(&resolved.program, spec.instance.as_ref()) {
                    verdict.refused.extend(refusal.errors);
                }
                verdict.resolved = Some(resolved);
            }
        },
        None => {
            if let Err(refusal) = refuse_instanceless(definition, None) {
                verdict.refused.extend(refusal.errors);
            }
        }
    }
    if let Some(bounds) = seed_bounds(seed_until, seed_before) {
        if let Err(why) = cut::walk(&ProjectGraph::new(definition), &bounds) {
            verdict.refused.push(why);
        }
    }
    verdict
}

/// The cut a seed reuses up to (`--seed-until`, `--seed-before`), walked
/// like any cut; none when neither bounds it.
pub fn seed_bounds(seed_until: &[String], seed_before: &[String]) -> Option<SelectionBounds> {
    (!seed_until.is_empty() || !seed_before.is_empty())
        .then(|| SelectionBounds { target: seed_until.to_vec(), before: seed_before.to_vec(), ..Default::default() })
}
