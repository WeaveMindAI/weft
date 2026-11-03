//! A program's calls on runs: starting one (`ctx.project(..).code()
//! .run()`), reading, counting and cleaning a project's runs
//! (`ctx.runs()`, `ctx.costs()`), and acting on one run named by its id
//! (`ctx.run_by_id(..)`, `ctx.resolve_run(..)`), as `weft stop`, `weft
//! wake`, `weft logs`, `weft events`, `weft freeze` and `weft clean <run>`
//! do.
//!
//! A run named by its id is held to its owner, never to a project: a run
//! outlives its project, and its record stays readable and removable after
//! `weft rm`. So the calls on one run carry no project of their own.

use serde::{Deserialize, Serialize};

use super::{CostFilter, RunFilter, RunStatus, StillGoing};
use crate::RunId;

/// One call on runs.
// SYNC: RunCall <-> crates/weft-dispatcher/src/actions/runs.rs (`program_call`'s match)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "call", rename_all = "snake_case")]
pub enum RunCall {
    /// Start a run of a version a build registered, as `weft run` does.
    Start { request: Box<crate::versions::VersionRunRequest> },
    /// Delete runs matching `filter`. Runs still going follow `running`
    /// ([`StillGoing`]).
    Clean { filter: RunFilter, running: StillGoing },
    /// The newest runs the listing reaches ([`super::RunPage`]), as `weft
    /// runs` lists them.
    List(RunList),
    /// How many runs match `filter` ([`super::RunsCounted`]).
    Count { filter: RunFilter },
    /// Where one node of a run stands ([`super::NodeState`]), read from
    /// that run's record. `node` is spelled the way `--from` takes it; a
    /// node inside a loop is refused, a loop itself is read.
    NodeState { run: RunId, node: String },
    /// What calls cost, filtered.
    Costs { filter: CostFilter },
    /// Stop a run, as `weft stop` does ([`RunStopped`]). Naming the
    /// calling run stops it: the call does not return.
    Stop { run: RunId },
    /// Resolve a wait of `node` in a run now, as `weft wake` does.
    Wake { run: RunId, node: String, request: WakeRequest },
    /// The tail of a run's log ([`super::RunLogs`]), as `weft logs`.
    Logs { run: RunId, request: LogsRequest },
    /// A run's events, filtered ([`super::RunEvents`]), as `weft events`.
    Events { run: RunId, request: EventsRequest },
    /// Everything a run produced ([`crate::run_spec::Expected`]), as
    /// `weft freeze` reads it.
    Outputs { run: RunId, request: OutputsRequest },
    /// One run as the listing shows it, and what it is parked on
    /// ([`super::RunDetail`]).
    Detail { run: RunId },
    /// The one run of the owner's whose id starts with `prefix`
    /// ([`super::ResolvedRun`]).
    Resolve { prefix: String },
    /// Delete one run, as `weft clean <run>` does ([`super::DeletedRun`]).
    Delete { run: RunId },
}

impl RunCall {
    pub const NAMES: &'static [&'static str] = &[
        "weft.runs.start",
        "weft.runs.clean",
        "weft.runs.list",
        "weft.runs.count",
        "weft.runs.node_state",
        "weft.costs.list",
        "weft.runs.stop",
        "weft.runs.wake",
        "weft.runs.logs",
        "weft.runs.events",
        "weft.runs.outputs",
        "weft.runs.detail",
        "weft.runs.resolve",
        "weft.runs.delete",
    ];

    pub fn name(&self) -> &'static str {
        match self {
            RunCall::Start { .. } => "weft.runs.start",
            RunCall::Clean { .. } => "weft.runs.clean",
            RunCall::List(_) => "weft.runs.list",
            RunCall::Count { .. } => "weft.runs.count",
            RunCall::NodeState { .. } => "weft.runs.node_state",
            RunCall::Costs { .. } => "weft.costs.list",
            RunCall::Stop { .. } => "weft.runs.stop",
            RunCall::Wake { .. } => "weft.runs.wake",
            RunCall::Logs { .. } => "weft.runs.logs",
            RunCall::Events { .. } => "weft.runs.events",
            RunCall::Outputs { .. } => "weft.runs.outputs",
            RunCall::Detail { .. } => "weft.runs.detail",
            RunCall::Resolve { .. } => "weft.runs.resolve",
            RunCall::Delete { .. } => "weft.runs.delete",
        }
    }

    /// Only a clean carries a [`crate::tag::StopSelf`] that matters: a stop
    /// names its run exactly, so naming the calling run is the choice.
    pub fn takes_down(&self) -> bool {
        matches!(self, RunCall::Clean { .. })
    }
}

/// What a runs listing reaches, newest first: `weft runs`'s flags, the
/// editor's Runs view and `ctx.runs().list(..)`. The project is the
/// listing's door's (the `project_id` query parameter, or the program
/// call's project); every other filter narrows.
// SYNC: RunList <-> crates/weft-dispatcher/src/api/run.rs (`ListRunsParams`, its URL spelling)
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunList {
    #[serde(default)]
    pub filter: RunFilter,
    /// How many runs one page holds: 1 to [`super::MAX_RUNS_PAGE`].
    pub limit: u32,
    /// Skip this many runs before the page.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub offset: u32,
    /// Only runs of this phase.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<crate::primitive::Phase>,
    /// Only runs started at or after this unix second.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_after: Option<u64>,
    /// Only runs started before this unix second.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_before: Option<u64>,
    /// Only finished runs in which this node fired, spelled the way `weft
    /// events` names nodes, once their search entry is built.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub through: Option<String>,
    /// Only finished runs whose recorded values carry every word of this,
    /// once their search entry is built.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search: Option<String>,
    /// The page after this run (the last run of the page before): a walk
    /// that hands each page's last run back reaches every run once,
    /// whatever starts or ends meanwhile, which `offset` cannot promise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<PageMark>,
}

fn is_zero(n: &u32) -> bool {
    *n == 0
}

/// The last run of a page, which the next page starts after.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PageMark {
    pub started_at: u64,
    pub run: RunId,
}

/// `POST /runs/{id}/wake/{node}`: which wait, for a node waiting in
/// several loop iterations at once.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WakeRequest {
    /// The loop iterations, `3`, or `3.0` inside an inner loop.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iteration: Option<String>,
}

/// `GET /runs/{id}/logs`: how many lines of the tail.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogsRequest {
    /// 1 to the ceiling the answer names; 1000 when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

/// `GET /runs/{id}/events`: the rows about one node or group (`node`,
/// spelled the way the program the run ran is written), of one kind,
/// inside some loop iterations (`iteration`, `3` or `3.0`). All optional.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventsRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iteration: Option<String>,
}

/// `GET /runs/{id}/outputs`: the places whose outputs a later diff puts
/// first (`weft freeze --expect`), comma-separated, each a node or a loop
/// spelled the way the program the run ran is written.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OutputsRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expect: Option<String>,
}

/// What a stop did: asked the run to stop, or found it already ended
/// (`already_ended`, its status), so a stop carried again answers the same.
/// `weft stop` reads the second as a 409 naming the status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunStopped {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub already_ended: Option<RunStatus>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::program::ProgramCall;

    fn round_trips(call: RunCall, expected: serde_json::Value) {
        let call = ProgramCall::Runs(call);
        let wire = serde_json::to_value(&call).unwrap();
        assert_eq!(wire, expected);
        assert_eq!(serde_json::from_value::<ProgramCall>(wire).unwrap(), call);
    }

    /// Every call on one run round-trips under its `area` and `call` tags,
    /// its fields in camelCase.
    #[test]
    fn the_calls_on_one_run_round_trip_under_their_tags() {
        let run = RunId::from_u128(7);
        let id = run.to_string();
        round_trips(RunCall::Stop { run }, serde_json::json!({ "area": "runs", "call": "stop", "run": id }));
        round_trips(
            RunCall::Wake { run, node: "lp.ask".into(), request: WakeRequest { iteration: Some("3.0".into()) } },
            serde_json::json!({ "area": "runs", "call": "wake", "run": id, "node": "lp.ask", "request": { "iteration": "3.0" } }),
        );
        round_trips(
            RunCall::Logs { run, request: LogsRequest { limit: Some(20) } },
            serde_json::json!({ "area": "runs", "call": "logs", "run": id, "request": { "limit": 20 } }),
        );
        round_trips(
            RunCall::Events { run, request: EventsRequest { node: Some("one".into()), kind: Some("node_failed".into()), iteration: None } },
            serde_json::json!({ "area": "runs", "call": "events", "run": id, "request": { "node": "one", "kind": "node_failed" } }),
        );
        round_trips(
            RunCall::Outputs { run, request: OutputsRequest { expect: Some("a,b".into()) } },
            serde_json::json!({ "area": "runs", "call": "outputs", "run": id, "request": { "expect": "a,b" } }),
        );
        round_trips(RunCall::Detail { run }, serde_json::json!({ "area": "runs", "call": "detail", "run": id }));
        round_trips(RunCall::Resolve { prefix: "0000".into() }, serde_json::json!({ "area": "runs", "call": "resolve", "prefix": "0000" }));
        round_trips(RunCall::Delete { run }, serde_json::json!({ "area": "runs", "call": "delete", "run": id }));
        for call in [RunCall::Stop { run }, RunCall::Delete { run }, RunCall::Detail { run }] {
            assert!(!call.takes_down(), "{call:?} names its run exactly");
        }
    }

    /// A listing's filters sit beside its tags, camelCase, and only the
    /// ones set are on the wire.
    #[test]
    fn a_listing_round_trips_with_every_filter() {
        let list = RunList {
            filter: RunFilter { tag: Some("draft".into()), ..RunFilter::default() },
            limit: 20,
            offset: 5,
            phase: Some(crate::primitive::Phase::Fire),
            started_after: Some(10),
            started_before: Some(20),
            through: Some("one.render".into()),
            search: Some("ada".into()),
            after: Some(PageMark { started_at: 15, run: RunId::from_u128(1) }),
        };
        round_trips(
            RunCall::List(list),
            serde_json::json!({
                "area": "runs", "call": "list", "filter": { "tag": "draft" }, "limit": 20, "offset": 5, "phase": "fire",
                "startedAfter": 10, "startedBefore": 20, "through": "one.render", "search": "ada",
                "after": { "startedAt": 15, "run": RunId::from_u128(1).to_string() }
            }),
        );
    }

    #[test]
    fn a_stop_says_when_the_run_had_already_ended() {
        let ended = RunStopped { already_ended: Some(RunStatus::Completed) };
        assert_eq!(serde_json::to_value(ended).unwrap(), serde_json::json!({ "alreadyEnded": "completed" }));
        assert_eq!(serde_json::to_value(RunStopped { already_ended: None }).unwrap(), serde_json::json!({}));
    }
}
