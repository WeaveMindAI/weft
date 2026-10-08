//! What the install's infra endpoints read and answer, and the CLI sends
//! and reads: one Rust type per message, so the two ends cannot drift.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::instance::InstanceId;
use crate::running_policy::{DeactivateSpec, RunningChoice};

/// `GET /projects/{id}/infra/doors`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DoorsResponse {
    /// Every door a copy has serving, at the address its host gave it.
    pub doors: Vec<Door>,
    /// Copies still being applied: their doors (if they declare any) get
    /// an address only once the apply lands, so they are named here
    /// rather than left out as if they had none.
    pub applying: Vec<CopyRef>,
}

/// One copy of an infra node: the shared one, or an instance's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CopyRef {
    /// The node as the program spells it.
    pub node: String,
    /// Which instance's copy: absent for the shared one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<InstanceId>,
}

/// One door serving right now.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Door {
    #[serde(flatten)]
    pub copy: CopyRef,
    pub endpoint: String,
    /// Where the door answers, exactly as its host wrote it (`host:port`):
    /// the machine's loopback on a local install, the unit's private
    /// address on the install's network on a cloud one. Not a URL: a door
    /// carries whatever protocol the endpoint speaks, and most of them
    /// are not HTTP.
    pub address: String,
}

/// One line a container wrote, with the instant its runtime stamped on
/// it (nanosecond precision, what `docker logs --timestamps` prints) and
/// the pipe it came out of.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogLine {
    pub at: DateTime<Utc>,
    pub pipe: Pipe,
    pub text: String,
}

/// Which of a container's two outputs a line was written to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Pipe {
    Stdout,
    Stderr,
}

/// Which lines of a unit's containers a read wants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogsFrom {
    /// The last N lines of each container.
    Tail(usize),
    /// Every line after the mark of its container (keyed by
    /// `LogStream::source`); all of a container that has no mark yet
    /// (one that appeared since the follow started).
    After(BTreeMap<String, LogMark>),
}

/// Where a follower stopped reading one container. A timestamp alone is
/// not a cursor: one write of several lines stamps them all with the same
/// instant, so the mark also counts how many lines AT that instant were
/// already delivered. Counted per pipe: the runtime hands stdout and
/// stderr back apart, so where the lines of one instant fall between the
/// two is not an order that holds from one read to the next.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogMark {
    pub at: DateTime<Utc>,
    pub stdout_seen: usize,
    pub stderr_seen: usize,
}

impl LogMark {
    /// The mark of a read that delivered nothing yet, taken at `at` by the
    /// clock that stamps the lines (the container runtime's, never the
    /// caller's: a line stamped by a clock running behind the caller's
    /// would fall before a caller-made mark and be dropped).
    pub fn start(at: DateTime<Utc>) -> Self {
        LogMark { at, stdout_seen: 0, stderr_seen: 0 }
    }

    fn seen(&mut self, pipe: Pipe) -> &mut usize {
        match pipe {
            Pipe::Stdout => &mut self.stdout_seen,
            Pipe::Stderr => &mut self.stderr_seen,
        }
    }

    /// The lines of `lines` (in the order written, read from `self.at`
    /// inclusive) that were not delivered yet.
    pub fn unseen(&self, lines: Vec<LogLine>) -> Vec<LogLine> {
        let mut skip = *self;
        lines
            .into_iter()
            .filter(|l| {
                if l.at < self.at {
                    return false;
                }
                let skip = skip.seen(l.pipe);
                if l.at == self.at && *skip > 0 {
                    *skip -= 1;
                    return false;
                }
                true
            })
            .collect()
    }

    /// The mark after delivering `delivered` on top of `self` (the
    /// previous mark, or `LogMark::start` of this read). Every read yields
    /// a mark, even one that delivered nothing: a follower whose first
    /// read was empty (`--tail 0`, a quiet container) must next ask for
    /// what came after that read, never for the whole history.
    pub fn advance(self, delivered: &[LogLine]) -> LogMark {
        let Some(last) = delivered.last() else {
            return self;
        };
        let mut next = if self.at == last.at { self } else { LogMark::start(last.at) };
        for l in delivered.iter().filter(|l| l.at == last.at) {
            *next.seen(l.pipe) += 1;
        }
        next
    }
}

/// What one container of a unit wrote, and the mark a follower resumes
/// from (made by the host that owns the container's clock).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogStream {
    /// The container, as its unit names it.
    pub source: String,
    pub lines: Vec<LogLine>,
    pub mark: LogMark,
}

/// `GET /projects/{id}/infra/logs`: each container's lines, headed by
/// what wrote them, plus the cursor a follower hands back as `after` to
/// get exactly what came next.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InfraLogs {
    pub blocks: Vec<LogBlock>,
    pub cursor: LogCursor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogBlock {
    #[serde(flatten)]
    pub copy: CopyRef,
    pub unit: String,
    pub stream: LogStream,
}

/// Every container's mark, keyed by `LogCursor::key`. Opaque to the
/// CLI: it only hands it back.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogCursor(pub BTreeMap<String, LogMark>);

impl LogCursor {
    /// One container of one unit of one copy (`copy_id` is the copy's
    /// stable id, so two instances' copies never share a key).
    pub fn key(copy_id: &str, unit: &str, source: &str) -> String {
        format!("{copy_id}/{unit}/{source}")
    }

    /// The marks of one unit, keyed by source, as a host reads them.
    pub fn for_unit(&self, copy_id: &str, unit: &str) -> BTreeMap<String, LogMark> {
        let prefix = format!("{copy_id}/{unit}/");
        self.0
            .iter()
            .filter_map(|(k, m)| k.strip_prefix(&prefix).map(|source| (source.to_string(), *m)))
            .collect()
    }
}

/// A shared infra place with no copy and nothing starting one, in the
/// project status.
// SYNC: INFRA_NOT_STARTED <-> packages/weft-graph/src/protocol.ts InfraPlacementStatus.status
pub const INFRA_NOT_STARTED: &str = "not_started";
/// A `@per_instance` infra place in the project status: it has no shared
/// copy, only its instances'.
// SYNC: INFRA_PER_INSTANCE <-> packages/weft-graph/src/protocol.ts InfraPlacementStatus.status
pub const INFRA_PER_INSTANCE: &str = "per_instance";

/// `POST /projects/{id}/infra/sync`: bring the infra up on a build.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SyncRequest {
    /// The build the client just made (`POST /projects/{id}/builds`).
    /// Named, the sync applies it and refuses when the registered build
    /// is another (a teammate's landed between the two). Absent (a
    /// program starting an instance's copy, which builds nothing), it
    /// applies the registered build. The images every place runs come
    /// with the build, never from here.
    #[serde(flatten)]
    pub build: crate::builds::BuildHashes,
    /// How the worker reconciliation inside sync (and an upgrade's
    /// stop leg) treats executions still running on an older image once
    /// a new one went live. `cancel` (the default) cancels them; `wait`
    /// lets them finish up to `drainTimeoutSecs`, then cancels what is
    /// left. Never a silent kill.
    /// On an upgrade, outranked by `triggerDeactivation`'s answer when
    /// that picker was shown.
    #[serde(flatten)]
    pub running: RunningChoice,
    /// Whose copies: an instance's copies of the nodes marked
    /// `@per_instance`, or (absent) the shared nodes. `weft infra start
    /// --instance`, and a program's `ctx.infra(..).instance(..).start()`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<InstanceId>,
    /// Only these infra nodes (by place), every one of the owner's kind
    /// when empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nodes: Vec<String>,
}

/// `POST /projects/{id}/infra/upgrade`: cycle the running infra onto the
/// current specs (the triggers reading it taken down per
/// `triggerDeactivation`, then a stop leg, then the start). What to run
/// is the sync body's.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UpgradeRequest {
    #[serde(flatten)]
    pub sync: SyncRequest,
    /// How to deactivate the triggers reading this infra when one is on
    /// (required then: 428 with the trigger-choice header otherwise).
    /// Same `DeactivateSpec` shape as the standalone `/deactivate`
    /// endpoint, so clients reuse one picker.
    #[serde(default, rename = "triggerDeactivation", skip_serializing_if = "Option::is_none")]
    pub trigger_deactivation: Option<DeactivateSpec>,
}

/// `POST /projects/{id}/infra/stop` and `/infra/terminate`. Carries the
/// trigger-deactivation choice when the project is Active (the same
/// picker as the standalone Deactivate verb, and its answer governs
/// the running executions), and the running-work choice on its own
/// for when it is not: an inactive project can still have executions
/// running on this infra, and `wait` lets them land before the
/// supervisor scales it down.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StopRequest {
    #[serde(default, rename = "triggerDeactivation", skip_serializing_if = "Option::is_none")]
    pub trigger_deactivation: Option<DeactivateSpec>,
    #[serde(flatten)]
    pub running: RunningChoice,
    /// Whose copies: one instance's (`--instance`), or (absent) the shared
    /// ones.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<InstanceId>,
}

/// `POST /projects/{id}/infra/nodes/{node}/stop` and `/terminate`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PerNodeRequest {
    /// What happens to the running executions this copy can reach.
    /// Which ones use this one instance is not recorded, so for the
    /// shared copy `cancel` (the default) ends every running execution
    /// of the project, and for an instance's copy every run of that
    /// instance; `wait` lets the same set land first.
    #[serde(flatten)]
    pub running: RunningChoice,
    /// Stop only: force scale-to-zero every unit, ignoring `on_stop`.
    /// Lets the user take down a unit that would normally stay up
    /// (NoOp) so they can update it on the next start. Ignored by
    /// terminate (terminate already removes everything).
    #[serde(default)]
    pub force: bool,
    /// Whose copy of the node: an instance's copy of a per-instance node,
    /// or (absent) the shared one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<InstanceId>,
}

/// What a sync answers, and `GET /projects/{id}/infra/status`: every
/// copy that exists.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraStatus {
    pub nodes: Vec<InfraStatusEntry>,
    /// weft's own calls this infra's work waits on that keep failing (a
    /// supervisor that cannot be woken never starts it). Empty when every
    /// one goes through.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unanswered: Vec<crate::projects::Unanswered>,
}

impl InfraStatus {
    /// What the host runs differently from what each copy asked (a GPU
    /// kind a local install cannot choose), across every copy.
    pub fn host_notes(&self) -> impl Iterator<Item = &str> {
        self.nodes.iter().flat_map(|entry| entry.notes.iter().map(String::as_str))
    }
}

/// One copy of an infra node, and its state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraStatusEntry {
    /// The node's placement, spelled the way a person writes the node
    /// (`db`, `one.db`): what a person is shown, what the editor matches
    /// against its canvas, and what every per-node verb takes.
    pub node: String,
    /// Whose copy: absent for the shared one, else the instance's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<InstanceId>,
    pub status: String,
    pub endpoint_url: Option<String>,
    /// Endpoint name to the address a caller outside the install uses,
    /// for each `Public` endpoint (the same address
    /// `ctx.endpoint(name)?.public_url()` gives the node).
    pub public_urls: BTreeMap<String, String>,
    pub failure_stage: Option<String>,
    pub failure_message: Option<String>,
    /// What the host runs differently from what was asked (a GPU kind it
    /// cannot choose), one plain sentence each; the CLI prints them as
    /// warnings. Empty when the copy runs as asked.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
    /// How far a start, stop or terminate of this copy got, while one is
    /// under way.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<ChangeProgress>,
}

/// How far a change of an infra copy got (a start, a stop or a
/// terminate): since when it runs, and what it waits on right now ("its
/// machine's agent does not answer yet: ...", "its host is taking it
/// down"). `waiting` is absent before anything reports (the machine is
/// still being made). `since_unix` is when the change was asked, and
/// `as_of_unix` the server's clock when it answered: both on one clock,
/// so how long it has run never depends on the reader's own.
// SYNC: ChangeProgress <-> packages/weft-graph/src/protocol.ts ChangeProgress
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeProgress {
    #[serde(rename = "sinceUnix")]
    pub since_unix: i64,
    #[serde(rename = "asOfUnix")]
    pub as_of_unix: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiting: Option<String>,
}

impl ChangeProgress {
    /// As a person reads it, `now_unix` being the time it is read, on the
    /// clock `since_unix` is on (`as_of_unix` reads it as of the answer):
    /// "for 3m12s, waiting on: ...".
    // SYNC: describe <-> packages/weft-graph/src/status.ts describeProgress
    pub fn describe(&self, now_unix: i64) -> String {
        let secs = (now_unix - self.since_unix).max(0);
        let elapsed = if secs >= 60 { format!("{}m{:02}s", secs / 60, secs % 60) } else { format!("{secs}s") };
        match &self.waiting {
            Some(waiting) => format!("for {elapsed}, waiting on: {waiting}"),
            None => format!("for {elapsed}"),
        }
    }
}

#[cfg(test)]
mod change_progress_tests {
    use super::ChangeProgress;

    #[test]
    fn progress_reads_as_how_long_and_on_what() {
        let p = ChangeProgress { since_unix: 100, as_of_unix: 292, waiting: Some("db: its machine's agent does not answer yet".into()) };
        assert_eq!(p.describe(p.as_of_unix), "for 3m12s, waiting on: db: its machine's agent does not answer yet");
        let bare = ChangeProgress { since_unix: 100, as_of_unix: 130, waiting: None };
        assert_eq!(bare.describe(bare.as_of_unix), "for 30s");
    }

    /// Read as of the answer, the duration is the server's alone: a
    /// reader whose clock runs 20s behind the server's still reads 20s.
    #[test]
    fn progress_as_of_the_answer_ignores_the_readers_clock() {
        let p = ChangeProgress { since_unix: 1_000, as_of_unix: 1_020, waiting: None };
        assert_eq!(p.describe(p.as_of_unix), "for 20s");
        assert_eq!(p.describe(990), "for 0s", "a time before the start reads as no time at all");
    }

    #[test]
    fn progress_travels_as_camel_case() {
        let p = ChangeProgress { since_unix: 1, as_of_unix: 2, waiting: None };
        assert_eq!(serde_json::to_value(&p).unwrap(), serde_json::json!({ "sinceUnix": 1, "asOfUnix": 2 }));
        assert!(
            serde_json::from_value::<ChangeProgress>(serde_json::json!({ "sinceUnix": 1 })).is_err(),
            "an answer without its clock is refused"
        );
    }
}

/// What a verb that enqueues a lifecycle command answers (202). No
/// `nodes`: the command has not been claimed yet, so any snapshot would
/// be the state before the action. Clients follow the command
/// (`/infra/commands/{id}`) for its outcome.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleCommandIssued {
    pub command_id: i64,
}

/// `GET /projects/{id}/infra/commands/{cmd_id}`: whether the command
/// finished, and how.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandStatus {
    /// True once the supervisor marked the command complete.
    pub done: bool,
    /// How it ended, only when `done`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<CommandOutcome>,
    /// Error (on failed) or reason (on cancelled).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// How a finished lifecycle command ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandOutcome {
    Succeeded,
    Failed,
    /// `weft infra cancel` halted it between steps.
    Cancelled,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_status_hands_its_host_notes_over_and_reads_an_entry_without_any() {
        let status: InfraStatus = serde_json::from_value(serde_json::json!({ "nodes": [
            { "node": "llm", "status": "running", "endpoint_url": null, "public_urls": {},
              "failure_stage": null, "failure_message": null, "notes": ["asked for 1 x l4"] },
            { "node": "db", "instance": "ann", "status": "running", "endpoint_url": "http://x", "public_urls": {},
              "failure_stage": null, "failure_message": null },
        ] }))
        .unwrap();
        assert_eq!(status.host_notes().collect::<Vec<_>>(), vec!["asked for 1 x l4"]);
        assert_eq!(status.nodes[1].instance.as_ref().map(InstanceId::as_str), Some("ann"));
    }

    #[test]
    fn a_command_outcome_is_one_lower_case_word() {
        let done: CommandStatus =
            serde_json::from_value(serde_json::json!({ "done": true, "outcome": "cancelled", "message": "why" })).unwrap();
        assert_eq!(done.outcome, Some(CommandOutcome::Cancelled));
        let pending = serde_json::to_value(CommandStatus { done: false, outcome: None, message: None }).unwrap();
        assert_eq!(pending, serde_json::json!({ "done": false }));
    }

    #[test]
    fn a_sync_names_only_what_it_was_given() {
        let wire = serde_json::to_value(SyncRequest {
            build: crate::builds::BuildHashes { infra_hash: Some("i".into()), ..Default::default() },
            ..Default::default()
        }).unwrap();
        assert_eq!(wire, serde_json::json!({ "infraHash": "i" }));
        let upgrade: UpgradeRequest = serde_json::from_value(serde_json::json!({
            "infraHash": "i", "triggerDeactivation": { "mode": "park", "runningPolicy": "wait" }
        }))
        .unwrap();
        assert_eq!(upgrade.sync.build.infra_hash.as_deref(), Some("i"));
        assert!(upgrade.trigger_deactivation.is_some());
    }

    fn at(nanos: u32) -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, nanos).unwrap()
    }

    fn line(nanos: u32, text: &str) -> LogLine {
        LogLine { at: at(nanos), pipe: Pipe::Stdout, text: text.into() }
    }

    fn err_line(nanos: u32, text: &str) -> LogLine {
        LogLine { at: at(nanos), pipe: Pipe::Stderr, text: text.into() }
    }

    fn texts(lines: &[LogLine]) -> Vec<&str> {
        lines.iter().map(|l| l.text.as_str()).collect()
    }

    #[test]
    fn lines_sharing_an_instant_are_neither_repeated_nor_dropped() {
        // First read: a, b, c where b and c came from one write.
        let first = vec![line(1, "a"), line(5, "b"), line(5, "c")];
        let mark = LogMark::start(at(0)).advance(&first);
        assert_eq!(mark, LogMark { at: at(5), stdout_seen: 2, stderr_seen: 0 });
        // Next read starts at the mark's instant inclusive, and a fourth
        // line of that same write arrived since.
        let again = vec![line(5, "b"), line(5, "c"), line(5, "d"), line(9, "e")];
        let fresh = mark.unseen(again);
        assert_eq!(texts(&fresh), ["d", "e"]);
        assert_eq!(mark.advance(&fresh), LogMark { at: at(9), stdout_seen: 1, stderr_seen: 0 });
    }

    #[test]
    fn a_read_ending_on_the_marks_instant_carries_its_count() {
        let mark = LogMark { at: at(5), stdout_seen: 2, stderr_seen: 0 };
        let fresh = mark.unseen(vec![line(5, "b"), line(5, "c"), line(5, "d")]);
        assert_eq!(mark.advance(&fresh), LogMark { at: at(5), stdout_seen: 3, stderr_seen: 0 });
        assert_eq!(mark.advance(&[]), mark);
    }

    /// The two pipes of one instant come back in whatever order the
    /// runtime merges them: a stderr line arriving at an instant whose
    /// stdout lines were delivered is new, however the merge interleaves.
    #[test]
    fn lines_of_one_instant_are_counted_per_pipe() {
        let first = vec![line(5, "out1"), err_line(5, "err1")];
        let mark = LogMark::start(at(0)).advance(&first);
        assert_eq!(mark, LogMark { at: at(5), stdout_seen: 1, stderr_seen: 1 });
        // The next read merges the pipes the other way round, and one
        // more stderr line of that instant arrived.
        let again = vec![err_line(5, "err1"), err_line(5, "err2"), line(5, "out1")];
        assert_eq!(texts(&mark.unseen(again)), ["err2"]);
    }

    #[test]
    fn an_empty_first_read_marks_its_own_start() {
        // `--tail 0`: the first read delivers nothing, and the next one
        // must start at that read, not at the container's first line.
        let mark = LogMark::start(at(5)).advance(&[]);
        assert_eq!(mark, LogMark::start(at(5)));
        let fresh = mark.unseen(vec![line(1, "old"), line(5, "same instant"), line(7, "new")]);
        assert_eq!(texts(&fresh), ["same instant", "new"]);
    }

    #[test]
    fn a_cursor_hands_each_unit_only_its_own_marks() {
        let mark = LogMark::start(at(1));
        let mut cursor = LogCursor::default();
        cursor.0.insert(LogCursor::key("i1", "main", "app"), mark);
        cursor.0.insert(LogCursor::key("i1", "side", "app"), mark);
        assert_eq!(cursor.for_unit("i1", "main").into_keys().collect::<Vec<_>>(), ["app"]);
        assert!(cursor.for_unit("i2", "main").is_empty());
    }
}
