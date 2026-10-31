//! A program's calls on a project's infra copies (`ctx.infra(..)` for one
//! node's, `ctx.all_infra()` for what reaches every copy at once), the
//! verbs `weft infra` has, with the same parameters.

use serde::{Deserialize, Serialize};

use crate::infra::wire::LogCursor;
use crate::instance::InstanceId;
use crate::live::LivePress;
use crate::take_down::TakeDownChoice;

/// One call on a project's infra copies.
// SYNC: InfraCall <-> crates/weft-dispatcher/src/actions/infra.rs (`program_call`'s match)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "call", rename_all = "snake_case")]
pub enum InfraCall {
    /// Bring up a copy of an infra node: the shared one, or an instance's
    /// copy of a node that exists once per instance. Answers once the start
    /// is queued; `ctx.infra(..).start()` then waits on
    /// [`InfraCall::Status`] until it runs.
    Start { node: String, instance: Option<InstanceId> },
    /// Scale a copy down, keeping its disk, taking down the triggers whose
    /// cut contains it as `take_down` says.
    Stop {
        node: String,
        instance: Option<InstanceId>,
        take_down: TakeDownChoice,
        /// Bring down the units whose `on_stop` would keep them up
        /// (`weft infra stop --force`).
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        force: bool,
    },
    /// Delete a copy and its disks, keeping the ones its node lists in
    /// `keepOnTerminate` unless `disks` says the owner is going for good.
    Terminate { node: String, instance: Option<InstanceId>, take_down: TakeDownChoice, disks: crate::infra::TerminateDisks },
    /// Build nothing and cycle a copy running on another build onto the
    /// registered one (a stop, then a start), taking down the triggers
    /// whose cut contains it as `take_down` says; they come back with it.
    /// A copy already on the registered build, stopped, or missing is left
    /// as it is (`weft infra upgrade`).
    Upgrade { node: String, instance: Option<InstanceId>, take_down: TakeDownChoice },
    /// One copy's state, or none when it was never started (or was
    /// terminated).
    Status { node: String, instance: Option<InstanceId> },
    /// Every copy of a node that exists: the shared one and each
    /// instance's.
    Copies { node: String },
    /// Halt the infra work under way for one owner's copies: the shared
    /// ones', or `instance`'s (`weft infra cancel`).
    Cancel { instance: Option<InstanceId> },
    /// Ask the supervisor to look at the project's infra again
    /// (`weft infra look`).
    Look,
    /// What the infra's containers wrote (`weft infra logs`).
    Logs { request: InfraLogsRequest },
    /// Every door the project's infra has serving (`weft infra list-doors`).
    Doors,
    /// Whether a lifecycle command a stop, terminate or upgrade issued has
    /// finished, and how; read at once, never held.
    Command { id: i64 },
    /// What a node's card shows (`weft infra show`).
    Show { node: String, instance: Option<InstanceId> },
    /// Press a button the node's card offers (`weft infra press`).
    Press { node: String, instance: Option<InstanceId>, press: LivePress },
    /// Start the node's infra setup again, so its baked outputs are made
    /// again (`weft infra rebake`). Answers once the setup is queued
    /// ([`RebakeAnswer`]); `ctx.infra(..).rebake()` follows it with
    /// [`InfraCall::Setup`].
    Rebake { node: String, instance: Option<InstanceId> },
    /// Where an infra setup run of the project stands ([`SetupEnd`]).
    Setup { run: crate::RunId },
}

impl InfraCall {
    pub const JOURNAL_NAMES: &'static [&'static str] = &[
        "weft.infra.start",
        "weft.infra.stop",
        "weft.infra.terminate",
        "weft.infra.upgrade",
        "weft.infra.status",
        "weft.infra.copies",
        "weft.infra.cancel",
        "weft.infra.look",
        "weft.infra.logs",
        "weft.infra.doors",
        "weft.infra.command",
        "weft.infra.show",
        "weft.infra.press",
        "weft.infra.rebake",
        "weft.infra.setup",
    ];

    pub fn journal_name(&self) -> &'static str {
        match self {
            InfraCall::Start { .. } => "weft.infra.start",
            InfraCall::Stop { .. } => "weft.infra.stop",
            InfraCall::Terminate { .. } => "weft.infra.terminate",
            InfraCall::Upgrade { .. } => "weft.infra.upgrade",
            InfraCall::Status { .. } => "weft.infra.status",
            InfraCall::Copies { .. } => "weft.infra.copies",
            InfraCall::Cancel { .. } => "weft.infra.cancel",
            InfraCall::Look => "weft.infra.look",
            InfraCall::Logs { .. } => "weft.infra.logs",
            InfraCall::Doors => "weft.infra.doors",
            InfraCall::Command { .. } => "weft.infra.command",
            InfraCall::Show { .. } => "weft.infra.show",
            InfraCall::Press { .. } => "weft.infra.press",
            InfraCall::Rebake { .. } => "weft.infra.rebake",
            InfraCall::Setup { .. } => "weft.infra.setup",
        }
    }

    pub fn takes_down(&self) -> bool {
        matches!(self, InfraCall::Stop { .. } | InfraCall::Terminate { .. } | InfraCall::Upgrade { .. })
    }
}

/// `GET /projects/{id}/infra/logs` and [`InfraCall::Logs`]: which
/// containers, and from where.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InfraLogsRequest {
    /// One infra placement (`db`, `one.db`); every one when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    /// The last lines of each container.
    #[serde(default = "InfraLogsRequest::default_tail")]
    pub tail: usize,
    /// A follower's cursor, as the previous answer gave it: only the lines
    /// written after it, `tail` ignored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<LogCursor>,
}

impl InfraLogsRequest {
    pub fn default_tail() -> usize {
        100
    }
}

impl Default for InfraLogsRequest {
    fn default() -> Self {
        Self { node: None, tail: Self::default_tail(), after: None }
    }
}

/// What [`InfraCall::Cancel`] answers: whether there was infra work under
/// way to halt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct InfraCancelAnswer {
    pub cancelled: bool,
}

/// What [`InfraCall::Rebake`] answers: the setup run that makes the bake
/// again; none yet because another setup of the same copies is under way,
/// which `ctx.infra(..).rebake()` waits out before it asks again; or none
/// at all, the node's setup having nothing to run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "answer", rename_all = "snake_case")]
pub enum RebakeAnswer {
    Started { run: crate::RunId },
    Waiting { reason: String },
    NothingToSetUp,
}

/// Where an infra setup run stands ([`InfraCall::Setup`]): still going, or
/// how it ended, with the reason its run recorded for a failure or a
/// cancel (none when the run recorded none).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "end", rename_all = "snake_case")]
pub enum SetupEnd {
    Going,
    Completed,
    Failed {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    Cancelled {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::program::ProgramCall;

    fn round_trip(call: InfraCall, tag: &str) -> serde_json::Value {
        let call = ProgramCall::Infra(call);
        let wire = serde_json::to_value(&call).unwrap();
        assert_eq!((&wire["area"], &wire["call"]), (&serde_json::json!("infra"), &serde_json::json!(tag)), "{wire}");
        assert_eq!(serde_json::from_value::<ProgramCall>(wire.clone()).unwrap(), call);
        wire
    }

    #[test]
    fn every_new_infra_call_round_trips_under_its_tag() {
        let ada = || Some(InstanceId::new("ada").unwrap());
        round_trip(InfraCall::Cancel { instance: ada() }, "cancel");
        round_trip(InfraCall::Look, "look");
        round_trip(InfraCall::Doors, "doors");
        round_trip(InfraCall::Command { id: 7 }, "command");
        round_trip(InfraCall::Show { node: "one.db".into(), instance: ada() }, "show");
        round_trip(InfraCall::Rebake { node: "db".into(), instance: None }, "rebake");
        round_trip(InfraCall::Setup { run: crate::RunId::from_u128(3) }, "setup");
        let press = round_trip(
            InfraCall::Press { node: "bridge".into(), instance: ada(), press: LivePress { kind: "logout".into(), payload: serde_json::json!({ "a": 1 }) } },
            "press",
        );
        assert_eq!(press["press"], serde_json::json!({ "kind": "logout", "payload": { "a": 1 } }));
        let logs = round_trip(InfraCall::Logs { request: InfraLogsRequest { node: Some("db".into()), tail: 5, after: Some(LogCursor::default()) } }, "logs");
        assert_eq!(logs["request"], serde_json::json!({ "node": "db", "tail": 5, "after": {} }));
    }

    #[test]
    fn a_logs_request_reads_its_defaults_and_camel_case_answers() {
        assert_eq!(serde_json::from_value::<InfraLogsRequest>(serde_json::json!({})).unwrap(), InfraLogsRequest::default());
        let rebake = RebakeAnswer::Started { run: crate::RunId::from_u128(9) };
        let wire = serde_json::to_value(&rebake).unwrap();
        assert_eq!(wire["answer"], "started");
        assert_eq!(serde_json::from_value::<RebakeAnswer>(wire).unwrap(), rebake);
        let waiting = serde_json::to_value(RebakeAnswer::Waiting { reason: "busy".into() }).unwrap();
        assert_eq!(waiting, serde_json::json!({ "answer": "waiting", "reason": "busy" }));
        assert_eq!(serde_json::to_value(InfraCancelAnswer { cancelled: true }).unwrap(), serde_json::json!({ "cancelled": true }));
    }

    #[test]
    fn a_setup_end_carries_its_reason_under_its_tag() {
        let cases = [
            (SetupEnd::Going, serde_json::json!({ "end": "going" })),
            (SetupEnd::Completed, serde_json::json!({ "end": "completed" })),
            (SetupEnd::Failed { reason: Some("the extension is missing".into()) }, serde_json::json!({ "end": "failed", "reason": "the extension is missing" })),
            (SetupEnd::Failed { reason: None }, serde_json::json!({ "end": "failed" })),
            (SetupEnd::Cancelled { reason: Some("Cancelled by user".into()) }, serde_json::json!({ "end": "cancelled", "reason": "Cancelled by user" })),
            (SetupEnd::Cancelled { reason: None }, serde_json::json!({ "end": "cancelled" })),
        ];
        for (end, wire) in cases {
            assert_eq!(serde_json::to_value(&end).unwrap(), wire);
            assert_eq!(serde_json::from_value::<SetupEnd>(wire).unwrap(), end);
        }
    }
}
