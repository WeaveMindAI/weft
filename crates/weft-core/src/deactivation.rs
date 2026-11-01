//! What `POST /projects/{id}/deactivate` and `/resync` read and answer,
//! and what every command that takes triggers down says it did. ONE
//! definition for the dispatcher and the CLI.

use serde::{Deserialize, Serialize};

use crate::activation::{ActivationScope, ActivationUrl, OwnerTriggers};
use crate::builds::BuildHashes;
use crate::instance::{InstanceId, Owner};
use crate::take_down::{Held, Running, TakeDown, TakeDownChoice};

/// What a take-down did, as every command that takes triggers down
/// answers it and prints it: the answers it acted on, the triggers it
/// took down, what is held on them, and what became of the runs going.
// SYNC: TakeDownReport <-> packages/weft-graph/src/protocol.ts TakeDownReport
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TakeDownReport {
    pub take_down: TakeDown,
    /// The triggers it took down, by owner. Empty when none it named was
    /// on.
    pub triggers: Vec<OwnerTriggers>,
    /// The work held on them: kept for when they are back, or (with
    /// `held: drop`) what was dropped.
    pub held: crate::projects::HeldWork,
    /// Runs going right now that it reached: cancelled (`running:
    /// cancel`), or left to finish on their own version, the command going
    /// on once they have (`wait`).
    pub runs: u64,
}

impl TakeDownReport {
    /// One line a person reads: "took down 'door', 'feed': the door holds
    /// what arrives; 3 arrivals held, 1 paused run kept; 2 runs waited
    /// for, finishing on their own version".
    pub fn describe(&self) -> String {
        if self.triggers.is_empty() {
            return "no trigger it names was on, so nothing was taken down".into();
        }
        let held = match (self.take_down.held, self.held.is_empty()) {
            (_, true) => "nothing held".to_string(),
            (Held::Keep, false) => format!("{} kept", self.held.describe()),
            (Held::Drop, false) => format!("{} dropped", self.held.describe()),
        };
        let runs = match (self.runs, self.take_down.running) {
            (0, _) => "no run going".to_string(),
            (n, Running::Wait) => format!("{} waited for, finishing on their own version", runs_counted(n)),
            (n, Running::Cancel) => format!("{} cancelled", runs_counted(n)),
        };
        format!("took down {}: {}; {held}; {runs}", OwnerTriggers::list_words(&self.triggers), self.take_down.door_words())
    }
}

/// "1 run" / "3 runs".
fn runs_counted(n: u64) -> String {
    if n == 1 { "1 run".into() } else { format!("{n} runs") }
}

/// What `POST /projects/{id}/deactivate` answers: what it did, and which
/// instances still have a trigger on afterwards, so a deactivate of the
/// shared triggers can say that instances' are still listening and how to
/// take them down too.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeactivateResponse {
    pub report: TakeDownReport,
    pub instances_still_on: Vec<InstanceId>,
    /// Under `running: wait`, with runs left going: the wait for them,
    /// followed in short looks (`GET /projects/{id}/follow/{task}`,
    /// [`Following`]) until they have ended.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiting: Option<uuid::Uuid>,
}

/// What `POST /projects/{id}/resync` (and `/resync/resume`) answers: done
/// at once (nothing behind), or under way, followed in short looks
/// (`GET /projects/{id}/follow/{task}`) until it ends, so no request has to
/// stay open for as long as it waits on the old version's runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ResyncAnswer {
    Done(ResyncResponse),
    UnderWay {
        task: uuid::Uuid,
        /// What the install's maintenance had left down in the project and
        /// the resync brought back first ([`ResyncResponse::brought_back`]).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        brought_back: Option<crate::maintenance::ProjectLifted>,
    },
}

/// What one look at a task a person follows answers
/// (`GET /projects/{id}/follow/{task}?wait_ms=<n>`): still going after
/// waiting at most that long, with where it last said it is when it says
/// (in its kind's own shape), or what it ended with. One that failed is
/// answered as the refusal it ended with.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Following {
    Going(Option<serde_json::Value>),
    Done(serde_json::Value),
}

/// `POST /projects/{id}/deactivate`: the take-down's answers and which
/// activations: the named triggers (every one when none is) of the
/// owners named (the shared ones when none is).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct DeactivateRequest {
    #[serde(flatten)]
    pub choice: TakeDownChoice,
    #[serde(default)]
    pub scope: ActivationScope,
}

/// `POST /projects/{id}/resync`: the build it brings live (the hashes the
/// caller built), how the triggers it refreshes go down for the swap, and
/// which: the named triggers (every one when none is) of the owners named
/// (the shared ones when none is), of which only those on and behind the
/// build are refreshed. They come back at once on the new build.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ResyncRequest {
    #[serde(flatten)]
    pub build: BuildHashes,
    #[serde(flatten)]
    pub choice: TakeDownChoice,
    #[serde(default)]
    pub scope: ActivationScope,
}

/// What a resync answers: what its take-down did, the addresses its
/// triggers answer on again, whose it refreshed in order, and the
/// triggers of the new build that are off, which a resync never turns on
/// (`weft activate --trigger` does).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResyncResponse {
    pub report: TakeDownReport,
    pub urls: Vec<ActivationUrl>,
    pub resynced: Vec<Owner>,
    /// Paused runs of the refreshed triggers moved onto the new build:
    /// they resume on it.
    pub moved_runs: u64,
    /// Triggers of the new build that are off, by owner.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub off: Vec<OwnerTriggers>,
    /// What the install's maintenance took down in the project and could
    /// not bring back then (its code did not build on the new weft), brought
    /// back first on this build: what came back, and what still did not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub brought_back: Option<crate::maintenance::ProjectLifted>,
}

/// Where a resync is, as its task says while it runs (its task's
/// progress), and what `weft status` shows of a resync under way. Its
/// task says it each time the runs it waits for change, and says none
/// once it waits for no more.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResyncProgress {
    /// The runs going on the old version, oldest first: the new version
    /// takes over once they end. A run a worker took at its door and has
    /// not put on record yet is waited for too, and is not named here.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub waits_for: Vec<RunGoing>,
}

/// Where a resync is, as its follower is answered (the `going` answer of
/// `/projects/{id}/follow/{task}`): what its task said, and the
/// activation it drives now, read off the activation rows it claimed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResyncGoing {
    #[serde(flatten)]
    pub progress: ResyncProgress,
    /// While it turns an owner's triggers back on: since when, and what
    /// that activation waits on now ("its setup run <id>").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turning_on: Option<crate::infra::wire::ChangeProgress>,
}

/// A run going, since `since_unix` (when it started, on the install's
/// clock).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunGoing {
    pub run_id: crate::RunId,
    pub since_unix: i64,
}

/// How a person reads one owner's triggers: "the program's triggers" or
/// "instance 'ada''s triggers".
pub fn whose_triggers(owner: &Owner) -> String {
    match owner {
        Owner::Shared => "the program's triggers".to_string(),
        Owner::Instance(instance) => format!("instance '{instance}''s triggers"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instance::InstanceTarget;
    use crate::take_down::Door;

    /// What a resync waits for reads back, camelCase on the wire, and a
    /// resync that waits for nothing says nothing.
    #[test]
    fn where_a_resync_is_reads_back() {
        let progress = ResyncProgress { waits_for: vec![RunGoing { run_id: uuid::Uuid::nil(), since_unix: 7 }] };
        let wire = serde_json::to_value(&progress).unwrap();
        assert_eq!(wire["waitsFor"][0]["sinceUnix"], 7);
        assert_eq!(serde_json::from_value::<ResyncProgress>(wire).unwrap(), progress);
        assert_eq!(serde_json::to_value(ResyncProgress::default()).unwrap(), serde_json::json!({}));
        // A follower is answered what the task said, flat, beside what it
        // turns on now.
        let going = ResyncGoing {
            progress,
            turning_on: Some(crate::infra::wire::ChangeProgress { since_unix: 8, as_of_unix: 9, waiting: Some("its setup run r1".into()) }),
        };
        let wire = serde_json::to_value(&going).unwrap();
        assert_eq!((wire["waitsFor"][0]["sinceUnix"].as_i64(), wire["turningOn"]["waiting"].as_str()), (Some(7), Some("its setup run r1")));
        assert_eq!(serde_json::from_value::<ResyncGoing>(wire).unwrap(), going);
    }

    #[test]
    fn a_deactivate_flattens_its_answers_beside_its_scope() {
        let body: DeactivateRequest = serde_json::from_value(serde_json::json!({
            "door": "hide", "running": "cancel", "scope": { "instances": "all_instances" }
        }))
        .unwrap();
        assert_eq!(body.choice.door, Some(Door::Hide));
        assert_eq!(body.choice.held, None, "left to the command's default");
        assert_eq!(body.scope.instances, InstanceTarget::EveryInstance);
        let wire = serde_json::to_value(&body).unwrap();
        assert_eq!(wire, serde_json::json!({ "door": "hide", "running": "cancel", "scope": { "instances": "all_instances" } }));
    }

    #[test]
    fn a_resync_names_its_build_and_its_answers() {
        let body = ResyncRequest {
            build: BuildHashes { binary_hash: Some("b".into()), ..Default::default() },
            choice: TakeDownChoice { held: Some(Held::Drop), ..Default::default() },
            scope: ActivationScope::default(),
        };
        let wire = serde_json::to_value(&body).unwrap();
        assert_eq!(wire["binaryHash"], "b");
        assert_eq!(wire["held"], "drop");
        let back: ResyncRequest = serde_json::from_value(wire).unwrap();
        assert_eq!(back.choice.resolve().unwrap().held, Held::Drop);
    }

    #[test]
    fn a_report_reads_as_what_was_done() {
        let ada = Owner::Instance(InstanceId::new("ada").unwrap());
        let report = TakeDownReport {
            take_down: TakeDown::DEFAULT,
            triggers: vec![
                OwnerTriggers { owner: Owner::Shared, triggers: vec!["door".into(), "feed".into()] },
                OwnerTriggers { owner: ada, triggers: vec!["inbox".into()] },
            ],
            held: crate::projects::HeldWork { arrivals: 3, paused: 1 },
            runs: 2,
        };
        assert_eq!(
            report.describe(),
            "took down 'door', 'feed'; 'inbox' of instance 'ada': the door holds what arrives; \
             3 arrivals and 1 paused run kept; 2 runs waited for, finishing on their own version"
        );
        let nothing = TakeDownReport { triggers: Vec::new(), ..report };
        assert!(nothing.describe().contains("nothing was taken down"));
    }

    #[test]
    fn each_owner_reads_as_whose_triggers() {
        assert_eq!(whose_triggers(&Owner::Shared), "the program's triggers");
        let ada = Owner::Instance(InstanceId::new("ada").unwrap());
        assert_eq!(whose_triggers(&ada), "instance 'ada''s triggers");
    }
}
