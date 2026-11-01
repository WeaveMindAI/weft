//! A program's calls on projects themselves (`ctx.projects()`,
//! `ctx.project(..)`): the owner's list of them, making a new one, one's
//! status, cancelling its build, removing it, and one look at a task a
//! command left to follow.

use serde::{Deserialize, Serialize};

use crate::projects::{DeclareRequest, QuiesceRequest, StatusRequest};

/// One call on a project, each carrying what its command sends (`weft
/// ps`, `weft status`, `weft cancel-build`, `weft rm`).
// SYNC: ProjectCall <-> crates/weft-dispatcher/src/actions/project.rs (`program_call`'s match)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "call", rename_all = "snake_case")]
pub enum ProjectCall {
    /// Every project of the owner.
    List,
    /// The project, as the list has it.
    Get,
    /// Make the project the request names, under the owner, with nothing
    /// built yet; one of the owner's already there is left as it is.
    Declare { request: DeclareRequest },
    /// Everything about the project now, drift against the hashes the
    /// request names.
    Status { request: StatusRequest },
    /// Cancel the build in flight.
    CancelBuild,
    /// `weft rm`'s first step: mark the project being removed and take it
    /// down whole, answering the task that waits for what that cancelled.
    Quiesce { request: QuiesceRequest },
    /// Remove the project, once it is quiesced.
    Remove {
        #[serde(default)]
        force: bool,
    },
    /// One look at a task of the project a command left to follow (a
    /// resync, a quiesce's wait): still going, or what it ended with.
    Follow { task: uuid::Uuid },
}

impl ProjectCall {
    pub const JOURNAL_NAMES: &'static [&'static str] = &[
        "weft.projects.list",
        "weft.projects.get",
        "weft.projects.declare",
        "weft.projects.status",
        "weft.projects.cancel_build",
        "weft.projects.quiesce",
        "weft.projects.remove",
        "weft.projects.follow",
    ];

    pub fn journal_name(&self) -> &'static str {
        match self {
            ProjectCall::List => "weft.projects.list",
            ProjectCall::Get => "weft.projects.get",
            ProjectCall::Declare { .. } => "weft.projects.declare",
            ProjectCall::Status { .. } => "weft.projects.status",
            ProjectCall::CancelBuild => "weft.projects.cancel_build",
            ProjectCall::Quiesce { .. } => "weft.projects.quiesce",
            ProjectCall::Remove { .. } => "weft.projects.remove",
            ProjectCall::Follow { .. } => "weft.projects.follow",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::program::ProgramCall;

    fn round_trip(call: ProjectCall) -> serde_json::Value {
        let wrapped = ProgramCall::Project(call);
        let wire = serde_json::to_value(&wrapped).unwrap();
        assert_eq!(serde_json::from_value::<ProgramCall>(wire.clone()).unwrap(), wrapped);
        assert_eq!(wire["area"], "project");
        wire
    }

    /// Every project call carries its area and call tags, and its request
    /// the way the route reads it.
    #[test]
    fn every_project_call_round_trips_with_its_tags() {
        assert_eq!(round_trip(ProjectCall::List)["call"], "list");
        assert_eq!(round_trip(ProjectCall::Get)["call"], "get");
        let id = uuid::Uuid::now_v7();
        let declare = round_trip(ProjectCall::Declare { request: DeclareRequest { id, name: "helper".into() } });
        assert_eq!((declare["call"].as_str(), declare["request"]["name"].as_str()), (Some("declare"), Some("helper")));
        let status = round_trip(ProjectCall::Status { request: StatusRequest { desired_binary_hash: Some("b".into()), ..Default::default() } });
        assert_eq!((status["call"].as_str(), status["request"]["desiredBinaryHash"].as_str()), (Some("status"), Some("b")));
        assert_eq!(round_trip(ProjectCall::CancelBuild)["call"], "cancel_build");
        let quiesce = round_trip(ProjectCall::Quiesce { request: QuiesceRequest { every_run: true } });
        assert_eq!((quiesce["call"].as_str(), quiesce["request"]["everyRun"].as_bool()), (Some("quiesce"), Some(true)));
        let remove = round_trip(ProjectCall::Remove { force: true });
        assert_eq!((remove["call"].as_str(), remove["force"].as_bool()), (Some("remove"), Some(true)));
        assert_eq!(round_trip(ProjectCall::Follow { task: id })["task"], id.to_string());
    }
}
