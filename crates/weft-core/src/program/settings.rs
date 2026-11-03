//! A program's calls on a project's settings on the install: its
//! frontends (`ctx.frontends()`), its worker levers (`ctx.workers()`), the
//! connections its own access nodes use (`ctx.picks()`), and the owner's
//! signal tokens (`ctx.signal_tokens()`). Each carries the request the
//! same command's HTTP route takes (`weft frontend`, `weft workers`,
//! `weft connect`, `weft token`).

use serde::{Deserialize, Serialize};

use crate::frontend::{AddFrontendRequest, VisitorAddressRequest};
use crate::picks::{ChangePicks, MovePicks};
use crate::workers::WorkerOverrides;

/// One call on a project's settings.
// SYNC: SettingsCall <-> crates/weft-dispatcher/src/actions/settings.rs (`program_call`'s match)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "call", rename_all = "snake_case")]
pub enum SettingsCall {
    /// Every frontend of the project, by name.
    FrontendsList,
    /// Make a frontend the install hosts (a program cannot make one that
    /// runs elsewhere: its token would be in the answer).
    FrontendAdd { request: AddFrontendRequest },
    /// Remove a frontend: its service, its tokens, its record. With
    /// `force`, what the cloud keeps of it is answered instead of stopping
    /// the removal.
    FrontendRemove {
        name: String,
        #[serde(default)]
        force: bool,
    },
    /// Set or clear where a frontend's server passes the install's doors on
    /// to its visitors.
    FrontendVisitorAddress { name: String, request: VisitorAddressRequest },
    /// The new token `token` of a frontend is in place: every other one of
    /// it stops working.
    FrontendTokenDone { name: String, token: uuid::Uuid },
    /// The new token `token` of a frontend will never be put in place.
    FrontendTokenDrop { name: String, token: uuid::Uuid },
    /// The project's worker levers, the install's, and what its workers
    /// run with.
    WorkersGet,
    /// Replace the project's worker levers (an unset one follows the
    /// install).
    WorkersPut { levers: WorkerOverrides },
    /// Every pick the install keeps for the project.
    PicksList,
    /// Pick and forget, all or none.
    PicksChange { request: ChangePicks },
    /// Carry a place's picks and every instance's values to another place.
    PicksMove { request: MovePicks },
    /// Every signal token of the project's owner, without its value.
    SignalTokensList,
    /// Revoke the signal token `id` of the project's owner.
    SignalTokenRevoke { id: uuid::Uuid },
}

impl SettingsCall {
    pub const NAMES: &'static [&'static str] = &[
        "weft.frontends.list",
        "weft.frontends.add",
        "weft.frontends.remove",
        "weft.frontends.visitor_address",
        "weft.frontends.token_done",
        "weft.frontends.token_drop",
        "weft.workers.get",
        "weft.workers.put",
        "weft.picks.list",
        "weft.picks.change",
        "weft.picks.move",
        "weft.signal_tokens.list",
        "weft.signal_tokens.revoke",
    ];

    pub fn name(&self) -> &'static str {
        match self {
            SettingsCall::FrontendsList => "weft.frontends.list",
            SettingsCall::FrontendAdd { .. } => "weft.frontends.add",
            SettingsCall::FrontendRemove { .. } => "weft.frontends.remove",
            SettingsCall::FrontendVisitorAddress { .. } => "weft.frontends.visitor_address",
            SettingsCall::FrontendTokenDone { .. } => "weft.frontends.token_done",
            SettingsCall::FrontendTokenDrop { .. } => "weft.frontends.token_drop",
            SettingsCall::WorkersGet => "weft.workers.get",
            SettingsCall::WorkersPut { .. } => "weft.workers.put",
            SettingsCall::PicksList => "weft.picks.list",
            SettingsCall::PicksChange { .. } => "weft.picks.change",
            SettingsCall::PicksMove { .. } => "weft.picks.move",
            SettingsCall::SignalTokensList => "weft.signal_tokens.list",
            SettingsCall::SignalTokenRevoke { .. } => "weft.signal_tokens.revoke",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::program::ProgramCall;

    fn round_trip(call: SettingsCall) -> serde_json::Value {
        let call = ProgramCall::Settings(call);
        let wire = serde_json::to_value(&call).unwrap();
        assert_eq!(wire["area"], "settings");
        let back: ProgramCall = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(back, call);
        assert!(!call.takes_down());
        wire
    }

    #[test]
    fn every_settings_call_round_trips_under_its_tags() {
        let token = uuid::Uuid::from_u128(3);
        let add = AddFrontendRequest {
            name: "shop".into(),
            host: crate::frontend::FrontendHost::CloudRun,
            repo: Some(crate::frontend::Repository { name: "ada/shop".into(), id: 42 }),
            visitor_address: Some("https://shop.example/weft".into()),
        };
        let wire = round_trip(SettingsCall::FrontendAdd { request: add });
        assert_eq!(wire["call"], "frontend_add");
        assert_eq!(wire["request"]["visitorAddress"], "https://shop.example/weft");
        assert_eq!(round_trip(SettingsCall::FrontendsList), serde_json::json!({ "area": "settings", "call": "frontends_list" }));
        let wire = round_trip(SettingsCall::FrontendRemove { name: "shop".into(), force: true });
        assert_eq!((&wire["name"], &wire["force"]), (&serde_json::json!("shop"), &serde_json::json!(true)));
        let wire = round_trip(SettingsCall::FrontendVisitorAddress {
            name: "shop".into(),
            request: VisitorAddressRequest { visitor_address: None },
        });
        assert_eq!(wire["request"], serde_json::json!({ "visitorAddress": null }));
        let wire = round_trip(SettingsCall::FrontendTokenDone { name: "shop".into(), token });
        assert_eq!(wire["call"], "frontend_token_done");
        round_trip(SettingsCall::FrontendTokenDrop { name: "shop".into(), token });
        round_trip(SettingsCall::WorkersGet);
        let levers = WorkerOverrides { min_instances: Some(1), cpu: Some("2".into()), ..Default::default() };
        let wire = round_trip(SettingsCall::WorkersPut { levers });
        assert_eq!(wire["levers"], serde_json::json!({ "min_instances": 1, "cpu": "2" }), "levers keep their weft.toml spelling");
        round_trip(SettingsCall::PicksList);
        let change = ChangePicks {
            set: vec![crate::picks::PickInput { node: "llm".into(), field: "key".into(), connection: token, service: "openai".into() }],
            clear: vec![crate::run_spec::InstanceFieldRef { node: "db".into(), field: "url".into() }],
        };
        assert_eq!(round_trip(SettingsCall::PicksChange { request: change })["call"], "picks_change");
        round_trip(SettingsCall::PicksMove { request: MovePicks { from: "a".into(), to: "b".into() } });
        round_trip(SettingsCall::SignalTokensList);
        assert_eq!(round_trip(SettingsCall::SignalTokenRevoke { id: token })["id"], token.to_string());
    }

    #[test]
    fn a_removal_without_force_says_no_force() {
        let call: SettingsCall = serde_json::from_value(serde_json::json!({ "call": "frontend_remove", "name": "shop" })).unwrap();
        assert_eq!(call, SettingsCall::FrontendRemove { name: "shop".into(), force: false });
    }
}
