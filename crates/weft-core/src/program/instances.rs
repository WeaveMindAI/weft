//! A program's calls on a project's instances: what they provide
//! (`ctx.values()`), their connections (`ctx.connections()`), their tokens
//! (`ctx.tokens()`), and the list of them (`ctx.instances()`).

use serde::{Deserialize, Serialize};

use crate::instance::InstanceId;

/// One call on a project's instances.
// SYNC: InstanceCall <-> crates/weft-dispatcher/src/actions/instances.rs (`program_call`'s match)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "call", rename_all = "snake_case")]
pub enum InstanceCall {
    /// An instance's connections.
    ConnectionsList { instance: InstanceId },
    /// Forget every connection of an instance (the values naming them go
    /// with them).
    ConnectionsForget { instance: InstanceId },
    /// What an instance provides for the program's `@instance_filled` fields.
    ValuesGet { instance: InstanceId },
    /// Change what an instance provides: `set` given, `clear` forgotten, all
    /// or none, re-arming the instance's live triggers that read them.
    ValuesChange {
        instance: InstanceId,
        #[serde(default)]
        set: Vec<crate::run_spec::InstanceValueInput>,
        #[serde(default)]
        clear: Vec<crate::run_spec::InstanceFieldRef>,
    },
    /// Forget everything an instance provides (what wiping an instance does),
    /// re-arming its live triggers that read any of it.
    ValuesForget { instance: InstanceId },
    /// Every instance weft holds anything for in the project, with what it
    /// holds ([`super::InstanceHoldings`]).
    List,
    /// Revoke instance tokens: every token of `instance`, or the one `id`.
    TokensRevoke { instance: InstanceId, id: Option<uuid::Uuid> },
}

impl InstanceCall {
    pub const NAMES: &'static [&'static str] = &[
        "weft.connections.list",
        "weft.connections.forget",
        "weft.values.get",
        "weft.values.change",
        "weft.values.forget",
        "weft.instances.list",
        "weft.tokens.revoke",
    ];

    pub fn name(&self) -> &'static str {
        match self {
            InstanceCall::ConnectionsList { .. } => "weft.connections.list",
            InstanceCall::ConnectionsForget { .. } => "weft.connections.forget",
            InstanceCall::ValuesGet { .. } => "weft.values.get",
            InstanceCall::ValuesChange { .. } => "weft.values.change",
            InstanceCall::ValuesForget { .. } => "weft.values.forget",
            InstanceCall::List => "weft.instances.list",
            InstanceCall::TokensRevoke { .. } => "weft.tokens.revoke",
        }
    }
}
