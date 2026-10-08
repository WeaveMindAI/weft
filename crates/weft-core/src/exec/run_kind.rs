//! What a run IS, decided once at its birth: a project run or a node's
//! self-test. Both are real executions (broker scope, cost attribution,
//! owner fencing, cancel); what differs is who owns their lifetime. How a
//! run is KEPT (fast or durable, recorded or not) is a separate choice,
//! `crate::run_settings::RunSettings`.
//!
//! The birth event carries it (`ExecutionStarted.run_kind`) and the
//! `run.kind` column copies it, so every sweep and listing filters on a
//! column. Only a project run is listed and swept by the project lifecycle.

use serde::{Deserialize, Serialize};

/// The kind of a run, as its birth names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunKind {
    /// A project run: listed, swept, cancelled and drained by the
    /// project's lifecycle.
    #[default]
    Execution,
    /// A node self-test's identity: its lifecycle is owned by the test
    /// task, so the project sweeps never touch it.
    NodeTest,
}

impl RunKind {
    /// The `ExecutionStarted.run_kind` spelling.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Execution => "execution",
            Self::NodeTest => "node_test",
        }
    }

    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "execution" => Ok(Self::Execution),
            "node_test" => Ok(Self::NodeTest),
            other => Err(format!("unknown run kind '{other}' (execution or node_test)")),
        }
    }

    pub fn is_execution(&self) -> bool {
        matches!(self, Self::Execution)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_kind_round_trips_on_the_wire() {
        for kind in [RunKind::Execution, RunKind::NodeTest] {
            let wire = serde_json::to_value(kind).unwrap();
            assert_eq!(wire, serde_json::json!(kind.as_str()));
            assert_eq!(serde_json::from_value::<RunKind>(wire).unwrap(), kind);
            assert_eq!(RunKind::parse(kind.as_str()).unwrap(), kind);
        }
    }
}
