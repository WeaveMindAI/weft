//! What a run IS, decided once at its birth: a project run, a node's
//! self-test, or a project run nobody keeps a record of. Every kind is a
//! real color (broker scope, cost attribution, owner fencing, cancel);
//! what differs is who owns its lifetime and what the journal keeps.
//!
//! The birth event carries it (`ExecutionStarted.run_kind`) and the
//! `execution_color.kind` column copies it, so every sweep and listing
//! filters on a column. Only `Execution` is listed and swept by the
//! project lifecycle.

use serde::{Deserialize, Deserializer, Serialize};

/// The kind of a run, as its birth names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunKind {
    /// A project run: listed, swept, cancelled and drained by the
    /// project's lifecycle, and journaled row by row.
    #[default]
    Execution,
    /// A node self-test's identity: its lifecycle is owned by the test
    /// task, so the project sweeps never touch it.
    NodeTest,
    /// A project run born from a trigger set not to record its runs (a
    /// Route with `recorded: false`). Its journal lives in the worker's
    /// memory: only cost rows reach the real journal while it runs. If it
    /// fails, its whole record is written afterwards and it becomes an
    /// `Execution`; otherwise nothing but its costs is left.
    Unrecorded,
}

impl RunKind {
    /// The `execution_color.kind` spelling.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Execution => "execution",
            Self::NodeTest => "node_test",
            Self::Unrecorded => "unrecorded",
        }
    }

    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "execution" => Ok(Self::Execution),
            "node_test" => Ok(Self::NodeTest),
            "unrecorded" => Ok(Self::Unrecorded),
            other => Err(format!("unknown run kind '{other}' (execution, node_test or unrecorded)")),
        }
    }

    pub fn is_execution(&self) -> bool {
        matches!(self, Self::Execution)
    }

    /// Whether the run's rows go to the real journal as they happen.
    pub fn journaled(&self) -> bool {
        !matches!(self, Self::Unrecorded)
    }
}

/// Reads the kind as it is written now (`"unrecorded"`), and as rows
/// written before the kind existed carry it: the `node_test` boolean,
/// true for a node test and false (or absent) for a project run. The
/// journal is append-only, so those rows stay readable forever.
impl<'de> Deserialize<'de> for RunKind {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Wire {
            Legacy(bool),
            Kind(String),
        }
        match Wire::deserialize(deserializer)? {
            Wire::Legacy(true) => Ok(Self::NodeTest),
            Wire::Legacy(false) => Ok(Self::Execution),
            Wire::Kind(raw) => Self::parse(&raw).map_err(serde::de::Error::custom),
        }
    }
}

/// THE refusal every wait answers inside an unrecorded run: a wait parks
/// the run in the journal, and this run has no journal to park in.
pub fn unrecorded_wait_error(place: &str) -> String {
    format!(
        "node '{place}' waits, and this run is not recorded (its Route has `recorded` off), so \
         there is nowhere to park it. Turn `recorded` back on for a route whose run waits, or \
         move the wait out of the run."
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_kind_round_trips_on_the_wire() {
        for kind in [RunKind::Execution, RunKind::NodeTest, RunKind::Unrecorded] {
            let wire = serde_json::to_value(kind).unwrap();
            assert_eq!(wire, serde_json::json!(kind.as_str()));
            assert_eq!(serde_json::from_value::<RunKind>(wire).unwrap(), kind);
            assert_eq!(RunKind::parse(kind.as_str()).unwrap(), kind);
        }
    }

    #[test]
    fn the_old_node_test_flag_still_reads() {
        assert_eq!(serde_json::from_value::<RunKind>(serde_json::json!(true)).unwrap(), RunKind::NodeTest);
        assert_eq!(serde_json::from_value::<RunKind>(serde_json::json!(false)).unwrap(), RunKind::Execution);
        assert!(serde_json::from_value::<RunKind>(serde_json::json!("other")).is_err());
    }
}
