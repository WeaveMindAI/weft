//! How long an execution may run: chosen where it starts, never per node.
//!
//! Nobody can know how long a program will take, so the choice is made
//! at the one place an execution comes from: the signal that starts it
//! (set by whatever registers the signal through the ctx) or the `weft
//! run` that starts it by hand. The execution keeps it for its whole
//! life, and every node of it runs under it.
//!
//! On a platform whose short runs have a hard cap (Cloud Run cuts a
//! request at 60 minutes), a `Short` run cut there fails loudly, naming
//! this setting. A `Long` run is started as a job instead, with a far
//! longer cap. Locally neither has a cap.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunClass {
    /// Served as one request to the project's workers. The default:
    /// no startup beyond the worker's own, and the cheapest.
    #[default]
    Short,
    /// Started as a job of its own, for runs that may outlive a
    /// request's cap.
    Long,
}

impl RunClass {
    pub fn is_short(self) -> bool {
        self == Self::Short
    }

    /// Whether this is the default class (for a field the wire leaves
    /// out when it holds the default).
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Short => "short",
            Self::Long => "long",
        }
    }

    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "short" => Ok(Self::Short),
            "long" => Ok(Self::Long),
            other => Err(format!("'{other}' is not a run class; use `short` or `long`")),
        }
    }
}

/// The input the language gives every trigger that is not a live
/// connection ([`crate::node::NodeMetadata::add_language_inputs`]), read
/// by the ctx when the node registers its signal, so no node declares or
/// reads it.
pub const LONG_RUNS_FIELD: &str = "longRuns";

impl RunClass {
    /// The `longRuns` input as the editor shows it.
    pub fn node_input() -> crate::node::InputSpec {
        serde_json::from_value(serde_json::json!({
            "name": LONG_RUNS_FIELD,
            "type": "Boolean",
            "label": "Long runs",
            "description": "Run each firing as a job of its own, which may run for days, instead of one request to the project's workers. A request is cut at an hour on a cloud install; a firing that may run longer needs this on. Off (default) starts faster and costs less."
        }))
        .expect("the longRuns input is a valid InputSpec")
    }

    /// The class a trigger node's author chose (`longRuns`): absent or
    /// `false` is short, `true` is long, anything else is refused.
    pub fn from_node_fields(fields: &serde_json::Map<String, serde_json::Value>) -> Result<Self, String> {
        match fields.get(LONG_RUNS_FIELD) {
            None | Some(serde_json::Value::Null) | Some(serde_json::Value::Bool(false)) => Ok(Self::Short),
            Some(serde_json::Value::Bool(true)) => Ok(Self::Long),
            Some(other) => Err(format!("{LONG_RUNS_FIELD} is on or off (true or false), got {other}")),
        }
    }
}

impl std::fmt::Display for RunClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_run_class_is_a_plain_word_on_the_wire() {
        assert_eq!(serde_json::to_value(RunClass::Long).unwrap(), serde_json::json!("long"));
        assert_eq!(serde_json::from_value::<RunClass>(serde_json::json!("short")).unwrap(), RunClass::Short);
        assert_eq!(RunClass::default(), RunClass::Short);
        assert_eq!(RunClass::parse("long").unwrap(), RunClass::Long);
        assert!(RunClass::parse("forever").is_err());
    }

    #[test]
    fn a_trigger_asks_for_long_runs_by_its_one_field() {
        let fields = |v: serde_json::Value| v.as_object().cloned().unwrap();
        assert_eq!(RunClass::from_node_fields(&fields(serde_json::json!({}))).unwrap(), RunClass::Short);
        assert_eq!(RunClass::from_node_fields(&fields(serde_json::json!({ "longRuns": true }))).unwrap(), RunClass::Long);
        assert!(RunClass::from_node_fields(&fields(serde_json::json!({ "longRuns": "yes" }))).is_err());
        assert_eq!(RunClass::node_input().name, LONG_RUNS_FIELD);
    }
}
