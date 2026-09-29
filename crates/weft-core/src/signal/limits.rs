//! How often, and how many at once, one trigger may start runs: per
//! caller for a trigger somebody outside calls (a route, a socket, a
//! form), and for everybody together on every trigger (a schedule, a
//! feed and a provider's push included).
//!
//! A property of the generic entry mechanism, like a public entry's auth
//! policy, so every kind gets it without any kind or node knowing: the
//! language gives every trigger the settings
//! ([`EntryLimits::node_inputs`]), the ctx reads them when the node
//! registers its signal (`ExecutionContext::register_signal`), and the
//! dispatcher enforces them before any worker is reserved or run
//! started: at the HTTP door for a call from outside, on the
//! `fire_signal` task for a fire the listener picked up itself.
//!
//! Every number can be raised to anything, or turned off: `0` means no
//! limit, the same way `0` turns off a route's session cap.

use serde::{Deserialize, Serialize};

/// Calls per minute from one caller, when the entry says nothing: a
/// person clicking fast never meets it, a script hammering one address
/// does.
pub const DEFAULT_PER_CALLER_PER_MINUTE: u32 = 60;
/// Runs one entry may have going at once, when it says nothing: what
/// stops many addresses together from running up a bill.
pub const DEFAULT_AT_ONCE: u32 = 100;

/// An entry's limits as its node declared them. `None` is the language
/// default; `Some(0)` is no limit; `Some(n)` is `n`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntryLimits {
    /// Calls per minute from one caller: the verified identity on an
    /// entry with auth, the caller's address otherwise.
    #[serde(default, rename = "perCallerPerMinute", skip_serializing_if = "Option::is_none")]
    pub per_caller_per_minute: Option<u32>,
    /// Calls per minute from everybody together. No limit by default.
    #[serde(default, rename = "perMinute", skip_serializing_if = "Option::is_none")]
    pub per_minute: Option<u32>,
    /// Runs this entry started that are still going.
    #[serde(default, rename = "atOnce", skip_serializing_if = "Option::is_none")]
    pub at_once: Option<u32>,
}

/// The limits in force: `None` is no limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedLimits {
    pub per_caller_per_minute: Option<u32>,
    pub per_minute: Option<u32>,
    pub at_once: Option<u32>,
}

impl EntryLimits {
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }

    pub fn resolve(&self) -> ResolvedLimits {
        let pick = |declared: Option<u32>, default: Option<u32>| match declared {
            None => default,
            Some(0) => None,
            Some(n) => Some(n),
        };
        ResolvedLimits {
            per_caller_per_minute: pick(self.per_caller_per_minute, Some(DEFAULT_PER_CALLER_PER_MINUTE)),
            per_minute: pick(self.per_minute, None),
            at_once: pick(self.at_once, Some(DEFAULT_AT_ONCE)),
        }
    }

    /// The three settings' names, in the order the editor shows them:
    /// calls per minute from one caller, from everybody, runs at once.
    pub const NODE_FIELDS: [&'static str; 3] = ["callsPerMinutePerCaller", "callsPerMinute", "callsAtOnce"];

    /// The inputs the language gives a trigger
    /// ([`crate::node::NodeMetadata::add_language_inputs`]): the
    /// per-minute and at-once limits on every trigger, and the
    /// per-caller one only when `has_caller` (somebody outside calls it,
    /// [`crate::node::NodeFeatures::has_outside_caller`]), since a
    /// trigger with no caller has nothing to count it by. A call is a
    /// request on a route, a new connection on a socket, a submitted
    /// form, and a fire the trigger picked up itself on any other.
    pub fn node_inputs(has_caller: bool) -> Vec<crate::node::InputSpec> {
        let [per_caller, per_minute, at_once] = Self::NODE_FIELDS;
        let number = |name: &str, label: &str, description: &str| -> crate::node::InputSpec {
            serde_json::from_value(serde_json::json!({
                "name": name,
                "type": "Number",
                "widget": { "kind": "number", "min": 0, "step": 1 },
                "label": label,
                "description": description,
            }))
            .expect("an entry limit input is a valid InputSpec")
        };
        let mut inputs = Vec::with_capacity(Self::NODE_FIELDS.len());
        if has_caller {
            inputs.push(number(
                per_caller,
                "Calls per minute from one caller",
                "How many calls (a request, a new connection on a socket, a submitted form) one caller may make per minute, counted by the identity `auth` established, or by the caller's address when the entry is open. A call past it is refused with 429 and a `Retry-After`, before any run starts, so it costs nothing. Unset, 60; `0` is no limit, and any other number is yours to pick. Counted by the whole install at once, however many dispatchers it runs.",
            ));
        }
        inputs.push(number(
            per_minute,
            "Runs started per minute",
            "How many runs this trigger may start per minute, every caller and every event together. Unset, no limit. Set it when the run behind this trigger costs money (a paid model, an outside API), so a flood of calls or events cannot run up the bill. A call from outside past it is refused with 429 and a `Retry-After`; an event the trigger picked up itself (a schedule tick, a new message) past it is dropped, and `weft status` counts it.",
        ));
        inputs.push(number(
            at_once,
            "Runs going at once",
            "How many runs this trigger may have going at the same time. Unset, 100; `0` is no limit. A call from outside past it is refused with 429 and a `Retry-After` until one of the runs ends; an event the trigger picked up itself waits until one ends.",
        ));
        inputs
    }

    /// The limits a trigger's author set on the three inputs
    /// ([`Self::NODE_FIELDS`]). Each is absent (the default), `0` (no
    /// limit) or a whole number; a trigger without them has the defaults.
    pub fn from_node_fields(fields: &serde_json::Map<String, serde_json::Value>) -> Result<Self, String> {
        let read = |name: &str| -> Result<Option<u32>, String> {
            match fields.get(name) {
                None | Some(serde_json::Value::Null) => Ok(None),
                Some(v) => match v.as_f64() {
                    Some(n) if n >= 0.0 && n.fract() == 0.0 && n <= f64::from(u32::MAX) => Ok(Some(n as u32)),
                    _ => Err(format!("{name} must be a whole number (0 for no limit), got {v}")),
                },
            }
        };
        let [per_caller, per_minute, at_once] = Self::NODE_FIELDS;
        Ok(Self {
            per_caller_per_minute: read(per_caller)?,
            per_minute: read(per_minute)?,
            at_once: read(at_once)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unset_is_the_default_and_zero_is_no_limit() {
        let r = EntryLimits::default().resolve();
        assert_eq!(r.per_caller_per_minute, Some(DEFAULT_PER_CALLER_PER_MINUTE));
        assert_eq!(r.per_minute, None);
        assert_eq!(r.at_once, Some(DEFAULT_AT_ONCE));
        let off = EntryLimits { per_caller_per_minute: Some(0), per_minute: Some(0), at_once: Some(0) }.resolve();
        assert_eq!(off, ResolvedLimits { per_caller_per_minute: None, per_minute: None, at_once: None });
        let raised = EntryLimits { per_caller_per_minute: Some(100_000), per_minute: Some(5), at_once: None }.resolve();
        assert_eq!(raised.per_caller_per_minute, Some(100_000));
        assert_eq!(raised.per_minute, Some(5));
        assert_eq!(raised.at_once, Some(DEFAULT_AT_ONCE));
    }

    #[test]
    fn node_fields_are_read_strictly() {
        let fields = serde_json::json!({ "callsPerMinutePerCaller": 0, "callsAtOnce": 5.0 });
        let limits = EntryLimits::from_node_fields(fields.as_object().unwrap()).unwrap();
        assert_eq!(limits, EntryLimits { per_caller_per_minute: Some(0), per_minute: None, at_once: Some(5) });
        for bad in [serde_json::json!({ "callsPerMinute": -1 }), serde_json::json!({ "callsAtOnce": 1.5 }), serde_json::json!({ "callsAtOnce": "many" })] {
            assert!(EntryLimits::from_node_fields(bad.as_object().unwrap()).is_err(), "{bad}");
        }
        let names = |has_caller| EntryLimits::node_inputs(has_caller).into_iter().map(|i| i.name).collect::<Vec<_>>();
        assert_eq!(names(true), EntryLimits::NODE_FIELDS);
        // With no caller there is nothing to count per caller.
        assert_eq!(names(false), EntryLimits::NODE_FIELDS[1..]);
    }

    #[test]
    fn the_wire_shape_leaves_defaults_out() {
        assert_eq!(serde_json::to_value(EntryLimits::default()).unwrap(), serde_json::json!({}));
        let l = EntryLimits { per_caller_per_minute: None, per_minute: Some(0), at_once: Some(3) };
        let v = serde_json::to_value(l).unwrap();
        assert_eq!(v, serde_json::json!({ "perMinute": 0, "atOnce": 3 }));
        assert_eq!(serde_json::from_value::<EntryLimits>(v).unwrap(), l);
    }
}
