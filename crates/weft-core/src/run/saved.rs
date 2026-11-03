//! A node's saved state: what one firing keeps across a pause.
//!
//! A node body resumes by running again from its top. What tells it
//! where it was is the state it keeps (`ctx.keep`, `crate::context::Kept`),
//! saved as ONE row per firing location (`RunEvent::NodeStateSaved`) on
//! `save()`, at the end of each `once` and `wait`, and when the run
//! pauses while the body sits in a call that holds the state. Beside the
//! author's value the row carries what the runtime itself must hand back
//! for the body to carry on where it stopped: how far it read each bus
//! and stream, how many items it yielded, and the numbers its next wait
//! and its next program call take.
//!
//! A row written at a pause INSIDE a call records the positions from
//! before that call: the wait it paused in takes the same number again,
//! the item it was yielding is the same item, the read it was waiting in
//! reads from the same place. The body runs into that same call on its
//! resume (the state says it is there), and it carries on from there.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// What one firing saved: the author's state and the runtime's
/// positions and counters beside it. Written whole on every save; the
/// latest one per firing location is the one a resume reads.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SavedState {
    /// The node's own state (`S` of `Kept<S>`), as JSON.
    pub state: Value,
    pub positions: Positions,
    pub counters: Counters,
    /// The waiting helper the firing paused in, and where it stood
    /// (`crate::context::Kept::follow`): the resumed body comes back into
    /// that helper's wait from there, making none of its calls again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub helper: Option<PausedHelper>,
}

/// A waiting helper (a program call followed in looks) paused in its
/// wait: which helper, and what it needs to carry on after that wait.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PausedHelper {
    pub helper: String,
    /// What the call asks for (its target and arguments), as a digest: a
    /// resumed body coming back to the same helper asking for something
    /// else did not come back to where it was.
    pub call: String,
    pub place: Value,
}

/// The numbers a firing hands out as it goes, which identify what it
/// asked for outside its run: the next wait's number (in the wait's
/// token, `SuspensionRegistered.wait_index`) and the next program
/// call's number (in the call's key). A resumed body skips every wait
/// and call its state says it already made, so the next one it makes
/// takes the number it took before the pause.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Counters {
    /// The number the next wait that actually waits takes.
    pub waits: u32,
    /// The number the next program call takes.
    pub program_calls: u32,
}

/// How far a firing went through what it reads and yields.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Positions {
    /// Per bus it reads or joined, by bus id.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub buses: BTreeMap<String, BusPosition>,
    /// Per `Generator[T]` input port, how many items it took.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub taken: BTreeMap<String, u64>,
    /// Per `Generator[T]` input port whose end reached the firing, that
    /// end: its close goes on record with this save, so the feed of a
    /// body run again from it starts ended (the items it had not taken
    /// still come first).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub ended: BTreeMap<String, crate::primitive::StreamEnd>,
    /// Per `Generator[T]` input port, the items a `drain` had taken and
    /// not handed to the body yet (it hands them all at the end): the
    /// feed of a body run again from this save starts with them.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub drained: BTreeMap<String, Vec<serde_json::Value>>,
    /// How many items it yielded on its `Generator[T]` outputs, counted
    /// across them (the number of the next item's emission,
    /// `crate::run::emission::stream_emission`).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub yielded: u64,
}

/// Where a firing stands on one bus.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BusPosition {
    /// The next offset of the firing's reader on the bus (a firing keeps
    /// one reader per bus): a resumed body's reader starts there.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<u64>,
    /// The name it holds on the bus, and the offset its `Joined` entry
    /// landed at: a paused firing keeps its name (nobody reads it leave),
    /// and its resumed body takes it back.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registered: Option<Registration>,
}

/// A name a firing holds on a bus.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Registration {
    pub name: String,
    pub joined_offset: u64,
}

/// Namespace of the digests that say what a helper's call asks for.
const HELPER_CALL_NAMESPACE: uuid::Uuid = uuid::Uuid::from_u128(0x5e1f_47a2_9c03_4b6e_8d15_2fa4_70c9_b381);

impl PausedHelper {
    /// What a helper's `call` asks for (its target and arguments), as the
    /// short digest [`Self::call`] holds.
    pub fn digest(call: &impl Serialize) -> Result<String, serde_json::Error> {
        let call = serde_json::to_vec(call)?;
        Ok(uuid::Uuid::new_v5(&HELPER_CALL_NAMESPACE, &call).simple().to_string())
    }
}

fn is_zero(n: &u64) -> bool {
    *n == 0
}

// Layer 2: the saved row's wire shape.
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_saved_state_round_trips_and_leaves_empty_positions_out() {
        let bare = SavedState { state: json!({ "posted": null }), positions: Positions::default(), counters: Counters { waits: 2, program_calls: 1 }, helper: None };
        let written = serde_json::to_value(&bare).expect("serializes");
        assert_eq!(written, json!({ "state": { "posted": null }, "positions": {}, "counters": { "waits": 2, "program_calls": 1 } }));
        assert_eq!(serde_json::from_value::<SavedState>(written).expect("reads back"), bare);

        let full = SavedState {
            state: json!(3),
            positions: Positions {
                buses: BTreeMap::from([(
                    "b".to_string(),
                    BusPosition { cursor: Some(9), registered: Some(Registration { name: "host".into(), joined_offset: 1 }) },
                )]),
                taken: BTreeMap::from([("rows".to_string(), 7)]),
                ended: BTreeMap::from([("rows".to_string(), crate::primitive::StreamEnd::Finished)]),
                drained: BTreeMap::from([("rows".to_string(), vec![json!(1), json!("two")])]),
                yielded: 5,
            },
            counters: Counters::default(),
            helper: Some(PausedHelper { helper: "infra.start".into(), call: "c0ffee".into(), place: json!({ "accepted": true }) }),
        };
        let written = serde_json::to_value(&full).expect("serializes");
        assert_eq!(serde_json::from_value::<SavedState>(written).expect("reads back"), full);
    }
}
