//! Whether a node of a run can still join a bus nobody is on.
//!
//! A bus ends once nobody is on it and no node can still join it
//! ([`crate::bus::BusInner::end_unattended`]). A node can join a bus only
//! through its marker, so the question is where the marker still is:
//!
//! - on a wire, in a pulse not taken yet: the node it goes to has not
//!   started;
//! - in a loop that has not ended: in what it hands every iteration, in
//!   a value it carries to the next one, or in what it gathered and will
//!   hand on when it ends, so an iteration still to come, or a node after
//!   the loop, gets it;
//! - in the input of a firing that has not ended (running, or parked on
//!   a wait) and has not used the bus yet: it may still register, and a
//!   node that only hands the marker on holds the bus until it does;
//! - with the firing that made the bus, until the marker went out on its
//!   port: nobody else has it yet, so whatever the maker does with the
//!   bus meanwhile (reading it, waiting on it), the bus waits for it.
//!
//! A firing that used the bus without registering on it (opened its
//! reader, waited on it) no longer counts: a reader waiting for the end
//! would otherwise keep the bus open itself. Nor does a firing whose
//! node was skipped, or a loop that ended: their markers went nowhere.
//! Which firings used a bus is the engine's to say (`used`): a running
//! body's use is the bus's own (`crate::bus::BusInner::used_by`), and a
//! firing parked on a wait counts as not having used it: its body runs
//! again from its saved state, and uses the bus again if it does, so a
//! run picked up by another worker reads the same.
//!
//! Where the marker still is says only that a node may still join: the
//! node it is on its way to may itself wait on what a reader of the bus
//! makes once the bus ends. When the run cannot move any other way, the
//! engine ends every bus nobody is on instead of finding the run stuck.

use std::collections::HashSet;

use crate::frames::FiringLocation;
use crate::pulse::PulseTable;
use crate::run::firing::{NodeFiring, NodeFiringTable};
use crate::run::loop_runtime::{LoopRuntime, LoopWrite};
use crate::weft_type::WeftType;

/// The id of every bus a node can still join through its marker, given
/// where the run stands: its pulses on their wires, its loops, and the
/// input of each firing that has not ended, unless that firing `used`
/// the bus already. One walk of the run, whatever number of buses asks.
pub fn joinable_buses(
    pulses: &PulseTable,
    firings: &NodeFiringTable,
    loops: &LoopRuntime,
    used: impl Fn(&NodeFiring, &str) -> bool,
) -> HashSet<String> {
    let mut joinable = HashSet::new();
    let mut add = |id: &str| {
        joinable.insert(id.to_string());
    };
    for pulse in pulses.values().flatten().filter(|pulse| !pulse.closed) {
        WeftType::buses_in(&pulse.value, &mut add);
    }
    for (_, instance) in loops.iter().filter(|(_, instance)| instance.terminated.is_none()) {
        for value in instance.outer_input.values().chain(instance.carry_values.values()) {
            WeftType::buses_in(value, &mut add);
        }
        for slot in instance.gather_lists.values().flat_map(|slots| slots.values()) {
            if let LoopWrite::Value(value) = slot {
                WeftType::buses_in(value, &mut add);
            }
        }
    }
    for firing in firings.open() {
        for value in firing.received.input.values() {
            WeftType::buses_in(value, &mut |id: &str| {
                if !used(firing, id) {
                    add(id);
                }
            });
        }
    }
    joinable
}

/// Whether the firing at `maker` made a bus it has not sent the marker of
/// yet, on its output `port`: nobody else has it, so whatever the maker
/// does with the bus meanwhile, the bus waits for it.
pub fn not_sent_yet(firings: &NodeFiringTable, maker: &FiringLocation, port: &str) -> bool {
    firings.at(&maker.node_id, &maker.frames).is_some_and(|firing| !firing.status().is_terminal() && !firing.mentioned_ports.contains(port))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::{json, Value};

    use super::*;
    use crate::pulse::Pulse;
    use crate::run::firing::{NodeFiring, NodeFiringStatus};
    use crate::run::ready::FiringInput;

    const BUS: &str = "6c0f5c1e-2a4b-4c3d-9e8f-7a6b5c4d3e2f";

    fn marker() -> Value {
        json!({ "__weft_bus__": { "id": BUS, "mode": "journaled" } })
    }

    fn other_marker() -> Value {
        json!({ "__weft_bus__": { "id": "0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d", "mode": "journaled" } })
    }

    fn pulses_with(value: Value) -> PulseTable {
        let mut pulses = PulseTable::new();
        let pulse = Pulse::new(uuid::Uuid::new_v4(), uuid::Uuid::nil(), vec![], "joiner", "stream", Arc::new(value));
        pulses.entry("joiner".into()).or_default().push(pulse);
        pulses
    }

    fn firing_holding(value: Value, status: NodeFiringStatus) -> NodeFiringTable {
        let mut firings = NodeFiringTable::new();
        let mut received = FiringInput::default();
        received.input.insert("stream".into(), Arc::new(value));
        firings.insert(NodeFiring::started("joiner", vec![], uuid::Uuid::nil(), received, vec![], 0));
        firings.set_status("joiner", &vec![], status);
        firings
    }

    fn nobody_used(_: &NodeFiring, _: &str) -> bool {
        false
    }

    fn joinable(pulses: &PulseTable, firings: &NodeFiringTable, used: impl Fn(&NodeFiring, &str) -> bool) -> bool {
        joinable_buses(pulses, firings, &LoopRuntime::new(), used).contains(BUS)
    }

    #[test]
    fn a_bus_whose_marker_is_nowhere_cannot_be_joined() {
        assert!(!joinable(&PulseTable::new(), &NodeFiringTable::new(), nobody_used));
    }

    #[test]
    fn a_marker_on_its_way_to_a_node_keeps_the_bus_joinable_however_deep_it_sits() {
        for value in [marker(), json!([1, marker()]), json!({ "session": { "stream": marker() } })] {
            assert!(joinable(&pulses_with(value.clone()), &NodeFiringTable::new(), nobody_used), "{value}");
        }
        assert!(!joinable(&pulses_with(other_marker()), &NodeFiringTable::new(), nobody_used));
    }

    #[test]
    fn a_closure_carries_no_bus() {
        let mut pulses = PulseTable::new();
        pulses.entry("joiner".into()).or_default().push(Pulse::closure(uuid::Uuid::new_v4(), uuid::Uuid::nil(), vec![], "joiner", "stream"));
        assert!(!joinable(&pulses, &NodeFiringTable::new(), nobody_used));
    }

    #[test]
    fn a_firing_holding_the_marker_counts_until_it_used_the_bus_or_ended() {
        for status in [NodeFiringStatus::Running, NodeFiringStatus::WaitingForInput] {
            let firings = firing_holding(marker(), status);
            assert!(joinable(&PulseTable::new(), &firings, nobody_used), "{status:?}");
            assert!(!joinable(&PulseTable::new(), &firings, |firing, bus| firing.node_id == "joiner" && bus == BUS), "{status:?}, used");
        }
        for status in [NodeFiringStatus::Completed, NodeFiringStatus::Failed, NodeFiringStatus::Skipped, NodeFiringStatus::Cancelled] {
            assert!(!joinable(&PulseTable::new(), &firing_holding(marker(), status), nobody_used), "{status:?}");
        }
    }

    #[test]
    fn using_one_bus_keeps_another_the_same_firing_holds_joinable() {
        let firings = firing_holding(json!([marker(), other_marker()]), NodeFiringStatus::Running);
        let joinable = joinable_buses(&PulseTable::new(), &firings, &LoopRuntime::new(), |_, bus| bus == BUS);
        assert!(!joinable.contains(BUS) && joinable.contains("0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d"), "{joinable:?}");
    }

    #[test]
    fn its_maker_holds_the_bus_until_the_marker_went_out() {
        let maker = FiringLocation::new("maker", vec![]);
        let mut firings = NodeFiringTable::new();
        firings.insert(NodeFiring::started("maker", vec![], uuid::Uuid::nil(), FiringInput::default(), vec![], 0));
        assert!(not_sent_yet(&firings, &maker, "stream"), "not sent yet");
        firings.at_mut("maker", &vec![]).expect("its record").mentioned_ports.insert("stream".into());
        assert!(!not_sent_yet(&firings, &maker, "stream"), "sent");
        let mut firings = NodeFiringTable::new();
        firings.insert(NodeFiring::started("maker", vec![], uuid::Uuid::nil(), FiringInput::default(), vec![], 0));
        firings.set_status("maker", &vec![], NodeFiringStatus::Completed);
        assert!(!not_sent_yet(&firings, &maker, "stream"), "returned without sending it");
    }
}
