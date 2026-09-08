//! Pulse emissions and their identity.
//!
//! Every act that puts pulses on wires is an EMISSION with an id: a
//! node body's `pulse_downstream` (the id is minted fresh and written
//! to the journal on the `PortEmitted` row), a firing's termination
//! sweep, a group boundary forwarding, a loop launching an iteration
//! or emitting outward (those ids are DERIVED from what the journal
//! already records: the color, the node or group, the frames, the
//! ordinal of the firing). A pulse's id is then derived from its
//! emission id and the wire it lands on. So the live engine and the
//! journal fold, given the same rows and the same program, put the
//! same pulses with the same ids in the same table, and any row that
//! names a pulse by id (a stream take, a loop's item launch) resolves
//! identically on both sides. That is what makes replay exact.

use uuid::Uuid;

use crate::frames::LoopFrames;
use crate::pulse::Pulse;
use crate::Color;

/// One pulse placed on a downstream edge, plus the provenance (which
/// node + port emitted it). The `pulse` is the EXACT pulse pushed onto
/// the live pulse table, held by value rather than re-listing its
/// fields (the value inside is an `Arc`, so this is a pointer bump).
/// `pulse.closed == true` means the pulse is a CLOSURE marker (a
/// structural "nothing arrives here", produced when the source firing
/// terminated without emitting on this port), not a data value.
#[derive(Debug, Clone)]
pub struct PulseEmission {
    pub pulse: Pulse,
    pub source_node: String,
    pub source_port: String,
}

/// The id of the pulse an emission puts on ONE wire: a function of the
/// emission and the whole wire (the port it left from, the node and
/// port it lands on), so the fold re-derives it. The source port is
/// part of it because one emission is one firing's whole bag, and the
/// id names the wire end to end. A data pulse and a closure on the
/// same wire from the same emission are two different pulses (an
/// emission that projects a `?` key delivers a value on one wire and
/// a closure on another).
pub fn pulse_id(
    emission_id: Uuid,
    source_port: &str,
    target_node: &str,
    target_port: &str,
    closed: bool,
) -> Uuid {
    let tag = if closed { "closed" } else { "value" };
    Uuid::new_v5(
        &emission_id,
        format!("{source_port}\0{target_node}\0{target_port}\0{tag}").as_bytes(),
    )
}

/// The emission of a firing's TERMINATION sweep (the closures on every
/// output port it never mentioned). `ordinal` is the firing's rank
/// among the records at the same (node, frames): a node fired twice at
/// one location closes its ports twice, and the two sweeps must not
/// collide.
pub fn terminal_sweep_emission(color: Color, node_id: &str, frames: &LoopFrames, ordinal: usize) -> Uuid {
    derived(&TERMINAL_SWEEP, &[&color.to_string(), node_id, &frames_key(frames), &ordinal.to_string()])
}

/// The emission of a group boundary firing (a `Passthrough` forwarding
/// its inputs, or the closures a gated scope owes the outside).
pub fn boundary_emission(color: Color, node_id: &str, frames: &LoopFrames, ordinal: usize) -> Uuid {
    derived(&BOUNDARY, &[&color.to_string(), node_id, &frames_key(frames), &ordinal.to_string()])
}

/// The emission of a loop launching iteration `index` (the body's
/// per-iteration pulses).
pub fn iteration_launch_emission(
    color: Color,
    group_id: &str,
    parent_frames: &LoopFrames,
    index: u32,
) -> Uuid {
    derived(&ITERATION_LAUNCH, &[&color.to_string(), group_id, &frames_key(parent_frames), &index.to_string()])
}

/// The emission of a loop instance ending (its outward values, or the
/// closures of an abnormal end). One per instance: an instance
/// terminates once.
pub fn loop_termination_emission(color: Color, group_id: &str, parent_frames: &LoopFrames) -> Uuid {
    derived(&LOOP_TERMINATION, &[&color.to_string(), group_id, &frames_key(parent_frames)])
}

/// Frames as a stable text key (`"3.0"` for `[3, 0]`, empty at root).
fn frames_key(frames: &LoopFrames) -> String {
    frames.iter().map(|f| f.index.to_string()).collect::<Vec<_>>().join(".")
}

fn derived(namespace: &Uuid, parts: &[&str]) -> Uuid {
    Uuid::new_v5(namespace, parts.join("\0").as_bytes())
}

const TERMINAL_SWEEP: Uuid = Uuid::from_bytes([
    0x5a, 0x1e, 0x9c, 0x0b, 0x7d, 0x2f, 0x4a, 0x8e, 0x9b, 0x3c, 0x1d, 0x6f, 0x2e, 0x8a, 0x4b, 0x01,
]);
const BOUNDARY: Uuid = Uuid::from_bytes([
    0x5a, 0x1e, 0x9c, 0x0b, 0x7d, 0x2f, 0x4a, 0x8e, 0x9b, 0x3c, 0x1d, 0x6f, 0x2e, 0x8a, 0x4b, 0x02,
]);
const ITERATION_LAUNCH: Uuid = Uuid::from_bytes([
    0x5a, 0x1e, 0x9c, 0x0b, 0x7d, 0x2f, 0x4a, 0x8e, 0x9b, 0x3c, 0x1d, 0x6f, 0x2e, 0x8a, 0x4b, 0x03,
]);
const LOOP_TERMINATION: Uuid = Uuid::from_bytes([
    0x5a, 0x1e, 0x9c, 0x0b, 0x7d, 0x2f, 0x4a, 0x8e, 0x9b, 0x3c, 0x1d, 0x6f, 0x2e, 0x8a, 0x4b, 0x04,
]);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frames::LoopIteration;

    #[test]
    fn a_pulse_id_is_a_function_of_the_emission_and_the_wire() {
        let emission = Uuid::new_v4();
        let base = pulse_id(emission, "out", "b", "in", false);
        assert_eq!(base, pulse_id(emission, "out", "b", "in", false));
        assert_ne!(base, pulse_id(emission, "out", "c", "in", false));
        assert_ne!(base, pulse_id(emission, "out", "b", "other", false));
        assert_ne!(base, pulse_id(emission, "out", "b", "in", true));
        assert_ne!(base, pulse_id(Uuid::new_v4(), "out", "b", "in", false));
        assert_ne!(
            base,
            pulse_id(emission, "out2", "b", "in", false),
            "two output ports into one input port are two wires, two pulses"
        );
    }

    #[test]
    fn derived_emissions_separate_by_every_part_and_by_kind() {
        let color = Uuid::nil();
        let root: LoopFrames = Vec::new();
        let inner: LoopFrames = vec![LoopIteration { index: 2 }];
        let sweep = terminal_sweep_emission(color, "n", &root, 0);
        assert_eq!(sweep, terminal_sweep_emission(color, "n", &root, 0));
        assert_ne!(sweep, terminal_sweep_emission(color, "n", &root, 1), "a second firing sweeps apart");
        assert_ne!(sweep, terminal_sweep_emission(color, "n", &inner, 0));
        assert_ne!(sweep, boundary_emission(color, "n", &root, 0), "same parts, different act");
        assert_ne!(
            iteration_launch_emission(color, "g", &root, 0),
            iteration_launch_emission(color, "g", &root, 1)
        );
        assert_ne!(
            loop_termination_emission(color, "g", &root),
            loop_termination_emission(color, "g", &inner)
        );
    }
}
