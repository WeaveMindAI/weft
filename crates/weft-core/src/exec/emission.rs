//! Pulse-emission events the engine produces as it runs postprocess.
//! One entry per pulse placed on a downstream edge; the runtime crate
//! translates these into worker -> dispatcher messages so the journal
//! records the same set of pulses the engine actually emitted.
//!
//! This is the load-bearing piece that makes replay exact:
//! `NodeStarted.pulses_absorbed` UUIDs match the `pulse_id`s here, so
//! the dispatcher's fold reconstructs the engine's pulse table
//! one-for-one.

use crate::pulse::Pulse;

/// One pulse placed on a downstream edge, plus the provenance (which
/// node + port emitted it) the journal needs. The `pulse` is the EXACT
/// pulse pushed onto the live pulse table, held by value rather than
/// re-listing its fields: that makes "the journal records exactly the
/// pulse placed" true by construction (a hand-copy could silently drop
/// or diverge a field, which is the replay-divergence this type exists
/// to prevent). Read `emission.pulse.id` / `.value` / `.closed` etc.
/// `pulse.closed == true` means the pulse is a CLOSURE marker (a
/// structural "nothing arrives here", produced when the source firing
/// terminated without emitting on this port), not a data value.
#[derive(Debug, Clone)]
pub struct PulseEmission {
    pub pulse: Pulse,
    pub source_node: String,
    pub source_port: String,
}
