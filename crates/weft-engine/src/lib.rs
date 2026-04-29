//! The weft execution engine. Linked into each compiled project
//! binary. Exposes one entry point: [`run`]. The binary's `main`
//! parses a `WakeSpec` from CLI args, hands it to `run`, and exits
//! when `run` returns.
//!
//! Phase A slice 0: the engine is the former `weft-runner` library
//! minus CLI concerns, minus the single-entry hack, with the
//! suspension path expressed in terms of [`WakeSignalSpec`] instead
//! of the old await_form/await_timer/await_callback trio.
//!
//! Later slices (3+) will replace the HTTP dispatcher client with a
//! WebSocket client and wire real stall+snapshot + await_signal
//! round-trips.

pub mod context;
pub mod dispatcher_link;
pub mod loop_driver;

pub use context::{
    ship_node_completed, ship_node_failed, ship_node_resumed, ship_node_skipped,
    ship_node_started, ship_node_suspended, ship_pulse_mutations, RunnerHandle,
};
pub use dispatcher_link::{DispatcherLink, StartPacket};
pub use loop_driver::{run_loop, run_with_link, LoopOutcome, RootSeed, WakeSpec};
