//! Completion detection. A run is complete when:
//! - every node that has a run record has reached a terminal
//!   status, AND
//! - no pulse is left on a wire (pending, or routed to a live sink).
//!
//! A run is failed when any run record is in Failed
//! status.

use crate::run::firing::NodeFiringTable;
use crate::pulse::PulseTable;

/// Return `Some(failed_flag)` when the run is complete:
/// - `Some(false)`: all terminal, nothing on a wire, no failures.
/// - `Some(true)`: all terminal, but at least one failed.
/// - `None`: still work to do (a pulse on a wire, or a non-terminal
///   firing).
pub fn check_completion(pulses: &PulseTable, firings: &NodeFiringTable) -> Option<bool> {
    // Any pulse still in a bucket is in flight (a taken one has left it):
    // a ROUTED pulse sits in a live sink awaiting its take, which is
    // still work; declaring the run complete over it would silently drop
    // a delivered-but-untaken stream item.
    if pulses.values().any(|bucket| !bucket.is_empty()) {
        return None;
    }

    if firings.any_open() {
        return None;
    }
    Some(firings.failed().next().is_some())
}
