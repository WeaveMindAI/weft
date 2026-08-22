//! Completion detection. An execution is complete when:
//! - every node that has an execution record has reached a terminal
//!   status, AND
//! - no pending pulses remain in the table.
//!
//! An execution is failed when any execution record is in Failed
//! status.

use crate::exec::execution::{NodeExecutionStatus, NodeExecutionTable};
use crate::pulse::PulseTable;

/// Return `Some(failed_flag)` when the execution is complete:
/// - `Some(false)`: all terminal, no pending, no failures.
/// - `Some(true)`: all terminal, but at least one failed.
/// - `None`: still work to do (pending pulses or non-terminal
///   executions).
pub fn check_completion(pulses: &PulseTable, executions: &NodeExecutionTable) -> Option<bool> {
    // `in_flight` (not `is_pending`): a ROUTED pulse sits in a live
    // sink awaiting its take, which is still work; declaring the
    // execution complete over it would silently drop a delivered-but-
    // untaken stream item.
    for bucket in pulses.values() {
        if bucket.iter().any(|p| p.status.in_flight()) {
            return None;
        }
    }

    let mut any_failed = false;
    for execs in executions.values() {
        for e in execs {
            if !e.status.is_terminal() {
                return None;
            }
            if e.status == NodeExecutionStatus::Failed {
                any_failed = true;
            }
        }
    }

    Some(any_failed)
}
