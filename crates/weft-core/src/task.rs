//! Where one queued task stands. The task store writes it; a client
//! waiting on a task it started (a node-test run) reads it off the wire,
//! so the enum lives here, below both.

crate::wire_enum! {
    /// The `task.status` column, and what a wait on a task answers.
    pub enum TaskStatus {
        Pending = "pending",
        Claimed = "claimed",
        Complete = "complete",
        Failed = "failed",
    }
}

impl TaskStatus {
    /// Whether the task is done, one way or the other.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Complete | Self::Failed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    crate::wire_enum_roundtrip_tests!(TaskStatus);
}
