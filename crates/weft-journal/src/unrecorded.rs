//! What an unrecorded run leaves of itself
//! (`weft_core::run_settings::RunSettings::recorded` off): a website
//! polling a status route every few seconds would otherwise leave a
//! recorded run per poll. Its worker keeps no history of it while it runs:
//! it holds the run's birth alone, so the run can still leave a note of
//! itself, and every other event is dropped as it comes.
//!
//! What is written of it, through its writer lane like any run's record
//! and marked as not recorded on its row:
//! - when it fails, its birth and its failure, which names the step that
//!   failed and why: nothing of what ran before;
//! - before it does something that names it to the install (asks for a
//!   stored file, a connection or an infrastructure address its worker
//!   does not already hold, a task, a stop by tag:
//!   `weft_engine::record_first`), and when it reports a cost, its birth, so the thing naming it finds a row. From then on its costs are
//!   written as they come, and its ending however it ends, because a row
//!   that exists has to be closed.
//!
//! A run that does neither leaves nothing, whether it completes or is
//! cancelled.

use crate::events::ExecEvent;

/// An unrecorded run's note: its birth until something makes it written.
#[derive(Debug, Default)]
pub struct UnrecordedNote {
    birth: Option<Box<ExecEvent>>,
    /// Its birth was written: its row exists.
    written: bool,
}

impl UnrecordedNote {
    /// The note of a run whose row is written already (a run picked up
    /// again from its record): its ending is written however it ends.
    pub fn after_written() -> Self {
        Self { birth: None, written: true }
    }

    /// What of `events` is written now (see the module doc), in order.
    pub fn route(&mut self, events: Vec<ExecEvent>) -> Vec<ExecEvent> {
        let mut out = Vec::new();
        for event in events {
            match &event {
                ExecEvent::ExecutionStarted { .. } => self.birth = Some(Box::new(event)),
                ExecEvent::CostReported { .. } | ExecEvent::ExecutionFailed { .. } => {
                    out.extend(self.take_birth());
                    out.push(event);
                }
                _ if event.is_execution_terminal() && self.written => out.push(event),
                _ => {}
            }
        }
        out
    }

    /// Whether its birth was written: its row exists.
    pub fn written(&self) -> bool {
        self.written
    }

    /// Its birth, when not written yet: what makes its row exist before
    /// something names it to the install. The run counts as written from
    /// here on.
    pub fn take_birth(&mut self) -> Option<ExecEvent> {
        self.written = true;
        self.birth.take().map(|birth| *birth)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::ExecutionId;

    fn log(message: &str) -> ExecEvent {
        ExecEvent::LogLine {
            execution_id: ExecutionId::nil(),
            node_id: "n".into(),
            frames: Vec::new(),
            level: "info".into(),
            message: message.into(),
            at_unix_ms: None,
            seq: None,
            at_unix: 0,
        }
    }

    fn birth() -> ExecEvent {
        serde_json::from_value(serde_json::json!({
            "kind": "execution_started", "project_id": ExecutionId::nil(),
            "entry_node": "door", "phase": "fire", "at_unix": 0
        }))
        .expect("a birth")
    }

    fn ended(failed: bool) -> ExecEvent {
        match failed {
            true => ExecEvent::ExecutionFailed { execution_id: ExecutionId::nil(), error: "n: boom".into(), at_unix: 0 },
            false => ExecEvent::ExecutionCompleted { execution_id: ExecutionId::nil(), at_unix: 0 },
        }
    }

    /// A run that ends well leaves nothing; one that fails leaves its birth
    /// and its failure, and none of its history.
    #[test]
    fn only_a_failure_leaves_a_note() {
        let mut note = UnrecordedNote::default();
        assert!(note.route(vec![birth(), log("a"), ended(false)]).is_empty());
        let mut note = UnrecordedNote::default();
        assert!(note.route(vec![birth(), log("a")]).is_empty());
        let written = note.route(vec![log("b"), ended(true)]);
        assert!(matches!(written.as_slice(), [ExecEvent::ExecutionStarted { .. }, ExecEvent::ExecutionFailed { .. }]), "{written:?}");
    }

    /// A run named to the install writes its birth once, then its ending
    /// however it ends.
    #[test]
    fn a_named_run_ends_its_row() {
        let mut note = UnrecordedNote::default();
        assert!(note.route(vec![birth()]).is_empty());
        assert!(matches!(note.take_birth(), Some(ExecEvent::ExecutionStarted { .. })));
        assert!(note.take_birth().is_none());
        let written = note.route(vec![log("a"), ended(false)]);
        assert!(matches!(written.as_slice(), [ExecEvent::ExecutionCompleted { .. }]), "{written:?}");
    }
}
