//! The calls a run's firings still owe when they end
//! (`RunEvent::EndCallOwed`, `weft_core::signal::cancel_call`): what a
//! node registered with `ctx.after_node` that no `EndCallsSettled` says
//! went yet. Owed until settled, rather than until the firing's ending:
//! a firing the engine ends without its body (one a dead worker left)
//! still owes its calls, which the end of the drive sends.
//!
//! Kept apart from [`crate::Fold`], like [`crate::ChargeBook`]: it needs
//! no program. A drive builds it from the record before anything runs
//! (so a firing a crashed worker left still owes its calls on the worker
//! that picks the run up) and keeps it as its nodes register more; it
//! settles a firing's calls when the firing ends. Whoever ends a run with
//! no drive (the dispatcher's cancel of a run nobody holds) reads it to
//! queue what is left.

use weft_core::frames::LoopFrames;
use weft_core::instance::InstanceId;
use weft_core::primitive::AccessRef;
use weft_core::signal::CancelCall;

use crate::events::RunEvent;

/// One call a firing owes, as its `EndCallOwed` wrote it.
#[derive(Debug, Clone, PartialEq)]
pub struct OwedCall {
    pub node_id: String,
    pub frames: LoopFrames,
    pub call: CancelCall,
    /// The connection it is sent through; `None` sends it plain.
    pub access: Option<AccessRef>,
}

impl OwedCall {
    /// Whether it is the same call, owed by the same firing: a body run
    /// again after a pause or a crash registers its calls again, and the
    /// firing still owes each once.
    fn same(&self, node_id: &str, frames: &LoopFrames, call: &CancelCall, access: &Option<AccessRef>) -> bool {
        self.node_id == node_id && &self.frames == frames && &self.call == call && &self.access == access
    }

    fn of(&self, node_id: &str, frames: &LoopFrames) -> bool {
        self.node_id == node_id && &self.frames == frames
    }
}

/// The calls a run's firings owe, in the order they were registered, and
/// the instance the run is for (whose connections they are sent through).
#[derive(Debug, Default)]
pub struct OwedCalls {
    instance: Option<InstanceId>,
    owed: Vec<OwedCall>,
}

impl OwedCalls {
    /// What `events`, a run's record in order, leaves owed.
    pub fn of_record<'a>(events: impl IntoIterator<Item = &'a RunEvent>) -> Self {
        let mut book = Self::default();
        for event in events {
            book.apply(event);
        }
        book
    }

    /// Apply one row: a registration adds its call (once per firing), a
    /// settlement clears what the firing owed, every other row leaves the
    /// book as it is.
    pub fn apply(&mut self, ev: &RunEvent) {
        match ev {
            RunEvent::RunStarted { instance, .. } => self.instance = instance.clone(),
            RunEvent::EndCallOwed { node_id, frames, call, access, .. } => {
                self.owe(node_id, frames, call, access);
            }
            RunEvent::EndCallsSettled { node_id, frames, .. } => {
                self.owed.retain(|owed| !owed.of(node_id, frames));
            }
            _ => {}
        }
    }

    /// Whether the firing at `node_id`/`frames` owes that very call
    /// already.
    pub fn owes(&self, node_id: &str, frames: &LoopFrames, call: &CancelCall, access: &Option<AccessRef>) -> bool {
        self.owed.iter().any(|owed| owed.same(node_id, frames, call, access))
    }

    /// Add `call` to what the firing at `node_id`/`frames` owes. Answers
    /// false, adding nothing, when it owes that very call already.
    pub fn owe(&mut self, node_id: &str, frames: &LoopFrames, call: &CancelCall, access: &Option<AccessRef>) -> bool {
        if self.owes(node_id, frames, call, access) {
            return false;
        }
        self.owed.push(OwedCall { node_id: node_id.to_string(), frames: frames.clone(), call: call.clone(), access: access.clone() });
        true
    }

    /// Take what the firing at `node_id`/`frames` owes, now that it ends
    /// (its `EndCallsSettled` goes on the record once they went).
    pub fn take(&mut self, node_id: &str, frames: &LoopFrames) -> Vec<OwedCall> {
        let (taken, kept) = std::mem::take(&mut self.owed).into_iter().partition(|owed| owed.of(node_id, frames));
        self.owed = kept;
        taken
    }

    /// Take everything still owed, now that the run ends.
    pub fn take_all(&mut self) -> Vec<OwedCall> {
        std::mem::take(&mut self.owed)
    }

    /// The firings that owe a call, each once, in the order they first
    /// registered one.
    pub fn owing(&self) -> Vec<(String, LoopFrames)> {
        settled_places(&self.owed)
    }

    /// The instance the run is for, as its `RunStarted` says.
    pub fn instance(&self) -> Option<&InstanceId> {
        self.instance.as_ref()
    }
}

/// The firings `owed` belong to, each once, in order: what an
/// `EndCallsSettled` is written for once they went.
pub fn settled_places(owed: &[OwedCall]) -> Vec<(String, LoopFrames)> {
    let mut places: Vec<(String, LoopFrames)> = Vec::new();
    for owed in owed {
        if !places.iter().any(|(node_id, frames)| owed.of(node_id, frames)) {
            places.push((owed.node_id.clone(), owed.frames.clone()));
        }
    }
    places
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::RunId;

    fn run() -> RunId {
        RunId::from_u128(9)
    }

    fn owed(node: &str, call: CancelCall) -> RunEvent {
        RunEvent::EndCallOwed {
            run_id: run(),
            node_id: node.into(),
            frames: Vec::new(),
            call,
            access: Some(AccessRef { id: "conn".into(), service: "provider".into(), required_values: Vec::new() }),
            at_unix: 1,
        }
    }

    /// The row rides the record as JSON with its call and connection, and
    /// reads back as written.
    #[test]
    fn an_owed_call_round_trips_on_the_record() {
        let written = vec![owed("upload", CancelCall::delete("https://api.example/uploads/1").always())];
        let row = crate::stored::encode(&written);
        let json: serde_json::Value = serde_json::from_slice(&zstd::stream::decode_all(&row[..]).unwrap()).unwrap();
        assert_eq!(
            json,
            serde_json::json!([{
                "kind": "end_call_owed",
                "node_id": "upload",
                "frames": [],
                "call": { "url": "https://api.example/uploads/1", "method": "delete", "when": "always" },
                "access": { "id": "conn", "service": "provider" },
                "at_unix": 1
            }])
        );
        let read = crate::stored::decode(run(), &row, crate::stored::STORED_EVENTS.version()).unwrap();
        assert_eq!(serde_json::to_value(&read).unwrap(), serde_json::to_value(&written).unwrap());
    }

    /// A firing owes each call once, however often its body registers it;
    /// its ending alone settles nothing, its settlement does, a firing
    /// elsewhere keeps its own, and the run's instance is read off its
    /// start.
    #[test]
    fn a_firing_owes_each_call_once_until_it_ends() {
        let upload = CancelCall::delete("https://api.example/uploads/1").always();
        let instance = InstanceId::new("acme").unwrap();
        let mut book = OwedCalls::of_record(&[
            RunEvent::RunStarted {
                given: Default::default(),
                run_id: run(),
                project_id: uuid::Uuid::nil(),
                entry_node: "a".into(),
                phase: weft_core::context::Phase::Fire,
                definition_hash: None,
                binary_hash: None,
                source_version: None,
                run_kind: weft_core::run::RunKind::Project,
                cut_from: None,
                seed: None,
                instance: Some(instance.clone()),
                stand_in: None,
                fired_trigger: None,
                instance_values: Default::default(),
                picks: Default::default(),
                settings: Default::default(),
                at_unix: 0,
            },
            RunEvent::NodeStarted { run_id: run(), node_id: "a".into(), frames: Vec::new(), at_unix: 1 },
            owed("a", upload.clone()),
            owed("a", upload.clone()),
            owed("b", CancelCall::put("https://api.example/jobs/2/cancel")),
        ]);
        assert_eq!(book.instance(), Some(&instance));
        assert!(!book.owe("a", &Vec::new(), &upload, &owed_access()), "already owed");
        book.apply(&RunEvent::NodeFailed { run_id: run(), node_id: "a".into(), frames: Vec::new(), error: "lost".into(), gave_up_wait: false, at_unix: 2 });
        assert!(book.owes("a", &Vec::new(), &upload, &owed_access()), "an ending alone settles nothing");
        book.apply(&RunEvent::EndCallsSettled { run_id: run(), node_id: "a".into(), frames: Vec::new(), at_unix: 3 });
        assert!(book.take("a", &Vec::new()).is_empty(), "settled");
        let left = book.take_all();
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].node_id, "b");
        assert!(book.take_all().is_empty());
    }

    fn owed_access() -> Option<AccessRef> {
        Some(AccessRef { id: "conn".into(), service: "provider".into(), required_values: Vec::new() })
    }
}
