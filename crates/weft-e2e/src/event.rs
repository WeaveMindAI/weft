//! The execution event stream, as the rig reads it.
//!
//! `GET /executions/{color}/replay` returns a JSON array of the dispatcher's
//! `DispatcherEvent` (a `{ "kind": "...", ... }` tagged union; see
//! `crates/weft-dispatcher/src/events.rs`). The rig does NOT re-declare that
//! 35-variant enum: doing so would fork the concept and create a large SYNC
//! surface that rots every time a variant changes. Instead an [`Event`] is a
//! thin typed accessor over the raw tagged JSON: it exposes `kind()` and reads
//! the handful of fields the assertions need BY NAME. A field the server
//! renames surfaces as a loud assertion failure carrying the real JSON, which
//! is the honest Layer-4 contract (the rig tests the wire shape, it does not
//! get to assume it).
//!
//! SYNC (loose, by string tag + field name, not by type):
//! `kind` values and field names here mirror
//! `crates/weft-dispatcher/src/events.rs::DispatcherEvent`
//! (`#[serde(tag = "kind", rename_all = "snake_case")]`). The node identifier
//! field is `node` there (NOT `node_id`); execution outputs are on
//! `execution_completed.outputs`; per-node values on `node_completed.output`.

use serde_json::Value;

/// One event from the replay stream. Wraps the raw tagged-union JSON.
#[derive(Debug, Clone)]
pub struct Event(pub Value);

impl Event {
    /// The variant tag (`execution_completed`, `node_skipped`, ...). Empty
    /// string if the row has no `kind` (which would itself be a server bug the
    /// assertions then flag).
    pub fn kind(&self) -> &str {
        self.0.get("kind").and_then(Value::as_str).unwrap_or("")
    }

    /// A string field by name, if present.
    pub fn str_field(&self, name: &str) -> Option<&str> {
        self.0.get(name).and_then(Value::as_str)
    }

    /// A JSON field by name, if present.
    pub fn field(&self, name: &str) -> Option<&Value> {
        self.0.get(name)
    }

    /// The `node` field (DispatcherEvent's node identifier on node_* events).
    pub fn node(&self) -> Option<&str> {
        self.str_field("node")
    }

    /// The `frames` array (loop iteration stack); empty `[]` at root. Returned
    /// as the raw JSON so callers can compare iteration depth / index without
    /// the rig re-deriving the LoopFrames type.
    pub fn frames(&self) -> &Value {
        self.0.get("frames").unwrap_or(&Value::Null)
    }

    /// True when this event names `node` at any frame stack.
    pub fn is_node(&self, node: &str) -> bool {
        self.node() == Some(node)
    }
}

/// The full ordered replay for one execution. The rig's single read-back
/// surface: every assertion folds or scans this.
#[derive(Debug, Clone)]
pub struct Replay {
    pub events: Vec<Event>,
}

impl Replay {
    /// Parse the JSON array returned by `/executions/{color}/replay`.
    pub fn from_array(raw: Vec<Value>) -> Self {
        Self {
            events: raw.into_iter().map(Event).collect(),
        }
    }

    /// Every event with the given `kind`, in order.
    pub fn by_kind<'a>(&'a self, kind: &'a str) -> impl Iterator<Item = &'a Event> + 'a {
        self.events.iter().filter(move |e| e.kind() == kind)
    }

    /// The first event with the given `kind`, if any. The returned reference
    /// borrows from `self`, independent of `kind`'s lifetime.
    pub fn first_kind(&self, kind: &str) -> Option<&Event> {
        self.events.iter().find(|e| e.kind() == kind)
    }

    /// Every event naming `node`, any kind, in order.
    pub fn for_node<'a>(&'a self, node: &'a str) -> impl Iterator<Item = &'a Event> + 'a {
        self.events.iter().filter(move |e| e.is_node(node))
    }

    /// True if any event has a kind in `kinds`.
    pub fn has_any_kind(&self, kinds: &[&str]) -> bool {
        self.events.iter().any(|e| kinds.contains(&e.kind()))
    }
}

/// The terminal-event kinds. An execution is settled once exactly one of these
/// appears (the journal writes exactly one, guarded). SYNC with the
/// `execution_*` variants of DispatcherEvent.
pub const TERMINAL_KINDS: [&str; 3] = [
    "execution_completed",
    "execution_failed",
    "execution_cancelled",
];
