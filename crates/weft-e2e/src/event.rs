//! The run event stream, as the rig reads it.
//!
//! `GET /runs/{run_id}/replay?with_values=true` returns a JSON array of
//! events with their values written in ([`ValuedEvent`], a `{ "kind":
//! "...", ... }` tagged union). The fetch decodes every row as that type first, so a shape the
//! dispatcher and weft-core disagree on fails there. An [`Event`] is then a
//! thin accessor over the row's JSON: it exposes `kind()` and reads the
//! handful of fields the assertions need by name (the node is `node`, NOT
//! `node_id`; run outputs are on `run_completed.outputs`;
//! per-node values on `node_completed.output`), and a failed read carries
//! the real JSON.

use serde_json::Value;

/// A replay row with its values written in, as the rig asks for it.
pub type ValuedEvent = weft_core::live_event::IdentifiedEvent<weft_core::live_event::DispatcherEvent<Value>>;

/// One event from the replay stream. Wraps the raw tagged-union JSON.
#[derive(Debug, Clone)]
pub struct Event(pub Value);

impl Event {
    /// The variant tag (`run_completed`, `node_skipped`, ...). Empty
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

    /// The `node` field (LiveEvent's node identifier on node_* events).
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

    /// The call sites this event's frames carry, outermost first: which
    /// use of an included file the firing belongs to.
    fn call_path(&self) -> Vec<String> {
        self.frames().as_array().map(|frames| frames.iter()
            .filter_map(|f| f.get("site").and_then(Value::as_str).map(str::to_string)).collect())
            .unwrap_or_default()
    }

    /// True when this event names `node` under exactly the call `path`
    /// (an event without a frames field, such as a cost row, is taken at
    /// the top).
    pub(crate) fn is_node_at(&self, node: &str, path: &[String]) -> bool {
        self.is_node(node) && self.call_path() == path
    }
}

/// The full ordered replay for one run. The rig's single read-back
/// surface: every assertion folds or scans this.
#[derive(Debug, Clone)]
pub struct Replay {
    pub events: Vec<Event>,
}

impl Replay {
    /// Parse the JSON array returned by `/runs/{run_id}/replay`.
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

    /// True once the run has ended: some event is a run's ending,
    /// by the one rule weft-core keeps (`DispatcherEvent::is_run_terminal`).
    /// A row that does not decode as a [`ValuedEvent`] is an error, never
    /// "not ended": a changed replay shape must fail by name, not time out.
    pub fn has_terminal(&self) -> anyhow::Result<bool> {
        for e in &self.events {
            let live: ValuedEvent = serde_json::from_value(e.0.clone())
                .map_err(|err| anyhow::anyhow!("a replay row does not decode as an event ({err}): {}", e.0))?;
            if live.event.is_run_terminal() {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

