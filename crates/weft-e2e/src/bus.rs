//! Bus conversation assertions over a settled run's replay.
//!
//! A bus shows up in the event log as `bus_joined` / `bus_window` /
//! `bus_left` / `bus_closed` events (plus `bus_participant` graph-wiring
//! markers). These helpers read those out of a [`SettledRun`] so a test can
//! assert "these participants joined", "this message was sent", "the bus
//! closed", without JSON spelunking.
//!
//! A `bus_window` row aggregates one journal window of messages: its
//! `messages` list carries every message for a journaled bus (payloads
//! tagged `{ "kind": "json", "data": <v> }` or `{ "kind": "bytes",
//! "data": "<base64>" }`), and is empty for an ephemeral bus, whose
//! `totals` rollup (count + bytes per sender/kind) is the whole story.
//! The accessors below unpack the windows back into per-message rows so
//! tests read messages, not windows.

use anyhow::{bail, Result};
use serde_json::Value;

use crate::run::SettledRun;

/// One bus message read from the replay.
#[derive(Debug, Clone)]
pub struct BusMessage {
    pub bus_id: String,
    pub from: String,
    pub msg_kind: String,
    /// The journaled value, if the bus is journaled; `None` for ephemeral
    /// (where the journal carries only the totals rollup).
    pub value: Option<Value>,
}

impl SettledRun {
    /// The distinct bus ids that had a join in this run.
    pub fn bus_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self
            .replay
            .by_kind("bus_joined")
            .filter_map(|e| e.str_field("bus_id").map(str::to_string))
            .collect();
        ids.sort();
        ids.dedup();
        ids
    }

    /// The registered names that joined `bus_id`, in event order.
    pub fn bus_participants(&self, bus_id: &str) -> Vec<String> {
        self.replay
            .by_kind("bus_joined")
            .filter(|e| e.str_field("bus_id") == Some(bus_id))
            .filter_map(|e| e.str_field("name").map(str::to_string))
            .collect()
    }

    /// Every message on `bus_id`, in event order (windows unpacked).
    pub fn bus_messages(&self, bus_id: &str) -> Vec<BusMessage> {
        self.replay
            .by_kind("bus_window")
            .filter(|e| e.str_field("bus_id") == Some(bus_id))
            .flat_map(|e| {
                e.field("messages")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default()
            })
            .map(|m| BusMessage {
                bus_id: bus_id.to_string(),
                from: m.get("from").and_then(Value::as_str).unwrap_or_default().to_string(),
                msg_kind: m
                    .get("msg_kind")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                // payload is { kind: json, data } or { kind: bytes, data: b64 };
                // tests read the JSON shape.
                value: m
                    .get("payload")
                    .filter(|p| p.get("kind").and_then(Value::as_str) == Some("json"))
                    .and_then(|p| p.get("data"))
                    .cloned(),
            })
            .collect()
    }

    /// True if `bus_id` was explicitly closed.
    pub fn bus_closed(&self, bus_id: &str) -> bool {
        self.replay
            .by_kind("bus_closed")
            .any(|e| e.str_field("bus_id") == Some(bus_id))
    }

    /// Assert a bus conversation happened: at least one bus, with at least
    /// `min_participants` participants and at least `min_messages` messages on
    /// it. Returns the bus id it asserted on. The broad "a bus conversation
    /// occurred" check; specific tests then read [`SettledRun::bus_messages`].
    pub fn assert_bus_conversation(
        &self,
        min_participants: usize,
        min_messages: usize,
    ) -> Result<String> {
        let Some(bus_id) = self.bus_ids().into_iter().next() else {
            bail!(
                "expected a bus conversation, but no bus_joined events in run {}",
                self.color
            );
        };
        let parts = self.bus_participants(&bus_id);
        if parts.len() < min_participants {
            bail!(
                "bus {bus_id}: expected >= {min_participants} participants, got {} ({parts:?})",
                parts.len()
            );
        }
        let msgs = self.bus_messages(&bus_id);
        if msgs.len() < min_messages {
            bail!(
                "bus {bus_id}: expected >= {min_messages} messages, got {}",
                msgs.len()
            );
        }
        Ok(bus_id)
    }
}
