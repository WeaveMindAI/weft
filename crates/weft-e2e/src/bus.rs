//! Bus conversation assertions over a settled run's replay.
//!
//! A bus shows up in the event log as `bus_joined` / `bus_message` /
//! `bus_left` / `bus_closed` events (plus `bus_participant` graph-wiring
//! markers). These helpers read those out of a [`SettledRun`] so a test can
//! assert "these participants joined", "this message was sent", "the bus
//! closed", without JSON spelunking.
//!
//! A `bus_message` payload is a tagged `JournaledPayload`: `{ "kind":
//! "journaled", "value": <v> }` for journaled buses, `{ "kind": "ephemeral" }`
//! (metadata only) for ephemeral ones. The accessors expose the journaled
//! value when present; an ephemeral message carries no value, only size + hash.

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
    /// (where the log carries only size + sha prefix).
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

    /// Every message on `bus_id`, in event order.
    pub fn bus_messages(&self, bus_id: &str) -> Vec<BusMessage> {
        self.replay
            .by_kind("bus_message")
            .filter(|e| e.str_field("bus_id") == Some(bus_id))
            .map(|e| BusMessage {
                bus_id: bus_id.to_string(),
                from: e.str_field("from").unwrap_or_default().to_string(),
                msg_kind: e.str_field("msg_kind").unwrap_or_default().to_string(),
                // payload is { kind: journaled, value } or { kind: ephemeral }.
                value: e
                    .field("payload")
                    .and_then(|p| p.get("value"))
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
