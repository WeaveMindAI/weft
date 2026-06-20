//! Shared plumbing for the outbound event-source kinds (`SseSubscribe`,
//! `PollEndpoint`, `SocketListen`). All three hold or poll an external
//! source and fire a fresh execution per event; what differs is HOW they
//! read (held SSE stream vs periodic poll vs bidirectional socket). The
//! fire path and the reconnect-backoff ladder are identical, so they live
//! here once and the three handlers call in.

use serde_json::Value;
use tokio::time::{sleep, Duration};
use tracing::warn;

use crate::fire_sink::FireSignalSink;

/// Coerce an event's raw text into the JSON fire payload: parse it as JSON,
/// or wrap the raw text as a JSON string. The fire pipeline is JSON-typed
/// end to end, so every event source funnels text through here.
pub fn coerce_text_payload(text: String) -> Value {
    serde_json::from_str::<Value>(&text).unwrap_or(Value::String(text))
}

/// Fire one event payload, logging (not propagating) an enqueue failure:
/// a dropped fire must not kill the event-source loop, but is never silent.
/// `target` is the caller's tracing target so the log names the right kind.
/// `tenant_id` is the signal's tenant, stamped on the enqueued fire (a
/// pooled listener serves many tenants, so it travels per-signal).
/// `placement_generation` is the generation this pod holds the signal under,
/// stamped on the fire so the broker can fence a stale old-pod fire during a
/// scale-down move overlap.
pub async fn fire_payload(
    sink: &FireSignalSink,
    token: &str,
    tenant_id: &str,
    placement_generation: i64,
    payload: Value,
    target: &str,
) {
    if let Err(e) = sink.fire(token, tenant_id, placement_generation, payload).await {
        warn!(target: "weft_listener::event_source", kind = target, %token, error = %e, "fire enqueue failed");
    }
}

/// Exponential reconnect backoff, shared by every event source. Starts at
/// 1s, doubles per failure, caps at 60s. A connection that stayed healthy
/// long enough to matter resets the ladder so a flapping endpoint is not
/// hammered while a genuinely-recovered one retries promptly.
pub struct Backoff {
    current_secs: u64,
}

/// A connection must stay up at least this long to count as "healthy" and
/// reset the backoff ladder. Below this we treat reconnect as part of a
/// flap and keep climbing.
const HEALTHY_THRESHOLD_SECS: u64 = 30;
const MAX_BACKOFF_SECS: u64 = 60;

impl Backoff {
    pub fn new() -> Self {
        Self { current_secs: 1 }
    }

    /// Sleep for the current backoff, then climb. Call after a failed or
    /// dropped connection before retrying.
    pub async fn wait_then_climb(&mut self) {
        sleep(Duration::from_secs(self.current_secs)).await;
        self.current_secs = (self.current_secs * 2).min(MAX_BACKOFF_SECS);
    }

    /// Reset to the floor if the connection stayed up long enough to be
    /// considered healthy. `uptime` is how long the just-dropped connection
    /// lasted.
    pub fn reset_if_healthy(&mut self, uptime: Duration) {
        if uptime >= Duration::from_secs(HEALTHY_THRESHOLD_SECS) {
            self.current_secs = 1;
        }
    }
}

impl Default for Backoff {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_climbs_and_caps() {
        let mut b = Backoff::new();
        assert_eq!(b.current_secs, 1);
        // Simulate the climb without sleeping by replicating the doubling.
        for expected in [2u64, 4, 8, 16, 32, 60, 60] {
            b.current_secs = (b.current_secs * 2).min(MAX_BACKOFF_SECS);
            assert_eq!(b.current_secs, expected);
        }
    }

    #[test]
    fn healthy_uptime_resets_ladder() {
        let mut b = Backoff { current_secs: 32 };
        b.reset_if_healthy(Duration::from_secs(5));
        assert_eq!(b.current_secs, 32, "short uptime keeps climbing");
        b.reset_if_healthy(Duration::from_secs(31));
        assert_eq!(b.current_secs, 1, "healthy uptime resets to floor");
    }
}
