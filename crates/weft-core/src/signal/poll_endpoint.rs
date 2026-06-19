//! Outbound event source (2 of 3): periodic HTTP poll. The listener hits
//! `url` every `interval_secs` and fires a fresh execution carrying the
//! response body. No persistent connection: this is the right shape for
//! APIs that only offer a "give me what's new" endpoint (long-poll or plain
//! poll), e.g. a bot getUpdates loop. The polling node owns any cursor/offset
//! bookkeeping by varying the URL it registers; the language only owns the
//! timer + fire + the listener keep-alive across worker stalls.
//!
//! For a held read-only stream see [`super::SseSubscribe`]; for a
//! bidirectional socket with a heartbeat see [`super::SocketListen`].

use serde::{Deserialize, Serialize};

use super::Signal;

/// Default poll cadence. A floor is enforced in `validate` so a node cannot
/// hammer an endpoint (cost + rate-limit protection).
pub const DEFAULT_POLL_INTERVAL_SECS: u64 = 30;
/// Minimum poll cadence. Below this the listener would generate runaway load
/// and external rate-limit bans; fail loud rather than silently clamp.
pub const MIN_POLL_INTERVAL_SECS: u64 = 5;

fn default_poll_interval_secs() -> u64 {
    DEFAULT_POLL_INTERVAL_SECS
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PollEndpoint {
    /// The endpoint to poll.
    pub url: String,
    /// Seconds between polls. Floored at [`MIN_POLL_INTERVAL_SECS`].
    #[serde(default = "default_poll_interval_secs")]
    pub interval_secs: u64,
}

impl Signal for PollEndpoint {
    const TAG: &'static str = "poll_endpoint";

    fn validate(&self) -> Result<(), String> {
        super::sse_subscribe::validate_http_url(&self.url, "poll_endpoint.url")?;
        if self.interval_secs < MIN_POLL_INTERVAL_SECS {
            return Err(format!(
                "poll_endpoint.interval_secs must be >= {MIN_POLL_INTERVAL_SECS}: a tighter \
                 poll generates runaway load and risks external rate-limit bans; got {}",
                self.interval_secs
            ));
        }
        Ok(())
    }
}

crate::register_signal_kind!(PollEndpoint);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_interval_is_stable() {
        let p: PollEndpoint =
            serde_json::from_value(serde_json::json!({ "url": "https://x/u" })).unwrap();
        assert_eq!(p.interval_secs, DEFAULT_POLL_INTERVAL_SECS);
    }

    #[test]
    fn too_tight_interval_rejected() {
        let p = PollEndpoint { url: "https://x".into(), interval_secs: 1 };
        assert!(p.validate().unwrap_err().contains("interval_secs"));
    }

    #[test]
    fn valid_round_trips() {
        let p = PollEndpoint { url: "https://x/u".into(), interval_secs: 10 };
        let spec = crate::signal::to_spec(p);
        assert_eq!(spec.kind, "poll_endpoint");
    }
}
