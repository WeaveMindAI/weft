//! Outbound event source (1 of 3): a long-lived Server-Sent Events
//! subscription. The listener opens a `GET` with `Accept: text/event-stream`,
//! parses SSE blocks, and fires a fresh execution per event whose `event:`
//! name matches `event_name` (or every event if `event_name` is empty).
//!
//! Receive-only: the listener never writes back up the connection. For a
//! source that needs a periodic poll instead of a held stream see
//! [`super::PollEndpoint`]; for a bidirectional socket with a heartbeat
//! (Discord/Slack gateway shape) see [`super::SocketListen`]. All three
//! share the listener-side fire + reconnect-backoff plumbing.

use serde::{Deserialize, Serialize};

use super::Signal;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SseSubscribe {
    /// The event-stream URL to subscribe to.
    pub url: String,
    /// Only fire on events with this `event:` name. Empty = every event.
    #[serde(default)]
    pub event_name: String,
}

impl Signal for SseSubscribe {
    const TAG: &'static str = "sse_subscribe";

    fn validate(&self) -> Result<(), String> {
        validate_http_url(&self.url, "sse_subscribe.url")
    }
}

/// Shared http(s)-URL check used by the outbound event-source kinds.
pub(super) fn validate_http_url(url: &str, field: &str) -> Result<(), String> {
    if url.trim().is_empty() {
        return Err(format!("{field} must not be empty"));
    }
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err(format!("{field} must be http(s): got '{url}'"));
    }
    Ok(())
}

crate::register_signal_kind!(SseSubscribe);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_url_rejected() {
        let s = SseSubscribe { url: "".into(), event_name: "".into() };
        assert!(s.validate().is_err());
    }

    #[test]
    fn non_http_url_rejected() {
        let s = SseSubscribe { url: "ftp://x".into(), event_name: "".into() };
        assert!(s.validate().unwrap_err().contains("http(s)"));
    }

    #[test]
    fn valid_round_trips() {
        let s = SseSubscribe { url: "https://x/stream".into(), event_name: "tick".into() };
        let spec = crate::signal::to_spec(s);
        assert_eq!(spec.kind, "sse_subscribe");
    }
}
