//! Outbound event source (3 of 3): a persistent BIDIRECTIONAL WebSocket the
//! listener dials OUT and keeps alive, firing a fresh execution per inbound
//! frame. This is the shape a gateway-style integration needs (Discord,
//! Slack socket mode): unlike [`super::SseSubscribe`] (receive-only) the
//! listener must WRITE up the socket, both an initial handshake (auth /
//! subscribe) and a periodic heartbeat, or the remote drops the connection.
//!
//! Kept generic: the language owns "hold the socket, send these frames on
//! this schedule, fire on inbound, reconnect on drop." The SERVICE-SPECIFIC
//! protocol (which op-codes mean what, how to compute the next heartbeat
//! payload from the last sequence number) is the node's concern, expressed
//! as the literal frames it puts in `handshake` / `heartbeat`. The node that
//! needs reply-driven heartbeats (Discord echoes a sequence) drives them by
//! treating the fired inbound frames as its own protocol and is free to keep
//! the static heartbeat as the keepalive floor.

use serde::{Deserialize, Serialize};

use super::Signal;

/// Default heartbeat cadence for an outbound gateway socket. Most gateways
/// advertise their own interval in the hello frame; this is the floor used
/// when the node sets a static heartbeat without negotiating one.
pub const DEFAULT_HEARTBEAT_SECS: u64 = 30;

fn default_heartbeat_secs() -> u64 {
    DEFAULT_HEARTBEAT_SECS
}

/// A frame the listener sends up the socket. Text or binary, matching the
/// gateway's expected encoding.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "encoding", rename_all = "snake_case")]
pub enum SocketFrame {
    /// A UTF-8 text frame (the common case: JSON gateway protocols).
    Text { body: String },
    /// A raw binary frame (base64 on the wire so the spec stays JSON).
    Binary { base64: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SocketListen {
    /// The `ws://`/`wss://` gateway URL to connect to.
    pub url: String,

    /// Optional frame sent once immediately after the socket opens (e.g. a
    /// Discord `identify` / Slack subscribe). `None` = send nothing on open.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handshake: Option<SocketFrame>,

    /// Optional frame resent every `heartbeat_secs` to keep the connection
    /// alive (e.g. a gateway heartbeat op). `None` = rely on protocol-level
    /// ping/pong only (no app-level heartbeat).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heartbeat: Option<SocketFrame>,

    /// Heartbeat cadence in seconds. Ignored when `heartbeat` is `None`.
    #[serde(default = "default_heartbeat_secs")]
    pub heartbeat_secs: u64,
}

impl Signal for SocketListen {
    const TAG: &'static str = "socket_listen";

    fn validate(&self) -> Result<(), String> {
        if self.url.trim().is_empty() {
            return Err("socket_listen.url must not be empty".into());
        }
        if !(self.url.starts_with("ws://") || self.url.starts_with("wss://")) {
            return Err(format!(
                "socket_listen.url must be ws(s): got '{}'",
                self.url
            ));
        }
        if self.heartbeat.is_some() && self.heartbeat_secs == 0 {
            return Err(
                "socket_listen.heartbeat_secs must be > 0 when a heartbeat frame is set: \
                 a zero interval would spin the heartbeat loop"
                    .into(),
            );
        }
        Ok(())
    }
}

crate::register_signal_kind!(SocketListen);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_ws_url_rejected() {
        let s = SocketListen {
            url: "https://gateway".into(),
            handshake: None,
            heartbeat: None,
            heartbeat_secs: DEFAULT_HEARTBEAT_SECS,
        };
        assert!(s.validate().unwrap_err().contains("ws(s)"));
    }

    #[test]
    fn zero_heartbeat_with_frame_rejected() {
        let s = SocketListen {
            url: "wss://gateway".into(),
            handshake: None,
            heartbeat: Some(SocketFrame::Text { body: "{}".into() }),
            heartbeat_secs: 0,
        };
        assert!(s.validate().unwrap_err().contains("heartbeat_secs"));
    }

    #[test]
    fn discord_shaped_config_round_trips() {
        let s = SocketListen {
            url: "wss://gateway.discord.gg/?v=10&encoding=json".into(),
            handshake: Some(SocketFrame::Text { body: "{\"op\":2}".into() }),
            heartbeat: Some(SocketFrame::Text { body: "{\"op\":1}".into() }),
            heartbeat_secs: 45,
        };
        let spec = crate::signal::to_spec(s.clone());
        assert_eq!(spec.kind, "socket_listen");
        let back: SocketListen = serde_json::from_value(spec.config).unwrap();
        assert_eq!(back.heartbeat_secs, 45);
        assert_eq!(back.handshake, Some(SocketFrame::Text { body: "{\"op\":2}".into() }));
    }
}
