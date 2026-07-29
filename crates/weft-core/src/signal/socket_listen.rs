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
use crate::access::spec::MintedSocket;

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
    /// The `ws://`/`wss://` gateway URL to connect to. May be empty
    /// when `connect` mints it.
    #[serde(default)]
    pub url: String,

    /// The minted-address machinery ([`MintedSocket`]): an optional
    /// `connect` call that mints the URL (the dynamic-gateway
    /// pattern: the provider hands out a short-lived, single-use
    /// address, re-minted on every reconnect), `url_from` naming the
    /// capture that carries it, and the declarative `replies` a
    /// gateway that demands per-event acks needs. Flattened, so the
    /// wire keeps the flat `connect` / `url_from` / `replies` fields.
    #[serde(flatten)]
    pub minted: MintedSocket,

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
        // Exactly one source for the address: a static URL, or a mint
        // call. Both would leave "which one wins" to the reader, and
        // neither leaves nothing to dial.
        match (&self.minted.connect, self.url.trim().is_empty()) {
            (None, true) => {
                return Err(
                    "socket_listen needs an address: either a ws(s) `url`, or a `connect` \
                     call that mints one"
                        .into(),
                )
            }
            (Some(_), false) => {
                return Err(
                    "socket_listen declares both a static `url` and a `connect` call that \
                     mints one; keep the one that is true for this service"
                        .into(),
                )
            }
            (Some(_), true) => {}
            (None, false) => {
                if !(self.url.starts_with("ws://") || self.url.starts_with("wss://")) {
                    return Err(format!(
                        "socket_listen.url must be ws(s): got '{}'",
                        self.url
                    ));
                }
            }
        }
        self.minted.validate("socket_listen")?;
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
    use crate::access::spec::{Capture, ConnectCall, ReplyRule, Template};

    fn base(url: &str) -> SocketListen {
        SocketListen {
            url: url.into(),
            minted: MintedSocket::default(),
            handshake: None,
            heartbeat: None,
            heartbeat_secs: DEFAULT_HEARTBEAT_SECS,
        }
    }

    fn mint_call() -> ConnectCall {
        ConnectCall {
            url: Template::new("https://slack.com/api/apps.connections.open"),
            method: crate::access::spec::TestMethod::Post,
            body: None,
            auth: vec![crate::access::spec::AuthStep::Header {
                name: "Authorization".into(),
                value: Template::new("Bearer {app_token}"),
            }],
            captures: vec![Capture { name: "url".into(), path: "url".into(), optional: false }],
        }
    }

    #[test]
    fn non_ws_url_rejected() {
        assert!(base("https://gateway").validate().unwrap_err().contains("ws(s)"));
    }

    #[test]
    fn zero_heartbeat_with_frame_rejected() {
        let mut s = base("wss://gateway");
        s.heartbeat = Some(SocketFrame::Text { body: "{}".into() });
        s.heartbeat_secs = 0;
        assert!(s.validate().unwrap_err().contains("heartbeat_secs"));
    }

    /// A minted address: the authed call replaces the static URL and
    /// must capture the value the socket is dialed at.
    #[test]
    fn a_minted_address_validates_through_its_capture() {
        let mut s = base("");
        s.minted.connect = Some(mint_call());
        s.validate().expect("a mint call capturing 'url' is a complete address");

        let mut missing = s.clone();
        missing.minted.connect.as_mut().unwrap().captures.clear();
        let err = missing.validate().unwrap_err();
        assert!(err.contains("'url'"), "{err}");
    }

    /// Exactly one source of truth for the address: neither is
    /// nothing to dial, both is an unanswerable question.
    #[test]
    fn the_address_has_exactly_one_source() {
        let err = base("").validate().unwrap_err();
        assert!(err.contains("either"), "{err}");

        let mut both = base("wss://gateway");
        both.minted.connect = Some(mint_call());
        let err = both.validate().unwrap_err();
        assert!(err.contains("both"), "{err}");
    }

    #[test]
    fn a_gateway_shaped_config_round_trips() {
        let mut s = base("wss://gateway.discord.gg/?v=10&encoding=json");
        s.handshake = Some(SocketFrame::Text { body: "{\"op\":2}".into() });
        s.heartbeat = Some(SocketFrame::Text { body: "{\"op\":1}".into() });
        s.heartbeat_secs = 45;
        s.minted.replies = vec![ReplyRule {
            when_field: "envelope_id".into(),
            frame: "{\"envelope_id\":\"{value}\"}".into(),
        }];
        s.validate().expect("valid");
        let spec = crate::signal::to_spec(s.clone());
        assert_eq!(spec.kind, "socket_listen");
        let back: SocketListen = serde_json::from_value(spec.config).unwrap();
        assert_eq!(back.heartbeat_secs, 45);
        assert_eq!(back.minted.replies.len(), 1);
        assert_eq!(back.handshake, Some(SocketFrame::Text { body: "{\"op\":2}".into() }));
    }

    /// The ack rule is pure: a frame carrying the watched field
    /// answers with the value interpolated; one that does not carry
    /// it (or carries null) answers nothing.
    #[test]
    fn a_reply_rule_echoes_the_watched_field() {
        let rule = ReplyRule {
            when_field: "envelope_id".into(),
            frame: "{\"envelope_id\":\"{value}\"}".into(),
        };
        assert_eq!(
            rule.reply_to(&serde_json::json!({ "envelope_id": "e-1" })).as_deref(),
            Some("{\"envelope_id\":\"e-1\"}")
        );
        assert!(rule.reply_to(&serde_json::json!({ "other": 1 })).is_none());
        assert!(rule.reply_to(&serde_json::json!({ "envelope_id": null })).is_none());
    }
}
