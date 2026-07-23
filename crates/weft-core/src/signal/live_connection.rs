//! Live caller connections: making a weft program a real live endpoint.
//! An outside caller hits a stable public URL, the dispatcher authenticates
//! and points the caller at a worker through the shared gateway, and any node
//! in the running program talks back over the held connection (reply once,
//! stream, or hold a two-way conversation).
//!
//! TWO user-facing signal kinds, because a node developer programs against
//! two genuinely different talk surfaces:
//!   - [`ApiEndpoint`]: inbound HTTP. The caller makes one request; the node
//!     replies once or streams a response (`HttpCaller`: respond / write /
//!     close). The caller is pointed at the worker by a `307` redirect.
//!   - [`LiveSocket`]: inbound WebSocket. Full two-way conversation
//!     (`WsCaller`: send / receive / request / close). The caller fetches the
//!     worker URL then opens the real socket to it (WS cannot be redirected).
//!
//! Both share their entire config body ([`LiveConnectionConfig`], flattened)
//! and the entire transport engine behind them (control handshake, gateway
//! routing, connect barrier, disconnect policy, reconciliation, heartbeat,
//! caps, journaling). The ONLY thing the kind distinction carries is the
//! wire [`Protocol`], which the runtime derives from the kind's TAG, never
//! from node identity, so the language stays generic.

use serde::{Deserialize, Serialize};

use super::Signal;

/// Wire protocol the caller speaks. A runtime value (the engine branches on
/// it at the two narrow protocol-specific edges), derived from which signal
/// kind fired ([`ApiEndpoint`] -> `Http`, [`LiveSocket`] -> `Websocket`),
/// NOT carried as a user-facing config field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    /// Request/response (or chunked streaming response) over plain HTTP.
    Http,
    /// Full-duplex WebSocket.
    Websocket,
}

impl Protocol {
    pub fn as_wire_str(&self) -> &'static str {
        match self {
            Protocol::Http => "http",
            Protocol::Websocket => "websocket",
        }
    }
}

/// How outbound talk behaves when the connection is congested (a slow
/// caller). A multiplexing pod must never OOM on one slow caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Backpressure {
    /// `write`/`send` awaits when the bounded outbound buffer is full, so
    /// a slow caller slows the producer rather than growing RAM.
    #[default]
    Block,
    /// Shed the oldest buffered chunk to make room (video-style: stale
    /// frames are worthless).
    DropOldest,
    /// Shed the incoming chunk when the buffer is full (keep what is
    /// already queued).
    DropNewest,
}

/// What happens to the caller when a node errors mid-exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ErrorMode {
    /// Surface the error to the caller. HTTP before any bytes = a real
    /// error status + body; HTTP after streaming started = in-band error
    /// chunk then close; WebSocket = close frame with code + reason.
    #[default]
    Surface,
    /// Drop the failed chunk and continue, for streams where a dropped
    /// frame is tolerable. Never a SILENT truncation: this mode is the
    /// explicit opt-in to tolerate it.
    DropChunk,
}

/// The shape of the bytes flowing in/out. The node works in a general
/// send-X / receive-X and the connection layer converts to/from this wire
/// shape. Nodes can query the declared type to branch (send bytes vs JSON).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    /// Bodies/messages are JSON values.
    #[default]
    Json,
    /// Bodies/messages are UTF-8 text.
    Text,
    /// Bodies/messages are raw bytes (binary WS frames / opaque HTTP body).
    Bytes,
}

/// Whether the caller exchange is PERSISTED to the journal/DB for replay.
/// This is ONLY about durability; it does NOT control the in-RAM window
/// (that is the separate `window` field, which bounds RAM in both modes,
/// cursors only ever read RAM). Mirrors the bus's journaled-vs-ephemeral
/// distinction (see `weft_core::bus::BusMode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum JournalMode {
    /// Full payload persisted to the journal/DB (replay survives a worker
    /// dying). The in-RAM window still bounds what a live cursor can reach.
    #[default]
    Journaled,
    /// Metadata-only in the journal (size + sha prefix); payloads are never
    /// persisted, so they live only in the in-RAM window.
    Ephemeral,
}

/// Default heartbeat interval. Clears the ~30s cellular-NAT floor and
/// sits under the ~60s proxy default, so one ping keeps both the gateway
/// hop and the caller last-mile alive.
pub const DEFAULT_HEARTBEAT_INTERVAL_SECS: u64 = 25;

/// Default per-request/message inbound size cap. Generous for chat-shaped
/// payloads, loud for accidental uploads. Untrusted-caller abuse vector.
pub const DEFAULT_MAX_INBOUND_BYTES: u64 = 16 * 1_048_576;

/// Default how long the connect barrier waits for the caller to actually
/// arrive after the worker is woken. Worker-clock driven so a vanished
/// caller cannot pin a worker.
pub const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 30;

fn default_heartbeat_interval_secs() -> u64 {
    DEFAULT_HEARTBEAT_INTERVAL_SECS
}
fn default_max_inbound_bytes() -> u64 {
    DEFAULT_MAX_INBOUND_BYTES
}
fn default_connect_timeout_secs() -> u64 {
    DEFAULT_CONNECT_TIMEOUT_SECS
}

/// The shared config body for both live-caller kinds. Flattened into
/// [`ApiEndpoint`] and [`LiveSocket`] so the two user-facing kinds stay
/// distinct names while their behavioral knobs are defined exactly once
/// (DRY internals behind a clear surface). Every knob has a sane default
/// (programming-language principle: maximize customization, never hardcode
/// policy). Note there is NO `protocol` field here: the protocol is the
/// kind, derived from the TAG.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveConnectionConfig {
    /// Route suffix under the gateway root. Empty means root. Must not
    /// start with `/`.
    pub path: String,

    /// Auth policy at the control handshake. `None` = anyone with the URL
    /// can connect, `OptionalApiKey` = the listener mints a key and stores
    /// its hash on the row.
    #[serde(default)]
    pub auth: super::PublicEntryAuth,

    /// Reusable suspension defaults (`can_suspend` + `default_hold_secs`),
    /// flattened so the wire shape stays flat. The generic `await`
    /// machinery resolves the effective wait policy from it. When absent,
    /// the inner fields' serde defaults apply (not suspendable,
    /// language-default hold), the right default for an interactive endpoint.
    #[serde(flatten)]
    pub suspend: crate::wait::SuspendPolicy,

    /// How long the connect barrier waits for the caller to arrive before
    /// failing loud. Worker-clock driven.
    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,

    /// Worker-side heartbeat interval. `0` disables (opt-out only). WS =
    /// protocol ping/pong; HTTP = chunked keep-alive trickle.
    #[serde(default = "default_heartbeat_interval_secs")]
    pub heartbeat_interval_secs: u64,

    /// Reject an inbound request body / WS message larger than this, loud.
    #[serde(default = "default_max_inbound_bytes")]
    pub max_inbound_bytes: u64,

    /// Max total session duration (separate from the idle/heartbeat
    /// window). `0` disables the cap.
    #[serde(default)]
    pub max_session_secs: u64,

    /// Declared inbound/outbound data shape; queryable by nodes.
    #[serde(default)]
    pub data_type: DataType,

    /// Outbound congestion behavior.
    #[serde(default)]
    pub backpressure: Backpressure,

    /// Mid-exchange error behavior toward the caller.
    #[serde(default)]
    pub error_mode: ErrorMode,

    /// How the caller exchange is journaled for replay.
    #[serde(default)]
    pub journal_mode: JournalMode,

    /// In-RAM inbound window size (how many recent messages a WebSocket
    /// retains for cursors). Bounds RAM regardless of `journal_mode`
    /// (cursors only read this window, never the DB; journaled just also
    /// persists each message for durability). `None` falls back to
    /// `weft_core::caller::DEFAULT_INBOUND_WINDOW`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<usize>,
}

impl LiveConnectionConfig {
    /// Build the shared body from a catalog node's config field map (the
    /// common authoring fields both live-caller nodes expose). Centralized
    /// here so the two nodes (`ApiEndpoint`, `LiveSocket`) do NOT each
    /// re-implement field parsing; every non-exposed knob keeps its default.
    /// `fields` is the node's `ctx.inputs.object()` map.
    pub fn from_node_fields(
        fields: &serde_json::Map<String, serde_json::Value>,
    ) -> Self {
        let path = fields.get("path").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let auth = if fields.get("generateApiKey").and_then(|v| v.as_bool()).unwrap_or(false) {
            super::PublicEntryAuth::OptionalApiKey
        } else {
            super::PublicEntryAuth::None
        };
        let can_suspend = fields.get("canSuspend").and_then(|v| v.as_bool()).unwrap_or(false);
        let default_hold_secs = fields
            .get("defaultHoldSecs")
            .and_then(|v| v.as_u64())
            .unwrap_or(crate::wait::LANGUAGE_DEFAULT_HOLD_SECS);
        Self {
            path,
            auth,
            suspend: crate::wait::SuspendPolicy { can_suspend, default_hold_secs },
            connect_timeout_secs: DEFAULT_CONNECT_TIMEOUT_SECS,
            heartbeat_interval_secs: DEFAULT_HEARTBEAT_INTERVAL_SECS,
            max_inbound_bytes: DEFAULT_MAX_INBOUND_BYTES,
            max_session_secs: 0,
            data_type: DataType::default(),
            backpressure: Backpressure::default(),
            error_mode: ErrorMode::default(),
            journal_mode: JournalMode::default(),
            window: None,
        }
    }

    /// Shared validation for both kinds. `kind_tag` only flavors the error
    /// messages so they name the actual kind the author used.
    fn validate(&self, kind_tag: &str) -> Result<(), String> {
        if self.path.starts_with('/') {
            return Err(format!(
                "{kind_tag} path must not start with '/': got '{}'",
                self.path
            ));
        }
        if self.connect_timeout_secs == 0 {
            return Err(format!(
                "{kind_tag} connect_timeout_secs must be > 0: an unbounded wait for \
                 the caller would let a vanished caller pin a worker"
            ));
        }
        if self.suspend.default_hold_secs == 0 {
            return Err(format!(
                "{kind_tag} default_hold_secs must be > 0: a zero hold means a wait \
                 that holds would give up instantly; set it to a real bound (a wait \
                 that wants to suspend immediately uses a per-call override, not a \
                 zero hold)"
            ));
        }
        if self.max_inbound_bytes == 0 {
            return Err(format!(
                "{kind_tag} max_inbound_bytes must be > 0: a zero cap rejects every \
                 request; disable the cap is not a goal, set it generously"
            ));
        }
        if let Some(w) = self.window {
            if w == 0 {
                return Err(format!(
                    "{kind_tag} window must be > 0 when set: a zero window \
                     evicts every payload immediately"
                ));
            }
        }
        Ok(())
    }
}

/// Inbound HTTP live endpoint. The caller makes one request and the node
/// streams or replies via the `HttpCaller` handle. See [`LiveConnectionConfig`]
/// for the knobs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiEndpoint {
    #[serde(flatten)]
    pub common: LiveConnectionConfig,
}

/// Inbound WebSocket live endpoint. Full two-way conversation via the
/// `WsCaller` handle. See [`LiveConnectionConfig`] for the knobs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveSocket {
    #[serde(flatten)]
    pub common: LiveConnectionConfig,
}

impl Signal for ApiEndpoint {
    const TAG: &'static str = "api_endpoint";
    fn validate(&self) -> Result<(), String> {
        self.common.validate(Self::TAG)
    }
}

impl Signal for LiveSocket {
    const TAG: &'static str = "live_socket";
    fn validate(&self) -> Result<(), String> {
        self.common.validate(Self::TAG)
    }
}

impl ApiEndpoint {
    /// The protocol this kind always speaks. The runtime derives `Protocol`
    /// from the kind, never from a config field.
    pub const PROTOCOL: Protocol = Protocol::Http;
}

impl LiveSocket {
    pub const PROTOCOL: Protocol = Protocol::Websocket;
}

/// Map a live-caller signal tag onto its wire protocol. The single place
/// the kind->protocol relationship lives, used by the dispatcher and worker
/// to recover the protocol from a fired spec without a config field. Returns
/// `None` for any non-live-caller tag.
pub fn protocol_for_tag(tag: &str) -> Option<Protocol> {
    match tag {
        t if t == ApiEndpoint::TAG => Some(Protocol::Http),
        t if t == LiveSocket::TAG => Some(Protocol::Websocket),
        _ => None,
    }
}

crate::register_signal_kind!(ApiEndpoint);
crate::register_signal_kind!(LiveSocket);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signal::PublicEntryAuth;

    /// A bare config (only the required field) round-trips and every
    /// optional knob lands on its documented default. The wire contract the
    /// catalog node and the listener both rely on.
    #[test]
    fn defaults_are_stable() {
        let json = serde_json::json!({ "path": "chat" });
        let ep: ApiEndpoint = serde_json::from_value(json).expect("deserialize");
        let c = &ep.common;
        assert_eq!(c.path, "chat");
        assert!(matches!(c.auth, PublicEntryAuth::None));
        assert!(!c.suspend.can_suspend);
        assert_eq!(c.suspend.default_hold_secs, crate::wait::LANGUAGE_DEFAULT_HOLD_SECS);
        assert_eq!(c.connect_timeout_secs, DEFAULT_CONNECT_TIMEOUT_SECS);
        assert_eq!(c.heartbeat_interval_secs, DEFAULT_HEARTBEAT_INTERVAL_SECS);
        assert_eq!(c.max_inbound_bytes, DEFAULT_MAX_INBOUND_BYTES);
        assert_eq!(c.max_session_secs, 0);
        assert_eq!(c.data_type, DataType::Json);
        assert_eq!(c.backpressure, Backpressure::Block);
        assert_eq!(c.error_mode, ErrorMode::Surface);
        assert_eq!(c.journal_mode, JournalMode::Journaled);
        assert_eq!(c.window, None);
    }

    /// Both kinds share the body, so a config valid for one parses for the
    /// other; the only difference is the TAG and the derived protocol.
    #[test]
    fn both_kinds_share_body_and_differ_only_by_protocol() {
        assert_eq!(ApiEndpoint::TAG, "api_endpoint");
        assert_eq!(LiveSocket::TAG, "live_socket");
        assert_eq!(ApiEndpoint::PROTOCOL, Protocol::Http);
        assert_eq!(LiveSocket::PROTOCOL, Protocol::Websocket);
        assert_eq!(protocol_for_tag("api_endpoint"), Some(Protocol::Http));
        assert_eq!(protocol_for_tag("live_socket"), Some(Protocol::Websocket));
        assert_eq!(protocol_for_tag("webhook"), None);
    }

    /// The suspend block round-trips FLAT on the wire (no nesting), next to
    /// the other flattened common fields.
    #[test]
    fn suspend_block_is_flat_on_the_wire() {
        let ep = ApiEndpoint {
            common: LiveConnectionConfig {
                suspend: crate::wait::SuspendPolicy { can_suspend: true, default_hold_secs: 42 },
                ..bare()
            },
        };
        let v = serde_json::to_value(&ep).expect("serialize");
        assert_eq!(v.get("can_suspend").and_then(|x| x.as_bool()), Some(true));
        assert_eq!(v.get("default_hold_secs").and_then(|x| x.as_u64()), Some(42));
        assert!(v.get("suspend").is_none(), "must be flattened");
        assert!(v.get("common").is_none(), "common must be flattened, not nested");
        assert!(v.get("protocol").is_none(), "protocol is the kind, not a field");
    }

    #[test]
    fn full_config_round_trips() {
        let sock = LiveSocket {
            common: LiveConnectionConfig {
                path: "tangle".into(),
                auth: PublicEntryAuth::OptionalApiKey,
                suspend: crate::wait::SuspendPolicy { can_suspend: true, default_hold_secs: 600 },
                connect_timeout_secs: 10,
                heartbeat_interval_secs: 15,
                max_inbound_bytes: 1024,
                max_session_secs: 3600,
                data_type: DataType::Bytes,
                backpressure: Backpressure::DropOldest,
                error_mode: ErrorMode::DropChunk,
                journal_mode: JournalMode::Ephemeral,
                window: Some(128),
            },
        };
        let spec = crate::signal::to_spec(sock.clone());
        assert_eq!(spec.kind, "live_socket");
        let back: LiveSocket =
            serde_json::from_value(spec.config).expect("config round-trips");
        assert!(back.common.suspend.can_suspend);
        assert_eq!(back.common.suspend.default_hold_secs, 600);
        assert_eq!(back.common.data_type, DataType::Bytes);
        assert_eq!(back.common.backpressure, Backpressure::DropOldest);
        assert_eq!(back.common.error_mode, ErrorMode::DropChunk);
        assert_eq!(back.common.journal_mode, JournalMode::Ephemeral);
        assert_eq!(back.common.window, Some(128));
    }

    #[test]
    fn leading_slash_path_rejected() {
        let ep = ApiEndpoint { common: LiveConnectionConfig { path: "/chat".into(), ..bare() } };
        let err = ep.validate().expect_err("leading slash should fail");
        assert!(err.contains("must not start with"), "got: {err}");
        assert!(err.contains("api_endpoint"), "error names the kind: {err}");
    }

    #[test]
    fn zero_connect_timeout_rejected() {
        let ep = ApiEndpoint { common: LiveConnectionConfig { connect_timeout_secs: 0, ..bare() } };
        let err = ep.validate().expect_err("zero timeout should fail");
        assert!(err.contains("connect_timeout_secs"), "got: {err}");
    }

    #[test]
    fn zero_default_hold_rejected() {
        let ep = ApiEndpoint {
            common: LiveConnectionConfig {
                suspend: crate::wait::SuspendPolicy { can_suspend: false, default_hold_secs: 0 },
                ..bare()
            },
        };
        let err = ep.validate().expect_err("zero hold should fail");
        assert!(err.contains("default_hold_secs"), "got: {err}");
    }

    #[test]
    fn zero_window_rejected() {
        let sock = LiveSocket { common: LiveConnectionConfig { window: Some(0), ..bare() } };
        let err = sock.validate().expect_err("zero window should fail");
        assert!(err.contains("window"), "got: {err}");
        assert!(err.contains("live_socket"), "error names the kind: {err}");
    }

    /// A minimal valid common body used as the spread base in the tests.
    fn bare() -> LiveConnectionConfig {
        LiveConnectionConfig {
            path: "chat".into(),
            auth: PublicEntryAuth::None,
            suspend: crate::wait::SuspendPolicy::default(),
            connect_timeout_secs: DEFAULT_CONNECT_TIMEOUT_SECS,
            heartbeat_interval_secs: DEFAULT_HEARTBEAT_INTERVAL_SECS,
            max_inbound_bytes: DEFAULT_MAX_INBOUND_BYTES,
            max_session_secs: 0,
            data_type: DataType::Json,
            backpressure: Backpressure::Block,
            error_mode: ErrorMode::Surface,
            journal_mode: JournalMode::Journaled,
            window: None,
        }
    }
}
