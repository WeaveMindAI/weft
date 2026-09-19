//! Live caller connections: making a weft program a real live endpoint.
//! An outside caller hits a stable public URL, the dispatcher matches the
//! route, checks the caller against the route's auth, points the caller
//! at a worker through the shared gateway, and any node in the running
//! program talks back over the held connection (reply once, stream, or
//! hold a two-way conversation).
//!
//! TWO user-facing signal kinds, because a node developer programs against
//! two genuinely different talk surfaces:
//!   - [`Route`]: inbound HTTP. The caller makes one request; the node
//!     replies once or streams a response (`HttpCaller`: respond / write /
//!     close). The caller is pointed at the worker by a `307` redirect.
//!   - [`Socket`]: inbound WebSocket. Full two-way conversation
//!     (`WsCaller`: send / receive / request / close). The caller fetches
//!     the worker URL then opens the real socket to it (WS cannot be
//!     redirected).
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
/// kind fired ([`Route`] -> `Http`, [`Socket`] -> `Websocket`), NOT carried
/// as a user-facing config field.
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

impl DataType {
    /// The node-facing spelling (`json`, `text`, `bytes`), as the
    /// trigger's `dataType` field takes it.
    pub fn parse_field(raw: &str) -> Result<Self, String> {
        match raw {
            "json" => Ok(Self::Json),
            "text" => Ok(Self::Text),
            "bytes" => Ok(Self::Bytes),
            other => Err(format!("dataType must be json, text or bytes, got '{other}'")),
        }
    }

    /// The same spelling back out, so a node naming the data type in a
    /// message says the word the author wrote in `dataType` instead of
    /// keeping its own copy of the mapping.
    pub fn as_wire_str(&self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Text => "text",
            Self::Bytes => "bytes",
        }
    }
}

/// Whether the caller exchange is PERSISTED to the journal/DB for replay.
/// This is ONLY about durability; it does NOT control the in-RAM window
/// (that is the separate `window` field, which bounds RAM in both modes,
/// cursors only ever read RAM). The same distinction a bus draws, and
/// the same code decides what it means for both
/// ([`crate::stream_journal`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum JournalMode {
    /// Full payload persisted to the journal/DB (replay survives a worker
    /// dying). The in-RAM window still bounds what a live cursor can reach.
    #[default]
    Journaled,
    /// Metadata-only in the journal (how many messages went each way
    /// and how many bytes); payloads are never persisted, so they live
    /// only in the in-RAM window.
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

/// Default how long the caller's machine may leave what we sent it
/// unacknowledged before the connection is called dead.
///
/// NOT a ceiling on the conversation: a machine that is still there
/// acknowledges every packet by itself, without the page or the program
/// doing anything, so a caller who stays resets this constantly and a
/// feed quiet for hours is never touched. It bounds SILENCE, and 30
/// seconds of silence from a machine that answers in milliseconds is
/// already far past gone.
///
/// Well above any real round trip (RFC 5482 warns that a user timeout
/// under the connection's retransmission timeout aborts healthy
/// connections) and far under the ~15 minutes the kernel takes alone.
pub const DEFAULT_CALLER_SILENCE_SECS: u64 = 30;

fn default_heartbeat_interval_secs() -> u64 {
    DEFAULT_HEARTBEAT_INTERVAL_SECS
}
fn default_caller_silence_secs() -> u64 {
    DEFAULT_CALLER_SILENCE_SECS
}
fn default_max_inbound_bytes() -> u64 {
    DEFAULT_MAX_INBOUND_BYTES
}
fn default_connect_timeout_secs() -> u64 {
    DEFAULT_CONNECT_TIMEOUT_SECS
}

/// The shared config body for both live-caller kinds. Flattened into
/// [`Route`] and [`Socket`] so the two user-facing kinds stay distinct
/// names while their behavioral knobs are defined exactly once (DRY
/// internals behind a clear surface). Every knob has a sane default
/// (programming-language principle: maximize customization, never hardcode
/// policy). Note there is NO `protocol` field here: the protocol is the
/// kind, derived from the TAG.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveConnectionConfig {
    /// Route pattern under the gateway root: literal segments and
    /// `{name}` captures (`chat/{room}`). Empty means root. Must not
    /// start with `/`. Parsed by `crate::route::RoutePattern`.
    pub path: String,

    /// The HTTP methods this route serves, uppercase. Empty = any
    /// method. A `Socket` ignores it (a WebSocket upgrade is a GET).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub methods: Vec<String>,

    /// Auth policy at the control handshake. `None` = anyone with the URL
    /// can connect, `Connection` = the caller is verified against a
    /// stored connection's `verify` recipe before a run starts.
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

    /// How long the caller's machine may leave what we sent it
    /// unacknowledged before the connection is called dead. The two
    /// halves work together: the heartbeat above is what puts bytes on
    /// the wire while a feed is quiet, and this is how long those bytes
    /// may go unanswered.
    ///
    /// Raise it for a caller on a link that genuinely goes quiet for
    /// longer than this (a satellite hop, a device that sleeps its radio
    /// between messages); lower it to notice a departure sooner. `0`
    /// leaves the machine's own default, which is about fifteen minutes.
    #[serde(default = "default_caller_silence_secs")]
    pub caller_silence_secs: u64,

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

    /// How long the journal accumulates before one row goes out, in
    /// seconds. `None` falls back to the window every channel in the
    /// language starts from
    /// ([`crate::stream_journal::DEFAULT_JOURNAL_WINDOW`], one second).
    /// A quiet conversation degenerates to one message per window, so a
    /// request and its answer read exactly as they did; a chatty socket
    /// collapses instead of writing a row per frame.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub journal_window_secs: Option<u64>,

    /// In-RAM inbound window size (how many recent messages a WebSocket
    /// retains for cursors). Bounds RAM regardless of `journal_mode`
    /// (cursors only read this window, never the DB; journaled just also
    /// persists each message for durability). `None` falls back to
    /// `weft_core::caller::DEFAULT_INBOUND_WINDOW`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<usize>,
}

impl LiveConnectionConfig {
    /// What the journal keeps of this conversation, in the shape every
    /// channel in the language answers with. The bus builds the same
    /// thing from its own options, which is what stops the two drifting
    /// on where content is trimmed or on what ephemeral means.
    pub fn journal_policy(&self) -> crate::stream_journal::JournalPolicy {
        crate::stream_journal::JournalPolicy {
            window: self
                .journal_window_secs
                .map(std::time::Duration::from_secs)
                .unwrap_or(crate::stream_journal::DEFAULT_JOURNAL_WINDOW),
            ephemeral: matches!(self.journal_mode, JournalMode::Ephemeral),
            ..Default::default()
        }
    }

    /// Build the shared body from a catalog node's config field map (the
    /// common authoring fields both live-caller nodes expose: `path`,
    /// `method`, `dataType`, `auth`, `outlivesCaller`, `defaultHoldSecs`,
    /// `maxSessionSecs`, `callerSilenceSecs`).
    /// Centralized here so the two nodes (`Route`, `Socket`) do NOT each
    /// re-implement field parsing; every non-exposed knob keeps its
    /// default. `fields` is the node's `ctx.inputs.object()` map.
    pub fn from_node_fields(
        fields: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<Self, String> {
        let path = fields.get("path").and_then(|v| v.as_str()).unwrap_or("").to_string();
        // A flag is absent or a boolean. Anything else is refused: read
        // as false, "may outlive the caller" would quietly change what a
        // disconnect does to the run.
        let flag = |name: &str| -> Result<bool, String> {
            match fields.get(name) {
                None | Some(serde_json::Value::Null) => Ok(false),
                Some(serde_json::Value::Bool(b)) => Ok(*b),
                Some(other) => Err(format!("{name} is yes or no, got {other}")),
            }
        };
        // One method on the node (`method: "POST"`), stored as the
        // one-element list the routing keeps; absent or empty = any.
        let methods = match fields.get("method") {
            None | Some(serde_json::Value::Null) => Vec::new(),
            Some(serde_json::Value::String(s)) if s.trim().is_empty() => Vec::new(),
            Some(serde_json::Value::String(s)) => vec![crate::route::normalize_method(s)?],
            Some(other) => return Err(format!("method must be a string, got {other}")),
        };
        let data_type = match fields.get("dataType") {
            None | Some(serde_json::Value::Null) => DataType::default(),
            Some(serde_json::Value::String(s)) if s.trim().is_empty() => DataType::default(),
            Some(serde_json::Value::String(s)) => DataType::parse_field(s)?,
            Some(other) => return Err(format!("dataType must be a string, got {other}")),
        };
        // The `auth` input carries an `Access` marker (an auth access
        // node's output); the routing keeps only what the gate needs to
        // find the connection again.
        let auth = match fields.get("auth") {
            None | Some(serde_json::Value::Null) => super::PublicEntryAuth::None,
            Some(value) => {
                let access = crate::access::Access::from_value(value)
                    .map_err(|e| format!("auth must be a connection (wire an auth access node): {e}"))?;
                super::PublicEntryAuth::Connection {
                    access_id: access.access_id().to_string(),
                    service: access.service().to_string(),
                }
            }
        };
        // The author-facing name says what the flag DECIDES (the run
        // outlives its caller); `can_suspend` inside is the mechanism
        // that follows from it (a wait may park instead of being killed).
        let can_suspend = flag("outlivesCaller")?;
        // Absent, the given default; present, a whole non-negative
        // number of seconds (a JSON `60.0` is one), anything else a
        // refusal rather than a silent fall back to the default.
        let seconds = |name: &str, fallback: u64| -> Result<u64, String> {
            match fields.get(name) {
                None | Some(serde_json::Value::Null) => Ok(fallback),
                Some(v) => match v.as_f64() {
                    Some(n) if n >= 0.0 && n.fract() == 0.0 => Ok(n as u64),
                    _ => Err(format!("{name} must be a whole number of seconds, got {v}")),
                },
            }
        };
        let default_hold_secs = seconds("defaultHoldSecs", crate::wait::LANGUAGE_DEFAULT_HOLD_SECS)?;
        // The only ceiling on a live exchange, and off unless the author
        // asks for one: weft puts no deadline on a wait a person
        // controls. It is here for the feed nothing else can watch, a
        // `raw` stream whose framing has no spare byte to poke the
        // connection with while it is quiet.
        let max_session_secs = seconds("maxSessionSecs", 0)?;
        // How long the caller's machine may go silent before its
        // connection is called dead. The trigger owns it because the
        // trigger is what opens the call path, and it is the one place
        // that knows what kind of client is on the other end.
        let caller_silence_secs = seconds("callerSilenceSecs", DEFAULT_CALLER_SILENCE_SECS)?;
        // What the journal keeps of the exchange. A node that wants to
        // hand that choice to its author surfaces a `journalEphemeral`
        // boolean; one that does not never sets it and gets the same
        // default every channel in the language starts from. Refused
        // rather than read as false when it is neither, because
        // "written down" and "not written down" is not a difference to
        // guess at.
        let journal_mode = match fields.get("journalEphemeral") {
            None | Some(serde_json::Value::Null) => JournalMode::default(),
            Some(serde_json::Value::Bool(true)) => JournalMode::Ephemeral,
            Some(serde_json::Value::Bool(false)) => JournalMode::Journaled,
            Some(v) => {
                return Err(format!("journalEphemeral must be true or false, got {v}"));
            }
        };
        Ok(Self {
            path,
            methods,
            auth,
            suspend: crate::wait::SuspendPolicy { can_suspend, default_hold_secs },
            connect_timeout_secs: DEFAULT_CONNECT_TIMEOUT_SECS,
            heartbeat_interval_secs: DEFAULT_HEARTBEAT_INTERVAL_SECS,
            caller_silence_secs,
            max_inbound_bytes: DEFAULT_MAX_INBOUND_BYTES,
            max_session_secs,
            data_type,
            backpressure: Backpressure::default(),
            error_mode: ErrorMode::default(),
            journal_mode,
            journal_window_secs: None,
            window: None,
        })
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
        crate::route::RoutePattern::parse(&self.path).map_err(|e| format!("{kind_tag} path: {e}"))?;
        for m in &self.methods {
            let normalized = crate::route::normalize_method(m).map_err(|e| format!("{kind_tag} method: {e}"))?;
            if normalized != *m {
                return Err(format!(
                    "{kind_tag} method '{m}' must be stored uppercase ('{normalized}')"
                ));
            }
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

/// Inbound HTTP route. The caller makes one request and the node streams
/// or replies via the `HttpCaller` handle. See [`LiveConnectionConfig`]
/// for the knobs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Route {
    #[serde(flatten)]
    pub common: LiveConnectionConfig,
}

/// Inbound WebSocket. Full two-way conversation via the `WsCaller`
/// handle. See [`LiveConnectionConfig`] for the knobs; `methods` is
/// ignored (the upgrade is always a GET).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Socket {
    #[serde(flatten)]
    pub common: LiveConnectionConfig,
}

impl Signal for Route {
    const TAG: &'static str = "route";
    fn validate(&self) -> Result<(), String> {
        self.common.validate(Self::TAG)
    }
}

impl Signal for Socket {
    const TAG: &'static str = "socket";
    fn validate(&self) -> Result<(), String> {
        if !self.common.methods.is_empty() {
            return Err(format!(
                "{} takes no method: a WebSocket upgrade is always a GET",
                Self::TAG
            ));
        }
        self.common.validate(Self::TAG)
    }
}

/// Map a live-caller signal tag onto its wire protocol. The single place
/// the kind->protocol relationship lives, used by the dispatcher and worker
/// to recover the protocol from a fired spec without a config field. Returns
/// `None` for any non-live-caller tag.
pub fn protocol_for_tag(tag: &str) -> Option<Protocol> {
    match tag {
        t if t == Route::TAG => Some(Protocol::Http),
        t if t == Socket::TAG => Some(Protocol::Websocket),
        _ => None,
    }
}

crate::register_signal_kind!(Route);
crate::register_signal_kind!(Socket);

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
        let ep: Route = serde_json::from_value(json).expect("deserialize");
        let c = &ep.common;
        assert_eq!(c.path, "chat");
        assert!(c.methods.is_empty());
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
        assert_eq!(Route::TAG, "route");
        assert_eq!(Socket::TAG, "socket");
        assert_eq!(protocol_for_tag("route"), Some(Protocol::Http));
        assert_eq!(protocol_for_tag("socket"), Some(Protocol::Websocket));
        assert_eq!(protocol_for_tag("webhook"), None);
        assert_eq!(protocol_for_tag("api_endpoint"), None, "the old tags are gone");
        assert_eq!(protocol_for_tag("live_socket"), None);
    }

    /// The suspend block round-trips FLAT on the wire (no nesting), next to
    /// the other flattened common fields.
    #[test]
    fn suspend_block_is_flat_on_the_wire() {
        let ep = Route {
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
        assert!(v.get("methods").is_none(), "an any-method route omits the list");
    }

    #[test]
    fn full_config_round_trips() {
        let sock = Socket {
            common: LiveConnectionConfig {
                path: "tangle/{room}".into(),
                methods: Vec::new(),
                auth: PublicEntryAuth::Connection {
                    access_id: "acc-1".into(),
                    service: "api_key_auth".into(),
                },
                suspend: crate::wait::SuspendPolicy { can_suspend: true, default_hold_secs: 600 },
                connect_timeout_secs: 10,
                heartbeat_interval_secs: 15,
                caller_silence_secs: 45,
                max_inbound_bytes: 1024,
                max_session_secs: 3600,
                data_type: DataType::Bytes,
                backpressure: Backpressure::DropOldest,
                error_mode: ErrorMode::DropChunk,
                journal_mode: JournalMode::Ephemeral,
                journal_window_secs: Some(5),
                window: Some(128),
            },
        };
        let spec = crate::signal::to_spec(sock.clone());
        assert_eq!(spec.kind, "socket");
        let back: Socket =
            serde_json::from_value(spec.config).expect("config round-trips");
        assert_eq!(back.common.path, "tangle/{room}");
        assert!(matches!(
            back.common.auth,
            PublicEntryAuth::Connection { ref access_id, ref service }
                if access_id == "acc-1" && service == "api_key_auth"
        ));
        assert!(back.common.suspend.can_suspend);
        assert_eq!(back.common.suspend.default_hold_secs, 600);
        assert_eq!(back.common.data_type, DataType::Bytes);
        assert_eq!(back.common.backpressure, Backpressure::DropOldest);
        assert_eq!(back.common.error_mode, ErrorMode::DropChunk);
        assert_eq!(back.common.journal_mode, JournalMode::Ephemeral);
        assert_eq!(back.common.journal_window_secs, Some(5));
        assert_eq!(back.common.window, Some(128));
    }

    /// A caller conversation and a bus answer the same question the
    /// same way. A route that says nothing about journaling gets the
    /// window and the size limit every channel in the language starts
    /// from; the two knobs a node can surface move exactly one thing
    /// each.
    #[test]
    fn a_conversation_journals_on_the_language_default() {
        let policy = bare().journal_policy();
        assert_eq!(policy, crate::stream_journal::JournalPolicy::default());
        assert_eq!(policy.window, crate::stream_journal::DEFAULT_JOURNAL_WINDOW);
        assert_eq!(policy.trim_bytes, crate::stream_journal::JOURNAL_TRIM_BYTES);
        assert!(!policy.ephemeral, "a conversation is written down unless it says otherwise");

        let quiet = LiveConnectionConfig { journal_mode: JournalMode::Ephemeral, ..bare() };
        assert!(quiet.journal_policy().ephemeral);
        assert_eq!(quiet.journal_policy().window, crate::stream_journal::DEFAULT_JOURNAL_WINDOW);

        let slow = LiveConnectionConfig { journal_window_secs: Some(5), ..bare() };
        assert_eq!(slow.journal_policy().window, std::time::Duration::from_secs(5));
        assert!(!slow.journal_policy().ephemeral, "the window says nothing about content");
    }

    /// `journalEphemeral` is the knob a node hands its author. It is
    /// read as a boolean or refused: whether a conversation is written
    /// down is not a thing to guess at from a stray value.
    #[test]
    fn a_node_can_hand_its_author_the_journaling_choice() {
        let with = |value: serde_json::Value| {
            let mut fields = serde_json::Map::new();
            fields.insert("path".to_string(), serde_json::json!("chat"));
            fields.insert("journalEphemeral".to_string(), value);
            LiveConnectionConfig::from_node_fields(&fields)
        };
        assert_eq!(with(serde_json::json!(true)).unwrap().journal_mode, JournalMode::Ephemeral);
        assert_eq!(with(serde_json::json!(false)).unwrap().journal_mode, JournalMode::Journaled);
        assert_eq!(with(serde_json::Value::Null).unwrap().journal_mode, JournalMode::default());

        let refusal = with(serde_json::json!("yes")).expect_err("a string is not an answer");
        assert!(refusal.contains("journalEphemeral must be true or false"), "{refusal}");

        // A node that never surfaces it gets the default, not a refusal.
        let mut bare_fields = serde_json::Map::new();
        bare_fields.insert("path".to_string(), serde_json::json!("chat"));
        assert_eq!(
            LiveConnectionConfig::from_node_fields(&bare_fields).unwrap().journal_mode,
            JournalMode::default()
        );
    }

    #[test]
    fn methods_ride_the_route_uppercase() {
        let r = Route { common: LiveConnectionConfig { methods: vec!["POST".into()], ..bare() } };
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v["methods"], serde_json::json!(["POST"]));
        r.validate().expect("uppercase known method is fine");
        let lower = Route { common: LiveConnectionConfig { methods: vec!["post".into()], ..bare() } };
        let err = lower.validate().expect_err("stored methods are uppercase");
        assert!(err.contains("uppercase"), "got: {err}");
        let bogus = Route { common: LiveConnectionConfig { methods: vec!["GTE".into()], ..bare() } };
        assert!(bogus.validate().is_err());
    }

    #[test]
    fn a_socket_takes_no_method() {
        let s = Socket { common: LiveConnectionConfig { methods: vec!["POST".into()], ..bare() } };
        let err = s.validate().expect_err("a socket has no method");
        assert!(err.contains("no method"), "got: {err}");
    }

    #[test]
    fn node_fields_build_the_body() {
        let access = crate::access::Access::new("acc-9", "jwt_auth", None).to_value();
        let fields = serde_json::json!({
            "path": "users/{id}",
            "method": "get",
            "dataType": "text",
            "auth": access,
            "outlivesCaller": true,
            "defaultHoldSecs": 120,
            "maxSessionSecs": 900,
            "callerSilenceSecs": 90,
        });
        let cfg = LiveConnectionConfig::from_node_fields(fields.as_object().unwrap()).expect("builds");
        assert_eq!(cfg.path, "users/{id}");
        assert_eq!(cfg.methods, vec!["GET".to_string()]);
        assert_eq!(cfg.data_type, DataType::Text);
        assert!(matches!(
            cfg.auth,
            PublicEntryAuth::Connection { ref access_id, ref service }
                if access_id == "acc-9" && service == "jwt_auth"
        ));
        assert!(cfg.suspend.can_suspend);
        assert_eq!(cfg.suspend.default_hold_secs, 120);
        assert_eq!(cfg.max_session_secs, 900, "the ceiling the author asked for");
        assert_eq!(
            cfg.caller_silence_secs, 90,
            "the node that opens the call path says how long a caller may go silent"
        );

        let empty = serde_json::json!({ "path": "x", "method": "", "dataType": "" });
        let cfg = LiveConnectionConfig::from_node_fields(empty.as_object().unwrap()).expect("builds");
        assert!(cfg.methods.is_empty(), "an empty method is any method");
        assert_eq!(cfg.data_type, DataType::Json);
        assert!(matches!(cfg.auth, PublicEntryAuth::None));
        assert_eq!(cfg.max_session_secs, 0, "no ceiling unless one was asked for");
        assert_eq!(
            cfg.caller_silence_secs, DEFAULT_CALLER_SILENCE_SECS,
            "a route that says nothing gets the default, never the machine's 15 minutes"
        );

        for (field, value) in [
            ("method", serde_json::json!("GTE")),
            ("method", serde_json::json!(3)),
            ("dataType", serde_json::json!("xml")),
            ("auth", serde_json::json!("not-a-connection")),
            ("outlivesCaller", serde_json::json!("yes")),
            ("defaultHoldSecs", serde_json::json!(1.5)),
            ("maxSessionSecs", serde_json::json!(1.5)),
            ("maxSessionSecs", serde_json::json!("forever")),
        ] {
            let bad = serde_json::json!({ "path": "x", field: value });
            assert!(
                LiveConnectionConfig::from_node_fields(bad.as_object().unwrap()).is_err(),
                "{field} must be refused"
            );
        }
    }

    #[test]
    fn leading_slash_path_rejected() {
        let ep = Route { common: LiveConnectionConfig { path: "/chat".into(), ..bare() } };
        let err = ep.validate().expect_err("leading slash should fail");
        assert!(err.contains("must not start with"), "got: {err}");
        assert!(err.contains("route"), "error names the kind: {err}");
    }

    #[test]
    fn malformed_pattern_rejected() {
        let ep = Route { common: LiveConnectionConfig { path: "chat/{room".into(), ..bare() } };
        let err = ep.validate().expect_err("unclosed parameter should fail");
        assert!(err.contains("never closes"), "got: {err}");
    }

    #[test]
    fn zero_connect_timeout_rejected() {
        let ep = Route { common: LiveConnectionConfig { connect_timeout_secs: 0, ..bare() } };
        let err = ep.validate().expect_err("zero timeout should fail");
        assert!(err.contains("connect_timeout_secs"), "got: {err}");
    }

    #[test]
    fn zero_default_hold_rejected() {
        let ep = Route {
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
        let sock = Socket { common: LiveConnectionConfig { window: Some(0), ..bare() } };
        let err = sock.validate().expect_err("zero window should fail");
        assert!(err.contains("window"), "got: {err}");
        assert!(err.contains("socket"), "error names the kind: {err}");
    }

    /// A minimal valid common body used as the spread base in the tests.
    fn bare() -> LiveConnectionConfig {
        LiveConnectionConfig {
            path: "chat".into(),
            methods: Vec::new(),
            auth: PublicEntryAuth::None,
            suspend: crate::wait::SuspendPolicy::default(),
            connect_timeout_secs: DEFAULT_CONNECT_TIMEOUT_SECS,
            heartbeat_interval_secs: DEFAULT_HEARTBEAT_INTERVAL_SECS,
            caller_silence_secs: DEFAULT_CALLER_SILENCE_SECS,
            max_inbound_bytes: DEFAULT_MAX_INBOUND_BYTES,
            max_session_secs: 0,
            data_type: DataType::Json,
            backpressure: Backpressure::Block,
            error_mode: ErrorMode::Surface,
            journal_mode: JournalMode::Journaled,
            journal_window_secs: None,
            window: None,
        }
    }
}
