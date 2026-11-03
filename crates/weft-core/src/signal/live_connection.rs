//! Live caller connections: making a weft program a real live endpoint.
//! An outside caller calls a stable public URL, the install passes the
//! call on to the project's workers, whose door matches the route, checks
//! the caller against its auth and limits and bears the run with the
//! caller on the line, and any node in the running program talks back
//! over the held connection (reply once, stream, or hold a two-way
//! conversation).
//!
//! TWO user-facing signal kinds, because a node developer programs against
//! two genuinely different talk surfaces:
//!   - [`Route`]: inbound HTTP. The caller makes one request; the node
//!     replies once or streams a response (`HttpCaller`: respond / write /
//!     close), in that same request.
//!   - [`Socket`]: inbound WebSocket. Full two-way conversation
//!     (`WsCaller`: send / receive / request / close). A browser, which
//!     cannot send credentials on a socket, asks first with a plain GET and
//!     opens its socket at the ticketed URL it gets back.
//!
//! Both share their entire config body ([`LiveConnectionConfig`], flattened)
//! and the entire transport engine behind them (the door,
//! disconnect policy, reconciliation, heartbeat, caps, journaling). The ONLY thing the kind distinction carries is the
//! wire [`Protocol`], which the runtime derives from the kind's TAG, never
//! from node identity, so the language stays generic.

use serde::{Deserialize, Serialize};

/// The live-caller input that says the run may outlive its caller: the
/// language gives it to every trigger holding a caller on the line
/// (`crate::run_settings::RunSettings::node_inputs`).
pub use crate::run_settings::OUTLIVES_CALLER_FIELD;

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

/// How outbound talk behaves when the caller reads slower than the
/// program writes, so the bounded outbound buffer fills. A multiplexing
/// process must never OOM on one slow caller, so every mode keeps the
/// buffer bounded; they differ in what gives. Spelled as the trigger's
/// `whenCallerIsSlow` setting takes it ([`Self::parse_field`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Backpressure {
    /// `write`/`send` waits for room, so a slow caller slows the
    /// producer and every chunk arrives.
    #[default]
    Block,
    /// Make room by shedding the oldest chunk still waiting, so the
    /// caller always gets the latest (live video, a price feed: a stale
    /// frame is worthless).
    DropOldest,
    /// Shed the chunk being written, so what was already waiting goes
    /// out in order and the gap is at the end.
    DropNewest,
}

impl Backpressure {
    /// The trigger setting's spelling (`block`, `drop_oldest`,
    /// `drop_newest`), the same words the signal's config carries.
    pub fn parse_field(raw: &str) -> Result<Self, String> {
        match raw {
            "block" => Ok(Self::Block),
            "drop_oldest" => Ok(Self::DropOldest),
            "drop_newest" => Ok(Self::DropNewest),
            other => Err(format!("whenCallerIsSlow must be block, drop_oldest or drop_newest, got '{other}'")),
        }
    }
}

/// What a node writing to the caller does with one chunk (an HTTP
/// stream's piece, a socket message) it could not make: a value that
/// does not fit the caller's data type, a file it could not read.
/// Read through `HttpCaller::drop_or_fail` / `WsCaller::drop_or_fail`.
///
/// It governs that one chunk only. An exchange that cannot go on (the
/// run failed or was stopped, the stream's source broke off) is told to
/// the caller in every mode: there is nothing left to continue, and
/// ending it cleanly would tell the caller a cut stream was whole.
/// Spelled as the trigger's `whenChunkFails` setting takes it
/// ([`Self::parse_field`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ErrorMode {
    /// The node fails, and the caller is told: before any bytes, a real
    /// error status; after an HTTP stream started, an in-band error,
    /// then the end; on a socket, a `1011` close.
    #[default]
    Surface,
    /// The chunk is skipped and the exchange goes on with the next one.
    /// The skip is on the exchange's record, so it is never silent.
    DropChunk,
}

impl ErrorMode {
    /// The trigger setting's spelling (`surface`, `drop_chunk`), the
    /// same words the signal's config carries.
    pub fn parse_field(raw: &str) -> Result<Self, String> {
        match raw {
            "surface" => Ok(Self::Surface),
            "drop_chunk" => Ok(Self::DropChunk),
            other => Err(format!("whenChunkFails must be surface or drop_chunk, got '{other}'")),
        }
    }
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

/// Default heartbeat interval. Clears the ~30s cellular-NAT floor and
/// sits under the ~60s proxy default, so one ping keeps both the gateway
/// hop and the caller last-mile alive.
pub const DEFAULT_HEARTBEAT_INTERVAL_SECS: u64 = 25;

/// Default per-request/message inbound size cap. Generous for chat-shaped
/// payloads, loud for accidental uploads. Untrusted-caller abuse vector.
pub const DEFAULT_MAX_INBOUND_BYTES: u64 = 16 * 1_048_576;

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

    /// Auth policy the worker's door applies. `None` = anyone with the URL
    /// can connect, `Connection` = the caller is verified against a
    /// stored connection's `verify` recipe before a run starts.
    #[serde(default)]
    pub auth: super::PublicEntryAuth,

    /// Whether the run goes on once its caller has left (the trigger's
    /// `outlivesCaller`). Off, the default for an interactive endpoint,
    /// the caller leaving cancels the run, and the run cannot pause while
    /// the caller is on the line.
    #[serde(default)]
    pub outlives_caller: bool,

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

    /// What the journal keeps of the exchange: whether content is
    /// written down, and how long one row gathers messages. The same
    /// policy a bus takes, stored flat (`journal_mode`,
    /// `journal_window_ms`).
    #[serde(flatten)]
    pub journal: crate::stream_journal::JournalPolicy,

    /// In-RAM inbound window size (how many recent messages a WebSocket
    /// retains for cursors). Bounds RAM regardless of the journal mode
    /// (cursors only read this window, never the DB; journaled just also
    /// persists each message for durability). `None` falls back to
    /// `weft_core::caller::DEFAULT_INBOUND_WINDOW`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<usize>,
}

impl LiveConnectionConfig {
    /// Build the shared body from a catalog node's config field map (the
    /// common authoring fields both live-caller nodes expose: `path`,
    /// `method`, `dataType`, `auth` and `connection`, `outlivesCaller`,
    /// `maxSessionSecs`, `callerSilenceSecs`, `maxInboundMb`,
    /// `whenChunkFails`, `whenCallerIsSlow`, and the journal settings the
    /// language gives a live-connection trigger, `journalEphemeral` and
    /// `journalWindowMs`). Centralized here so the two nodes (`Route`,
    /// `Socket`) do NOT each re-implement field parsing; every non-exposed
    /// knob keeps its default. `fields` is the node's `ctx.inputs.object()`
    /// map; `place` names the node in a refusal (`weft connect <place>`).
    pub fn from_node_fields(
        fields: &serde_json::Map<String, serde_json::Value>,
        place: &str,
    ) -> Result<Self, String> {
        let path = fields.get("path").and_then(|v| v.as_str()).unwrap_or("").to_string();
        // One method on the node (`method: "POST"`), stored as the
        // one-element list the routing keeps; absent or empty = any.
        let methods = match fields.get("method") {
            None | Some(serde_json::Value::Null) => Vec::new(),
            Some(serde_json::Value::String(s)) if s.trim().is_empty() => Vec::new(),
            Some(serde_json::Value::String(s)) => vec![crate::route::normalize_method(s)?],
            Some(other) => return Err(format!("method must be a string, got {other}")),
        };
        // A choice from a fixed set: absent or empty is the default, a
        // word outside the set a refusal naming the set.
        fn choice<T: Default>(
            fields: &serde_json::Map<String, serde_json::Value>,
            name: &str,
            parse: fn(&str) -> Result<T, String>,
        ) -> Result<T, String> {
            match fields.get(name) {
                None | Some(serde_json::Value::Null) => Ok(T::default()),
                Some(serde_json::Value::String(s)) if s.trim().is_empty() => Ok(T::default()),
                Some(serde_json::Value::String(s)) => parse(s),
                Some(other) => Err(format!("{name} must be a string, got {other}")),
            }
        }
        let data_type = choice(fields, "dataType", DataType::parse_field)?;
        let error_mode = choice(fields, "whenChunkFails", ErrorMode::parse_field)?;
        let backpressure = choice(fields, "whenCallerIsSlow", Backpressure::parse_field)?;
        // `auth` names the check, `connection` (the install's pick, put
        // in where the compiler marked it) holds what it checks against;
        // the routing keeps the check and what finds the connection again.
        use crate::access::callers;
        let auth = match callers::parse_auth(fields.get(callers::AUTH_FIELD))? {
            None => super::PublicEntryAuth::None,
            Some(verify) => match fields.get(callers::CONNECTION_FIELD) {
                None | Some(serde_json::Value::Null) => return Err(callers::no_connection(place, &verify)),
                Some(value) => {
                    let access = crate::access::Access::from_value(value)
                        .map_err(|e| format!("{} must be a connection: {e}", callers::CONNECTION_FIELD))?;
                    let scheme = callers::CallerScheme::of(&verify).expect("parse_auth answers caller schemes only");
                    if access.service() != scheme.service() {
                        return Err(format!(
                            "'{place}' checks callers with {} against a '{}' connection, but that check \
                             reads a '{}' connection; run `weft connect {place}`",
                            verify.tag(),
                            access.service(),
                            scheme.service()
                        ));
                    }
                    super::PublicEntryAuth::Connection {
                        access_id: access.access_id().to_string(),
                        service: access.service().to_string(),
                        verify: Box::new(verify),
                    }
                }
            },
        };
        // Read the way the run's settings read it (refused rather than read
        // as off: "may outlive the caller" decides what a disconnect does).
        let outlives_caller = crate::run_settings::read_switch(fields, OUTLIVES_CALLER_FIELD, false)?;
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
        // The largest request body (route) or message (socket) a caller
        // may send, in whole megabytes; absent, the default. Zero would
        // refuse every call, so the smallest is one.
        let max_inbound_bytes = match fields.get("maxInboundMb") {
            None | Some(serde_json::Value::Null) => DEFAULT_MAX_INBOUND_BYTES,
            Some(v) => match v.as_f64() {
                Some(n) if n >= 1.0 && n.fract() == 0.0 && n <= (u64::MAX / 1_048_576) as f64 => {
                    n as u64 * 1_048_576
                }
                _ => return Err(format!("maxInboundMb must be a whole number of megabytes, 1 or more, got {v}")),
            },
        };
        // What the journal keeps of the exchange, read the way every
        // channel's settings are read.
        let journal = crate::stream_journal::JournalPolicy::from_node_fields(fields)?;
        Ok(Self {
            path,
            methods,
            auth,
            outlives_caller,
            heartbeat_interval_secs: DEFAULT_HEARTBEAT_INTERVAL_SECS,
            caller_silence_secs,
            max_inbound_bytes,
            max_session_secs,
            data_type,
            backpressure,
            error_mode,
            journal,
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
        if self.max_inbound_bytes == 0 {
            return Err(format!(
                "{kind_tag} max_inbound_bytes must be > 0: a zero cap rejects every \
                 request; disable the cap is not a goal, set it generously"
            ));
        }
        self.journal.check().map_err(|why| format!("{kind_tag}: {why}"))?;
        match &self.auth {
            super::PublicEntryAuth::None => {}
            super::PublicEntryAuth::Connection { service, verify, .. } => {
                verify.validate(service, crate::access::events::VerifySide::Caller).map_err(|e| format!("{kind_tag} auth: {e}"))?;
            }
            super::PublicEntryAuth::CheckNotKept { service, .. } => {
                return Err(format!("{kind_tag}: {}", super::auth::uncarried_gate(service)));
            }
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
    const CALLER: Option<Protocol> = Some(Protocol::Http);
    fn live_connection(&self) -> Option<&LiveConnectionConfig> {
        Some(&self.common)
    }
    fn validate(&self) -> Result<(), String> {
        self.common.validate(Self::TAG)
    }
}

impl Signal for Socket {
    const TAG: &'static str = "socket";
    const CALLER: Option<Protocol> = Some(Protocol::Websocket);
    fn live_connection(&self) -> Option<&LiveConnectionConfig> {
        Some(&self.common)
    }
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
        assert!(!c.outlives_caller);
        assert_eq!(c.heartbeat_interval_secs, DEFAULT_HEARTBEAT_INTERVAL_SECS);
        assert_eq!(c.max_inbound_bytes, DEFAULT_MAX_INBOUND_BYTES);
        assert_eq!(c.max_session_secs, 0);
        assert_eq!(c.data_type, DataType::Json);
        assert_eq!(c.backpressure, Backpressure::Block);
        assert_eq!(c.error_mode, ErrorMode::Surface);
        assert_eq!(c.journal, crate::stream_journal::JournalPolicy::default());
        assert_eq!(c.window, None);
    }

    /// Both kinds share the body, so a config valid for one parses for the
    /// other; the only difference is the TAG and the derived protocol.
    #[test]
    fn both_kinds_share_body_and_differ_only_by_protocol() {
        assert_eq!(Route::TAG, "route");
        assert_eq!(Socket::TAG, "socket");
        assert_eq!(super::super::caller_protocol("route"), Some(Protocol::Http));
        assert_eq!(super::super::caller_protocol("socket"), Some(Protocol::Websocket));
        assert_eq!(super::super::caller_protocol("timer"), None, "a kind nobody waits on declares no caller");
        assert_eq!(super::super::caller_protocol("api_endpoint"), None, "the old tags are gone");
    }

    /// The common body round-trips FLAT on the wire (no nesting).
    #[test]
    fn common_body_is_flat_on_the_wire() {
        let ep = Route { common: LiveConnectionConfig { outlives_caller: true, ..bare() } };
        let v = serde_json::to_value(&ep).expect("serialize");
        assert_eq!(v.get("outlives_caller").and_then(|x| x.as_bool()), Some(true));
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
                    verify: Box::new(crate::access::callers::named_check("api_keys").unwrap().verify()),
                },
                outlives_caller: true,
                heartbeat_interval_secs: 15,
                caller_silence_secs: 45,
                max_inbound_bytes: 1024,
                max_session_secs: 3600,
                data_type: DataType::Bytes,
                backpressure: Backpressure::DropOldest,
                error_mode: ErrorMode::DropChunk,
                journal: crate::stream_journal::JournalPolicy {
                    mode: crate::stream_journal::JournalMode::Ephemeral,
                    window: std::time::Duration::from_secs(5),
                },
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
            PublicEntryAuth::Connection { ref access_id, ref service, .. }
                if access_id == "acc-1" && service == "api_key_auth"
        ));
        assert!(back.common.outlives_caller);
        assert_eq!(back.common.data_type, DataType::Bytes);
        assert_eq!(back.common.backpressure, Backpressure::DropOldest);
        assert_eq!(back.common.error_mode, ErrorMode::DropChunk);
        assert!(back.common.journal.is_ephemeral());
        assert_eq!(back.common.journal.window, std::time::Duration::from_secs(5));
        assert_eq!(back.common.window, Some(128));
    }

    /// The journal settings a node hands its author reach the
    /// conversation through the one reader every channel shares; a node
    /// that sets neither gets the policy every channel starts from, and a
    /// refusal names the setting.
    #[test]
    fn a_node_can_hand_its_author_the_journal_settings() {
        let with = |extra: serde_json::Value| {
            let mut fields = serde_json::Map::new();
            fields.insert("path".to_string(), serde_json::json!("chat"));
            fields.extend(extra.as_object().unwrap().clone());
            LiveConnectionConfig::from_node_fields(&fields, "chat")
        };
        let set = with(serde_json::json!({ "journalEphemeral": true, "journalWindowMs": 250 })).unwrap();
        assert!(set.journal.is_ephemeral());
        assert_eq!(set.journal.window, std::time::Duration::from_millis(250));
        assert_eq!(with(serde_json::json!({})).unwrap().journal, crate::stream_journal::JournalPolicy::default());

        let refusal = with(serde_json::json!({ "journalEphemeral": "yes" })).expect_err("a string is not an answer");
        assert!(refusal.contains("journalEphemeral"), "{refusal}");
        let refusal = with(serde_json::json!({ "journalWindowMs": 50 })).expect_err("under the floor");
        assert!(refusal.contains("journalWindowMs") && refusal.contains("between 100 and 60000 ms"), "{refusal}");
    }

    /// A stored config whose window is out of range (written by hand, or
    /// by an older weft) is refused when the kind is validated, naming
    /// the kind.
    #[test]
    fn a_journal_window_out_of_range_is_refused() {
        let journal = crate::stream_journal::JournalPolicy {
            window: std::time::Duration::from_secs(61),
            ..Default::default()
        };
        let err = Route { common: LiveConnectionConfig { journal, ..bare() } }.validate().unwrap_err();
        assert!(err.starts_with("route:") && err.contains("between 100 and 60000 ms"), "{err}");
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
        let access = crate::access::Access::new("acc-9", "hmac_auth", None).to_value();
        let fields = serde_json::json!({
            "path": "users/{id}",
            "method": "get",
            "dataType": "text",
            "auth": "hmac_sha256",
            "connection": access,
            "outlivesCaller": true,
            "maxSessionSecs": 900,
            "callerSilenceSecs": 90,
            "maxInboundMb": 64,
        });
        let cfg = LiveConnectionConfig::from_node_fields(fields.as_object().unwrap(), "users").expect("builds");
        assert_eq!(cfg.path, "users/{id}");
        assert_eq!(cfg.methods, vec!["GET".to_string()]);
        assert_eq!(cfg.data_type, DataType::Text);
        assert!(matches!(
            cfg.auth,
            PublicEntryAuth::Connection { ref access_id, ref service, ref verify }
                if access_id == "acc-9" && service == "hmac_auth" && verify.tag() == "hmac"
        ));
        assert!(cfg.outlives_caller);
        assert_eq!(cfg.max_session_secs, 900, "the ceiling the author asked for");
        assert_eq!(cfg.max_inbound_bytes, 64 * 1_048_576, "the body cap the author asked for");
        assert_eq!(
            cfg.caller_silence_secs, 90,
            "the node that opens the call path says how long a caller may go silent"
        );

        let empty = serde_json::json!({ "path": "x", "method": "", "dataType": "" });
        let cfg = LiveConnectionConfig::from_node_fields(empty.as_object().unwrap(), "x").expect("builds");
        assert!(cfg.methods.is_empty(), "an empty method is any method");
        assert_eq!(cfg.data_type, DataType::Json);
        assert_eq!(cfg.error_mode, ErrorMode::Surface, "a route that says nothing tells its caller");
        assert_eq!(cfg.backpressure, Backpressure::Block, "a route that says nothing waits for a slow caller");
        assert!(matches!(cfg.auth, PublicEntryAuth::None));
        assert_eq!(cfg.max_session_secs, 0, "no ceiling unless one was asked for");
        assert_eq!(cfg.max_inbound_bytes, DEFAULT_MAX_INBOUND_BYTES, "a route that says nothing gets 16 MB");
        assert_eq!(
            cfg.caller_silence_secs, DEFAULT_CALLER_SILENCE_SECS,
            "a route that says nothing gets the default, never the machine's 15 minutes"
        );

        for (field, value) in [
            ("method", serde_json::json!("GTE")),
            ("method", serde_json::json!(3)),
            ("dataType", serde_json::json!("xml")),
            ("auth", serde_json::json!("not-a-scheme")),
            ("auth", serde_json::json!({ "kind": "token_echo" })),
            ("outlivesCaller", serde_json::json!("yes")),
            ("maxSessionSecs", serde_json::json!(1.5)),
            ("maxSessionSecs", serde_json::json!("forever")),
            ("maxInboundMb", serde_json::json!(0)),
            ("maxInboundMb", serde_json::json!(1.5)),
            ("maxInboundMb", serde_json::json!("big")),
            ("whenChunkFails", serde_json::json!("ignore")),
            ("whenChunkFails", serde_json::json!(true)),
            ("whenCallerIsSlow", serde_json::json!("drop")),
            ("whenCallerIsSlow", serde_json::json!(2)),
        ] {
            let bad = serde_json::json!({ "path": "x", field: value });
            assert!(
                LiveConnectionConfig::from_node_fields(bad.as_object().unwrap(), "x").is_err(),
                "{field} must be refused"
            );
        }
    }

    /// `auth` names the check and `connection` what it reads: a scheme
    /// with no connection, or one of another service, is refused naming
    /// the node's place and `weft connect`, never served open.
    #[test]
    fn a_scheme_needs_a_connection_of_its_service() {
        let with = |fields: serde_json::Value| LiveConnectionConfig::from_node_fields(fields.as_object().unwrap(), "secure");
        let missing = with(serde_json::json!({ "path": "x", "auth": "api_keys" })).unwrap_err();
        assert_eq!(missing, "'secure' checks callers with api_keys but has no connection; run `weft connect secure`");
        let other = crate::access::Access::new("acc-1", "hmac_auth", None).to_value();
        let wrong = with(serde_json::json!({ "path": "x", "auth": "api_keys", "connection": other })).unwrap_err();
        assert!(wrong.contains("'hmac_auth'") && wrong.contains("'api_key_auth'"), "{wrong}");
        let record = serde_json::json!({ "kind": "hmac", "signature": { "in": "header:X-Hub-Signature-256" }, "concat": "{body}" });
        let cfg = with(serde_json::json!({ "path": "x", "auth": record, "connection": other })).unwrap();
        assert!(matches!(cfg.auth, PublicEntryAuth::Connection { ref verify, .. } if verify.credential_headers() == ["X-Hub-Signature-256"]));
        let open = with(serde_json::json!({ "path": "x", "auth": "none" })).unwrap();
        assert_eq!(open.auth, PublicEntryAuth::None);
    }

    /// A gate whose check an older weft did not keep is never served:
    /// registering it again is refused, naming the fix.
    #[test]
    fn a_gate_whose_check_was_not_kept_is_refused() {
        let auth = PublicEntryAuth::CheckNotKept { access_id: "a".into(), service: "jwt_auth".into() };
        let err = Route { common: LiveConnectionConfig { auth, ..bare() } }.validate().unwrap_err();
        assert!(err.contains("weft resync") && err.contains("jwt_auth"), "{err}");
    }

    /// The two settings that say what gives on a live exchange reach the
    /// config under the words the trigger's select offers, each value to
    /// its own mode, and empty is the default like every other choice.
    #[test]
    fn node_fields_pick_what_gives_on_a_live_exchange() {
        let with = |field: &str, value: &str| {
            let fields = serde_json::json!({ "path": "feed", field: value });
            LiveConnectionConfig::from_node_fields(fields.as_object().unwrap(), "feed").expect("builds")
        };
        assert_eq!(with("whenChunkFails", "surface").error_mode, ErrorMode::Surface);
        assert_eq!(with("whenChunkFails", "drop_chunk").error_mode, ErrorMode::DropChunk);
        assert_eq!(with("whenChunkFails", "").error_mode, ErrorMode::Surface);
        assert_eq!(with("whenCallerIsSlow", "block").backpressure, Backpressure::Block);
        assert_eq!(with("whenCallerIsSlow", "drop_oldest").backpressure, Backpressure::DropOldest);
        assert_eq!(with("whenCallerIsSlow", "drop_newest").backpressure, Backpressure::DropNewest);
        assert_eq!(with("whenCallerIsSlow", "").backpressure, Backpressure::Block);

        // The setting's words are the config's own: what the trigger reads
        // is what the signal carries to the worker.
        for mode in [Backpressure::Block, Backpressure::DropOldest, Backpressure::DropNewest] {
            let word = serde_json::to_value(mode).unwrap();
            assert_eq!(Backpressure::parse_field(word.as_str().unwrap()), Ok(mode));
        }
        for mode in [ErrorMode::Surface, ErrorMode::DropChunk] {
            let word = serde_json::to_value(mode).unwrap();
            assert_eq!(ErrorMode::parse_field(word.as_str().unwrap()), Ok(mode));
        }

        let refusal = LiveConnectionConfig::from_node_fields(
            serde_json::json!({ "path": "feed", "whenCallerIsSlow": "skip" }).as_object().unwrap(),
            "feed",
        )
        .expect_err("not one of the modes");
        assert!(refusal.contains("block, drop_oldest or drop_newest"), "{refusal}");
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
            outlives_caller: false,
            heartbeat_interval_secs: DEFAULT_HEARTBEAT_INTERVAL_SECS,
            caller_silence_secs: DEFAULT_CALLER_SILENCE_SECS,
            max_inbound_bytes: DEFAULT_MAX_INBOUND_BYTES,
            max_session_secs: 0,
            data_type: DataType::Json,
            backpressure: Backpressure::Block,
            error_mode: ErrorMode::Surface,
            journal: crate::stream_journal::JournalPolicy::default(),
            window: None,
        }
    }
}
