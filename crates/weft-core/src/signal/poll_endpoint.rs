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

use crate::primitive::AccessRef;

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
    /// How the poll asks. `Get` (default) sends the bare URL; `Post`
    /// sends `body` as JSON: the shape for feeds that only answer a
    /// query POST (Notion's data-source query).
    #[serde(default)]
    pub method: PollMethod,
    /// The JSON body a `Post` poll sends every tick. Refused on `Get`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<serde_json::Value>,
    /// How the response parses. `Json` (default) reads it as JSON;
    /// `Feed` reads it as a syndication feed (RSS or Atom, either
    /// works) and answers `{ "items": [...] }` where each item is
    /// `{ id, title, link, summary, published, author }`, so the
    /// delta machinery (and `delta.items = "items"`) works unchanged.
    #[serde(default)]
    pub format: PollFormat,
    /// Seconds between polls. Floored at [`MIN_POLL_INTERVAL_SECS`].
    #[serde(default = "default_poll_interval_secs")]
    pub interval_secs: u64,
    /// Delta mode: instead of firing the whole response every poll,
    /// fire once per NEW item since the last poll. The cursor is
    /// durable (it survives listener restarts and pod moves), and the
    /// FIRST poll primes it silently: activating a trigger means
    /// "from now on", never "replay all history".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delta: Option<PollDelta>,
    /// The connection whose credential signs each poll (a provider
    /// API the anonymous client cannot read). Lifted onto the spec by
    /// [`super::to_spec`]; not part of the kind's own config blob
    /// (hence `skip`), so there is exactly one home for it. `None` =
    /// a public URL, polled plain.
    #[serde(skip)]
    pub access: Option<AccessRef>,
    /// The pre-fire filter, over the fire payload (a delta fire is
    /// `{ item }`, positional mode also carrying `index`, so item
    /// fields address as `item.<path>`).
    /// Lifted onto the spec like `access`; the shared fire plumbing
    /// evaluates it, so a filtered-out item costs no execution.
    #[serde(skip)]
    pub filters: Vec<crate::signal::predicate::Predicate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PollDelta {
    /// Dotted path to the response's item array (empty = the response
    /// IS the array).
    pub items: String,
    /// Per-item field that identifies items. With mode `max`, new =
    /// strictly above the stored high-water mark (numeric compare
    /// when both sides parse as numbers, else lexicographic). With
    /// mode `set`, new = an id not in the stored seen-set. `None` =
    /// positional: the array only ever grows and new = items beyond
    /// the stored length (the spreadsheet-rows shape).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor_field: Option<String>,
    /// How `cursor_field` marks newness. `max` (default) requires the
    /// feed's cursor field to be MONOTONIC in visibility order: an
    /// item that appears below the stored high-water mark is treated
    /// as already-seen and never fires. That is a correctness
    /// precondition, not a performance hint; a feed whose items can
    /// surface out of cursor order must use `set`, which remembers
    /// the recently seen ids instead of one high-water mark (a
    /// mailbox listing newest-first with opaque ids).
    #[serde(default)]
    pub mode: DeltaMode,
    /// Feeds that must be TOLD the consumed cursor to drain their
    /// queue (Telegram's `getUpdates` only discards updates once a
    /// later `offset` is sent): when set, each poll appends
    /// `<name>=<stored cursor + offset>` as a query parameter once a
    /// cursor exists (the priming poll sends the bare URL). Requires
    /// mode `max` with a numeric `cursor_field`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor_param: Option<CursorParam>,
}

/// The acknowledged-cursor query parameter of [`PollDelta::cursor_param`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CursorParam {
    /// Query-parameter name (e.g. `offset`).
    pub name: String,
    /// Added to the stored cursor before sending (Telegram wants
    /// `last_update_id + 1`). Defaults to 0.
    #[serde(default)]
    pub offset: i64,
    /// Value sent on the PRIMING poll (no cursor stored yet), for
    /// feeds whose queue must be drained at activation rather than
    /// sampled: Telegram's `offset=-1` answers only the newest queued
    /// update and discards the rest, so "activation means from now
    /// on" holds even with a deep pre-activation backlog. Absent, the
    /// priming poll sends the bare URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prime: Option<i64>,
}

/// The empty GET poll at the default cadence: registration sites fill
/// `url` (+ whatever else they need) and `..Default::default()` the
/// rest. An unset `url` is refused by `validate`, never sent.
impl Default for PollEndpoint {
    fn default() -> Self {
        Self {
            url: String::new(),
            method: PollMethod::Get,
            body: None,
            format: PollFormat::Json,
            interval_secs: DEFAULT_POLL_INTERVAL_SECS,
            delta: None,
            access: None,
            filters: Vec::new(),
        }
    }
}

/// How a poll response's body parses; see [`PollEndpoint::format`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PollFormat {
    #[default]
    Json,
    Feed,
}

/// The HTTP verb a poll tick uses; see [`PollEndpoint::method`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PollMethod {
    #[default]
    Get,
    Post,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeltaMode {
    #[default]
    Max,
    Set,
}

impl Signal for PollEndpoint {
    const TAG: &'static str = "poll_endpoint";

    fn access(&self) -> Option<AccessRef> {
        self.access.clone()
    }

    fn match_predicates(&self) -> &[crate::signal::predicate::Predicate] {
        &self.filters
    }

    fn validate(&self) -> Result<(), String> {
        super::sse_subscribe::validate_http_url(&self.url, "poll_endpoint.url")?;
        if self.interval_secs < MIN_POLL_INTERVAL_SECS {
            return Err(format!(
                "poll_endpoint.interval_secs must be >= {MIN_POLL_INTERVAL_SECS}: a tighter \
                 poll generates runaway load and risks external rate-limit bans; got {}",
                self.interval_secs
            ));
        }
        if self.body.is_some() && self.method != PollMethod::Post {
            return Err(
                "poll_endpoint.body only rides a `post` poll; set method: Post or drop the \
                 body"
                    .into(),
            );
        }
        if let Some(delta) = &self.delta {
            if let Some(f) = &delta.cursor_field {
                if f.trim().is_empty() {
                    return Err(
                        "poll_endpoint.delta.cursor_field is empty; omit it for positional \
                         (count-based) delta instead"
                            .into(),
                    );
                }
            }
            if let Some(cp) = &delta.cursor_param {
                if delta.cursor_field.is_none() || delta.mode != DeltaMode::Max {
                    return Err(
                        "poll_endpoint.delta.cursor_param needs mode `max` with a \
                         cursor_field (the sent value IS the stored high-water mark)"
                            .into(),
                    );
                }
                if cp.name.is_empty()
                    || !cp.name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                {
                    return Err(format!(
                        "poll_endpoint.delta.cursor_param.name must be a plain query \
                         parameter name (alphanumeric or '_'); got '{}'",
                        cp.name
                    ));
                }
            }
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
        let p =
            PollEndpoint { url: "https://x".into(), interval_secs: 1, ..Default::default() };
        assert!(p.validate().unwrap_err().contains("interval_secs"));
    }

    #[test]
    fn valid_round_trips() {
        let p =
            PollEndpoint { url: "https://x/u".into(), interval_secs: 10, ..Default::default() };
        let spec = crate::signal::to_spec(p);
        assert_eq!(spec.kind, "poll_endpoint");
    }

    #[test]
    fn delta_round_trips_and_validates() {
        let p: PollEndpoint = serde_json::from_value(serde_json::json!({
            "url": "https://x/u",
            "delta": { "items": "values" }
        }))
        .unwrap();
        p.validate().unwrap();
        assert_eq!(p.delta.as_ref().unwrap().items, "values");
        // A plain (non-delta) spec keeps its old wire shape: no field.
        let plain =
            PollEndpoint { url: "https://x".into(), interval_secs: 30, ..Default::default() };
        let wire = serde_json::to_value(&plain).unwrap();
        assert!(wire.get("delta").is_none());

        let bad = PollEndpoint {
            url: "https://x".into(),
            interval_secs: 30,
            delta: Some(PollDelta {
                items: "values".into(),
                cursor_field: Some(" ".into()),
                mode: Default::default(),
                cursor_param: None,
            }),
            ..Default::default()
        };
        assert!(bad.validate().unwrap_err().contains("cursor_field"));
    }

    /// The acknowledged-cursor parameter is only coherent on a
    /// max-mode cursor feed, and its name must be a plain query
    /// parameter name.
    #[test]
    fn cursor_param_validates() {
        let p = |delta: serde_json::Value| -> PollEndpoint {
            serde_json::from_value(serde_json::json!({
                "url": "https://x/u", "interval_secs": 30, "delta": delta
            }))
            .unwrap()
        };
        p(serde_json::json!({
            "items": "result", "cursor_field": "update_id",
            "cursor_param": { "name": "offset", "offset": 1 }
        }))
        .validate()
        .unwrap();

        let no_field = p(serde_json::json!({
            "items": "result", "cursor_param": { "name": "offset" }
        }));
        assert!(no_field.validate().unwrap_err().contains("cursor_param"));

        let set_mode = p(serde_json::json!({
            "items": "result", "cursor_field": "id", "mode": "set",
            "cursor_param": { "name": "offset" }
        }));
        assert!(set_mode.validate().unwrap_err().contains("cursor_param"));

        let bad_name = p(serde_json::json!({
            "items": "result", "cursor_field": "id",
            "cursor_param": { "name": "off set" }
        }));
        assert!(bad_name.validate().unwrap_err().contains("name"));
    }
}
