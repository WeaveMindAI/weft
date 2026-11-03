//! Outbound event source (2 of 3): periodic HTTP poll. The listener hits
//! `url` every `interval_secs` and fires with the response body. No
//! persistent connection: this is the right shape for APIs that only offer
//! a "give me what's new" endpoint (long-poll or plain poll), e.g. a bot
//! getUpdates loop. The polling node owns any cursor/offset bookkeeping by
//! varying the URL it registers; the language only owns the timer + fire +
//! the listener keep-alive across worker stalls.
//!
//! Two ways to use it:
//!   - `ctx.register_signal(PollEndpoint { .. })`: an entry trigger, each
//!     fire starts a fresh run.
//!   - `state.wait(field, PollEndpoint { .. })`: a node that started an
//!     outside job waits for it without holding a worker. The first poll
//!     runs at once (a job already done resumes straight away), and the
//!     first response that passes `filters` resumes the run, as the value
//!     the wait returns; then the polling stops. With no filters the
//!     first successful response resumes it. A non-2xx answer or a failed
//!     request is not an answer: it is retried at the next poll and shown
//!     on the node. `delta` is refused here, since its first poll primes
//!     silently and would swallow the answer. While it waits, the raw
//!     last answer it read shows on the waiting node, and
//!     `PollEndpoint { .. }.cancel_with(call)` names the job's own cancel address,
//!     called once if the run is cancelled meanwhile
//!     ([`super::cancel_call`]).
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
    /// durable (it survives listener restarts and process moves), and the
    /// FIRST poll primes it silently: activating a trigger means
    /// "from now on", never "replay all history".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delta: Option<PollDelta>,
    /// The fields of the response a fire carries, as dotted paths
    /// (`status`, `error.message`); empty carries the whole response.
    /// The filters still read the whole response, so a wait can say
    /// "done" on a field and carry only what the node reads next. For an
    /// answer that is mostly data the node fetches again anyway (a job
    /// status that embeds its first page of results), this keeps that
    /// data out of the queued fire and the run's journal. Plain mode only.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub carry: Vec<String>,
    /// The connection whose credential signs each poll (a provider
    /// API the anonymous client cannot read). Lifted onto the spec by
    /// [`super::to_spec`]; not part of the kind's own config blob
    /// (hence `skip`), so there is exactly one home for it. `None` =
    /// a public URL, polled plain.
    #[serde(skip)]
    pub access: Option<AccessRef>,
    /// The pre-fire filter, over the fire payload (a delta fire is
    /// `{ item }`, positional mode also carrying `index`, so item
    /// fields address as `item.<path>`; a plain fire is the response
    /// itself). For a wait this is what says the job is done.
    /// Lifted onto the spec like `access`; the shared fire plumbing
    /// evaluates it, so a filtered-out item costs no run.
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
    /// For a feed that answers in pages, newest first: where each answer
    /// says the next page's cursor, and where the next request carries
    /// it. Each poll follows the pages until one holds an item already
    /// seen (or there is no next page), and fires every new item it
    /// read, so a burst bigger than a page is never cut to its newest
    /// page. The priming poll reads the first page alone. Past
    /// [`crate::access::client::MAX_PAGES`] pages of new items the poll
    /// fails loudly and fires nothing, until the trigger polls often
    /// enough (or asks for pages big enough) to catch up. Absent, a poll
    /// reads one answer. Needs a `cursor_field` (paging stops at an item
    /// already seen) and the JSON format, and is refused beside
    /// `cursor_param`, which already says where the next request starts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paging: Option<PollPaging>,
}

/// How a paged feed's next page is asked for ([`PollDelta::paging`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PollPaging {
    /// Dotted path to the next page's cursor in an answer
    /// (`nextPageToken`, `next_cursor`). An answer whose cursor is
    /// absent, not a string, or empty is the last page.
    pub next: String,
    /// Where the next request carries the cursor.
    pub sent_in: PageCursorIn,
}

/// Where a paged poll's next request carries the cursor
/// ([`PollPaging::sent_in`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum PageCursorIn {
    /// A query parameter of that name, on a `get` or a `post` poll.
    Query(String),
    /// A top-level field of that name in a `post` poll's JSON body
    /// (Notion's `start_cursor`).
    Body(String),
}

impl PollPaging {
    /// The cursor at `next`, sent back as the query parameter `name`.
    pub fn query(next: impl Into<String>, name: impl Into<String>) -> Self {
        Self { next: next.into(), sent_in: PageCursorIn::Query(name.into()) }
    }

    /// The cursor at `next`, sent back as the body field `field`.
    pub fn body(next: impl Into<String>, field: impl Into<String>) -> Self {
        Self { next: next.into(), sent_in: PageCursorIn::Body(field.into()) }
    }
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
            carry: Vec::new(),
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
        if !self.carry.is_empty() && self.delta.is_some() {
            return Err("poll_endpoint.carry trims a plain poll's answer; a delta poll fires one item at a time, so drop one of the two".into());
        }
        if let Some(empty) = self.carry.iter().find(|path| path.trim().is_empty()) {
            return Err(format!("poll_endpoint.carry names an empty path ({empty:?}); list the fields the fire keeps, like `status`"));
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
                if !plain_name(&cp.name) {
                    return Err(format!(
                        "poll_endpoint.delta.cursor_param.name must be a plain query \
                         parameter name (alphanumeric or '_'); got '{}'",
                        cp.name
                    ));
                }
            }
            if let Some(paging) = &delta.paging {
                self.validate_paging(delta, paging)?;
            }
        }
        Ok(())
    }
}

/// What a fire carries of `response`: the fields `carry` names, each
/// at its own path, or the whole response when `carry` is empty. A
/// named field the response lacks is left out, so the node reads it as
/// absent, the way the filters do.
pub fn carried(response: &serde_json::Value, carry: &[String]) -> serde_json::Value {
    if carry.is_empty() {
        return response.clone();
    }
    let mut out = serde_json::Value::Object(serde_json::Map::new());
    for path in carry {
        let Some(found) = crate::access::spec::lookup_path(response, path) else { continue };
        let mut at = &mut out;
        let mut segments = path.split('.').peekable();
        while let Some(segment) = segments.next() {
            let serde_json::Value::Object(map) = at else { break };
            if segments.peek().is_none() {
                map.insert(segment.to_string(), found.clone());
                break;
            }
            at = map.entry(segment.to_string()).or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
        }
    }
    out
}

impl PollEndpoint {
    /// Refuse a paging description that could not be followed
    /// ([`PollDelta::paging`]).
    fn validate_paging(&self, delta: &PollDelta, paging: &PollPaging) -> Result<(), String> {
        if delta.cursor_field.is_none() {
            return Err("poll_endpoint.delta.paging stops at the first page holding an item already seen, \
                        which needs a cursor_field to tell; a positional delta reads one answer"
                .into());
        }
        if delta.cursor_param.is_some() {
            return Err("poll_endpoint.delta.paging and cursor_param both say where the next request \
                        starts; keep the one the feed answers to"
                .into());
        }
        if self.format != PollFormat::Json {
            return Err("poll_endpoint.delta.paging reads the next page's cursor out of a JSON answer; \
                        a syndication feed has none"
                .into());
        }
        if paging.next.trim().is_empty() {
            return Err("poll_endpoint.delta.paging.next is empty; name where an answer says its next \
                        page's cursor, like `nextPageToken`"
                .into());
        }
        match &paging.sent_in {
            PageCursorIn::Query(name) if !plain_name(name) => Err(format!(
                "poll_endpoint.delta.paging sends the cursor as a query parameter, which must be a \
                 plain name (alphanumeric or '_'); got '{name}'"
            )),
            PageCursorIn::Body(_) if self.method != PollMethod::Post => Err(
                "poll_endpoint.delta.paging sends the cursor in the body, which only a `post` poll \
                 sends; set method: Post or send it as a query parameter"
                    .into(),
            ),
            PageCursorIn::Body(field) if field.trim().is_empty() => {
                Err("poll_endpoint.delta.paging sends the cursor in the body under an empty field name".into())
            }
            PageCursorIn::Body(_) if self.body.as_ref().is_some_and(|body| !body.is_object()) => Err(
                "poll_endpoint.delta.paging sends the cursor as a field of the body, so the body must \
                 be a JSON object"
                    .into(),
            ),
            PageCursorIn::Query(_) | PageCursorIn::Body(_) => Ok(()),
        }
    }
}

/// A plain query parameter name: alphanumeric or `_`, not empty.
fn plain_name(name: &str) -> bool {
    !name.is_empty() && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

crate::register_signal_kind!(PollEndpoint);

#[cfg(test)]
mod tests {
    use super::*;

    /// A fire carries only the named fields, each at its own path; an
    /// empty list carries the whole answer, and a missing field is left out.
    #[test]
    fn a_fire_carries_only_the_named_fields() {
        let answer = serde_json::json!({"status": "completed", "error": {"code": 3, "message": "x"}, "data": [1, 2, 3]});
        assert_eq!(carried(&answer, &[]), answer);
        assert_eq!(
            carried(&answer, &["status".into(), "error.message".into(), "missing".into()]),
            serde_json::json!({"status": "completed", "error": {"message": "x"}})
        );
        let mut delta = PollEndpoint { url: "https://example.com/x".into(), carry: vec!["status".into()], ..Default::default() };
        delta.delta = Some(PollDelta { items: String::new(), cursor_field: None, mode: DeltaMode::default(), cursor_param: None, paging: None });
        assert!(delta.validate().unwrap_err().contains("carry"));
    }

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
                paging: None,
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

    /// The paging description rides the spec as JSON, each place the
    /// cursor goes keeping its wire word, and an unknown key is refused.
    #[test]
    fn paging_round_trips() {
        let p: PollEndpoint = serde_json::from_value(serde_json::json!({
            "url": "https://x/u",
            "delta": {
                "items": "messages", "cursor_field": "id", "mode": "set",
                "paging": { "next": "nextPageToken", "sent_in": { "query": "pageToken" } }
            }
        }))
        .unwrap();
        p.validate().unwrap();
        assert_eq!(p.delta.as_ref().unwrap().paging, Some(PollPaging::query("nextPageToken", "pageToken")));
        let wire = serde_json::to_value(&p).unwrap();
        assert_eq!(wire["delta"]["paging"], serde_json::json!({ "next": "nextPageToken", "sent_in": { "query": "pageToken" } }));
        let in_body = PollPaging::body("next_cursor", "start_cursor");
        assert_eq!(serde_json::to_value(&in_body).unwrap(), serde_json::json!({ "next": "next_cursor", "sent_in": { "body": "start_cursor" } }));
        assert_eq!(serde_json::from_value::<PollPaging>(serde_json::to_value(&in_body).unwrap()).unwrap(), in_body);
        assert!(serde_json::from_value::<PollPaging>(serde_json::json!({ "next": "n", "sent_in": { "query": "p" }, "param": "p" })).is_err());
        // A delta without paging writes no field.
        let plain = PollDelta { items: "items".into(), cursor_field: Some("id".into()), mode: DeltaMode::Max, cursor_param: None, paging: None };
        assert!(serde_json::to_value(&plain).unwrap().get("paging").is_none());
    }

    /// Paging is refused where it could not be followed: no cursor field
    /// to stop on, beside an acknowledged cursor, on a syndication feed, a
    /// body cursor on a `get` poll or beside a body that is not an object,
    /// and a query parameter that is not a plain name.
    #[test]
    fn paging_validates() {
        let poll = |config: serde_json::Value| -> PollEndpoint { serde_json::from_value(config).unwrap() };
        let delta = |paging: serde_json::Value| serde_json::json!({ "items": "results", "cursor_field": "id", "mode": "set", "paging": paging });
        let query = serde_json::json!({ "next": "next", "sent_in": { "query": "offset" } });
        let body = serde_json::json!({ "next": "next_cursor", "sent_in": { "body": "start_cursor" } });
        poll(serde_json::json!({ "url": "https://x/u", "delta": delta(query.clone()) })).validate().unwrap();
        poll(serde_json::json!({ "url": "https://x/u", "method": "post", "body": { "page_size": 100 }, "delta": delta(body.clone()) })).validate().unwrap();
        poll(serde_json::json!({ "url": "https://x/u", "method": "post", "delta": delta(body.clone()) })).validate().unwrap();

        let positional = poll(serde_json::json!({ "url": "https://x/u", "delta": { "items": "values", "paging": query } }));
        assert!(positional.validate().unwrap_err().contains("cursor_field"));
        let acknowledged = poll(serde_json::json!({ "url": "https://x/u", "delta": {
            "items": "result", "cursor_field": "update_id", "cursor_param": { "name": "offset" }, "paging": query
        } }));
        assert!(acknowledged.validate().unwrap_err().contains("cursor_param"));
        let feed = poll(serde_json::json!({ "url": "https://x/u", "format": "feed", "delta": delta(query.clone()) }));
        assert!(feed.validate().unwrap_err().contains("JSON"));
        let on_get = poll(serde_json::json!({ "url": "https://x/u", "delta": delta(body.clone()) }));
        assert!(on_get.validate().unwrap_err().contains("post"));
        let listed_body = poll(serde_json::json!({ "url": "https://x/u", "method": "post", "body": [1], "delta": delta(body) }));
        assert!(listed_body.validate().unwrap_err().contains("object"));
        let bad_name = poll(serde_json::json!({ "url": "https://x/u", "delta": delta(serde_json::json!({ "next": "next", "sent_in": { "query": "page token" } })) }));
        assert!(bad_name.validate().unwrap_err().contains("plain name"));
        let no_next = poll(serde_json::json!({ "url": "https://x/u", "delta": delta(serde_json::json!({ "next": " ", "sent_in": { "query": "offset" } })) }));
        assert!(no_next.validate().unwrap_err().contains("next"));
    }
}
