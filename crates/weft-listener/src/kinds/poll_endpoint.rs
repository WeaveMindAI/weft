//! Periodic HTTP poll handler. Hits the configured URL once per wake,
//! every `interval_secs`, and fires a fresh execution carrying the
//! response body (JSON if it parses, else a JSON string), or, in delta
//! mode, one per new item past the cursor kept on the signal row. Nothing
//! runs between polls: each wake polls once and sets the next.


use anyhow::Result;
use serde_json::Value;
use tracing::warn;
use weft_core::primitive::{SignalAuth, SignalRouting, SignalSpec, SignalSurface};
use weft_core::signal::{PollEndpoint, Signal};

use weft_core::signal::listener_protocol::{ProcessOutcome, ProcessTarget};
use crate::registry::RegisteredSignal;

use async_trait::async_trait;

use super::{BetweenFires, KindHandler, LiveCtx, SpawnCtx, WakeFrom, Woken};
use weft_core::live::{LiveFeed, LiveItem};

pub struct PollEndpointHandler;

#[async_trait]
impl KindHandler for PollEndpointHandler {
    fn tag(&self) -> &'static str {
        PollEndpoint::TAG
    }

    fn between_fires(&self) -> BetweenFires {
        BetweenFires::Wakes
    }

    fn compute_routing(&self, _spec: &SignalSpec) -> Result<SignalRouting> {
        Ok(SignalRouting {
            surface: SignalSurface::Internal,
            auth: SignalAuth::None,
            auth_config: Value::Null,
        })
    }

    /// The poll state is a FEED CURSOR (what was already delivered),
    /// not a schedule: a reactivate must carry it forward verbatim.
    /// Re-priming instead would silently discard everything the feed
    /// queued while the project was inactive (and on an
    /// acknowledged-cursor feed like Telegram, actively tell the
    /// provider to drop it).
    fn compute_initial_state(&self, _spec: &SignalSpec, prior: Option<&Value>, _asked_at_unix_ms: i64) -> Result<Value> {
        Ok(prior.cloned().unwrap_or_else(|| Value::Object(serde_json::Map::new())))
    }

    /// The next point on the poll's grid: every `interval_secs` counted
    /// from the epoch, so everyone asking within one interval (a
    /// registration and the rehydrate after it, two copies of the
    /// listener) names the same moment and sets one wake. A slow poll
    /// or a late wake skips the points it missed rather than polling in
    /// a burst to catch up.
    fn next_wake(&self, spec: &SignalSpec, _state: &Value, _from: WakeFrom, now_ms: i64) -> Result<Option<i64>> {
        let poll: PollEndpoint = serde_json::from_value(spec.config.clone())
            .map_err(|e| anyhow::anyhow!("malformed poll_endpoint spec: {e}"))?;
        Ok(Some(next_grid_point(now_ms, poll.interval_secs)))
    }

    async fn on_wake(&self, spec: &SignalSpec, woken: Woken, ctx: SpawnCtx) -> Result<Value> {
        let poll: PollEndpoint = serde_json::from_value(spec.config.clone())
            .map_err(|e| anyhow::anyhow!("malformed poll_endpoint spec: {e}"))?;
        poll_once(&poll, &spec.access, woken.state, woken.seq, &ctx).await
    }

    fn process_entry(&self, _sig: &RegisteredSignal, payload: Value) -> ProcessOutcome {
        // A poll result (raised by a wake via `ctx.fire`) routes to the
        // entry trigger.
        ProcessOutcome { value: payload, target: ProcessTarget::Entry }
    }

    /// A poll has no address either: what somebody wants to see is
    /// what it is reading and how often, plus whether its last polls
    /// went through.
    fn live(&self, ctx: &LiveCtx<'_>) -> LiveFeed {
        let sig = ctx.sig;
        let poll = match super::config_for_display::<PollEndpoint>(sig, "Polling") {
            Ok(poll) => poll,
            Err(feed) => return feed,
        };
        let mut items: Vec<LiveItem> =
            super::configured_url_item("Polling", &poll.url).into_iter().collect();
        items.push(LiveItem::text("Every", format!("{}s", poll.interval_secs)));
        // The status is read off the row (`sig.kind_state`), since each
        // poll may have run on another copy of the listener.
        match sig.kind_state.as_ref().map(PollState::read) {
            Some(Ok(state)) => items.push(LiveItem::text("State", state.status_line())),
            Some(Err(e)) => items.push(LiveItem::text("State", format!("{e:#}"))),
            None => {}
        }
        LiveFeed::new(items)
    }

    fn render(&self, _token: &str, _sig: &RegisteredSignal) -> Result<Option<Value>> {
        Ok(None)
    }
}

/// The first multiple of `interval_secs` (from the epoch) strictly after
/// `now_ms`.
fn next_grid_point(now_ms: i64, interval_secs: u64) -> i64 {
    let step = (interval_secs.max(1) as i64).saturating_mul(1000);
    (now_ms.div_euclid(step) + 1).saturating_mul(step)
}

/// A poll's durable state, all of it on the signal row: each wake may
/// land on a different copy of the listener, so nothing about the poll
/// lives in a process between wakes.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PollState {
    /// The delta cursor (what was already delivered). PRIMED iff present:
    /// a priming poll that pinned nothing (an empty first page) leaves it
    /// absent, and a primed cursor never falls back to the prime value,
    /// which on a queue-draining feed means "discard everything queued".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    delta: Option<Value>,
    /// Polls in a row that failed, across every copy that ran one.
    #[serde(default, skip_serializing_if = "is_zero")]
    consecutive_failures: u32,
    /// Why the last of them failed. Absent once a poll went through.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_failure: Option<String>,
}

fn is_zero(n: &u32) -> bool {
    *n == 0
}

impl PollState {
    fn read(state: &Value) -> Result<Self> {
        serde_json::from_value(state.clone()).map_err(|e| anyhow::anyhow!("malformed poll_endpoint state {state}: {e}"))
    }

    fn to_value(&self) -> Value {
        serde_json::to_value(self).expect("a poll state always serializes")
    }

    /// What the node's display says about the polls.
    fn status_line(&self) -> String {
        match (&self.last_failure, self.consecutive_failures) {
            (Some(why), n) if n > 0 => format!("{why} (failed {n} in a row)"),
            _ => "polling".to_string(),
        }
    }
}

/// Move the poll's state from `seq` to `next`, as a claim: of two copies
/// that polled at the same state, one write lands and the other's is
/// dropped (they read the same feed, so either answer stands). A write
/// that could not be made is logged: the next poll starts from the older
/// state, which only re-offers items, never loses them.
async fn store(ctx: &SpawnCtx, seq: i64, next: &PollState) {
    match ctx.fire.claim_kind_state(next.to_value(), seq).await {
        Ok(true) => {}
        Ok(false) => tracing::debug!(
            target: "weft_listener::poll_endpoint",
            token = %ctx.fire.token(),
            "another copy moved this poll's state first; its write stands"
        ),
        Err(e) => warn!(
            target: "weft_listener::poll_endpoint",
            token = %ctx.fire.token(), error = %format!("{e:#}"),
            "poll state write failed"
        ),
    }
}

/// One poll: fetch, fire what is new, persist the advanced cursor.
/// Answers the state the signal now stands at. A failed poll fires
/// nothing and moves no cursor; it counts the streak on the row, where
/// the node's display reads it, and a streak that keeps growing
/// escalates to an error so a misconfigured trigger is never
/// indistinguishable from a quiet feed.
async fn poll_once(
    poll: &PollEndpoint,
    access: &Option<weft_core::primitive::AccessRef>,
    kind_state: Value,
    seq: i64,
    ctx: &SpawnCtx,
) -> Result<Value> {
    let url = poll.url.as_str();
    let before = PollState::read(&kind_state)?;
    let state = before.delta.clone();
    let failed = |what: &str, detail: String| {
        let streak = before.consecutive_failures + 1;
        if streak >= POLL_FAILURE_ESCALATION {
            tracing::error!(
                target: "weft_listener::poll_endpoint",
                %url, consecutive_failures = streak, error = %detail,
                "{what}; this trigger has not produced a successful poll in {streak} attempts, \
                 check its endpoint and recipe",
            );
        } else {
            warn!(target: "weft_listener::poll_endpoint", %url, error = %detail, "{what}; will retry next poll");
        }
        PollState { delta: before.delta.clone(), consecutive_failures: streak, last_failure: Some(format!("{what}: {detail}")) }
    };
    let next = 'poll: {
        // Signed-in polls resolve the connection PER POLL: the credential
        // is refreshed store-side and never frozen. No connection = a
        // plain client.
        let client = match crate::listener_access::client_for(access, ctx).await {
            Ok(c) => c,
            Err(e) => break 'poll failed("connection resolve failed", format!("{e:#}")),
        };
        let url = match crate::infra_address::for_listener(url, ctx).await {
            Ok(u) => u,
            Err(e) => break 'poll failed("address resolve failed", format!("{e:#}")),
        };
        let poll_url = match poll_url(&url, poll.delta.as_ref(), state.as_ref()) {
            Ok(u) => u,
            // A primed-but-unreadable cursor must NEVER fall back to the
            // prime value: on a queue-draining feed the prime means
            // "discard everything".
            Err(e) => break 'poll failed("cursor unusable", format!("{e:#}")),
        };
        // The verb and body are part of the registered recipe: a POST
        // feed (a query endpoint) sends its declared body every poll, a
        // GET feed sends the bare URL.
        let request = match poll.method {
            weft_core::signal::PollMethod::Get => client.get(&poll_url),
            weft_core::signal::PollMethod::Post => {
                let req = client.post(&poll_url);
                match &poll.body {
                    Some(b) => req.json(b),
                    None => req,
                }
            }
        };
        let resp = match request.send().await {
            Ok(r) if r.status().is_success() => r,
            Ok(r) => break 'poll failed("non-success poll", r.status().to_string()),
            Err(e) => break 'poll failed("poll request failed", e.to_string()),
        };
        let body = match resp.text().await {
            Ok(b) => b,
            Err(e) => break 'poll failed("poll body read failed", e.to_string()),
        };
        let Some(delta) = &poll.delta else {
            // Plain mode: every poll fires the whole response. It has no
            // replay cursor; the delivery outcome is already logged by
            // the fire path.
            let _ = ctx.fire.fire(super::event_source::coerce_text_payload(body), "poll_endpoint").await;
            break 'poll PollState { delta: None, consecutive_failures: 0, last_failure: None };
        };

        // Delta mode: fire once per NEW item, then persist the advanced
        // cursor. The declared format decides how the body becomes the
        // item-bearing JSON.
        let parsed: Value = match poll.format {
            weft_core::signal::PollFormat::Json => match serde_json::from_str(&body) {
                Ok(v) => v,
                Err(e) => break 'poll failed("delta poll needs a JSON response", e.to_string()),
            },
            weft_core::signal::PollFormat::Feed => match feed_items(&body) {
                Ok(v) => v,
                Err(e) => break 'poll failed("delta poll needs an RSS/Atom feed", e),
            },
        };
        let (fires, idle_state) = match delta_advance(delta, &parsed, state.as_ref()) {
            Ok(x) => x,
            Err(e) => break 'poll failed("delta poll skipped", e.to_string()),
        };
        // The first successful poll PRIMES the cursor silently
        // (activation means "from now on"); `delta_advance` returns no
        // fires for it by construction. Each fire carries the state that
        // acknowledges EXACTLY it, and the cursor advances to the last
        // item that actually delivered: a dropped enqueue stops the
        // batch, so its item (and everything after it) is re-offered next
        // poll, while already-delivered items are never re-offered (a
        // re-offer past a completed task would EXECUTE again; the enqueue
        // dedup only collapses onto live task rows).
        let advanced = if fires.is_empty() {
            idle_state.or(state.clone())
        } else {
            let mut advanced = state.clone();
            for (payload, state_after) in fires {
                match ctx.fire.fire(payload, "poll_endpoint").await {
                    // Not delivered (a transient failure, or a token the
                    // broker does not know): hold the cursor so the item
                    // is re-offered.
                    crate::event_context::FireOutcome::EnqueueFailed
                    | crate::event_context::FireOutcome::UnknownSignal => break,
                    crate::event_context::FireOutcome::Fired | crate::event_context::FireOutcome::Filtered => {
                        advanced = Some(state_after);
                    }
                }
            }
            advanced
        };
        PollState { delta: advanced, consecutive_failures: 0, last_failure: None }
    };
    if next != before {
        store(ctx, seq, &next).await;
    }
    Ok(next.to_value())
}

/// Consecutive failed polls before the per-poll warning escalates to
/// an error naming the streak.
const POLL_FAILURE_ESCALATION: u32 = 3;

/// An RSS/Atom body as the delta pipeline's item JSON:
/// `{ "items": [{ id, title, link, summary, published, author }] }`.
/// The id is the feed's own GUID; for a feed that mints none, the
/// parser synthesizes a stable content hash, so dedup always has
/// something durable.
fn feed_items(body: &str) -> Result<Value, String> {
    let feed = feed_rs::parser::parse(body.as_bytes())
        .map_err(|e| format!("feed does not parse: {e}"))?;
    let items: Vec<Value> = feed
        .entries
        .iter()
        .map(|e| {
            let link = e.links.first().map(|l| l.href.clone());
            let title = e.title.as_ref().map(|t| t.content.clone());
            serde_json::json!({
                "id": e.id,
                "title": title,
                "link": link,
                "summary": e.summary.as_ref().map(|s| s.content.clone()),
                "published": e.published.or(e.updated).map(|t| t.to_rfc3339()),
                "author": e.authors.first().map(|a| a.name.clone()),
            })
        })
        .collect();
    Ok(serde_json::json!({ "items": items }))
}

/// The URL one poll tick actually requests: the configured URL, plus
/// the acknowledged-cursor parameter when the spec declares one and a
/// numeric cursor is stored. The priming poll (no stored state at
/// all) sends the declared `prime` value instead when one is set (a
/// queue-draining feed's "give me only the newest" idiom); bare URL
/// otherwise. A PRIMED state whose cursor does not read as a number
/// is an error, never a re-prime: on a queue-draining feed the prime
/// value means "discard everything queued".
fn poll_url(
    url: &str,
    delta: Option<&weft_core::signal::PollDelta>,
    state: Option<&Value>,
) -> anyhow::Result<String> {
    let Some(cp) = delta.and_then(|d| d.cursor_param.as_ref()) else {
        return Ok(url.to_string());
    };
    let value = match state {
        None => match cp.prime {
            Some(prime) => prime,
            None => return Ok(url.to_string()),
        },
        Some(s) => {
            let cursor = s.get("cursor").and_then(|c| match c {
                Value::Number(n) => n.as_i64(),
                Value::String(s) => s.parse().ok(),
                _ => None,
            });
            match cursor {
                Some(cursor) => cursor.saturating_add(cp.offset),
                None => anyhow::bail!(
                    "the stored cursor ({}) is not a number, but the '{}' \
                     cursor parameter needs one; the feed's cursor field \
                     answered a non-numeric value",
                    s.get("cursor").unwrap_or(&Value::Null),
                    cp.name,
                ),
            }
        }
    };
    let sep = if url.contains('?') { '&' } else { '?' };
    Ok(format!("{url}{sep}{}={value}", cp.name))
}

/// What one poll advanced by: the new items as `(cursor, item)` pairs in
/// ascending cursor order, and the state to store for the next poll.
type DeltaAdvance = (Vec<(Value, Value)>, Option<Value>);

/// The pure delta step. Returns the NEW items past `state` as
/// `(payload, state_after)` pairs, each `state_after` being the durable
/// state that acknowledges exactly that item and everything before it
/// (so the caller can persist a PREFIX when a fire fails mid-batch),
/// plus the idle state to persist when nothing fires (priming, a
/// shrunken positional array); an idle of `None` means NOTHING to
/// persist (a cursor-mode priming poll that saw an empty page stays
/// unprimed). `state` `None` = unprimed: no fires, prime to the
/// current end. Each payload is wrapped as
/// `{ "item": <the item> }`; POSITIONAL mode adds `"index"` (the
/// item's 0-based array position), which is a stable row address there
/// because the array only grows (a spreadsheet row number). Field and
/// set modes carry NO index: their arrays may reorder between polls,
/// so a page position is meaningless, and a stable payload is what
/// lets a retried enqueue of the same item collapse onto the in-flight
/// task instead of delivering it twice.
///
/// Positional mode (no cursor_field): the array only grows; new =
/// items beyond the stored `count`. A SHRUNKEN array re-baselines to
/// the shorter length without firing (rows were deleted; the items
/// that later reappear at those positions are genuinely new).
///
/// Field mode: new = items whose field orders strictly above the
/// stored `cursor` (numeric compare when both sides parse as numbers,
/// else lexicographic), OFFERED IN ASCENDING cursor order so the
/// prefix-acknowledge rule holds. An item missing the field is an
/// error naming it (the recipe is wrong; the caller skips the poll
/// loudly rather than silently dropping items).
fn delta_advance(
    delta: &weft_core::signal::PollDelta,
    response: &Value,
    state: Option<&Value>,
) -> anyhow::Result<DeltaAdvance> {
    let items = if delta.items.is_empty() {
        response
    } else {
        weft_core::access::spec::lookup_path(response, &delta.items)
            .ok_or_else(|| anyhow::anyhow!("the response carries nothing at '{}'", delta.items))?
    };
    let items = items
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("'{}' is not an array in the response", delta.items))?;

    match &delta.cursor_field {
        None => {
            let seen = state.and_then(|s| s.get("count")).and_then(Value::as_u64);
            let idle = Some(serde_json::json!({ "count": items.len() }));
            match seen {
                None => Ok((Vec::new(), idle)),
                Some(seen) => {
                    let fires = items
                        .iter()
                        .enumerate()
                        .skip(seen as usize)
                        .map(|(i, item)| {
                            (
                                serde_json::json!({ "item": item, "index": i }),
                                serde_json::json!({ "count": i + 1 }),
                            )
                        })
                        .collect();
                    Ok((fires, idle))
                }
            }
        }
        Some(field) => {
            let mut keyed = Vec::with_capacity(items.len());
            for item in items {
                let key = weft_core::access::spec::lookup_path(item, field)
                    .ok_or_else(|| {
                        anyhow::anyhow!("an item carries nothing at cursor_field '{field}'")
                    })?
                    .clone();
                // An acknowledged-cursor feed does arithmetic on the
                // cursor (`stored + offset` on the next poll's URL), so
                // a non-numeric key must be refused HERE, before it is
                // ever stored, not discovered at URL-build time.
                if delta.cursor_param.is_some()
                    && !matches!(&key, Value::Number(_))
                    && !matches!(&key, Value::String(s) if s.parse::<i64>().is_ok())
                {
                    anyhow::bail!(
                        "cursor_field '{field}' answered a non-numeric value ({key}), \
                         but the acknowledged-cursor parameter needs a number"
                    );
                }
                keyed.push((key, item.clone()));
            }
            if delta.mode == weft_core::signal::DeltaMode::Set {
                let (fires, idle) = set_advance(keyed, state);
                return Ok((fires, Some(idle)));
            }
            let stored = state.and_then(|s| s.get("cursor")).cloned();
            let Some(stored) = stored else {
                // Priming: pin the cursor at the current max. An EMPTY
                // first page persists NOTHING (stay unprimed): there
                // is no cursor to pin, and a persisted cursor-less
                // state would read as primed-but-unreadable forever.
                let max = keyed.iter().map(|(k, _)| k.clone()).max_by(cursor_cmp);
                let idle = max.map(|m| serde_json::json!({ "cursor": m }));
                return Ok((Vec::new(), idle));
            };
            // New items ascending by cursor, so acknowledging a prefix
            // of the fires never skips an unfired item's key.
            let mut fresh: Vec<(usize, (Value, Value))> = keyed
                .into_iter()
                .enumerate()
                .filter(|(_, (k, _))| cursor_cmp(k, &stored) == std::cmp::Ordering::Greater)
                .collect();
            fresh.sort_by(|(_, (a, _)), (_, (b, _))| cursor_cmp(a, b));
            let fires = fresh
                .into_iter()
                .map(|(_, (k, item))| {
                    (
                        serde_json::json!({ "item": item }),
                        serde_json::json!({ "cursor": k }),
                    )
                })
                .collect();
            let idle = Some(serde_json::json!({ "cursor": stored }));
            Ok((fires, idle))
        }
    }
}

/// How many seen ids the set mode remembers. Far above any sane poll
/// page size; the cap only exists so the row's state cannot grow
/// without bound on a huge feed.
const SEEN_SET_CAP: usize = 1000;

/// The set-mode delta step: fire items whose id is not in the stored
/// seen list. The persisted memory keeps ids in RECENCY order: the
/// current page's acknowledged ids first (they are definitionally the
/// most recent), then the stored ids, truncated at the cap. That
/// ordering is load-bearing: an id still visible on every page must
/// never age out of the memory (a pinned message would re-fire each
/// time it cycled back in); only genuinely-gone ids may. Each fire's
/// `state_after` adds exactly the ids acknowledged so far, so a
/// mid-batch failure re-offers only the unfired ids next poll. When
/// nothing fires the stored state is returned VERBATIM: a feed that
/// merely reordered its page must not churn a durable write per tick.
/// Unprimed = prime silently, exactly like the other modes.
fn set_advance(
    keyed: Vec<(Value, Value)>,
    state: Option<&Value>,
) -> (Vec<(Value, Value)>, Value) {
    let id_of = |v: &Value| match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    let stored: Option<Vec<String>> = state.and_then(|s| s.get("seen")).map(|seen| {
        seen.as_array()
            .into_iter()
            .flatten()
            .map(id_of)
            .collect()
    });
    let current_ids: Vec<String> = keyed.iter().map(|(k, _)| id_of(k)).collect();
    let Some(stored) = stored else {
        // Priming: remember the page in its own (recency) order.
        let mut seen: Vec<String> = Vec::with_capacity(current_ids.len());
        for id in current_ids {
            if !seen.contains(&id) {
                seen.push(id);
            }
        }
        seen.truncate(SEEN_SET_CAP);
        return (Vec::new(), serde_json::json!({ "seen": seen }));
    };

    // The seen-memory once the ids in `acknowledged` have fired: page
    // ids that are stored-or-acknowledged (page order), then stored
    // ids off the page, truncated.
    let seen_with = |acknowledged: &[String]| -> Value {
        let mut seen: Vec<String> = Vec::with_capacity(current_ids.len());
        for id in &current_ids {
            let known = stored.iter().any(|s| s == id) || acknowledged.iter().any(|a| a == id);
            if known && !seen.iter().any(|s| s == id) {
                seen.push(id.clone());
            }
        }
        for id in &stored {
            if !seen.iter().any(|s| s == id) {
                seen.push(id.clone());
            }
        }
        seen.truncate(SEEN_SET_CAP);
        serde_json::json!({ "seen": seen })
    };

    let mut fires = Vec::new();
    let mut acknowledged: Vec<String> = Vec::new();
    for (k, item) in keyed.iter() {
        let id = id_of(k);
        if stored.iter().any(|s| s == &id) || acknowledged.iter().any(|a| a == &id) {
            continue;
        }
        acknowledged.push(id);
        fires.push((serde_json::json!({ "item": item }), seen_with(&acknowledged)));
    }
    // Nothing new: keep the stored state verbatim (no write for pure
    // page reordering).
    let idle = state.cloned().unwrap_or_else(|| serde_json::json!({ "seen": stored }));
    (fires, idle)
}

/// Order two cursor values: numeric when both sides are (or parse as)
/// numbers, else by their string form. Total, so `max_by` is safe.
fn cursor_cmp(a: &Value, b: &Value) -> std::cmp::Ordering {
    let num = |v: &Value| -> Option<f64> {
        match v {
            Value::Number(n) => n.as_f64(),
            Value::String(s) => s.parse().ok(),
            _ => None,
        }
    };
    match (num(a), num(b)) {
        (Some(x), Some(y)) => x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal),
        _ => {
            let s = |v: &Value| match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            s(a).cmp(&s(b))
        }
    }
}

inventory::submit!(&PollEndpointHandler as &dyn KindHandler);

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use weft_core::signal::PollDelta;

    fn positional() -> PollDelta {
        PollDelta {
            items: "values".into(),
            cursor_field: None,
            mode: Default::default(),
            cursor_param: None,
        }
    }

    fn by_field(f: &str) -> PollDelta {
        PollDelta {
            items: "items".into(),
            cursor_field: Some(f.into()),
            mode: Default::default(),
            cursor_param: None,
        }
    }

    fn by_set(f: &str) -> PollDelta {
        PollDelta {
            items: "items".into(),
            cursor_field: Some(f.into()),
            mode: weft_core::signal::DeltaMode::Set,
            cursor_param: None,
        }
    }

    /// First poll primes silently; growth fires exactly the tail,
    /// each fire acknowledging exactly its own prefix (so a mid-batch
    /// enqueue failure re-offers only the unfired tail); a shrunken
    /// array re-baselines without firing.
    #[test]
    fn positional_delta_fires_the_tail_only() {
        let d = positional();
        let (fires, s1) = delta_advance(&d, &json!({ "values": [1, 2] }), None).unwrap();
        assert!(fires.is_empty());
        let s1 = s1.unwrap();
        assert_eq!(s1, json!({ "count": 2 }));

        let (fires, s2) = delta_advance(&d, &json!({ "values": [1, 2, 3, 4] }), Some(&s1)).unwrap();
        assert_eq!(
            fires,
            vec![
                (json!({ "item": 3, "index": 2 }), json!({ "count": 3 })),
                (json!({ "item": 4, "index": 3 }), json!({ "count": 4 })),
            ]
        );
        let s2 = s2.unwrap();
        assert_eq!(s2, json!({ "count": 4 }), "idle state = whole page acknowledged");

        let (fires, s3) = delta_advance(&d, &json!({ "values": [1] }), Some(&s2)).unwrap();
        assert!(fires.is_empty());
        assert_eq!(s3.unwrap(), json!({ "count": 1 }));
    }

    /// Field mode: strictly-above the stored cursor fires; the cursor
    /// is the max seen; numeric strings compare numerically ("9" <
    /// "10"); a missing field is a loud error.
    #[test]
    fn field_delta_fires_above_the_cursor() {
        let d = by_field("id");
        let two = json!({ "items": [ { "id": "9" }, { "id": "10" } ] });
        let (fires, s1) = delta_advance(&d, &two, None).unwrap();
        assert!(fires.is_empty());
        let s1 = s1.unwrap();
        assert_eq!(s1, json!({ "cursor": "10" }));

        // A priming poll over an EMPTY page persists NOTHING: there is
        // no cursor to pin, and staying unprimed means the next
        // non-empty page primes for real.
        let (fires, empty_prime) = delta_advance(&d, &json!({ "items": [] }), None).unwrap();
        assert!(fires.is_empty());
        assert_eq!(empty_prime, None, "empty priming page stays unprimed");

        let three = json!({ "items": [ { "id": "10" }, { "id": "11" }, { "id": "9" } ] });
        let (fires, s2) = delta_advance(&d, &three, Some(&s1)).unwrap();
        assert_eq!(
            fires,
            vec![(json!({ "item": { "id": "11" } }), json!({ "cursor": "11" }))]
        );
        assert_eq!(
            s2.unwrap(),
            json!({ "cursor": "10" }),
            "idle state keeps the stored cursor"
        );
        let s2 = fires_final_state(&d, &three, &s1);

        // An empty poll keeps the cursor.
        let (fires, s3) = delta_advance(&d, &json!({ "items": [] }), Some(&s2)).unwrap();
        assert!(fires.is_empty());
        let s3 = s3.unwrap();
        assert_eq!(s3, json!({ "cursor": "11" }));

        let err = delta_advance(&d, &json!({ "items": [ { "other": 1 } ] }), Some(&s3))
            .unwrap_err()
            .to_string();
        assert!(err.contains("cursor_field"), "{err}");
    }

    /// The state after firing every item of a page (what the loop
    /// persists when the whole batch enqueues).
    fn fires_final_state(d: &PollDelta, page: &serde_json::Value, state: &serde_json::Value) -> serde_json::Value {
        let (fires, idle) = delta_advance(d, page, Some(state)).unwrap();
        fires.into_iter().last().map(|(_, s)| s).or(idle).expect("a primed poll persists")
    }

    /// Multiple new items are offered ASCENDING by cursor, each
    /// acknowledging its own prefix: a failure after the first fire
    /// persists the first item's cursor, never a later one's.
    #[test]
    fn field_delta_acknowledges_ascending_prefixes() {
        let d = by_field("id");
        let stored = json!({ "cursor": 5 });
        let page = json!({ "items": [ { "id": 8 }, { "id": 6 }, { "id": 7 } ] });
        let (fires, _) = delta_advance(&d, &page, Some(&stored)).unwrap();
        let states: Vec<_> = fires.iter().map(|(_, s)| s.clone()).collect();
        assert_eq!(
            states,
            vec![json!({ "cursor": 6 }), json!({ "cursor": 7 }), json!({ "cursor": 8 })],
            "ascending order, per-item acknowledge"
        );
    }

    /// Set mode: unseen ids fire regardless of order (a newest-first
    /// mailbox listing); seen ids never re-fire; priming is silent.
    #[test]
    fn set_delta_fires_unseen_ids_only() {
        let d = by_set("id");
        let first = json!({ "items": [ { "id": "b" }, { "id": "a" } ] });
        let (fires, s1) = delta_advance(&d, &first, None).unwrap();
        assert!(fires.is_empty());
        let s1 = s1.unwrap();
        assert_eq!(s1, json!({ "seen": ["b", "a"] }));

        // A NEW id lands at the top (newest-first feed); the old ones
        // are still on the page and must not re-fire.
        let second = json!({ "items": [ { "id": "c" }, { "id": "b" }, { "id": "a" } ] });
        let (fires, idle) = delta_advance(&d, &second, Some(&s1)).unwrap();
        assert_eq!(fires.len(), 1);
        assert_eq!(fires[0].0, json!({ "item": { "id": "c" } }));
        assert_eq!(fires[0].1, json!({ "seen": ["c", "b", "a"] }));
        assert_eq!(idle.unwrap(), s1, "idle keeps the stored state verbatim");
        let s2 = fires[0].1.clone();

        // An id dropping off the page stays remembered: it must not
        // re-fire if it reappears within the memory window. And a
        // page that merely reordered writes NOTHING (idle == stored).
        let third = json!({ "items": [ { "id": "a" }, { "id": "c" } ] });
        let (fires, idle) = delta_advance(&d, &third, Some(&s2)).unwrap();
        assert!(fires.is_empty());
        assert_eq!(idle.unwrap(), s2, "pure reordering never churns a durable write");
    }

    /// The seen memory is RECENCY-ordered: acknowledged page ids
    /// first, then the stored ids, so the cap always evicts
    /// genuinely-gone ids and a persistently-visible item can never
    /// age out and re-fire.
    #[test]
    fn set_memory_keeps_visible_ids_ahead_of_the_cap() {
        let d = by_set("id");
        let stored = json!({ "seen": ["old1", "pinned", "old2"] });
        let page = json!({ "items": [ { "id": "pinned" }, { "id": "new" } ] });
        let (fires, _) = delta_advance(&d, &page, Some(&stored)).unwrap();
        assert_eq!(fires.len(), 1);
        assert_eq!(fires[0].0, json!({ "item": { "id": "new" } }));
        // Visible ids lead; off-page ids trail in their stored order.
        assert_eq!(fires[0].1, json!({ "seen": ["pinned", "new", "old1", "old2"] }));
    }

    /// The acknowledged-cursor parameter rides the URL only once a
    /// cursor exists, added to the configured offset; priming and
    /// param-less specs poll the bare URL.
    #[test]
    fn poll_url_appends_the_cursor_param() {
        let delta: weft_core::signal::PollDelta = serde_json::from_value(json!({
            "items": "result",
            "cursor_field": "update_id",
            "cursor_param": { "name": "offset", "offset": 1 }
        }))
        .unwrap();
        let url = "https://api.example/getUpdates?limit=100";
        assert_eq!(poll_url(url, Some(&delta), None).unwrap(), url, "priming poll is bare");

        // A declared prime value rides the UNPRIMED poll (Telegram's
        // offset=-1 drain idiom); the cursor takes over once stored.
        let priming: weft_core::signal::PollDelta = serde_json::from_value(json!({
            "items": "result",
            "cursor_field": "update_id",
            "cursor_param": { "name": "offset", "offset": 1, "prime": -1 }
        }))
        .unwrap();
        assert_eq!(
            poll_url(url, Some(&priming), None).unwrap(),
            "https://api.example/getUpdates?limit=100&offset=-1"
        );
        assert_eq!(
            poll_url(url, Some(&priming), Some(&json!({ "cursor": 41 }))).unwrap(),
            "https://api.example/getUpdates?limit=100&offset=42"
        );
        assert_eq!(
            poll_url(url, Some(&delta), Some(&json!({ "cursor": 41 }))).unwrap(),
            "https://api.example/getUpdates?limit=100&offset=42"
        );
        assert_eq!(
            poll_url("https://api.example/u", Some(&delta), Some(&json!({ "cursor": "7" })))
                .unwrap(),
            "https://api.example/u?offset=8",
            "string cursors parse; bare URLs get '?'"
        );
        assert_eq!(poll_url(url, None, Some(&json!({ "cursor": 41 }))).unwrap(), url);

        // A PRIMED state whose cursor is not numeric errors instead of
        // silently re-priming (on a draining feed the prime value
        // means "discard everything queued").
        assert!(poll_url(url, Some(&priming), Some(&json!({ "cursor": "abc" }))).is_err());
        assert!(poll_url(url, Some(&priming), Some(&json!({}))).is_err());

        // An extreme cursor saturates instead of wrapping negative (a
        // wrapped value would read as a drain instruction).
        assert_eq!(
            poll_url("https://x/u", Some(&delta), Some(&json!({ "cursor": i64::MAX }))).unwrap(),
            format!("https://x/u?offset={}", i64::MAX),
        );
    }


    /// The items path itself failing is loud, not a silent no-fire.
    #[test]
    fn missing_items_path_is_an_error() {
        let err = delta_advance(&positional(), &json!({ "rows": [] }), None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("values"), "{err}");
    }

    /// An RSS body parses into the delta pipeline's item JSON, ids
    /// falling back to the link when the feed mints no GUID; garbage
    /// is a loud parse error.
    #[test]
    fn feed_bodies_become_item_json() {
        let rss = r#"<?xml version="1.0"?>
            <rss version="2.0"><channel><title>Blog</title>
              <item><title>Post one</title><link>https://b.example/1</link>
                <guid>tag:1</guid><description>first</description></item>
              <item><title>Post two</title><link>https://b.example/2</link></item>
            </channel></rss>"#;
        let parsed = feed_items(rss).unwrap();
        let items = parsed["items"].as_array().unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0]["id"], json!("tag:1"));
        assert_eq!(items[0]["title"], json!("Post one"));
        assert_eq!(items[0]["summary"], json!("first"));
        let synthesized = items[1]["id"].as_str().unwrap();
        assert!(
            !synthesized.is_empty() && synthesized != "tag:1",
            "a guid-less entry gets a synthesized stable id: {synthesized}"
        );
        assert_eq!(
            feed_items(rss).unwrap()["items"][1]["id"].as_str().unwrap(),
            synthesized,
            "the synthesized id is stable across polls"
        );
        assert!(feed_items("not a feed").is_err());
    }

    /// Everyone asking within one interval names the same moment, so a
    /// registration and the rehydrate after it set one wake; the next
    /// point is always strictly ahead, so a late wake skips what it
    /// missed.
    #[test]
    fn the_poll_grid_names_one_moment_per_interval() {
        assert_eq!(next_grid_point(0, 30), 30_000);
        assert_eq!(next_grid_point(29_999, 30), 30_000);
        assert_eq!(next_grid_point(30_000, 30), 60_000, "a point already reached is behind");
        assert_eq!(next_grid_point(1_000, 30), next_grid_point(12_345, 30));
        assert_eq!(next_grid_point(95_000, 30), 120_000, "late by two points: skip to the next");
    }
}
