//! Provider-side event subscriptions: the rows behind
//! `route_by: subscription` services, and the declared subscribe /
//! renew / unsubscribe calls that keep them alive.
//!
//! One subscription serves one registered signal. Weft mints the id
//! and the token, tells the provider both plus the receiver address
//! through the service's declared subscribe call, and records what
//! the provider answered. Renewal is a FRESH subscription (new id,
//! new token) started before the old one dies, with the old one
//! stopped only after the new one lives: an overlap window where the
//! provider may deliver on both is normal and both route correctly.
//!
//! The outbound calls run here (the store side) like every other
//! declared call against a tenant-influenced URL (test calls, token
//! exchanges): the broker's egress lock is the containment.

use std::collections::BTreeMap;

use serde_json::Value;
use sqlx::PgPool;

use weft_core::access::client::run_connect_call as core_run_connect_call;
use weft_core::access::events::{
    SubscriptionCalls, EXPIRES_AT, MINTED_ID, MINTED_TOKEN, MINTED_VALUE_NAMES, RECEIVER_URL,
};
use weft_core::access::spec::ConnectCall;

use crate::flows::apply_captures;
use crate::resolve::resolve_event_source;
use crate::{values_of, AccessError};

/// One stored subscription, as the receiver and the renewal loop
/// read it.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Subscription {
    /// The id weft minted and the provider echoes on every push.
    pub id: String,
    pub tenant_id: String,
    pub service: String,
    /// The event topic this subscription is on.
    pub topic: String,
    pub access_id: uuid::Uuid,
    /// The registered signal this subscription feeds.
    pub signal_token: String,
    /// The secret weft minted; what a token-echo verification
    /// compares against.
    pub token: String,
    /// What the provider's subscribe answer captured.
    pub captures: BTreeMap<String, String>,
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// What `ensure_subscription` needs: which signal, through which
/// connection, plus the node-supplied values the service's subscribe
/// call interpolates (the file to watch, the mailbox's label filter).
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct EnsureSubscription {
    pub tenant: String,
    pub service: String,
    /// Which of the service's event topics to subscribe on.
    pub topic: String,
    pub access_id: uuid::Uuid,
    pub signal_token: String,
    /// Values the subscribe call's templates name beyond the
    /// connection's stored values and the minted ones (from the
    /// subscription's `params`).
    #[serde(default)]
    pub params: BTreeMap<String, String>,
    /// The public address the provider should post to, resolved by
    /// the caller from its deployment configuration. `None` = this
    /// weft has no address the internet can reach; a topic that must
    /// TELL the provider where to send then refuses with the teaching
    /// error, while a topic the provider pushes to an
    /// operator-configured address needs nothing from us.
    pub receiver_url: Option<String>,
}

/// The answer: when the provider will stop sending (`None` = never),
/// so the serving loop knows when to come back.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct EnsuredSubscription {
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Make sure a live provider subscription serves this signal:
/// subscribe when none exists, renew (fresh id + token, then stop the
/// old channel) when the recorded one is inside its renewal margin,
/// and answer the current expiry either way. A service whose recipe
/// declares no subscribe call needs nothing and answers `None`.
///
/// Idempotent per (signal, healthy subscription): the serving loop
/// calls it at spawn and again at each renewal wake without tracking
/// which case it is in.
pub async fn ensure_subscription(
    pool: &PgPool,
    req: &EnsureSubscription,
) -> anyhow::Result<EnsuredSubscription> {
    let source = resolve_event_source(pool, &req.tenant, req.access_id, &req.service, &[]).await?;
    let Some(topic) = source.events.get(&req.topic) else {
        return Err(AccessError::Invalid(format!(
            "'{}' declares no event topic named '{}'",
            req.service, req.topic
        ))
        .into());
    };
    let Some(webhook) = &topic.webhook else {
        return Err(AccessError::Invalid(format!(
            "'{}.{}' declares no webhook transport, so there is nothing to subscribe",
            req.service, req.topic
        ))
        .into());
    };
    let Some(calls) = &webhook.subscribe else {
        // The provider pushes without being asked (an app-level
        // events URL); nothing to keep alive.
        return Ok(EnsuredSubscription { expires_at: None });
    };

    let Some(receiver_url) = req.receiver_url.as_deref() else {
        return Err(AccessError::Invalid(no_public_url_error(&req.service)).into());
    };

    // Every row already serving this signal, newest first. The newest
    // decides whether anything needs doing; the REST are strays a
    // crash between insert and cleanup left behind, and the renewal
    // below stops all of them, not just the one it replaces.
    let existing = list_for_signal(pool, &req.tenant, &req.signal_token).await?;
    if let Some(sub) = existing.first() {
        if !needs_renewal(sub.expires_at, calls.renew_margin_secs, chrono::Utc::now()) {
            return Ok(EnsuredSubscription { expires_at: sub.expires_at });
        }
    }

    // Subscribe fresh: a new id + token every time, because a renewal
    // is a NEW provider-side channel (the surveyed providers have no
    // extend verb), and reusing the token would make the old channel
    // and the new one indistinguishable.
    let minted_id = uuid::Uuid::new_v4().to_string();
    let minted_token = mint_token();
    let mut values = call_values(&source.values, &source.recipe_values, &req.params)?;
    values.insert(MINTED_ID.to_string(), minted_id.clone());
    values.insert(MINTED_TOKEN.to_string(), minted_token.clone());
    values.insert(RECEIVER_URL.to_string(), receiver_url.to_string());

    let resp = run_connect_call(&calls.subscribe, &values).await?;
    apply_captures(&calls.subscribe.captures, &resp, &mut values)?;
    let expires_at = match calls.renew_margin_secs {
        None => None,
        Some(_) => Some(parse_expiry(values.get(EXPIRES_AT).map(String::as_str).ok_or_else(
            || {
                AccessError::Invalid(format!(
                    "the '{}' subscribe answered without the expiry its recipe captures",
                    req.service
                ))
            },
        )?)?),
    };

    // Record the new channel FIRST, so a crash between here and the
    // old channel's stop leaves both routable (the overlap window)
    // rather than a live provider channel no row knows about.
    let captures: BTreeMap<String, String> = calls
        .subscribe
        .captures
        .iter()
        .filter_map(|c| values.get(&c.name).map(|v| (c.name.clone(), v.clone())))
        .collect();
    sqlx::query(
        "INSERT INTO signal_subscription
           (id, tenant_id, service, topic, access_id, signal_token, token_sealed,
            captures_json, expires_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
    )
    .bind(&minted_id)
    .bind(&req.tenant)
    .bind(&req.service)
    .bind(&req.topic)
    .bind(req.access_id)
    .bind(&req.signal_token)
    .bind(crate::seal_str(&minted_token))
    .bind(serde_json::to_value(&captures)?)
    .bind(expires_at)
    .execute(pool)
    .await?;

    // Now stop EVERY channel this one replaces: the newest, and any
    // stray an earlier crash between insert and cleanup left behind.
    // Best-effort by design: the old channels expire on their own
    // shortly (that is why we are renewing), so a provider refusal
    // here must not fail a renewal that already succeeded.
    for old in existing {
        if let Err(e) = stop_subscription(pool, &source, calls, &old, &req.params).await {
            tracing::warn!(
                target: "weft_access_store::subscriptions",
                service = %req.service, old_id = %old.id, error = %format!("{e:#}"),
                "stopping the replaced subscription failed; it will lapse on its own expiry"
            );
            // The row still dies: the provider channel outlives it
            // briefly, and a push on it simply finds no row (dropped,
            // logged), which is the overlap window's normal shape.
            delete_row(pool, &old.id).await?;
        }
    }

    Ok(EnsuredSubscription { expires_at })
}

/// Stop and forget every subscription serving `signal_token`: the
/// signal is unregistering, so pushes for it must stop at the
/// provider, not just be dropped here. A provider refusal on the stop
/// call still deletes the row (the channel lapses on its own expiry;
/// keeping an untracked row would be junk nobody can act on) and is
/// logged loudly.
pub async fn drop_subscriptions_for_signal(
    pool: &PgPool,
    tenant: &str,
    signal_token: &str,
) -> anyhow::Result<u64> {
    let rows: Vec<Subscription> = list_for_signal(pool, tenant, signal_token).await?;
    let count = rows.len() as u64;
    for sub in rows {
        let stopped = async {
            let source =
                resolve_event_source(pool, &sub.tenant_id, sub.access_id, &sub.service, &[])
                    .await?;
            let calls = source
                .events
                .get(&sub.topic)
                .and_then(|t| t.webhook.as_ref())
                .and_then(|w| w.subscribe.as_ref())
                .ok_or_else(|| {
                    anyhow::anyhow!("the service no longer declares subscribe calls")
                })?
                .clone();
            stop_subscription(pool, &source, &calls, &sub, &BTreeMap::new()).await
        }
        .await;
        if let Err(e) = stopped {
            tracing::warn!(
                target: "weft_access_store::subscriptions",
                service = %sub.service, id = %sub.id, error = %format!("{e:#}"),
                "provider unsubscribe failed while unregistering; the channel will lapse \
                 on its own expiry"
            );
            delete_row(pool, &sub.id).await?;
        }
    }
    Ok(count)
}

/// The subscription a push names, for `route_by: subscription`
/// routing. Keyed by the minted id (an unguessable UUID weft minted;
/// the token check on top of it is the verification) AND the
/// endpoint's service + topic, so a push replayed at another
/// service's or topic's endpoint finds nothing and drops.
pub async fn subscription_by_id(
    pool: &PgPool,
    minted_id: &str,
    service: &str,
    topic: &str,
) -> anyhow::Result<Option<Subscription>> {
    let row: Option<SubscriptionRow> = sqlx::query_as(
        "SELECT id, tenant_id, service, topic, access_id, signal_token, token_sealed, captures_json,
                expires_at
         FROM signal_subscription WHERE id = $1 AND service = $2 AND topic = $3",
    )
    .bind(minted_id)
    .bind(service)
    .bind(topic)
    .fetch_optional(pool)
    .await?;
    row.map(row_to_subscription).transpose()
}

/// Run the provider's unsubscribe call (when one is declared) and
/// delete the row. The call's templates read the connection's values
/// plus everything the subscribe captured (the resource id the stop
/// call needs).
async fn stop_subscription(
    pool: &PgPool,
    source: &crate::resolve::ResolvedEventSource,
    calls: &SubscriptionCalls,
    sub: &Subscription,
    params: &BTreeMap<String, String>,
) -> anyhow::Result<()> {
    if let Some(unsub) = &calls.unsubscribe {
        let mut values = call_values(&source.values, &source.recipe_values, params)?;
        values.insert(MINTED_ID.to_string(), sub.id.clone());
        values.insert(MINTED_TOKEN.to_string(), sub.token.clone());
        for (k, v) in &sub.captures {
            values.insert(k.clone(), v.clone());
        }
        run_connect_call(unsub, &values).await?;
    }
    delete_row(pool, &sub.id).await
}

async fn delete_row(pool: &PgPool, id: &str) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM signal_subscription WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Every subscription serving `signal_token`, tenant-walled like
/// every read, newest first (the first row is the live channel; any
/// others are strays a crash left behind). The ONE reader for both
/// the renewal path and the drop path.
async fn list_for_signal(
    pool: &PgPool,
    tenant: &str,
    signal_token: &str,
) -> anyhow::Result<Vec<Subscription>> {
    let rows: Vec<SubscriptionRow> = sqlx::query_as(
        "SELECT id, tenant_id, service, topic, access_id, signal_token, token_sealed, captures_json,
                expires_at
         FROM signal_subscription WHERE tenant_id = $1 AND signal_token = $2
         ORDER BY created_at DESC",
    )
    .bind(tenant)
    .bind(signal_token)
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(row_to_subscription).collect()
}

/// The raw row tuple; one alias so the three readers cannot drift.
type SubscriptionRow = (
    String,
    String,
    String,
    String,
    uuid::Uuid,
    String,
    String,
    Value,
    Option<chrono::DateTime<chrono::Utc>>,
);

fn row_to_subscription(
    (id, tenant_id, service, topic, access_id, signal_token, token_sealed, captures_json, expires_at):
        SubscriptionRow,
) -> anyhow::Result<Subscription> {
    Ok(Subscription {
        id,
        tenant_id,
        service,
        topic,
        access_id,
        signal_token,
        token: crate::open_str(&token_sealed)?,
        captures: values_of(&captures_json),
        expires_at,
    })
}

/// The activation error a dial-in trigger meets on a weft the
/// internet cannot reach: states the need, the risk-accepting fix,
/// and the door to the full explanation. The doc path is stable on
/// purpose; the editor renders it as a link.
// SYNC: no_public_url_error <-> docs/event-triggers.md (the section
//       '"This trigger needs your weft to be reachable..."')
pub fn no_public_url_error(service: &str) -> String {
    format!(
        "this trigger needs '{service}' to deliver events TO your weft, which requires \
         your weft to be reachable from the internet, and it is not. If you accept \
         making its public trigger surface reachable, rerun `./setup.sh --public-url` \
         and re-activate; or use a connection that supports dialing out (your own \
         provider app), where the service offers one. See docs/event-triggers.md"
    )
}

/// Should this subscription be renewed now? Pure. A subscription with
/// no recorded expiry never renews; one with an expiry renews once
/// now is inside the margin before it (or past it entirely: a lapsed
/// one is re-made the same way).
pub fn needs_renewal(
    expires_at: Option<chrono::DateTime<chrono::Utc>>,
    renew_margin_secs: Option<u64>,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    match (expires_at, renew_margin_secs) {
        (Some(at), Some(margin)) => now + chrono::Duration::seconds(margin as i64) >= at,
        // An expiry with no declared margin cannot happen past spec
        // validation; renewing is the safe read if it somehow does.
        (Some(at), None) => now >= at,
        (None, _) => false,
    }
}

/// One declared authenticated call, executed through the shared core
/// executor, with its failure carried as the store's caller-fixable
/// error class (the provider's refusal is the caller's to read).
pub async fn run_connect_call(
    call: &ConnectCall,
    values: &BTreeMap<String, String>,
) -> anyhow::Result<Value> {
    core_run_connect_call(call, values).await.map_err(|e| AccessError::Invalid(e).into())
}

/// The value map a recipe call resolves against: the caller's
/// per-subscription params, the recipe's extra stored values, and the
/// connection's auth-step values, with params LOWEST. A param whose
/// name collides with a stored value or a weft-minted name is refused
/// loudly: node-supplied params fill the call's own blanks (the file
/// to watch, a label filter) and may never replace what the
/// connection or weft supplies.
fn call_values(
    auth_values: &BTreeMap<String, String>,
    recipe_values: &BTreeMap<String, String>,
    params: &BTreeMap<String, String>,
) -> anyhow::Result<BTreeMap<String, String>> {
    for name in params.keys() {
        if auth_values.contains_key(name)
            || recipe_values.contains_key(name)
            || MINTED_VALUE_NAMES.contains(&name.as_str())
        {
            return Err(AccessError::Invalid(format!(
                "the trigger's params may not replace the connection's stored value '{name}'"
            ))
            .into());
        }
    }
    let mut values = params.clone();
    for (k, v) in recipe_values {
        values.insert(k.clone(), v.clone());
    }
    for (k, v) in auth_values {
        values.insert(k.clone(), v.clone());
    }
    Ok(values)
}

/// An unguessable per-subscription secret.
fn mint_token() -> String {
    weft_core::access::hex_of(&rand::random::<[u8; 32]>())
}

/// Parse the expiry a subscribe answer captured: RFC3339, epoch
/// seconds, or epoch milliseconds (the surveyed providers answer all
/// three; millis are told apart by magnitude).
fn parse_expiry(raw: &str) -> anyhow::Result<chrono::DateTime<chrono::Utc>> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(raw) {
        return Ok(dt.with_timezone(&chrono::Utc));
    }
    let n: i64 = raw.trim().parse().map_err(|_| {
        AccessError::Invalid(format!(
            "the subscribe answered an expiry '{raw}' that is neither a date nor a number"
        ))
    })?;
    // Epoch seconds run out of 10 digits in 2286; every current
    // millisecond epoch has 13. The magnitude split is unambiguous
    // for any date this code will see.
    let secs = if n > 100_000_000_000 { n / 1000 } else { n };
    chrono::DateTime::from_timestamp(secs, 0)
        .ok_or_else(|| AccessError::Invalid(format!("unrepresentable expiry '{raw}'")).into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renewal_fires_inside_the_margin_and_not_before() {
        let now = chrono::Utc::now();
        let in_two_hours = Some(now + chrono::Duration::hours(2));
        assert!(!needs_renewal(in_two_hours, Some(3600), now), "well before the margin");
        assert!(needs_renewal(in_two_hours, Some(3 * 3600), now), "inside the margin");
        assert!(
            needs_renewal(Some(now - chrono::Duration::hours(1)), Some(3600), now),
            "a lapsed subscription is re-made"
        );
        assert!(!needs_renewal(None, Some(3600), now), "no expiry, nothing to renew");
    }

    #[test]
    fn expiries_parse_in_all_three_provider_shapes() {
        let rfc = parse_expiry("2030-01-02T03:04:05Z").unwrap();
        assert_eq!(rfc.timestamp(), 1893553445);
        assert_eq!(parse_expiry("1893553445").unwrap().timestamp(), 1893553445);
        // Google answers epoch milliseconds as a string.
        assert_eq!(parse_expiry("1893553445000").unwrap().timestamp(), 1893553445);
        assert!(parse_expiry("soon").is_err());
    }

    /// Params fill the call's own blanks and never replace what the
    /// connection or weft supplies: a collision with a stored value
    /// or a minted name is refused naming the param; a fresh name
    /// merges beneath the stored values.
    #[test]
    fn params_never_replace_stored_or_minted_values() {
        let auth = BTreeMap::from([("token".to_string(), "t1".to_string())]);
        let recipe = BTreeMap::from([("app_token".to_string(), "xapp".to_string())]);

        let fresh = BTreeMap::from([("target".to_string(), "file-9".to_string())]);
        let v = call_values(&auth, &recipe, &fresh).unwrap();
        assert_eq!(v["token"], "t1");
        assert_eq!(v["app_token"], "xapp");
        assert_eq!(v["target"], "file-9");

        for stolen in ["token", "app_token", MINTED_TOKEN] {
            let params = BTreeMap::from([(stolen.to_string(), "override".to_string())]);
            let err = call_values(&auth, &recipe, &params).unwrap_err().to_string();
            assert!(err.contains(stolen), "{err}");
            assert!(err.contains("may not replace"), "{err}");
        }
    }
}
