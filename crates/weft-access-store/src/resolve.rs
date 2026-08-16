//! Resolution side: hand a WORKER the short-lived pieces it needs
//! (lazy refresh, single-flight), and run the editor's declarative
//! resource lookups through a stored access.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;

use sha2::Digest;

use weft_core::access::client::{authed_client, base_client, resolve_steps, run_connect_call};
use weft_core::access::events::EventsSpec;
use weft_core::access::spec::{
    lookup_path, AccessSpec, Acquisition, AppRegistration, Door, OAuthGrant,
};
use weft_core::node::Lookup;

use crate::flows::{apply_captures, expires_at_of, token_request};
use crate::{scopes_of, spec_of, values_of, AccessError};

pub use weft_core::access::events::ResolvedEventSource;

/// Refresh when the token dies within this margin: a token that
/// expires mid-call is a failed call, so it is refreshed a little
/// early instead.
const REFRESH_MARGIN: chrono::Duration = chrono::Duration::seconds(60);

/// What a resolution hands out: everything the connection stores
/// except the store's own keep-alive material, the auth steps, whose
/// credential they are, and display context. For an `Ours`-owned
/// connection the values are EMPTY here: the caller (the broker
/// handler) fills the auth steps' value names from the runtime's
/// credential source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedAccess {
    pub values: BTreeMap<String, String>,
    pub auth: Vec<weft_core::access::spec::AuthStep>,
    pub identity: Option<String>,
    pub service: String,
    pub owner: weft_core::CredentialOwner,
    /// The PUBLIC client id of the app the connection was made
    /// through, when one was (a pasted key has no app). Provider
    /// chooser widgets need it (Google's picker derives its required
    /// app id from it); it is never a secret.
    pub app_client_id: Option<String>,
}

/// The permission drift backstop, pure: a VERIFIED connection short of
/// a required permission is refused; a claimed/unknown set passes
/// (nobody actually knows, and refusing would block every pasted key
/// on a service that reports nothing). `Some(missing)` = refuse,
/// naming the shortfall.
pub fn permission_shortfall<'r>(
    verified: bool,
    granted: &[String],
    required: &'r [String],
) -> Option<&'r str> {
    if !verified {
        return None;
    }
    required.iter().find(|s| !granted.contains(s)).map(String::as_str)
}

/// The own-account gate, pure: an own-account-only capability (it
/// creates or reads things INSIDE the credential's account: minted
/// voices, configured agents) can never be served by a runtime-supplied
/// credential; the result would land in the runtime's account. Checked
/// against the spec's catalogue, independent of verification: this is
/// a policy of the capability, not a provider-reported scope.
/// `Some(reason)` = refuse.
pub fn own_only_refusal(
    owner: weft_core::CredentialOwner,
    spec: &AccessSpec,
    required: &[String],
) -> Option<String> {
    if owner != weft_core::CredentialOwner::Ours {
        return None;
    }
    let p = spec.permissions.iter().find(|p| p.own_only && required.contains(&p.id))?;
    let link = p
        .guide
        .as_ref()
        .and_then(|g| g.link.as_deref())
        .map(|l| format!(" (set-up guide: {l})"))
        .unwrap_or_default();
    Some(format!(
        "'{}' only works on your own {} account (the shared credential's account would \
         hold the result); connect your own on the access node{link}",
        p.label, spec.service,
    ))
}

/// The consumer-declared VALUE check, pure: the first required value
/// the connection does not store, or `None`. No unknown case (a value
/// is stored or it is not), so every shortfall refuses.
pub fn value_shortfall<'r>(
    stored: &BTreeMap<String, String>,
    required: &'r [String],
) -> Option<&'r str> {
    required
        .iter()
        .find(|name| stored.get(*name).is_none_or(|v| v.trim().is_empty()))
        .map(String::as_str)
}

/// One connection row, read through the wall and refreshed: what
/// every resolution starts from. Private on purpose; the public
/// surfaces below decide which slice of it leaves.
struct WalledGrant {
    service: String,
    spec: AccessSpec,
    registration: Option<AppRegistration>,
    /// The FULL stored value map (fresh: the lazy refresh ran).
    /// Empty for a runtime-supplied credential.
    values: BTreeMap<String, String>,
    identity: Option<String>,
    owner: weft_core::CredentialOwner,
    door: Door,
    provider_account: Option<String>,
}

/// Whether a walled read runs the lazy refresh. A `Stored` read is a
/// pure read (no row lock, no provider call): what an editor lookup
/// wants, where holding the row's write lock across a provider's HTTP
/// round-trip would let a dropdown click stall running work on the
/// same connection.
#[derive(Clone, Copy, PartialEq)]
enum Freshness {
    Refreshed,
    Stored,
}

/// Read one grant row for a resolution: tenant wall (missing and
/// cross-tenant answer identically, so the wall leaks no existence),
/// service check, required-permission drift backstop, sealed open,
/// spec parse, and (for a `Refreshed` read) the lazy single-flight
/// refresh with rotated-token write-back. Every read that opens a
/// row's values goes through here, so none of them can fork the wall.
async fn read_walled_grant(
    pool: &PgPool,
    tenant: &str,
    access_id: uuid::Uuid,
    service: &str,
    required_permissions: &[String],
    freshness: Freshness,
) -> anyhow::Result<WalledGrant> {
    // For a Refreshed read the row lock IS the single-flight: parallel
    // resolutions of one grant queue here, and whoever wins refreshes
    // once; the rest see the fresh values. The refresh's HTTP
    // round-trip runs inside the transaction on purpose (that is the
    // queueing). A Stored read takes no lock: it writes nothing.
    const COLUMNS: &str = "SELECT tenant_id, service, registration_sealed, spec_json, \
                           values_sealed, granted_scopes, permissions_verified, owner, door, \
                           identity, provider_account, expires_at \
                           FROM access_grant WHERE id = $1";
    #[allow(clippy::type_complexity)]
    type Row = (
        String,
        String,
        Option<String>,
        Value,
        String,
        Value,
        bool,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<chrono::DateTime<chrono::Utc>>,
    );
    let mut tx = None;
    let row: Option<Row> = match freshness {
        Freshness::Refreshed => {
            let mut t = pool.begin().await?;
            let row = sqlx::query_as(&format!("{COLUMNS} FOR UPDATE"))
                .bind(access_id)
                .fetch_optional(&mut *t)
                .await?;
            tx = Some(t);
            row
        }
        Freshness::Stored => {
            sqlx::query_as(COLUMNS).bind(access_id).fetch_optional(pool).await?
        }
    };
    let Some((
        row_tenant,
        row_service,
        registration_sealed,
        spec_json,
        values_sealed,
        granted,
        verified,
        owner,
        door,
        identity,
        provider_account,
        expires_at,
    )) = row
    else {
        return Err(AccessError::NotFound.into());
    };
    if row_tenant != tenant {
        return Err(AccessError::NotFound.into());
    }
    if row_service != service {
        return Err(AccessError::Invalid(format!(
            "this connection is a '{row_service}' account, but the node asked for \
             '{service}'; wire the right service's access node"
        ))
        .into());
    }
    let owner = crate::owner_of(&owner)?;
    let door = crate::door_of(&door)?;
    let granted = scopes_of(&granted);
    if let Some(missing) = permission_shortfall(verified, &granted, required_permissions) {
        return Err(AccessError::NeedsReconnect {
            service: service.to_string(),
            reason: format!(
                "the node needs permission '{missing}' but the connection does not hold \
                 it; reconnect (or upgrade) the account on the access node"
            ),
        }
        .into());
    }

    let spec = spec_of(&spec_json)?;
    if let Some(reason) = own_only_refusal(owner, &spec, required_permissions) {
        return Err(AccessError::NeedsReconnect { service: service.to_string(), reason }.into());
    }
    let registration = registration_sealed
        .as_deref()
        .map(crate::open_json)
        .transpose()?
        .map(serde_json::from_value::<AppRegistration>)
        .transpose()
        .map_err(|e| anyhow::anyhow!("grant has a malformed app snapshot: {e}"))?;
    if matches!(spec.acquisition, Acquisition::Runtime {}) {
        // Nothing stored, nothing to refresh: the caller asks the
        // runtime's credential source and fills the auth values.
        if let Some(tx) = tx {
            tx.commit().await?;
        }
        return Ok(WalledGrant {
            service: row_service,
            spec,
            registration,
            values: BTreeMap::new(),
            identity,
            owner,
            door,
            provider_account,
        });
    }
    let mut values = values_of(&crate::open_json(&values_sealed)?);
    if let Some(mut tx) = tx {
        let stale = match expires_at {
            Some(at) => at < chrono::Utc::now() + REFRESH_MARGIN,
            None => false,
        };
        if stale {
            let new_expiry = refresh(&spec, registration.clone(), &mut values).await?;
            sqlx::query(
                "UPDATE access_grant
                 SET values_sealed = $2, value_names = $3, expires_at = $4, updated_at = now()
                 WHERE id = $1",
            )
            .bind(access_id)
            .bind(crate::seal_json(&serde_json::to_value(&values)?)?)
            .bind(crate::flows::value_names_of(&values))
            .bind(new_expiry)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
    }
    Ok(WalledGrant {
        service: row_service,
        spec,
        registration,
        values,
        identity,
        owner,
        door,
        provider_account,
    })
}

/// Resolve a connection for a WORKER (or for the store's own
/// lookup/test use): the walled read above, then the worker handoff
/// filter and the consumer's shortfall checks. An `Ours`-owned row
/// (runtime-supplied credential) skips refresh and hands back empty
/// values; the caller fills them.
pub async fn resolve_for_worker(
    pool: &PgPool,
    tenant: &str,
    access_id: uuid::Uuid,
    service: &str,
    required_permissions: &[String],
    required_values: &[String],
) -> anyhow::Result<ResolvedAccess> {
    let grant =
        read_walled_grant(pool, tenant, access_id, service, required_permissions, Freshness::Refreshed)
            .await?;
    worker_handoff(grant, service, required_values)
}

/// The worker slice of a walled grant: the handoff filter plus the
/// completeness and consumer-declared value checks.
fn worker_handoff(
    grant: WalledGrant,
    service: &str,
    required_values: &[String],
) -> anyhow::Result<ResolvedAccess> {
    let WalledGrant { service: row_service, spec, registration, values, identity, owner, .. } =
        grant;
    let app_client_id = registration.as_ref().map(|r| r.client_id.clone());
    if matches!(spec.acquisition, Acquisition::Runtime {}) {
        return Ok(ResolvedAccess {
            values: BTreeMap::new(),
            auth: spec.auth.clone(),
            identity,
            service: row_service,
            owner,
            app_client_id,
        });
    }

    // The handoff: everything the connection holds except the store's
    // own keep-alive material. A connection is the user's; what it
    // stores is theirs to use, whatever shape its credential takes
    // (a bearer token, a mail server's host + user + password, a
    // database's settings). Only what weft acquired to keep the
    // connection alive stays behind.
    let needed = spec.worker_value_names().map_err(AccessError::Invalid)?;
    let handoff = spec.handoff_values(&values);
    // Completeness, separately: a connection missing a value its own
    // declared sign-in interpolates cannot sign anything, so say so
    // here rather than let the provider refuse later.
    for name in &needed {
        if !handoff.contains_key(name) {
            return Err(AccessError::NeedsReconnect {
                service: service.to_string(),
                reason: format!(
                    "the connection stores no value named '{name}' (the service's auth \
                     steps need it); reconnect the account"
                ),
            }
            .into());
        }
    }
    // What the CONSUMER declared it needs, for a service whose
    // optional fields decide what a connection can do. Always a hard
    // error: unlike a permission set, a stored value is knowable.
    if let Some(missing) = value_shortfall(&handoff, required_values) {
        return Err(AccessError::NeedsReconnect {
            service: service.to_string(),
            reason: format!(
                "this node needs the connection's '{missing}', which it does not store; \
                 add it to the connection on the access node"
            ),
        }
        .into());
    }
    Ok(ResolvedAccess {
        values: handoff,
        auth: spec.auth.clone(),
        identity,
        service: row_service,
        owner,
        app_client_id,
    })
}

/// Resolve a connection for the side SERVING its event subscriptions.
/// The same walled read as a worker resolve (tenant wall, refresh,
/// all unchanged), the same handoff slice, plus the service's events
/// recipe and the extra values that recipe's own calls name.
///
/// A service with no events section resolves fine with an empty
/// recipe map: kinds that only need the connection's VALUES (a held
/// raw pipe signing in with them) resolve through here too. A
/// subscription that needs a topic the map lacks is refused by the
/// subscription's own handling, naming the topic.
pub async fn resolve_event_source(
    pool: &PgPool,
    tenant: &str,
    access_id: uuid::Uuid,
    service: &str,
    required_values: &[String],
) -> anyhow::Result<ResolvedEventSource> {
    let grant =
        read_walled_grant(pool, tenant, access_id, service, &[], Freshness::Refreshed).await?;
    // A recipe value may live on the GRANT (a pasted token) or on the
    // APP the connection was made through. The app's values join ONLY
    // for an own-door grant, where the app is the user's; a
    // shared-door grant's app material serves connecting (token
    // exchange, refresh) and never event serving.
    let mut stored = grant.values.clone();
    if grant.door == Door::Own {
        if let Some(registration) = &grant.registration {
            for (k, v) in registration.to_values() {
                stored.entry(k).or_insert(v);
            }
        }
    }
    let mut recipe_values = BTreeMap::new();
    for topic in grant.spec.events.values() {
        for name in recipe_value_names(topic).map_err(AccessError::Invalid)? {
            if let Some(v) = stored.get(&name) {
                recipe_values.insert(name, v.clone());
            }
        }
    }
    let events = grant.spec.events.clone();
    let provider_account = grant.provider_account.clone();
    let access = worker_handoff(grant, service, required_values)?;
    Ok(ResolvedEventSource {
        values: access.values,
        auth: access.auth,
        events,
        recipe_values,
        provider_account,
    })
}

/// A stable content hash of an events-recipe map: sha256 hex of its
/// canonical JSON (BTreeMap keys are ordered, so serialization is
/// deterministic). `None` for a spec with no topics. What scopes a
/// recorded recipe to the connections that carry it: a push is
/// answered by the recipe the connection itself declared.
pub fn events_recipe_hash(events: &BTreeMap<String, EventsSpec>) -> Option<String> {
    if events.is_empty() {
        return None;
    }
    let canonical = serde_json::to_string(events).expect("events recipes serialize");
    Some(weft_core::access::hex_of(&sha2::Sha256::digest(canonical.as_bytes())))
}

/// Record the events recipes a spec declares, so the public receiver
/// can verify and route that service's pushes without any grant in
/// hand. Upserted from every store flow a spec passes through (the
/// door probe runs whenever an access node renders, so the recipe is
/// on file long before the provider's first push). Keyed by content
/// hash, so a push is answered by the recipe the connection itself
/// declared and recipes recorded by different specs stand side by
/// side. A spec with no topics records nothing.
pub async fn record_events_recipes(pool: &PgPool, spec: &AccessSpec) -> anyhow::Result<()> {
    let Some(hash) = events_recipe_hash(&spec.events) else {
        return Ok(());
    };
    sqlx::query(
        "INSERT INTO service_events_recipe (service, recipe_hash, events_json, updated_at)
         VALUES ($1, $2, $3, now())
         ON CONFLICT (service, recipe_hash) DO UPDATE SET updated_at = now()",
    )
    .bind(&spec.service)
    .bind(&hash)
    .bind(serde_json::to_value(&spec.events)?)
    .execute(pool)
    .await?;
    Ok(())
}

/// One recorded events recipe of a service, with the hash that scopes
/// it to the connections carrying the same recipe.
#[derive(Debug, Clone)]
pub struct RecordedEventsRecipe {
    pub recipe_hash: String,
    pub events: BTreeMap<String, EventsSpec>,
}

/// Every recorded events recipe of a service, newest first, for the
/// receiver. Empty = this weft has never seen the service declare
/// events, so a push for it cannot be verified and is refused.
pub async fn events_recipes_of(
    pool: &PgPool,
    service: &str,
) -> anyhow::Result<Vec<RecordedEventsRecipe>> {
    let rows: Vec<(String, Value)> = sqlx::query_as(
        "SELECT recipe_hash, events_json FROM service_events_recipe
         WHERE service = $1 ORDER BY updated_at DESC",
    )
    .bind(service)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|(recipe_hash, v)| {
            let events = serde_json::from_value(v)
                .map_err(|e| anyhow::anyhow!("stored events recipe no longer parses: {e}"))?;
            Ok(RecordedEventsRecipe { recipe_hash, events })
        })
        .collect()
}

/// One connection an inbound event concerns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventTarget {
    pub id: uuid::Uuid,
    pub tenant_id: String,
}

/// The connections an inbound push feeds: every grant at `service`
/// whose captured account matches, made through the app that SIGNED
/// the push.
///
/// Deliberately tenant-blind: the push proved which provider account
/// it concerns, and that IS the routing. Two containment rules make
/// that safe, and both are here rather than in the caller so no
/// caller can forget one:
///
///  - `allowed_client_ids` pins the lookup to the operator-registered
///    receiving app(s), and for a scheme where ONE app's secret
///    verified the signature, to exactly that app. Without it, an
///    event delivered by the operator's shared app could feed a
///    subscription registered against a DIFFERENT app's connection
///    to the same workspace, and a user-registered app (rows on
///    their connections, never in the operator's file) could never
///    have enabled receiving in the first place.
///  - a row with no captured account never matches, so a connection
///    to a service that reports no events cannot be reached this way
///    at all.
///  - `recipe_hash` pins the match to connections whose snapshot
///    declares the exact recipe that verified the push: a push is
///    answered by the recipe the connection itself declared, so a
///    recipe recorded by one spec never routes another spec's
///    connections (a row with no events block carries NULL and never
///    matches).
pub async fn connections_for_event(
    pool: &PgPool,
    service: &str,
    provider_account: &str,
    allowed_client_ids: &[String],
    recipe_hash: &str,
) -> anyhow::Result<Vec<EventTarget>> {
    let rows: Vec<(uuid::Uuid, String)> = sqlx::query_as(
        "SELECT id, tenant_id FROM access_grant
         WHERE service = $1 AND provider_account = $2
           AND client_id = ANY($3) AND events_recipe_hash = $4",
    )
    .bind(service)
    .bind(provider_account)
    .bind(allowed_client_ids)
    .bind(recipe_hash)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(id, tenant_id)| EventTarget { id, tenant_id }).collect())
}

/// Every stored value name the events recipe's own calls interpolate:
/// the allowlist extension that lets a socket mint read an app-level
/// token the API auth steps never mention. Values weft MINTS per
/// subscription are excluded (they come from the subscription row,
/// not from the connection), so naming one cannot smuggle a stored
/// value out.
pub fn recipe_value_names(
    events: &weft_core::access::events::EventsSpec,
) -> Result<Vec<String>, String> {
    let mut names = Vec::new();
    let mut add = |call: &weft_core::access::spec::ConnectCall| -> Result<(), String> {
        for n in call.value_names()? {
            if !weft_core::access::events::MINTED_VALUE_NAMES.contains(&n.as_str())
                && !names.contains(&n)
            {
                names.push(n);
            }
        }
        Ok(())
    };
    if let Some(socket) = &events.socket {
        if let Some(connect) = &socket.minted.connect {
            add(connect)?;
        }
    }
    if let Some(webhook) = &events.webhook {
        if let Some(calls) = &webhook.subscribe {
            add(&calls.subscribe)?;
            if let Some(unsub) = &calls.unsubscribe {
                add(unsub)?;
            }
        }
    }
    Ok(names)
}

/// Re-acquire the short-lived pieces, per the acquisition kind. Loud
/// (with the reconnect affordance named) when the provider refuses:
/// a revoked grant is never silently recoverable.
async fn refresh(
    spec: &AccessSpec,
    registration: Option<AppRegistration>,
    values: &mut BTreeMap<String, String>,
) -> anyhow::Result<Option<chrono::DateTime<chrono::Utc>>> {
    let reconnect = |reason: String| AccessError::NeedsReconnect {
        service: spec.service.clone(),
        reason,
    };
    match &spec.acquisition {
        Acquisition::Static { .. } => Err(reconnect(
            "its pasted credential expired (a static credential has no refresh); paste a \
             fresh one"
                .into(),
        )
        .into()),
        Acquisition::MintJwt { .. } => mint_and_exchange(spec, values).await,
        // A runtime-supplied credential is never refreshed here: the
        // resolve path answers before reaching refresh.
        Acquisition::Runtime {} => {
            Err(anyhow::anyhow!("a runtime acquisition never reaches refresh"))
        }
        Acquisition::OAuth2 { grant, token_url, refresh: refresh_call, token_auth, .. } => {
            let Some(registration) = registration else {
                return Err(reconnect(
                    "the grant has no app to refresh with; reconnect".into(),
                )
                .into());
            };
            let reg = registration.to_values();
            // A declared renewal call replaces the standard one
            // entirely (Meta's long-lived-token exchange): run it
            // over the stored values plus the app's, write its
            // captures back, read the expiry off the answer.
            if let Some(call) = refresh_call {
                let mut inputs = values.clone();
                for (k, v) in reg {
                    inputs.entry(k).or_insert(v);
                }
                let resp = run_connect_call(call, &inputs).await.map_err(|e| {
                    anyhow::Error::from(reconnect(format!("the renewal was refused ({e:#})")))
                })?;
                apply_captures(&call.captures, &resp, values)?;
                return Ok(expires_at_of(&resp));
            }
            match grant {
                OAuthGrant::ClientCredentials => {
                    client_credentials_token_with(token_url, spec, &reg, values).await
                }
                OAuthGrant::AuthorizationCode { .. } => {
                    let Some(refresh_token) = values.get("refresh_token").cloned() else {
                        return Err(reconnect(
                            "the provider issued no refresh token, so the expired sign-in \
                             cannot be renewed; reconnect the account"
                                .into(),
                        )
                        .into());
                    };
                    let mut params: Vec<(String, String)> = vec![
                        ("grant_type".into(), "refresh_token".into()),
                        ("refresh_token".into(), refresh_token),
                    ];
                    let basic =
                        crate::flows::place_client_auth(*token_auth, &reg, &mut params);
                    let resp = token_request(token_url, &params, basic).await.map_err(|e| {
                        anyhow::Error::from(reconnect(format!("the refresh was refused ({e})")))
                    })?;
                    let token = resp
                        .get("access_token")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            reconnect("the refresh answered without an access_token".into())
                        })?;
                    values.insert("token".into(), token.to_string());
                    // Single-use refresh tokens (Xero-class) rotate: the
                    // answer's replacement is written back; absent =
                    // the old one stays valid.
                    if let Some(rt) = resp.get("refresh_token").and_then(Value::as_str) {
                        values.insert("refresh_token".into(), rt.to_string());
                    }
                    Ok(expires_at_of(&resp))
                }
            }
        }
    }
}

/// Request a client-credentials token (Zoom S2S class). Split so the
/// connect flow (which has the spec at hand) and refresh share it.
pub(crate) async fn client_credentials_token(
    spec: &AccessSpec,
    reg: &BTreeMap<String, String>,
    values: &mut BTreeMap<String, String>,
) -> anyhow::Result<Option<chrono::DateTime<chrono::Utc>>> {
    let Acquisition::OAuth2 { token_url, .. } = &spec.acquisition else {
        return Err(anyhow::anyhow!("client_credentials_token on a non-oauth2 spec"));
    };
    client_credentials_token_with(token_url, spec, reg, values).await
}

async fn client_credentials_token_with(
    token_url: &str,
    spec: &AccessSpec,
    reg: &BTreeMap<String, String>,
    values: &mut BTreeMap<String, String>,
) -> anyhow::Result<Option<chrono::DateTime<chrono::Utc>>> {
    let Acquisition::OAuth2 { extra_params, captures, token_auth, .. } = &spec.acquisition
    else {
        return Err(anyhow::anyhow!("client_credentials_token on a non-oauth2 spec"));
    };
    let mut params: Vec<(String, String)> =
        vec![("grant_type".into(), "client_credentials".into())];
    let basic = crate::flows::place_client_auth(*token_auth, reg, &mut params);
    for (name, template) in extra_params {
        // Extra params interpolate registration values (Zoom's
        // account_id is pasted at registration time).
        params.push((name.clone(), template.resolve(reg).map_err(AccessError::Invalid)?));
    }
    let resp = token_request(token_url, &params, basic).await?;
    let token = resp
        .get("access_token")
        .and_then(Value::as_str)
        .ok_or_else(|| AccessError::Invalid("token response carries no access_token".into()))?;
    values.insert("token".into(), token.to_string());
    apply_captures(captures, &resp, values)?;
    Ok(expires_at_of(&resp))
}

/// Mint the JWT and (when the spec declares an exchange) trade it for
/// the working token. Returns the token's expiry. Shared by connect
/// (validate now) and refresh (re-mint lazily).
pub(crate) async fn mint_and_exchange(
    spec: &AccessSpec,
    values: &mut BTreeMap<String, String>,
) -> anyhow::Result<Option<chrono::DateTime<chrono::Utc>>> {
    let Acquisition::MintJwt { algorithm, claims, jwt_ttl_secs, exchange, .. } =
        &spec.acquisition
    else {
        return Err(anyhow::anyhow!("mint_and_exchange on a non-mint_jwt spec"));
    };
    let weft_core::access::spec::JwtAlgorithm::RS256 = algorithm;
    let now = chrono::Utc::now().timestamp();
    let mut claim_map = serde_json::Map::new();
    for (name, template) in claims {
        claim_map.insert(
            name.clone(),
            Value::String(template.resolve(values).map_err(AccessError::Invalid)?),
        );
    }
    // 30s of backdating absorbs clock skew against the provider.
    claim_map.insert("iat".into(), Value::from(now - 30));
    claim_map.insert("exp".into(), Value::from(now + *jwt_ttl_secs as i64));
    let private_key = values.get("private_key").ok_or_else(|| {
        AccessError::Invalid("a mint_jwt access stores its key under 'private_key'".into())
    })?;
    let key = jsonwebtoken::EncodingKey::from_rsa_pem(private_key.as_bytes())
        .map_err(|e| AccessError::Invalid(format!("the pasted private key is not RSA PEM: {e}")))?;
    let jwt = jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
        &Value::Object(claim_map),
        &key,
    )
    .map_err(|e| anyhow::anyhow!("jwt mint failed: {e}"))?;

    let Some(exchange) = exchange else {
        values.insert("token".into(), jwt);
        return Ok(Some(
            chrono::Utc::now() + chrono::Duration::seconds(*jwt_ttl_secs as i64),
        ));
    };
    let url = exchange.url.resolve(values).map_err(AccessError::Invalid)?;
    let resp = base_client()
        .post(&url)
        .bearer_auth(&jwt)
        .header(reqwest::header::ACCEPT, "application/json")
        .send()
        .await
        .map_err(|e| {
            // The template, never the resolved URL: a recipe may
            // interpolate a secret into it, and reqwest's error text
            // would echo the URL whole.
            anyhow::anyhow!(
                "token exchange at {} failed: {}",
                exchange.url.0,
                e.without_url()
            )
        })?;
    let status = resp.status();
    let body: Value = resp.json().await.unwrap_or(Value::Null);
    if !status.is_success() {
        return Err(AccessError::NeedsReconnect {
            service: spec.service.clone(),
            reason: format!(
                "the token exchange answered {status}: {}",
                body.get("message").and_then(Value::as_str).unwrap_or("no detail")
            ),
        }
        .into());
    }
    let token = lookup_path(&body, &exchange.token_path)
        .and_then(Value::as_str)
        .ok_or_else(|| {
            AccessError::Invalid(format!(
                "the exchange response carries nothing at '{}'",
                exchange.token_path
            ))
        })?;
    values.insert("token".into(), token.to_string());
    apply_captures(&exchange.captures, &body, values)?;
    let expiry = match &exchange.expires_path {
        None => Some(chrono::Utc::now() + chrono::Duration::seconds(*jwt_ttl_secs as i64)),
        Some(path) => match lookup_path(&body, path) {
            Some(Value::String(s)) => Some(
                chrono::DateTime::parse_from_rfc3339(s)
                    .map_err(|e| {
                        AccessError::Invalid(format!("unparseable expiry '{s}' at '{path}': {e}"))
                    })?
                    .with_timezone(&chrono::Utc),
            ),
            Some(Value::Number(n)) => n
                .as_i64()
                .and_then(|secs| chrono::DateTime::from_timestamp(secs, 0)),
            _ => {
                return Err(AccessError::Invalid(format!(
                    "the exchange response carries no expiry at '{path}'"
                ))
                .into())
            }
        },
    };
    Ok(expiry)
}

// ---------- Editor resource lookups ----------

/// One page of lookup options, as the editor renders them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LookupPage {
    pub items: Vec<LookupItem>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LookupItem {
    pub id: String,
    pub label: String,
}

/// One `remote_select` lookup request as it travels the wire: the
/// editor posts it to the dispatcher, and the dispatcher forwards it
/// (tenant-wrapped) to the broker, which makes the outbound call.
/// ONE definition so the two hops cannot drift.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LookupRequest {
    /// The connection to sign with; `None` for a `public` lookup (no
    /// connection is involved at all, and `service` may be empty).
    #[serde(default)]
    pub access_id: Option<uuid::Uuid>,
    #[serde(default)]
    pub service: String,
    /// The widget's declarative lookup, verbatim from the node's
    /// (compiler-resolved) metadata.
    pub lookup: Lookup,
    #[serde(default)]
    pub query: String,
    /// Picked parent values for drill-down (`depends_on`).
    #[serde(default)]
    pub parents: BTreeMap<String, String>,
    #[serde(default)]
    pub cursor: Option<String>,
}

/// A `granted` resource-source read as it travels the wire (editor ->
/// dispatcher -> broker): which connection, and which stored capture's
/// id/label pairs to read. ONE definition so the hops cannot drift.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrantedQuery {
    pub access_id: uuid::Uuid,
    pub service: String,
    /// The stored value (captured at connect) holding the JSON array.
    pub from: String,
    /// Dotted path to one item's label (same vocabulary as [`Lookup`]).
    pub label: String,
    /// Dotted path to one item's id.
    pub value: String,
}

/// Read a `granted` resource source off the connection row: the stored
/// value `from` (captured at connect) parsed as a JSON array;
/// `label_path` / `value_path` address one item (same vocabulary as a
/// [`Lookup`]). Free: no provider call at all. Only the id/label pairs
/// leave; the rest of the stored values never do.
pub async fn granted_items(
    pool: &PgPool,
    tenant: &str,
    access_id: uuid::Uuid,
    service: &str,
    from: &str,
    label_path: &str,
    value_path: &str,
) -> anyhow::Result<Vec<LookupItem>> {
    // The same walled read every resolution runs, but the FULL stored
    // map (the worker-handoff filter would drop a captured list) and
    // STORED freshness: a captured list has no expiry, and an editor
    // lookup must never hold the row's write lock across a provider
    // refresh (a dropdown click would stall running work on the same
    // connection).
    let grant =
        read_walled_grant(pool, tenant, access_id, service, &[], Freshness::Stored).await?;
    let Some(raw) = grant.values.get(from) else {
        // The connection recorded nothing under that name: an empty
        // list, so the editor falls through to the next source.
        return Ok(Vec::new());
    };
    let parsed: Value = serde_json::from_str(raw).map_err(|e| {
        AccessError::Invalid(format!(
            "the connection's '{from}' capture is not a JSON list: {e}"
        ))
    })?;
    let items = parsed.as_array().ok_or_else(|| {
        AccessError::Invalid(format!("the connection's '{from}' capture is not a JSON list"))
    })?;
    Ok(items
        .iter()
        .filter_map(|item| {
            let id = display_of(lookup_path(item, value_path)?);
            let label =
                lookup_path(item, label_path).map(display_of).unwrap_or_else(|| id.clone());
            Some(LookupItem { id, label })
        })
        .collect())
}

/// The URL a lookup will call: the widget's `get` template with
/// `{query}` and `{<parent>}` substituted, percent-encoded. Unknown
/// placeholders are loud: a typo'd parent name must not silently hit
/// the service with a literal brace. Public so the lookup's caller
/// can name the exact URL when asking a credential policy about it.
pub fn lookup_url(
    spec: &Lookup,
    query: &str,
    parents: &BTreeMap<String, String>,
) -> anyhow::Result<String> {
    let mut url = String::with_capacity(spec.get.len());
    let mut rest = spec.get.as_str();
    while let Some(start) = rest.find('{') {
        url.push_str(&rest[..start]);
        let after = &rest[start + 1..];
        let Some(end) = after.find('}') else {
            return Err(AccessError::Invalid(format!(
                "unclosed '{{' in lookup URL {:?}",
                spec.get
            ))
            .into());
        };
        let name = &after[..end];
        let value = if name == "query" {
            query
        } else {
            parents.get(name).map(String::as_str).ok_or_else(|| {
                AccessError::Invalid(format!(
                    "lookup URL references '{{{name}}}' but no parent value named '{name}' \
                     was provided (pick it first)"
                ))
            })?
        };
        url.push_str(&urlencode(value));
        rest = &after[end + 1..];
    }
    url.push_str(rest);
    Ok(url)
}

/// Run a `remote_select` widget's declarative lookup at `url` (built
/// by [`lookup_url`]; the caller owns it so a credential policy can
/// inspect it, and reroute it through a relay, before the call is
/// made). `resolved` signs the call; `None` is the `public` lookup (a
/// credential-free endpoint, called bare). The caller also fills the
/// auth values when the credential is the runtime's. This side holds
/// the token and makes the call; nothing credential-shaped goes back
/// to the editor.
pub async fn lookup(
    resolved: Option<&ResolvedAccess>,
    spec: &Lookup,
    cursor: Option<&str>,
    url: &str,
) -> anyhow::Result<LookupPage> {
    let steps = match resolved {
        Some(r) => resolve_steps(&r.auth, &r.values).map_err(AccessError::Invalid)?,
        None => Vec::new(),
    };
    let client = authed_client(steps);

    let mut req = client.get(url);
    if let (Some(page), Some(cursor)) = (&spec.page, cursor) {
        req = req.query(&[(page.cursor_param.as_str(), cursor)]);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| {
            // The send error's own URL carries the applied auth (a query
            // token, a path-prefix token); `url` here is pre-auth and safe.
            anyhow::anyhow!(
                "lookup call to {url} failed: {}",
                weft_core::access::client::send_error(e)
            )
        })?;
    let status = resp.status();
    let body: Value = resp.json().await.unwrap_or(Value::Null);
    if !status.is_success() {
        return Err(AccessError::Invalid(format!(
            "the service answered {status} to the lookup; the access may need reconnecting"
        ))
        .into());
    }
    if body.get("ok").and_then(Value::as_bool) == Some(false) {
        return Err(AccessError::Invalid(format!(
            "the service refused the lookup: {}",
            body.get("error").and_then(Value::as_str).unwrap_or("unknown error")
        ))
        .into());
    }

    let items = lookup_path(&body, &spec.items)
        .and_then(Value::as_array)
        .ok_or_else(|| {
            AccessError::Invalid(format!(
                "the lookup response carries no array at '{}'",
                spec.items
            ))
        })?;
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let id = lookup_path(item, &spec.value).map(display_of).ok_or_else(|| {
            AccessError::Invalid(format!("a lookup item carries nothing at '{}'", spec.value))
        })?;
        let label = lookup_path(item, &spec.label).map(display_of).unwrap_or_else(|| id.clone());
        out.push(LookupItem { id, label });
    }
    let next_cursor = spec
        .page
        .as_ref()
        .and_then(|p| lookup_path(&body, &p.cursor_path))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    Ok(LookupPage { items: out, next_cursor })
}

fn display_of(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn urlencode(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// URL templating: `{query}` and `{<parent>}` substitute
    /// percent-encoded, unknown placeholders and unclosed braces are
    /// loud, and a substitution inside a query string stays a query.
    #[test]
    fn lookup_url_substitutes_encodes_and_refuses() {
        let spec = |get: &str| -> Lookup {
            serde_json::from_value(serde_json::json!({
                "get": get, "items": "data", "label": "id", "value": "id"
            }))
            .unwrap()
        };
        let parents: BTreeMap<String, String> =
            [("team".to_string(), "T 1/2".to_string())].into_iter().collect();
        assert_eq!(
            lookup_url(&spec("https://x.example/v1/items?q={query}&team={team}"), "a&b", &parents)
                .unwrap(),
            "https://x.example/v1/items?q=a%26b&team=T+1%2F2"
        );
        assert_eq!(
            lookup_url(&spec("https://x.example/v1/items"), "ignored", &BTreeMap::new()).unwrap(),
            "https://x.example/v1/items"
        );
        let unknown =
            lookup_url(&spec("https://x.example/{nope}"), "", &BTreeMap::new()).unwrap_err();
        assert!(unknown.to_string().contains("nope"), "{unknown}");
        let unclosed =
            lookup_url(&spec("https://x.example/{query"), "", &BTreeMap::new()).unwrap_err();
        assert!(unclosed.to_string().contains("unclosed"), "{unclosed}");
    }

    /// The recipe hash is a stable content address: identical maps
    /// hash identically, any content change re-hashes, and an empty
    /// map (no events declared) has no hash at all.
    #[test]
    fn events_recipe_hash_is_content_addressed() {
        let recipe = |account_path: &str| -> BTreeMap<String, EventsSpec> {
            serde_json::from_value(serde_json::json!({
                "things": {
                    "fields": { "kind": "kind" },
                    "account": { "value": "user", "path": account_path },
                    "webhook": { "verify": { "kind": "token_echo" } }
                }
            }))
            .unwrap()
        };
        let a = events_recipe_hash(&recipe("account")).unwrap();
        let same = events_recipe_hash(&recipe("account")).unwrap();
        let other = events_recipe_hash(&recipe("workspace")).unwrap();
        assert_eq!(a, same, "the hash is deterministic");
        assert_ne!(a, other, "different content, different hash");
        assert_eq!(a.len(), 64, "sha256 hex");
        assert_eq!(events_recipe_hash(&BTreeMap::new()), None);
    }

    /// The drift backstop's four ways: verified-and-covered passes,
    /// verified-and-short refuses naming the shortfall,
    /// claimed-and-short passes, and an UNKNOWN (empty, unverified)
    /// granted set passes too. The last case is the one that would
    /// refuse every pasted key on a non-reporting service if got
    /// backwards.
    #[test]
    fn permission_shortfall_only_refuses_a_verified_gap() {
        let granted = vec!["a".to_string(), "b".to_string()];
        let need_ab = vec!["a".to_string(), "b".to_string()];
        let need_c = vec!["c".to_string()];

        assert_eq!(permission_shortfall(true, &granted, &need_ab), None);
        assert_eq!(permission_shortfall(true, &granted, &need_c), Some("c"));
        assert_eq!(permission_shortfall(false, &granted, &need_c), None);
        assert_eq!(
            permission_shortfall(false, &[], &need_c),
            None,
            "an unknown granted set never blocks"
        );
        assert_eq!(permission_shortfall(true, &granted, &[]), None);
    }

    /// The own-account gate: an `Ours` credential is refused for an
    /// own-only capability (with the guide link in the reason), passes
    /// for plain capabilities, and a user-owned credential always
    /// passes whatever is required.
    #[test]
    fn own_only_gate_refuses_the_runtime_credential_only() {
        let spec: AccessSpec = serde_json::from_value(serde_json::json!({
            "service": "elevenlabs",
            "acquisition": { "kind": "static", "fields": [{ "name": "key" }] },
            "auth": [{ "kind": "header", "name": "xi-api-key", "value": "{key}" }],
            "permissions": [
                { "id": "generate", "label": "Generate audio", "description": "x" },
                { "id": "voice_lab", "label": "Voice creation", "description": "x",
                  "own_only": true,
                  "guide": { "link": "https://example/voices", "steps": ["s"] } },
            ],
        }))
        .expect("spec parses");
        use weft_core::CredentialOwner::{Ours, TheirOwn};
        let need = |ids: &[&str]| ids.iter().map(|s| s.to_string()).collect::<Vec<_>>();

        let reason =
            own_only_refusal(Ours, &spec, &need(&["voice_lab"])).expect("Ours is refused");
        assert!(reason.contains("Voice creation"), "{reason}");
        assert!(reason.contains("https://example/voices"), "{reason}");
        assert_eq!(own_only_refusal(Ours, &spec, &need(&["generate"])), None);
        assert_eq!(own_only_refusal(TheirOwn, &spec, &need(&["voice_lab"])), None);
        assert_eq!(own_only_refusal(Ours, &spec, &[]), None);
    }
}
