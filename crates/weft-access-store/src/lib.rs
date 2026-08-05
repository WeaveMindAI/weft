//! The access store: stored REGISTRATIONS (a tenant's app identity at
//! an OAuth service: client id + secret, entered once) and GRANTS (one
//! consent's/paste's result: stored values + granted scopes + account
//! identity), plus the pending rows an in-flight OAuth connect parks in.
//!
//! Postgres is the single source of truth; every function takes the
//! pool and the caller's tenant, and every row carries `tenant_id`
//! (the tenant wall). Secrets live ONLY here: the editor sends pasted
//! values straight to these functions and reads back summaries that
//! never contain a stored value; a resolution hands a worker
//! everything its connection holds except the store's own keep-alive
//! material (the refresh token it uses to renew an expired sign-in).
//!
//! Refresh is LAZY, at resolution time, single-flight via the grant
//! row's lock; there is no background keep-alive. A grant the provider
//! revoked fails loud with a reconnect message, never a silent retry.

mod crypt;
mod flows;
mod resolve;
mod subscriptions;

pub use crypt::{open_json, open_str, seal_json, seal_str};

pub use flows::{
    begin_oauth, begin_picker, complete_oauth, connect_direct, delete_grant, finish_picker,
    list_grants, load_picker, sweep_expired_connects, take_connect_result, BeginOAuth,
    BeginPicker, CompletedConnect, ConnectDirect, MintAppRequest, MintAppResponse, OAuthComplete,
    PickerSession, SharedDoorPick, StartedOAuth,
};
pub use subscriptions::{
    drop_subscriptions_for_signal, ensure_subscription, needs_renewal, no_public_url_error,
    run_connect_call, subscription_by_id, EnsureSubscription, EnsuredSubscription, Subscription,
};
pub use resolve::{
    connections_for_event, events_recipe_hash, events_recipes_of, granted_items, lookup,
    recipe_value_names, record_events_recipes, resolve_event_source, resolve_for_worker,
    EventTarget, GrantedQuery, LookupItem, LookupPage, LookupRequest, RecordedEventsRecipe,
    ResolvedAccess,
    ResolvedEventSource,
};

use serde::{Deserialize, Serialize};

/// One registered app the editor offers as its own shared-door option:
/// its label and its FIXED permission set. The user picks an option;
/// they never tick permissions on the shared door.
// SYNC: SharedAppChoice <-> packages/weft-graph/src/webview/lib/components/project/AccessField.svelte SharedAppChoice
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedAppChoice {
    pub label: String,
    pub covers: Vec<String>,
}

/// The doors probe's answer core: which shared-door options exist right
/// now. The ONE definition every service answering or forwarding the
/// probe uses (the editor hides what is not offered; it never greys).
// SYNC: DoorsAnswer <-> crates/weft-dispatcher/src/api/access.rs DoorsStatus (flattens it), packages/weft-graph/src/webview/lib/components/project/AccessField.svelte DoorsStatus
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoorsAnswer {
    /// The registered apps, one shared-door option each (oauth
    /// services only). Empty = the one-click door is hidden.
    pub shared_apps: Vec<SharedAppChoice>,
    /// Whether a runtime credential backs the shared door of a
    /// non-oauth (key) service.
    pub shared_credential: bool,
}

/// A connection row as the EDITOR sees it: names, ids, identity,
/// permissions. Never a stored value. Everything the connection list
/// renders (identity / app label / what it can do, plus the
/// spends-credits and verified marks) rides here, so the list needs no
/// second call.
// SYNC: GrantSummary <-> packages/weft-graph/src/protocol.ts GrantSummary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrantSummary {
    pub id: uuid::Uuid,
    pub service: String,
    /// The owning project for a `coexisting`-class grant; `None` for an
    /// `exclusive`-class shared grant (every project referencing it
    /// follows its rotations).
    pub project_id: Option<String>,
    pub identity: Option<String>,
    /// The connection list's middle column: the app's label, or the
    /// name the user typed for a pasted credential. `None` only for
    /// rows with neither (a runtime-supplied credential).
    pub label: Option<String>,
    /// The granted permission set (the "what it can do" column).
    pub scopes: Vec<String>,
    /// Whether `scopes` came from the provider (verified) or from the
    /// user's ticks (claimed). Decides how hard a shortfall fails.
    pub permissions_verified: bool,
    /// The NAMES of the values this connection stores (never the
    /// values themselves). What the editor's live `requiresValues`
    /// check compares against, for a service whose optional fields
    /// decide what a connection can do (a mailbox holding only the
    /// sending server can send and cannot receive).
    #[serde(default)]
    pub value_names: Vec<String>,
    /// Whose credential the row resolves to. `Ours` rows are the
    /// spends-credits connections.
    pub owner: weft_core::CredentialOwner,
    /// Which door created it; drives the shared-door displacement
    /// warning (shown once per connection created through it).
    pub door: weft_core::access::spec::Door,
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// The store's failure vocabulary, downcast at the API edge for status
/// codes. Everything else rides `anyhow` as a 500.
#[derive(Debug, thiserror::Error)]
pub enum AccessError {
    /// Missing OR another tenant's (one answer, no existence leak).
    #[error("access not found")]
    NotFound,
    /// The grant cannot authenticate calls anymore; the fix is the
    /// editor's reconnect affordance.
    #[error("the '{service}' access needs reconnecting: {reason}")]
    NeedsReconnect { service: String, reason: String },
    /// Caller-fixable bad input (unknown service on the marker, scope
    /// drift, malformed spec).
    #[error("{0}")]
    Invalid(String),
}

/// Map a store error to an HTTP status at an API edge: missing/foreign
/// rows are 404 (one answer, no existence leak), caller-fixable input
/// is 400, a dead grant is 409 (the fix is a reconnect, not a retry).
/// `None` = internal: the edge logs the chain itself and answers 500
/// without echoing detail. ONE mapping so every surface fronting the
/// store (dispatcher and broker) answers identically.
pub fn client_status(e: &anyhow::Error) -> Option<(u16, String)> {
    match e.downcast_ref::<AccessError>() {
        Some(AccessError::NotFound) => Some((404, format!("{e}"))),
        Some(AccessError::Invalid(_)) => Some((400, format!("{e}"))),
        Some(AccessError::NeedsReconnect { .. }) => Some((409, format!("{e}"))),
        None => None,
    }
}

/// Create the store's tables. Registered in the dispatcher's
/// `run_core_migrations_locked`; idempotent, guarded by the caller's
/// advisory lock.
pub async fn migrate(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    sqlx::raw_sql(
        r#"
        CREATE TABLE IF NOT EXISTS access_grant (
            id UUID PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            service TEXT NOT NULL,
            -- The resolved app credentials (client id + secret + extras),
            -- snapshotted at connect so runtime refresh needs no lookup.
            -- SEALED (crypt.rs). NULL for services that use no OAuth app
            -- (a pasted token).
            registration_sealed TEXT,
            -- The app snapshot's client id, plain: it is public by
            -- OAuth's design, and event routing filters on it in SQL.
            client_id TEXT,
            -- NULL for an exclusive-class shared grant.
            project_id TEXT,
            -- The AccessSpec snapshot: refresh/auth need no catalog.
            spec_json JSONB NOT NULL,
            -- The stored values (token, refresh_token, captures, pasted
            -- fields), SEALED (crypt.rs). What a resolution hands over
            -- is everything here EXCEPT the store's own keep-alive
            -- material (the refresh token), so a credential of any
            -- shape travels whole.
            values_sealed TEXT NOT NULL,
            -- The NAMES of the sealed values, plain: the editor's
            -- connection list shows what a connection stores without
            -- the store opening every row.
            value_names JSONB NOT NULL DEFAULT '[]',
            granted_scopes JSONB NOT NULL DEFAULT '[]',
            -- Whether granted_scopes came from the provider (verified)
            -- or from the user's ticks (claimed); a shortfall only
            -- hard-fails when verified.
            permissions_verified BOOLEAN NOT NULL DEFAULT FALSE,
            -- Whose credential the row resolves to: 'their-own' (the
            -- stored values) or 'ours' (the runtime's credential
            -- source answers per call; values_sealed holds an empty map).
            owner TEXT NOT NULL DEFAULT 'their-own',
            -- Which door created it ('shared' / 'own'); drives the
            -- shared-door displacement warning.
            door TEXT NOT NULL DEFAULT 'own',
            -- The connection list's middle column: the app's label, or
            -- the name the user typed for a pasted credential.
            label TEXT,
            identity TEXT,
            -- The provider's own identifier for the account this
            -- connection belongs to (a workspace id, a mailbox
            -- address), captured at connect from the value the
            -- service's events recipe names. Indexed because an
            -- inbound event names this and nothing else: routing a
            -- push must be one index lookup, never a scan digging
            -- through every row's stored values. NULL for a service
            -- that reports no events.
            provider_account TEXT,
            -- The content hash (sha256 hex) of the spec snapshot's
            -- events block, NULL when it declares none. Written
            -- beside every spec_json write. An inbound push is
            -- answered by the recipe the connection itself declared,
            -- so routing filters on this against the verifying
            -- recipe's hash.
            events_recipe_hash TEXT,
            expires_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS access_grant_tenant_service
            ON access_grant (tenant_id, service);
        -- The inbound-event lookup: an incoming push names a service
        -- and an account, and must find every connection to it
        -- without knowing a tenant (which is the point: the push
        -- proves which account it concerns, and that IS the routing).
        CREATE INDEX IF NOT EXISTS access_grant_service_account
            ON access_grant (service, provider_account);
        -- One in-flight OAuth connect per state nonce. Postgres-backed so
        -- the callback may land on any dispatcher pod.
        CREATE TABLE IF NOT EXISTS access_connect (
            state TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            service TEXT NOT NULL,
            -- The resolved app credentials for the code exchange, carried
            -- from begin to callback (any dispatcher pod completes it).
            -- SEALED (crypt.rs).
            registration_sealed TEXT NOT NULL,
            project_id TEXT,
            spec_json JSONB NOT NULL,
            scopes JSONB NOT NULL DEFAULT '[]',
            -- SEALED (crypt.rs): with the consent code intercepted, the
            -- verifier is what stands between a dump and the token.
            pkce_verifier TEXT,
            -- Which door started this consent ('shared' / 'own');
            -- recorded onto the grant at completion.
            door TEXT NOT NULL DEFAULT 'own',
            -- Set when this connect upgrades/rotates an existing
            -- exclusive-class grant in place.
            upgrade_grant_id UUID,
            redirect_uri TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        -- The sweep deletes abandoned rows by age; without this index
        -- it would scan.
        CREATE INDEX IF NOT EXISTS access_connect_created
            ON access_connect (created_at);
        -- One in-flight resource-picker session per state nonce: the
        -- node-declared chooser (script + glue) plus the connection it
        -- picks against. The weft-served picker page reads it; the
        -- outcome parks in access_connect_result like a consent's.
        CREATE TABLE IF NOT EXISTS access_picker (
            state TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            access_id UUID NOT NULL,
            service TEXT NOT NULL,
            script TEXT NOT NULL,
            code TEXT NOT NULL,
            mime_types JSONB NOT NULL DEFAULT '[]',
            -- The permissions choosing a resource GRANTS (the node's
            -- declared `grants`), parked with the session; a finished
            -- pick unions them into the grant row's granted_scopes.
            grants JSONB NOT NULL DEFAULT '[]',
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS access_picker_created
            ON access_picker (created_at);
        -- The browser flows' outcome (a consent's grant, a picker's
        -- pick), parked for the EDITOR to poll (the flow finished in a
        -- page the editor cannot see). One row per state nonce;
        -- consumed on read.
        CREATE TABLE IF NOT EXISTS access_connect_result (
            state TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            result_json JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS access_connect_result_created
            ON access_connect_result (created_at);
        -- One provider-side event subscription serving one registered
        -- signal: the id + token weft minted, what the provider
        -- answered (its resource id, the expiry), and which signal it
        -- feeds. Written when the serving side runs the service's
        -- subscribe call, renewed by re-running it before expires_at,
        -- deleted (after the provider's unsubscribe call) when the
        -- signal unregisters. An inbound push that routes by a minted
        -- id looks this table up and nothing else.
        CREATE TABLE IF NOT EXISTS signal_subscription (
            -- The id weft minted and sent to the provider.
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            service TEXT NOT NULL,
            -- Which event topic of the service this subscription is
            -- on; keys the recipe its calls and verification use.
            topic TEXT NOT NULL,
            access_id UUID NOT NULL,
            -- The registered signal this subscription feeds.
            signal_token TEXT NOT NULL,
            -- The secret weft minted; what a no-signature provider
            -- echoes on every push and the verify compares against.
            -- SEALED (crypt.rs): plain, a dump could forge pushes.
            token_sealed TEXT NOT NULL,
            -- Values the provider's subscribe answer captured
            -- (resource id, anything the unsubscribe call needs).
            captures_json JSONB NOT NULL DEFAULT '{}',
            expires_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        -- The events recipes seen per service, keyed by content hash:
        -- upserted from every spec that passes through a store flow
        -- (door probe, connects), read by the public events receiver,
        -- which must know how to verify and route a push BEFORE it
        -- knows which connection it concerns (and must answer a
        -- provider's address-proving handshake when no connection
        -- exists yet). Hash-keyed so a push is answered by the recipe
        -- the connection itself declared: routing pairs a verifying
        -- row's hash with access_grant.events_recipe_hash.
        CREATE TABLE IF NOT EXISTS service_events_recipe (
            service TEXT NOT NULL,
            -- sha256 hex of the recipe's canonical JSON.
            recipe_hash TEXT NOT NULL,
            events_json JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (service, recipe_hash)
        );
        -- The renewal sweep asks "which subscriptions die soon".
        CREATE INDEX IF NOT EXISTS signal_subscription_expiry
            ON signal_subscription (expires_at);
        -- Unregistering a signal deletes its subscriptions.
        CREATE INDEX IF NOT EXISTS signal_subscription_signal
            ON signal_subscription (signal_token);
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Parse a grant row's spec snapshot back into the typed recipe. A
/// snapshot that no longer parses is a real bug (the spec vocabulary
/// only grows), surfaced loudly.
pub(crate) fn spec_of(spec_json: &serde_json::Value) -> anyhow::Result<weft_core::AccessSpec> {
    serde_json::from_value(spec_json.clone())
        .map_err(|e| anyhow::anyhow!("stored access spec no longer parses: {e}"))
}

pub(crate) fn values_of(
    values_json: &serde_json::Value,
) -> std::collections::BTreeMap<String, String> {
    values_json
        .as_object()
        .map(|o| {
            o.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

/// The DB string of an owner / door and back: exactly serde's
/// kebab/snake tag, so the column and the wire can never disagree.
pub(crate) fn owner_str(owner: weft_core::CredentialOwner) -> &'static str {
    match owner {
        weft_core::CredentialOwner::TheirOwn => "their-own",
        weft_core::CredentialOwner::Ours => "ours",
    }
}

pub(crate) fn owner_of(s: &str) -> anyhow::Result<weft_core::CredentialOwner> {
    serde_json::from_value(serde_json::Value::String(s.to_string()))
        .map_err(|_| anyhow::anyhow!("connection row has an unknown owner '{s}'"))
}

pub(crate) fn door_str(door: weft_core::access::spec::Door) -> &'static str {
    match door {
        weft_core::access::spec::Door::Shared => "shared",
        weft_core::access::spec::Door::Own => "own",
    }
}

pub(crate) fn door_of(s: &str) -> anyhow::Result<weft_core::access::spec::Door> {
    serde_json::from_value(serde_json::Value::String(s.to_string()))
        .map_err(|_| anyhow::anyhow!("connection row has an unknown door '{s}'"))
}

pub(crate) fn scopes_of(scopes_json: &serde_json::Value) -> Vec<String> {
    scopes_json
        .as_array()
        .map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
        .unwrap_or_default()
}
