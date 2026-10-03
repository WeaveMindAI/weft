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
mod picks;
mod resolve;
mod subscriptions;

pub use crypt::{open_json, open_str, seal_json, seal_str};

pub use flows::{
    begin_oauth, begin_picker, own_connection_gate, change_instance_values, complete_oauth, connection_handle, connect_direct, delete_grant,
    delete_published_grants, finish_picker, forget_instance_grants, forget_project_access, list_grants, load_picker,
    instance_connection_counts, instance_value_counts, instance_values, publish_grant, published_connection, sweep_expired_connects,
    ConnectionValue, InstanceValueWrite,
    take_connect_result, BeginPicker, GrantOwnerScope, OAuthComplete, PickerSession,
    PublishAccess,
};
pub use picks::{change_install_picks, install_picks, move_stored, stored_fields, PickWrite};
pub use subscriptions::{
    drop_subscriptions_for_signal, ensure_subscription, needs_renewal, no_public_url_error,
    run_connect_call, subscription_by_id, EnsureSubscription, EnsuredSubscription, Subscription,
};
pub use resolve::{
    caller_verifier, connections_for_event, events_recipe_hash, events_recipes_of, granted_items,
    lookup, lookup_url, recipe_value_names, record_events_recipes, resolve_event_source,
    resolve_for_worker, CallerVerifier, EventTarget, GrantUser, GrantedQuery, LookupItem, LookupPage,
    LookupRequest, RecordedEventsRecipe, ResolvedAccess, ResolvedEventSource,
};

// The connect flow's wire shapes live in weft-core `access::wire`,
// under ONE import path for every crate; nothing re-exports them.

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
    /// A browser flow's state that is unknown or past its time: nothing
    /// will ever land under it, so a poll must stop.
    #[error("{0}")]
    Gone(String),
    /// A provider that was not reached, or did not answer in time:
    /// nothing is wrong with the request, and trying again is the fix.
    #[error("{0}")]
    Unreached(String),
}

/// Map a store error to an HTTP status at an API edge: missing/foreign
/// rows are 404 (one answer, no existence leak), caller-fixable input
/// is 400, a dead grant is 409 (the fix is a reconnect, not a retry),
/// a browser flow that is gone is 410 (the fix is starting it again), a
/// provider not reached is 502 (the fix is trying again).
/// `None` = internal: the edge logs the chain itself and answers 500
/// without echoing detail. ONE mapping so every surface fronting the
/// store (dispatcher and broker) answers identically.
fn client_status(e: &anyhow::Error) -> Option<(u16, String)> {
    match e.downcast_ref::<AccessError>() {
        Some(AccessError::NotFound) => Some((404, format!("{e}"))),
        Some(AccessError::Invalid(_)) => Some((400, format!("{e}"))),
        Some(AccessError::NeedsReconnect { .. }) => Some((409, format!("{e}"))),
        Some(AccessError::Gone(_)) => Some((410, format!("{e}"))),
        Some(AccessError::Unreached(_)) => Some((502, format!("{e}"))),
        None => None,
    }
}

/// A provider's answer as text: one that stops arriving is a provider not
/// reached, never an empty answer.
pub(crate) async fn read_answer(resp: reqwest::Response, shown: &str) -> anyhow::Result<String> {
    Ok(resp
        .text()
        .await
        .map_err(|e| AccessError::Unreached(format!("the answer from {shown} did not arrive: {}", e.without_url())))?)
}

/// [`read_answer`] as JSON: an empty or unparsable one is `Null` (the
/// status and the fields read next say what is wrong).
pub(crate) async fn read_json_answer(resp: reqwest::Response, shown: &str) -> anyhow::Result<serde_json::Value> {
    Ok(serde_json::from_str(&read_answer(resp, shown).await?).unwrap_or(serde_json::Value::Null))
}

/// A store error as a surface should ANSWER it: the client-fixable
/// classes keep their status and message, and anything else is logged
/// here (with its cause chain, which the caller must not echo) and
/// answered opaquely.
///
/// ONE definition, so the dispatcher and the broker cannot drift on
/// what a store failure looks like or on what leaks with it.
pub fn client_error(e: anyhow::Error) -> (u16, String) {
    match client_status(&e) {
        Some(answer) => answer,
        None => {
            tracing::error!(target: "weft_access_store", "access store error: {e:#}");
            (500, "access store error".to_string())
        }
    }
}

/// The store's schema, applied at boot via
/// `weft_task_store::schema_guard::apply_groups` alongside every other
/// module's group.
pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "access_grant",
    tables: &[
        "access_grant",
        "instance_value",
        "install_pick",
        "access_connect",
        "access_picker",
        "access_connect_result",
        "signal_subscription",
        "service_events_recipe",
    ],
    ddl: &[
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
            project_id UUID,
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
            -- Set ONLY on a connection a node published for something
            -- it runs itself (`ctx.publish_access`): the id of that
            -- node. NULL on every connection a person made, which is
            -- what tells the two apart. A published connection is
            -- owned by its node: republishing finds it instead of
            -- making a second one, and terminating the node deletes
            -- it, so it lives exactly as long as the thing it opens.
            published_by_node TEXT,
            -- Whose connection, inside a project: NULL for one the
            -- author made (the project's, or a tenant-wide shared one),
            -- else the instance of `project_id` that connected it (or whose
            -- copy of a node published it). An instance is always a
            -- project's, so an instance's connection always names one.
            instance_id TEXT,
            expires_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT access_grant_instance_has_project
                CHECK (instance_id IS NULL OR project_id IS NOT NULL)
        );
        CREATE INDEX IF NOT EXISTS access_grant_tenant_service
            ON access_grant (tenant_id, service);
        -- A node publishes ONE connection per service it runs, so
        -- republishing is a lookup on this key, and two racing runs
        -- cannot leave two rows behind.
        CREATE UNIQUE INDEX IF NOT EXISTS access_grant_published
            ON access_grant (tenant_id, project_id, published_by_node, service, instance_id) NULLS NOT DISTINCT
            WHERE published_by_node IS NOT NULL;
        -- An instance's connections, for its picker and its forget.
        CREATE INDEX IF NOT EXISTS access_grant_instance
            ON access_grant (project_id, instance_id) WHERE instance_id IS NOT NULL;
        -- What an instance provides for the fields its program writes
        -- `@instance_filled`: the value a run for it puts where the
        -- source would hold one. The step is its place, spelled the
        -- way the program reads it (`read`, `one.read`), and the field
        -- one of its inputs. A value that is a connection (the
        -- `{id, identity}` handle an access field holds) names its
        -- grant, so removing the connection removes every value using
        -- it, and its identity is read fresh from the grant.
        CREATE TABLE IF NOT EXISTS instance_value (
            tenant_id TEXT NOT NULL,
            project_id UUID NOT NULL,
            instance_id TEXT NOT NULL,
            step TEXT NOT NULL,
            field TEXT NOT NULL,
            value JSONB NOT NULL,
            grant_id UUID REFERENCES access_grant(id) ON DELETE CASCADE,
            set_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (project_id, instance_id, step, field)
        );
        CREATE INDEX IF NOT EXISTS instance_value_grant ON instance_value (grant_id) WHERE grant_id IS NOT NULL;
        -- The connection each of a program's access nodes uses on this
        -- install (`weft_core::picks`): picked here, never written in the
        -- source, since a connection's id means nothing on another
        -- install. One of the author's own connections (no instance's),
        -- and removing the connection removes the pick with it.
        CREATE TABLE IF NOT EXISTS install_pick (
            tenant_id TEXT NOT NULL,
            project_id UUID NOT NULL,
            step TEXT NOT NULL,
            field TEXT NOT NULL,
            grant_id UUID NOT NULL REFERENCES access_grant(id) ON DELETE CASCADE,
            set_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (project_id, step, field)
        );
        CREATE INDEX IF NOT EXISTS install_pick_grant ON install_pick (grant_id);
        -- The inbound-event lookup: an incoming push names a service
        -- and an account, and must find every connection to it
        -- without knowing a tenant (which is the point: the push
        -- proves which account it concerns, and that IS the routing).
        CREATE INDEX IF NOT EXISTS access_grant_service_account
            ON access_grant (service, provider_account);
        -- One in-flight OAuth connect per state nonce. Postgres-backed so
        -- the callback may land on any dispatcher replica.
        CREATE TABLE IF NOT EXISTS access_connect (
            state TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            service TEXT NOT NULL,
            -- The resolved app credentials for the code exchange, carried
            -- from begin to callback (any dispatcher replica completes it).
            -- SEALED (crypt.rs).
            registration_sealed TEXT NOT NULL,
            project_id UUID,
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
            -- The instance this connect is for (its browser went
            -- through an instance token, or the program's backend named it):
            -- recorded onto the grant at completion. NULL for the
            -- author's own connect.
            instance_id TEXT,
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
            -- The instance the pick is for, when it opened at its own
            -- door: the chooser then signs in with a connection only
            -- that instance may use.
            project_id UUID,
            instance_id TEXT,
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
            -- The instance whose signal this is, when the connection is
            -- theirs: stopping the channel signs in as them.
            project_id UUID,
            instance_id TEXT,
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
    ],
    seed: &[],
};

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

/// The DB string of an owner, and back. The column says where the
/// credential comes from: `ours` (the runtime's own key) or `their-own`
/// (material the row stores). WHOSE own it is, the author's or an
/// instance's, is the row's `instance_id`, so reading an owner takes both.
pub(crate) fn owner_str(owner: &weft_core::CredentialOwner) -> &'static str {
    if owner.is_platform() { "ours" } else { "their-own" }
}

pub(crate) fn owner_of(s: &str, instance: Option<&str>) -> anyhow::Result<weft_core::CredentialOwner> {
    match s {
        "ours" => Ok(weft_core::CredentialOwner::Platform),
        "their-own" => Ok(weft_core::CredentialOwner::own(
            instance
                .map(|m| weft_core::instance::InstanceId::new(m).map_err(anyhow::Error::msg))
                .transpose()?,
        )),
        other => anyhow::bail!("connection row has an unknown owner '{other}'"),
    }
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
