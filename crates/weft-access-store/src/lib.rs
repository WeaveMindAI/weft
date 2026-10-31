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
pub mod instance_tokens;
pub mod migrate;
mod picks;
mod resolve;
mod subscriptions;
mod transfer;

use weft_task_store::db::{Client as _, JsonText, Transaction};
use weft_task_store::documents::Rederived;

pub use crypt::{open_json, open_str, seal_json, seal_str};

pub use flows::{
    begin_oauth, begin_picker, own_connection_gate, change_instance_values, complete_oauth, connection_handle, connect_direct, delete_grant,
    delete_published_grants, finish_picker, forget_instance_grants, forget_project_access, list_grants, load_picker,
    instance_connection_counts, instance_value_counts, instance_values, own_sign_in_services, publish_grant, published_connection,
    ConnectionValue, InstanceValueWrite,
    take_connect_result, BeginPicker, GrantOwnerScope, OAuthComplete, PickerSession,
    PublishAccess, write_pushed_values,
};
pub use picks::{change_install_picks, install_picks, move_stored, stored_fields, PickWrite};
pub use subscriptions::{
    drop_subscriptions_for_signal, ensure_subscription, needs_renewal, no_public_url_error,
    run_connect_call, subscription_by_id, EnsureSubscription, EnsuredSubscription, Subscription,
};
pub use resolve::{
    caller_credential_headers, caller_verifier, connections_for_event, events_recipe_hash, events_recipes_of, granted_items,
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

/// The store's error for a provider's refusal of `kind` (read off its
/// answer with [`RefusalKind::of`]): one saying it could not answer
/// now (busy, slow or down) is [`AccessError::Unreached`], tried again,
/// never a reconnect; any other is what `refused` makes of it, since only
/// the caller knows whether the credential or the request was refused.
///
/// [`RefusalKind::of`]: weft_core::access::client::RefusalKind::of
pub(crate) fn refusal(
    kind: weft_core::access::client::RefusalKind,
    why: String,
    refused: impl FnOnce(String) -> AccessError,
) -> AccessError {
    match kind {
        weft_core::access::client::RefusalKind::Temporary => AccessError::Unreached(why),
        weft_core::access::client::RefusalKind::Auth | weft_core::access::client::RefusalKind::Other => refused(why),
    }
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
        "access_transfer_key",
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
            -- The AccessSpec snapshot: refresh/auth need no catalog. As
            -- its JSON text (`db::JsonText`): a node publishing a
            -- connection writes it.
            spec TEXT NOT NULL,
            -- The version `spec` was written at (`AccessSpec::STORED`).
            spec_version INTEGER NOT NULL,
            -- Whether the spec's `grants` is `exclusive` (the provider
            -- keeps one grant per app and account), copied out of it on
            -- every write for the `access_grant_one_exclusive` index.
            exclusive BOOLEAN NOT NULL,
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
            -- beside every spec write. An inbound push is
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
            -- How many seconds the connection's token lasts, as the
            -- provider last stated it (its expires_in, or an absolute
            -- expiry less the moment the answer arrived), never
            -- re-measured at the write. A renewal answer naming no
            -- usable expiry gets this lifetime again. NULL while the
            -- token has no expiry.
            token_lifetime_secs BIGINT,
            -- Until when one resolution holds the renewal of the token
            -- (`resolve.rs`, `claim_renewal`): the others wait for what
            -- it writes, so the provider sees one renewal, and no
            -- database connection is held across its answer. NULL while
            -- nobody renews; a connection made again over the row clears
            -- it, so a renewal in flight never writes over it.
            renewal_claimed_until TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            -- How many times the connection was made again over this row
            -- (a reconnect or a rotate, `flows.rs`): what tells a signal
            -- acting as it that its credentials changed. A token refresh,
            -- a scope widened or a label renamed leaves it as it is.
            reconnects BIGINT NOT NULL DEFAULT 0,
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
        -- An exclusive-class service (the provider keeps one grant per
        -- app and account) holds ONE row per owner for an account signed
        -- in through an app, so a second sign-in, even one landing at the
        -- same moment as the first, rotates that row instead of leaving
        -- two (`flows.rs`, `insert_grant`). A pasted credential with no
        -- app and a node's published connection are not that row.
        -- SYNC: access_grant_one_exclusive <-> weft_access_store::flows::insert_grant, weft_access_store::flows::exclusive_row
        CREATE UNIQUE INDEX IF NOT EXISTS access_grant_one_exclusive
            ON access_grant (tenant_id, service, identity, client_id, instance_id, project_id) NULLS NOT DISTINCT
            WHERE published_by_node IS NULL AND client_id IS NOT NULL AND exclusive;
        -- An instance's connections, for its picker and its forget.
        CREATE INDEX IF NOT EXISTS access_grant_instance
            ON access_grant (project_id, instance_id) WHERE instance_id IS NOT NULL;
        -- What an instance provides for the fields its program writes
        -- `@instance_filled`: the value a run for it puts where the
        -- source would hold one. The node is its place, spelled the
        -- way the program reads it (`read`, `one.read`), and the field
        -- one of its inputs. A value that is a connection (the
        -- `{id, identity}` handle an access field holds) names its
        -- grant, so removing the connection removes every value using
        -- it, and its identity is read fresh from the grant.
        CREATE TABLE IF NOT EXISTS instance_value (
            tenant_id TEXT NOT NULL,
            project_id UUID NOT NULL,
            instance_id TEXT NOT NULL,
            node TEXT NOT NULL,
            field TEXT NOT NULL,
            -- As its JSON text (`db::JsonText`): a value a person typed
            -- comes back exactly.
            value TEXT NOT NULL,
            grant_id UUID REFERENCES access_grant(id) ON DELETE CASCADE,
            set_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (project_id, instance_id, node, field)
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
            node TEXT NOT NULL,
            field TEXT NOT NULL,
            grant_id UUID NOT NULL REFERENCES access_grant(id) ON DELETE CASCADE,
            set_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (project_id, node, field)
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
            -- As its JSON text (`db::JsonText`), like the grant's.
            spec_json TEXT NOT NULL,
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
            -- As its JSON text (`db::JsonText`), `null` while the flow is
            -- still out: it quotes what the provider answered.
            result_json TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS access_connect_result_created
            ON access_connect_result (created_at);
        -- A key pair `weft migrate connections` made on this install, the
        -- one connections move TO: the source seals them to its public
        -- half, and only this install opens them, with the private half
        -- kept here (any dispatcher or broker copy may take the batch).
        -- Used once, gone after its time (`migrate.rs`).
        CREATE TABLE IF NOT EXISTS access_transfer_key (
            id UUID PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            -- SEALED (crypt.rs).
            secret_sealed TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
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
            -- As its JSON text (`db::JsonText`), what the provider sent.
            captures_json TEXT NOT NULL DEFAULT '{}',
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
            -- As its JSON text (`db::JsonText`): part of a spec, which a
            -- node publishing a connection writes.
            events_json TEXT NOT NULL,
            -- The version `events_json` was written at
            -- (`weft_core::access::events::STORED_EVENTS`).
            events_json_version INTEGER NOT NULL,
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
        // Tell every process keeping a copy of a project's connections,
        // its install picks or its instances' values (a worker's opened
        // connections, the dispatcher's picks for a run's birth, the
        // broker's caller verifiers) that they changed, named so that every
        // process that may read the row hears it: a connection that is no
        // instance's own can serve any project of the tenant, so it is
        // announced as `tenant:<tenant>`; everything else as its project.
        // An update announces the row as it was and as it is, since a
        // rotate can move a connection from one owner to another.
        // SYNC: 'weft_access' and its payloads <-> weft_broker_client::line::ACCESS_CHANNEL, crates/weft-broker/src/line.rs (audience), crates/weft-broker/src/caller_auth.rs (names), crates/weft-dispatcher/src/held.rs (access_changed)
        r#"CREATE OR REPLACE FUNCTION access_notify() RETURNS trigger AS $$
            BEGIN
                IF TG_OP <> 'INSERT' THEN
                    -- Nested: a field only access_grant has is read only there.
                    IF TG_TABLE_NAME = 'access_grant' THEN
                        IF OLD.instance_id IS NULL THEN
                            PERFORM pg_notify('weft_access', 'tenant:' || OLD.tenant_id);
                        ELSE
                            PERFORM pg_notify('weft_access', OLD.project_id::text);
                        END IF;
                    ELSE
                        PERFORM pg_notify('weft_access', OLD.project_id::text);
                    END IF;
                END IF;
                IF TG_OP <> 'DELETE' THEN
                    -- Nested: a field only access_grant has is read only there.
                    IF TG_TABLE_NAME = 'access_grant' THEN
                        IF NEW.instance_id IS NULL THEN
                            PERFORM pg_notify('weft_access', 'tenant:' || NEW.tenant_id);
                        ELSE
                            PERFORM pg_notify('weft_access', NEW.project_id::text);
                        END IF;
                    ELSE
                        PERFORM pg_notify('weft_access', NEW.project_id::text);
                    END IF;
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql"#,
        r#"DROP TRIGGER IF EXISTS access_grant_notify ON access_grant"#,
        r#"CREATE TRIGGER access_grant_notify
            AFTER INSERT OR UPDATE OR DELETE ON access_grant
            FOR EACH ROW EXECUTE FUNCTION access_notify()"#,
        r#"DROP TRIGGER IF EXISTS install_pick_notify ON install_pick"#,
        r#"CREATE TRIGGER install_pick_notify
            AFTER INSERT OR UPDATE OR DELETE ON install_pick
            FOR EACH ROW EXECUTE FUNCTION access_notify()"#,
        r#"DROP TRIGGER IF EXISTS instance_value_notify ON instance_value"#,
        r#"CREATE TRIGGER instance_value_notify
            AFTER INSERT OR UPDATE OR DELETE ON instance_value
            FOR EACH ROW EXECUTE FUNCTION access_notify()"#,
    ],
    seed: &[],
    data_steps: &[EXCLUSIVE_FOLD],
};

/// A connection's spec, which an older weft kept in `spec_json` as `JSONB`,
/// copied into `spec` as its JSON text, at the version it was written at,
/// with whether it is exclusive: the fill before `access_grant.spec`'s
/// first step (`weft_runtime::schema::documents`), so the rows it copies
/// are carried to today's shape with the rest and their recipes hashed
/// again ([`rehash_grant_recipes`]). A data step would copy them only after
/// the carry. The rows there came out of `JSONB`, so none holds a NUL
/// character the cast would refuse. A database whose `spec_json` is gone
/// (a fresh one, one this ran on) has nothing to copy.
pub const SPEC_FILLS: &[weft_task_store::documents::Fill] = &[weft_task_store::documents::Fill {
    from: 1,
    sql: "DO $$
          BEGIN
              IF EXISTS (SELECT 1 FROM information_schema.columns
                          WHERE table_schema = current_schema() AND table_name = 'access_grant'
                            AND column_name = 'spec_json') THEN
                  UPDATE access_grant SET spec = spec_json::text, spec_version = spec_json_version,
                         exclusive = COALESCE(spec_json->>'grants' = 'exclusive', FALSE)
                   WHERE spec IS NULL;
              END IF;
          END
          $$",
}];

/// Before `access_grant_one_exclusive` is built on a database from before it,
/// the rows it would refuse are folded: two sign-ins to one exclusive
/// account that raced under older code each left a row, and a unique
/// index over them fails the boot, with rows nobody can mend by hand
/// (their values are sealed). First an author's row (no instance) that
/// older code stored with a project is made the tenant-wide one it is
/// (`project_id` NULL), which is what `flows::exclusive_row` looks up
/// now; then, per exclusive key, the row written last is kept, with every
/// permission any of them was granted, and everything naming a row that
/// goes (a pick, an instance's value, a connect or a chooser in flight, a
/// finished consent's outcome the editor has yet to collect, a provider
/// subscription, a signal acting as it) names the kept one before the
/// rest are deleted. An instance's value, a parked outcome and a signal's
/// spec name the connection inside their text, so the id is replaced
/// there as it is (a connection id is a uuid, found nowhere else in it).
/// Run twice, the second run finds no duplicate and changes nothing.
// SYNC: the exclusive key <-> access_grant_one_exclusive (the DDL above), crate::flows::exclusive_row
// SYNC: what names a grant <-> instance_value, install_pick, access_connect, access_picker, access_connect_result, signal_subscription (the DDL above), crates/weft-dispatcher/src/store/postgres.rs (signal.access_id)
const EXCLUSIVE_FOLD: weft_task_store::DataStep = weft_task_store::DataStep {
    table: "access_grant",
    until: weft_task_store::Until::Indexed("access_grant_one_exclusive"),
    work: weft_task_store::StepWork::Sql(r#"
        UPDATE access_grant SET project_id = NULL
         WHERE instance_id IS NULL AND project_id IS NOT NULL
           AND published_by_node IS NULL AND client_id IS NOT NULL AND exclusive;
        CREATE TEMP TABLE access_grant_fold AS
            SELECT id AS gone, first_value(id) OVER exclusive_key AS keep
              FROM access_grant
             WHERE published_by_node IS NULL AND client_id IS NOT NULL AND exclusive
            WINDOW exclusive_key AS (PARTITION BY tenant_id, service, identity, client_id, instance_id, project_id
                           ORDER BY updated_at DESC, id);
        DELETE FROM access_grant_fold WHERE gone = keep;
        UPDATE access_grant g SET granted_scopes = (
            SELECT COALESCE(jsonb_agg(DISTINCT scope ORDER BY scope), '[]'::jsonb)
              FROM access_grant a CROSS JOIN LATERAL jsonb_array_elements_text(a.granted_scopes) AS scope
             WHERE a.id = g.id OR a.id IN (SELECT gone FROM access_grant_fold WHERE keep = g.id))
         WHERE g.id IN (SELECT keep FROM access_grant_fold);
        UPDATE install_pick p SET grant_id = f.keep FROM access_grant_fold f WHERE p.grant_id = f.gone;
        UPDATE instance_value v SET grant_id = f.keep, value = replace(v.value, f.gone::text, f.keep::text)
          FROM access_grant_fold f WHERE v.grant_id = f.gone;
        UPDATE access_connect c SET upgrade_grant_id = f.keep FROM access_grant_fold f WHERE c.upgrade_grant_id = f.gone;
        UPDATE access_picker p SET access_id = f.keep FROM access_grant_fold f WHERE p.access_id = f.gone;
        UPDATE signal_subscription s SET access_id = f.keep FROM access_grant_fold f WHERE s.access_id = f.gone;
        UPDATE access_connect_result r SET result_json = replace(r.result_json, f.gone::text, f.keep::text)
          FROM access_grant_fold f WHERE strpos(r.result_json, f.gone::text) > 0;
        DO $$
        BEGIN
            -- The signal table is the dispatcher's group's, which a
            -- database holding only the access store's has not.
            IF to_regclass('signal') IS NOT NULL THEN
                UPDATE signal s SET access_id = f.keep::text, spec_json = replace(s.spec_json, f.gone::text, f.keep::text)
                  FROM access_grant_fold f WHERE s.access_id = f.gone::text;
            END IF;
        END
        $$;
        DELETE FROM access_grant WHERE id IN (SELECT gone FROM access_grant_fold);
        DROP TABLE access_grant_fold;
    "#),
};

/// A connection's `events_recipe_hash` is the hash of the recipe in its
/// own snapshot, which is what routes a push to it: hashed again from
/// each snapshot once the snapshots are in today's shape, so a connection
/// keeps matching the recipe [`rekey_events_recipes`] keeps it under.
/// What `access_grant.spec` re-derives once its documents are carried to
/// today's shape (`weft_runtime::schema::documents`).
pub fn rehash_grant_recipes<'c>(tx: &'c Transaction<'c>) -> Rederived<'c> {
    Box::pin(async move {
        let rows: Vec<(uuid::Uuid, JsonText<serde_json::Value>, weft_core::stored::Version, Option<String>)> = tx
            .all("SELECT id, spec, spec_version, events_recipe_hash FROM access_grant", &[])
            .await?;
        for (id, JsonText(spec), version, stored) in rows {
            let hash = resolve::events_recipe_hash(&spec_of(format_args!("connection {id}"), spec, version)?.events);
            if hash != stored {
                tx.execute("UPDATE access_grant SET events_recipe_hash = $2 WHERE id = $1", &[&id, &hash]).await?;
            }
        }
        Ok(())
    })
}

/// A recorded recipe is kept under the hash of its JSON: keyed again
/// once its JSON is in today's shape. Two recipes landing on one hash are
/// one recipe, kept once. What `service_events_recipe.events_json`
/// re-derives once its documents are carried to today's shape.
pub fn rekey_events_recipes<'c>(tx: &'c Transaction<'c>) -> Rederived<'c> {
    Box::pin(async move {
        let rows: Vec<(String, String, JsonText<serde_json::Value>, weft_core::stored::Version)> =
            tx.all("SELECT service, recipe_hash, events_json, events_json_version FROM service_events_recipe", &[]).await?;
        for (service, stored, JsonText(events), version) in rows {
            let events = weft_core::access::events::STORED_EVENTS
                .decode(events, version)
                .map_err(|e| e.at(format_args!("a recorded events recipe of '{service}' ({stored})")))?;
            let hash = resolve::events_recipe_hash(&events)
                .ok_or_else(|| anyhow::anyhow!("a recorded events recipe of '{service}' declares no topic"))?;
            if hash == stored {
                continue;
            }
            let taken: bool = tx
                .scalar("SELECT EXISTS (SELECT 1 FROM service_events_recipe WHERE service = $1 AND recipe_hash = $2)", &[&service, &hash])
                .await?;
            if taken {
                tx.execute("DELETE FROM service_events_recipe WHERE service = $1 AND recipe_hash = $2", &[&service, &stored]).await?;
            } else {
                tx.execute(
                    "UPDATE service_events_recipe SET recipe_hash = $3 WHERE service = $1 AND recipe_hash = $2",
                    &[&service, &stored, &hash],
                )
                .await?;
            }
        }
        Ok(())
    })
}

/// A grant row's spec snapshot (`access_grant.spec`), written at
/// `version`, as the typed recipe; `at` names the connection for a message.
pub(crate) fn spec_of(
    at: impl std::fmt::Display,
    spec_json: serde_json::Value,
    version: weft_core::stored::Version,
) -> anyhow::Result<weft_core::AccessSpec> {
    Ok(weft_core::AccessSpec::STORED.decode(spec_json, version).map_err(|e| e.at(at))?)
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
