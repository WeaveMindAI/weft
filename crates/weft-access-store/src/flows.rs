//! Connect-side flows: direct connects (paste / mint / server-to-server)
//! and the two halves of a browser OAuth consent. The app credentials a
//! flow needs arrive already resolved on the request (project-declared,
//! else the fallback provider); this module never looks an app up.

use std::collections::BTreeMap;

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::PgPool;

use weft_core::access::client::{authed_client, base_client, resolve_steps};
use weft_core::access::spec::{
    lookup_path, Acquisition, CredentialField, Door, GrantCoexistence, OAuthGrant, TestCall,
    VerificationRung,
};
// The connect wire shapes live in the core (weft-core `access::wire`),
// one definition for every client; this crate only executes them.
use weft_core::access::wire::{
    BeginOAuth, CompletedConnect, ConnectDirect, GrantSummary, PublishedConnection, StartedOAuth,
};
use weft_core::{AccessSpec, AppRegistration, CredentialOwner};

use crate::{door_of, door_str, owner_of, owner_str, scopes_of, AccessError};

/// How long a pending browser consent may take before its state nonce
/// goes stale. Generous (the user may read the consent page); short
/// enough that an abandoned row cannot be replayed much later.
const CONNECT_TTL: chrono::Duration = chrono::Duration::minutes(15);

// ---------- Grants: list / delete ----------

/// The tenant's connections, optionally narrowed to one service.
/// Summaries only; stored values never leave the store side.
pub async fn list_grants(
    pool: &PgPool,
    tenant: &str,
    service: Option<&str>,
) -> anyhow::Result<Vec<GrantSummary>> {
    #[allow(clippy::type_complexity)]
    let rows: Vec<(
        uuid::Uuid,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Value,
        bool,
        String,
        String,
        Option<chrono::DateTime<chrono::Utc>>,
        Vec<String>,
    )> = sqlx::query_as(
        // Only the value NAMES: the values themselves never leave the
        // store side, and the editor's check needs nothing more.
        "SELECT id, service, project_id, identity, label, granted_scopes,
                permissions_verified, owner, door, expires_at,
                ARRAY(SELECT jsonb_array_elements_text(value_names)) AS value_names
         FROM access_grant
         WHERE tenant_id = $1 AND ($2::text IS NULL OR service = $2)
         ORDER BY created_at",
    )
    .bind(tenant)
    .bind(service)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(
            |(
                id,
                service,
                project_id,
                identity,
                label,
                scopes,
                verified,
                owner,
                door,
                expires_at,
                value_names,
            )| {
                Ok(GrantSummary {
                    id,
                    service,
                    project_id,
                    identity,
                    label,
                    scopes: scopes_of(&scopes),
                    permissions_verified: verified,
                    owner: owner_of(&owner)?,
                    door: door_of(&door)?,
                    expires_at,
                    value_names,
                })
            },
        )
        .collect()
}

/// Delete pending-connect and parked-result rows past the TTL. The TTL
/// was only ever checked on read, so an abandoned consent and an
/// unpolled result would otherwise live forever; the periodic sweep
/// (mirroring the broker's expiry sweeps) is what actually reclaims
/// them. Returns how many rows died, for the sweep's log line.
pub async fn sweep_expired_connects(pool: &PgPool) -> anyhow::Result<u64> {
    let cutoff = chrono::Utc::now() - CONNECT_TTL;
    let mut swept = 0;
    for table in ["access_connect", "access_connect_result", "access_picker"] {
        swept += sqlx::query(&format!("DELETE FROM {table} WHERE created_at < $1"))
            .bind(cutoff)
            .execute(pool)
            .await?
            .rows_affected();
    }
    Ok(swept)
}

pub async fn delete_grant(pool: &PgPool, tenant: &str, id: uuid::Uuid) -> anyhow::Result<()> {
    let done = sqlx::query("DELETE FROM access_grant WHERE tenant_id = $1 AND id = $2")
        .bind(tenant)
        .bind(id)
        .execute(pool)
        .await?;
    if done.rows_affected() == 0 {
        return Err(AccessError::NotFound.into());
    }
    Ok(())
}

// ---------- Direct connects (no browser consent) ----------

/// The provider redirect's payload for [`complete_oauth`], as it
/// travels dispatcher (public callback door) -> broker (which makes
/// the exchange). No tenant wrapper: the parked state nonce is what
/// authenticates the exchange.
#[derive(Debug, Serialize, Deserialize)]
pub struct OAuthComplete {
    pub state: String,
    pub code: String,
}

/// The resolved app for a flow that needs one, or a loud error naming
/// the fix (the app is declared in the project's `accessApps`, or the
/// fallback provider supplies it). Never reached when the editor gated
/// the Connect button on the app being configured; this is the backstop.
fn require_registration<'a>(
    registration: &'a Option<AppRegistration>,
    spec: &AccessSpec,
) -> Result<&'a AppRegistration, AccessError> {
    let reg = registration.as_ref().ok_or_else(|| {
        AccessError::Invalid(format!(
            "no app is configured for '{}'; declare it under `accessApps` in the \
             project (or a package root) before connecting",
            spec.service
        ))
    })?;
    // A half-filled app fails HERE with a clear message, not at the
    // token call: every extra the acquisition declares must be present.
    reg.validate(declared_registration_fields(spec)).map_err(AccessError::Invalid)?;
    Ok(reg)
}

/// The registration extras the spec's acquisition declares (empty for a
/// non-OAuth acquisition). The one derivation, shared by the fresh-app
/// validation and the upgrade path's re-validation of a row's app.
fn declared_registration_fields(spec: &AccessSpec) -> &[CredentialField] {
    match &spec.acquisition {
        Acquisition::OAuth2 { registration_fields, .. } => registration_fields.as_slice(),
        _ => &[],
    }
}

/// The credential snapshot for a grant / pending-connect row: the app as
/// JSON, or SQL NULL for a service that uses no app.
fn registration_snapshot(
    registration: &Option<AppRegistration>,
) -> anyhow::Result<Option<String>> {
    registration
        .as_ref()
        .map(|r| crate::seal_json(&serde_json::to_value(r)?))
        .transpose()
}

pub async fn connect_direct(
    pool: &PgPool,
    tenant: &str,
    req: ConnectDirect,
) -> anyhow::Result<CompletedConnect> {
    // A paste connect swaps in the declared static variant BEFORE
    // anything else: everything downstream (validation, field checks,
    // test call, snapshot) then treats it exactly as a natively
    // static service, which is the point.
    let spec = &if req.paste {
        req.spec.validate().map_err(AccessError::Invalid)?;
        req.spec.paste_variant().ok_or_else(|| {
            AccessError::Invalid(format!(
                "the '{}' service declares no paste section",
                req.spec.service
            ))
        })?
    } else {
        req.spec.clone()
    };
    spec.validate().map_err(AccessError::Invalid)?;
    crate::resolve::record_events_recipes(pool, spec).await?;
    for p in &req.permissions {
        if !spec.declares_permission(p) {
            return Err(AccessError::Invalid(format!(
                "'{p}' is not in the '{}' permission catalogue",
                spec.service
            ))
            .into());
        }
    }
    if !spec.doors.contains(&req.door) {
        return Err(AccessError::Invalid(format!(
            "the '{}' service does not offer that connect door",
            spec.service
        ))
        .into());
    }

    // The SHARED door of a non-consent service: nothing is stored and
    // nothing can be validated; the row only marks that the user picked
    // the runtime's credential, and resolution asks the credential
    // source per call. (A consent service's shared door goes through
    // begin_oauth with the registered app instead.)
    if req.door == Door::Shared && !spec.is_oauth() {
        let mut stored = spec.clone();
        stored.acquisition = Acquisition::Runtime {};
        return insert_grant(pool, tenant, NewGrant {
            spec: &stored,
            registration: &None,
            project_id: req.project_id,
            published_by_node: None,
            values: BTreeMap::new(),
            granted: Vec::new(),
            verified: false,
            owner: CredentialOwner::Ours,
            door: Door::Shared,
            label: None,
            identity: None,
            expires_at: None,
        })
        .await;
    }

    let mut values = req.values;
    let mut expires_at: Option<chrono::DateTime<chrono::Utc>> = None;
    match &spec.acquisition {
        Acquisition::Static { fields } => {
            values = storable_values(spec, fields, values)?;
        }
        Acquisition::MintJwt { fields, .. } => {
            // Held to the same rule as any other pasted set: these are
            // the fields a PERSON filled in, and a missing or blank one
            // would otherwise fail somewhere inside the mint instead of
            // naming itself here.
            values = storable_values(spec, fields, values)?;
            // Mint + exchange once now, so a bad key/id fails at
            // connect instead of at first run. The minted token is
            // stored like any other value and re-minted lazily on
            // expiry at resolution time.
            expires_at = crate::resolve::mint_and_exchange(spec, &mut values).await?;
        }
        Acquisition::OAuth2 { grant: OAuthGrant::ClientCredentials, .. } => {
            let reg = require_registration(&req.registration, spec)?;
            expires_at =
                crate::resolve::client_credentials_token(spec, &reg.to_values(), &mut values)
                    .await?;
        }
        Acquisition::OAuth2 { grant: OAuthGrant::AuthorizationCode { .. }, .. } => {
            return Err(AccessError::Invalid(
                "this service connects through a browser consent; use the Connect button".into(),
            )
            .into());
        }
        Acquisition::Runtime {} => {
            // spec.validate() refused an authored Runtime acquisition
            // above; this arm is unreachable and says so loudly.
            return Err(anyhow::anyhow!("a runtime acquisition never reaches connect_direct"));
        }
    }
    // A verification whose check may bill is NEVER auto-run: the
    // credential is recorded as claimed and the first real call
    // surfaces the truth.
    if spec.verification.cost.may_auto_run() {
        run_test_call(spec.test.as_ref(), &spec.auth, &mut values).await?;
    }
    let identity = resolve_identity(spec, &values)?;
    // What the row records as granted: the introspected set when the
    // service's verification produced one (the `granted_permissions`
    // capture), else the user's claimed ticks.
    let (granted, verified) = recorded_permissions(
        spec.verification.rung,
        introspected_permissions(&values),
        req.permissions,
    );
    // A pasted secret is not display text: whichever set is recorded,
    // the reserved capture never stays in the stored values.
    values.remove(GRANTED_PERMISSIONS_CAPTURE);
    let label = match req.label {
        Some(l) if !l.trim().is_empty() => Some(l.trim().to_string()),
        _ => req.registration.as_ref().map(|r| r.label.clone()),
    };
    insert_grant(pool, tenant, NewGrant {
        spec,
        registration: &req.registration,
        project_id: req.project_id,
        published_by_node: None,
        values,
        granted,
        verified,
        owner: CredentialOwner::TheirOwn,
        door: req.door,
        label,
        identity,
        expires_at,
    })
    .await
}

/// The reserved capture name of a SELF-INTROSPECTING verification: a
/// test call capturing the credential's own permission list under this
/// name records it as the VERIFIED granted set (a JSON array, or a
/// space/comma-joined string).
pub const GRANTED_PERMISSIONS_CAPTURE: &str = "granted_permissions";

fn introspected_permissions(values: &BTreeMap<String, String>) -> Option<Vec<String>> {
    let raw = values.get(GRANTED_PERMISSIONS_CAPTURE)?;
    if let Ok(Value::Array(items)) = serde_json::from_str::<Value>(raw) {
        return Some(
            items.iter().filter_map(|v| v.as_str().map(str::to_string)).collect(),
        );
    }
    Some(
        raw.split([' ', ','])
            .filter(|p| !p.is_empty())
            .map(str::to_string)
            .collect(),
    )
}

/// The permission-recording decision, pure: what lands on the row and
/// whether it is authoritative. Only an authoritative rung with a
/// provider-produced set records VERIFIED; everything else records the
/// claimed ticks, and a later shortfall on a claimed set never
/// hard-fails (nobody actually knows).
pub fn recorded_permissions(
    rung: VerificationRung,
    provider_reported: Option<Vec<String>>,
    claimed: Vec<String>,
) -> (Vec<String>, bool) {
    match provider_reported {
        Some(reported) if rung.is_authoritative() => (reported, true),
        _ => (claimed, false),
    }
}

/// The provider's identifier for the account a connection belongs to,
/// read from the stored values at the name the service's events
/// recipe declares. `None` for a service that reports no events, or
/// one whose connect captured nothing under that name.
///
/// ONE derivation, called by every write path: an inbound event finds
/// its connections through this column alone, so a path that forgot
/// to fill it would produce a connection that silently never fires.
pub(crate) fn provider_account_of(
    spec: &AccessSpec,
    values: &BTreeMap<String, String>,
) -> Option<String> {
    // Every topic names the SAME stored value (validated on the
    // spec), so any topic answers for the service.
    let events = spec.events.values().next()?;
    values.get(&events.account.value).cloned()
}

/// The plain value-NAMES column written beside every sealed values
/// map, so the editor's list never needs the store to open a row.
pub(crate) fn value_names_of(values: &BTreeMap<String, String>) -> Value {
    Value::Array(values.keys().map(|k| Value::String(k.clone())).collect())
}

/// Everything one grant INSERT needs. The single write path for every
/// way a connection comes into being (a person pasting one, a browser
/// consent finishing, and a node publishing one for something it
/// runs), so the column list exists once and cannot drift between
/// them. A re-consent that rotates an EXISTING row is the one write
/// that is not an insert, and updates in place.
struct NewGrant<'a> {
    spec: &'a AccessSpec,
    registration: &'a Option<AppRegistration>,
    project_id: Option<String>,
    /// Set only by [`publish_grant`]; a person's connect leaves it None.
    published_by_node: Option<String>,
    values: BTreeMap<String, String>,
    granted: Vec<String>,
    verified: bool,
    owner: CredentialOwner,
    door: Door,
    label: Option<String>,
    identity: Option<String>,
    expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// `values` reduced to what may actually be STORED for `spec`, or the
/// reason they cannot be: every required field present and non-blank,
/// no name the service does not declare, the service's all-or-nothing
/// groups satisfied, and blanks dropped so a later read sees "missing"
/// rather than an empty secret.
///
/// The gate for every set of values a PERSON or a NODE supplied: a
/// pasted connection, a minted one, and one a node published all
/// answer the same questions and are held to the same standard. A
/// browser consent is the one that does not come through here, and
/// cannot: its values are the provider's own answer to the token
/// exchange, not fields anybody filled in.
fn storable_values(
    spec: &AccessSpec,
    fields: &[CredentialField],
    mut values: BTreeMap<String, String>,
) -> anyhow::Result<BTreeMap<String, String>> {
    for f in fields {
        let empty = values.get(&f.name).is_none_or(|v| v.trim().is_empty());
        if empty && !f.optional {
            return Err(AccessError::Invalid(format!("field '{}' is required", f.name)).into());
        }
    }
    if let Some(unknown) = values.keys().find(|k| !fields.iter().any(|f| &&f.name == k)) {
        return Err(AccessError::Invalid(format!(
            "the '{}' service declares no value named '{unknown}'",
            spec.service
        ))
        .into());
    }
    weft_core::access::spec::capability_shortfall(&spec.capabilities, |name| {
        values.get(name).is_some_and(|v| !v.trim().is_empty())
    })
    .map_err(AccessError::Invalid)?;
    values.retain(|_, v| !v.trim().is_empty());
    Ok(values)
}

async fn insert_grant(
    pool: &PgPool,
    tenant: &str,
    grant: NewGrant<'_>,
) -> anyhow::Result<CompletedConnect> {
    // A node's published connection REPLACES the one it published
    // before (its key is the node, not the moment), so the same write
    // serves a first publish and a re-publish. A person's connect has
    // no such key and always inserts. One statement either way, so the
    // column list exists once.
    //
    // EVERY non-key column is written, not a chosen subset. The row
    // becomes exactly what was just built, so a column added to the
    // INSERT above can never be quietly left stale on a re-publish
    // (which nothing would catch: the answer returned to the caller
    // reports the new value while the row keeps the old one).
    let upsert = if grant.published_by_node.is_some() {
        " ON CONFLICT (tenant_id, project_id, published_by_node, service)
            WHERE published_by_node IS NOT NULL
          DO UPDATE SET
             registration_sealed = EXCLUDED.registration_sealed,
             client_id = EXCLUDED.client_id,
             spec_json = EXCLUDED.spec_json,
             events_recipe_hash = EXCLUDED.events_recipe_hash,
             values_sealed = EXCLUDED.values_sealed,
             value_names = EXCLUDED.value_names,
             granted_scopes = EXCLUDED.granted_scopes,
             permissions_verified = EXCLUDED.permissions_verified,
             owner = EXCLUDED.owner,
             door = EXCLUDED.door,
             label = EXCLUDED.label,
             identity = EXCLUDED.identity,
             provider_account = EXCLUDED.provider_account,
             expires_at = EXCLUDED.expires_at,
             updated_at = now()"
    } else {
        ""
    };
    let id: uuid::Uuid = sqlx::query_scalar(&format!(
        "INSERT INTO access_grant
           (id, tenant_id, service, registration_sealed, client_id, project_id, spec_json,
            events_recipe_hash, values_sealed, value_names, granted_scopes,
            permissions_verified, owner, door, label, identity, provider_account, expires_at,
            published_by_node)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
                 $18, $19){upsert}
         RETURNING id"
    ))
    .bind(uuid::Uuid::new_v4())
    .bind(tenant)
    .bind(&grant.spec.service)
    .bind(registration_snapshot(grant.registration)?)
    .bind(grant.registration.as_ref().map(|r| r.client_id.clone()))
    .bind(&grant.project_id)
    .bind(serde_json::to_value(grant.spec)?)
    .bind(crate::resolve::events_recipe_hash(&grant.spec.events))
    .bind(crate::seal_json(&serde_json::to_value(&grant.values)?)?)
    .bind(value_names_of(&grant.values))
    .bind(serde_json::to_value(&grant.granted)?)
    .bind(grant.verified)
    .bind(owner_str(grant.owner))
    .bind(door_str(grant.door))
    .bind(&grant.label)
    .bind(&grant.identity)
    .bind(provider_account_of(grant.spec, &grant.values))
    .bind(grant.expires_at)
    .bind(&grant.published_by_node)
    .fetch_one(pool)
    .await?;
    Ok(CompletedConnect {
        grant: GrantSummary {
            id,
            service: grant.spec.service.clone(),
            project_id: grant.project_id,
            identity: grant.identity,
            label: grant.label,
            scopes: grant.granted,
            permissions_verified: grant.verified,
            owner: grant.owner,
            door: grant.door,
            expires_at: grant.expires_at,
            value_names: grant.values.keys().cloned().collect(),
        },
    })
}

// ---------- Resource picker sessions ----------

/// Start a picker session: park the node-declared chooser (script +
/// glue) with the connection it picks against. The editor opens the
/// weft-served picker page for the returned state in the user's
/// browser and polls the parked outcome exactly like a consent's.
// SYNC: BeginPicker <-> packages/weft-graph/src/webview/lib/components/project/RemoteSelectField.svelte picker/begin body
#[derive(Debug, Serialize, Deserialize)]
pub struct BeginPicker {
    pub access_id: uuid::Uuid,
    pub service: String,
    /// The chooser script's https address, from the node's declared
    /// picker source.
    pub script: String,
    /// The node author's glue, run on the picker page.
    pub code: String,
    #[serde(default)]
    pub mime_types: Vec<String>,
    /// The permissions choosing a resource GRANTS (the node's declared
    /// `grants`); a finished pick unions them into the connection's
    /// granted scopes.
    #[serde(default)]
    pub grants: Vec<String>,
}

/// A parked picker session, as the picker page needs it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PickerSession {
    pub tenant: String,
    pub access_id: uuid::Uuid,
    pub service: String,
    pub script: String,
    pub code: String,
    pub mime_types: Vec<String>,
}

pub async fn begin_picker(
    pool: &PgPool,
    tenant: &str,
    req: BeginPicker,
) -> anyhow::Result<String> {
    if !req.script.starts_with("https://") {
        return Err(AccessError::Invalid(format!(
            "a picker's script must be an https:// address, got '{}'",
            req.script
        ))
        .into());
    }
    if req.code.trim().is_empty() {
        return Err(AccessError::Invalid("a picker needs its glue code".into()).into());
    }
    let state = uuid::Uuid::new_v4().simple().to_string();
    sqlx::query(
        "INSERT INTO access_picker
             (state, tenant_id, access_id, service, script, code, mime_types, grants)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(&state)
    .bind(tenant)
    .bind(req.access_id)
    .bind(&req.service)
    .bind(&req.script)
    .bind(&req.code)
    .bind(serde_json::to_value(&req.mime_types)?)
    .bind(serde_json::to_value(&req.grants)?)
    .execute(pool)
    .await?;
    Ok(state)
}

/// The picker page's read: NOT consumed (the page may reload), but
/// dead past the TTL like a pending consent.
pub async fn load_picker(pool: &PgPool, state: &str) -> anyhow::Result<PickerSession> {
    #[allow(clippy::type_complexity)]
    let row: Option<(String, uuid::Uuid, String, String, String, Value, chrono::DateTime<chrono::Utc>)> =
        sqlx::query_as(
            "SELECT tenant_id, access_id, service, script, code, mime_types, created_at
             FROM access_picker WHERE state = $1",
        )
        .bind(state)
        .fetch_optional(pool)
        .await?;
    let Some((tenant, access_id, service, script, code, mime_types, created_at)) = row else {
        return Err(AccessError::Invalid(
            "this picker link is unknown or expired; open the picker again from the editor"
                .into(),
        )
        .into());
    };
    if chrono::Utc::now() - created_at > CONNECT_TTL {
        return Err(AccessError::Invalid(
            "this picker link expired; open the picker again from the editor".into(),
        )
        .into());
    }
    Ok(PickerSession {
        tenant,
        access_id,
        service,
        script,
        code,
        mime_types: scopes_of(&mime_types),
    })
}

/// The picker page posted its outcome: claim the session (single use)
/// and park the result for the editor's poll, under the session's
/// tenant. `result_json` is `{"picked": {"id", "label"}}` or
/// `{"error": "..."}`, written by the weft-served page. A successful
/// pick also lands the session's parked `grants` on the connection:
/// they union into the grant row's `granted_scopes` (the provider
/// granted them by the pick itself), so the editor's shortfall check
/// sees them; `permissions_verified` is untouched.
pub async fn finish_picker(
    pool: &PgPool,
    state: &str,
    result_json: Value,
) -> anyhow::Result<()> {
    let row: Option<(String, uuid::Uuid, Value)> = sqlx::query_as(
        "DELETE FROM access_picker WHERE state = $1
         RETURNING tenant_id, access_id, grants",
    )
    .bind(state)
    .fetch_optional(pool)
    .await?;
    let Some((tenant, access_id, grants)) = row else {
        return Err(AccessError::Invalid(
            "this picker link is unknown or was already used".into(),
        )
        .into());
    };
    let granted = scopes_of(&grants);
    if result_json.get("picked").is_some() && !granted.is_empty() {
        sqlx::query(
            "UPDATE access_grant
             SET granted_scopes = (
                     SELECT COALESCE(jsonb_agg(to_jsonb(s) ORDER BY s), '[]'::jsonb)
                     FROM (
                         SELECT jsonb_array_elements_text(granted_scopes) AS s
                         UNION
                         SELECT unnest($3::text[])
                     ) merged
                 ),
                 updated_at = now()
             WHERE tenant_id = $1 AND id = $2",
        )
        .bind(&tenant)
        .bind(access_id)
        .bind(&granted)
        .execute(pool)
        .await?;
    }
    sqlx::query(
        "INSERT INTO access_connect_result (state, tenant_id, result_json)
         VALUES ($1, $2, $3)
         ON CONFLICT (state) DO NOTHING",
    )
    .bind(state)
    .bind(&tenant)
    .bind(result_json)
    .execute(pool)
    .await?;
    Ok(())
}

// ---------- Browser OAuth: begin ----------

/// Park a pending consent and build the consent URL. Requires the
/// resolved app (`req.registration`) and the callback URL
/// (`req.redirect_uri`), both filled before this is reached.
pub async fn begin_oauth(
    pool: &PgPool,
    tenant: &str,
    req: BeginOAuth,
) -> anyhow::Result<StartedOAuth> {
    let redirect_uri = req.redirect_uri.as_str();
    if redirect_uri.trim().is_empty() {
        return Err(anyhow::anyhow!("begin_oauth reached with no redirect_uri"));
    }
    let spec = &req.spec;
    spec.validate().map_err(AccessError::Invalid)?;
    crate::resolve::record_events_recipes(pool, spec).await?;
    let Acquisition::OAuth2 {
        grant: OAuthGrant::AuthorizationCode { auth_url, pkce },
        scope_delimiter,
        auth_params,
        ..
    } = &spec.acquisition
    else {
        return Err(AccessError::Invalid(
            "this service does not use a browser consent".into(),
        )
        .into());
    };
    if !spec.doors.contains(&req.door) {
        return Err(AccessError::Invalid(format!(
            "the '{}' service does not offer that connect door",
            spec.service
        ))
        .into());
    }
    for p in &req.permissions {
        if !spec.declares_permission(p) {
            return Err(AccessError::Invalid(format!(
                "'{p}' is not in the '{}' permission catalogue",
                spec.service
            ))
            .into());
        }
    }
    if req.upgrade_grant_id.is_some() && spec.grants != GrantCoexistence::Exclusive {
        return Err(AccessError::Invalid(
            "only an exclusive-class grant upgrades in place; reconnect instead".into(),
        )
        .into());
    }
    // An in-place upgrade reuses the ROW's app: the grant row already
    // knows which registered app it belongs to, and re-deriving the app
    // from the request (an empty editor form falls back to the
    // project's default app) would silently reassign the grant to a
    // different app on token rotation. The lookup is guarded by SERVICE
    // too: an upgrade id naming a grant of a different service would
    // otherwise park that service's app credentials into this consent
    // and overwrite the row with the wrong provider's tokens.
    let row_registration: Option<AppRegistration> = match req.upgrade_grant_id {
        Some(id) => {
            let row: Option<(Option<String>,)> = sqlx::query_as(
                "SELECT registration_sealed FROM access_grant \
                 WHERE id = $1 AND tenant_id = $2 AND service = $3",
            )
            .bind(id)
            .bind(tenant)
            .bind(&spec.service)
            .fetch_optional(pool)
            .await?;
            let Some((sealed,)) = row else {
                return Err(AccessError::NotFound.into());
            };
            let Some(sealed) = sealed else {
                return Err(AccessError::Invalid(
                    "this connection was made without an app; it cannot be upgraded \
                     through a browser consent"
                        .into(),
                )
                .into());
            };
            let reg: AppRegistration = serde_json::from_value(crate::open_json(&sealed)?)
                .map_err(|e| anyhow::anyhow!("grant row has a malformed app: {e}"))?;
            // The row's app must still satisfy the spec (a row sealed
            // under an older spec missing a now-declared field fails
            // HERE with the field's name, not at the token call).
            reg.validate(declared_registration_fields(spec)).map_err(AccessError::Invalid)?;
            // The request may carry an app too (the editor resolved
            // one); a DIFFERENT app is a contradiction, refused loudly
            // rather than silently preferring either side.
            if let Some(requested) = &req.registration {
                if requested.client_id != reg.client_id {
                    return Err(AccessError::Invalid(
                        "this connection was made through a different app; forget it and \
                         connect again to switch apps"
                            .into(),
                    )
                    .into());
                }
            }
            Some(reg)
        }
        None => None,
    };
    let registration = match &row_registration {
        Some(r) => r,
        None => require_registration(&req.registration, spec)?,
    };
    let client_id = registration.client_id.clone();

    let state = uuid::Uuid::new_v4().simple().to_string();
    let verifier = pkce.then(|| {
        format!("{}{}", uuid::Uuid::new_v4().simple(), uuid::Uuid::new_v4().simple())
    });

    let mut url: url::Url = auth_url
        .parse()
        .map_err(|e| AccessError::Invalid(format!("bad auth_url '{auth_url}': {e}")))?;
    {
        let mut q = url.query_pairs_mut();
        q.append_pair("response_type", "code");
        q.append_pair("client_id", &client_id);
        q.append_pair("redirect_uri", redirect_uri);
        q.append_pair("state", &state);
        if !req.permissions.is_empty() {
            let delim = scope_delimiter.as_deref().unwrap_or(" ");
            q.append_pair("scope", &req.permissions.join(delim));
        }
        for (k, v) in auth_params {
            q.append_pair(k, v);
        }
        if let Some(verifier) = &verifier {
            let challenge = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(Sha256::digest(verifier.as_bytes()));
            q.append_pair("code_challenge", &challenge);
            q.append_pair("code_challenge_method", "S256");
        }
    }

    sqlx::query(
        "INSERT INTO access_connect
           (state, tenant_id, service, registration_sealed, project_id, spec_json, scopes,
            pkce_verifier, door, upgrade_grant_id, redirect_uri)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
    )
    .bind(&state)
    .bind(tenant)
    .bind(&spec.service)
    .bind(crate::seal_json(&serde_json::to_value(registration)?)?)
    .bind(&req.project_id)
    .bind(serde_json::to_value(spec)?)
    .bind(serde_json::to_value(&req.permissions)?)
    .bind(verifier.as_deref().map(crate::seal_str))
    .bind(door_str(req.door))
    .bind(req.upgrade_grant_id)
    .bind(redirect_uri)
    .execute(pool)
    .await?;

    Ok(StartedOAuth { consent_url: url.to_string(), state })
}

// ---------- Browser OAuth: complete (the callback door's body) ----------

/// Complete a parked consent: validate the state nonce, exchange the
/// code, run captures + the test call, enforce ticked ⊆ granted, and
/// write the grant (a new row, or an in-place rotation for an
/// exclusive-class upgrade/re-consent). The outcome (success OR
/// failure) is also parked in `access_connect_result` for the editor's
/// poll; an unknown/expired state has nothing to park.
pub async fn complete_oauth(
    pool: &PgPool,
    state: &str,
    code: &str,
) -> anyhow::Result<CompletedConnect> {
    let (tenant, outcome) = complete_oauth_inner(pool, state, code).await?;
    let result_json = match &outcome {
        Ok(done) => serde_json::json!({ "grant": done.grant }),
        Err(e) => serde_json::json!({ "error": e.to_string() }),
    };
    sqlx::query(
        "INSERT INTO access_connect_result (state, tenant_id, result_json)
         VALUES ($1, $2, $3)
         ON CONFLICT (state) DO NOTHING",
    )
    .bind(state)
    .bind(&tenant)
    .bind(result_json)
    .execute(pool)
    .await?;
    outcome
}

/// The editor's poll: consume the parked outcome for `state`, walled
/// to the polling tenant. `None` = the consent has not landed yet.
/// The returned JSON is `{"grant": {...}}` or `{"error": "..."}`.
pub async fn take_connect_result(
    pool: &PgPool,
    tenant: &str,
    state: &str,
) -> anyhow::Result<Option<Value>> {
    let row: Option<(Value,)> = sqlx::query_as(
        "DELETE FROM access_connect_result WHERE state = $1 AND tenant_id = $2
         RETURNING result_json",
    )
    .bind(state)
    .bind(tenant)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(v,)| v))
}

/// A claimed pending-connect row, mid-completion.
struct ClaimedConnect {
    tenant: String,
    service: String,
    registration: AppRegistration,
    project_id: Option<String>,
    spec_json: Value,
    ticked_json: Value,
    verifier: Option<String>,
    door: Door,
    upgrade_grant_id: Option<uuid::Uuid>,
    redirect_uri: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

/// The claim + exchange + grant write, with the claiming row's tenant
/// pulled out so the wrapper can park the outcome under it. An
/// unknown state is the ONE unparkable failure (nothing names a
/// tenant), so only that propagates as the outer `Err`.
async fn complete_oauth_inner(
    pool: &PgPool,
    state: &str,
    code: &str,
) -> anyhow::Result<(String, anyhow::Result<CompletedConnect>)> {
    // Claim the pending row (single use: DELETE .. RETURNING).
    #[allow(clippy::type_complexity)]
    let row: Option<(
        String,
        String,
        String,
        Option<String>,
        Value,
        Value,
        Option<String>,
        String,
        Option<uuid::Uuid>,
        String,
        chrono::DateTime<chrono::Utc>,
    )> = sqlx::query_as(
        "DELETE FROM access_connect WHERE state = $1
         RETURNING tenant_id, service, registration_sealed, project_id, spec_json, scopes,
                   pkce_verifier, door, upgrade_grant_id, redirect_uri, created_at",
    )
    .bind(state)
    .fetch_optional(pool)
    .await?;
    let Some((
        tenant,
        service,
        registration_sealed,
        project_id,
        spec_json,
        ticked_json,
        verifier,
        door,
        upgrade_grant_id,
        redirect_uri,
        created_at,
    )) = row
    else {
        return Err(AccessError::Invalid(
            "this sign-in link is unknown or was already used; start the connect again".into(),
        )
        .into());
    };
    let registration: AppRegistration = serde_json::from_value(crate::open_json(
        &registration_sealed,
    )?)
    .map_err(|e| anyhow::anyhow!("pending connect row has a malformed app: {e}"))?;
    let claimed = ClaimedConnect {
        tenant: tenant.clone(),
        service,
        registration,
        project_id,
        spec_json,
        ticked_json,
        verifier: verifier.as_deref().map(crate::open_str).transpose()?,
        door: door_of(&door)?,
        upgrade_grant_id,
        redirect_uri,
        created_at,
    };
    Ok((tenant, finish_connect(pool, claimed, code).await))
}

async fn finish_connect(
    pool: &PgPool,
    claimed: ClaimedConnect,
    code: &str,
) -> anyhow::Result<CompletedConnect> {
    let ClaimedConnect {
        tenant,
        service,
        registration,
        project_id,
        spec_json,
        ticked_json,
        verifier,
        door,
        upgrade_grant_id,
        redirect_uri,
        created_at,
    } = claimed;
    if chrono::Utc::now() - created_at > CONNECT_TTL {
        return Err(AccessError::Invalid(
            "this sign-in link expired; start the connect again".into(),
        )
        .into());
    }
    let spec = crate::spec_of(&spec_json)?;
    let ticked = scopes_of(&ticked_json);
    let Acquisition::OAuth2 { token_url, captures, token_auth, .. } = &spec.acquisition else {
        return Err(anyhow::anyhow!("pending connect row is not an oauth2 spec"));
    };

    let reg = registration.to_values();
    let mut params: Vec<(String, String)> = vec![
        ("grant_type".into(), "authorization_code".into()),
        ("code".into(), code.to_string()),
        ("redirect_uri".into(), redirect_uri),
    ];
    let basic = place_client_auth(*token_auth, &reg, &mut params);
    if let Some(verifier) = verifier {
        params.push(("code_verifier".into(), verifier));
    }
    let resp = token_request(token_url, &params, basic).await?;

    let mut values: BTreeMap<String, String> = BTreeMap::new();
    let token = resp
        .get("access_token")
        .and_then(Value::as_str)
        .ok_or_else(|| AccessError::Invalid("token response carries no access_token".into()))?;
    values.insert("token".into(), token.to_string());
    if let Some(rt) = resp.get("refresh_token").and_then(Value::as_str) {
        values.insert("refresh_token".into(), rt.to_string());
    }
    let expires_at = expires_at_of(&resp);
    apply_captures(captures, &resp, &mut values)?;

    // The provider's granted-scope echo, when it answers with one.
    let echo: Option<Vec<String>> = resp
        .get("scope")
        .and_then(Value::as_str)
        .map(|s| {
            s.split([' ', ','])
                .filter(|p| !p.is_empty())
                .map(str::to_string)
                .collect()
        })
        .filter(|v: &Vec<String>| !v.is_empty());
    if let Some(echoed) = &echo {
        if let Some(missing) = ticked.iter().find(|t| !echoed.contains(t)) {
            return Err(AccessError::Invalid(format!(
                "the provider granted fewer permissions than requested (missing \
                 '{missing}'); check the app's allowed permissions on the provider's site"
            ))
            .into());
        }
    }
    let (granted, verified) =
        recorded_permissions(spec.verification.rung, echo, ticked.clone());

    if spec.verification.cost.may_auto_run() {
        run_test_call(spec.test.as_ref(), &spec.auth, &mut values).await?;
    }
    let identity = resolve_identity(&spec, &values)?;
    let label = Some(registration.label.clone());

    // Where the grant lands:
    // - explicit upgrade: rotate that row (scope union recorded below);
    // - exclusive class: rotate the tenant's existing grant for the
    //   SAME app + account if there is one, because the provider
    //   structurally rotated it under us anyway; else a fresh shared
    //   row. Exclusivity is per app + workspace, so the app must match
    //   too: a row made through a DIFFERENT app (another registered
    //   tier, or a pasted hand-made bot with no app at all) holds its
    //   own untouched token and must never be absorbed;
    // - coexisting class: always a fresh per-project row.
    let rotate_id: Option<uuid::Uuid> = match upgrade_grant_id {
        Some(id) => Some(id),
        None if spec.grants == GrantCoexistence::Exclusive => {
            let existing: Option<(uuid::Uuid,)> = sqlx::query_as(
                "SELECT id FROM access_grant
                 WHERE tenant_id = $1 AND service = $2 AND identity IS NOT DISTINCT FROM $3
                   AND client_id = $4",
            )
            .bind(&tenant)
            .bind(&service)
            .bind(&identity)
            .bind(&registration.client_id)
            .fetch_optional(pool)
            .await?;
            existing.map(|(id,)| id)
        }
        None => None,
    };

    let (grant_id, project_id, recorded_scopes) = match rotate_id {
        Some(id) => {
            // Union of scopes: an exclusive grant only ever grows, and
            // every referencing project follows the row.
            // Guarded by service like the begin-side lookup: the rotate
            // must never land on a row of a different provider.
            let prior: Option<(Value,)> = sqlx::query_as(
                "SELECT granted_scopes FROM access_grant \
                 WHERE id = $1 AND tenant_id = $2 AND service = $3",
            )
            .bind(id)
            .bind(&tenant)
            .bind(&service)
            .fetch_optional(pool)
            .await?;
            let Some((prior_scopes,)) = prior else {
                return Err(AccessError::NotFound.into());
            };
            let mut union = scopes_of(&prior_scopes);
            for s in &granted {
                if !union.contains(s) {
                    union.push(s.clone());
                }
            }
            sqlx::query(
                "UPDATE access_grant
                 SET values_sealed = $3, value_names = $4, granted_scopes = $5,
                     permissions_verified = $6, identity = $7, label = $8, door = $9,
                     expires_at = $10, spec_json = $11, registration_sealed = $12,
                     client_id = $13, provider_account = $14, events_recipe_hash = $15,
                     project_id = NULL, updated_at = now()
                 WHERE id = $1 AND tenant_id = $2",
            )
            .bind(id)
            .bind(&tenant)
            .bind(crate::seal_json(&serde_json::to_value(&values)?)?)
            .bind(value_names_of(&values))
            .bind(serde_json::to_value(&union)?)
            .bind(verified)
            .bind(&identity)
            .bind(&label)
            .bind(door_str(door))
            .bind(expires_at)
            .bind(&spec_json)
            // Re-consent carries the current app; write it through so a
            // later refresh uses the app this consent actually used, not
            // the one the row was first created with.
            .bind(crate::seal_json(&serde_json::to_value(&registration)?)?)
            .bind(&registration.client_id)
            .bind(provider_account_of(&spec, &values))
            .bind(crate::resolve::events_recipe_hash(&spec.events))
            .execute(pool)
            .await?;
            (id, None, union)
        }
        None => {
            let project = match spec.grants {
                GrantCoexistence::Exclusive => None,
                GrantCoexistence::Coexisting => project_id.clone(),
            };
            // The same write path a pasted connection and a published
            // one take. A consent is a third way a connection comes
            // into being, and it earns nothing by spelling the column
            // list out a third time.
            let done = insert_grant(
                pool,
                &tenant,
                NewGrant {
                    spec: &spec,
                    registration: &Some(registration.clone()),
                    project_id: project.clone(),
                    published_by_node: None,
                    values: values.clone(),
                    granted: granted.clone(),
                    verified,
                    owner: CredentialOwner::TheirOwn,
                    door,
                    label: label.clone(),
                    identity: identity.clone(),
                    expires_at,
                },
            )
            .await?;
            (done.grant.id, project, granted)
        }
    };

    Ok(CompletedConnect {
        grant: GrantSummary {
            id: grant_id,
            service,
            project_id,
            identity,
            label,
            scopes: recorded_scopes,
            permissions_verified: verified,
            owner: CredentialOwner::TheirOwn,
            door,
            expires_at,
            value_names: values.keys().cloned().collect(),
        },
    })
}

// ---------- Shared helpers (also used by resolve) ----------

/// POST a token-endpoint form and parse the JSON answer. Loud on a
/// non-success status AND on the Slack-style `{"ok": false}` body
/// (Slack answers 200 for errors).
/// Place the app's client_id + client_secret where the spec's
/// `token_auth` says the token endpoint reads them: `Body` pushes them
/// into the form params (the return is `None`), `Basic` hands them
/// back for the request's HTTP Basic header. One helper so exchange,
/// refresh, and client-credentials can never disagree.
pub(crate) fn place_client_auth(
    token_auth: weft_core::access::spec::TokenAuth,
    reg: &BTreeMap<String, String>,
    params: &mut Vec<(String, String)>,
) -> Option<(String, String)> {
    match token_auth {
        weft_core::access::spec::TokenAuth::Body => {
            for name in ["client_id", "client_secret"] {
                if let Some(v) = reg.get(name) {
                    params.push((name.into(), v.clone()));
                }
            }
            None
        }
        weft_core::access::spec::TokenAuth::Basic => Some((
            reg.get("client_id").cloned().unwrap_or_default(),
            reg.get("client_secret").cloned().unwrap_or_default(),
        )),
    }
}

pub(crate) async fn token_request(
    token_url: &str,
    params: &[(String, String)],
    basic: Option<(String, String)>,
) -> anyhow::Result<Value> {
    let mut req = base_client()
        .post(token_url)
        .header(reqwest::header::ACCEPT, "application/json")
        .form(params);
    if let Some((user, pass)) = basic {
        req = req.basic_auth(user, Some(pass));
    }
    let resp = req
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("token endpoint unreachable: {e}"))?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(AccessError::Invalid(format!(
            "token endpoint answered {status}: {}",
            head_of(&body)
        ))
        .into());
    }
    let json: Value = serde_json::from_str(&body)
        .map_err(|_| AccessError::Invalid(format!(
            "token endpoint answered non-JSON: {}",
            head_of(&body)
        )))?;
    if json.get("ok").and_then(Value::as_bool) == Some(false) {
        return Err(AccessError::Invalid(format!(
            "token endpoint refused: {}",
            json.get("error").and_then(Value::as_str).unwrap_or("unknown error")
        ))
        .into());
    }
    if let Some(err) = json.get("error").and_then(Value::as_str) {
        return Err(AccessError::Invalid(format!("token endpoint refused: {err}")).into());
    }
    Ok(json)
}

/// `expires_in` seconds -> an absolute expiry (with a safety margin
/// applied at REFRESH time, not here).
pub(crate) fn expires_at_of(resp: &Value) -> Option<chrono::DateTime<chrono::Utc>> {
    resp.get("expires_in")
        .and_then(Value::as_i64)
        .map(|secs| chrono::Utc::now() + chrono::Duration::seconds(secs))
}

pub(crate) fn apply_captures(
    captures: &[weft_core::access::spec::Capture],
    resp: &Value,
    values: &mut BTreeMap<String, String>,
) -> anyhow::Result<()> {
    for c in captures {
        match lookup_path(resp, &c.path) {
            Some(v) => {
                let s = match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                values.insert(c.name.clone(), s);
            }
            None if c.optional => {}
            None => {
                return Err(AccessError::Invalid(format!(
                    "the response carries nothing at '{}' (capture '{}')",
                    c.path, c.name
                ))
                .into())
            }
        }
    }
    Ok(())
}

/// Run the spec's declared test call with the values acquired so far;
/// merge its captures back in. The connect fails here when the
/// credential does not actually work.
pub(crate) async fn run_test_call(
    test: Option<&TestCall>,
    auth: &[weft_core::access::spec::AuthStep],
    values: &mut BTreeMap<String, String>,
) -> anyhow::Result<()> {
    let Some(test) = test else { return Ok(()) };
    let url = test.url.resolve(values).map_err(AccessError::Invalid)?;
    let steps = resolve_steps(auth, values).map_err(AccessError::Invalid)?;
    let client = authed_client(steps);
    let req = match test.method {
        weft_core::access::spec::TestMethod::Get => client.get(&url),
        weft_core::access::spec::TestMethod::Post => client.post(&url),
    };
    let resp = req
        .send()
        .await
        .map_err(|e| {
            // The template, never the resolved URL: it may interpolate a
            // secret, and the send error's own URL carries the applied
            // auth (a query token, a path-prefix token).
            anyhow::anyhow!(
                "test call to {} failed: {}",
                test.url.0,
                weft_core::access::client::send_error(e)
            )
        })?;
    let status = resp.status().as_u16();
    let body: Value = resp.json().await.unwrap_or(Value::Null);
    if status != test.expect_status {
        return Err(AccessError::Invalid(format!(
            "the credential did not pass the service's test call ({} answered {status}, \
             expected {})",
            test.url.0,
            test.expect_status
        ))
        .into());
    }
    // Slack answers 200 with ok:false for a dead token; same guard as
    // the token endpoint.
    if body.get("ok").and_then(Value::as_bool) == Some(false) {
        return Err(AccessError::Invalid(format!(
            "the credential did not pass the service's test call: {}",
            body.get("error").and_then(Value::as_str).unwrap_or("unknown error")
        ))
        .into());
    }
    apply_captures(&test.captures, &body, values)
}

pub(crate) fn resolve_identity(
    spec: &AccessSpec,
    values: &BTreeMap<String, String>,
) -> anyhow::Result<Option<String>> {
    match &spec.identity {
        None => Ok(None),
        Some(t) => Ok(Some(t.resolve(values).map_err(AccessError::Invalid)?)),
    }
}

fn head_of(body: &str) -> String {
    weft_core::truncate_user_string(body, 300)
}


#[cfg(test)]
mod tests {
    use super::*;

    fn field(name: &str, optional: bool) -> CredentialField {
        CredentialField {
            name: name.into(),
            label: None,
            optional,
            secret: false,
            placeholder: None,
        }
    }

    fn values(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    /// The gate every set of values a person or a node supplied
    /// passes. A blank required value is MISSING, not stored: a
    /// downstream node would otherwise get an empty password and fail
    /// at the provider instead of here.
    #[test]
    fn the_value_check_holds_a_pasted_and_a_published_connection_to_one_rule() {
        // The spec DECLARES the two fields the check is run against,
        // so the rule and the recipe cannot be a mismatched pair the
        // production callers could never produce.
        let spec: AccessSpec = serde_json::from_value(serde_json::json!({
            "service": "pg",
            "acquisition": { "kind": "static", "fields": [
                { "name": "host" },
                { "name": "port", "optional": true },
            ]},
        }))
        .expect("a static spec");
        let fields = [field("host", false), field("port", true)];
        let check = |vals| storable_values(&spec, &fields, vals);

        check(values(&[("host", "db"), ("port", "5432")])).expect("a complete set passes");

        let kept = check(values(&[("host", "db"), ("port", "   ")]))
            .expect("a blank optional passes");
        assert!(!kept.contains_key("port"), "a blank optional stores nothing");

        let e = check(values(&[("port", "5432")])).expect_err("required missing");
        assert!(e.to_string().contains("'host' is required"), "{e}");

        let e = check(values(&[("host", "  ")]))
            .expect_err("a blank required value is missing");
        assert!(e.to_string().contains("'host' is required"), "{e}");

        let e = check(values(&[("host", "db"), ("hostname", "db")]))
            .expect_err("undeclared name");
        assert!(e.to_string().contains("no value named 'hostname'"), "{e}");
    }

    /// What lands on the row: only an authoritative rung with a
    /// provider-produced set records VERIFIED; a non-authoritative
    /// rung records the claimed ticks even when something echoed, and
    /// a missing echo always records claimed.
    #[test]
    fn recorded_permissions_needs_an_authoritative_provider_answer() {
        let claimed = vec!["x".to_string()];
        let reported = vec!["x".to_string(), "y".to_string()];

        let (set, verified) = recorded_permissions(
            VerificationRung::ReportsPermissions,
            Some(reported.clone()),
            claimed.clone(),
        );
        assert_eq!(set, reported);
        assert!(verified);

        let (set, verified) = recorded_permissions(
            VerificationRung::SelfIntrospect,
            Some(reported.clone()),
            claimed.clone(),
        );
        assert!(verified);
        assert_eq!(set, reported);

        // A liveness-only rung proves nothing about the set.
        let (set, verified) = recorded_permissions(
            VerificationRung::ReportsValidity,
            Some(reported.clone()),
            claimed.clone(),
        );
        assert!(!verified);
        assert_eq!(set, claimed);

        let (set, verified) =
            recorded_permissions(VerificationRung::ReportsPermissions, None, claimed.clone());
        assert!(!verified);
        assert_eq!(set, claimed);

        let (set, verified) =
            recorded_permissions(VerificationRung::Silent, None, Vec::new());
        assert!(!verified);
        assert!(set.is_empty());
    }

    /// The self-introspection capture parses both a JSON array and a
    /// delimited string, and an absent capture is None.
    #[test]
    fn introspected_permissions_parse_both_shapes() {
        let mut values = BTreeMap::new();
        assert!(introspected_permissions(&values).is_none());

        values.insert(GRANTED_PERMISSIONS_CAPTURE.into(), r#"["a","b"]"#.into());
        assert_eq!(introspected_permissions(&values).unwrap(), vec!["a", "b"]);

        values.insert(GRANTED_PERMISSIONS_CAPTURE.into(), "a b,c".into());
        assert_eq!(introspected_permissions(&values).unwrap(), vec!["a", "b", "c"]);
    }
}

// ---------- Published connections (a node opens what it runs) ----------

/// A node handing out a connection to something it runs itself: the
/// database its own infra spec brought up, whose credentials the node
/// read back off the running container. The values are the service's
/// own declared fields, exactly what a person would have pasted.
#[derive(Debug, Serialize, Deserialize)]
pub struct PublishAccess {
    pub spec: AccessSpec,
    pub project_id: String,
    /// The node doing the publishing. Its connection, so a second
    /// publish updates that one row instead of piling up a new one,
    /// and terminating the node takes it away with it.
    pub node_id: String,
    pub values: BTreeMap<String, String>,
    pub label: Option<String>,
}

/// Store the connection a node published, or update the one it
/// published before.
///
/// A published connection describes a service the user's OWN project
/// runs: there is no provider behind it, no consent to renew, nothing
/// for weft to call on its behalf. The recipe is held to exactly that
/// shape here, because unlike every other spec that reaches this store
/// (authored node metadata, validated at load) this one arrives from a
/// worker, which runs tenant code. A recipe carrying provider events
/// or a connect-time call would later have the CONTROL PLANE make an
/// HTTP request the tenant chose, from inside the cluster; refusing
/// those shapes is what makes that unreachable rather than unlikely.
///
/// Always the user's own credential: nothing published can resolve to
/// the runtime's, whatever the caller sends.
pub async fn publish_grant(
    pool: &PgPool,
    tenant: &str,
    req: PublishAccess,
) -> anyhow::Result<CompletedConnect> {
    let PublishAccess { spec, project_id, node_id, values, label } = req;
    spec.validate().map_err(AccessError::Invalid)?;
    let fields =
        weft_core::access::spec::publishable_fields(&spec).map_err(AccessError::Invalid)?;
    let values = storable_values(&spec, fields, values)?;
    let identity = resolve_identity(&spec, &values)?;
    insert_grant(
        pool,
        tenant,
        NewGrant {
            spec: &spec,
            registration: &None,
            project_id: Some(project_id),
            published_by_node: Some(node_id),
            values,
            granted: Vec::new(),
            verified: false,
            owner: CredentialOwner::TheirOwn,
            door: Door::Own,
            label,
            identity,
            expires_at: None,
        },
    )
    .await
}

/// The connection this node published for `service`, if it has one.
/// The node's own read-back path: it republishes with the credential
/// it stored the first time rather than asking the container again.
/// Answers the same [`PublishedConnection`] shape the publish did.
pub async fn published_connection(
    pool: &PgPool,
    tenant: &str,
    project_id: &str,
    node_id: &str,
    service: &str,
) -> anyhow::Result<Option<PublishedConnection>> {
    let row: Option<(uuid::Uuid, Option<String>)> = sqlx::query_as(
        "SELECT id, identity FROM access_grant
         WHERE tenant_id = $1 AND project_id = $2 AND published_by_node = $3
           AND service = $4",
    )
    .bind(tenant)
    .bind(project_id)
    .bind(node_id)
    .bind(service)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(id, identity)| PublishedConnection { connection_id: id.to_string(), identity }))
}

/// Drop every connection a node published. Called when its infra is
/// terminated: the credentials open something that no longer exists,
/// and a connection nobody can use is junk.
/// Takes an executor rather than the pool so the caller can run it
/// inside the transaction that removes the node itself: the node and
/// the connection it published go away together or not at all.
///
/// `node_id` of `None` drops every published connection in the
/// project, for the path where the whole project goes (there is no
/// node left to name). A connection a PERSON made is never touched by
/// either: it is theirs, listed and deletable tenant-wide.
pub async fn delete_published_grants<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    tenant: &str,
    project_id: &str,
    node_id: Option<&str>,
) -> anyhow::Result<u64> {
    Ok(sqlx::query(
        "DELETE FROM access_grant
         WHERE tenant_id = $1 AND project_id = $2 AND published_by_node IS NOT NULL
           AND ($3::text IS NULL OR published_by_node = $3)",
    )
    .bind(tenant)
    .bind(project_id)
    .bind(node_id)
    .execute(executor)
    .await?
    .rows_affected())
}
