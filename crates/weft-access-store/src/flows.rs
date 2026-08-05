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
use weft_core::{AccessSpec, AppRegistration, CredentialOwner};

use crate::{door_of, door_str, owner_of, owner_str, scopes_of, AccessError, GrantSummary};

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

/// A connect that completes in one request: `static` (pasted fields),
/// `mint_jwt` (pasted key, minted + exchanged once to validate),
/// `oauth2`/`client_credentials` (token requested server-to-server), or
/// the SHARED door of a key service (nothing pasted; the runtime's
/// credential source answers per call). A browser-consent acquisition
/// is refused here; that is [`begin_oauth`] / [`complete_oauth`].
#[derive(Debug, Serialize, Deserialize)]
pub struct ConnectDirect {
    pub spec: AccessSpec,
    /// Which door the user walked through. The SHARED door of a
    /// non-consent service stores a runtime-owned row; everything else
    /// is the user's own credential.
    #[serde(default = "own_door")]
    pub door: Door,
    /// The pasted field values, keyed by the spec's declared field
    /// names. Sent editor -> store directly; never node config.
    /// Empty for a shared-door connect.
    #[serde(default)]
    pub values: BTreeMap<String, String>,
    /// The name the user gave this connection (the list's middle
    /// column, standing in where no app label exists). `None` shows
    /// the service's own label.
    #[serde(default)]
    pub label: Option<String>,
    /// The permissions the user CLAIMS the pasted credential holds
    /// (ticked on the picker, for key services with a permission
    /// concept). Recorded claimed unless verification upgrades them.
    /// On a SHARED-door connect the broker overwrites this with the
    /// chosen app's fixed `covers`; nothing the client sent survives.
    #[serde(default)]
    pub permissions: Vec<String>,
    /// The app to use, for the acquisitions that need one
    /// (`client_credentials`). Resolved at the broker before this is
    /// reached (the user's pasted app for the own door, the registered
    /// app for the shared door); `None` for a service using no app.
    #[serde(default)]
    pub registration: Option<AppRegistration>,
    /// The user pasted a ready credential through the service's
    /// `own_page.paste` section instead of going through an app: the
    /// connect runs on [`AccessSpec::paste_variant`] (a `Static`
    /// acquisition over the paste fields).
    #[serde(default)]
    pub paste: bool,
    /// The connecting project (grants are per-project by default; an
    /// exclusive-class OAuth grant ignores it, but direct connects are
    /// per-paste anyway).
    pub project_id: Option<String>,
}

fn own_door() -> Door {
    Door::Own
}

/// The "Create it for me" app mint as it travels the wire (editor ->
/// dispatcher -> broker): the service's spec (whose `own_page.mint`
/// recipe runs) and the ticked permissions baked into the manifest.
/// ONE definition so the hops cannot drift.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MintAppRequest {
    pub spec: AccessSpec,
    #[serde(default)]
    pub permissions: Vec<String>,
}

/// What the mint captured: the fresh app's values, for the editor to
/// prefill the "Your own" form (the user still names it and connects).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MintAppResponse {
    pub values: BTreeMap<String, String>,
}

/// The connect flow's answer: the summary the editor stores on the
/// node (`{id, identity}`) and shows.
#[derive(Debug, Serialize, Deserialize)]
pub struct CompletedConnect {
    pub grant: GrantSummary,
}

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
            for f in fields {
                let empty = values.get(&f.name).map_or(true, |v| v.trim().is_empty());
                if empty && !f.optional {
                    return Err(AccessError::Invalid(format!(
                        "field '{}' is required",
                        f.name
                    ))
                    .into());
                }
            }
            // The service's all-or-nothing groups: a half-filled one
            // (a mail server with no port) and a connect filling none
            // are both refused here, naming the fix.
            weft_core::access::spec::capability_shortfall(&spec.capabilities, |name| {
                values.get(name).is_some_and(|v| !v.trim().is_empty())
            })
            .map_err(AccessError::Invalid)?;
            // An optional field left blank stores nothing, so later
            // reads see "missing" rather than an empty secret.
            values.retain(|_, v| !v.trim().is_empty());
        }
        Acquisition::MintJwt { .. } => {
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

/// Everything one grant INSERT needs; the single write path for both
/// direct connects, so the column list exists once.
struct NewGrant<'a> {
    spec: &'a AccessSpec,
    registration: &'a Option<AppRegistration>,
    project_id: Option<String>,
    values: BTreeMap<String, String>,
    granted: Vec<String>,
    verified: bool,
    owner: CredentialOwner,
    door: Door,
    label: Option<String>,
    identity: Option<String>,
    expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

async fn insert_grant(
    pool: &PgPool,
    tenant: &str,
    grant: NewGrant<'_>,
) -> anyhow::Result<CompletedConnect> {
    let id = uuid::Uuid::new_v4();
    sqlx::query(
        "INSERT INTO access_grant
           (id, tenant_id, service, registration_sealed, client_id, project_id, spec_json,
            events_recipe_hash, values_sealed, value_names, granted_scopes,
            permissions_verified, owner, door, label, identity, provider_account, expires_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
                 $18)",
    )
    .bind(id)
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
    .execute(pool)
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

/// A connect request as the EDITOR sends it: the store's shape plus
/// `shared_app`, the label of the registered app a shared-door connect
/// goes through (each registered app is its own option in the editor).
/// The label is consumed at the BROKER, which resolves it to the app
/// and its fixed `covers` before the store flow runs; the flows
/// themselves never read it. Defined here so the dispatcher's
/// forwarding and the broker's handling share one wire shape.
#[derive(Debug, Serialize, Deserialize)]
pub struct SharedDoorPick<T> {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shared_app: Option<String>,
    #[serde(flatten)]
    pub inner: T,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BeginOAuth {
    pub spec: AccessSpec,
    /// Which door the user walked through. Decides which app the
    /// broker resolves (the registered app for `shared`, the user's
    /// own / project app for `own`) and is recorded on the grant.
    #[serde(default = "own_door")]
    pub door: Door,
    /// The resolved app for the consent (client id, and the secret for
    /// the later code exchange). Resolved AT THE BROKER before this is
    /// reached; a client-sent app is only ever honored on the own door.
    #[serde(default)]
    pub registration: Option<AppRegistration>,
    /// The permissions the consent asks for (drive the consent URL and
    /// are recorded on the grant): the user's ticks on the own door,
    /// the chosen app's fixed `covers` on the shared door (overwritten
    /// by the broker; nothing the client sent survives there).
    pub permissions: Vec<String>,
    pub project_id: Option<String>,
    /// Upgrade/rotate this existing exclusive-class grant in place
    /// instead of minting a new row.
    pub upgrade_grant_id: Option<uuid::Uuid>,
    /// The callback URL the provider redirects to, derived from the
    /// dispatcher's public base and filled by it before the forward
    /// (the editor does not know the dispatcher's public host).
    #[serde(default)]
    pub redirect_uri: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StartedOAuth {
    /// Open this in the user's browser.
    pub consent_url: String,
    pub state: String,
}

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
    let Acquisition::OAuth2 { token_url, captures, .. } = &spec.acquisition else {
        return Err(anyhow::anyhow!("pending connect row is not an oauth2 spec"));
    };

    let reg = registration.to_values();
    let mut params: Vec<(String, String)> = vec![
        ("grant_type".into(), "authorization_code".into()),
        ("code".into(), code.to_string()),
        ("redirect_uri".into(), redirect_uri),
    ];
    for name in ["client_id", "client_secret"] {
        if let Some(v) = reg.get(name) {
            params.push((name.into(), v.clone()));
        }
    }
    if let Some(verifier) = verifier {
        params.push(("code_verifier".into(), verifier));
    }
    let resp = token_request(token_url, &params).await?;

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
            let id = uuid::Uuid::new_v4();
            let project = match spec.grants {
                GrantCoexistence::Exclusive => None,
                GrantCoexistence::Coexisting => project_id.clone(),
            };
            sqlx::query(
                "INSERT INTO access_grant
                   (id, tenant_id, service, registration_sealed, client_id, project_id,
                    spec_json, events_recipe_hash, values_sealed, value_names,
                    granted_scopes, permissions_verified, owner, door, label,
                    identity, provider_account, expires_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
                         $16, $17, $18)",
            )
            .bind(id)
            .bind(&tenant)
            .bind(&service)
            .bind(crate::seal_json(&serde_json::to_value(&registration)?)?)
            .bind(&registration.client_id)
            .bind(&project)
            .bind(&spec_json)
            .bind(crate::resolve::events_recipe_hash(&spec.events))
            .bind(crate::seal_json(&serde_json::to_value(&values)?)?)
            .bind(value_names_of(&values))
            .bind(serde_json::to_value(&granted)?)
            .bind(verified)
            .bind(owner_str(CredentialOwner::TheirOwn))
            .bind(door_str(door))
            .bind(&label)
            .bind(&identity)
            .bind(provider_account_of(&spec, &values))
            .bind(expires_at)
            .execute(pool)
            .await?;
            (id, project, granted)
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
pub(crate) async fn token_request(
    token_url: &str,
    params: &[(String, String)],
) -> anyhow::Result<Value> {
    let resp = base_client()
        .post(token_url)
        .header(reqwest::header::ACCEPT, "application/json")
        .form(params)
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
