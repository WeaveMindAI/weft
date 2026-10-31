//! Connect-side flows: direct connects (paste / mint / server-to-server)
//! and the two halves of a browser OAuth consent. The app credentials a
//! flow needs arrive already resolved on the request (project-declared,
//! else the fallback provider); this module never looks an app up.

use std::collections::BTreeMap;

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use weft_task_store::db::{self, Client, FromRow, JsonText, Pool, Row};

use weft_core::access::client::{authed_client, base_client, resolve_steps, RefusalKind};
use weft_core::access::spec::{
    lookup_path, Acquisition, AnswerReader, CredentialField, Door, GrantCoexistence, OAuthGrant, TestCall,
    TokenAnswer, TokenCall, TokenExpiry, TokenLife, TokenRead, VerificationRung,
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

/// Whose connections a list or a forget is about: the author's own (the
/// ones the editor picks from), or one instance's of one project.
#[derive(Debug, Clone, Copy)]
pub enum GrantOwnerScope<'a> {
    Author,
    Instance { project_id: uuid::Uuid, instance: &'a weft_core::instance::InstanceId },
}

impl GrantOwnerScope<'_> {
    /// The (project, instance) pair the SQL filters on: `instance_id IS
    /// NULL` for the author, `project_id = $p AND instance_id = $m` for an
    /// instance.
    fn columns(&self) -> (Option<uuid::Uuid>, Option<&str>) {
        match self {
            GrantOwnerScope::Author => (None, None),
            GrantOwnerScope::Instance { project_id, instance } => (Some(*project_id), Some(instance.as_str())),
        }
    }
}

/// The SQL predicate for a [`GrantOwnerScope`] bound at `$p` (project)
/// and `$m` (instance).
fn owner_scope_sql(p: &str, m: &str) -> String {
    format!("(({m}::text IS NULL AND instance_id IS NULL) OR (project_id = {p} AND instance_id = {m}))")
}

/// The SQL predicate "this row belongs to the same owner" for a
/// connect bound at `$m` (instance, NULL for the author) and `$p`
/// (project): the author's rows, or that instance's rows IN that
/// project. An instance id alone names nobody: the same name in another
/// project is another person.
fn same_owner_sql(m: &str, p: &str) -> String {
    format!("instance_id IS NOT DISTINCT FROM {m} AND ({m}::text IS NULL OR project_id = {p})")
}

/// The `access_grant` columns a listing reads.
struct ListedRow {
    id: uuid::Uuid,
    service: String,
    project_id: Option<uuid::Uuid>,
    identity: Option<String>,
    label: Option<String>,
    granted_scopes: Value,
    permissions_verified: bool,
    owner: String,
    door: String,
    expires_at: Option<chrono::DateTime<chrono::Utc>>,
    value_names: Vec<String>,
    instance_id: Option<String>,
}

impl FromRow for ListedRow {
    fn from_row(row: Row) -> db::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            service: row.try_get("service")?,
            project_id: row.try_get("project_id")?,
            identity: row.try_get("identity")?,
            label: row.try_get("label")?,
            granted_scopes: row.try_get::<_, db::Json<Value>>("granted_scopes")?.0,
            permissions_verified: row.try_get("permissions_verified")?,
            owner: row.try_get("owner")?,
            door: row.try_get("door")?,
            expires_at: row.try_get("expires_at")?,
            value_names: row.try_get("value_names")?,
            instance_id: row.try_get("instance_id")?,
        })
    }
}

/// One owner's connections, optionally narrowed to one service.
/// Summaries only; stored values never leave the store side.
pub async fn list_grants(
    pool: &Pool,
    tenant: &str,
    service: Option<&str>,
    owner: GrantOwnerScope<'_>,
) -> anyhow::Result<Vec<GrantSummary>> {
    let (scope_project, scope_instance) = owner.columns();
    let sql = format!(
        // Only the value NAMES: the values themselves never leave the
        // store side, and the editor's check needs nothing more.
        "SELECT id, service, project_id, identity, label, granted_scopes,
                permissions_verified, owner, door, expires_at,
                ARRAY(SELECT jsonb_array_elements_text(value_names)) AS value_names,
                instance_id
         FROM access_grant
         WHERE tenant_id = $1 AND ($2::text IS NULL OR service = $2) AND {scope}
         ORDER BY created_at",
        scope = owner_scope_sql("$3", "$4"),
    );
    let rows: Vec<ListedRow> = pool.all(&sql, &[&tenant, &service, &scope_project, &scope_instance]).await?;
    rows.into_iter()
        .map(|row| {
            let owner = owner_of(&row.owner, row.instance_id.as_deref())?;
            Ok(GrantSummary {
                id: row.id,
                service: row.service,
                project_id: row.project_id,
                instance: row
                    .instance_id
                    .map(weft_core::instance::InstanceId::new)
                    .transpose()
                    .map_err(anyhow::Error::msg)?,
                identity: row.identity,
                label: row.label,
                scopes: scopes_of(&row.granted_scopes),
                permissions_verified: row.permissions_verified,
                // A stored credential is one; a runtime-owned row's
                // is the broker's to confirm (the dispatcher's
                // listing asks it and overwrites this).
                has_credential: !owner.is_platform(),
                owner,
                door: door_of(&row.door)?,
                expires_at: row.expires_at,
                value_names: row.value_names,
            })
        })
        .collect()
}

/// The services `tenant` signed in to through an app of its own (the own
/// door, a browser consent): each such app lists the install's OAuth
/// callback at its provider, so it must be told when that callback moves.
pub async fn own_sign_in_services(pool: &Pool, tenant: &str) -> anyhow::Result<Vec<String>> {
    let rows: Vec<(uuid::Uuid, String, JsonText<Value>, weft_core::stored::Version)> = pool
        .all(
            "SELECT id, service, spec, spec_version FROM access_grant
             WHERE tenant_id = $1 AND door = 'own' AND registration_sealed IS NOT NULL",
            &[&tenant],
        )
        .await?;
    let mut services = std::collections::BTreeSet::new();
    for (id, service, JsonText(spec), version) in rows {
        if crate::spec_of(format_args!("connection {id}"), spec, version)?.needs_browser_consent() {
            services.insert(service);
        }
    }
    Ok(services.into_iter().collect())
}

/// The `WITH` clause that deletes `table`'s rows past their life (made
/// before the bind `cutoff_param`, [`CONNECT_TTL`] ago) in the statement
/// that adds one to it: an abandoned consent, picker or unpolled result
/// goes when the next one of its kind is made, with no timer looking for
/// it, and a read that finds one past its life deletes it. Named
/// `pruned`, for a statement that adds clauses of its own after it.
pub(crate) fn pruning(table: &str, cutoff_param: &str) -> String {
    format!("WITH pruned AS (DELETE FROM {table} WHERE created_at < {cutoff_param})")
}

/// Forget one connection of `owner`'s. An instance forgets only its
/// own, the author only theirs: an id of anybody else's is NotFound (no
/// existence leak). The instance's values naming it go with it (`instance_value`
/// cascades), so a field that used it asks for a connection again.
pub async fn delete_grant(
    pool: &Pool,
    tenant: &str,
    id: uuid::Uuid,
    owner: GrantOwnerScope<'_>,
) -> anyhow::Result<()> {
    let (scope_project, scope_instance) = owner.columns();
    let done = pool
        .execute(
            &format!("DELETE FROM access_grant WHERE tenant_id = $1 AND id = $2 AND {}", owner_scope_sql("$3", "$4")),
            &[&tenant, &id, &scope_project, &scope_instance],
        )
        .await?;
    if done == 0 {
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

/// An instance's connection lives in a project: refused without one.
fn instance_needs_project(instance: &Option<weft_core::instance::InstanceId>, project_id: Option<uuid::Uuid>) -> Result<(), AccessError> {
    match (instance, project_id) {
        (Some(instance), None) => Err(AccessError::Invalid(format!(
            "a connection of instance '{instance}' belongs to a project; name the project"
        ))),
        _ => Ok(()),
    }
}

/// The shared door runs on the program author's (or the runtime's)
/// credential: an instance connects its own account. Refused before
/// anything is written.
fn instance_not_shared(instance: &Option<weft_core::instance::InstanceId>, door: Door) -> Result<(), AccessError> {
    match (instance, door) {
        (Some(instance), Door::Shared) => Err(AccessError::Invalid(format!(
            "instance '{instance}' connects its own account; the shared door is the program author's"
        ))),
        _ => Ok(()),
    }
}

pub async fn connect_direct(
    pool: &Pool,
    tenant: &str,
    req: ConnectDirect,
) -> anyhow::Result<CompletedConnect> {
    instance_needs_project(&req.instance, req.project_id)?;
    instance_not_shared(&req.instance, req.door)?;
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
            instance: req.instance.clone(),
            published_by_node: None,
            values: BTreeMap::new(),
            granted: Vec::new(),
            verified: false,
            platform: true,
            door: Door::Shared,
            label: Some(connection_name(req.label, &None, spec)),
            identity: None,
            life: None,
        })
        .await;
    }

    let mut values = req.values;
    let mut life: Option<TokenLife> = None;
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
            life = crate::resolve::mint_and_exchange(spec, &mut values).await?;
        }
        Acquisition::OAuth2 { grant: OAuthGrant::ClientCredentials, .. } => {
            let reg = require_registration(&req.registration, spec)?;
            let expiry =
                crate::resolve::client_credentials_token(spec, &reg.to_values(), &mut values, AnswerOf::First)
                    .await?;
            life = connect_expiry(spec, expiry, &values)?;
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
    let label = Some(connection_name(req.label, &req.registration, spec));
    insert_grant(pool, tenant, NewGrant {
        spec,
        registration: &req.registration,
        project_id: req.project_id,
        instance: req.instance,
        published_by_node: None,
        values,
        granted,
        verified,
        platform: false,
        door: req.door,
        label,
        identity,
        life,
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
pub(crate) fn value_names_of(values: &BTreeMap<String, String>) -> Vec<&str> {
    values.keys().map(String::as_str).collect()
}

/// Everything one grant INSERT needs. The single write path for every
/// way a connection comes into being (a person pasting one, a browser
/// consent finishing, a node publishing one for something it runs, and
/// one copied from another install), so the column list exists once and
/// cannot drift between them. A re-consent that rotates an EXISTING row is the one write
/// that is not an insert, and updates in place.
pub(crate) struct NewGrant<'a> {
    pub(crate) spec: &'a AccessSpec,
    pub(crate) registration: &'a Option<AppRegistration>,
    pub(crate) project_id: Option<uuid::Uuid>,
    /// Whose connection inside the project: an instance's (which connected
    /// it, or whose copy of a node published it), `None` for the
    /// author's.
    pub(crate) instance: Option<weft_core::instance::InstanceId>,
    /// Set only by [`publish_grant`]; a person's connect leaves it None.
    pub(crate) published_by_node: Option<String>,
    pub(crate) values: BTreeMap<String, String>,
    pub(crate) granted: Vec<String>,
    pub(crate) verified: bool,
    /// Whether calls spend the runtime's own credential (a shared-door
    /// connection): its owner is then the platform, else whoever made
    /// it, the instance or the author. Never together with `instance`: an
    /// instance connects its own account.
    pub(crate) platform: bool,
    pub(crate) door: Door,
    pub(crate) label: Option<String>,
    pub(crate) identity: Option<String>,
    /// When its token dies and the lifetime it was issued with; `None`
    /// for a credential that does not expire.
    pub(crate) life: Option<TokenLife>,
}

/// The id, birth and token expiry of a connection copied from another
/// install (`crate::migrate`): kept as they were there, where a new
/// connection gets fresh ones. The expiry is kept column for column (a
/// row from before lifetimes were recorded has an expiry and none), so
/// the copy's [`NewGrant::life`] is `None`.
pub(crate) struct Moved {
    pub id: uuid::Uuid,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
    pub token_lifetime_secs: Option<i64>,
}

/// The name a person's connection is listed under: the one they typed,
/// else its app's, else the service's own ("Firecrawl"), so every
/// connection a person makes has a name each list can show.
fn connection_name(typed: Option<String>, registration: &Option<AppRegistration>, spec: &AccessSpec) -> String {
    match typed {
        Some(l) if !l.trim().is_empty() => l.trim().to_string(),
        _ => registration.as_ref().map_or_else(|| spec.display_label().to_string(), |r| r.label.clone()),
    }
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

pub(crate) async fn insert_grant(db: &impl Client, tenant: &str, grant: NewGrant<'_>) -> anyhow::Result<CompletedConnect> {
    write_grant(db, tenant, None, grant)
        .await?
        .ok_or_else(|| anyhow::anyhow!("a new connection's write answered no row, which only a copy's can"))
}

/// Write `grant`, copied from another install with what it keeps from
/// there (`moved`), as a new row (`crate::migrate`). `None` when a
/// connection to the same exclusive account landed here first (a sign-in
/// racing the copy, past the copy's own look): the copy is skipped, the
/// way it is when that row was already there.
pub(crate) async fn copy_grant(
    db: &impl Client,
    tenant: &str,
    moved: Moved,
    grant: NewGrant<'_>,
) -> anyhow::Result<Option<CompletedConnect>> {
    write_grant(db, tenant, Some(moved), grant).await
}

/// The one write of a connection's row ([`insert_grant`], [`copy_grant`]):
/// `None` only for a copy that met an exclusive row.
async fn write_grant(
    db: &impl Client,
    tenant: &str,
    moved: Option<Moved>,
    mut grant: NewGrant<'_>,
) -> anyhow::Result<Option<CompletedConnect>> {
    let owner = if grant.platform { CredentialOwner::Platform } else { CredentialOwner::own(grant.instance.clone()) };
    let (expires_at, lifetime_secs) = match &moved {
        Some(moved) => (moved.expires_at, moved.token_lifetime_secs),
        None => life_columns(grant.life),
    };
    // Two kinds of connection have a key, and land on the row already
    // holding it instead of making a second one. A node's published
    // connection REPLACES the one it published before (its key is the
    // node, not the moment), so the same write serves a first publish and
    // a re-publish. A person's sign-in to an exclusive-class service
    // through an app is the account's one row ([`exclusive_row`]): signing
    // in again, or two first sign-ins landing at once, rotates that row
    // the way a reconnect does, its permissions only growing. A copy from
    // another install keeps the id it had there, so it never lands on
    // another row: `crate::migrate` looked first, and one that a sign-in
    // raced past that look writes nothing and answers no row. Anything
    // else has no key and always inserts. One statement either way, so
    // the column list exists once.
    //
    // EVERY non-key column is written, not a chosen subset. The row
    // becomes exactly what was just built, so a column added to the
    // INSERT below can never be quietly left stale on a re-publish
    // (which nothing would catch: the answer returned to the caller
    // reports the new value while the row keeps the old one).
    // SYNC: the exclusive key <-> crate::GROUP's access_grant_one_exclusive index, exclusive_row
    let exclusive_key = "(tenant_id, service, identity, client_id, instance_id, project_id)
               WHERE published_by_node IS NULL AND client_id IS NOT NULL AND exclusive";
    let exclusive = grant.spec.grants == GrantCoexistence::Exclusive && grant.registration.is_some();
    let lands_on = if grant.published_by_node.is_some() {
        Some((
            "(tenant_id, project_id, published_by_node, service, instance_id) WHERE published_by_node IS NOT NULL",
            "granted_scopes = EXCLUDED.granted_scopes".to_string(),
        ))
    } else if moved.is_none() && exclusive {
        Some((
            exclusive_key,
            // `rotate_grant`'s union (`granted_union_sql`), its columns
            // named by table: in an upsert a bare one is ambiguous.
            "granted_scopes = (SELECT COALESCE(jsonb_agg(to_jsonb(s) ORDER BY s), '[]'::jsonb)
                               FROM (SELECT jsonb_array_elements_text(access_grant.granted_scopes) AS s
                                     UNION SELECT jsonb_array_elements_text(EXCLUDED.granted_scopes)) merged),
             reconnects = access_grant.reconnects + 1"
                .to_string(),
        ))
    } else {
        None
    };
    let upsert = match lands_on {
        None if moved.is_some() && exclusive => format!(" ON CONFLICT {exclusive_key} DO NOTHING"),
        Some((key, landing)) => format!(
            " ON CONFLICT {key}
              DO UPDATE SET
                 registration_sealed = EXCLUDED.registration_sealed,
                 client_id = EXCLUDED.client_id,
                 spec = EXCLUDED.spec,
                 spec_version = EXCLUDED.spec_version,
                 exclusive = EXCLUDED.exclusive,
                 events_recipe_hash = EXCLUDED.events_recipe_hash,
                 values_sealed = EXCLUDED.values_sealed,
                 value_names = EXCLUDED.value_names,
                 permissions_verified = EXCLUDED.permissions_verified,
                 owner = EXCLUDED.owner,
                 door = EXCLUDED.door,
                 label = EXCLUDED.label,
                 identity = EXCLUDED.identity,
                 provider_account = EXCLUDED.provider_account,
                 expires_at = EXCLUDED.expires_at,
                 token_lifetime_secs = EXCLUDED.token_lifetime_secs,
                 renewal_claimed_until = NULL,
                 updated_at = now(),
                 {landing}"
        ),
        None => String::new(),
    };
    let sql = format!(
        "INSERT INTO access_grant
           (id, tenant_id, service, registration_sealed, client_id, project_id, spec,
            events_recipe_hash, values_sealed, value_names, granted_scopes,
            permissions_verified, owner, door, label, identity, provider_account, expires_at,
            published_by_node, instance_id, spec_version, token_lifetime_secs, created_at, exclusive)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
                 $18, $19, $20, $21, $22, COALESCE($23, now()), $24){upsert}
         RETURNING id, granted_scopes"
    );
    let written: Option<(uuid::Uuid, db::Json<Value>)> = db
        .opt(
            &sql,
            &[
                &moved.as_ref().map_or_else(uuid::Uuid::new_v4, |m| m.id),
                &tenant,
                &grant.spec.service,
                &registration_snapshot(grant.registration)?,
                &grant.registration.as_ref().map(|r| r.client_id.clone()),
                &grant.project_id,
                &JsonText(grant.spec),
                &crate::resolve::events_recipe_hash(&grant.spec.events),
                &crate::seal_json(&serde_json::to_value(&grant.values)?)?,
                &db::Json(db::Names(&value_names_of(&grant.values))),
                &db::Json(db::Names(&grant.granted)),
                &grant.verified,
                &owner_str(&owner),
                &door_str(grant.door),
                &grant.label,
                &grant.identity,
                &provider_account_of(grant.spec, &grant.values),
                &expires_at,
                &grant.published_by_node,
                &grant.instance.as_ref().map(|m| m.as_str()),
                &weft_core::AccessSpec::STORED.version(),
                &lifetime_secs,
                &moved.map(|m| m.created_at),
                &(grant.spec.grants == GrantCoexistence::Exclusive),
            ],
        )
        .await?;
    let Some((id, db::Json(granted))) = written else { return Ok(None) };
    grant.granted = scopes_of(&granted);
    let label = grant.label.clone();
    Ok(Some(completed(id, grant, label)))
}

/// Write `grant` over the existing row `id` in place: an exclusive-class
/// grant upgraded or signed in again, so every project following it
/// keeps it. Every column [`insert_grant`] writes is written here too
/// (the row becomes exactly what was just built), the reconnect is
/// counted, and a renewal in flight on the old token is let go so it
/// cannot write over the new one. The granted permissions are the ones
/// the row holds and the grant's together, in the same statement: an
/// exclusive grant only grows, and two sign-ins landing at once both
/// keep theirs. Guarded by service and owner like every write: an id
/// naming another provider's row never takes this provider's tokens, and
/// an instance rotates only its own row in its project.
///
/// An upgrade whose consent signed in as another account, one that has
/// its own exclusive row already, cannot become that account's second
/// row: it is refused, naming the row to upgrade instead.
async fn rotate_grant(
    pool: &Pool,
    tenant: &str,
    service: &str,
    id: uuid::Uuid,
    mut grant: NewGrant<'_>,
) -> anyhow::Result<CompletedConnect> {
    let owner = if grant.platform { CredentialOwner::Platform } else { CredentialOwner::own(grant.instance.clone()) };
    let (expires_at, lifetime_secs) = life_columns(grant.life);
    let sql = format!(
        "UPDATE access_grant
         SET values_sealed = $3, value_names = $4, granted_scopes = {union},
             permissions_verified = $6, identity = $7, label = $8, door = $9,
             expires_at = $10, spec = $11, registration_sealed = $12,
             client_id = $13, provider_account = $14, events_recipe_hash = $15,
             project_id = $16, spec_version = $17, token_lifetime_secs = $18, owner = $19,
             exclusive = $22,
             renewal_claimed_until = NULL, reconnects = reconnects + 1, updated_at = now()
         WHERE id = $1 AND tenant_id = $2 AND service = $21 AND {owned}
         RETURNING granted_scopes",
        union = granted_union_sql("$5"),
        owned = same_owner_sql("$20", "$16"),
    );
    let rotated: db::Result<Option<db::Json<Value>>> = pool
        .scalar_opt(
            &sql,
            &[
                &id,
                &tenant,
                &crate::seal_json(&serde_json::to_value(&grant.values)?)?,
                &db::Json(db::Names(&value_names_of(&grant.values))),
                &grant.granted,
                &grant.verified,
                &grant.identity,
                &grant.label,
                &door_str(grant.door),
                &expires_at,
                // The recipe as this weft writes it, whatever shape the row kept it in.
                &JsonText(grant.spec),
                // Re-consent carries the current app; write it through so a later
                // refresh uses the app this consent actually used, not the one the
                // row was first created with.
                &registration_snapshot(grant.registration)?,
                &grant.registration.as_ref().map(|r| r.client_id.clone()),
                &provider_account_of(grant.spec, &grant.values),
                &crate::resolve::events_recipe_hash(&grant.spec.events),
                &grant.project_id,
                &weft_core::AccessSpec::STORED.version(),
                &lifetime_secs,
                &owner_str(&owner),
                &grant.instance.as_ref().map(|m| m.as_str()),
                &service,
                &(grant.spec.grants == GrantCoexistence::Exclusive),
            ],
        )
        .await;
    let rotated = match rotated {
        // SYNC: access_grant_one_exclusive <-> crate::GROUP's index, exclusive_row, insert_grant
        Err(e) if e.as_db_error().and_then(|db| db.constraint()) == Some("access_grant_one_exclusive") => {
            return Err(signed_in_as_another(pool, tenant, service, &grant).await);
        }
        rotated => rotated?,
    };
    let Some(db::Json(granted)) = rotated else {
        return Err(AccessError::NotFound.into());
    };
    grant.granted = scopes_of(&granted);
    let label = grant.label.clone();
    Ok(completed(id, grant, label))
}

/// The refusal of an upgrade that signed in as an account with its own
/// exclusive row already ([`rotate_grant`]), naming that row.
async fn signed_in_as_another(pool: &Pool, tenant: &str, service: &str, grant: &NewGrant<'_>) -> anyhow::Error {
    let who = grant.identity.as_deref().map_or_else(|| "another account".to_string(), |identity| format!("'{identity}'"));
    // Only a row made through an app is exclusive, so the grant has one.
    let client_id = grant.registration.as_ref().map_or("", |r| r.client_id.as_str());
    let theirs = exclusive_row(pool, tenant, service, grant.identity.as_deref(), client_id, grant.instance.as_ref(), grant.project_id).await;
    let which = match theirs {
        Ok(Some(id)) => format!("its own connection {id}"),
        // Removed since the write met it.
        Ok(None) => "its own connection".to_string(),
        Err(e) => format!("its own connection (which could not be looked up: {e:#})"),
    };
    AccessError::Invalid(format!(
        "you signed in as {who}, which already has {which}; this upgrade is for a different account, so upgrade that \
         connection instead (or sign in as the account this one is for)"
    ))
    .into()
}

/// The SQL value of a row's `granted_scopes` widened by the text array
/// bound at `param`: every permission either holds, once each. Computed
/// in the writing statement, so a concurrent widening is never lost.
fn granted_union_sql(param: &str) -> String {
    format!(
        "(SELECT COALESCE(jsonb_agg(to_jsonb(s) ORDER BY s), '[]'::jsonb)
          FROM (
              SELECT jsonb_array_elements_text(granted_scopes) AS s
              UNION
              SELECT unnest({param}::text[])
          ) merged)"
    )
}

/// The row an exclusive-class service keeps for one account: `tenant`'s
/// connection to `service` as `identity`, made through the app
/// `client_id`, of the same owner (an instance's own row in its project,
/// the author's own, which no project holds). Exclusivity is per app and
/// account, so a row made through another app is never this one. The
/// database holds one at most: [`insert_grant`] lands a second sign-in on
/// it.
// SYNC: the exclusive key <-> crate::GROUP's access_grant_one_exclusive index, insert_grant
pub(crate) async fn exclusive_row(
    db: &impl Client,
    tenant: &str,
    service: &str,
    identity: Option<&str>,
    client_id: &str,
    instance: Option<&weft_core::instance::InstanceId>,
    project_id: Option<uuid::Uuid>,
) -> anyhow::Result<Option<uuid::Uuid>> {
    Ok(db
        .scalar_opt(
            "SELECT id FROM access_grant
             WHERE tenant_id = $1 AND service = $2 AND identity IS NOT DISTINCT FROM $3 AND client_id = $4
               AND instance_id IS NOT DISTINCT FROM $5 AND project_id IS NOT DISTINCT FROM $6
               AND published_by_node IS NULL AND exclusive",
            &[&tenant, &service, &identity, &client_id, &instance.map(|m| m.as_str()), &project_id],
        )
        .await?)
}

/// What a connect that just wrote `grant` as row `id`, named `label`,
/// answers. It was gated on its credential existing (the broker refuses
/// a shared-key connect without one).
fn completed(id: uuid::Uuid, grant: NewGrant<'_>, label: Option<String>) -> CompletedConnect {
    let owner = if grant.platform { CredentialOwner::Platform } else { CredentialOwner::own(grant.instance.clone()) };
    let (expires_at, _) = life_columns(grant.life);
    CompletedConnect {
        grant: GrantSummary {
            id,
            service: grant.spec.service.clone(),
            project_id: grant.project_id,
            instance: grant.instance,
            identity: grant.identity,
            label,
            scopes: grant.granted,
            permissions_verified: grant.verified,
            owner,
            door: grant.door,
            expires_at,
            value_names: grant.values.keys().cloned().collect(),
            has_credential: true,
        },
    }
}

/// The app the row `id` an in-place upgrade is about to overwrite was
/// signed in through, sealed (`None` for one made by pasting a
/// credential). The row must be of the same tenant, the same service (an
/// id naming another provider's row must never take this provider's
/// tokens) and the same owner (an instance's own row in its project, the
/// author's own); anything else is a clean miss.
async fn upgraded_row_app(
    db: &impl Client,
    tenant: &str,
    service: &str,
    id: uuid::Uuid,
    instance: Option<&weft_core::instance::InstanceId>,
    project_id: Option<uuid::Uuid>,
) -> anyhow::Result<Option<String>> {
    let row: Option<Option<String>> = db
        .scalar_opt(
            &format!(
                "SELECT registration_sealed FROM access_grant \
                 WHERE id = $1 AND tenant_id = $2 AND service = $3 AND {}",
                same_owner_sql("$4", "$5")
            ),
            &[&id, &tenant, &service, &instance.map(|m| m.as_str()), &project_id],
        )
        .await?;
    Ok(row.ok_or(AccessError::NotFound)?)
}

// ---------- Resource picker sessions ----------

/// Start a picker session: park the node-declared chooser (script +
/// glue) with the connection it picks against. The editor opens the
/// weft-served picker page for the returned state in the user's
/// browser and polls the parked outcome exactly like a consent's.
// SYNC: BeginPicker <-> packages/weft-graph/src/webview/lib/components/project/editor-connect.ts beginPicker (the picker/begin body)
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BeginPicker {
    /// The instance the pick is for (its own door); `None`: the author.
    /// Set by the dispatcher alone, never taken from the caller.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub for_instance: Option<weft_core::instance::InstanceScope>,
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
    /// The instance the pick is for (its own door); `None`: the author.
    pub for_instance: Option<weft_core::instance::InstanceScope>,
    pub access_id: uuid::Uuid,
    pub service: String,
    pub script: String,
    pub code: String,
    pub mime_types: Vec<String>,
}

/// The one rule for a surface that hands a connection to the caller's
/// own browser (a chooser's raw token, a pick that widens its scopes):
/// it passes only on the caller's OWN connection, the instance's when an
/// instance asks (in that instance's project), the author's otherwise. An
/// instance may resolve the author's shared rows for a run (the token
/// stays server-side), never for a chooser. `row_project` is the
/// connection row's project, `None` when the caller already resolved
/// the row through the instance's own project.
pub fn own_connection_gate(
    owner: &CredentialOwner,
    row_project: Option<uuid::Uuid>,
    for_instance: Option<&weft_core::instance::InstanceScope>,
) -> Result<(), &'static str> {
    let same_project = match (for_instance, row_project) {
        (Some(scope), Some(project)) => scope.project_id == project,
        _ => true,
    };
    if same_project && *owner == CredentialOwner::own(for_instance.map(|s| s.instance.clone())) {
        return Ok(());
    }
    Err(match for_instance {
        Some(_) => "a provider chooser only runs on your own connection; connect your own account for this field",
        None => "a provider chooser only runs on your own connection",
    })
}

pub async fn begin_picker(
    pool: &Pool,
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
    // The chooser signs in with the connection's raw token, and a
    // finished pick widens that connection's scopes, so a picker only
    // opens on the caller's OWN connection (the one rule the broker's
    // token read applies too).
    let row: Option<(String, Option<uuid::Uuid>, Option<String>)> = pool
        .opt("SELECT owner, project_id, instance_id FROM access_grant WHERE tenant_id = $1 AND id = $2", &[&tenant, &req.access_id])
        .await?;
    let Some((owner, project_id, instance)) = row else {
        return Err(AccessError::NotFound.into());
    };
    own_connection_gate(
        &crate::owner_of(&owner, instance.as_deref())?,
        project_id,
        req.for_instance.as_ref(),
    )
    .map_err(|why| AccessError::Invalid(why.into()))?;
    let state = uuid::Uuid::new_v4().simple().to_string();
    pool.execute(
        &format!(
            "{pruned}
             INSERT INTO access_picker
                 (state, tenant_id, access_id, service, script, code, mime_types, grants, project_id, instance_id)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            pruned = pruning("access_picker", "$11"),
        ),
        &[
            &state,
            &tenant,
            &req.access_id,
            &req.service,
            &req.script,
            &req.code,
            &db::Json(db::Names(&req.mime_types)),
            &db::Json(db::Names(&req.grants)),
            &req.for_instance.as_ref().map(|m| m.project_id),
            &req.for_instance.as_ref().map(|m| m.instance.as_str()),
            &(chrono::Utc::now() - CONNECT_TTL),
        ],
    )
    .await?;
    Ok(state)
}

/// The picker page's read: NOT consumed (the page may reload), but
/// dead past the TTL like a pending consent, and deleted when found so.
pub async fn load_picker(pool: &Pool, state: &str) -> anyhow::Result<PickerSession> {
    #[allow(clippy::type_complexity)]
    let row: Option<(String, uuid::Uuid, String, String, String, db::Json<Value>, chrono::DateTime<chrono::Utc>, Option<uuid::Uuid>, Option<String>)> =
        pool.opt(
            "SELECT tenant_id, access_id, service, script, code, mime_types, created_at, project_id, instance_id
             FROM access_picker WHERE state = $1",
            &[&state],
        )
        .await?;
    let Some((tenant, access_id, service, script, code, db::Json(mime_types), created_at, project_id, instance_id)) = row else {
        return Err(AccessError::Invalid(
            "this picker link is unknown or expired; open the picker again from the editor"
                .into(),
        )
        .into());
    };
    if chrono::Utc::now() - created_at > CONNECT_TTL {
        pool.execute("DELETE FROM access_picker WHERE state = $1", &[&state]).await?;
        return Err(AccessError::Invalid(
            "this picker link expired; open the picker again from the editor".into(),
        )
        .into());
    }
    let for_instance = match (project_id, instance_id) {
        (Some(project_id), Some(instance)) => Some(weft_core::instance::InstanceScope {
            project_id,
            instance: weft_core::instance::InstanceId::new(instance).map_err(anyhow::Error::msg)?,
        }),
        _ => None,
    };
    Ok(PickerSession {
        tenant,
        for_instance,
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
/// sees them; `permissions_verified` is untouched. One transaction, so
/// a poll sees the session or its outcome, never neither (which it
/// would read as gone). A session past its life widens nothing: it is
/// as gone as a used one.
pub async fn finish_picker(
    pool: &Pool,
    state: &str,
    result_json: Value,
) -> anyhow::Result<()> {
    let cutoff = chrono::Utc::now() - CONNECT_TTL;
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    #[allow(clippy::type_complexity)]
    let row: Option<(String, uuid::Uuid, db::Json<Value>, Option<uuid::Uuid>, Option<String>)> = tx
        .opt(
            "DELETE FROM access_picker WHERE state = $1 AND created_at >= $2
             RETURNING tenant_id, access_id, grants, project_id, instance_id",
            &[&state, &cutoff],
        )
        .await?;
    let Some((tenant, access_id, db::Json(grants), project_id, instance)) = row else {
        return Err(AccessError::Invalid(
            "this picker link is unknown, already used, or expired; open the picker again from the editor".into(),
        )
        .into());
    };
    let granted = scopes_of(&grants);
    if result_json.get("picked").is_some() && !granted.is_empty() {
        // Only the session owner's own row widens: the gate at begin
        // already refused anything else, and the UPDATE says so too.
        tx.execute(
            &format!(
                "UPDATE access_grant
                 SET granted_scopes = {union}, updated_at = now()
                 WHERE tenant_id = $1 AND id = $2 AND owner = 'their-own' AND {owned}",
                union = granted_union_sql("$3"),
                owned = same_owner_sql("$4", "$5"),
            ),
            &[&tenant, &access_id, &granted, &instance.as_deref(), &project_id],
        )
        .await?;
    }
    tx.execute(
        &format!(
            "{pruned}
             INSERT INTO access_connect_result (state, tenant_id, result_json)
             VALUES ($1, $2, $3)
             ON CONFLICT (state) DO NOTHING",
            pruned = pruning("access_connect_result", "$4"),
        ),
        &[&state, &tenant, &JsonText(&result_json), &cutoff],
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

// ---------- Browser OAuth: begin ----------

/// Park a pending consent and build the consent URL. Requires the
/// resolved app (`req.registration`) and the callback URL
/// (`req.redirect_uri`), both filled before this is reached.
pub async fn begin_oauth(
    pool: &Pool,
    tenant: &str,
    req: BeginOAuth,
) -> anyhow::Result<StartedOAuth> {
    let redirect_uri = req.redirect_uri.as_str();
    if redirect_uri.trim().is_empty() {
        return Err(anyhow::anyhow!("begin_oauth reached with no redirect_uri"));
    }
    instance_needs_project(&req.instance, req.project_id)?;
    instance_not_shared(&req.instance, req.door)?;
    let spec = &req.spec;
    spec.validate().map_err(AccessError::Invalid)?;
    crate::resolve::record_events_recipes(pool, spec).await?;
    let Acquisition::OAuth2 {
        grant: OAuthGrant::AuthorizationCode { auth_url, pkce },
        scope_delimiter,
        auth_params,
        client_params,
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
            // Upgrading is the owner's: an instance upgrades its own row,
            // the author theirs.
            let Some(sealed) = upgraded_row_app(pool, tenant, &spec.service, id, req.instance.as_ref(), req.project_id).await? else {
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
    // The consent URL's extra params read the app's values (validation
    // held them to its non-secret ones).
    let app_values = registration.to_values();
    let extra: Vec<(&String, String)> = auth_params
        .iter()
        .map(|(name, template)| Ok((name, template.resolve(&app_values).map_err(AccessError::Invalid)?)))
        .collect::<Result<_, AccessError>>()?;

    // The ticked permissions, with what the service asks for always (what
    // its test call reads the account by): a grant is checked against
    // the whole of it once it lands.
    let requested = spec.requested_permissions(&req.permissions);
    let state = uuid::Uuid::new_v4().simple().to_string();
    let verifier = pkce.then(|| {
        format!("{}{}", uuid::Uuid::new_v4().simple(), uuid::Uuid::new_v4().simple())
    });

    let mut url: url::Url = auth_url
        .parse()
        .map_err(|e| AccessError::Invalid(format!("bad authUrl '{auth_url}': {e}")))?;
    {
        let mut q = url.query_pairs_mut();
        q.append_pair("response_type", "code");
        q.append_pair(&client_params.id, &client_id);
        q.append_pair("redirect_uri", redirect_uri);
        q.append_pair("state", &state);
        if !requested.is_empty() {
            let delim = scope_delimiter.as_deref().unwrap_or(" ");
            q.append_pair("scope", &requested.join(delim));
        }
        for (k, v) in &extra {
            q.append_pair(k, v);
        }
        if let Some(verifier) = &verifier {
            let challenge = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(Sha256::digest(verifier.as_bytes()));
            q.append_pair("code_challenge", &challenge);
            q.append_pair("code_challenge_method", "S256");
        }
    }

    pool.execute(
        &format!(
            "{pruned}
             INSERT INTO access_connect
               (state, tenant_id, service, registration_sealed, project_id, spec_json, scopes,
                pkce_verifier, door, upgrade_grant_id, redirect_uri, instance_id)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
            pruned = pruning("access_connect", "$13"),
        ),
        &[
            &state,
            &tenant,
            &spec.service,
            &crate::seal_json(&serde_json::to_value(registration)?)?,
            &req.project_id,
            &JsonText(spec),
            &db::Json(db::Names(&requested)),
            &verifier.as_deref().map(crate::seal_str),
            &door_str(req.door),
            &req.upgrade_grant_id,
            &redirect_uri,
            &req.instance.as_ref().map(|m| m.as_str()),
            &(chrono::Utc::now() - CONNECT_TTL),
        ],
    )
    .await?;

    Ok(StartedOAuth { consent_url: url.to_string(), state })
}

// ---------- Browser OAuth: complete (the callback door's body) ----------

/// Complete a parked consent: validate the state nonce, exchange the
/// code, run captures + the test call, enforce ticked ⊆ granted, and
/// write the grant (a new row, or an in-place rotation for an
/// exclusive-class upgrade/re-consent). The outcome (success OR
/// failure) is also parked in `access_connect_result` for the editor's
/// poll, over the in-flight row the claim left there; an unknown state
/// has nothing to park.
// SYNC: the parked result_json <-> packages/weft-connect/src/core/wire.ts ConsentOutcome, crates/weft-cli/src/commands/connect.rs ConnectOutcome
pub async fn complete_oauth(
    pool: &Pool,
    state: &str,
    code: &str,
) -> anyhow::Result<CompletedConnect> {
    let outcome = complete_oauth_inner(pool, state, code).await?;
    let result_json = match &outcome {
        Ok(done) => serde_json::json!({ "grant": done.grant }),
        Err(e) => serde_json::json!({ "error": e.to_string() }),
    };
    pool.execute("UPDATE access_connect_result SET result_json = $2 WHERE state = $1 AND result_json = 'null'", &[&state, &JsonText(&result_json)])
        .await?;
    outcome
}

/// The editor's poll: consume the parked outcome for `state`, walled
/// to the polling tenant. `None` = the flow is live and has not landed
/// yet (a consent or a chooser still open, or a callback mid-exchange).
/// A state with nothing live under it (never started, already taken,
/// or past its time) is `AccessError::Gone`: nothing will ever land,
/// so the poll must stop. The returned JSON is a consent's
/// `{"grant": {...}}` or a chooser's `{"picked": ..}`, or
/// `{"error": "..."}`.
// SYNC: the consent outcome it returns <-> packages/weft-connect/src/core/wire.ts ConsentOutcome, crates/weft-cli/src/commands/connect.rs ConnectOutcome
pub async fn take_connect_result(
    pool: &Pool,
    tenant: &str,
    state: &str,
) -> anyhow::Result<Option<Value>> {
    let cutoff = chrono::Utc::now() - CONNECT_TTL;
    let row: Option<JsonText<Value>> = pool
        .scalar_opt(
            "DELETE FROM access_connect_result
             WHERE state = $1 AND tenant_id = $2 AND result_json <> 'null' AND created_at >= $3
             RETURNING result_json",
            &[&state, &tenant, &cutoff],
        )
        .await?;
    if let Some(JsonText(outcome)) = row {
        return Ok(Some(outcome));
    }
    let live: bool = pool
        .scalar(
            "SELECT EXISTS (SELECT 1 FROM access_connect WHERE state = $1 AND tenant_id = $2 AND created_at >= $3)
                 OR EXISTS (SELECT 1 FROM access_picker WHERE state = $1 AND tenant_id = $2 AND created_at >= $3)
                 OR EXISTS (SELECT 1 FROM access_connect_result WHERE state = $1 AND tenant_id = $2 AND created_at >= $3)",
            &[&state, &tenant, &cutoff],
        )
        .await?;
    if live {
        return Ok(None);
    }
    // Whatever is left under the state is past its life: it goes now.
    pool.execute(
        "WITH a AS (DELETE FROM access_connect WHERE state = $1 AND tenant_id = $2),
              b AS (DELETE FROM access_picker WHERE state = $1 AND tenant_id = $2)
         DELETE FROM access_connect_result WHERE state = $1 AND tenant_id = $2",
        &[&state, &tenant],
    )
    .await?;
    Err(AccessError::Gone(
        "this sign-in is unknown, already finished, or expired; start it again".into(),
    )
    .into())
}

/// A claimed pending-connect row, mid-completion.
struct ClaimedConnect {
    tenant: String,
    service: String,
    registration: AppRegistration,
    project_id: Option<uuid::Uuid>,
    instance: Option<weft_core::instance::InstanceId>,
    spec_json: Value,
    ticked_json: Value,
    verifier: Option<String>,
    door: Door,
    upgrade_grant_id: Option<uuid::Uuid>,
    redirect_uri: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

/// The claim + exchange + grant write. An unknown state is the ONE
/// unparkable failure (no row, so no in-flight result to fill), so only
/// that propagates as the outer `Err`; every failure after the claim is
/// the inner one, parked for the poll.
async fn complete_oauth_inner(
    pool: &Pool,
    state: &str,
    code: &str,
) -> anyhow::Result<anyhow::Result<CompletedConnect>> {
    // Claim the pending row (single use: DELETE .. RETURNING), and in the
    // same statement leave an in-flight result (JSON `null`) in its
    // place: a poll between the claim and the outcome then reads
    // "pending", never "gone". The in-flight row is dated NOW, not
    // with the connect's own start: its TTL counts from the claim, so
    // an exchange that finishes late still parks where the poll reads.
    #[allow(clippy::type_complexity)]
    let row: Option<(
        String,
        String,
        String,
        Option<uuid::Uuid>,
        JsonText<Value>,
        db::Json<Value>,
        Option<String>,
        String,
        Option<uuid::Uuid>,
        String,
        chrono::DateTime<chrono::Utc>,
        Option<String>,
    )> = pool
        .opt(
            &format!(
                "{pruned}, claimed AS (
                 DELETE FROM access_connect WHERE state = $1
                 RETURNING tenant_id, service, registration_sealed, project_id, spec_json, scopes,
                           pkce_verifier, door, upgrade_grant_id, redirect_uri, created_at, instance_id
             ), in_flight AS (
                 INSERT INTO access_connect_result (state, tenant_id, result_json, created_at)
                 SELECT $1::text, tenant_id, 'null', now() FROM claimed
             )
             SELECT * FROM claimed",
                pruned = pruning("access_connect_result", "$2"),
            ),
            &[&state, &(chrono::Utc::now() - CONNECT_TTL)],
        )
        .await?;
    let Some((
        tenant,
        service,
        registration_sealed,
        project_id,
        JsonText(spec_json),
        db::Json(ticked_json),
        verifier,
        door,
        upgrade_grant_id,
        redirect_uri,
        created_at,
        instance,
    )) = row
    else {
        return Err(AccessError::Invalid(
            "this sign-in link is unknown or was already used; start the connect again".into(),
        )
        .into());
    };
    let claimed = (|| -> anyhow::Result<ClaimedConnect> {
        let registration: AppRegistration = serde_json::from_value(crate::open_json(&registration_sealed)?)
            .map_err(|e| anyhow::anyhow!("pending connect row has a malformed app: {e}"))?;
        Ok(ClaimedConnect {
            tenant,
            service,
            registration,
            project_id,
            instance: instance.map(weft_core::instance::InstanceId::new).transpose().map_err(anyhow::Error::msg)?,
            spec_json,
            ticked_json,
            verifier: verifier.as_deref().map(crate::open_str).transpose()?,
            door: door_of(&door)?,
            upgrade_grant_id,
            redirect_uri,
            created_at,
        })
    })();
    Ok(match claimed {
        Ok(claimed) => finish_connect(pool, claimed, code).await,
        Err(e) => Err(e),
    })
}

async fn finish_connect(
    pool: &Pool,
    claimed: ClaimedConnect,
    code: &str,
) -> anyhow::Result<CompletedConnect> {
    let ClaimedConnect {
        tenant,
        service,
        registration,
        project_id,
        instance,
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
    // A connect in flight lives minutes and is no stored document: its
    // recipe is the one this weft's begin wrote.
    let spec: weft_core::AccessSpec = serde_json::from_value(spec_json)
        .map_err(|e| anyhow::anyhow!("a connect in flight holds a recipe that does not read: {e}"))?;
    let ticked = scopes_of(&ticked_json);
    let Acquisition::OAuth2 { token_url, captures, answer, after_exchange, .. } = &spec.acquisition else {
        return Err(anyhow::anyhow!("pending connect row is not an oauth2 spec"));
    };

    let reg = registration.to_values();
    let mut params: Vec<(String, String)> = vec![
        ("grant_type".into(), "authorization_code".into()),
        ("code".into(), code.to_string()),
        ("redirect_uri".into(), redirect_uri),
    ];
    if let Some(verifier) = verifier {
        params.push(("code_verifier".into(), verifier));
    }
    let basic = token_params(&spec, &reg, &mut params)?;
    let resp = token_request(token_url, &params, basic, answer).await.map_err(sign_in_spent)?;

    let mut values: BTreeMap<String, String> = BTreeMap::new();
    let read = read_token(answer, &resp, "the token endpoint")?;
    // The provider's granted-scope echo, when it answers with one: the
    // exchange's, the one answer that is about what was consented to.
    let echo = read.scope.clone();
    let mut expiry = store_token(read, &mut values, AnswerOf::First);
    apply_captures(captures, &resp, &mut values, AnswerOf::First)?;
    // A short-lived token swapped at once for the one to keep.
    if let Some(call) = after_exchange {
        expiry = run_token_call(&spec.service, call, &reg, &mut values, AnswerOf::First)
            .await
            .map_err(sign_in_spent)?;
    }
    let life = connect_expiry(&spec, expiry, &values)?;
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
        run_test_call(spec.test.as_ref(), &spec.auth, &mut values).await.map_err(sign_in_spent)?;
    }
    let identity = resolve_identity(&spec, &values)?;
    let label = Some(registration.label.clone());

    // An instance's connection always lives in its project (whatever the
    // service's class); the author's exclusive-class one is shared by
    // every project, the coexisting one belongs to the connecting one.
    let owner_project = match (&instance, spec.grants) {
        (Some(_), _) => project_id,
        (None, GrantCoexistence::Exclusive) => None,
        (None, GrantCoexistence::Coexisting) => project_id,
    };
    let registration = Some(registration);
    let grant = |granted: Vec<String>, project_id: Option<uuid::Uuid>| NewGrant {
        spec: &spec,
        registration: &registration,
        project_id,
        instance: instance.clone(),
        published_by_node: None,
        values: values.clone(),
        granted,
        verified,
        platform: false,
        door,
        label: label.clone(),
        identity: identity.clone(),
        life,
    };
    // Where the grant lands: an explicit upgrade rotates that row, and
    // everything else goes through the write a pasted connection and a
    // published one take (a consent is a third way a connection comes
    // into being). For an exclusive-class service that write lands on the
    // tenant's row for the SAME app and account when there is one,
    // because the provider structurally rotated it under us anyway.
    // Exclusivity is per app and account, so a row made through a
    // DIFFERENT app (another registered tier, or a pasted hand-made bot
    // with no app at all) holds its own untouched token and is never
    // absorbed. A coexisting service always gets a fresh row.
    match upgrade_grant_id {
        // Every referencing project follows the row, whose permissions
        // only grow (`rotate_grant`).
        Some(id) => rotate_grant(pool, &tenant, &service, id, grant(granted, owner_project)).await,
        None => insert_grant(pool, &tenant, grant(granted, owner_project)).await,
    }
}

// ---------- Shared helpers (also used by resolve) ----------

/// Finish a token call's form for an OAuth2 `spec`: the app's client
/// id and secret where its `tokenAuth` says the endpoint reads them,
/// and its `extraParams`. `Body` pushes the two into the form under
/// the `clientParams` names (the return is `None`); `Basic` hands them
/// back for the request's HTTP Basic header. One helper so the
/// exchange, the refresh and client credentials can never disagree.
pub(crate) fn token_params(
    spec: &AccessSpec,
    reg: &BTreeMap<String, String>,
    params: &mut Vec<(String, String)>,
) -> anyhow::Result<Option<(String, String)>> {
    use weft_core::access::spec::TokenAuth;
    let Acquisition::OAuth2 { token_auth, client_params, extra_params, .. } = &spec.acquisition else {
        return Err(anyhow::anyhow!("token_params on a non-oauth2 spec"));
    };
    let basic = match token_auth {
        TokenAuth::Body => {
            for (stored, wire) in [("client_id", &client_params.id), ("client_secret", &client_params.secret)] {
                if let Some(v) = reg.get(stored) {
                    params.push((wire.clone(), v.clone()));
                }
            }
            None
        }
        TokenAuth::Basic => Some((
            reg.get("client_id").cloned().unwrap_or_default(),
            reg.get("client_secret").cloned().unwrap_or_default(),
        )),
    };
    // Extra params interpolate registration values (Zoom's account_id
    // is pasted at registration time).
    for (name, template) in extra_params {
        params.push((name.clone(), template.resolve(reg).map_err(AccessError::Invalid)?));
    }
    Ok(basic)
}

/// Read a token answer with `answer`, once whoever received it has
/// refused a refusal in it ([`token_request`], or the declared call's
/// own check): the token, refresh token, expiry and scope echo. `from`
/// names the endpoint in errors.
pub(crate) fn read_token(answer: &TokenAnswer, body: &Value, from: &str) -> anyhow::Result<TokenRead> {
    answer
        .read(body, chrono::Utc::now())
        .map_err(|e| AccessError::Invalid(format!("{from}: {e}")).into())
}

/// Store what a token answer gave and hand back the token's expiry:
/// the token, and the refresh token as `of` says. The FIRST answer of a
/// connection is its whole token set, so one carrying no refresh token
/// leaves none (an `afterExchange` answer drops the exchange's, which
/// belongs to the token it discarded). A RENEWAL carrying none keeps the
/// stored one, which a provider that does not rotate leaves valid; one
/// carrying one replaces it.
pub(crate) fn store_token(read: TokenRead, values: &mut BTreeMap<String, String>, of: AnswerOf) -> TokenExpiry {
    values.insert("token".into(), read.token);
    match (read.refresh_token, of) {
        (Some(refresh_token), _) => {
            values.insert("refresh_token".into(), refresh_token);
        }
        (None, AnswerOf::First) => {
            values.remove("refresh_token");
        }
        (None, AnswerOf::Renewal) => {}
    }
    read.expiry
}

/// Run a declared token call (`afterExchange`, a declared `refresh`)
/// over the stored values plus the app's, store what its answer gives
/// (as `of` says), and write its captures back. Hands back the token's
/// expiry. The call's own reader refuses a refusal in its answer.
pub(crate) async fn run_token_call(
    service: &str,
    call: &TokenCall,
    reg: &BTreeMap<String, String>,
    values: &mut BTreeMap<String, String>,
    of: AnswerOf,
) -> anyhow::Result<TokenExpiry> {
    let mut inputs = values.clone();
    for (k, v) in reg {
        inputs.entry(k.clone()).or_insert_with(|| v.clone());
    }
    let resp = crate::run_connect_call(service, call, &inputs).await?;
    let read = read_token(&call.answer, &resp, &call.url.0)?;
    let expiry = store_token(read, values, of);
    apply_captures(&call.captures, &resp, values, of)?;
    Ok(expiry)
}

/// The two `access_grant` columns a token's life is stored in:
/// `expires_at`, and `token_lifetime_secs`, the lifetime the provider
/// stated, which a later renewal answer naming no usable expiry is given
/// again ([`renewed_life`]). Both `None` for a token with no expiry.
pub(crate) fn life_columns(life: Option<TokenLife>) -> (Option<chrono::DateTime<chrono::Utc>>, Option<i64>) {
    (life.map(|l| l.at), life.map(|l| l.lifetime.num_seconds()))
}

/// The expiry a fresh CONNECT stores, read off the last answer (the
/// `afterExchange` call's when there is one). A connection is renewable
/// when it stored a refresh token, declares a renewal call, or is
/// re-requested from client credentials, and a renewable token whose
/// answer names no expiry would never be renewed, so the connect is
/// refused, naming where the recipe should say the expiry is. An expiry
/// the reader declares but the answer lacks or garbles is refused too.
fn connect_expiry(
    spec: &AccessSpec,
    expiry: TokenExpiry,
    values: &BTreeMap<String, String>,
) -> anyhow::Result<Option<TokenLife>> {
    let Acquisition::OAuth2 { grant, answer, after_exchange, refresh, .. } = &spec.acquisition else {
        return Err(anyhow::anyhow!("connect_expiry on a non-oauth2 spec"));
    };
    let renewable = values.contains_key("refresh_token")
        || refresh.is_some()
        || matches!(grant, OAuthGrant::ClientCredentials);
    let (reader, site) = match after_exchange {
        Some(call) => (&call.answer, "acquisition.afterExchange"),
        None => (answer, "the acquisition"),
    };
    match expiry {
        TokenExpiry::At(life) => Ok(Some(life)),
        TokenExpiry::Never => Ok(None),
        TokenExpiry::Unknown if !renewable => Ok(None),
        TokenExpiry::Unknown => Err(AccessError::Invalid(format!(
            "the provider's answer names no expiry at {}, so weft would never know when to \
             renew this connection and it would stop working without a word. If the provider \
             sends the lifetime under another name, declare it on {site} (`expiresInPath` for \
             seconds, `expiresPath` for a time); if the token never expires, declare \
             `neverExpires: true` there",
            reader.expiry_source()
        ))
        .into()),
        TokenExpiry::Unreadable(why) => Err(AccessError::Invalid(format!(
            "{why} (declared on {site}), so weft cannot know when to renew this connection"
        ))
        .into()),
    }
}

/// The life a RENEWED token is stored with. An answer naming no expiry,
/// or one that does not read (a lifetime of zero or a time already past
/// included), lasts `lifetime` again, the one the provider stated for
/// the connection's token before: RFC 6749 (5.1) lets a token answer
/// leave `expires_in` out when the lifetime is documented elsewhere, and
/// a token stored as already dead would be renewed on every use. A
/// connection that never stored a lifetime keeps no expiry.
pub(crate) fn renewed_life(expiry: TokenExpiry, lifetime: Option<chrono::Duration>) -> Option<TokenLife> {
    match expiry {
        TokenExpiry::At(life) => Some(life),
        TokenExpiry::Never => None,
        TokenExpiry::Unknown | TokenExpiry::Unreadable(_) => {
            lifetime.map(|lifetime| TokenLife::lasting(chrono::Utc::now(), lifetime))
        }
    }
}

/// Which answer a connection's values are read from, which decides what
/// a value the answer lacks does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AnswerOf {
    /// The first answer a connection (or a subscription) is made from:
    /// a capture it lacks fails loudly unless `optional`, since it must
    /// carry what the recipe promised, and a refresh token it lacks is
    /// none ([`store_token`]).
    First,
    /// A renewal's answer, often slimmer than the connect's (no team,
    /// no account): a capture or refresh token it lacks keeps the value
    /// already stored, which stays true.
    Renewal,
}

/// A provider not reached while finishing a sign-in, which was claimed
/// (its row is gone) and whose code is single-use: trying again cannot
/// help, and the only way on is a new sign-in.
fn sign_in_spent(e: anyhow::Error) -> anyhow::Error {
    match e.downcast_ref::<AccessError>() {
        Some(AccessError::Unreached(_)) => AccessError::Gone(format!("{e}; the sign-in cannot be finished, start the connect again")).into(),
        _ => e,
    }
}

/// One request to a provider's token endpoint, answered as JSON. Not
/// reached, cut off, or answered busy or down (408, 429, 5xx, or an
/// error code saying so whatever the status: [`RefusalKind::of`])
/// is [`AccessError::Unreached`]; any other refusal, a success-status
/// answer refusing where `answer` says (its `errorPath` and `okPath`)
/// included, is [`AccessError::Invalid`], which the caller names for
/// what it is.
pub(crate) async fn token_request(
    token_url: &str,
    params: &[(String, String)],
    basic: Option<(String, String)>,
    answer: &TokenAnswer,
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
        .map_err(|e| AccessError::Unreached(format!("token endpoint unreachable: {}", e.without_url())))?;
    let status = resp.status();
    let body = crate::read_answer(resp, "the token endpoint").await?;
    let parsed: Option<Value> = serde_json::from_str(&body).ok();
    let paths = answer.refusal_paths();
    if !status.is_success() {
        let answered = format!("token endpoint answered {status}: {}", head_of(&body));
        let kind = RefusalKind::of(status, paths, parsed.as_ref().unwrap_or(&Value::Null));
        return Err(crate::refusal(kind, answered, AccessError::Invalid).into());
    }
    let parsed = parsed.ok_or_else(|| AccessError::Invalid(format!(
        "token endpoint answered non-JSON: {}",
        head_of(&body)
    )))?;
    // A refusal answered with a success status, where the recipe's
    // reader says it sits, classified by its code like any other answer
    // (so a busy provider is tried again).
    if let Some(why) = paths.refusal(&parsed) {
        return Err(crate::refusal(
            RefusalKind::of(status, paths, &parsed),
            format!("the token endpoint refused: {why}"),
            AccessError::Invalid,
        )
        .into());
    }
    Ok(parsed)
}

pub(crate) fn apply_captures(
    captures: &[weft_core::access::spec::Capture],
    resp: &Value,
    values: &mut BTreeMap<String, String>,
    of: AnswerOf,
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
            None if c.optional || of == AnswerOf::Renewal => {}
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
    let url = test.url.resolve_url(values).map_err(AccessError::Invalid)?;
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
            AccessError::Unreached(format!("test call to {} failed: {}", test.url.0, weft_core::access::client::send_error(e)))
        })?;
    let status = resp.status();
    let body: Value = crate::read_json_answer(resp, &test.url.0).await?;
    let paths = test.answer.refusal_paths();
    let refused = if status.as_u16() != test.expect_status {
        Some(format!(
            "the credential did not pass the service's test call ({} answered {status}, \
             expected {})",
            test.url.0, test.expect_status
        ))
    } else {
        // A refusal answered with the expected status, where the test
        // call's reader says it sits.
        paths.refusal(&body).map(|why| format!("the credential did not pass the service's test call: {why}"))
    };
    if let Some(why) = refused {
        // A provider busy or down (429, 5xx, Slack's `ratelimited`)
        // said nothing about the credential: the connect can be tried
        // again. Any other refusal is of the credential.
        return Err(crate::refusal(RefusalKind::of(status, paths, &body), why, AccessError::Invalid).into());
    }
    apply_captures(&test.captures, &body, values, AnswerOf::First)
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

    /// A token endpoint answering every request with `status` and `body`;
    /// returns its URL.
    async fn token_endpoint(status: u16, body: &'static str) -> String {
        let app = axum::Router::new().route(
            "/token",
            axum::routing::post(move || async move { (axum::http::StatusCode::from_u16(status).unwrap(), body) }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}/token")
    }

    /// A token endpoint busy or down (408, 429, 5xx), or never reached,
    /// refused nothing, so the request is answered as not reached and
    /// tried again; any other refusal is of the request.
    #[tokio::test]
    async fn a_token_endpoint_busy_or_down_is_not_reached_and_a_refusal_is_invalid() {
        let oauth = TokenAnswer::default();
        for status in [408, 429, 500, 502, 503] {
            let err = token_request(&token_endpoint(status, "busy").await, &[], None, &oauth).await.unwrap_err();
            assert!(matches!(err.downcast_ref::<AccessError>(), Some(AccessError::Unreached(_))), "{status}: {err:#}");
        }
        for status in [400, 401, 403, 404] {
            let err = token_request(&token_endpoint(status, r#"{"error":"invalid_grant"}"#).await, &[], None, &oauth).await.unwrap_err();
            assert!(matches!(err.downcast_ref::<AccessError>(), Some(AccessError::Invalid(_))), "{status}: {err:#}");
        }
        // A refusal answered 200 is seen where the reader says, and read
        // by its code: busy is tried again, anything else is of the
        // request. Slack's reader declares its success flag.
        let slack: TokenAnswer = serde_json::from_value(serde_json::json!({ "okPath": "ok" })).unwrap();
        let instagram: TokenAnswer = serde_json::from_value(serde_json::json!({ "errorPath": "error_type" })).unwrap();
        let refused_200 = |reader: TokenAnswer, body: &'static str| async move {
            token_request(&token_endpoint(200, body).await, &[], None, &reader).await.unwrap_err()
        };
        for (reader, body) in [
            (slack.clone(), r#"{"ok":false,"error":"ratelimited"}"#),
            (oauth.clone(), r#"{"error":"temporarily_unavailable"}"#),
            (instagram.clone(), r#"{"error_type":"temporarily_unavailable"}"#),
        ] {
            let err = refused_200(reader, body).await;
            assert!(matches!(err.downcast_ref::<AccessError>(), Some(AccessError::Unreached(_))), "{body}: {err:#}");
        }
        for (reader, body) in [
            (slack.clone(), r#"{"ok":false,"error":"invalid_auth"}"#),
            (slack.clone(), r#"{"ok":false}"#),
            (oauth.clone(), r#"{"error":"invalid_grant"}"#),
            (oauth.clone(), r#"{"error":{"message":"bad code","type":"OAuthException"}}"#),
            (instagram.clone(), r#"{"error_type":"OAuthException","error":"ratelimited"}"#),
        ] {
            let err = refused_200(reader, body).await;
            assert!(matches!(err.downcast_ref::<AccessError>(), Some(AccessError::Invalid(_))), "{body}: {err:#}");
        }
        assert!(refused_200(oauth.clone(), r#"{"error":{"message":"bad code"}}"#).await.to_string().contains("bad code"));
        // A success flag nobody declared is just data.
        let ok = token_request(&token_endpoint(200, r#"{"ok":false,"access_token":"t"}"#).await, &[], None, &oauth).await.unwrap();
        assert_eq!(ok["access_token"], "t");
        // Nothing listens on the port once its listener is dropped.
        let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap();
        let err = token_request(&format!("http://{closed}/token"), &[], None, &oauth).await.unwrap_err();
        assert!(matches!(err.downcast_ref::<AccessError>(), Some(AccessError::Unreached(_))), "{err:#}");
        let ok = token_request(&token_endpoint(200, r#"{"access_token":"t"}"#).await, &[], None, &oauth).await.unwrap();
        assert_eq!(ok["access_token"], "t");
    }

    /// A test call the provider answers busy or down (429, 5xx, or a 200
    /// whose code says so, Slack's `ratelimited`) refused nothing about
    /// the credential, so it is answered as not reached; a refusal of
    /// the credential stays invalid.
    #[tokio::test]
    async fn a_test_call_busy_or_down_is_not_reached_and_a_refusal_is_invalid() {
        let test = |url: String, reader: serde_json::Value| -> TestCall {
            let mut raw = serde_json::json!({ "url": url, "method": "POST" });
            raw.as_object_mut().unwrap().extend(reader.as_object().unwrap().clone());
            serde_json::from_value(raw).unwrap()
        };
        let slack = serde_json::json!({ "okPath": "ok", "errorPath": "error" });
        let run = |call: TestCall| async move { run_test_call(Some(&call), &[], &mut BTreeMap::new()).await };
        for status in [429, 500, 503] {
            let err = run(test(token_endpoint(status, "busy").await, serde_json::json!({}))).await.unwrap_err();
            assert!(matches!(err.downcast_ref::<AccessError>(), Some(AccessError::Unreached(_))), "{status}: {err:#}");
        }
        let err = run(test(token_endpoint(200, r#"{"ok":false,"error":"ratelimited"}"#).await, slack.clone()))
            .await
            .unwrap_err();
        assert!(matches!(err.downcast_ref::<AccessError>(), Some(AccessError::Unreached(_))), "{err:#}");
        for (status, body) in [(401, "{}"), (200, r#"{"ok":false,"error":"invalid_auth"}"#)] {
            let err = run(test(token_endpoint(status, body).await, slack.clone())).await.unwrap_err();
            assert!(matches!(err.downcast_ref::<AccessError>(), Some(AccessError::Invalid(_))), "{status} {body}: {err:#}");
        }
        run(test(token_endpoint(200, r#"{"ok":true}"#).await, slack)).await.expect("a passing test call");
    }

    /// A renewal answer naming no expiry, or one that does not read,
    /// lasts the lifetime the connection last had, so it is never stored
    /// as already dead; a connection that never stored one keeps none.
    #[test]
    fn a_renewal_naming_no_usable_expiry_reuses_the_known_lifetime() {
        let hour = chrono::Duration::hours(1);
        for expiry in [TokenExpiry::Unknown, TokenExpiry::Unreadable("garbled".into())] {
            let life = renewed_life(expiry, Some(hour)).expect("an expiry");
            assert_eq!(life.lifetime, hour, "the stated lifetime is kept whole, never re-measured");
            let off = (life.at - (chrono::Utc::now() + hour)).num_seconds().abs();
            assert!(off <= 2, "{}", life.at);
        }
        assert_eq!(renewed_life(TokenExpiry::Unknown, None), None);
        assert_eq!(renewed_life(TokenExpiry::Never, Some(hour)), None);
        let stated = TokenLife::lasting(chrono::Utc::now(), chrono::Duration::seconds(60));
        assert_eq!(renewed_life(TokenExpiry::At(stated), Some(hour)), Some(stated));
        assert_eq!(life_columns(Some(stated)), (Some(stated.at), Some(60)), "the stored lifetime is the stated one");
        assert_eq!(life_columns(None), (None, None));
    }

    /// A connect refuses a renewable token whose answer names no expiry
    /// (nothing would ever renew it), naming where to declare one, and an
    /// expiry that does not read; a token nothing renews lasts.
    #[test]
    fn a_connect_refuses_a_renewable_token_with_no_known_expiry() {
        let spec: AccessSpec = serde_json::from_value(serde_json::json!({
            "service": "fakeoauth",
            "acquisition": {
                "kind": "oauth2",
                "grant": { "kind": "authorization_code", "authUrl": "https://p/auth" },
                "tokenUrl": "https://p/token"
            },
            "auth": [{ "kind": "header", "name": "Authorization", "value": "Bearer {token}" }],
        }))
        .expect("spec parses");
        let renewable = BTreeMap::from([("refresh_token".to_string(), "r".to_string())]);
        let err = connect_expiry(&spec, TokenExpiry::Unknown, &renewable).unwrap_err().to_string();
        assert!(err.contains("expiresInPath") && err.contains("neverExpires"), "{err}");
        let err = connect_expiry(&spec, TokenExpiry::Unreadable("garbled".into()), &renewable).unwrap_err().to_string();
        assert!(err.contains("garbled"), "{err}");
        assert_eq!(connect_expiry(&spec, TokenExpiry::Unknown, &BTreeMap::new()).unwrap(), None);
        assert_eq!(connect_expiry(&spec, TokenExpiry::Never, &renewable).unwrap(), None);
    }

    /// The first answer is the connection's whole token set: no refresh
    /// token in it leaves none (an `afterExchange` answer drops the
    /// exchange's); a renewal without one keeps the stored one.
    #[test]
    fn a_first_answer_replaces_the_refresh_token_and_a_renewal_keeps_it() {
        let read = |refresh: Option<&str>| TokenRead {
            token: "t2".into(),
            refresh_token: refresh.map(str::to_string),
            expiry: TokenExpiry::Unknown,
            scope: None,
        };
        let stored = || BTreeMap::from([("token".to_string(), "t1".to_string()), ("refresh_token".to_string(), "r1".to_string())]);
        let mut values = stored();
        store_token(read(None), &mut values, AnswerOf::First);
        assert_eq!(values.get("refresh_token"), None, "the exchange's refresh token belonged to the discarded token");
        assert_eq!(values["token"], "t2");
        let mut values = stored();
        store_token(read(None), &mut values, AnswerOf::Renewal);
        assert_eq!(values["refresh_token"], "r1");
        let mut values = stored();
        store_token(read(Some("r2")), &mut values, AnswerOf::Renewal);
        assert_eq!(values["refresh_token"], "r2");
    }

    /// A chooser gets the raw token, so only the caller's own
    /// connection passes: never the runtime's, and never the author's
    /// for an instance, even though an instance's RUN may use it.
    #[test]
    fn the_picker_gate_passes_only_the_callers_own_connection() {
        use weft_core::instance::{InstanceId, InstanceScope};
        let ada = InstanceId::new("ada").unwrap();
        let as_ada = InstanceScope { project_id: uuid::Uuid::from_u128(1), instance: ada.clone() };
        assert!(own_connection_gate(&CredentialOwner::Author, None, None).is_ok());
        assert!(own_connection_gate(&CredentialOwner::Platform, None, None).is_err());
        assert!(own_connection_gate(&CredentialOwner::Instance(ada.clone()), None, Some(&as_ada)).is_ok());
        let err = own_connection_gate(&CredentialOwner::Author, None, Some(&as_ada)).unwrap_err();
        assert!(err.contains("connect your own account"), "{err}");
        assert!(own_connection_gate(&CredentialOwner::Platform, None, Some(&as_ada)).is_err());
        let bob = InstanceId::new("bob").unwrap();
        assert!(own_connection_gate(&CredentialOwner::Instance(bob), None, Some(&as_ada)).is_err());
        // Ada's name in another project is another person.
        let other = Some(uuid::Uuid::from_u128(2));
        assert!(own_connection_gate(&CredentialOwner::Instance(ada.clone()), other, Some(&as_ada)).is_err());
    }


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

    /// A connection left unnamed is named after its app, else its
    /// service, so no list ever shows a nameless row a person made.
    #[test]
    fn an_unnamed_connection_is_named_after_its_app_else_its_service() {
        let spec: AccessSpec = serde_json::from_value(serde_json::json!({
            "service": "firecrawl",
            "label": "Firecrawl",
            "acquisition": { "kind": "static", "fields": [{ "name": "api_key" }] },
        }))
        .expect("a static spec");
        let app = Some(AppRegistration {
            label: "Acme app".into(),
            client_id: "id".into(),
            client_secret: None,
            extra: BTreeMap::new(),
        });
        assert_eq!(connection_name(Some(" acme-scraper ".into()), &app, &spec), "acme-scraper");
        assert_eq!(connection_name(Some("  ".into()), &app, &spec), "Acme app");
        assert_eq!(connection_name(None, &None, &spec), "Firecrawl");
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
    pub project_id: uuid::Uuid,
    /// The node doing the publishing. Its connection, so a second
    /// publish updates that one row instead of piling up a new one,
    /// and terminating the node takes it away with it.
    pub node_id: String,
    /// Whose copy of the node published it: an instance's copy publishes
    /// a connection of that instance's, `None` for the shared copy.
    #[serde(default)]
    pub instance: Option<weft_core::instance::InstanceId>,
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
/// HTTP request the tenant chose, from inside the install; refusing
/// those shapes is what makes that unreachable rather than unlikely.
///
/// Always the user's own credential: nothing published can resolve to
/// the runtime's, whatever the caller sends.
///
/// A publish that would change nothing writes nothing: a node publishes on
/// every run, and a write would tell every worker of the project that the
/// connection changed (`ACCESS_CHANNEL`), so none could keep it.
pub async fn publish_grant(
    pool: &Pool,
    tenant: &str,
    req: PublishAccess,
) -> anyhow::Result<PublishedConnection> {
    publish_grant_in(&pool.get().await?, tenant, req).await
}

/// [`publish_grant`] on the caller's connection, so a write that must land
/// with others (a push carrying both a connection's values and baked
/// outputs) commits or fails as one.
async fn publish_grant_in(
    db: &impl Client,
    tenant: &str,
    req: PublishAccess,
) -> anyhow::Result<PublishedConnection> {
    let PublishAccess { spec, project_id, node_id, instance, values, label } = req;
    spec.validate().map_err(AccessError::Invalid)?;
    let fields =
        weft_core::access::spec::publishable_fields(&spec).map_err(AccessError::Invalid)?;
    let values = storable_values(&spec, fields, values)?;
    let identity = resolve_identity(&spec, &values)?;
    let current: Option<Published> = db
        .opt(
            "SELECT id, spec, spec_version, values_sealed, label, identity FROM access_grant
             WHERE tenant_id = $1 AND project_id = $2 AND published_by_node = $3
               AND service = $4 AND instance_id IS NOT DISTINCT FROM $5",
            &[&tenant, &project_id, &node_id, &spec.service, &instance.as_ref().map(|m| m.as_str())],
        )
        .await?;
    if let Some(current) = current {
        // Every other column a publish writes follows from these (it
        // stores no app, no scopes and no expiry), so equal ones mean an
        // equal row. Stored values that no longer open are not equal to
        // anything: the publish writes the row over them.
        let unchanged = current.spec_version == weft_core::AccessSpec::STORED.version()
            && current.spec == serde_json::to_value(&spec)?
            && crate::open_json(&current.values_sealed).is_ok_and(|stored| crate::values_of(&stored) == values)
            && current.label == label
            && current.identity == identity;
        if unchanged {
            return Ok(PublishedConnection { connection_id: current.id.to_string(), identity: current.identity });
        }
    }
    let done = insert_grant(
        db,
        tenant,
        NewGrant {
            spec: &spec,
            registration: &None,
            project_id: Some(project_id),
            instance,
            published_by_node: Some(node_id),
            values,
            granted: Vec::new(),
            verified: false,
            platform: false,
            door: Door::Own,
            label,
            identity,
            life: None,
        },
    )
    .await?;
    Ok(PublishedConnection { connection_id: done.grant.id.to_string(), identity: done.grant.identity })
}

/// Write what an infra copy said changed of what its node handed weft
/// (`weft_core::infra::bake::PushedValues`), with no node running:
/// `connection` over the values of the connection the node published,
/// through the same publish a node makes (so workers hear it), and
/// `outputs` over the baked outputs saved with the copy
/// (`infra_node.baked_json`). Only what the node handed weft already can
/// change: a key the connection does not store, or an output the copy did
/// not bake, is refused as [`AccessError::Invalid`], naming it, and so is a
/// copy that is gone. Pushes to one copy take turns, so two at once never
/// lose each other's keys, and a push lands whole or not at all: both
/// writes share one transaction.
pub async fn write_pushed_values(
    pool: &Pool,
    project_id: uuid::Uuid,
    copy_id: &str,
    values: &weft_core::infra::bake::PushedValues,
) -> anyhow::Result<()> {
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    /// node, instance, what it baked.
    type CopyRow = (String, Option<String>, JsonText<BTreeMap<String, Value>>);
    let copy: Option<CopyRow> = tx
        .opt(
            "SELECT node_id, instance_id, baked_json FROM infra_node WHERE project_id = $1 AND copy_id = $2 FOR UPDATE",
            &[&project_id, &copy_id],
        )
        .await?;
    let Some((node_id, instance_id, JsonText(mut baked))) = copy else {
        return Err(AccessError::Invalid(format!("infra copy {copy_id} is no longer running here, so nothing it says changed can be written")).into());
    };
    if let Some(stray) = values.outputs.keys().find(|port| !baked.contains_key(port.as_str())) {
        return Err(AccessError::Invalid(format!("{node_id} has no baked output '{stray}', so the value its infrastructure sent for it has nowhere to go")).into());
    }
    if !values.connection.is_empty() {
        #[allow(clippy::type_complexity)]
        let published: Vec<(JsonText<Value>, weft_core::stored::Version, String, Option<String>, String)> = tx
            .all(
                "SELECT g.spec, g.spec_version, g.values_sealed, g.label, g.tenant_id FROM access_grant g JOIN project p ON p.tenant_id = g.tenant_id \
                 WHERE p.id = $1 AND g.project_id = $1 AND g.published_by_node = $2 AND g.instance_id IS NOT DISTINCT FROM $3",
                &[&project_id, &node_id, &instance_id],
            )
            .await?;
        let (JsonText(spec), version, sealed, label, tenant) = match published.as_slice() {
            [one] => one,
            [] => return Err(AccessError::Invalid(format!("{node_id} has published no connection, so the connection values its infrastructure sent have nowhere to go")).into()),
            _ => return Err(AccessError::Invalid(format!("{node_id} published several connections, so the connection values its infrastructure sent name none of them")).into()),
        };
        let mut stored = crate::values_of(&crate::open_json(sealed)?);
        if let Some(stray) = values.connection.keys().find(|key| !stored.contains_key(key.as_str())) {
            return Err(AccessError::Invalid(format!(
                "{node_id}'s connection stores no '{stray}', so the value its infrastructure sent for it has nowhere to go (it stores {})",
                stored.keys().map(String::as_str).collect::<Vec<_>>().join(", ")
            ))
            .into());
        }
        stored.extend(values.connection.clone());
        let spec = crate::spec_of(format_args!("the connection {node_id} published"), spec.clone(), *version)?;
        publish_grant_in(
            &tx,
            tenant,
            PublishAccess {
                spec,
                project_id,
                node_id: node_id.clone(),
                instance: instance_id.map(weft_core::instance::InstanceId::new).transpose().map_err(anyhow::Error::msg)?,
                values: stored,
                label: label.clone(),
            },
        )
        .await?;
    }
    if !values.outputs.is_empty() {
        // Merged here, on the row locked above: Postgres reads no JSON
        // holding a NUL character, and a baked value may hold one.
        baked.extend(values.outputs.clone());
        tx.execute(
            "UPDATE infra_node SET baked_json = $3 WHERE project_id = $1 AND copy_id = $2",
            &[&project_id, &copy_id, &JsonText(&baked)],
        )
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// What a node's published connection holds now, as a publish compares it.
struct Published {
    id: uuid::Uuid,
    spec: Value,
    spec_version: weft_core::stored::Version,
    values_sealed: String,
    label: Option<String>,
    identity: Option<String>,
}

impl FromRow for Published {
    fn from_row(row: Row) -> db::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            spec: row.try_get::<_, JsonText<Value>>("spec")?.0,
            spec_version: row.try_get("spec_version")?,
            values_sealed: row.try_get("values_sealed")?,
            label: row.try_get("label")?,
            identity: row.try_get("identity")?,
        })
    }
}

/// The connection this node published for `service`, if it has one.
/// The node's own read-back path: it republishes with the credential
/// it stored the first time rather than asking the container again.
/// Answers the same [`PublishedConnection`] shape the publish did.
pub async fn published_connection(
    pool: &Pool,
    tenant: &str,
    project_id: uuid::Uuid,
    node_id: &str,
    instance: Option<&weft_core::instance::InstanceId>,
    service: &str,
) -> anyhow::Result<Option<PublishedConnection>> {
    let row: Option<(uuid::Uuid, Option<String>)> = pool
        .opt(
            "SELECT id, identity FROM access_grant
             WHERE tenant_id = $1 AND project_id = $2 AND published_by_node = $3
               AND service = $4 AND instance_id IS NOT DISTINCT FROM $5",
            &[&tenant, &project_id, &node_id, &service, &instance.map(|m| m.as_str())],
        )
        .await?;
    Ok(row.map(|(id, identity)| PublishedConnection { connection_id: id.to_string(), identity }))
}

/// Drop every connection a node published. Called when its infra is
/// terminated: the credentials open something that no longer exists,
/// and a connection nobody can use is junk.
/// Takes any client rather than the pool so the caller can run it
/// inside the transaction that removes the node itself: the node and
/// the connection it published go away together or not at all.
///
/// `copy` names the node and whose copy of it (`None` for the shared
/// one); `None` drops every published connection in the project, for
/// the path where the whole project goes (there is no node left to
/// name). A connection a PERSON made is never touched by either: it is
/// theirs, listed and deletable where they made it.
pub async fn delete_published_grants(
    db: &impl Client,
    tenant: &str,
    project_id: uuid::Uuid,
    copy: Option<(&str, Option<&weft_core::instance::InstanceId>)>,
) -> anyhow::Result<u64> {
    let (node_id, instance) = match copy {
        Some((node, instance)) => (Some(node), instance),
        None => (None, None),
    };
    Ok(db
        .execute(
            "DELETE FROM access_grant
             WHERE tenant_id = $1 AND project_id = $2 AND published_by_node IS NOT NULL
               AND ($3::text IS NULL OR (published_by_node = $3 AND instance_id IS NOT DISTINCT FROM $4))",
            &[&tenant, &project_id, &node_id, &instance.map(|m| m.as_str())],
        )
        .await?)
}

// ---------- Instance values (what an instance provides for `@instance_filled`) ----------

/// One value an instance provides: `value` for `field` of the node at `node`
/// (its place, spelled). `connection` is set when the field is a
/// connection field: the value is then the `{id, ..}` handle of one of
/// the instance's own connections to that service.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InstanceValueWrite {
    pub node: String,
    pub field: String,
    pub value: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection: Option<ConnectionValue>,
}

/// The connection a connection field's value names, and the service the
/// field connects to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionValue {
    pub grant_id: uuid::Uuid,
    pub service: String,
}

/// Store `writes` and forget `clears` (node, field) for instance `instance`
/// of `project_id`, all or none, on `db` (the caller's transaction).
/// A connection value must be the instance's own connection in that
/// project and of the field's service, or the whole change is refused: a
/// value naming somebody else's connection would spend their account on
/// this instance's runs. A cleared field falls back as if never given.
pub async fn change_instance_values(
    db: &impl Client,
    tenant: &str,
    project_id: uuid::Uuid,
    instance: &weft_core::instance::InstanceId,
    writes: &[InstanceValueWrite],
    clears: &[(String, String)],
) -> anyhow::Result<()> {
    for write in writes {
        let Some(connection) = &write.connection else { continue };
        let owned: Option<String> = db
            .scalar_opt(
                "SELECT service FROM access_grant
                 WHERE id = $1 AND tenant_id = $2 AND project_id = $3 AND instance_id = $4",
                &[&connection.grant_id, &tenant, &project_id, &instance.as_str()],
            )
            .await?;
        let Some(grant_service) = owned else {
            return Err(AccessError::Invalid(format!(
                "connection {} is not one of instance '{instance}''s connections in this project",
                connection.grant_id
            ))
            .into());
        };
        if grant_service != connection.service {
            return Err(AccessError::Invalid(format!(
                "'{}.{}' connects to '{}', and connection {} is a '{grant_service}' one",
                write.node, write.field, connection.service, connection.grant_id
            ))
            .into());
        }
    }
    for write in writes {
        db.execute(
            "INSERT INTO instance_value (tenant_id, project_id, instance_id, node, field, value, grant_id)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (project_id, instance_id, node, field) DO UPDATE
               SET value = EXCLUDED.value, grant_id = EXCLUDED.grant_id, set_at = now()",
            &[
                &tenant,
                &project_id,
                &instance.as_str(),
                &write.node,
                &write.field,
                &JsonText(&write.value),
                &write.connection.as_ref().map(|c| c.grant_id),
            ],
        )
        .await?;
    }
    for (node, field) in clears {
        db.execute(
            "DELETE FROM instance_value
             WHERE tenant_id = $1 AND project_id = $2 AND instance_id = $3 AND node = $4 AND field = $5",
            &[&tenant, &project_id, &instance.as_str(), node, field],
        )
        .await?;
    }
    Ok(())
}

/// Every value instance `instance` of `project_id` provides, by node and
/// field. A connection value comes back as the handle a source pick
/// holds (`{id, identity}`), its identity read from the connection now.
pub async fn instance_values(
    db: &impl Client,
    tenant: &str,
    project_id: uuid::Uuid,
    instance: &weft_core::instance::InstanceId,
) -> anyhow::Result<weft_core::instance::InstanceValues> {
    /// node, field, value, the connection it names, that connection's identity.
    type ValueRow = (String, String, JsonText<Value>, Option<uuid::Uuid>, Option<String>);
    let rows: Vec<ValueRow> = db
        .all(
            "SELECT v.node, v.field, v.value, v.grant_id, g.identity FROM instance_value v
             LEFT JOIN access_grant g ON g.id = v.grant_id
             WHERE v.tenant_id = $1 AND v.project_id = $2 AND v.instance_id = $3",
            &[&tenant, &project_id, &instance.as_str()],
        )
        .await?;
    let mut values = weft_core::instance::InstanceValues::new();
    for (node, field, JsonText(value), grant_id, identity) in rows {
        let value = match grant_id {
            Some(id) => connection_handle(id, identity.as_deref()),
            None => value,
        };
        values.entry(node).or_default().insert(field, value);
    }
    Ok(values)
}

/// How many values each instance of `project_id` gave, one entry per instance
/// with at least one.
pub async fn instance_value_counts(
    db: &impl Client,
    tenant: &str,
    project_id: uuid::Uuid,
) -> anyhow::Result<Vec<(weft_core::instance::InstanceId, u32)>> {
    let rows: Vec<(String, i64)> = db
        .all(
            "SELECT instance_id, count(*)::bigint FROM instance_value
             WHERE tenant_id = $1 AND project_id = $2 GROUP BY instance_id ORDER BY instance_id",
            &[&tenant, &project_id],
        )
        .await?;
    counted_by_instance(rows, "instance_value")
}

/// How many connections each instance of `project_id` made there, one entry
/// per instance with at least one.
pub async fn instance_connection_counts(
    db: &impl Client,
    tenant: &str,
    project_id: uuid::Uuid,
) -> anyhow::Result<Vec<(weft_core::instance::InstanceId, u32)>> {
    let rows: Vec<(String, i64)> = db
        .all(
            "SELECT instance_id, count(*)::bigint FROM access_grant
             WHERE tenant_id = $1 AND project_id = $2 AND instance_id IS NOT NULL
             GROUP BY instance_id ORDER BY instance_id",
            &[&tenant, &project_id],
        )
        .await?;
    counted_by_instance(rows, "access_grant")
}

fn counted_by_instance(rows: Vec<(String, i64)>, table: &str) -> anyhow::Result<Vec<(weft_core::instance::InstanceId, u32)>> {
    rows.into_iter()
        .map(|(instance, n)| {
            let instance = weft_core::instance::InstanceId::new(instance).map_err(|e| anyhow::anyhow!("{table}.instance_id: {e}"))?;
            Ok((instance, u32::try_from(n)?))
        })
        .collect()
}

/// The handle a connection field holds: what a pick in the editor writes
/// into the source, and what an instance's connection value reads back as.
pub fn connection_handle(id: uuid::Uuid, identity: Option<&str>) -> Value {
    match identity {
        Some(identity) => serde_json::json!({ "id": id, "identity": identity }),
        None => serde_json::json!({ "id": id }),
    }
}

/// Forget the access removing `project_id` strands: every instance's
/// connections (the values naming them go with them) and the install's
/// picks for the project. Nobody could reach either once the project is
/// gone (the author's list never shows an instance's connection). A
/// connection the author made is theirs and stays.
///
/// An instance's consent or pick still in flight goes too: finishing after
/// the removal would write a grant nobody can reach. The author's own
/// stay, for the same reason their connections do.
pub async fn forget_project_access(db: &impl Client, project_id: uuid::Uuid) -> anyhow::Result<()> {
    for table in ["access_connect", "access_picker"] {
        db.execute(&format!("DELETE FROM {table} WHERE project_id = $1 AND instance_id IS NOT NULL"), &[&project_id]).await?;
    }
    db.execute("DELETE FROM instance_value WHERE project_id = $1", &[&project_id]).await?;
    db.execute("DELETE FROM install_pick WHERE project_id = $1", &[&project_id]).await?;
    db.execute("DELETE FROM access_grant WHERE project_id = $1 AND instance_id IS NOT NULL", &[&project_id]).await?;
    Ok(())
}

/// Forget every connection of instance `instance` of `project_id` (the
/// values naming them go with them). What `ctx.connections().instance(..).forget()`
/// does; answers how many connections went.
pub async fn forget_instance_grants(
    pool: &Pool,
    tenant: &str,
    project_id: uuid::Uuid,
    instance: &weft_core::instance::InstanceId,
) -> anyhow::Result<u64> {
    Ok(pool
        .execute(
            "DELETE FROM access_grant WHERE tenant_id = $1 AND project_id = $2 AND instance_id = $3",
            &[&tenant, &project_id, &instance.as_str()],
        )
        .await?)
}
