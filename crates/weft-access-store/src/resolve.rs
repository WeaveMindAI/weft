//! Resolution side: hand a WORKER the short-lived pieces it needs
//! (lazy refresh, single-flight), and run the editor's declarative
//! resource lookups through a stored access.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use weft_task_store::db::{self, Client, FromRow, JsonText, Pool, Row};

use sha2::Digest;

use weft_core::access::client::{authed_client, base_client, resolve_steps, RefusalKind};
use weft_core::access::events::EventsSpec;
use weft_core::access::spec::{
    lookup_path, AccessSpec, Acquisition, AnswerReader, AppRegistration, Door, OAuthGrant, Template,
    TemplateError, TokenExpiry, TokenLife,
};
use weft_core::net::EmptyBody;
use weft_core::node::Lookup;

use crate::flows::{
    apply_captures, life_columns, read_token, renewed_life, run_token_call, store_token, token_params,
    token_request, AnswerOf,
};
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
    /// Until when the values handed over stay the ones a resolve would
    /// hand: the moment they are due a refresh, `None` when they never
    /// are. What lets a worker keep them (the broker's
    /// `ResolveConnectionResponse::keep_until_unix`).
    pub fresh_until: Option<chrono::DateTime<chrono::Utc>>,
    /// The infra node (spelled) that published the connection, when one
    /// did: its values were made by that node's infra setup, and a failure
    /// using them may mean they changed where weft could not see.
    pub published_by: Option<String>,
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
    owner: &weft_core::CredentialOwner,
    spec: &AccessSpec,
    required: &[String],
) -> Option<String> {
    if !owner.is_platform() {
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

/// The `access_grant` columns a walled read selects.
struct GrantRow {
    tenant_id: String,
    service: String,
    registration_sealed: Option<String>,
    spec: Value,
    spec_version: weft_core::stored::Version,
    values_sealed: String,
    granted_scopes: Value,
    permissions_verified: bool,
    owner: String,
    door: String,
    identity: Option<String>,
    provider_account: Option<String>,
    expires_at: Option<chrono::DateTime<chrono::Utc>>,
    token_lifetime_secs: Option<i64>,
    instance_id: Option<String>,
    project_id: Option<uuid::Uuid>,
    published_by_node: Option<String>,
}

impl FromRow for GrantRow {
    fn from_row(row: Row) -> db::Result<Self> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            service: row.try_get("service")?,
            registration_sealed: row.try_get("registration_sealed")?,
            spec: row.try_get::<_, JsonText<Value>>("spec")?.0,
            spec_version: row.try_get("spec_version")?,
            values_sealed: row.try_get("values_sealed")?,
            granted_scopes: row.try_get::<_, db::Json<Value>>("granted_scopes")?.0,
            permissions_verified: row.try_get("permissions_verified")?,
            owner: row.try_get("owner")?,
            door: row.try_get("door")?,
            identity: row.try_get("identity")?,
            provider_account: row.try_get("provider_account")?,
            expires_at: row.try_get("expires_at")?,
            token_lifetime_secs: row.try_get("token_lifetime_secs")?,
            instance_id: row.try_get("instance_id")?,
            project_id: row.try_get("project_id")?,
            published_by_node: row.try_get("published_by_node")?,
        })
    }
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
    /// See [`ResolvedAccess::fresh_until`].
    fresh_until: Option<chrono::DateTime<chrono::Utc>>,
    /// See [`ResolvedAccess::published_by`].
    published_by: Option<String>,
}

/// Who is about to use a connection. An instance's connection is its own,
/// in its project, alone: only a run for that instance of that project
/// (or that instance at its own door) may use it. Anyone else, the
/// author's editor, a run for nobody, another instance's run, reads it as
/// not there. Every other connection is the tenant's, usable by any of
/// them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrantUser<'a> {
    /// The author, or a run for nobody in particular.
    Author,
    Instance { project_id: uuid::Uuid, instance: &'a weft_core::instance::InstanceId },
}

impl<'a> GrantUser<'a> {
    /// The user a carried scope names: that instance, or the author.
    pub fn of(scope: Option<&'a weft_core::instance::InstanceScope>) -> Self {
        match scope {
            Some(scope) => GrantUser::Instance { project_id: scope.project_id, instance: &scope.instance },
            None => GrantUser::Author,
        }
    }

    /// Whether this user may use a connection whose row says
    /// `row_project` / `row_instance`.
    fn may_use(&self, row_project: Option<uuid::Uuid>, row_instance: Option<&str>) -> bool {
        match (row_instance, self) {
            (None, _) => true,
            (Some(_), GrantUser::Author) => false,
            (Some(row_instance), GrantUser::Instance { project_id, instance }) => {
                row_project == Some(*project_id) && row_instance == instance.as_str()
            }
        }
    }
}

/// The wall every read of one connection row passes, pure: somebody
/// else's connection reads the same as none at all (no existence leak),
/// and one of another service is refused by name.
fn walled(
    row_tenant: &str,
    row_service: &str,
    row_project: Option<uuid::Uuid>,
    row_instance: Option<&str>,
    tenant: &str,
    user: GrantUser<'_>,
    service: &str,
) -> anyhow::Result<()> {
    if row_tenant != tenant || !user.may_use(row_project, row_instance) {
        return Err(AccessError::NotFound.into());
    }
    if row_service != service {
        return Err(AccessError::Invalid(format!(
            "this connection is a '{row_service}' account, but the node asked for \
             '{service}'; wire the right service's access node"
        ))
        .into());
    }
    Ok(())
}

/// Whether a walled read renews a token that is due. A `Stored` read
/// makes no provider call and writes nothing: what a read of a captured
/// list or of pasted material wants, which never expires.
#[derive(Clone, Copy, PartialEq)]
enum Freshness {
    Refreshed,
    Stored,
}

/// The columns a walled read selects, in [`GrantRow`]'s order: a plain
/// read and a renewal's claim answer the same row.
const WALLED_COLUMNS: &str = "tenant_id, service, registration_sealed, spec, spec_version, \
                              values_sealed, granted_scopes, permissions_verified, owner, door, \
                              identity, provider_account, expires_at, token_lifetime_secs, \
                              instance_id, project_id, published_by_node";

/// How long a renewal may take before it counts as not answered (tried
/// again later, like a provider that is down), so a claim on a renewal
/// always ends while its claimant lives.
const RENEWAL_WAIT: std::time::Duration = weft_core::access::client::CONNECT_CALL_TIMEOUT;

/// How long a claim on a renewal lasts: past [`RENEWAL_WAIT`], so it only
/// ever runs out on a claimant that died mid-renewal, and then another
/// resolution takes the renewal over.
const RENEWAL_CLAIM: std::time::Duration = std::time::Duration::from_secs(90);

/// The waits between looks at a connection another resolution is
/// renewing, and between tries to write a renewed token while the
/// database cannot be reached: at first, then doubling up to the longest.
const RENEWAL_LOOK_FIRST: std::time::Duration = std::time::Duration::from_millis(50);
const RENEWAL_LOOK_LONGEST: std::time::Duration = std::time::Duration::from_secs(1);

/// A walled grant whose token is due a renewal: when the row says it
/// dies, and the lifetime a renewal naming no expiry gets again.
struct Due {
    expires_at: chrono::DateTime<chrono::Utc>,
    lifetime: Option<chrono::Duration>,
}

/// Read one grant row for a resolution: tenant wall (missing and
/// cross-tenant answer identically, so the wall leaks no existence),
/// service check, required-permission drift backstop, sealed open,
/// spec parse, and (for a `Refreshed` read) the lazy single-flight
/// renewal with rotated-token write-back. Every read that opens a
/// row's values goes through here, so none of them can fork the wall.
///
/// The read is one plain statement, and no database connection is held
/// across a provider's answer. A due token is renewed by whichever
/// resolution claims the renewal on the row (`renewal_claimed_until`,
/// one statement); the others wait, holding nothing, and read the row
/// again until it is fresh, so the provider sees one renewal however
/// many resolutions ask at once.
async fn read_walled_grant(
    pool: &Pool,
    tenant: &str,
    user: GrantUser<'_>,
    access_id: uuid::Uuid,
    service: &str,
    required_permissions: &[String],
    freshness: Freshness,
) -> anyhow::Result<WalledGrant> {
    let open = |row: Option<GrantRow>| open_walled_grant(row, tenant, user, access_id, service, required_permissions);
    let mut look = RENEWAL_LOOK_FIRST;
    loop {
        let row = pool.opt(&format!("SELECT {WALLED_COLUMNS} FROM access_grant WHERE id = $1"), &[&access_id]).await?;
        let (grant, due) = open(row)?;
        let Some(due) = due.filter(|_| freshness == Freshness::Refreshed) else {
            return Ok(grant);
        };
        // Taken before the claim is asked for, so the claim, which the
        // database starts later, lapses no sooner than this says.
        let claim_lapses = std::time::Instant::now() + RENEWAL_CLAIM;
        if let Some((row, claim)) = claim_renewal(pool, access_id, due.expires_at).await? {
            // Renewed from the row as the claim found it, which still dies
            // when `due` says (the claim was won on that).
            let renewed = match open(Some(row)) {
                Ok((grant, _)) => renew(pool, access_id, grant, due, claim, claim_lapses).await,
                Err(e) => Err(e),
            };
            if !matches!(renewed, Ok(Some(_))) {
                let_go_of_renewal(pool, access_id, claim).await;
            }
            match renewed? {
                Some(grant) => return Ok(grant),
                // The connection was made again while it renewed: read
                // what that wrote.
                None => continue,
            }
        }
        tokio::time::sleep(look).await;
        look = (look * 2).min(RENEWAL_LOOK_LONGEST);
    }
}

/// The wall and the opening of one read row, pure: what every
/// resolution starts from, and whether its token is due a renewal
/// (`None` for a runtime-supplied credential, or a token that lives
/// past [`REFRESH_MARGIN`]).
fn open_walled_grant(
    row: Option<GrantRow>,
    tenant: &str,
    user: GrantUser<'_>,
    access_id: uuid::Uuid,
    service: &str,
    required_permissions: &[String],
) -> anyhow::Result<(WalledGrant, Option<Due>)> {
    let Some(GrantRow {
        tenant_id: row_tenant,
        service: row_service,
        registration_sealed,
        spec: spec_json,
        spec_version: spec_json_version,
        values_sealed,
        granted_scopes: granted,
        permissions_verified: verified,
        owner,
        door,
        identity,
        provider_account,
        expires_at,
        token_lifetime_secs,
        instance_id: instance,
        project_id: row_project,
        published_by_node: published_by,
    }) = row
    else {
        return Err(AccessError::NotFound.into());
    };
    walled(&row_tenant, &row_service, row_project, instance.as_deref(), tenant, user, service)?;
    let owner = crate::owner_of(&owner, instance.as_deref())?;
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

    let spec = spec_of(format_args!("connection {access_id}"), spec_json, spec_json_version)?;
    if let Some(reason) = own_only_refusal(&owner, &spec, required_permissions) {
        return Err(AccessError::NeedsReconnect { service: service.to_string(), reason }.into());
    }
    let registration = registration_sealed
        .as_deref()
        .map(crate::open_json)
        .transpose()?
        .map(serde_json::from_value::<AppRegistration>)
        .transpose()
        .map_err(|e| anyhow::anyhow!("grant has a malformed app snapshot: {e}"))?;
    // A runtime-supplied credential stores nothing and is never renewed
    // here: the caller asks the runtime's credential source and fills the
    // auth values.
    let runtime = matches!(spec.acquisition, Acquisition::Runtime {});
    let values = if runtime { BTreeMap::new() } else { values_of(&crate::open_json(&values_sealed)?) };
    let due = expires_at
        .filter(|at| !runtime && *at < chrono::Utc::now() + REFRESH_MARGIN)
        .map(|expires_at| Due { expires_at, lifetime: token_lifetime_secs.map(chrono::Duration::seconds) });
    let grant = WalledGrant {
        service: row_service,
        spec,
        registration,
        values,
        identity,
        owner,
        door,
        provider_account,
        fresh_until: if runtime { None } else { expires_at.map(|at| at - REFRESH_MARGIN) },
        published_by,
    };
    Ok((grant, due))
}

/// Claim the renewal of connection `access_id`, whose token the row said
/// dies at `due`: won only while the row still says so (nobody renewed it
/// since) and no live claim is on it. Answers the row as the claim found
/// it, and the claim, which the renewal's write and its letting go are
/// keyed on; `None` when the claim was not won.
async fn claim_renewal(
    pool: &Pool,
    access_id: uuid::Uuid,
    due: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<Option<(GrantRow, chrono::DateTime<chrono::Utc>)>> {
    let claimed: Option<Row> = pool
        .opt(
            &format!(
                "UPDATE access_grant SET renewal_claimed_until = now() + make_interval(secs => $3)
                 WHERE id = $1 AND expires_at = $2
                   AND (renewal_claimed_until IS NULL OR renewal_claimed_until < now())
                 RETURNING {WALLED_COLUMNS}, renewal_claimed_until"
            ),
            &[&access_id, &due, &RENEWAL_CLAIM.as_secs_f64()],
        )
        .await?;
    let Some(claimed) = claimed else { return Ok(None) };
    let claim = claimed.try_get("renewal_claimed_until").map_err(db::Error::from)?;
    Ok(Some((GrantRow::from_row(claimed)?, claim)))
}

/// Renew a claimed grant's token and write it back: the provider is
/// asked with no database connection held, for at most [`RENEWAL_WAIT`].
/// `None` when the connection was made again while it renewed (the
/// claim is gone from the row), so what that wrote stands.
///
/// The write is tried again while the database cannot be reached, but
/// only while the claim, lapsing at `claim_lapses`, is still this
/// renewal's: past it another resolution may claim the renewal and ask
/// the provider with the token this renewal just retired, and the token
/// this renewal holds would then match no row. So no try starts once the
/// claim has less left than the pool's wait for a connection, and the
/// renewal fails loudly instead, for this one connection, saying the
/// account must be connected again ([`unsaved_renewal`]). A try whose
/// reply was lost may have landed: the next one recognises the values it
/// wrote. One that finds neither the claim nor those values fails the
/// same way: it cannot tell a connection made again from a claim taken
/// over.
async fn renew(
    pool: &Pool,
    access_id: uuid::Uuid,
    mut grant: WalledGrant,
    due: Due,
    claim: chrono::DateTime<chrono::Utc>,
    claim_lapses: std::time::Instant,
) -> anyhow::Result<Option<WalledGrant>> {
    let life = tokio::time::timeout(RENEWAL_WAIT, refresh(&grant.spec, grant.registration.clone(), &mut grant.values, due.lifetime))
        .await
        .unwrap_or_else(|_| {
            Err(AccessError::Unreached(format!(
                "the provider did not answer the renewal within {}s",
                RENEWAL_WAIT.as_secs()
            ))
            .into())
        })?;
    let (expires_at, lifetime) = life_columns(life);
    let sealed = crate::seal_json(&serde_json::to_value(&grant.values)?)?;
    let names = crate::flows::value_names_of(&grant.values);
    // The provider may have rotated the refresh token, so the old one no
    // longer works: what it answered exists nowhere else, and is written
    // for as long as the database cannot be reached and the claim holds.
    let last_try = claim_lapses.checked_sub(pool.acquire_timeout()).unwrap_or(claim_lapses);
    let mut wait = RENEWAL_LOOK_FIRST;
    let mut missed = false;
    let written = loop {
        // A try whose reply was lost may still have landed, clearing the
        // claim: the row then holds this renewal's own sealed values
        // (sealing draws a fresh random nonce, so no other write carries
        // the same text), and the next try matches on them instead of
        // reading the landed write as a claim taken away. The first try
        // can only match on the claim, so the common path is one
        // statement as before.
        let write = pool
            .execute(
                "UPDATE access_grant
                 SET values_sealed = $3, value_names = $4, expires_at = $5,
                     token_lifetime_secs = $6, renewal_claimed_until = NULL, updated_at = now()
                 WHERE id = $1 AND (renewal_claimed_until = $2 OR values_sealed = $3)",
                &[&access_id, &claim, &sealed, &db::Json(db::Names(&names)), &expires_at, &lifetime],
            )
            .await;
        match write {
            Ok(1) => break true,
            Ok(_) if missed => return Err(unsaved_renewal(access_id, &grant.service, "the renewal's claim on it was gone by the time it could be written")),
            Ok(_) => break false,
            Err(e) if write_again(&e) && std::time::Instant::now() + wait < last_try => {
                tracing::warn!(
                    target: "weft_access_store::resolve",
                    %access_id,
                    error = %e,
                    "a renewed token could not be written yet, and the provider may have retired the one before it; writing it again"
                );
                missed = true;
                tokio::time::sleep(wait).await;
                wait = (wait * 2).min(RENEWAL_LOOK_LONGEST);
            }
            // Not the database's reach, or no time left on the claim:
            // said as what it is, never as a database that could not be
            // had, since the renewal happened.
            Err(e) => return Err(unsaved_renewal(access_id, &grant.service, &format!("the renewed token could not be written ({e})"))),
        }
    };
    if !written {
        return Ok(None);
    }
    grant.fresh_until = expires_at.map(|at| at - REFRESH_MARGIN);
    Ok(Some(grant))
}

/// Whether a failed write of a renewed token is worth trying again: the
/// database could not be reached ([`db::unreachable`]), short of a closed
/// pool (this process is going). A conversation with the database that
/// went wrong (a message the driver did not expect) is no failure to
/// reach it, so it is never tried again: sending the same statement again
/// does not fix it.
fn write_again(e: &db::Error) -> bool {
    db::unreachable(e) && db::no_connection(e) != Some(db::NoConnection::Closed)
}

/// A renewal the provider made and weft could not keep, for `why`: the
/// provider may already have retired the token before it, so the
/// connection cannot be trusted to renew again, and the account is
/// connected again. Fails this one connection's resolution.
fn unsaved_renewal(access_id: uuid::Uuid, service: &str, why: &str) -> anyhow::Error {
    AccessError::NeedsReconnect {
        service: service.to_string(),
        reason: format!(
            "connection {access_id} was renewed at the provider, but weft could not save the renewed key: {why}. The \
             provider may already have retired the key before it, so connect the account again (Connect on its access \
             node in the editor, or `weft connect` in the project's folder)"
        ),
    }
    .into()
}

/// Let go of a renewal claim that did not end in a written renewal, so the
/// next resolution renews at once. A claim not let go (the database could
/// not be reached) runs out on its own after [`RENEWAL_CLAIM`].
async fn let_go_of_renewal(pool: &Pool, access_id: uuid::Uuid, claim: chrono::DateTime<chrono::Utc>) {
    let let_go = pool
        .execute("UPDATE access_grant SET renewal_claimed_until = NULL WHERE id = $1 AND renewal_claimed_until = $2", &[&access_id, &claim])
        .await;
    if let Err(e) = let_go {
        tracing::warn!(
            target: "weft_access_store::resolve",
            %access_id,
            error = %e,
            "could not let go of a renewal that did not land; the next renewal waits until it runs out ({}s)",
            RENEWAL_CLAIM.as_secs()
        );
    }
}

/// Resolve a connection for a WORKER (or for the store's own
/// lookup/test use): the walled read above, then the worker handoff
/// filter and the consumer's shortfall checks. An `Ours`-owned row
/// (runtime-supplied credential) skips refresh and hands back empty
/// values; the caller fills them.
pub async fn resolve_for_worker(
    pool: &Pool,
    tenant: &str,
    user: GrantUser<'_>,
    access_id: uuid::Uuid,
    service: &str,
    required_permissions: &[String],
    required_values: &[String],
) -> anyhow::Result<ResolvedAccess> {
    let grant =
        read_walled_grant(pool, tenant, user, access_id, service, required_permissions, Freshness::Refreshed)
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
    let WalledGrant { service: row_service, spec, registration, values, identity, owner, fresh_until, published_by, .. } =
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
            fresh_until,
            published_by,
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
        fresh_until,
        published_by,
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
    pool: &Pool,
    tenant: &str,
    user: GrantUser<'_>,
    access_id: uuid::Uuid,
    service: &str,
    required_values: &[String],
) -> anyhow::Result<ResolvedEventSource> {
    let grant =
        read_walled_grant(pool, tenant, user, access_id, service, &[], Freshness::Refreshed).await?;
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
        expires_at_unix: access.fresh_until.map(|at| at.timestamp()),
    })
}

/// What verifying a CALLER against a connection needs: the scheme the
/// connection's service recipe declares (`verify`) and the connection's
/// stored values the scheme reads its material from. The tenant wall
/// applies as everywhere; a connection of another service is refused
/// by name. A stored read: the scheme keys on pasted material, so no
/// refresh and no provider call.
pub struct CallerVerifier {
    pub verify: Option<weft_core::access::events::VerifyKind>,
    pub values: BTreeMap<String, String>,
}

pub async fn caller_verifier(
    pool: &Pool,
    tenant: &str,
    user: GrantUser<'_>,
    access_id: uuid::Uuid,
    service: &str,
) -> anyhow::Result<CallerVerifier> {
    let grant =
        read_walled_grant(pool, tenant, user, access_id, service, &[], Freshness::Stored).await?;
    let values = grant.spec.handoff_values(&grant.values);
    Ok(CallerVerifier { verify: grant.spec.verify.clone(), values })
}

/// The request headers a route gated by connection `access_id` reads its
/// caller's credential from (`VerifyKind::credential_headers`): what a run
/// it admits holds as secrets, so what a request typed by hand for it
/// (`weft run --fire`) cannot carry. A recipe that declares no `verify`
/// block gates no route, so it is refused, as the broker refuses every
/// call through it. Reads the recipe through the wall, never the values.
pub async fn caller_credential_headers(
    pool: &Pool,
    tenant: &str,
    user: GrantUser<'_>,
    access_id: uuid::Uuid,
    service: &str,
) -> anyhow::Result<Vec<String>> {
    let row: Option<RecipeRow> = pool
        .opt(
            "SELECT tenant_id, service, spec, spec_version, instance_id, project_id FROM access_grant WHERE id = $1",
            &[&access_id],
        )
        .await?;
    let Some(row) = row else {
        return Err(AccessError::NotFound.into());
    };
    walled(&row.tenant_id, &row.service, row.project_id, row.instance_id.as_deref(), tenant, user, service)?;
    let spec = spec_of(format_args!("connection {access_id}"), row.spec, row.spec_version)?;
    match spec.verify {
        Some(kind) => Ok(kind.credential_headers()),
        None => Err(AccessError::Invalid(format!(
            "the '{service}' service declares no `verify` block, so its connections cannot gate a route"
        ))
        .into()),
    }
}

/// The `access_grant` columns the wall and a connection's recipe need:
/// nothing sealed.
struct RecipeRow {
    tenant_id: String,
    service: String,
    spec: Value,
    spec_version: weft_core::stored::Version,
    instance_id: Option<String>,
    project_id: Option<uuid::Uuid>,
}

impl FromRow for RecipeRow {
    fn from_row(row: Row) -> db::Result<Self> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            service: row.try_get("service")?,
            spec: row.try_get::<_, JsonText<Value>>("spec")?.0,
            spec_version: row.try_get("spec_version")?,
            instance_id: row.try_get("instance_id")?,
            project_id: row.try_get("project_id")?,
        })
    }
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
    // Canonical key order: the recipe's JSON values keep insertion order
    // in any build with serde_json's `preserve_order` (the broker's), so a
    // plain serialization would hash one recipe two ways depending on
    // whether it was just parsed or read back out of a jsonb column.
    let canonical = weft_core::project::hash::canonical_json(
        &serde_json::to_value(events).expect("events recipes serialize"),
    );
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
pub async fn record_events_recipes(db: &impl Client, spec: &AccessSpec) -> anyhow::Result<()> {
    let Some(hash) = events_recipe_hash(&spec.events) else {
        return Ok(());
    };
    db.execute(
        "INSERT INTO service_events_recipe (service, recipe_hash, events_json, events_json_version, updated_at)
         VALUES ($1, $2, $3, $4, now())
         ON CONFLICT (service, recipe_hash) DO UPDATE SET updated_at = now()",
        &[&spec.service, &hash, &JsonText(&spec.events), &weft_core::access::events::STORED_EVENTS.version()],
    )
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
    pool: &Pool,
    service: &str,
) -> anyhow::Result<Vec<RecordedEventsRecipe>> {
    let rows: Vec<(String, JsonText<Value>, weft_core::stored::Version)> = pool
        .all(
            "SELECT recipe_hash, events_json, events_json_version FROM service_events_recipe
             WHERE service = $1 ORDER BY updated_at DESC",
            &[&service],
        )
        .await?;
    rows.into_iter()
        .map(|(recipe_hash, JsonText(v), version)| {
            let events = weft_core::access::events::STORED_EVENTS
                .decode(v, version)
                .map_err(|e| e.at(format_args!("a recorded events recipe of '{service}' ({recipe_hash})")))?;
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
    pool: &Pool,
    service: &str,
    provider_account: &str,
    allowed_client_ids: &[String],
    recipe_hash: &str,
) -> anyhow::Result<Vec<EventTarget>> {
    let rows: Vec<(uuid::Uuid, String)> = pool
        .all(
            "SELECT id, tenant_id FROM access_grant
             WHERE service = $1 AND provider_account = $2
               AND client_id = ANY($3) AND events_recipe_hash = $4",
            &[&service, &provider_account, &allowed_client_ids, &recipe_hash],
        )
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

/// Re-acquire the short-lived pieces into `values`, per the acquisition
/// kind, and answer when they stop working ([`renewed_life`]: an answer
/// naming no usable expiry lasts `lifetime` again, the connection's
/// stored token lifetime). Loud (with the reconnect affordance named)
/// when the provider refuses: a revoked grant is never silently
/// recoverable.
async fn refresh(
    spec: &AccessSpec,
    registration: Option<AppRegistration>,
    values: &mut BTreeMap<String, String>,
    lifetime: Option<chrono::Duration>,
) -> anyhow::Result<Option<TokenLife>> {
    let reconnect = |reason: String| AccessError::NeedsReconnect {
        service: spec.service.clone(),
        reason,
    };
    // A token endpoint not reached, or busy or down, may come back with
    // the grant fine, so that is tried again; any other refusal is of
    // the grant itself.
    let refused = |e: anyhow::Error| match e.downcast_ref::<AccessError>() {
        Some(AccessError::Unreached(_) | AccessError::NeedsReconnect { .. }) => e,
        _ => anyhow::Error::from(reconnect(format!("the refresh was refused ({e})"))),
    };
    let expiry = match &spec.acquisition {
        Acquisition::Static { .. } => {
            return Err(reconnect(
                "its pasted credential expired (a static credential has no refresh); paste a \
                 fresh one"
                    .into(),
            )
            .into())
        }
        Acquisition::MintJwt { .. } => return mint_and_exchange(spec, values).await,
        // A runtime-supplied credential is never refreshed here: the
        // resolve path answers before reaching refresh.
        Acquisition::Runtime {} => {
            return Err(anyhow::anyhow!("a runtime acquisition never reaches refresh"))
        }
        Acquisition::OAuth2 { grant, token_url, refresh: refresh_call, answer, captures, .. } => {
            let Some(registration) = registration else {
                return Err(reconnect(
                    "the grant has no app to refresh with; reconnect".into(),
                )
                .into());
            };
            let reg = registration.to_values();
            // A declared renewal call replaces the standard one
            // entirely (Instagram's refresh GET): run it over the
            // stored values plus the app's, store what its answer
            // gives, write its captures back.
            match (refresh_call, grant) {
                (Some(call), _) => run_token_call(&spec.service, call, &reg, values, AnswerOf::Renewal)
                    .await
                    .map_err(refused)?,
                (None, OAuthGrant::ClientCredentials) => {
                    client_credentials_token(spec, &reg, values, AnswerOf::Renewal).await.map_err(refused)?
                }
                (None, OAuthGrant::AuthorizationCode { .. }) => {
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
                    let basic = token_params(spec, &reg, &mut params)?;
                    let resp = token_request(token_url, &params, basic, answer).await.map_err(refused)?;
                    let read = read_token(answer, &resp, "the token endpoint").map_err(refused)?;
                    let expiry = store_token(read, values, AnswerOf::Renewal);
                    apply_captures(captures, &resp, values, AnswerOf::Renewal)?;
                    expiry
                }
            }
        }
    };
    Ok(renewed_life(expiry, lifetime))
}

/// Request a client-credentials token (Zoom S2S class) into `values`,
/// for the connect (`AnswerOf::First`: the first answer must carry
/// every capture) and for the re-request on expiry
/// (`AnswerOf::Renewal`). Hands back the answer's expiry for the caller
/// to judge: a connect refuses one it cannot know, a renewal reuses the
/// connection's lifetime.
pub(crate) async fn client_credentials_token(
    spec: &AccessSpec,
    reg: &BTreeMap<String, String>,
    values: &mut BTreeMap<String, String>,
    of: AnswerOf,
) -> anyhow::Result<TokenExpiry> {
    let Acquisition::OAuth2 { token_url, captures, answer, .. } = &spec.acquisition else {
        return Err(anyhow::anyhow!("client_credentials_token on a non-oauth2 spec"));
    };
    let mut params: Vec<(String, String)> =
        vec![("grant_type".into(), "client_credentials".into())];
    let basic = token_params(spec, reg, &mut params)?;
    let resp = token_request(token_url, &params, basic, answer).await?;
    let read = read_token(answer, &resp, "the token endpoint")?;
    let expiry = store_token(read, values, of);
    apply_captures(captures, &resp, values, of)?;
    Ok(expiry)
}

/// Mint the JWT and (when the spec declares an exchange) trade it for
/// the working token. Returns the token's expiry. Shared by connect
/// (validate now) and refresh (re-mint lazily).
pub(crate) async fn mint_and_exchange(
    spec: &AccessSpec,
    values: &mut BTreeMap<String, String>,
) -> anyhow::Result<Option<TokenLife>> {
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
    let key = weft_core::jwt::EncodingKey::from_rsa_pem(private_key.as_bytes())
        .map_err(|e| AccessError::Invalid(format!("the pasted private key is not RSA PEM: {e}")))?;
    let jwt = weft_core::jwt::encode(
        &weft_core::jwt::Header::new(weft_core::jwt::Algorithm::RS256),
        &Value::Object(claim_map),
        &key,
    )
    .map_err(|e| anyhow::anyhow!("jwt mint failed: {e}"))?;

    let jwt_life = TokenLife::lasting(chrono::Utc::now(), chrono::Duration::seconds(*jwt_ttl_secs as i64));
    let Some(exchange) = exchange else {
        values.insert("token".into(), jwt);
        return Ok(Some(jwt_life));
    };
    let url = exchange.url.resolve_url(values).map_err(AccessError::Invalid)?;
    let resp = base_client()
        .post(&url)
        .bearer_auth(&jwt)
        .header(reqwest::header::ACCEPT, "application/json")
        .empty_body()
        .send()
        .await
        .map_err(|e| {
            // The template, never the resolved URL: a recipe may
            // interpolate a secret into it, and reqwest's error text
            // would echo the URL whole.
            AccessError::Unreached(format!("token exchange at {} failed: {}", exchange.url.0, e.without_url()))
        })?;
    let status = resp.status();
    let body: Value = crate::read_json_answer(resp, &exchange.url.0).await?;
    let paths = exchange.answer.refusal_paths();
    let refused = if !status.is_success() {
        Some(format!(
            "the token exchange at {} answered {status}: {}",
            exchange.url.0,
            body.get("message").and_then(Value::as_str).unwrap_or("no detail")
        ))
    } else {
        paths.refusal(&body).map(|why| format!("the token exchange at {} refused: {why}", exchange.url.0))
    };
    if let Some(answered) = refused {
        // Any refusal but a busy provider is of the credential itself.
        return Err(crate::refusal(RefusalKind::of(status, paths, &body), answered, |reason| AccessError::NeedsReconnect {
            service: spec.service.clone(),
            reason,
        })
        .into());
    }
    let read = read_token(&exchange.answer, &body, &exchange.url.0)?;
    // A minted token is minted again, never refreshed: the reader
    // refuses a refresh token path at load, so none is stored here, and
    // a token whose expiry is unreadable is simply minted again.
    let expiry = match read.expiry {
        TokenExpiry::At(life) => Some(life),
        TokenExpiry::Never => None,
        // Minted again once the JWT itself would have run out.
        TokenExpiry::Unknown => Some(jwt_life),
        TokenExpiry::Unreadable(why) => {
            return Err(AccessError::Invalid(format!("{}: {why} (declared on acquisition.exchange)", exchange.url.0)).into())
        }
    };
    values.insert("token".into(), read.token);
    apply_captures(&exchange.captures, &body, values, AnswerOf::First)?;
    Ok(expiry)
}

// ---------- Editor resource lookups ----------

pub use weft_core::access::lookup::{GrantedQuery, LookupItem, LookupPage, LookupRequest};

/// Read a `granted` resource source off the connection row: the stored
/// value `from` (captured at connect) parsed as a JSON array;
/// `label_path` / `value_path` address one item (same vocabulary as a
/// [`Lookup`]). Free: no provider call at all. Only the id/label pairs
/// leave; the rest of the stored values never do.
pub async fn granted_items(
    pool: &Pool,
    tenant: &str,
    user: GrantUser<'_>,
    access_id: uuid::Uuid,
    service: &str,
    from: &str,
    label_path: &str,
    value_path: &str,
) -> anyhow::Result<Vec<LookupItem>> {
    // The same walled read every resolution runs, but the FULL stored
    // map (the worker-handoff filter would drop a captured list) and
    // STORED freshness: a captured list has no expiry, so reading it
    // never needs the provider.
    let grant =
        read_walled_grant(pool, tenant, user, access_id, service, &[], Freshness::Stored).await?;
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
/// `{query}` and `{<parent>}` substituted, the way every declared URL
/// is ([`Template::resolve_url`]: a value in the path or the query is
/// percent-encoded, one before the path, like a `{base}` holding
/// `https://...`, is URL structure). A placeholder nothing fills is
/// loud: a typo'd parent name must not silently hit the service with a
/// literal brace. Public so the lookup's caller can name the exact URL
/// when asking a credential policy about it.
pub fn lookup_url(
    spec: &Lookup,
    query: &str,
    parents: &BTreeMap<String, String>,
) -> anyhow::Result<String> {
    let mut values = parents.clone();
    values.insert("query".into(), query.to_string());
    Template::new(spec.get.clone()).resolve_url_typed(&values).map_err(|e| {
        AccessError::Invalid(match e {
            TemplateError::Missing(name) => format!(
                "lookup URL references '{{{name}}}' but no parent value named '{name}' \
                 was provided (pick it first)"
            ),
            TemplateError::Malformed(why) => why,
        })
        .into()
    })
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
            AccessError::Unreached(format!("lookup call to {url} failed: {}", weft_core::access::client::send_error(e)))
        })?;
    let status = resp.status();
    let body: Value = crate::read_json_answer(resp, url).await?;
    let paths = spec.refusal_paths();
    let refusal = if !status.is_success() {
        let said: String = body.to_string().chars().take(300).collect();
        Some((RefusalKind::of(status, paths, &body), format!("the service answered {status} to the lookup: {said}")))
    } else {
        paths
            .refusal(&body)
            .map(|why| (RefusalKind::of(status, paths, &body), format!("the service refused the lookup: {why}")))
    };
    if let Some((kind, why)) = refusal {
        // A refused credential is the connection's to fix, by a
        // reconnect, when it is a person's own; a runtime-supplied one
        // (or none, for a public source) has nothing to reconnect, and
        // any other refusal is about the lookup itself.
        return Err(crate::refusal(kind, why, |why| match resolved {
            Some(r) if kind == RefusalKind::Auth && !r.owner.is_platform() => {
                AccessError::NeedsReconnect { service: r.service.clone(), reason: why }
            }
            _ => AccessError::Invalid(why),
        })
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
    for item in items.iter().filter(|item| lookup_keeps(spec, item)) {
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
        // A cursor can be a number (a page index, an offset) as well as
        // text; null or empty means no more pages.
        .filter(|v| !v.is_null())
        .map(display_of)
        .filter(|s| !s.is_empty());
    Ok(LookupPage { items: out, next_cursor })
}

/// Whether one answered item passes the lookup's `where`: the item
/// holds the given value at every path, compared as JSON. Nothing at a
/// path fails it; an empty `where` passes everything.
fn lookup_keeps(spec: &Lookup, item: &Value) -> bool {
    spec.where_equals.iter().all(|(path, want)| lookup_path(item, path) == Some(want))
}

fn display_of(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    /// A renewed token's write is tried again only while the database is
    /// out of reach; a closed pool or a statement that failed fails it at
    /// once, so a write that keeps failing the same way never spins.
    #[test]
    fn a_renewed_token_is_written_again_only_while_the_database_is_out_of_reach() {
        let no_connection = |why| db::Error::NoConnection { why, refused: None };
        assert!(write_again(&no_connection(db::NoConnection::Unreachable)));
        assert!(write_again(&no_connection(db::NoConnection::Busy)));
        assert!(!write_again(&no_connection(db::NoConnection::Closed)));
        assert!(!write_again(&db::Error::NoRow));
    }

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
            "https://x.example/v1/items?q=a%26b&team=T%201%2F2"
        );
        assert_eq!(
            lookup_url(&spec("https://x.example/v1/items"), "ignored", &BTreeMap::new()).unwrap(),
            "https://x.example/v1/items"
        );
        // A value before the path is URL structure: a `{base}` holding an
        // absolute URL goes in as it is, while what follows is encoded.
        let based: BTreeMap<String, String> =
            [("base".to_string(), "https://acme.example:8443/api".to_string())].into_iter().collect();
        assert_eq!(
            lookup_url(&spec("{base}/v1/items?q={query}"), "a b", &based).unwrap(),
            "https://acme.example:8443/api/v1/items?q=a%20b"
        );
        let unknown =
            lookup_url(&spec("https://x.example/{nope}"), "", &BTreeMap::new()).unwrap_err();
        assert!(unknown.to_string().contains("nope"), "{unknown}");
        let unclosed =
            lookup_url(&spec("https://x.example/{query"), "", &BTreeMap::new()).unwrap_err();
        assert!(unclosed.to_string().contains("unclosed"), "{unclosed}");
    }

    /// A lookup's `where`: an item is kept only when it holds every
    /// given value at its path (nested paths too), an item with nothing
    /// at a path is dropped, and no `where` keeps everything.
    #[test]
    fn lookup_where_keeps_only_items_matching_every_entry() {
        let spec = |filter: Option<Value>| -> Lookup {
            let mut raw = serde_json::json!({
                "get": "https://x.example/v1/models", "items": "", "label": "name", "value": "model_id"
            });
            if let Some(filter) = filter {
                raw["where"] = filter;
            }
            serde_json::from_value(raw).unwrap()
        };
        let speech = serde_json::json!({
            "model_id": "tts", "can_do_text_to_speech": true, "limits": { "tier": "free" }
        });
        let voice = serde_json::json!({
            "model_id": "vc", "can_do_text_to_speech": false, "limits": { "tier": "paid" }
        });
        let bare = serde_json::json!({ "model_id": "bare" });

        let tts = spec(Some(serde_json::json!({ "can_do_text_to_speech": true })));
        assert!(lookup_keeps(&tts, &speech));
        assert!(!lookup_keeps(&tts, &voice), "a different value drops the item");
        assert!(!lookup_keeps(&tts, &bare), "nothing at the path drops the item");

        let nested = spec(Some(serde_json::json!({ "limits.tier": "free" })));
        assert!(lookup_keeps(&nested, &speech));
        assert!(!lookup_keeps(&nested, &voice));

        let both = spec(Some(serde_json::json!({ "can_do_text_to_speech": true, "limits.tier": "paid" })));
        assert!(!lookup_keeps(&both, &speech), "every entry must match, not just one");
        assert!(!lookup_keeps(&both, &voice));

        for unfiltered in [spec(None), spec(Some(serde_json::json!({})))] {
            assert!(unfiltered.where_equals.is_empty());
            for item in [&speech, &voice, &bare] {
                assert!(lookup_keeps(&unfiltered, item), "no `where` keeps {item}");
            }
        }
        assert!(
            serde_json::to_value(spec(Some(serde_json::json!({})))).unwrap().get("where").is_none(),
            "an empty `where` is left off the wire"
        );
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
                  "ownOnly": true,
                  "guide": { "link": "https://example/voices", "steps": ["s"] } },
            ],
        }))
        .expect("spec parses");
        use weft_core::CredentialOwner::{Author as TheirOwn, Platform as Ours};
        let need = |ids: &[&str]| ids.iter().map(|s| s.to_string()).collect::<Vec<_>>();

        let reason =
            own_only_refusal(&Ours, &spec, &need(&["voice_lab"])).expect("Ours is refused");
        assert!(reason.contains("Voice creation"), "{reason}");
        assert!(reason.contains("https://example/voices"), "{reason}");
        assert_eq!(own_only_refusal(&Ours, &spec, &need(&["generate"])), None);
        assert_eq!(own_only_refusal(&TheirOwn, &spec, &need(&["voice_lab"])), None);
        assert_eq!(own_only_refusal(&Ours, &spec, &[]), None);
    }
}
