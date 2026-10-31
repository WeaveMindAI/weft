//! Copying a tenant's connections to another install (`weft migrate
//! connections`), their secrets sealed the whole way.
//!
//! The install they move TO makes a key pair ([`make_transfer_key`]); the
//! one they move FROM opens the picked rows and seals them to that key
//! ([`export`]); the target opens the batch with its kept half, used once
//! ([`begin_import`]), and writes each connection under its own
//! sealing key through the one insert every connection takes
//! ([`import`]). A connection keeps its id and when it was made, so the
//! programs and data that name it find it on the target; the tenant is
//! the one importing.
//!
//! What stays behind, because it means something on one install only:
//! the install's picks (the source's are answered so the person can pick
//! again), event subscriptions (renewed on the target by the triggers
//! that use them), consents and pickers in flight, instance values, and a
//! connection a node published for something it runs (it publishes it
//! again where it runs).

use std::collections::{BTreeMap, BTreeSet};

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use weft_core::migrate::{
    Candidate, ConnectionPick, ExportedConnection, ExportedConnections, ImportedConnection, ImportedConnections, Outcome,
    Selection, TransferKey,
};
use weft_core::AppRegistration;
use weft_task_store::db::{self, Client, FromRow, JsonText, Pool, Row, Transaction};

use crate::flows::{copy_grant, Moved, NewGrant};
use crate::{door_of, scopes_of, values_of, AccessError};

/// How long a transfer key may wait for its batch.
const TRANSFER_KEY_TTL: chrono::Duration = chrono::Duration::minutes(15);

/// Make a key pair for one move to this install, keeping its private half
/// sealed like every secret the store keeps. Keys past their time go as
/// the next one is made.
pub async fn make_transfer_key(pool: &Pool, tenant: &str) -> anyhow::Result<TransferKey> {
    let (secret, public_key) = crate::transfer::key_pair();
    let key_id = uuid::Uuid::new_v4();
    pool.execute(
        &format!(
            "{} INSERT INTO access_transfer_key (id, tenant_id, secret_sealed) VALUES ($2, $3, $4)",
            crate::flows::pruning("access_transfer_key", "$1")
        ),
        &[&(chrono::Utc::now() - TRANSFER_KEY_TTL), &key_id, &tenant, &crate::seal_str(&BASE64.encode(&secret))],
    )
    .await?;
    Ok(TransferKey { key_id, public_key })
}

/// A batch opened with this install's transfer key, waiting to be written
/// ([`begin_import`]). The key is forgotten only when the writes commit,
/// in the transaction that holds it (locked) for them: an import that
/// fails part way leaves nothing written and the key still there, so
/// running it again just works.
pub struct Import {
    pool: Pool,
    key_id: uuid::Uuid,
    batch: Vec<MovedGrant>,
}

/// The message for a transfer key that is not there to open a batch.
const KEY_GONE: &str = "the target install's transfer key is gone (it opens one batch, for 15 minutes); run the migrate again";

/// Open `sealed` with `tenant`'s transfer key `key_id`, still within its
/// time. A key opens one batch: [`import`] holds it while it writes, so
/// the next import that names it waits for that one, and finds it gone
/// once that one commits. Nothing is held in between: the caller may
/// take its time over the batch (asking what keys this install holds).
pub async fn begin_import(pool: &Pool, tenant: &str, key_id: uuid::Uuid, sealed: &str) -> anyhow::Result<Import> {
    let kept: Option<String> = pool
        .scalar_opt(
            "SELECT secret_sealed FROM access_transfer_key WHERE id = $1 AND tenant_id = $2 AND created_at >= $3",
            &[&key_id, &tenant, &(chrono::Utc::now() - TRANSFER_KEY_TTL)],
        )
        .await?;
    let kept = kept.ok_or_else(|| AccessError::Invalid(KEY_GONE.into()))?;
    let secret = BASE64.decode(crate::open_str(&kept)?).map_err(|e| anyhow::anyhow!("the kept transfer key is not base64: {e}"))?;
    let batch = open_batch(&secret, sealed).map_err(|e| AccessError::Invalid(format!("{e:#}")))?;
    Ok(Import { pool: pool.clone(), key_id, batch })
}

impl Import {
    pub fn batch(&self) -> &[MovedGrant] {
        &self.batch
    }
}

/// One connection as it travels, its secrets opened: only ever inside a
/// sealed batch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MovedGrant {
    id: uuid::Uuid,
    service: String,
    project_id: Option<uuid::Uuid>,
    instance_id: Option<String>,
    /// The recipe as the source stored it, and the version it was stored
    /// at: the target reads it the way it reads its own rows.
    spec: Value,
    spec_version: weft_core::stored::Version,
    values: BTreeMap<String, String>,
    registration: Option<AppRegistration>,
    granted_scopes: Vec<String>,
    permissions_verified: bool,
    /// Whether it spends the runtime's own key (`owner = 'ours'`).
    platform: bool,
    door: String,
    label: Option<String>,
    identity: Option<String>,
    expires_at: Option<chrono::DateTime<chrono::Utc>>,
    token_lifetime_secs: Option<i64>,
    created_at: chrono::DateTime<chrono::Utc>,
}

impl MovedGrant {
    pub fn id(&self) -> uuid::Uuid {
        self.id
    }

    pub fn service(&self) -> &str {
        &self.service
    }

    /// Whether it spends the runtime's own key, which the target must
    /// hold for it to work there.
    pub fn runs_on_shared_key(&self) -> bool {
        self.platform
    }
}

/// The `access_grant` columns a move reads.
struct GrantRow {
    id: uuid::Uuid,
    tenant_id: String,
    service: String,
    project_id: Option<uuid::Uuid>,
    instance_id: Option<String>,
    spec: Value,
    spec_version: i32,
    values_sealed: String,
    registration_sealed: Option<String>,
    granted_scopes: Value,
    permissions_verified: bool,
    owner: String,
    door: String,
    label: Option<String>,
    identity: Option<String>,
    expires_at: Option<chrono::DateTime<chrono::Utc>>,
    token_lifetime_secs: Option<i64>,
    created_at: chrono::DateTime<chrono::Utc>,
    published_by_node: Option<String>,
}

impl FromRow for GrantRow {
    fn from_row(row: Row) -> db::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            tenant_id: row.try_get("tenant_id")?,
            service: row.try_get("service")?,
            project_id: row.try_get("project_id")?,
            instance_id: row.try_get("instance_id")?,
            spec: row.try_get::<_, JsonText<Value>>("spec")?.0,
            spec_version: row.try_get("spec_version")?,
            values_sealed: row.try_get("values_sealed")?,
            registration_sealed: row.try_get("registration_sealed")?,
            granted_scopes: row.try_get::<_, db::Json<Value>>("granted_scopes")?.0,
            permissions_verified: row.try_get("permissions_verified")?,
            owner: row.try_get("owner")?,
            door: row.try_get("door")?,
            label: row.try_get("label")?,
            identity: row.try_get("identity")?,
            expires_at: row.try_get("expires_at")?,
            token_lifetime_secs: row.try_get("token_lifetime_secs")?,
            created_at: row.try_get("created_at")?,
            published_by_node: row.try_get("published_by_node")?,
        })
    }
}

const GRANT_COLUMNS: &str = "id, tenant_id, service, project_id, instance_id, spec, spec_version, values_sealed, \
    registration_sealed, granted_scopes, permissions_verified, owner, door, label, identity, expires_at, \
    token_lifetime_secs, created_at, published_by_node";

impl GrantRow {
    /// The row as it travels, its secrets opened.
    fn opened(self) -> anyhow::Result<MovedGrant> {
        let registration = self
            .registration_sealed
            .as_deref()
            .map(|sealed| -> anyhow::Result<AppRegistration> { Ok(serde_json::from_value(crate::open_json(sealed)?)?) })
            .transpose()?;
        Ok(MovedGrant {
            id: self.id,
            service: self.service,
            project_id: self.project_id,
            instance_id: self.instance_id,
            spec: self.spec,
            spec_version: self.spec_version,
            values: values_of(&crate::open_json(&self.values_sealed)?),
            registration,
            granted_scopes: scopes_of(&self.granted_scopes),
            permissions_verified: self.permissions_verified,
            platform: self.owner == "ours",
            door: self.door,
            label: self.label,
            identity: self.identity,
            expires_at: self.expires_at,
            token_lifetime_secs: self.token_lifetime_secs,
            created_at: self.created_at,
        })
    }

    fn summary(&self, skipped: Option<String>) -> anyhow::Result<ExportedConnection> {
        let spec = crate::spec_of(format!("connection {}", self.id), self.spec.clone(), self.spec_version)?;
        Ok(ExportedConnection {
            id: self.id,
            service: self.service.clone(),
            label: self.label.clone(),
            project_id: self.project_id,
            instance: self.instance_id.clone(),
            created_at: self.created_at,
            sign_in: spec.needs_browser_consent(),
            skipped,
        })
    }
}

/// Why a picked row is not copied, when it is not.
fn not_copied(row: &GrantRow) -> Option<String> {
    row.published_by_node.as_ref().map(|node| {
        format!("node '{node}' published it for something it runs; it publishes it again on the target when it runs there")
    })
}

/// `tenant`'s connections `selection` picks, and each copyable one sealed
/// to `public_key` (none sealed without a key: a dry run), with the
/// source's picks of them.
pub async fn export(pool: &Pool, tenant: &str, selection: &Selection, public_key: Option<&str>) -> anyhow::Result<ExportedConnections> {
    selection.validate().map_err(AccessError::Invalid)?;
    let rows: Vec<GrantRow> =
        pool.all(&format!("SELECT {GRANT_COLUMNS} FROM access_grant WHERE tenant_id = $1 ORDER BY created_at, id"), &[&tenant]).await?;
    let mut connections = Vec::new();
    let mut moving = Vec::new();
    for row in rows {
        let id = row.id.to_string();
        let candidate = Candidate { id: &id, created_at: row.created_at, service: Some(&row.service), project: row.project_id, key: None };
        if !selection.picks(&candidate) {
            continue;
        }
        let skipped = not_copied(&row);
        connections.push(row.summary(skipped.clone())?);
        if skipped.is_none() {
            moving.push(row);
        }
    }
    let ids: Vec<uuid::Uuid> = moving.iter().map(|row| row.id).collect();
    let picks: Vec<(uuid::Uuid, String, String, uuid::Uuid)> = pool
        .all(
            "SELECT project_id, node, field, grant_id FROM install_pick WHERE tenant_id = $1 AND grant_id = ANY($2) \
             ORDER BY project_id, node, field",
            &[&tenant, &ids],
        )
        .await?;
    let picks = picks.into_iter().map(|(project_id, node, field, grant_id)| ConnectionPick { project_id, node, field, grant_id }).collect();
    let sealed = match public_key {
        Some(public_key) if !moving.is_empty() => {
            let batch: Vec<MovedGrant> = moving.into_iter().map(GrantRow::opened).collect::<anyhow::Result<_>>()?;
            Some(crate::transfer::seal_to(public_key, &serde_json::to_vec(&batch)?)?)
        }
        _ => None,
    };
    Ok(ExportedConnections { connections, sealed, picks })
}

/// Open a batch [`export`] sealed to this install's transfer key.
fn open_batch(secret: &[u8], sealed: &str) -> anyhow::Result<Vec<MovedGrant>> {
    let plain = crate::transfer::open_with(secret, sealed)?;
    serde_json::from_slice(&plain).map_err(|e| {
        anyhow::anyhow!("the sealed connections do not read on this install ({e}); the two installs may run different wefts")
    })
}

/// The recipe `grant` carries, as this weft reads it.
fn recipe(grant: &MovedGrant) -> anyhow::Result<weft_core::AccessSpec> {
    crate::spec_of(format!("connection {}", grant.id), grant.spec.clone(), grant.spec_version)
}

/// Whether two copies of one connection hold the same thing: the recipe
/// compared as this weft reads it, so one stored at another version still
/// matches.
fn same_connection(a: &MovedGrant, b: &MovedGrant) -> anyhow::Result<bool> {
    let comparable = |g: &MovedGrant| -> anyhow::Result<MovedGrant> {
        Ok(MovedGrant { spec: serde_json::to_value(recipe(g)?)?, spec_version: weft_core::AccessSpec::STORED.version(), ..g.clone() })
    };
    Ok(comparable(a)? == comparable(b)?)
}

/// Write each connection of `import`'s batch as `tenant`'s, keeping its
/// id, in one transaction that holds its key (gone already, because
/// another import of it committed first or its time ran out, and nothing
/// is written), and forget the
/// key as it commits. One already here holding exactly the same is already
/// there; one whose id is here holding anything else is left alone,
/// naming how to replace it; one whose account this install already has
/// a connection to (a service with one connection per account) is left
/// alone too; one spending the runtime's own key copies only when this
/// install holds that key (`shared_keys`, the services it does); one this
/// weft cannot read is skipped, saying why.
pub async fn import(import: Import, tenant: &str, shared_keys: &BTreeSet<String>) -> anyhow::Result<ImportedConnections> {
    let Import { pool, key_id, batch } = import;
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    // Still within its time as it is held: the batch is written only under
    // a key that has not expired, however long the caller took over it.
    let held: Option<uuid::Uuid> = tx
        .scalar_opt(
            "SELECT id FROM access_transfer_key WHERE id = $1 AND tenant_id = $2 AND created_at >= $3 FOR UPDATE",
            &[&key_id, &tenant, &(chrono::Utc::now() - TRANSFER_KEY_TTL)],
        )
        .await?;
    if held.is_none() {
        return Err(AccessError::Invalid(KEY_GONE.into()).into());
    }
    let mut connections = Vec::with_capacity(batch.len());
    for grant in batch {
        let outcome = import_one(&tx, tenant, &grant, shared_keys).await?;
        connections.push(ImportedConnection { id: grant.id, service: grant.service, outcome });
    }
    tx.execute("DELETE FROM access_transfer_key WHERE id = $1", &[&key_id]).await?;
    tx.commit().await?;
    Ok(ImportedConnections { connections })
}

/// What writing a moved connection needs, read the way this weft reads
/// its own rows; a reason in words when it does not read here.
struct Readable {
    spec: weft_core::AccessSpec,
    instance: Option<weft_core::instance::InstanceId>,
    door: weft_core::access::spec::Door,
}

fn readable(grant: &MovedGrant) -> Result<Readable, String> {
    let spec = recipe(grant).map_err(|e| format!("{e:#}"))?;
    let instance = grant.instance_id.as_deref().map(weft_core::instance::InstanceId::new).transpose()?;
    let door = door_of(&grant.door).map_err(|e| format!("{e:#}"))?;
    Ok(Readable { spec, instance, door })
}

async fn import_one(
    tx: &Transaction<'_>,
    tenant: &str,
    grant: &MovedGrant,
    shared_keys: &BTreeSet<String>,
) -> anyhow::Result<Outcome> {
    let Readable { spec, instance, door } = match readable(grant) {
        Ok(readable) => readable,
        Err(why) => return Ok(Outcome::Skipped { why: format!("it does not read on this install ({why}); the two installs may run different wefts") }),
    };
    let here: Option<GrantRow> = tx.opt(&format!("SELECT {GRANT_COLUMNS} FROM access_grant WHERE id = $1"), &[&grant.id]).await?;
    if let Some(row) = here {
        if row.tenant_id != tenant {
            return Ok(Outcome::Skipped { why: format!("another account on this install has a connection {}", grant.id) });
        }
        let same = same_connection(&row.opened()?, grant).map_err(|e| e.context(format!("compare connection {}", grant.id)))?;
        return Ok(match same {
            true => Outcome::AlreadyThere,
            false => Outcome::Skipped {
                why: format!(
                    "this install already has a connection {id} holding something else (it was used or refreshed since an \
                     earlier copy); to copy it again, remove that one first (`weft connect --forget {id}` on this install)",
                    id = grant.id
                ),
            },
        });
    }
    // A service with one connection per account keeps one row for it: a
    // second would leave the next sign-in rotating one of them while the
    // other kept a token the provider has revoked.
    if let (weft_core::access::spec::GrantCoexistence::Exclusive, Some(registration)) = (spec.grants, &grant.registration) {
        if let Some(existing) = crate::flows::exclusive_row(
            tx,
            tenant,
            &grant.service,
            grant.identity.as_deref(),
            &registration.client_id,
            instance.as_ref(),
            grant.project_id,
        )
        .await?
        {
            return Ok(Outcome::Skipped {
                why: format!("this install already has a connection to that account ({existing}), which programs can pick instead"),
            });
        }
    }
    if grant.platform && !shared_keys.contains(&grant.service) {
        return Ok(Outcome::Skipped {
            why: format!(
                "it spends the install's own key for '{}', and this install holds none (its shared-credentials file); add the \
                 key there, or connect an account of your own here",
                grant.service
            ),
        });
    }
    crate::resolve::record_events_recipes(tx, &spec).await?;
    let copied = copy_grant(
        tx,
        tenant,
        Moved { id: grant.id, created_at: grant.created_at, expires_at: grant.expires_at, token_lifetime_secs: grant.token_lifetime_secs },
        NewGrant {
            spec: &spec,
            registration: &grant.registration,
            project_id: grant.project_id,
            instance,
            published_by_node: None,
            values: grant.values.clone(),
            granted: grant.granted_scopes.clone(),
            verified: grant.permissions_verified,
            platform: grant.platform,
            door,
            label: grant.label.clone(),
            identity: grant.identity.clone(),
            life: None,
        },
    )
    .await?;
    Ok(match copied {
        Some(_) => Outcome::Copied,
        // A sign-in to the same account landed between the look above
        // and this write: the same as finding it there.
        None => Outcome::Skipped {
            why: "this install already has a connection to that account (signed in while this copy ran), which programs can \
                  pick instead"
                .into(),
        },
    })
}
