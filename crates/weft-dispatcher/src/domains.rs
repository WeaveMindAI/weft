//! The domains an install answers at (`weft domain add|list|rm`).
//!
//! Kept in the install's database rather than its config file: the CLI
//! adds one over HTTP, wherever the dispatcher runs; the platform's door
//! (`weft_platform_traits::DomainHosting`) holds a certificate for each,
//! and the dispatcher's own door (`crate::door`) routes by them. A
//! project's domains go with it.
//!
//! The platform's door follows the stored domains on its own: every row
//! that comes or goes announces itself ([`DOMAINS_CHANNEL`]), and
//! [`drain_loop`] puts the door on what is stored then, retrying while
//! the platform refuses. So a change whose request was cut off, failed
//! halfway, or never touched the door itself (a project removed with
//! its domains) still ends with the door matching the rows, and nothing
//! billed stands for a domain nobody keeps. `weft domain add` and `rm`
//! also change the door themselves, to answer with the outcome.

use std::net::IpAddr;
use std::time::Duration;

use anyhow::Result;
use sqlx::PgPool;
use weft_core::install::{Domain, DomainServes};
use weft_platform_traits::DomainHosting;
use weft_task_store::drain::{DrainLoop, DrainStep, WakeOn, DB_NOW_MS};

use crate::state::DispatcherState;

/// Announced when a domain row comes or goes (the table's triggers).
// SYNC: DOMAINS_CHANNEL <-> GROUP's install_domain_notify (below)
pub const DOMAINS_CHANNEL: &str = "weft_install_domains";

pub(crate) static ON_DOMAINS: &[WakeOn] = &[WakeOn::any(DOMAINS_CHANNEL)];

// SYNC: install_domain <-> crates/weft-core/src/install.rs (Domain)
pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "install_domain",
    tables: &["install_domain", "install_domain_door"],
    ddl: &[r#"CREATE TABLE IF NOT EXISTS install_domain (
            -- Lower case, as `weft_core::install::normalize_domain_name`
            -- leaves it.
            name TEXT PRIMARY KEY,
            -- What it serves: 'install', 'frontend' or 'api'.
            serves TEXT NOT NULL CHECK (serves IN ('install', 'frontend', 'api')),
            -- The project a frontend or API domain belongs to.
            project_id UUID REFERENCES project(id) ON DELETE CASCADE,
            -- Where a frontend runs (its service's https address).
            upstream TEXT,
            added_unix BIGINT NOT NULL,
            CHECK ((serves = 'install') = (project_id IS NULL)),
            CHECK ((serves = 'frontend') = (upstream IS NOT NULL))
        )"#,
        // Every row that comes or goes, a project's removal included,
        // wakes the loop that puts the platform's door on the rows.
        // SYNC: 'weft_install_domains' <-> DOMAINS_CHANNEL (above)
        r#"CREATE OR REPLACE FUNCTION install_domain_notify() RETURNS trigger AS $$
            BEGIN
                PERFORM pg_notify('weft_install_domains', '');
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql"#,
        // What the platform's door last did: the names it last accepted
        // and when (so the pass a change wakes right after it served them
        // asks the platform nothing again), and, while it keeps refusing,
        // why and how many times in a row (so the retries back off, and
        // `weft domain list` says so). One row, and only ever a hint: a
        // pass that finds it stale, or cannot read it, asks the platform.
        r#"CREATE TABLE IF NOT EXISTS install_domain_door (
            singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
            served TEXT[] NOT NULL,
            -- When the door last accepted `served` (unix ms, the
            -- database's clock).
            served_at_ms BIGINT NOT NULL,
            refused TEXT,
            refusals INT NOT NULL
        )"#,
        r#"DROP TRIGGER IF EXISTS install_domain_changed ON install_domain"#,
        r#"CREATE TRIGGER install_domain_changed
            AFTER INSERT OR DELETE ON install_domain
            FOR EACH ROW
            EXECUTE FUNCTION install_domain_notify()"#,
    ],
    seed: &[],
};

#[derive(sqlx::FromRow)]
struct Row {
    name: String,
    serves: String,
    project_id: Option<uuid::Uuid>,
    upstream: Option<String>,
}

impl Row {
    fn into_domain(self) -> Result<Domain> {
        let serves = match (self.serves.as_str(), self.project_id, self.upstream) {
            ("install", None, None) => DomainServes::Install,
            ("frontend", Some(project), Some(upstream)) => DomainServes::Frontend { project, upstream },
            ("api", Some(project), None) => DomainServes::Api { project },
            (other, _, _) => anyhow::bail!("the stored domain '{}' has an unreadable shape ('{other}')", self.name),
        };
        Ok(Domain { name: self.name, serves })
    }
}

/// What storing a domain did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Added {
    /// It is stored now.
    New,
    /// Exactly this domain was stored already.
    AlreadyStored,
    /// The name is stored for something else, which it never quietly
    /// moves from.
    TakenForSomethingElse,
}

/// Store `domain`.
pub async fn add(pool: &PgPool, domain: &Domain, now_unix: i64) -> Result<Added> {
    domain.validate().map_err(anyhow::Error::msg)?;
    let (serves, project, upstream) = match &domain.serves {
        DomainServes::Install => ("install", None, None),
        DomainServes::Frontend { project, upstream } => ("frontend", Some(*project), Some(upstream.as_str())),
        DomainServes::Api { project } => ("api", Some(*project), None),
    };
    let added = sqlx::query(
        "INSERT INTO install_domain (name, serves, project_id, upstream, added_unix) \
         VALUES ($1, $2, $3, $4, $5) ON CONFLICT (name) DO NOTHING",
    )
    .bind(&domain.name)
    .bind(serves)
    .bind(project)
    .bind(upstream)
    .bind(now_unix)
    .execute(pool)
    .await?
    .rows_affected();
    if added == 1 {
        return Ok(Added::New);
    }
    Ok(match list(pool).await?.iter().any(|stored| stored == domain) {
        true => Added::AlreadyStored,
        false => Added::TakenForSomethingElse,
    })
}

/// Every stored domain, by name.
pub async fn list(pool: &PgPool) -> Result<Vec<Domain>> {
    sqlx::query_as::<_, Row>("SELECT name, serves, project_id, upstream FROM install_domain ORDER BY name")
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(Row::into_domain)
        .collect()
}

/// Remove `name`. `false` when it was not stored.
pub async fn remove(pool: &PgPool, name: &str) -> Result<bool> {
    Ok(sqlx::query("DELETE FROM install_domain WHERE name = $1").bind(name).execute(pool).await?.rows_affected() > 0)
}

/// Why the platform's door could not be put on the stored domains.
#[derive(Debug)]
pub enum ServeError {
    /// Reading or writing the stored domains.
    Store(anyhow::Error),
    /// The platform's door.
    Door(anyhow::Error),
}

/// Put the platform's door on every stored domain, and answer its address
/// (`None` once no domain is left). Callers hold the domains' lock
/// (`crate::lease::DOMAINS_DOMAIN`), so the door serves what is stored.
pub async fn serve_stored(pool: &PgPool, hosting: &dyn DomainHosting) -> Result<Option<IpAddr>, ServeError> {
    let domains = list(pool).await.map_err(ServeError::Store)?;
    serve_domains(pool, hosting, &domains).await
}

/// What the door's record says it serves for `domain`: its name, and the
/// project an API domain is sent to (the door routes it there itself).
fn served_as(domain: &Domain) -> String {
    match &domain.serves {
        DomainServes::Api { project } => format!("{} api {project}", domain.name),
        DomainServes::Install | DomainServes::Frontend { .. } => domain.name.clone(),
    }
}

/// Put the door on exactly `domains` (sorted by name), for a change a
/// person made, and record what it did. The record is a hint the next
/// pass reads, so failing to write it is logged and changes nothing about
/// the answer the person gets; [`settle`] writes it strictly.
async fn serve_domains(pool: &PgPool, hosting: &dyn DomainHosting, domains: &[Domain]) -> Result<Option<IpAddr>, ServeError> {
    let served_as: Vec<String> = domains.iter().map(served_as).collect();
    let (served, recorded) = match hosting.serve(domains).await {
        Ok(address) => (Ok(address), record_served(pool, &served_as).await),
        Err(refused) => {
            let recorded = record_refused(pool, &format!("{refused:#}")).await;
            (Err(ServeError::Door(refused)), recorded)
        }
    };
    if let Err(e) = recorded {
        tracing::warn!(target: "weft_dispatcher::domains", error = %format!("{e:#}"), "could not record what the door did; the next pass asks the platform again");
    }
    served
}

/// What the platform's door last did (`install_domain_door`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DoorState {
    /// The names it last accepted, sorted, and how long ago (ms, the
    /// database's clock); `None` before it was ever asked.
    pub served: Option<(Vec<String>, i64)>,
    /// Why it refused, while it keeps refusing.
    pub refused: Option<String>,
    /// How many times in a row it refused.
    pub refusals: i32,
}

pub async fn door_state(pool: &PgPool) -> Result<DoorState> {
    let row: Option<(Vec<String>, i64, Option<String>, i32)> = sqlx::query_as(&format!(
        "SELECT served, {DB_NOW_MS} - served_at_ms, refused, refusals FROM install_domain_door"
    ))
    .fetch_optional(pool)
    .await?;
    Ok(row.map_or_else(DoorState::default, |(served, ago, refused, refusals)| DoorState {
        served: Some((served, ago)),
        refused,
        refusals,
    }))
}


async fn record_served(pool: &PgPool, names: &[String]) -> Result<()> {
    sqlx::query(&format!(
        "INSERT INTO install_domain_door (singleton, served, served_at_ms, refused, refusals) VALUES (TRUE, $1, {DB_NOW_MS}, NULL, 0) \
         ON CONFLICT (singleton) DO UPDATE SET served = EXCLUDED.served, served_at_ms = EXCLUDED.served_at_ms, \
             refused = NULL, refusals = 0"
    ))
    .bind(names)
    .execute(pool)
    .await?;
    Ok(())
}

/// Record a refusal. What the door last accepted stays: it may still
/// stand.
async fn record_refused(pool: &PgPool, why: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO install_domain_door (singleton, served, served_at_ms, refused, refusals) VALUES (TRUE, '{}', 0, $1, 1) \
         ON CONFLICT (singleton) DO UPDATE SET refused = EXCLUDED.refused, refusals = install_domain_door.refusals + 1",
    )
    .bind(why)
    .execute(pool)
    .await?;
    Ok(())
}

/// How long after the door accepted the stored domains a pass trusts the
/// record instead of asking the platform: the pass the same change woke,
/// which runs right after it. Past that, a pass asks, so a door changed
/// outside the install is put back at its next look.
const SERVED_JUST_NOW: Duration = Duration::from_secs(60);

/// Put the door on the stored domains now that `added` is one of them
/// (`new` when this add stored it). When the door refuses a domain this
/// add stored, it is forgotten again, and [`drain_loop`] puts the door
/// back on the domains that remain (taking it down when none does).
pub async fn serve_added(pool: &PgPool, hosting: &dyn DomainHosting, added: &str, new: bool) -> Result<IpAddr, ServeError> {
    let refused = match serve_stored(pool, hosting).await {
        Ok(Some(address)) => return Ok(address),
        Ok(None) => anyhow::anyhow!("the platform answered no door for a stored domain"),
        Err(ServeError::Door(e)) => e,
        Err(store) => return Err(store),
    };
    if !new {
        // Stored before this add: it stays, and adding it again retries.
        return Err(ServeError::Door(refused));
    }
    if let Err(e) = remove(pool, added).await {
        return Err(ServeError::Door(anyhow::anyhow!(
            "{refused:#}; forgetting '{added}' again failed too ({e:#}), so it stays stored: \
             `weft domain rm {added}` forgets it"
        )));
    }
    Err(ServeError::Door(refused.context(format!(
        "'{added}' is forgotten again; the door goes back to the domains that remain on its own"
    ))))
}

/// Stop answering at `name`, one of `stored`, and forget it. The door
/// goes first: if it refuses, `name` stays stored and removing it again
/// retries the whole step.
pub async fn unserve(pool: &PgPool, hosting: &dyn DomainHosting, stored: &[Domain], name: &str) -> Result<(), ServeError> {
    let rest: Vec<Domain> = stored.iter().filter(|d| d.name != name).cloned().collect();
    serve_domains(pool, hosting, &rest).await?;
    remove(pool, name).await.map_err(ServeError::Store)?;
    Ok(())
}

/// How soon the door is put on the stored domains again after the
/// platform refused once; each refusal in a row doubles it, up to the
/// idle look, so a refusal that needs a person does not keep the install
/// awake.
fn door_retry(refusals: i32) -> Duration {
    let first = weft_core::time_scale::scaled(Duration::from_secs(300));
    let doublings = u32::try_from(refusals.saturating_sub(1).clamp(0, 16)).unwrap_or(16);
    first.saturating_mul(1 << doublings).min(weft_task_store::drain::IDLE_LOOK)
}

/// Put the platform's door on what is stored, when the rows change and
/// now and then, one change to the domains at a time install-wide (the
/// lock `weft domain add` and `rm` take). Asks the platform nothing when
/// the door already serves exactly what is stored. A refusal is logged,
/// recorded for `weft domain list`, and retried, later each time.
pub fn drain_loop(state: &DispatcherState) -> DrainLoop {
    let state = state.clone();
    DrainLoop::new("domains_door", ON_DOMAINS, door_retry(1), move || {
        let state = state.clone();
        async move {
            let key = crate::lease::advisory_key(crate::lease::DOMAINS_DOMAIN, "install");
            let settled = crate::lease::with_advisory_lock(&state.lock_pool, key, || settle(&state.pg_pool, state.domains.as_ref())).await?;
            // A change to the domains holding the lock may have read before
            // the write this wake is for.
            Ok(settled.unwrap_or(DrainStep::RetryIn(weft_task_store::drain::LOCK_HELD_RETRY)))
        }
    })
}

/// One pass of [`drain_loop`], under the domains' lock: put the door on
/// what is stored unless it serves exactly that already, and answer when
/// to look again.
pub async fn settle(pool: &PgPool, hosting: &dyn DomainHosting) -> Result<DrainStep> {
    let domains = list(pool).await?;
    let names: Vec<String> = domains.iter().map(served_as).collect();
    let door = door_state(pool).await?;
    let just_served = door.refused.is_none()
        && door.served.as_ref().is_some_and(|(served, ago_ms)| {
            *served == names && u64::try_from(*ago_ms).is_ok_and(|ago| ago < SERVED_JUST_NOW.as_millis() as u64)
        });
    // Nothing stored and no door: nothing to ask the platform to change.
    if just_served || (names.is_empty() && hosting.address().await?.is_none()) {
        return Ok(DrainStep::Done);
    }
    // The record decides the next pass here (the backoff, what `weft
    // domain list` says), so failing to write it fails the pass, which
    // the loop retries.
    match hosting.serve(&domains).await {
        Ok(_) => {
            record_served(pool, &names).await?;
            Ok(DrainStep::Done)
        }
        Err(refused) => {
            record_refused(pool, &format!("{refused:#}")).await?;
            let retry = door_retry(door.refusals + 1);
            tracing::warn!(
                target: "weft_dispatcher::domains",
                error = %format!("{refused:#}"), retry_in_secs = retry.as_secs(),
                "the platform's door refused to follow the stored domains; `weft domain list` shows why, and it is tried again"
            );
            Ok(DrainStep::RetryIn(retry))
        }
    }
}
