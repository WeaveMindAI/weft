//! The domains an install answers at (`weft domain add|list|rm`).
//!
//! Kept in the install's database rather than its config file: the CLI
//! adds one over HTTP, wherever the dispatcher runs, and the front door on
//! the machine reads them to get each a certificate and to route by it.
//! A project's domains go with the project.

use anyhow::Result;
use sqlx::PgPool;
use weft_core::install::{Domain, DomainServes};

// SYNC: install_domain <-> crates/weft-core/src/install.rs (Domain)
pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "install_domain",
    tables: &["install_domain"],
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
        )"#],
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

/// Store `domain`. A name already stored is refused, naming what it
/// serves, so a domain never quietly moves.
pub async fn add(pool: &PgPool, domain: &Domain, now_unix: i64) -> Result<()> {
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
    if added == 0 {
        anyhow::bail!("'{}' is already one of this install's domains; remove it first to point it elsewhere", domain.name);
    }
    Ok(())
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

/// Whether the install itself answers at a domain (not only at its
/// address).
pub async fn install_has_domain(pool: &PgPool) -> Result<bool> {
    Ok(sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM install_domain WHERE serves = 'install')")
        .fetch_one(pool)
        .await?)
}
