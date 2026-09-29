//! The exclusive ownership of a project's infrastructure.
//!
//! `infra_owner(project_id PK, supervisor_instance, tenant_id,
//! leased_until_unix)`: exactly one supervisor instance applies a project's
//! infra at a time, so two never change the same project's infrastructure
//! at once (which would corrupt it). A supervisor claims and renews these
//! leases on its ownership tick (the broker's `sync_ownership`); a dead
//! one's leases expire and a live one adopts them.

use anyhow::Result;
use sqlx::postgres::PgPool;

pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "infra_owner",
    tables: &["infra_owner"],
    ddl: &[
        // The EXCLUSIVE ownership lease. One row per project whose infra is
        // currently owned by a supervisor, carrying the tenant the apply
        // path needs.
        r#"CREATE TABLE IF NOT EXISTS infra_owner (
            project_id          UUID PRIMARY KEY,
            supervisor_instance TEXT NOT NULL,
            tenant_id           TEXT NOT NULL,
            leased_until_unix   BIGINT NOT NULL
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_infra_owner_instance
             ON infra_owner(supervisor_instance)"#,
    ],
    seed: &[],
};

/// Release a project's exclusive supervisor lease (its `infra_owner`
/// row). Called by project removal alongside the other `infra_*` row
/// drops: the owning supervisor stops reconciling the project the moment
/// the row is gone (its loop only acts on projects it owns), and a row
/// left behind would be renewed forever.
pub async fn release_project(pg_pool: &PgPool, project_id: uuid::Uuid) -> Result<u64> {
    let res = sqlx::query("DELETE FROM infra_owner WHERE project_id = $1")
        .bind(project_id)
        .execute(pg_pool)
        .await?;
    Ok(res.rows_affected())
}

/// Drop `infra_owner` rows whose project no longer exists. Should never
/// fire now that project removal releases the lease in the same pass
/// (`release_project`); kept as self-healing for rows minted before that
/// fix or by a removal that died between the two writes, and it WARNS so
/// a recurring leak is visible instead of silently mopped.
pub async fn release_ghost_leases(pg_pool: &PgPool) -> Result<()> {
    let res = sqlx::query(
        "DELETE FROM infra_owner io \
         WHERE NOT EXISTS (SELECT 1 FROM project p WHERE p.id = io.project_id)",
    )
    .execute(pg_pool)
    .await?;
    if res.rows_affected() > 0 {
        tracing::warn!(
            target: "weft_dispatcher::infra_owner",
            dropped = res.rows_affected(),
            "dropped infra_owner leases pointing at deleted projects; \
             a recurring count here means some removal path skips release_project"
        );
    }
    Ok(())
}
