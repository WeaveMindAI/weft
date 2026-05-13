//! Worker / listener / sidecar facing read surface against the
//! `infra_pod` table. The table is owned by the dispatcher (it
//! provisions sidecars and writes the rows); this crate exists so
//! engine code can read it without depending on the dispatcher
//! crate, and so a broker-client implementation can sit alongside
//! the Postgres-direct one.

use anyhow::Result;
use async_trait::async_trait;
use sqlx::postgres::PgPool;

/// Worker-facing read surface for `infra_pod`. Two implementations:
///   - `PostgresInfraReader` (this crate): direct DB. Used by the
///     dispatcher and by the broker (after its scope check).
///   - `BrokerInfraClient` (in `weft-broker-client`): HTTP through
///     the broker. Used by workers and listeners.
#[async_trait]
pub trait InfraReader: Send + Sync {
    /// Look up a sidecar's cluster-internal endpoint URL for a
    /// `(project, node)` pair. Returns None when the sidecar is not
    /// running. Used by `ctx.sidecar_endpoint` inside node code.
    async fn sidecar_endpoint(&self, project_id: &str, node_id: &str) -> Result<Option<String>>;
}

pub struct PostgresInfraReader {
    pool: PgPool,
}

impl PostgresInfraReader {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl InfraReader for PostgresInfraReader {
    async fn sidecar_endpoint(&self, project_id: &str, node_id: &str) -> Result<Option<String>> {
        use sqlx::Row;
        let row = sqlx::query(
            "SELECT endpoint_url FROM infra_pod \
             WHERE project_id = $1 AND node_id = $2 AND status = 'running'",
        )
        .bind(project_id)
        .bind(node_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.and_then(|r| r.try_get::<Option<String>, _>("endpoint_url").ok().flatten()))
    }
}
