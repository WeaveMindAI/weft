//! Worker / listener / infra-pod facing read surface against the
//! `infra_node` table. The table is owned by the dispatcher (it
//! provisions infrastructure and writes the rows); this crate exists
//! so engine code can read it without depending on the dispatcher
//! crate, and so a broker-client implementation can sit alongside
//! the Postgres-direct one.

use anyhow::Result;
use async_trait::async_trait;
use sqlx::postgres::PgPool;
use sqlx::Row;

/// Worker / listener facing read surface for `infra_node`. Two
/// implementations:
///   - `PostgresInfraReader` (this crate): direct DB. Used by the
///     dispatcher and by the broker (after its scope check).
///   - `BrokerInfraClient` (in `weft-broker-client`): HTTP through
///     the broker. Used by workers and listeners.
#[async_trait]
pub trait InfraReader: Send + Sync {
    /// Look up the cluster-internal URL for an `(infra_node,
    /// endpoint_name)` triple. Returns None when the infra node is
    /// not Running, or when the endpoint name isn't declared. Used
    /// by `ctx.endpoint(name)` inside node code at fire-time, which
    /// caches the URL on an `EndpointHandle`.
    async fn endpoint_url(
        &self,
        project_id: &str,
        node_id: &str,
        endpoint_name: &str,
    ) -> Result<Option<String>>;
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
    async fn endpoint_url(
        &self,
        project_id: &str,
        node_id: &str,
        endpoint_name: &str,
    ) -> Result<Option<String>> {
        let row = sqlx::query(
            "SELECT endpoints_json FROM infra_node \
             WHERE project_id = $1 AND node_id = $2 AND status = 'running'",
        )
        .bind(project_id)
        .bind(node_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row
            .and_then(|r| r.try_get::<serde_json::Value, _>("endpoints_json").ok())
            .and_then(|v| {
                v.as_object()
                    .and_then(|m| m.get(endpoint_name))
                    .and_then(|val| val.as_str().map(|s| s.to_string()))
            }))
    }
}
