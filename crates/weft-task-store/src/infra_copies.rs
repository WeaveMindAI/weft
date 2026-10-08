//! The copies of a project's infra nodes, as every role reads them: the
//! dispatcher's run gate and a worker's door (through the broker) alike.
//! A status no part of weft knows fails the read, naming the row.

/// One copy of an infra node and whether it is up: what a run checks
/// before it starts.
#[derive(Debug, Clone)]
pub struct CopyStatus {
    pub node_id: String,
    pub instance: Option<weft_core::instance::InstanceId>,
    pub status: weft_core::infra::InfraNodeStatus,
    /// When its last apply landed (a start, an upgrade), if one did.
    pub applied_at_unix: Option<i64>,
    /// What it saved for its baked outputs, port to value
    /// (`weft_core::infra::bake`).
    pub baked: std::collections::BTreeMap<String, serde_json::Value>,
}

impl CopyStatus {
    /// The copy as the infra gate and a run's birth read it
    /// (`weft_core::infra::run_gate`).
    pub fn up(&self) -> weft_core::infra::run_gate::InfraCopyUp {
        weft_core::infra::run_gate::InfraCopyUp {
            node_id: self.node_id.clone(),
            instance: self.instance.clone(),
            running: self.status == weft_core::infra::InfraNodeStatus::Running,
            baked: self.baked.clone(),
        }
    }
}

/// One `infra_node` row as [`statuses`] reads it: node, instance,
/// status, last apply, saved bake.
type CopyRow = (String, Option<String>, String, Option<i64>, sqlx::types::Json<std::collections::BTreeMap<String, serde_json::Value>>);

/// Every copy of `project_id`'s infra nodes and its status.
pub async fn statuses(pool: &sqlx::PgPool, project_id: uuid::Uuid) -> anyhow::Result<Vec<CopyStatus>> {
    let rows: Vec<CopyRow> =
        sqlx::query_as("SELECT node_id, instance_id, status, applied_at_unix, baked_json FROM infra_node WHERE project_id = $1")
            .bind(project_id)
            .fetch_all(pool)
            .await?;
    rows.into_iter()
        .map(|(node_id, instance, status, applied_at_unix, baked)| {
            let instance = instance
                .map(weft_core::instance::InstanceId::new)
                .transpose()
                .map_err(|e| anyhow::anyhow!("infra_node.instance_id for project={project_id} node={node_id}: {e}"))?;
            let status = weft_core::infra::InfraNodeStatus::parse(&status).ok_or_else(|| {
                anyhow::anyhow!("infra_node.status='{status}' for project={project_id} node={node_id} is not a status this weft knows")
            })?;
            Ok(CopyStatus { node_id, instance, status, applied_at_unix, baked: baked.0 })
        })
        .collect()
}

