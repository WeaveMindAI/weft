//! What a listener pod holds: the one query behind `signal/list_for_pod`,
//! kept out of the handler so a database test can drive the exact
//! statement the pod rehydrates from.

use sqlx::{PgPool, Row};
use weft_broker_client::protocol::{
    SignalAuthKind, SignalRowWire, SignalSurfaceKind, LISTENER_HELD_PROJECT_STATUSES,
};

/// The signal rows a booting or rehydrating pod must hold: every row
/// placed on `pod_name` whose project is in
/// [`LISTENER_HELD_PROJECT_STATUSES`]. A hibernated or parked project
/// keeps its rows placed on the pod (reactivate restores them from
/// here) while deactivate told the pod to forget them; handing them
/// back to a restarting pod would revive a parked project's timers
/// behind the user's back, so those rows never come out of this query.
/// Mixed tenants: each row carries its own.
pub async fn signals_held_by_pod(pool: &PgPool, pod_name: &str) -> anyhow::Result<Vec<SignalRowWire>> {
    let rows = sqlx::query(
        "SELECT s.token, s.tenant_id, s.node_id, s.spec_json, s.is_resume, s.color, \
                s.surface_kind, s.mount_path, s.auth_kind, s.auth_config, \
                s.kind_state, s.kind_state_seq, s.placement_generation \
         FROM signal s JOIN project p ON p.id::TEXT = s.project_id \
         WHERE s.listener_pod = $1 AND p.status = ANY($2)",
    )
    .bind(pod_name)
    .bind(&LISTENER_HELD_PROJECT_STATUSES[..])
    .fetch_all(pool)
    .await?;
    // try_get on a NOT NULL column should never fail; if it does the
    // row shape has drifted from the schema. Surface that loudly
    // rather than silently returning empty strings to the listener.
    rows.into_iter()
        .map(|r| -> anyhow::Result<SignalRowWire> {
            let surface_str: String = r.try_get("surface_kind")?;
            let surface_kind = SignalSurfaceKind::parse(&surface_str)
                .ok_or_else(|| anyhow::anyhow!("unknown surface_kind '{surface_str}'"))?;
            let auth_str: String = r.try_get("auth_kind")?;
            let auth_kind = SignalAuthKind::parse(&auth_str)
                .ok_or_else(|| anyhow::anyhow!("unknown auth_kind '{auth_str}'"))?;
            Ok(SignalRowWire {
                token: r.try_get("token")?,
                tenant_id: r.try_get("tenant_id")?,
                node_id: r.try_get("node_id")?,
                spec_json: r.try_get("spec_json")?,
                is_resume: r.try_get("is_resume")?,
                color: r.try_get("color")?,
                surface_kind,
                mount_path: r.try_get("mount_path")?,
                auth_kind,
                auth_config: r.try_get("auth_config")?,
                kind_state: r.try_get("kind_state")?,
                kind_state_seq: r.try_get("kind_state_seq")?,
                placement_generation: r.try_get("placement_generation")?,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()
        .map_err(|e| anyhow::anyhow!("signal row decode: {e}"))
}
