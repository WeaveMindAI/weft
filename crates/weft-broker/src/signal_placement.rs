//! What a listener pod holds: the one query behind `signal/list_for_pod`,
//! kept out of the handler so a database test can drive the exact
//! statement the pod rehydrates from.

use sqlx::{PgPool, Row};
use weft_broker_client::protocol::{
    SignalAuthKind, SignalRowWire, SignalSurfaceKind, LISTENER_HELD_STATUSES, SIGNAL_ACTIVATION_JOIN,
};

/// The signal rows a booting or rehydrating pod must hold: every row
/// placed on `pod_name` whose governing activation is in
/// [`LISTENER_HELD_STATUSES`] (or that none governs). A hibernated or
/// parked activation keeps its rows placed on the pod (reactivate
/// restores them from here) while deactivate told the pod to forget
/// them; handing them back to a restarting pod would revive a parked
/// trigger's timers behind the user's back, so those rows never come
/// out of this query. Mixed tenants: each row carries its own.
pub async fn signals_held_by_pod(pool: &PgPool, pod_name: &str) -> anyhow::Result<Vec<SignalRowWire>> {
    let rows = sqlx::query(&format!(
        "SELECT s.token, s.tenant_id, s.node_id, s.spec_json, s.is_resume, s.color, \
                s.surface_kind, s.mount_path, s.mount_methods, s.auth_kind, s.auth_config, \
                s.kind_state, s.kind_state_seq, s.placement_generation, s.project_id, s.member_id \
         FROM signal s {SIGNAL_ACTIVATION_JOIN} \
         WHERE s.listener_pod = $1 AND COALESCE(a.status, 'active') = ANY($2)"
    ))
    .bind(pod_name)
    .bind(&LISTENER_HELD_STATUSES[..])
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
            let for_member =
                weft_core::member::MemberScope::from_columns(r.try_get("project_id")?, r.try_get("member_id")?)
                    .map_err(|e| anyhow::anyhow!("corrupt member_id: {e}"))?;
            Ok(SignalRowWire {
                token: r.try_get("token")?,
                tenant_id: r.try_get("tenant_id")?,
                for_member,
                node_id: r.try_get("node_id")?,
                spec_json: r.try_get("spec_json")?,
                is_resume: r.try_get("is_resume")?,
                color: r.try_get("color")?,
                surface_kind,
                mount_path: r.try_get("mount_path")?,
                mount_methods: r.try_get("mount_methods")?,
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
