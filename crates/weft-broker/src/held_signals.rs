//! What the listener holds, and the one write it makes to a signal row.
//!
//! The queries behind `signal/list_held`, `signal/get_held` and
//! `signal/write_kind_state`, kept out of the handlers so a database test
//! can drive the exact statements the listener depends on.

use sqlx::{PgPool, Row};
use weft_broker_client::protocol::{
    SignalAuthKind, SignalRowWire, SignalSurfaceKind, LISTENER_HELD_STATUSES, SIGNAL_ACTIVATION_JOIN,
};

/// The signal rows a booting or rehydrating listener must hold: every row
/// whose governing activation is in [`LISTENER_HELD_STATUSES`] (or that
/// none governs). A hibernated or parked activation keeps its rows
/// (reactivate restores them from here) while deactivate told the
/// listener to forget them; handing them back to a restarting listener
/// would revive a parked trigger's timers behind the user's back, so those
/// rows never come out of this query. Mixed tenants: each row carries its
/// own. `project` narrows it to one project's rows.
pub async fn signals_held(pool: &PgPool, project: Option<uuid::Uuid>) -> anyhow::Result<Vec<SignalRowWire>> {
    let rows = sqlx::query(&format!(
        "{} WHERE COALESCE(a.status, 'active') = ANY($1) AND ($2::uuid IS NULL OR s.project_id = $2)",
        held_select()
    ))
    .bind(&LISTENER_HELD_STATUSES[..])
    .bind(project)
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(decode).collect::<anyhow::Result<Vec<_>>>().map_err(|e| anyhow::anyhow!("signal row decode: {e}"))
}

/// One held signal by its token, or `None` when no row with that token is
/// held (gone, or its activation parked): what the listener loads a
/// signal it has not seen yet from.
pub async fn signal_held(pool: &PgPool, token: &str) -> anyhow::Result<Option<SignalRowWire>> {
    let row = sqlx::query(&format!(
        "{} WHERE s.token = $2 AND COALESCE(a.status, 'active') = ANY($1)",
        held_select()
    ))
    .bind(&LISTENER_HELD_STATUSES[..])
    .bind(token)
    .fetch_optional(pool)
    .await?;
    row.map(decode).transpose().map_err(|e| anyhow::anyhow!("signal row decode: {e}"))
}

/// The project of one held signal, or `None` when no row with that token
/// is held: what scopes a listener's question about a signal to that
/// signal's own project.
pub async fn signal_held_project(pool: &PgPool, token: &str) -> anyhow::Result<Option<uuid::Uuid>> {
    let row: Option<(uuid::Uuid,)> = sqlx::query_as(&format!(
        "SELECT s.project_id FROM signal s {SIGNAL_ACTIVATION_JOIN} \
         WHERE s.token = $2 AND COALESCE(a.status, 'active') = ANY($1)"
    ))
    .bind(&LISTENER_HELD_STATUSES[..])
    .bind(token)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(p,)| p))
}

/// Claim a signal kind's moment: write its durable state at version
/// `from_seq + 1`, only while the row is still at `from_seq`. Of two
/// listeners woken for the same moment exactly one wins, and only the
/// winner acts on it; a registration that rewrote the row since the read
/// moved its version, so a claim from before it loses. Returns whether
/// the row was written.
pub async fn write_kind_state(pool: &PgPool, token: &str, kind_state: &serde_json::Value, from_seq: i64) -> anyhow::Result<bool> {
    let res = sqlx::query(
        "UPDATE signal SET kind_state = $2::jsonb, kind_state_seq = $3 + 1 \
         WHERE token = $1 AND kind_state_seq = $3",
    )
    .bind(token)
    .bind(kind_state)
    .bind(from_seq)
    .execute(pool)
    .await?;
    Ok(res.rows_affected() > 0)
}

/// Every column a held-signal read decodes, with the governing
/// activation joined as `a`.
fn held_select() -> String {
    format!(
        "SELECT s.token, s.tenant_id, s.node_id, s.spec_json, s.is_resume, s.execution_id, \
                s.surface_kind, s.mount_path, s.mount_methods, s.auth_kind, s.auth_config, \
                s.kind_state, s.kind_state_seq, s.project_id, s.member_id \
         FROM signal s {SIGNAL_ACTIVATION_JOIN}"
    )
}

// try_get on a NOT NULL column should never fail; if it does the row
// shape has drifted from the schema. Surface that loudly rather than
// silently returning empty strings to the listener.
fn decode(r: sqlx::postgres::PgRow) -> anyhow::Result<SignalRowWire> {
    let surface_str: String = r.try_get("surface_kind")?;
    let surface_kind = SignalSurfaceKind::parse(&surface_str)
        .ok_or_else(|| anyhow::anyhow!("unknown surface_kind '{surface_str}'"))?;
    let auth_str: String = r.try_get("auth_kind")?;
    let auth_kind = SignalAuthKind::parse(&auth_str)
        .ok_or_else(|| anyhow::anyhow!("unknown auth_kind '{auth_str}'"))?;
    let for_member = weft_core::member::MemberScope::from_columns(r.try_get("project_id")?, r.try_get("member_id")?)
        .map_err(|e| anyhow::anyhow!("corrupt member_id: {e}"))?;
    Ok(SignalRowWire {
        token: r.try_get("token")?,
        tenant_id: r.try_get("tenant_id")?,
        for_member,
        node_id: r.try_get("node_id")?,
        spec_json: r.try_get("spec_json")?,
        is_resume: r.try_get("is_resume")?,
        execution_id: r.try_get("execution_id")?,
        surface_kind,
        mount_path: r.try_get("mount_path")?,
        mount_methods: r.try_get("mount_methods")?,
        auth_kind,
        auth_config: r.try_get("auth_config")?,
        kind_state: r.try_get("kind_state")?,
        kind_state_seq: r.try_get("kind_state_seq")?,
    })
}
