//! What the listener holds, the claims holders take on held signals, and
//! the writes the listener makes to a signal row.
//!
//! The queries behind `signal/list_held`, `signal/get_held`,
//! `signal/write_kind_state`, `signal/hold`, `signal/let_go` and
//! `signal/set_holds`, kept out
//! of the handlers so a database test can drive the exact statements the
//! listener depends on. Every time is the database's, never a process's,
//! so a holder with a skewed clock cannot misjudge a claim.

use sqlx::{PgPool, Row};
use weft_broker_client::protocol::{
    HeldNow, HeldServing, SignalAuthKind, SignalHoldResponse, SignalRowWire, SignalSurfaceKind, LISTENER_HELD_STATUSES,
    SIGNAL_ACTIVATION_JOIN, STILL_HELD_BY,
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

/// One look of holder `replica`: renew its claims on the `holding` it
/// still may hold (writing what each connection says it is doing, when it
/// said), name the ones of them that ended (their row is gone or no
/// longer held), then take up to `room` more (every one there is when
/// `None`), the `want` ones first, from the held signals no live holder
/// claims and the ones it claims but does not say it holds (it restarted
/// under its name, or it wants one taken afresh). In one transaction, and
/// the take skips rows a sibling is taking, so two holders never take one
/// signal.
pub async fn hold(
    pool: &PgPool,
    replica: &str,
    holding: &[HeldNow],
    room: Option<u32>,
    want: &[String],
    lease_secs: i64,
) -> anyhow::Result<SignalHoldResponse> {
    let tokens: Vec<&str> = holding.iter().map(|h| h.token.as_str()).collect();
    let servings: Vec<Option<serde_json::Value>> =
        holding.iter().map(|h| h.serving.as_ref().map(serde_json::to_value).transpose()).collect::<Result<_, _>>()?;
    let mut tx = pool.begin().await?;
    let kept: Vec<String> = sqlx::query_scalar(&format!(
        "UPDATE signal r SET held_until = EXTRACT(EPOCH FROM NOW())::BIGINT + $3, \
                serving = COALESCE(x.serving, r.serving) \
         FROM unnest($1::text[], $2::jsonb[]) AS x(token, serving) \
         WHERE r.token = x.token AND r.holds AND r.held_by = $4 AND {held} \
         RETURNING r.token",
        held = activation_held("r", "$5"),
    ))
    .bind(&tokens)
    .bind(&servings)
    .bind(lease_secs)
    .bind(replica)
    .bind(&LISTENER_HELD_STATUSES[..])
    .fetch_all(&mut *tx)
    .await?;
    let ended: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT x.token FROM unnest($1::text[]) AS x(token) WHERE NOT {held}",
        held = activation_held("x", "$2"),
    ))
    .bind(&tokens)
    .bind(&LISTENER_HELD_STATUSES[..])
    .fetch_all(&mut *tx)
    .await?;
    let room = room.map(|r| i64::from(r).saturating_sub(kept.len() as i64).max(0));
    let taken: Vec<String> = sqlx::query_scalar(&format!(
        "WITH free AS ( \
             SELECT r.token FROM signal r \
             WHERE r.holds AND (r.held_by IS NULL OR r.held_by = $4 OR r.held_until < EXTRACT(EPOCH FROM NOW())::BIGINT) \
               AND NOT (r.token = ANY($1)) AND {held} \
             ORDER BY (r.token = ANY($2)) DESC, r.created_at \
             LIMIT $3 \
             FOR UPDATE OF r SKIP LOCKED \
         ) \
         UPDATE signal s SET held_by = $4, held_until = EXTRACT(EPOCH FROM NOW())::BIGINT + $5, serving = NULL \
         FROM free WHERE s.token = free.token \
         RETURNING s.token",
        held = activation_held("r", "$6"),
    ))
    .bind(&tokens)
    .bind(want)
    .bind(room)
    .bind(replica)
    .bind(lease_secs)
    .bind(&LISTENER_HELD_STATUSES[..])
    .fetch_all(&mut *tx)
    .await?;
    let rows = sqlx::query(&format!("{} WHERE s.token = ANY($1)", held_select()))
        .bind(&taken)
        .fetch_all(&mut *tx)
        .await?;
    tx.commit().await?;
    let taken = rows.into_iter().map(decode).collect::<anyhow::Result<Vec<_>>>().map_err(|e| anyhow::anyhow!("signal row decode: {e}"))?;
    Ok(SignalHoldResponse { kept, ended, taken })
}

/// Whether `token`'s row is still held under `replica`'s claim
/// ([`STILL_HELD_BY`]).
pub async fn still_held_by(pool: &PgPool, token: &str, replica: &str) -> anyhow::Result<bool> {
    Ok(sqlx::query_scalar(STILL_HELD_BY).bind(token).bind(replica).fetch_one(pool).await?)
}

/// What becomes of a fire as it arrives from `sender` (the calling
/// replica).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeldFire {
    /// It is taken: not a held connection's, or its holder still holds
    /// the row.
    Taken,
    /// It names a holder other than the one sending it.
    NotItsSender,
    /// Its holder no longer holds the row (another took it, or it was
    /// registered again to be served another way): it delivers nothing
    /// from then on, and hears so at once.
    NoLongerHeld,
}

pub async fn judge_held_fire(
    pool: &PgPool,
    sender: Option<&str>,
    fire: &weft_task_store::kinds::FireSignalPayload,
) -> anyhow::Result<HeldFire> {
    let Some(held_by) = fire.held_by.as_deref() else { return Ok(HeldFire::Taken) };
    if sender != Some(held_by) {
        return Ok(HeldFire::NotItsSender);
    }
    Ok(match still_held_by(pool, &fire.token, held_by).await? {
        true => HeldFire::Taken,
        false => HeldFire::NoLongerHeld,
    })
}

/// Record whether `token` keeps a connection open, as its kind decides
/// now. A row that stops holding lets its holder go with it.
pub async fn set_holds(pool: &PgPool, token: &str, holds: bool) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE signal SET holds = $2, \
                held_by = CASE WHEN $2 THEN held_by END, held_until = CASE WHEN $2 THEN held_until END, \
                serving = CASE WHEN $2 THEN serving END \
         WHERE token = $1",
    )
    .bind(token)
    .bind(holds)
    .execute(pool)
    .await?;
    Ok(())
}

/// Give up every claim holder `replica` holds, so another holder takes
/// them at its next look.
pub async fn let_go(pool: &PgPool, replica: &str) -> anyhow::Result<()> {
    sqlx::query("UPDATE signal SET held_by = NULL, held_until = NULL, serving = NULL WHERE held_by = $1")
        .bind(replica)
        .execute(pool)
        .await?;
    Ok(())
}

/// Whether the token `row.token` names a signal row that a listener holds
/// (`statuses` names the bound list of [`LISTENER_HELD_STATUSES`]): false
/// when the row is gone or its activation is parked. A condition an
/// UPDATE can carry, read through the one [`SIGNAL_ACTIVATION_JOIN`].
/// That join names its signal `s`, so `row` is any other alias.
fn activation_held(row: &str, statuses: &str) -> String {
    format!(
        "EXISTS (SELECT 1 FROM signal s {SIGNAL_ACTIVATION_JOIN} \
                 WHERE s.token = {row}.token AND COALESCE(a.status, 'active') = ANY({statuses}))"
    )
}

/// Every column a held-signal read decodes, with the governing
/// activation joined as `a`. A held signal's serving shows only while a
/// holder's claim is live: a dead holder's last word is not the truth.
fn held_select() -> String {
    format!(
        "SELECT s.token, s.tenant_id, s.node_id, s.spec_json, s.is_resume, s.execution_id, \
                s.surface_kind, s.mount_path, s.mount_methods, s.auth_kind, s.auth_config, \
                s.kind_state, s.kind_state_seq, s.project_id, s.instance_id, s.holds, \
                CASE WHEN s.held_until >= EXTRACT(EPOCH FROM NOW())::BIGINT THEN s.serving END AS serving \
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
    let for_instance = weft_core::instance::InstanceScope::from_columns(r.try_get("project_id")?, r.try_get("instance_id")?)
        .map_err(|e| anyhow::anyhow!("corrupt instance_id: {e}"))?;
    Ok(SignalRowWire {
        token: r.try_get("token")?,
        tenant_id: r.try_get("tenant_id")?,
        for_instance,
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
        holds: r.try_get("holds")?,
        serving: r
            .try_get::<Option<serde_json::Value>, _>("serving")?
            .map(serde_json::from_value::<HeldServing>)
            .transpose()
            .map_err(|e| anyhow::anyhow!("corrupt signal.serving: {e}"))?,
    })
}
