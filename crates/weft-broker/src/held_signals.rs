//! What the listener holds, the claims holders take on held signals, and
//! the writes the listener makes to a signal row.
//!
//! The queries behind `signal/list_held`, `signal/get_held`,
//! `signal/write_kind_state`, `signal/hold`, `signal/let_go`,
//! `signal/set_holds`, `signal/give_up` and the holders' check
//! (`/held/check`), kept out of the handlers so a database test can drive
//! the exact statements the listener depends on. A holder's claim has no
//! expiry: it lasts until the holder lets it go, or until the check the
//! holder keeps pushing ahead of itself fires because it stopped (it
//! died), which frees it ([`release`]).

use weft_task_store::db::{Client as _, JsonText, Pool, Row};
use weft_broker_client::protocol::{
    HeldNow, HeldServing, SignalAuthKind, SignalHoldResponse, SignalRowWire, SignalSurfaceKind, ACTIVATION_LISTENS,
    SIGNAL_ACTIVATION_JOIN, STILL_HELD_BY,
};

/// The signal rows a booting or rehydrating listener must hold: every row
/// whose governing activation listens ([`ACTIVATION_LISTENS`], or that
/// none governs). A trigger taken down refusing with nothing kept has no
/// rows left; one whose door refuses with work kept, or whose window is
/// over, keeps its rows (its next activation starts from them) but no
/// longer listens, so they never come out of this query. Mixed
/// tenants: each row carries its own. `project` narrows it to one
/// project's rows.
pub async fn signals_held(pool: &Pool, project: Option<uuid::Uuid>) -> anyhow::Result<Vec<SignalRowWire>> {
    let rows = pool
        .query(&format!("{} WHERE {ACTIVATION_LISTENS} AND ($1::uuid IS NULL OR s.project_id = $1)", held_select()), &[&project])
        .await?;
    rows.into_iter().map(decode).collect::<anyhow::Result<Vec<_>>>().map_err(|e| anyhow::anyhow!("signal row decode: {e}"))
}

/// One held signal by its token, or `None` when no row with that token is
/// held (gone, or its activation no longer listens): what the listener
/// loads a signal it has not seen yet from.
pub async fn signal_held(pool: &Pool, token: &str) -> anyhow::Result<Option<SignalRowWire>> {
    let row: Option<Row> = pool.opt(&format!("{} WHERE s.token = $1 AND {ACTIVATION_LISTENS}", held_select()), &[&token]).await?;
    row.map(decode).transpose().map_err(|e| anyhow::anyhow!("signal row decode: {e}"))
}

/// Claim a signal kind's moment: write its durable state at version
/// `from_seq + 1`, only while the row is still at `from_seq`. Of two
/// listeners woken for the same moment exactly one wins, and only the
/// winner acts on it; a registration that rewrote the row since the read
/// moved its version, so a claim from before it loses. Returns whether
/// the row was written.
pub async fn write_kind_state(pool: &Pool, token: &str, kind_state: &serde_json::Value, from_seq: i64) -> anyhow::Result<bool> {
    let written = pool
        .execute(
            "UPDATE signal SET kind_state = $2, kind_state_seq = $3 + 1 \
             WHERE token = $1 AND kind_state_seq = $3",
            &[&token, &JsonText(kind_state), &from_seq],
        )
        .await?;
    Ok(written > 0)
}

/// One look of holder `replica`: name which of the `holding` it still
/// holds (writing what each connection says it is doing, only when it
/// said), name the ones of them that ended (their row is gone or no
/// longer held), then take up to `room` more (every one there is when
/// `None`), the `want` ones first, from the held signals nobody claims
/// and the ones it claims but does not say it keeps (it restarted under
/// its name, wants one taken afresh, or the row was registered again
/// under it). A signal its holder gave up on (`held_failed`) is never
/// taken. A look that says nothing new and finds nothing to take writes
/// nothing. In one transaction, and the take skips rows a sibling is
/// taking, so two holders never take one signal.
pub async fn hold(
    pool: &Pool,
    replica: &str,
    holding: &[HeldNow],
    room: Option<u32>,
    want: &[String],
) -> anyhow::Result<SignalHoldResponse> {
    let tokens: Vec<&str> = holding.iter().map(|h| h.token.as_str()).collect();
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    let kept: Vec<String> = tx
        .scalars(
            &format!(
                "SELECT r.token FROM signal r \
                 WHERE r.token = ANY($1) AND r.holds AND r.held_by = $2 AND {held} \
                 FOR UPDATE OF r",
                held = activation_held("r"),
            ),
            &[&tokens, &replica],
        )
        .await?;
    let said: Vec<&HeldNow> = holding.iter().filter(|h| h.serving.is_some() && kept.contains(&h.token)).collect();
    if !said.is_empty() {
        let said_tokens: Vec<&str> = said.iter().map(|h| h.token.as_str()).collect();
        let servings: Vec<JsonText<&HeldServing>> = said.iter().filter_map(|h| h.serving.as_ref()).map(JsonText).collect();
        tx.execute(
            "UPDATE signal r SET serving = x.serving \
             FROM unnest($1::text[], $2::text[]) AS x(token, serving) WHERE r.token = x.token",
            &[&said_tokens, &servings],
        )
        .await?;
    }
    let ended: Vec<String> = tx
        .scalars(&format!("SELECT x.token FROM unnest($1::text[]) AS x(token) WHERE NOT {held}", held = activation_held("x")), &[&tokens])
        .await?;
    let room = room.map(|r| i64::from(r).saturating_sub(kept.len() as i64).max(0));
    let taken: Vec<String> = tx
        .scalars(
            &format!(
                "WITH free AS ( \
                     SELECT r.token FROM signal r \
                     WHERE r.holds AND r.held_failed IS NULL AND (r.held_by IS NULL OR r.held_by = $4) \
                       AND NOT (r.token = ANY($1)) AND {held} \
                     ORDER BY (r.token = ANY($2)) DESC, r.created_at \
                     LIMIT $3 \
                     FOR UPDATE OF r SKIP LOCKED \
                 ) \
                 UPDATE signal s SET held_by = $4, serving = NULL \
                 FROM free WHERE s.token = free.token \
                 RETURNING s.token",
                held = activation_held("r"),
            ),
            &[&kept, &want, &room, &replica],
        )
        .await?;
    let rows = tx.query(&format!("{} WHERE s.token = ANY($1)", held_select()), &[&taken]).await?;
    tx.commit().await?;
    let taken = rows.into_iter().map(decode).collect::<anyhow::Result<Vec<_>>>().map_err(|e| anyhow::anyhow!("signal row decode: {e}"))?;
    Ok(SignalHoldResponse { kept, ended, taken })
}

/// Whether `token`'s row is still held under `replica`'s claim
/// ([`STILL_HELD_BY`]).
pub async fn still_held_by(pool: &Pool, token: &str, replica: &str) -> anyhow::Result<bool> {
    Ok(pool.scalar(STILL_HELD_BY, &[&token, &replica]).await?)
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

/// Whether an event naming the holder `held_by` comes from that holder: a
/// copy speaks only for the claims it holds.
pub fn sent_by_its_holder(sender: Option<&str>, held_by: &str) -> bool {
    sender == Some(held_by)
}

pub async fn judge_held_fire(
    pool: &Pool,
    sender: Option<&str>,
    fire: &weft_task_store::kinds::FireSignalPayload,
) -> anyhow::Result<HeldFire> {
    let Some(held_by) = fire.held_by.as_deref() else { return Ok(HeldFire::Taken) };
    if !sent_by_its_holder(sender, held_by) {
        return Ok(HeldFire::NotItsSender);
    }
    Ok(match still_held_by(pool, &fire.token, held_by).await? {
        true => HeldFire::Taken,
        false => HeldFire::NoLongerHeld,
    })
}

/// Record whether `token` keeps a connection open, as its kind decides
/// now. A row that stops holding lets its holder go with it, and what was
/// given up on it is forgotten, so the decision runs again.
pub async fn set_holds(pool: &Pool, token: &str, holds: bool) -> anyhow::Result<()> {
    pool.execute(
        "UPDATE signal SET holds = $2, \
                held_by = CASE WHEN $2 THEN held_by END, serving = CASE WHEN $2 THEN serving END, \
                held_failed = NULL \
         WHERE token = $1",
        &[&token, &holds],
    )
    .await?;
    Ok(())
}

/// Free every claim holder `replica` holds, so another holder takes them
/// at its next look: the holder letting go as it stops, or its check
/// firing because it stopped pushing it (it died). The row's trigger
/// announces each one freed (`signal_held_on_release`). How many it freed.
pub async fn release(pool: &Pool, replica: &str) -> anyhow::Result<u64> {
    Ok(pool.execute("UPDATE signal SET held_by = NULL, serving = NULL WHERE held_by = $1", &[&replica]).await?)
}

/// Write down that the listener or a holder gave up on `token`'s
/// registration `generation` for good (`reason`, shown on its node), and
/// let it go: only that registration, not given up already, and, for a
/// held one, only by the holder whose claim it is (`caller_replica`); a
/// caller with no replica gives up a held one only while nobody claims it.
/// Whether it was written.
pub async fn give_up(pool: &Pool, token: &str, generation: i64, reason: &str, caller_replica: Option<&str>) -> anyhow::Result<bool> {
    let written = pool
        .execute(
            "UPDATE signal SET held_failed = $3, held_by = NULL, serving = NULL \
             WHERE token = $1 AND generation = $2 AND held_failed IS NULL \
               AND (NOT holds OR held_by IS NOT DISTINCT FROM $4::text)",
            &[&token, &generation, &reason, &caller_replica],
        )
        .await?;
    Ok(written > 0)
}

/// Whether the token `row.token` names a signal row that a listener holds
/// ([`ACTIVATION_LISTENS`]): false when the row is gone or its activation
/// no longer listens. A condition an UPDATE can carry, read through the
/// one [`SIGNAL_ACTIVATION_JOIN`]. That join names its signal `s`, so
/// `row` is any other alias.
fn activation_held(row: &str) -> String {
    format!(
        "EXISTS (SELECT 1 FROM signal s {SIGNAL_ACTIVATION_JOIN} \
                 WHERE s.token = {row}.token AND {ACTIVATION_LISTENS})"
    )
}

/// Every column a held-signal read decodes, with the governing
/// activation joined as `a`. A held signal's serving shows only while a
/// holder claims it: a freed claim's last word is not the truth.
fn held_select() -> String {
    format!(
        "SELECT s.token, s.tenant_id, s.node_id, s.spec_json, s.spec_json_version, s.is_resume, s.run_id, \
                s.surface_kind, s.mount_path, s.mount_methods, s.auth_kind, s.auth_config, \
                s.kind_state, s.kind_state_seq, s.project_id, s.instance_id, s.holds, \
                s.generation, s.held_failed, \
                CASE WHEN s.held_by IS NOT NULL THEN s.serving END AS serving \
         FROM signal s {SIGNAL_ACTIVATION_JOIN}"
    )
}

// try_get on a NOT NULL column should never fail; if it does the row
// shape has drifted from the schema. Surface that loudly rather than
// silently returning empty strings to the listener.
fn decode(r: Row) -> anyhow::Result<SignalRowWire> {
    let surface_str: String = r.try_get("surface_kind")?;
    let surface_kind = SignalSurfaceKind::parse(&surface_str)
        .ok_or_else(|| anyhow::anyhow!("unknown surface_kind '{surface_str}'"))?;
    let auth_str: String = r.try_get("auth_kind")?;
    let auth_kind = SignalAuthKind::parse(&auth_str)
        .ok_or_else(|| anyhow::anyhow!("unknown auth_kind '{auth_str}'"))?;
    let for_instance = weft_core::instance::InstanceScope::from_columns(r.try_get("project_id")?, r.try_get("instance_id")?)
        .map_err(|e| anyhow::anyhow!("corrupt instance_id: {e}"))?;
    let token: String = r.try_get("token")?;
    Ok(SignalRowWire {
        tenant_id: r.try_get("tenant_id")?,
        for_instance,
        node_id: r.try_get("node_id")?,
        // Handed to the listener in today's shape.
        spec_json: weft_core::primitive::SignalSpec::STORED
            .in_todays_shape(r.try_get("spec_json")?, r.try_get("spec_json_version")?)
            .map_err(|e| e.at(format_args!("signal {token}")))?,
        token,
        is_resume: r.try_get("is_resume")?,
        run_id: r.try_get("run_id")?,
        surface_kind,
        mount_path: r.try_get("mount_path")?,
        mount_methods: r.try_get("mount_methods")?,
        auth_kind,
        auth_config: r.try_get::<_, Option<JsonText<serde_json::Value>>>("auth_config")?.map(|json| json.0),
        kind_state: r.try_get::<_, JsonText<serde_json::Value>>("kind_state")?.0,
        kind_state_seq: r.try_get("kind_state_seq")?,
        holds: r.try_get("holds")?,
        serving: r.try_get::<_, Option<JsonText<HeldServing>>>("serving")?.map(|serving| serving.0),
        generation: r.try_get("generation")?,
        held_failed: r.try_get("held_failed")?,
    })
}
