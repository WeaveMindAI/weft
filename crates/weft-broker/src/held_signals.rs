//! What the listener holds, the claims holders take on held signals, and
//! the writes the listener makes to a signal row.
//!
//! The queries behind `signal/list_held`, `signal/get_held`,
//! `signal/write_kind_state`, `signal/hold`, `signal/let_go`,
//! `signal/holder_load`, `signal/set_holds`, `signal/give_up` and the holders' check
//! (`/held/check`), kept out of the handlers so a database test can drive
//! the exact statements the listener depends on. A holder's claim has no
//! expiry: it lasts until the holder lets it go, or until the check the
//! holder keeps pushing ahead of itself fires because it stopped (it
//! died), which frees it ([`release`]).

use weft_task_store::db::{Client, JsonText, Pool, Row};
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
/// taken. A look that takes something marks the load the holder last
/// said ([`report_load`]) as behind what it holds: it measured less than
/// it holds now, and the sizing takes no copy away until it says it
/// again. A look also writes the room it is taken with onto the holder's
/// load row when it differs from the one there, so the room the sizing
/// counts is the room the holder runs with even when the answer to a
/// report was lost on its way back. The answer names the load weft then
/// keeps for it, when that is measured on what it holds, and the most
/// room a spare up has ([`spares_sql`]), which a holder over the target
/// hands work to ([`report_load`]). Its own claims it did not say it keeps
/// are always taken back, whatever its room: each is its responsibility
/// (one it stopped for a hand-over the broker refused, one handed to it as
/// a spare while it took free ones, a row registered again under it), and
/// one left untaken would stay its claim and never come up. Its room
/// bounds only what it takes of the free ones, the `want` ones first, then
/// the oldest. A look that says nothing new and finds nothing to take
/// writes nothing. In one transaction, and the take skips rows a
/// sibling is taking, so two holders never take one signal.
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
    // Every row this holder claims, locked in one pass in token order, the
    // order a release takes them in too: what it says it holds and still
    // claims (kept), and what it claims without saying so (its own, taken
    // back below whatever its room, but never one given up on).
    let claimed: Vec<(String, bool, bool)> = tx
        .all(
            &format!(
                "SELECT r.token, r.token = ANY($1), r.held_failed IS NULL FROM signal r \
                 WHERE r.holds AND r.held_by = $2 AND {held} \
                 ORDER BY r.token \
                 FOR UPDATE OF r",
                held = activation_held("r"),
            ),
            &[&tokens, &replica],
        )
        .await?;
    let kept: Vec<String> = claimed.iter().filter(|(_, listed, _)| *listed).map(|(token, ..)| token.clone()).collect();
    let own: Vec<String> = claimed.iter().filter(|(_, listed, open)| !listed && *open).map(|(token, ..)| token.clone()).collect();
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
    let full = room.map(i64::from);
    let room = full.map(|r| r.saturating_sub(kept.len() as i64).max(0));
    // The take, the load row it brings up to date, the load left and
    // whether a spare is up, in one statement: the rows read are the ones
    // from before the statement, the load row left behind here when the
    // take took something, and the looker never its own spare.
    let (taken, load, spare): (Vec<String>, Option<i32>, Option<i64>) = tx
        .one(
            &format!(
                "WITH own AS (SELECT unnest($1::text[]) AS token), free AS ( \
                     SELECT r.token FROM signal r \
                     WHERE r.holds AND r.held_failed IS NULL AND r.held_by IS NULL AND {held} \
                     ORDER BY (r.token = ANY($2)) DESC, r.created_at \
                     LIMIT CASE WHEN $3::bigint IS NULL THEN NULL ELSE greatest($3 - cardinality($1::text[]), 0) END \
                     FOR UPDATE OF r SKIP LOCKED \
                 ), took AS ( \
                     UPDATE signal s SET held_by = $4, serving = NULL \
                     FROM (SELECT token FROM own UNION ALL SELECT token FROM free) t WHERE s.token = t.token \
                     RETURNING s.token \
                 ), noted AS ( \
                     UPDATE holder_load \
                     SET settled = settled AND NOT EXISTS (SELECT 1 FROM took), \
                         room = coalesce($5, room) \
                     WHERE replica = $4 \
                       AND ((settled AND EXISTS (SELECT 1 FROM took)) OR room IS DISTINCT FROM coalesce($5, room)) \
                 ) \
                 SELECT ARRAY(SELECT token FROM took), \
                        (SELECT cpu_percent FROM holder_load WHERE replica = $4 AND settled AND NOT EXISTS (SELECT 1 FROM took)), \
                        (SELECT max(l.room) FROM {spares})",
                held = activation_held("r"),
                spares = spares_sql("$4", "1"),
            ),
            &[&own, &want, &room, &replica, &full],
        )
        .await?;
    let rows = tx.query(&format!("{} WHERE s.token = ANY($1)", held_select()), &[&taken]).await?;
    tx.commit().await?;
    let taken = rows.into_iter().map(decode).collect::<anyhow::Result<Vec<_>>>().map_err(|e| anyhow::anyhow!("signal row decode: {e}"))?;
    let load = load.map(|percent| u16::try_from(percent).map_err(|_| anyhow::anyhow!("holder_load.cpu_percent {percent} is out of range"))).transpose()?;
    let spare = spare.map(|room| u32::try_from(room).map_err(|_| anyhow::anyhow!("holder_load.room {room} is out of range"))).transpose()?;
    Ok(SignalHoldResponse { kept, ended, taken, load, spare })
}

/// SQL: the spares a holder named `looker` could hand `handing` signals
/// to, as rows of `holder_load l` from `FROM` on: the other holders that
/// said their load and claim no held signal, copies the sizing added for
/// the load and still waiting for work, with room for all of them (a
/// spare takes what is handed to it within its room, and what it could not
/// take would stay its claim, never brought up). A holder that has said
/// nothing yet is no spare: it may still be starting.
fn spares_sql(looker: &str, handing: &str) -> String {
    format!(
        "holder_load l \
         WHERE l.replica <> {looker} AND l.room >= {handing} \
           AND NOT EXISTS (SELECT 1 FROM signal c WHERE c.holds AND c.held_by = l.replica AND {held})",
        held = activation_held("c"),
    )
}

/// Where a holder's hand-over goes ([`report_load`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandTo {
    /// To a spare, or nowhere: the holder is only over the target, and
    /// moving its work is worth a reconnect only when a spare takes it.
    Spare,
    /// To a spare when one is up, else freed for any holder to take: the
    /// holder is over the high mark, and sheds its work either way.
    SpareOrAny,
}

/// What became of a load said ([`report_load`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reported {
    /// Written, with how many signals it handed over and to which spare
    /// (`None`: freed, or nothing handed over).
    Said { handed: u64, to: Option<String> },
    /// Nothing written: it hands signals over but claims none of them any
    /// more (its check went off while it was alive, or every one moved
    /// on), and a load said for work it no longer holds would size the
    /// pool on nothing.
    NothingHeld,
    /// Nothing written: it hands over only to a spare, and none is free
    /// (another holder handed one work first).
    NoSpare,
}

/// Holder `replica` says the CPU it uses (`cpu_percent`, of what it
/// keeps): hand `let_go` over as `to` says (to one spare, chosen and
/// locked here so two holders never hand the same spare work, the spare
/// marked behind what it holds until it says its load; or freed), then
/// write the load and the room it takes from now on
/// ([`weft_broker_client::protocol::SignalHolderLoadRequest`]), in one
/// transaction. A load said with nothing handed over is measured on what
/// the holder holds (`settled`); one said handing signals over is behind
/// it, until the holder says it again. A report that repeats the row
/// writes nothing, so it wakes nobody; a new one wakes the holder sizing
/// (`holder_load`'s triggers), and each signal handed over is announced as
/// any claim leaving its holder is (`signal_held_on_release`), so the spare
/// looks and brings it up.
pub async fn report_load(
    pool: &Pool,
    replica: &str,
    cpu_percent: u16,
    room: u32,
    let_go: &[String],
    to: HandTo,
) -> anyhow::Result<Reported> {
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    let (handed, spare) = if let_go.is_empty() {
        (0, None)
    } else {
        let handing = i64::try_from(let_go.len())?;
        let spares = spares_sql("$1", "$2");
        let spare: Option<String> = tx
            .scalar_opt(&format!("SELECT l.replica FROM {spares} ORDER BY l.room DESC, l.replica LIMIT 1 FOR UPDATE OF l SKIP LOCKED"), &[&replica, &handing])
            .await?;
        // A holder that handed this spare work and committed while this
        // statement ran left the row unlocked, so it was locked here, but
        // judged "claims nothing" on what this statement saw when it began:
        // a statement of its own, which sees that hand-over, judges again.
        let spare = match spare {
            Some(spare) => {
                let still: bool =
                    tx.scalar(&format!("SELECT EXISTS (SELECT 1 FROM {spares} AND l.replica = $3)"), &[&replica, &handing, &spare]).await?;
                still.then_some(spare)
            }
            None => None,
        };
        if spare.is_none() && to == HandTo::Spare {
            return Ok(Reported::NoSpare);
        }
        let handed = release_in(&tx, replica, Some(let_go), spare.as_deref()).await?;
        if handed == 0 {
            return Ok(Reported::NothingHeld);
        }
        if let Some(spare) = &spare {
            tx.execute("UPDATE holder_load SET settled = FALSE WHERE replica = $1", &[spare]).await?;
        }
        (handed, spare)
    };
    tx.execute(
        "INSERT INTO holder_load (replica, cpu_percent, settled, room) VALUES ($1, $2, $3, $4) \
         ON CONFLICT (replica) DO UPDATE SET cpu_percent = EXCLUDED.cpu_percent, settled = EXCLUDED.settled, room = EXCLUDED.room \
         WHERE (holder_load.cpu_percent, holder_load.settled, holder_load.room) \
               IS DISTINCT FROM (EXCLUDED.cpu_percent, EXCLUDED.settled, EXCLUDED.room)",
        &[&replica, &i32::from(cpu_percent), &let_go.is_empty(), &i64::from(room)],
    )
    .await?;
    tx.commit().await?;
    Ok(Reported::Said { handed, to: spare })
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
/// at its next look, and forget the load it said: the holder letting go
/// as it stops, or its check firing because it stopped pushing it (it
/// died), so a holder gone never keeps the pool sized for it. The row's
/// trigger announces each one freed (`signal_held_on_release`). How many
/// it freed.
pub async fn release(pool: &Pool, replica: &str) -> anyhow::Result<u64> {
    release_in(pool, replica, None, None).await
}

/// Move the claims of `replica` that `only` names (every one when `None`,
/// which also forgets the load it said) to holder `to` (`None`: freed), in
/// one statement on `db`. The rows are locked in token order first, the
/// order every release and hand-over of one holder's claims takes, so a
/// hand-over and a check going off for the same holder wait on each other
/// and never deadlock.
async fn release_in(db: &impl Client, replica: &str, only: Option<&[String]>, to: Option<&str>) -> anyhow::Result<u64> {
    Ok(db
        .execute(
            "WITH locked AS ( \
                 SELECT token FROM signal WHERE held_by = $1 AND ($2::text[] IS NULL OR token = ANY($2)) ORDER BY token FOR UPDATE \
             ), gone AS (DELETE FROM holder_load WHERE replica = $1 AND $2::text[] IS NULL) \
             UPDATE signal s SET held_by = $3, serving = NULL FROM locked WHERE s.token = locked.token",
            &[&replica, &only, &to],
        )
        .await?)
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
