//! A trigger's queue of events waiting to become runs (`parked_fire`): the
//! events a parked trigger keeps, the ones that could not become a run yet
//! (what they read was not ready, the worker was full or out of reach),
//! and the answers waiting for a run that cannot take them now. The
//! install's door and a worker's door both park through [`park`]; the
//! dispatcher drains each trigger's queue head by head once it can take
//! them ([`take_head`]), and a run's owner takes the answers handed to it
//! ([`hand_answer_in`], [`answers_for`]).

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::db::{Client, FromRow, JsonText, Pool, Row};

/// The channel an event parked for a trigger is announced on, with its
/// project's id, and an answer handed to a run a worker drives, with that
/// run (`crate::runs::run_payload`): the dispatcher's drain wakes on it,
/// and the broker pushes the second down the line of the run's worker,
/// which then reads the run's answers.
// SYNC: PARKED_FIRE_CHANNEL <-> 'weft_parked_fire' in parked_fire_notify (GROUP below), weft_broker_client::line::LINE_CHANNELS, crates/weft-broker/src/line.rs (audience)
pub const PARKED_FIRE_CHANNEL: &str = "weft_parked_fire";

/// The events waiting for their trigger, one row each, first in first out
/// per trigger (`seq`): the events a parked trigger keeps, the ones that
/// could not become a run yet, and the answers waiting for a run that
/// cannot take them now (it is parked behind a parked trigger, or another
/// worker drives it). A drain takes only each token's head row, under
/// `FOR UPDATE SKIP LOCKED`, so two drains never hand one event over twice.
pub static GROUP: crate::SchemaGroup = crate::SchemaGroup {
    name: "parked_fire",
    tables: &["parked_fire"],
    ddl: &[
        r#"CREATE TABLE IF NOT EXISTS parked_fire (
            -- The trigger's signal token, or the token a waiting run's
            -- answer resolves.
            token TEXT NOT NULL,
            seq BIGINT GENERATED ALWAYS AS IDENTITY,
            -- The event's identity: the run it starts is
            -- `weft_core::door_fire::run_of_fire(fire_id)`, so an event
            -- handed over twice is born once.
            fire_id UUID NOT NULL UNIQUE,
            -- What the trigger wakes with: every event waits here already
            -- processed by its kind (`/process`), as its JSON text
            -- (`db::JsonText`): it must come back exactly.
            payload TEXT NOT NULL,
            -- Who sent it, for a trigger somebody outside calls.
            caller TEXT,
            -- How many times it waited for room to run, and the moment
            -- it may be tried again (`park_backoff_secs`).
            attempts INTEGER NOT NULL DEFAULT 0,
            not_before BIGINT NOT NULL,
            -- Set while it waits on something no clock brings
            -- (`WaitsFor`, `<kind>: <reason>`): a value its instance has
            -- not given, infra the run reads, connections the install has
            -- not picked, or a hand-over that kept failing. Not retried on
            -- a timer: what it waits on hands it over.
            waits_for TEXT,
            -- How many hand-overs in a row failed (`Turn::next`).
            failures INTEGER NOT NULL DEFAULT 0,
            -- An answer to a waiting run, rather than an event of an entry.
            is_resume BOOLEAN NOT NULL,
            -- An answer that is a person skipping the wait (its `/skip`
            -- door), not a value: `payload` is then null and unread.
            skipped BOOLEAN NOT NULL DEFAULT FALSE,
            -- Set on an answer already taken off its wait (the wait's
            -- signal is gone) for a run a worker drives: the run it is
            -- for. Its worker takes it; if the worker lets go of the run
            -- first, the letting go writes it into the run's record
            -- (`weft_journal::record::resolve_handed_in`).
            run_id UUID,
            PRIMARY KEY (token, seq)
        )"#,
        r#"CREATE INDEX IF NOT EXISTS parked_fire_handed ON parked_fire (run_id) WHERE run_id IS NOT NULL"#,
        // Wake the drain when an event is parked, so a trigger that is
        // already back takes it at once instead of on the drain's next look,
        // and a run's worker when an answer is handed to it. Through the
        // outbox (`crate::announce`): these are writes on a run's path, and
        // their writers poke the flusher once they commit.
        // An event names its trigger's project; an answer handed to a run
        // names the run, the way every announcement does
        // (`crate::runs::run_payload`), so whoever holds the run hears it is
        // for them, and it is the install's news too (`crate::announce`), for
        // the copy holding the line of the run's worker.
        // SYNC: 'weft_parked_fire' <-> PARKED_FIRE_CHANNEL
        // SYNC: the handed answer's payload <-> crate::runs::run_payload
        // SYNC: 'answer' <-> crate::announce::kind_named
        r#"CREATE OR REPLACE FUNCTION parked_fire_notify() RETURNS trigger AS $$
            DECLARE
                v_project UUID;
            BEGIN
                IF NEW.run_id IS NULL THEN
                    PERFORM weft_announce('weft_parked_fire', (SELECT s.project_id FROM signal s WHERE s.token = NEW.token)::text);
                ELSE
                    SELECT r.project_id INTO v_project FROM run r WHERE r.run_id = NEW.run_id;
                    PERFORM weft_announce('weft_parked_fire', v_project::text || ' ' || NEW.run_id::text);
                    PERFORM weft_tell(v_project, NEW.run_id, 'answer');
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql"#,
        r#"DROP TRIGGER IF EXISTS parked_fire_notify_on_insert ON parked_fire"#,
        r#"CREATE TRIGGER parked_fire_notify_on_insert
            AFTER INSERT ON parked_fire
            FOR EACH ROW
            EXECUTE FUNCTION parked_fire_notify()"#,
        // A signal going takes the events waiting for it along, whoever
        // removes it (a wipe, an answer, a project going): nothing could
        // hand them over any more. An answer already handed to its run
        // stays: its run's worker takes it.
        r#"CREATE OR REPLACE FUNCTION parked_fire_drop_with_signal() RETURNS trigger AS $$
            BEGIN
                DELETE FROM parked_fire WHERE token = OLD.token AND run_id IS NULL;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql"#,
        r#"DROP TRIGGER IF EXISTS parked_fire_drop_on_signal_delete ON signal"#,
        r#"CREATE TRIGGER parked_fire_drop_on_signal_delete
            AFTER DELETE ON signal
            FOR EACH ROW
            EXECUTE FUNCTION parked_fire_drop_with_signal()"#,
    ],
    seed: &[],
    data_steps: &[],
};

pub use weft_core::door_fire::{UnclockedWait, WaitsOn};

/// What a queued event waits on that no clock brings, as
/// `parked_fire.waits_for` holds it (`<kind>: <reason>`): a value its
/// instance has not given, infra the run reads, connections the install
/// has not picked, or a hand-over that kept failing ([`UnclockedWait`]).
/// Shown per trigger by `weft status` and `ctx.instances().list()`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "String", try_from = "String")]
pub struct WaitsFor {
    pub on: UnclockedWait,
    /// Why, as whoever parked it said (an instance's refusal names each
    /// field; a failure is its error).
    pub reason: String,
}

impl WaitsFor {
    /// What it waits on, for the kinds a clock does not bring.
    pub fn new(on: WaitsOn, reason: &str) -> Option<Self> {
        on.unclocked().map(|on| WaitsFor { on, reason: reason.to_string() })
    }

    /// Read `parked_fire.waits_for`.
    pub fn parse(text: &str) -> anyhow::Result<Self> {
        let (kind, reason) = text.split_once(": ").ok_or_else(|| anyhow::anyhow!("'{text}' names no kind"))?;
        WaitsOn::parse(kind)
            .and_then(|on| Self::new(on, reason))
            .ok_or_else(|| anyhow::anyhow!("'{kind}' is nothing a queued event waits on without a clock"))
    }
}

impl From<WaitsFor> for String {
    fn from(waits: WaitsFor) -> String {
        format!("{}: {}", waits.on.as_str(), waits.reason)
    }
}

impl TryFrom<String> for WaitsFor {
    type Error = anyhow::Error;
    fn try_from(text: String) -> anyhow::Result<Self> {
        Self::parse(&text)
    }
}

/// Seconds after which a hand-over that failed is tried again, in a row:
/// past the last, the event stops ([`WaitsOn::Failure`]).
const FAILURE_RETRIES_SECS: [i64; 2] = [5, 20];

/// When, and on what, a queued event is tried again: the columns a park
/// writes and a drain stamps on a head it could not hand over.
#[derive(Debug, Clone, PartialEq)]
pub struct Turn {
    pub attempts: u32,
    pub not_before: i64,
    pub waits_for: Option<WaitsFor>,
    pub failures: u32,
}

impl Turn {
    /// The turn of an event that waited for room `attempts` times and
    /// failed `failures` times in a row before, and now waits on `on`
    /// for `reason`, at `now` (unix seconds):
    ///   - room or its trigger: after its backoff, which grows with each
    ///     wait (the sweep leaves a parked trigger's queue alone, and the
    ///     activation that brings it back hands it over whatever its
    ///     clock says);
    ///   - its instance, infra or picks: on that, never on a clock;
    ///   - a failure: [`FAILURE_RETRIES_SECS`] later, and past the last
    ///     it stops until something that may fix it hands it over.
    pub fn next(attempts: u32, failures: u32, on: WaitsOn, reason: &str, now: i64) -> Self {
        match on {
            WaitsOn::Room | WaitsOn::Trigger => {
                let attempts = attempts.saturating_add(1);
                Turn { attempts, not_before: now + park_backoff_secs(attempts), waits_for: None, failures: 0 }
            }
            WaitsOn::Instance | WaitsOn::Infra | WaitsOn::Picks => Turn { attempts, not_before: now, waits_for: WaitsFor::new(on, reason), failures: 0 },
            WaitsOn::Failure => {
                let failures = failures.saturating_add(1);
                match usize::try_from(failures - 1).ok().and_then(|tried| FAILURE_RETRIES_SECS.get(tried)) {
                    Some(after) => Turn { attempts, not_before: now + after, waits_for: None, failures },
                    None => Turn { attempts, not_before: now, waits_for: WaitsFor::new(on, reason), failures },
                }
            }
        }
    }
}

/// An event as it waits in its trigger's queue: what [`park`] writes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Waiting {
    /// The event's identity: the run it starts is
    /// `weft_core::door_fire::run_of_fire(fire_id)`, so an event handed
    /// over again (a drain retried after a crash, a park that raced its
    /// own start) is born once.
    pub fire_id: uuid::Uuid,
    /// What the trigger wakes with, already processed by its kind.
    pub payload: Value,
    /// Who sent it, for a trigger somebody outside calls
    /// (`weft_core::door_fire::DoorFire::caller`): what its per-caller
    /// limit counts when it is handed over.
    pub caller: Option<String>,
    /// How many times it waited for room to run. Drives the backoff
    /// ([`park_backoff_secs`]).
    pub attempts: u32,
    /// Not handed over before this instant (unix seconds).
    pub not_before: i64,
    /// Set while it waits on something no clock brings ([`WaitsFor`]).
    pub waits_for: Option<WaitsFor>,
    /// How many hand-overs of it failed in a row.
    pub failures: u32,
    /// An answer (`is_resume`) that is a person skipping the wait, not a
    /// value: `payload` is then null and unread. Always false for an
    /// entry's event.
    pub skipped: bool,
}

impl Waiting {
    /// The answer this is, when it answers a waiting run.
    pub fn answer(&self) -> weft_core::primitive::WaitAnswer {
        stored_answer(self.payload.clone(), self.skipped)
    }
}

/// An answer as a row holds it (`payload`, `skipped`) and back: the one
/// spelling of a skip in this table.
fn answer_row(answer: &weft_core::primitive::WaitAnswer) -> (Value, bool) {
    match answer {
        weft_core::primitive::WaitAnswer::Given { value } => (value.clone(), false),
        weft_core::primitive::WaitAnswer::Skipped => (Value::Null, true),
    }
}

/// A stored answer as the run takes it: a skip, or the value given.
pub fn stored_answer(payload: Value, skipped: bool) -> weft_core::primitive::WaitAnswer {
    match skipped {
        true => weft_core::primitive::WaitAnswer::Skipped,
        false => weft_core::primitive::WaitAnswer::Given { value: payload },
    }
}

/// Seconds an event waits before its `attempts`-th try for room: 1, 2,
/// 4, ... doubling, capped at five minutes. Room comes back as the
/// program's runs end or a limit's window does, so the first try is
/// almost at once; one that keeps finding none settles at the cap, and
/// the dispatcher's parked-fire sweep picks it up when due.
pub fn park_backoff_secs(attempts: u32) -> i64 {
    const CAP_SECS: i64 = 300;
    if attempts == 0 {
        return 0;
    }
    1i64.checked_shl(attempts - 1).unwrap_or(CAP_SECS).min(CAP_SECS)
}

/// How many events one entry's queue holds at most. Entry events
/// accumulate while a trigger is parked; an unbounded queue lets an
/// external caller who knows a public path grow it without limit, so a
/// flood is refused loudly (`QueueFull`). A run's wait holds one answer.
pub const MAX_PARKED_ENTRY_FIRES: i64 = 1000;

fn now_unix() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_secs() as i64)
}

/// An event that could not become a run now, as it waits: it waited for
/// room `attempts` times before, and now waits on `on` for `reason`
/// ([`Turn::next`]).
pub fn waiting(fire_id: uuid::Uuid, payload: Value, caller: Option<String>, attempts: u32, on: WaitsOn, reason: &str) -> Waiting {
    let Turn { attempts, not_before, waits_for, failures } = Turn::next(attempts, 0, on, reason, now_unix());
    Waiting { fire_id, payload, caller, attempts, not_before, waits_for, failures, skipped: false }
}

/// An answer to a waiting run, as it waits until the run can take it: a
/// value, or a person's skip. Handed over as soon as its run's trigger
/// takes work.
pub fn waiting_answer(fire_id: uuid::Uuid, answer: weft_core::primitive::WaitAnswer) -> Waiting {
    let (payload, skipped) = answer_row(&answer);
    Waiting { fire_id, payload, caller: None, attempts: 0, not_before: now_unix(), waits_for: None, failures: 0, skipped }
}

/// THE one park. Every park site (an answer whose run cannot take it now, a
/// worker's door, the install's fire path when the worker is out of reach)
/// goes through here, under the trigger's signal row lock, so the guards
/// hold together:
///   - a waiting run's answer is kept only while none waits for it already
///     (one answer resolves one wait); an entry's events while its queue is
///     under [`MAX_PARKED_ENTRY_FIRES`];
///   - an event a held connection picked up (`held_by`) is kept only while
///     its holder still holds the signal; once kept it is the trigger's;
///   - an event already waiting (its `fire_id`) is not kept twice.
pub async fn park(pool: &Pool, token: &str, waiting: &Waiting, held_by: Option<&str>) -> anyhow::Result<ParkAppend> {
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    let signal: Option<(bool, bool)> = tx
        .opt(
            "SELECT is_resume, ($2::text IS NULL OR (holds AND held_by = $2)) FROM signal WHERE token = $1 FOR UPDATE",
            &[&token, &held_by],
        )
        .await?;
    let Some((is_resume, held)) = signal else { return Ok(ParkAppend::Refused(ParkRefusal::RowGone)) };
    if !held {
        return Ok(ParkAppend::Refused(ParkRefusal::NotHeld));
    }
    let (queued, already): (i64, bool) = tx
        .one(
            "SELECT count(*), COALESCE(bool_or(fire_id = $2), FALSE) FROM parked_fire WHERE token = $1",
            &[&token, &waiting.fire_id],
        )
        .await?;
    if already {
        return Ok(ParkAppend::Refused(ParkRefusal::AlreadyQueued));
    }
    if is_resume && queued > 0 {
        return Ok(ParkAppend::Refused(ParkRefusal::ResumeAlreadyAnswered));
    }
    if !is_resume && queued >= MAX_PARKED_ENTRY_FIRES {
        return Ok(ParkAppend::Refused(ParkRefusal::QueueFull));
    }
    let inserted = tx
        .execute(
            "INSERT INTO parked_fire (token, fire_id, payload, caller, attempts, not_before, waits_for, failures, is_resume, skipped) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT (fire_id) DO NOTHING",
            &[
                &token,
                &waiting.fire_id,
                &JsonText(&waiting.payload),
                &waiting.caller,
                &count(waiting.attempts),
                &waiting.not_before,
                &waiting.waits_for.clone().map(String::from),
                &count(waiting.failures),
                &is_resume,
                &waiting.skipped,
            ],
        )
        .await?;
    tx.commit().await?;
    crate::announce::committed(pool);
    // The id is unique across tokens: an event parked under another token
    // already (it cannot be) would read as queued.
    Ok(if inserted == 0 { ParkAppend::Refused(ParkRefusal::AlreadyQueued) } else { ParkAppend::Parked })
}

/// Outcome of [`park`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParkAppend {
    Parked,
    Refused(ParkRefusal),
}

/// Why [`park`] refused, one per guard plus the signal being gone. Each is a
/// different fact for the caller: a retry that finds its event already
/// waiting has lost nothing; a cap has refused a NEW event, which is a loss
/// the caller must say out loud; a vanished signal means the trigger or
/// the wait is gone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParkRefusal {
    /// The event (its `fire_id`) is already waiting.
    AlreadyQueued,
    /// A run's wait whose one answer is already waiting.
    ResumeAlreadyAnswered,
    /// An entry's queue at [`MAX_PARKED_ENTRY_FIRES`].
    QueueFull,
    /// No signal for this token.
    RowGone,
    /// The holder it was picked up under (a held connection's) no longer
    /// holds the signal: another holder serves its events.
    NotHeld,
}

/// The head of one trigger's queue, as a drain took it.
#[derive(Debug, Clone, PartialEq)]
pub struct Head {
    pub token: String,
    pub seq: i64,
    pub waiting: Waiting,
    pub is_resume: bool,
}

/// A count as its `INTEGER` column holds it.
fn count(n: u32) -> i32 {
    i32::try_from(n).unwrap_or(i32::MAX)
}

/// The columns a head is read with.
const HEAD_COLUMNS: &str = "token, seq, fire_id, payload, caller, attempts, not_before, waits_for, failures, is_resume, skipped";

/// Take the head of `token`'s queue on the caller's transaction, locked
/// for it (`FOR UPDATE SKIP LOCKED`: a head another drain holds is
/// skipped, and the row behind it is no head while it waits). `None` when
/// the queue is empty, its head is locked, or its head is not due at `now`
/// (backing off, or waiting on something no clock brings): one trigger's
/// events never overtake each other.
pub async fn take_head(tx: &impl Client, token: &str, now: i64) -> anyhow::Result<Option<Head>> {
    let row: Option<HeadRow> = tx
        .opt(
            &format!(
                "SELECT {HEAD_COLUMNS} FROM parked_fire p \
                 WHERE p.token = $1 AND p.seq = (SELECT min(q.seq) FROM parked_fire q WHERE q.token = $1) \
                   AND p.not_before <= $2 AND p.waits_for IS NULL \
                 FOR UPDATE SKIP LOCKED"
            ),
            &[&token, &now],
        )
        .await?;
    row.map(HeadRow::into_head).transpose()
}

/// The head of `token`'s queue, whatever its turn says: the same as
/// [`take_head`] for what something that happened hands over again (an
/// activation, an instance's values changing, infra coming back).
pub async fn take_head_now(tx: &impl Client, token: &str) -> anyhow::Result<Option<Head>> {
    let row: Option<HeadRow> = tx
        .opt(
            &format!(
                "SELECT {HEAD_COLUMNS} FROM parked_fire p \
                 WHERE p.token = $1 AND p.seq = (SELECT min(q.seq) FROM parked_fire q WHERE q.token = $1) \
                 FOR UPDATE SKIP LOCKED"
            ),
            &[&token],
        )
        .await?;
    row.map(HeadRow::into_head).transpose()
}

/// A head handed over: it goes.
pub async fn remove_in(tx: &impl Client, head: &Head) -> anyhow::Result<()> {
    tx.execute("DELETE FROM parked_fire WHERE token = $1 AND seq = $2", &[&head.token, &head.seq]).await?;
    Ok(())
}

/// A head that could not be handed over: it stays the head, tried again
/// as `turn` says.
pub async fn restamp_in(tx: &impl Client, head: &Head, turn: &Turn) -> anyhow::Result<()> {
    tx.execute(
        "UPDATE parked_fire SET attempts = $3, not_before = $4, waits_for = $5, failures = $6 WHERE token = $1 AND seq = $2",
        &[
            &head.token,
            &head.seq,
            &count(turn.attempts),
            &turn.not_before,
            &turn.waits_for.clone().map(String::from),
            &count(turn.failures),
        ],
    )
    .await?;
    Ok(())
}

/// Hand `answer`, the answer to the wait `token` of run `run_id`,
/// which a worker drives, to that worker, on the caller's transaction:
/// nobody but a run's worker writes the record of a run it drives. The
/// wait's signal is consumed by the caller in the same transaction, so it
/// is answered once.
pub async fn hand_answer_in(tx: &impl Client, token: &str, run_id: weft_core::RunId, answer: &weft_core::primitive::WaitAnswer) -> anyhow::Result<()> {
    let (value, skipped) = answer_row(answer);
    tx.execute(
        "INSERT INTO parked_fire (token, fire_id, payload, attempts, not_before, is_resume, run_id, skipped) \
         VALUES ($1, $2, $3, 0, 0, TRUE, $4, $5)",
        &[&token, &uuid::Uuid::new_v4(), &JsonText(&value), &run_id, &skipped],
    )
    .await?;
    Ok(())
}

/// THE answers handed to runs ([`hand_answer_in`]), as a SQL `SELECT` of
/// `(run_id, token, payload, skipped, seq)` for the runs the SQL
/// expression `runs` names (an id, or a subquery of ids). Read oldest first
/// (`ORDER BY seq`), each answer decoded with [`stored_answer`]. Every
/// reader of a run's answers ([`answers_for`], the broker's recheck of a
/// worker's runs) reads them through here.
pub fn handed_answers_sql(runs: &str) -> String {
    format!("SELECT p.run_id, p.token, p.payload, p.skipped, p.seq FROM parked_fire p WHERE p.run_id IN ({runs})")
}

/// The answers handed to run `run_id` ([`handed_answers_sql`]),
/// oldest first: what the worker driving the run takes. They go once the
/// run's record holds them (`weft_record_batch` deletes the answers a
/// batch resolves).
pub async fn answers_for(db: &impl Client, run_id: weft_core::RunId) -> anyhow::Result<Vec<(String, weft_core::primitive::WaitAnswer)>> {
    let rows: Vec<(weft_core::RunId, String, JsonText<Value>, bool, i64)> =
        db.all(&format!("{} ORDER BY seq", handed_answers_sql("$1")), &[&run_id]).await?;
    Ok(rows.into_iter().map(|(_, token, payload, skipped, _)| (token, stored_answer(payload.0, skipped))).collect())
}

struct HeadRow {
    token: String,
    seq: i64,
    fire_id: uuid::Uuid,
    payload: Value,
    caller: Option<String>,
    attempts: i32,
    not_before: i64,
    waits_for: Option<String>,
    failures: i32,
    is_resume: bool,
    skipped: bool,
}

impl FromRow for HeadRow {
    fn from_row(row: Row) -> crate::db::Result<Self> {
        Ok(Self {
            token: row.try_get("token")?,
            seq: row.try_get("seq")?,
            fire_id: row.try_get("fire_id")?,
            payload: row.try_get::<_, JsonText<Value>>("payload")?.0,
            caller: row.try_get("caller")?,
            attempts: row.try_get("attempts")?,
            not_before: row.try_get("not_before")?,
            waits_for: row.try_get("waits_for")?,
            failures: row.try_get("failures")?,
            is_resume: row.try_get("is_resume")?,
            skipped: row.try_get("skipped")?,
        })
    }
}

impl HeadRow {
    fn into_head(self) -> anyhow::Result<Head> {
        Ok(Head {
            token: self.token,
            seq: self.seq,
            waiting: Waiting {
                fire_id: self.fire_id,
                payload: self.payload,
                caller: self.caller,
                attempts: self.attempts.max(0).unsigned_abs(),
                not_before: self.not_before,
                waits_for: self.waits_for.as_deref().map(WaitsFor::parse).transpose()?,
                failures: self.failures.max(0).unsigned_abs(),
                skipped: self.skipped,
            },
            is_resume: self.is_resume,
        })
    }
}

#[cfg(test)]
mod park_backoff_tests {
    use super::*;

    /// 1, 2, 4, ... doubling from the first failed route, capped at five
    /// minutes, and never overflowing on an absurd count.
    #[test]
    fn backoff_doubles_from_one_second_and_caps() {
        assert_eq!(park_backoff_secs(0), 0);
        assert_eq!(park_backoff_secs(1), 1);
        assert_eq!(park_backoff_secs(2), 2);
        assert_eq!(park_backoff_secs(5), 16);
        assert_eq!(park_backoff_secs(9), 256);
        assert_eq!(park_backoff_secs(10), 300);
        assert_eq!(park_backoff_secs(40), 300);
        assert_eq!(park_backoff_secs(u32::MAX), 300);
    }

    /// An event waiting for room or its trigger waits its backoff, longer
    /// each time; one waiting on its instance, its infra or its picks is
    /// never due on a clock.
    #[test]
    fn an_event_waits_on_what_parked_it() {
        let now = 1_000;
        let room = Turn::next(3, 0, WaitsOn::Room, "full", now);
        assert_eq!((room.attempts, room.not_before, room.waits_for), (4, now + 8, None));
        let trigger = Turn::next(3, 2, WaitsOn::Trigger, "parked", now);
        assert_eq!(trigger, Turn { attempts: 4, not_before: now + 8, waits_for: None, failures: 0 }, "a failure streak ends");
        for (on, unclocked) in [
            (WaitsOn::Instance, UnclockedWait::Instance),
            (WaitsOn::Infra, UnclockedWait::Infra),
            (WaitsOn::Picks, UnclockedWait::Picks),
        ] {
            let turn = Turn::next(3, 0, on, "why", now);
            assert_eq!(turn.waits_for, Some(WaitsFor { on: unclocked, reason: "why".into() }));
            assert_eq!(turn.attempts, 3, "waiting on an event is no try for room");
        }
    }

    /// A hand-over that fails is tried again after 5 then 20 seconds,
    /// and the third failure in a row stops it until something hands it
    /// over.
    #[test]
    fn a_failing_event_tries_twice_more_then_stops() {
        let now = 1_000;
        let first = Turn::next(0, 0, WaitsOn::Failure, "unreachable", now);
        assert_eq!((first.failures, first.not_before, first.waits_for), (1, now + 5, None));
        let second = Turn::next(0, first.failures, WaitsOn::Failure, "unreachable", now);
        assert_eq!((second.failures, second.not_before, second.waits_for), (2, now + 20, None));
        let third = Turn::next(0, second.failures, WaitsOn::Failure, "unreachable", now);
        assert_eq!(third.waits_for, Some(WaitsFor { on: UnclockedWait::Failure, reason: "unreachable".into() }));
    }

    /// What an event waits on reads back as it was written, and nothing
    /// a clock brings is ever written.
    #[test]
    fn waits_for_reads_back_as_written() {
        let waits = WaitsFor::new(WaitsOn::Instance, "instance 'ada' at 'answer': 'key' is not filled").unwrap();
        let text = String::from(waits.clone());
        assert_eq!(text, "instance: instance 'ada' at 'answer': 'key' is not filled");
        assert_eq!(WaitsFor::parse(&text).unwrap(), waits);
        assert!(WaitsFor::new(WaitsOn::Room, "full").is_none());
        assert!(WaitsFor::parse("room: full").is_err());
        assert!(WaitsFor::parse("no kind").is_err());
    }

    /// A park reads back on the broker as the worker sent it, what it
    /// waits on included.
    #[test]
    fn a_park_reads_back_as_sent() {
        let now = waiting(uuid::Uuid::nil(), Value::Null, None, 0, WaitsOn::Picks, "pick a connection");
        let wire = serde_json::to_value(&now).unwrap();
        assert_eq!(wire["waits_for"], serde_json::json!("picks: pick a connection"));
        assert_eq!(serde_json::from_value::<Waiting>(wire).unwrap(), now);
    }
}
