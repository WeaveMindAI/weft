//! What a run's ending leaves for the dispatcher to do.
//!
//! Most endings need nothing: the batch that writes a run's ending also
//! queues its search entry and its storage sweep. Two kinds of run leave
//! work behind, and keep a flag on their row until it is done, which the
//! `run_end_unhandled` index finds:
//!
//! - `watch_end`: somebody waits on the run's ending. A trigger setup's
//!   ending is baked here (`crate::store::TriggerBake`), so an activation
//!   whose request died still gets its bake;
//! - `holds_signals`: the run ended holding resume signals. Their rows go,
//!   and the listener is told to let go of them.
//!
//! The write that ends such a run announces it on
//! `weft_journal::RUN_ENDED_CHANNEL`, which wakes this loop at once. The
//! two jobs are done apart: each is attempted whatever the other did, and
//! a run keeps the flag of each job that failed (only that one), and the
//! loop fails with it: it is tried
//! again shortly a few times, then written down for `weft status` until a
//! later ending wakes the loop (`weft_task_store::drain`).
//!
//! A watched run's ending may also leave a hold with nobody to take it
//! back: an activation whose driver died while its setup run went, a task
//! claim that rested on that run. The `lapses` loop judges those, so it is
//! booked to look.

use weft_core::RunId;
use weft_task_store::db::{self, Client, FromRow, Row};
use weft_task_store::drain::{DrainLoop, DrainStep, WakeOn, SAFETY_POLL_INTERVAL};

use crate::state::DispatcherState;

pub(crate) static WAKE_ON: &[WakeOn] = &[WakeOn::any(weft_journal::RUN_ENDED_CHANNEL)];

/// How many flagged runs one read of them brings.
const PAGE: i64 = 100;

pub fn drain_loop(state: DispatcherState) -> DrainLoop {
    DrainLoop::new("run_ends", "finish what runs' endings leave to do", WAKE_ON, SAFETY_POLL_INTERVAL, move || {
        let state = state.clone();
        async move { handle_endings(&state).await }
    })
}

/// Handle every ended run still flagged, one at a time, read in pages by
/// id: each in a transaction of its own that holds that run's row alone
/// (so two dispatchers never take the same run, and a crash midway leaves
/// it flagged for the next look), its flags cleared once its work is done.
/// A run whose job fails keeps that job's flag and is said in the log (the
/// flag of a job of it that was done goes all the same), the runs after it
/// are handled all the same, and the drain fails naming how many.
async fn handle_endings(state: &DispatcherState) -> anyhow::Result<DrainStep> {
    let mut after: Option<RunId> = None;
    let mut failed = 0usize;
    let mut watched = false;
    // The earliest erase time of the runs unflagged.
    let mut erased_at: Option<i64> = None;
    loop {
        let page: Vec<RunId> = state
            .pg_pool
            .scalars(
                "SELECT run_id FROM run WHERE state = 'ended' AND (watch_end OR holds_signals) \
                   AND ($1::uuid IS NULL OR run_id > $1) ORDER BY run_id LIMIT $2",
                &[&after, &PAGE],
            )
            .await?;
        for run_id in &page {
            let mut conn = state.pg_pool.get().await?;
            let tx = conn.transaction().await?;
            let Some(ending) = take_flagged(&tx, *run_id).await? else { continue };
            let handled = handle_ending(state, &ending).await;
            if handled.done != Jobs::default() {
                let kept = clear_flags(&tx, ending.run_id, handled.done).await?;
                tx.commit().await?;
                erased_at = kept.into_iter().chain(erased_at).min();
                watched |= handled.done.watch_end;
            }
            if !handled.failed.is_empty() {
                failed += 1;
            }
            for e in &handled.failed {
                tracing::error!(
                    target: "weft_dispatcher::run_ends",
                    run_id = %ending.run_id,
                    error = %format!("{e:#}"),
                    "what a run's ending left could not be done; the run stays flagged for it"
                );
            }
        }
        if (page.len() as i64) < PAGE {
            break;
        }
        after = page.last().copied();
    }
    if let Some(at) = erased_at {
        crate::retention::book_at(state, at).await?;
    }
    if watched {
        crate::lapses::book_after(state, 0).await?;
    }
    anyhow::ensure!(failed == 0, "what {failed} runs' endings left could not be done");
    Ok(DrainStep::Done)
}

/// What `ending` left: its trigger setup's bake, its resume signals taken
/// down and let go of by the listener, each attempted whatever the other
/// did.
async fn handle_ending(state: &DispatcherState, ending: &Flagged) -> Handled {
    let setup = finish_trigger_setup(state, ending);
    let signals = async {
        let removed = state.registrations.signal_remove_for_run_id(ending.run_id).await?;
        state.listener.unregister_many(&removed).await;
        Ok(())
    };
    do_jobs(ending, setup, signals).await
}

/// Do each job `ending` is flagged for (`setup` for `watch_end`, `signals`
/// for `holds_signals`), the second whatever the first did: which are
/// done, and why each other one failed, named.
async fn do_jobs(
    ending: &Flagged,
    setup: impl std::future::Future<Output = anyhow::Result<()>>,
    signals: impl std::future::Future<Output = anyhow::Result<()>>,
) -> Handled {
    let mut handled = Handled { done: Jobs::default(), failed: Vec::new() };
    if ending.watch_end {
        match setup.await {
            Ok(()) => handled.done.watch_end = true,
            Err(e) => handled.failed.push(e.context("bake its trigger setup")),
        }
    }
    if ending.holds_signals {
        match signals.await {
            Ok(()) => handled.done.holds_signals = true,
            Err(e) => handled.failed.push(e.context("take down its resume signals")),
        }
    }
    handled
}

/// The jobs a run's ending leaves, by the flag each keeps on its row.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Jobs {
    pub watch_end: bool,
    pub holds_signals: bool,
}

/// What handling one ending did.
#[derive(Debug)]
struct Handled {
    /// The jobs done, whose flags go.
    done: Jobs,
    /// Why each other job it was flagged for failed.
    failed: Vec<anyhow::Error>,
}

/// An ended run whose ending still has work left (see the module doc).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Flagged {
    pub run_id: RunId,
    pub project_id: uuid::Uuid,
    /// The instance the run ran inside (`None`: none), as its row holds it:
    /// whose triggers a trigger setup baked, read here so a setup whose
    /// record does not read still says which `weft bake` sets them up again.
    pub instance_id: Option<String>,
    pub watch_end: bool,
    pub holds_signals: bool,
}

impl FromRow for Flagged {
    fn from_row(row: Row) -> db::Result<Self> {
        Ok(Self {
            run_id: row.try_get("run_id")?,
            project_id: row.try_get("project_id")?,
            instance_id: row.try_get("instance_id")?,
            watch_end: row.try_get("watch_end")?,
            holds_signals: row.try_get("holds_signals")?,
        })
    }
}

/// The ended run `run_id`, still flagged, locked for the caller's
/// transaction (`FOR UPDATE SKIP LOCKED`: one another dispatcher holds is
/// left to it). Read off the row, so an announcement lost on the way loses
/// nothing.
pub async fn take_flagged(db: &impl Client, run_id: RunId) -> anyhow::Result<Option<Flagged>> {
    Ok(db
        .opt(
            "SELECT run_id, project_id, instance_id, watch_end, holds_signals FROM run \
             WHERE run_id = $1 AND state = 'ended' AND (watch_end OR holds_signals) FOR UPDATE SKIP LOCKED",
            &[&run_id],
        )
        .await?)
}

/// The jobs `done` of run `run_id` are done: their flags go, the flag of a
/// job not done stays. Answers until when the run is kept once no flag is
/// left (`None`: for ever, or a flag is left): retention left it alone
/// while it was flagged, so no look it booked counts it.
pub async fn clear_flags(db: &impl Client, run_id: RunId, done: Jobs) -> anyhow::Result<Option<i64>> {
    Ok(db
        .scalar(
            "UPDATE run SET watch_end = watch_end AND NOT $2, holds_signals = holds_signals AND NOT $3 \
             WHERE run_id = $1 RETURNING CASE WHEN watch_end OR holds_signals THEN NULL ELSE keep_until END",
            &[&run_id, &done.watch_end, &done.holds_signals],
        )
        .await?)
}

/// Bake the trigger setup `ending` from its record, when it is one
/// still waiting for its bake. A capture that cannot be read leaves the
/// last good bake in place and says so on the project's events; a store
/// that fails on the way fails the pass, the setup left waiting for the
/// next one.
async fn finish_trigger_setup(state: &DispatcherState, ending: &Flagged) -> anyhow::Result<()> {
    let (run_id, project_id) = (ending.run_id, ending.project_id);
    if !state.bakes.is_trigger_setup_pending(run_id).await? {
        return Ok(());
    }
    // A trigger setup's record is a few rows, read whole.
    let read = match state.runs.readable_events(run_id).await? {
        Err(reason) => Capture::Unreadable(anyhow::Error::msg(reason)),
        Ok(rows) => {
            let events = rows.into_iter().map(|row| row.event).collect::<Vec<_>>();
            let program = state.run_program_identity(&events).await;
            capture(&events, program)?
        }
    };
    match read {
        Capture::Read(bake) => land_trigger_bake(state, run_id, project_id, bake.as_ref()).await,
        Capture::Unreadable(error) => {
            let landed = land_trigger_bake(state, run_id, project_id, None).await;
            if landed.is_ok() {
                // Whose triggers the setup baked (its instance, or none
                // for the shared ones) is on its run's row, which reads
                // even when its record does not.
                crate::live_view::publish_unreadable(
                    state,
                    run_id,
                    project_id,
                    weft_core::primitive::CorruptionSite::UndecodableRow,
                    unreadable_capture_reason(&error, ending.instance_id.as_deref()),
                )
                .await;
            }
            landed
        }
    }
}

/// Land trigger setup `run_id` of project `project_id` with its `bake`
/// (`None`: it captured none), and say on the project's events, as for
/// an unreadable capture, when an earlier bake of the same triggers no
/// longer read and this one replaced it: the triggers this setup did not
/// set up lost what it held for them.
pub(crate) async fn land_trigger_bake(
    state: &DispatcherState,
    run_id: RunId,
    project_id: uuid::Uuid,
    bake: Option<&crate::store::TriggerBake>,
) -> anyhow::Result<()> {
    let lost = state.bakes.finish_trigger_setup(run_id, bake).await?;
    if let (Some(lost), Some(bake)) = (lost, bake) {
        crate::live_view::publish_unreadable(
            state,
            run_id,
            project_id,
            weft_core::primitive::CorruptionSite::UndecodableRow,
            replaced_bake_reason(&lost, bake),
        )
        .await;
    }
    Ok(())
}

/// What a person is told when `bake` replaced an earlier bake that no
/// longer read (`lost`): which triggers it baked, that every other one
/// the earlier bake held has none now, and how to bake them again.
fn replaced_bake_reason(lost: &crate::store::UnreadableBake, bake: &crate::store::TriggerBake) -> String {
    let baked: Vec<&str> = if bake.targets.is_empty() {
        bake.captured.keys().map(String::as_str).collect()
    } else {
        bake.targets.iter().map(String::as_str).collect()
    };
    format!(
        "{}. This trigger setup's bake replaced it: {} {} baked from this setup, and any other trigger it held has no bake \
         until {} sets it up again",
        lost.error.trim_end_matches('.'),
        if baked.is_empty() { "no trigger".to_string() } else { baked.join(", ") },
        if baked.len() == 1 { "is" } else { "are" },
        bake_again(bake.instance.as_ref()),
    )
}

/// What a person is told when a trigger setup's capture cannot be read
/// (`error`): the last good bake stays, and how to bake the triggers of
/// the setup's `instance` again, as its run's row holds it (`None`: the
/// shared ones). An instance that does not parse is unreadable too, and
/// said in the same message, naming the row and the value it holds.
fn unreadable_capture_reason(error: &anyhow::Error, instance: Option<&str>) -> String {
    let again = match instance.map(weft_core::instance::InstanceId::new).transpose() {
        Ok(instance) => format!("Run {} again to refresh it.", bake_again(instance.as_ref())),
        Err(e) => format!(
            "Its run's row (run.instance_id) also holds an instance that does not read ({e}), so whose triggers \
             it set up is not known: run `weft bake --instance <the instance>` again to refresh them."
        ),
    };
    format!("cannot save trigger bake: {error:#}; the last good bake is preserved. {again}")
}

/// The command that bakes the triggers of `instance` again (`None`: the
/// shared ones), quoted the way a message names it.
fn bake_again(instance: Option<&weft_core::instance::InstanceId>) -> String {
    match instance {
        Some(instance) => format!("`weft bake --instance {instance}`"),
        None => "`weft bake`".to_string(),
    }
}

/// What a trigger setup's record captured.
#[derive(Debug)]
enum Capture {
    /// Its bake (`None`: the setup did not complete, so there is none).
    Read(Option<crate::store::TriggerBake>),
    /// The record holds no bake that can be read, and never will.
    Unreadable(anyhow::Error),
}

/// What the setup whose record is `events` captured, `program` the
/// lookup of the program it ran. A lookup the store failed (a dropped
/// connection, a pool that timed out) is the `Err`: the capture itself
/// may be fine, so the setup waits for the next pass. Anything else the
/// lookup or the bake
/// refuses (a record with no birth naming its program, a worker its
/// project no longer records) is in the record itself, and reading it
/// again finds the same.
fn capture(
    events: &[weft_journal::RunEvent],
    program: anyhow::Result<weft_core::project::hash::ProgramIdentity>,
) -> anyhow::Result<Capture> {
    let program = match program {
        Ok(program) => program,
        Err(e) if e.downcast_ref::<db::Error>().is_some() => return Err(e),
        Err(e) => return Ok(Capture::Unreadable(e)),
    };
    Ok(match crate::store::TriggerBake::from_events(events, &program) {
        Ok(bake) => Capture::Read(bake),
        Err(e) => Capture::Unreadable(e),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A store that fails while the program is looked up fails the pass,
    /// so the setup stays waiting and nothing says its capture is
    /// unreadable; a record whose program cannot be named is unreadable.
    #[test]
    fn only_the_record_itself_makes_a_capture_unreadable() {
        let dropped = anyhow::Error::from(db::Error::NoConnection { why: db::NoConnection::Busy, refused: None })
            .context("look up the run's program");
        let failed = capture(&[], Err(dropped)).expect_err("a store failure is the pass's");
        assert!(failed.downcast_ref::<db::Error>().is_some(), "{failed:#}");
        let unnamed = anyhow::anyhow!("the run's history does not start with a birth naming its program");
        assert!(matches!(capture(&[], Err(unnamed)), Ok(Capture::Unreadable(_))));
    }

    fn flagged() -> Flagged {
        Flagged {
            run_id: RunId::from_u128(7),
            project_id: uuid::Uuid::nil(),
            instance_id: None,
            watch_end: true,
            holds_signals: true,
        }
    }

    /// A trigger setup whose bake fails (the setup left pending) does not
    /// hold up taking down the run's resume signals: both are attempted,
    /// the signals' job is done, and only the setup's failure is named.
    #[tokio::test]
    async fn the_signals_go_while_the_setup_is_pending() {
        let ending = flagged();
        let signals_ran = std::cell::Cell::new(false);
        let handled = do_jobs(&ending, async { Err(anyhow::anyhow!("the store went away")) }, async {
            signals_ran.set(true);
            Ok(())
        })
        .await;
        assert!(signals_ran.get(), "the signals' job ran");
        assert_eq!(handled.done, Jobs { watch_end: false, holds_signals: true });
        assert_eq!(handled.failed.len(), 1);
        assert_eq!(format!("{:#}", handled.failed[0]), "bake its trigger setup: the store went away");

        // A job the run is not flagged for is not attempted.
        let only_setup = Flagged { holds_signals: false, ..ending };
        let handled = do_jobs(&only_setup, async { Ok(()) }, async { panic!("not flagged for its signals") }).await;
        assert_eq!(handled.done, Jobs { watch_end: true, holds_signals: false });
        assert!(handled.failed.is_empty());
    }

    /// A bake that replaced an earlier one which no longer read says why,
    /// which triggers it baked, and the command that bakes the rest again
    /// for the triggers' owner.
    #[test]
    fn a_replaced_bake_names_what_it_baked_and_how_to_bake_the_rest() {
        let lost = crate::store::UnreadableBake { program_hash: "p".into(), error: "the bake of the shared triggers: no".into() };
        let mut bake = crate::store::TriggerBake {
            project_id: uuid::Uuid::nil(),
            instance: None,
            source_version: "source".into(),
            program: weft_core::project::hash::ProgramIdentity {
                definition_hash: "graph".into(),
                binary_hash: "binary".into(),
                implementations: Default::default(),
            },
            run_id: uuid::Uuid::nil(),
            captured: Default::default(),
            targets: ["door".to_string(), "feed".to_string()].into(),
            at_unix: 1,
        };
        assert_eq!(
            replaced_bake_reason(&lost, &bake),
            "the bake of the shared triggers: no. This trigger setup's bake replaced it: door, feed are baked from \
             this setup, and any other trigger it held has no bake until `weft bake` sets it up again",
        );
        bake.instance = Some(weft_core::instance::InstanceId::new("ada").unwrap());
        bake.targets = ["feed".to_string()].into();
        assert_eq!(
            replaced_bake_reason(&lost, &bake),
            "the bake of the shared triggers: no. This trigger setup's bake replaced it: feed is baked from this \
             setup, and any other trigger it held has no bake until `weft bake --instance ada` sets it up again",
        );
    }

    /// Both messages that send a person to bake again name one command,
    /// the instance's for an instance's setup: a capture that cannot be
    /// read says the same as a replaced bake.
    #[test]
    fn baking_again_names_the_setups_own_command() {
        let ada = weft_core::instance::InstanceId::new("ada").unwrap();
        assert_eq!(bake_again(None), "`weft bake`");
        assert_eq!(bake_again(Some(&ada)), "`weft bake --instance ada`");
        let error = anyhow::anyhow!("row 3 does not read");
        assert_eq!(
            unreadable_capture_reason(&error, Some("ada")),
            "cannot save trigger bake: row 3 does not read; the last good bake is preserved. \
             Run `weft bake --instance ada` again to refresh it.",
        );
        assert_eq!(
            unreadable_capture_reason(&error, None),
            "cannot save trigger bake: row 3 does not read; the last good bake is preserved. \
             Run `weft bake` again to refresh it.",
        );
    }

    /// An instance on the run's row that does not parse never parses, so
    /// it is said once in the same unreadable message, naming the row and
    /// the value, rather than failing every pass.
    #[test]
    fn an_instance_that_does_not_parse_is_said_with_the_unreadable_capture() {
        let error = anyhow::anyhow!("row 3 does not read");
        let broken = "not an instance!";
        assert!(weft_core::instance::InstanceId::new(broken).is_err(), "the value must not parse");
        let reason = unreadable_capture_reason(&error, Some(broken));
        assert!(reason.starts_with("cannot save trigger bake: row 3 does not read; the last good bake is preserved."), "{reason}");
        assert!(reason.contains("Its run's row (run.instance_id) also holds an instance that does not read ("), "{reason}");
        assert!(reason.contains("'not an instance!'"), "the value is named: {reason}");
        assert!(reason.contains("`weft bake --instance <the instance>`"), "{reason}");
    }
}
