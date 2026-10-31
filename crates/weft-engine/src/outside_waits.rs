//! What a run keeps, while its process keeps it, for the waits only the
//! outside can end: the ask for their answers ([`AnswerAsk`]), and the hold
//! clock that gives up the waits held in a node's call once nothing moved
//! in the run for its `holdSecs` ([`HoldClock`],
//! `weft_core::run_settings::RunSettings::hold_secs`).
//!
//! The drive of every run keeps them (`crate::run_driver`). A node
//! test's run has no drive: its rig keeps them through [`keep_held`], the
//! same pieces, so a live test waits exactly the way a run that cannot
//! pause does.

use std::time::Duration;

use weft_broker_client::protocol::RunAnswer;
use weft_core::RunId;

use crate::context::EngineClients;
use crate::held_waits::{HeldWaits, Unsuspendable};

/// The ask for a run's answers, made only while something of it waits on
/// one, and kept across the turns of whoever waits on it: a chatty bus
/// turns a drive many times a second, and an ask started afresh on every
/// turn would read as often.
pub(crate) struct AnswerAsk<'a> {
    clients: &'a EngineClients,
    run_id: RunId,
    /// Every wait whose answer this ask took, which the broker leaves out.
    taken: Vec<String>,
    asking: Option<futures::future::BoxFuture<'static, anyhow::Result<Vec<RunAnswer>>>>,
}

impl<'a> AnswerAsk<'a> {
    pub(crate) fn new(clients: &'a EngineClients, run_id: RunId) -> Self {
        Self { clients, run_id, taken: Vec::new(), asking: None }
    }

    /// Ask while `needed`, keeping an ask already under way; with nothing
    /// needed, nothing is asked.
    pub(crate) fn while_needed(&mut self, needed: bool) {
        if !needed {
            self.asking = None;
        } else if self.asking.is_none() {
            self.asking = Some(Box::pin(answers_once_told(
                self.clients.runs.clone(),
                self.clients.told.subscribe(),
                self.run_id,
                self.taken.clone(),
            )));
        }
    }

    /// The answers, once the broker has some (never while nothing is
    /// asked). Dropping this midway keeps the ask under way.
    pub(crate) async fn next(&mut self) -> anyhow::Result<Vec<RunAnswer>> {
        let Some(asking) = self.asking.as_mut() else { return std::future::pending().await };
        let answers = asking.await;
        self.asking = None;
        let answers = answers?;
        self.taken.extend(answers.iter().map(|answer| answer.token.clone()));
        Ok(answers)
    }
}

/// The answers waiting for run `run_id`'s waits but the ones
/// `taken`, once there are some: read now (the run starts waiting, or just
/// took an answer), and again each time its process's line says one was
/// handed to this run (`told`, subscribed before this is called, so an
/// answer handed between an empty read and the wait still wakes it). When
/// the line may have missed some, the process reads once for all its runs,
/// and this run takes its own from that. Nothing else reads: a run that
/// waits a day leaves the database asleep all that time.
async fn answers_once_told(
    runs: std::sync::Arc<dyn crate::context::RunClient>,
    mut told: tokio::sync::broadcast::Receiver<crate::context::RunTold>,
    run_id: RunId,
    taken: Vec<String>,
) -> anyhow::Result<Vec<RunAnswer>> {
    use crate::context::RunTold;
    use tokio::sync::broadcast::error::RecvError;
    loop {
        let answers = runs.answers(run_id, &taken).await?;
        if !answers.is_empty() {
            return Ok(answers);
        }
        loop {
            match told.recv().await {
                Ok(RunTold::Answered(run)) if run == run_id => break,
                Ok(RunTold::Answered(_)) => {}
                Ok(RunTold::Rechecked(all)) => {
                    let mine: Vec<_> = all
                        .iter()
                        .filter(|answer| answer.run_id == run_id && !taken.contains(&answer.token))
                        .map(|answer| RunAnswer { token: answer.token.clone(), answer: answer.answer.clone() })
                        .collect();
                    if !mine.is_empty() {
                        return Ok(mine);
                    }
                }
                // Fell behind what the line said: what was missed is read.
                Err(RecvError::Lagged(_)) => break,
                Err(RecvError::Closed) => anyhow::bail!("the worker stopped telling its runs about their answers"),
            }
        }
    }
}

/// How long a run has been quiet (every node it runs waits, and only the
/// outside can end one of those waits), and when its hold gives up: its
/// `holdSecs` after the last time anything moved in it.
pub(crate) struct HoldClock<'a> {
    clock: &'a dyn weft_platform_traits::Clock,
    run_id: RunId,
    hold_secs: u32,
    /// When the run went quiet.
    quiet_since: Option<std::time::Instant>,
    /// When the run's hold gives up (unix seconds), while it is quiet:
    /// set once as it goes quiet.
    quiet_until: Option<i64>,
}

impl<'a> HoldClock<'a> {
    pub(crate) fn new(clock: &'a dyn weft_platform_traits::Clock, run_id: RunId, hold_secs: u32) -> Self {
        Self { clock, run_id, hold_secs, quiet_since: None, quiet_until: None }
    }

    /// Whether the run is quiet now: the clock runs from when it went
    /// quiet, and stops once it is not.
    pub(crate) fn quiet(&mut self, quiet: bool) {
        self.quiet_since = quiet.then(|| self.quiet_since.unwrap_or_else(|| self.clock.now()));
    }

    /// How long the run has been quiet, while the hold `counts` (it never
    /// does while a node waits on its caller: a silent caller is never cut),
    /// which also settles when the hold gives up ([`Self::until`]).
    pub(crate) fn quiet_for(&mut self, counts: bool) -> Option<Duration> {
        let quiet_for = self.quiet_since.filter(|_| counts).map(|since| self.clock.now().saturating_duration_since(since));
        let hold_for = self.hold_for();
        self.quiet_until =
            quiet_for.map(|quiet_for| self.quiet_until.unwrap_or_else(|| hold_ends_unix(crate::now_unix(), hold_for.saturating_sub(quiet_for))));
        quiet_for
    }

    /// When the hold gives up (unix seconds), while it counts.
    pub(crate) fn until(&self) -> Option<i64> {
        self.quiet_until
    }

    /// Ends once the hold runs out, the run having been quiet for
    /// `quiet_for` ([`Self::quiet_for`]); never without.
    pub(crate) fn held_out(&self, quiet_for: Option<Duration>) -> impl std::future::Future<Output = ()> + 'a {
        let (clock, left) = (self.clock, quiet_for.map(|quiet_for| self.hold_for().saturating_sub(quiet_for)));
        async move {
            match left {
                Some(left) => clock.sleep(left).await,
                None => std::future::pending().await,
            }
        }
    }

    /// The hold ran out: every wait `held` keeps is given up, for why the
    /// run cannot pause (none when it can, with nothing held to give up).
    /// The clock starts over. Answers how many were given up.
    pub(crate) fn ran_out(&mut self, held: &HeldWaits, why: Option<Unsuspendable>) -> usize {
        self.quiet_since = None;
        let given_up = why.map_or(0, |why| held.give_up(&crate::held_waits::gave_up_because(why, self.hold_secs)));
        tracing::info!(
            target: "weft_engine::outside_waits",
            run_id = %self.run_id,
            hold_secs = self.hold_secs,
            given_up,
            "nothing moved in the run for its hold; giving up the waits it holds"
        );
        given_up
    }

    /// A turn of whoever keeps the clock ended, the run having `moved` in
    /// it or not, which starts the clock over. Answers whether the run is
    /// quiet no longer (it moved, or its hold ran out).
    pub(crate) fn turned(&mut self, moved: bool) -> bool {
        if moved {
            self.quiet_since = None;
        }
        if self.quiet_since.is_none() {
            self.quiet_until = None;
        }
        self.quiet_since.is_none()
    }

    fn hold_for(&self) -> Duration {
        Duration::from_secs(self.hold_secs.into())
    }
}

/// When the hold of a run that went quiet gives up (unix seconds), `now`
/// being the moment and `left` what is left of it, rounded up.
fn hold_ends_unix(now: u64, left: Duration) -> i64 {
    let secs = now.saturating_add(left.as_secs()).saturating_add(u64::from(left.subsec_nanos() > 0));
    i64::try_from(secs).unwrap_or(i64::MAX)
}

/// Keep the waits of a run no drive keeps: a node test's, whose rig runs
/// the node under test, at `firing`, alone. Its waits all hold in their
/// calls (the run cannot pause, for `why`); their answers go to those
/// calls, and once nothing moved in the run for its `hold_secs` while
/// `firing` only holds, they are given up. A cancel of the run gives up
/// every wait it holds from then on, naming its cause. Runs until dropped.
#[cfg(any(test, feature = "node-tests"))]
pub(crate) async fn keep_held(
    clients: &EngineClients,
    run_id: RunId,
    waits: &crate::wait_tracker::WaitTracker,
    firing: weft_core::liveness::FiringLocation,
    hold_secs: u32,
    why: Unsuspendable,
    cancellation: &weft_core::cancellation::CancellationFlag,
) {
    let firing: std::collections::HashSet<_> = [firing].into_iter().collect();
    let held = waits.held();
    let mut ask = AnswerAsk::new(clients, run_id);
    let mut clock = HoldClock::new(clients.clock.as_ref(), run_id, hold_secs);
    loop {
        // Armed before anything is read, so a wait that begins or ends
        // between the read and the wait still wakes it.
        let on_wait_change = waits.wait_notified();
        tokio::pin!(on_wait_change);
        on_wait_change.as_mut().enable();
        if let Some(cause) = cancellation.cause() {
            held.give_up(&format!("its run was cancelled ({cause})"));
        }
        let holding = held.pending();
        clock.quiet(holding && waits.deadlock_provable(&firing));
        ask.while_needed(holding);
        let quiet_for = clock.quiet_for(true);
        let held_out = clock.held_out(quiet_for);
        tokio::pin!(held_out);
        let mut moved = true;
        tokio::select! {
            answers = ask.next() => match answers {
                Ok(answers) => {
                    moved = !answers.is_empty();
                    for answer in answers {
                        held.answer(&answer.token, answer.answer);
                    }
                }
                Err(e) => {
                    held.give_up(&format!("its answers could not be read: {e:#}"));
                }
            },
            () = held_out.as_mut(), if quiet_for.is_some() => {
                moved = false;
                clock.ran_out(held, Some(why));
            }
            _ = on_wait_change.as_mut() => {}
            () = cancellation.cancelled(), if !cancellation.is_cancelled() => {}
        }
        clock.turned(moved);
    }
}

#[cfg(test)]
mod tests {
    //! Layer 3: the waits of a run no drive keeps (a node test's), through
    //! the production handle, with the broker faked.

    use std::sync::Arc;

    use serde_json::json;
    use weft_core::cancellation::CancellationFlag;
    use weft_core::context::ContextHandle;
    use weft_core::error::WeftResult;
    use weft_core::liveness::FiringLocation;

    use super::*;
    use crate::context::{BusCoordinator, RunRecord, RunnerHandle};
    use crate::run_driver::engine_test_rig::{clients, handle, Answers, AwaitTasks, MemJournal};

    /// A node's wait in a run with no program, held for `hold_secs`, and
    /// what it went through.
    struct Held {
        tasks: Arc<AwaitTasks>,
        answers: Arc<Answers>,
        cancellation: Arc<CancellationFlag>,
        run_id: RunId,
        /// The node's call: what its wait ended with.
        call: tokio::task::JoinHandle<WeftResult<serde_json::Value>>,
        keeper: tokio::task::JoinHandle<()>,
    }

    /// The node `node` waits on a timer in a run with no program, its waits
    /// kept by [`keep_held`], as a node test's rig composes it.
    fn held(hold_secs: u32) -> Held {
        let (tasks, answers) = (AwaitTasks::new(), Arc::new(Answers::default()));
        let clients = EngineClients { tasks: tasks.clone(), told: answers.told.clone(), runs: answers.clone(), ..clients(Arc::new(MemJournal::default())) };
        let run_id = weft_core::new_run_id();
        let record = RunRecord { journal: handle(&clients, run_id, &[], 0, Vec::new()), costs: crate::metering::PendingCostRecords::new() };
        let waits = crate::wait_tracker::WaitTracker::new();
        let cancellation = CancellationFlag::new_arc();
        let settings = weft_core::run_settings::RunSettings::bookkeeping().holding_for(hold_secs).unwrap();
        let node = RunnerHandle::new(
            uuid::Uuid::nil(),
            run_id,
            "node".into(),
            "node".into(),
            "Waits".into(),
            Default::default(),
            clients.clone(),
            record,
            None,
            "replica".into(),
            "tenant".into(),
            cancellation.clone(),
            waits.clone(),
            BusCoordinator::new(waits.clone()),
            Default::default(),
            Default::default(),
            Default::default(),
            false,
        )
        .with_settings(settings)
        .without_program();
        let call = tokio::spawn(async move {
            let timer = weft_core::signal::Timer { spec: weft_core::signal::TimerSpec::After { duration_ms: 600_000 } };
            node.await_signal(weft_core::signal::to_spec(timer)).await
        });
        let keeper = tokio::spawn({
            let cancellation = cancellation.clone();
            async move {
                let here = FiringLocation::new("node", Default::default());
                keep_held(&clients, run_id, &waits, here, hold_secs, Unsuspendable::NoProgram, &cancellation).await
            }
        });
        Held { tasks, answers, cancellation, run_id, call, keeper }
    }

    impl Held {
        /// The wait's token, once the node registered it and its keeper
        /// asked for its answers.
        async fn asked(&self) -> String {
            let token = self.tasks.await_token().await;
            while self.answers.reads() == 0 {
                tokio::task::yield_now().await;
            }
            token
        }

        /// What the node's call ended with; the keeper stops.
        async fn ended(self) -> (WeftResult<serde_json::Value>, Vec<String>) {
            let ended = tokio::time::timeout(std::time::Duration::from_secs(10), self.call).await.expect("the wait ended").unwrap();
            self.keeper.abort();
            (ended, self.tasks.withdrawn.lock().unwrap().clone())
        }
    }

    // A wait held in a run with no program takes its answer once the news
    // of it comes, the way a drive hands a held wait its answer: the call
    // returns the value, and nothing is withdrawn.
    weft_core::stress_test! {
        name: a_held_wait_of_a_run_with_no_program_takes_its_answer,
        runs: 20,
        worker_threads: 4,
        async fn body() {
            let held = held(60);
            let token = held.asked().await;
            // Handed to the run, and the news of it passed on, as the
            // line watch passes it on.
            held.answers.keep(token, json!("done"));
            held.answers.told.send(crate::context::RunTold::Answered(held.run_id)).unwrap();
            let (ended, withdrawn) = held.ended().await;
            assert_eq!(ended.unwrap(), json!("done"));
            assert!(withdrawn.is_empty(), "an answered wait is not withdrawn: {withdrawn:?}");
        }
    }

    /// A wait nothing answers is given up once nothing moved for the hold:
    /// the call fails naming why the run could not pause, and the wait is
    /// withdrawn.
    #[tokio::test]
    async fn a_held_wait_nothing_answers_is_given_up_after_its_hold() {
        let held = held(1);
        let token = held.asked().await;
        let (ended, withdrawn) = held.ended().await;
        let error = ended.unwrap_err().to_string();
        assert!(error.contains("gave up its wait") && error.contains("node test") && error.contains("1 seconds"), "{error}");
        assert_eq!(withdrawn, vec![token]);
    }

    /// A cancel of the run gives up the wait it holds, naming the cause.
    #[tokio::test]
    async fn a_cancel_gives_up_the_held_wait() {
        let held = held(60);
        let token = held.asked().await;
        held.cancellation.cancel_because(weft_core::run::CancelCause::User);
        let (ended, withdrawn) = held.ended().await;
        let error = ended.unwrap_err().to_string();
        assert!(error.contains("its run was cancelled"), "{error}");
        assert_eq!(withdrawn, vec![token]);
    }
}
