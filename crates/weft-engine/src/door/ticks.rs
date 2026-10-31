//! When the door ticks (`/v1/door/tick`, [`super::Door::start_ticks`]):
//! once a second while this worker has something under way, and not at
//! all once it has nothing. A worker with no runs and no callers makes no
//! broker call, so the broker and its database can go to sleep behind it.
//!
//! ```text
//!   work arrives ─► wake ─► tick now, then every second
//!   nothing under way, and the last tick said so ─► sleep until woken
//! ```
//!
//! "Under way" is: a run of the door going (`Limits::going`), counts of
//! this minute the broker has not heard yet, or what the worker says it
//! still holds (the runs it drives, the records it still has to write:
//! [`WorkUnderWay`]). The last tick before a sleep is the one that
//! reported nothing in flight and every count, so the broker's picture is
//! the latest one.
//!
//! A worker whose every run only waits on something outside it
//! (`crate::run_holds`) sleeps too, once a tick landed with every count and
//! vouched for the worker until the earliest of those waits gives up, and a
//! lease's length past it: the broker keeps its lease that long, so those
//! runs, the door's own included, stay the worker's and the copy stays
//! counted with what it last reported, and a database that sleeps once
//! nothing queries it can sleep through a wait of hours. When that wait
//! ends its run moves and the tick goes again, vouching for the next one
//! (shorter or longer: it vouches again whenever what it would vouch for
//! changed). The dispatcher's look at lapsed leases stays booked for the
//! latest end any tick vouched for, so a worker that dies after its
//! waiting runs moved again is noticed then. A worker told to stop
//! ([`UnderWay::Stopping`]) ticks with no vouch for as long as it lives
//! (`crate::worker`), and the first of those ticks, when a vouch was out,
//! says the lease got shorter ([`shortens`]): the broker rings that look,
//! so the runs the stopping worker leaves behind are found a lease length
//! after it goes.
//!
//! The lease is what the tick keeps: a run on record as this worker's is
//! read as lost once the lease lapsed and a margin as long again went by
//! (`weft_dispatcher::worker_door`). Every way a run comes onto this worker
//! wakes the tick, and the run goes on at once without waiting for it: the
//! tick it woke renews the lease a round trip later, long before the
//! margin runs out.

use std::time::Duration;

/// How often the loop ticks while something is under way (scaled).
pub(crate) const TICK_EVERY: Duration = Duration::from_secs(1);

/// What the worker says it has under way, besides the door's own count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnderWay {
    /// Nothing: the door sleeps once its own runs are done too.
    Nothing,
    /// Runs, every one of them (the door's own included) only waiting on
    /// something outside it, and every record written: the door sleeps
    /// once a tick landed with every count and vouched for the worker until
    /// `vouch_until` (unix seconds).
    OnlyQuiet { vouch_until: i64 },
    /// Work: the tick goes on.
    Working,
    /// The worker was told to stop: the tick goes on, vouching for nothing,
    /// for as long as the process lives.
    Stopping,
}

impl UnderWay {
    /// What a worker has under way, from whether its records are all
    /// written, how many runs it drives, how many of those only wait and
    /// the earliest moment one of those waits gives up, and how long its
    /// lease holds without a tick (`lease_secs`).
    pub fn of(records_written: bool, driving: usize, (quiet, earliest): (usize, Option<i64>), lease_secs: i64) -> Self {
        match (records_written, driving, earliest) {
            (false, _, _) => UnderWay::Working,
            (true, 0, _) => UnderWay::Nothing,
            (true, driving, Some(earliest)) if driving <= quiet => UnderWay::OnlyQuiet { vouch_until: earliest.saturating_add(lease_secs) },
            (true, _, _) => UnderWay::Working,
        }
    }

    /// What the tick vouches for: until when the worker's lease must hold
    /// without another tick.
    pub fn vouch_until(self) -> Option<i64> {
        match self {
            UnderWay::OnlyQuiet { vouch_until } => Some(vouch_until),
            UnderWay::Nothing | UnderWay::Working | UnderWay::Stopping => None,
        }
    }
}

/// What the worker says it has under way, asked before each sleep.
pub type WorkUnderWay = Box<dyn Fn() -> UnderWay + Send + Sync>;

/// What the last tick that landed said: whether nothing was in flight,
/// and until when it vouched for the worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Landed {
    pub(super) nothing_in_flight: bool,
    pub(super) vouched_until: Option<i64>,
}

/// Whether the tick loop may sleep: the worker has `under_way`, the last
/// tick `landed` (`None` when it failed), the door's own runs are all done
/// (`door_idle`) and every count it took was reported (`counts_reported`).
pub(super) fn may_sleep(under_way: UnderWay, landed: Option<Landed>, door_idle: bool, counts_reported: bool) -> bool {
    let Some(landed) = landed else { return false };
    match under_way {
        UnderWay::Nothing => landed.nothing_in_flight && door_idle && counts_reported,
        UnderWay::OnlyQuiet { vouch_until } => counts_reported && landed.vouched_until == Some(vouch_until),
        UnderWay::Working | UnderWay::Stopping => false,
    }
}

/// Whether the tick of a worker with `under_way` says its lease got
/// shorter: it was told to stop while the last tick that landed had
/// `vouched` for it. The broker then rings the look at lapsed leases,
/// which was booked for the vouch.
pub(super) fn shortens(vouched: Option<i64>, under_way: UnderWay) -> bool {
    under_way == UnderWay::Stopping && vouched.is_some()
}

/// The tick's sleep and wake (see the module doc).
pub struct Ticking {
    /// Notified on every wake. With nobody waiting it keeps one permit, so
    /// a wake that lands just before the loop starts waiting is not lost:
    /// the loop finds it and looks again.
    woken: tokio::sync::Notify,
}

impl Ticking {
    pub fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self { woken: tokio::sync::Notify::new() })
    }

    /// Something came under way: a sleeping tick loop ticks now. Called
    /// after the work is counted where the loop looks for it (the door's
    /// limits, the worker's [`WorkUnderWay`]), so the loop, woken now or
    /// finding the permit this leaves, sees the work.
    pub fn wake(&self) {
        self.woken.notify_one();
    }

    /// Wait for the next [`Self::wake`], or return at once on a permit one
    /// left meanwhile. The loop looks again either way: a permit left
    /// while it was ticking is no reason to tick.
    pub(super) async fn sleep(&self) {
        self.woken.notified().await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use axum::http::HeaderMap;
    use futures::FutureExt as _;
    use tokio::time::{sleep, Instant};
    use weft_broker_client::protocol::DoorTickRequest;

    use super::super::fake::*;
    use super::super::{Arrived, Call, Door};
    use super::*;

    fn secs(n: f64) -> Duration {
        Duration::from_secs_f64(n)
    }

    async fn arrive(door: &Door, headers: &HeaderMap) -> Arrived {
        let call = Call { method: "GET", raw_path: "/x", raw_query: "", headers, peer: "10.0.0.1".parse().unwrap(), relayed: false, route_prefix: "" };
        door.arrive(call, axum::body::Body::empty()).await
    }

    /// A door serving `x`, ticking, with nothing under way but what
    /// `busy` says.
    async fn ticking(busy: impl Fn() -> bool + Send + Sync + 'static) -> (Arc<Door>, Arc<FakeDoorBroker>) {
        ticking_with(move || if busy() { UnderWay::Working } else { UnderWay::Nothing }).await
    }

    /// A door serving `x`, ticking on the test's own runtime (its clock
    /// paused), the worker saying it has `under_way`.
    async fn ticking_with(under_way: impl Fn() -> UnderWay + Send + Sync + 'static) -> (Arc<Door>, Arc<FakeDoorBroker>) {
        let broker = FakeDoorBroker::new(vec![route("t1", "route", "x")]);
        let door = door(broker.clone());
        door.start_ticks(&tokio::runtime::Handle::current(), Box::new(under_way));
        (door, broker)
    }

    fn ticks(broker: &FakeDoorBroker) -> Vec<DoorTickRequest> {
        broker.ticks.lock().unwrap().clone()
    }

    fn the_minute() -> i64 {
        weft_core::signal::limits::window(crate::now_unix() as i64).0
    }

    /// A call keeps the door ticking every second while its run goes; once
    /// it ends, one tick says so with the latest counts, and then nothing.
    #[tokio::test(start_paused = true)]
    async fn the_door_ticks_while_a_run_goes_then_once_more_then_never() {
        let (door, broker) = ticking(|| false).await;
        sleep(secs(10.0)).await;
        assert!(ticks(&broker).is_empty(), "a door with nothing under way never ticks, at startup neither");

        let minute = the_minute();
        let Arrived::Run(admitted, _) = arrive(&door, &HeaderMap::new()).await else { panic!("admitted") };
        sleep(secs(3.5)).await;
        let busy = ticks(&broker);
        assert!(busy.len() >= 4, "the call is decided on what the copy last heard, then its tick goes out at once, then one a second: {}", busy.len());
        assert!(busy.iter().all(|tick| tick.in_flight.get("t1") == Some(&1)), "every tick after the admission says the run is in flight");

        drop(admitted);
        sleep(secs(3.0)).await;
        let after = ticks(&broker);
        assert_eq!(after.len(), busy.len() + 1, "one last tick once the run ended");
        let last = after.last().unwrap();
        assert!(last.in_flight.is_empty(), "the last tick says nothing is in flight");
        if last.window_start == minute {
            let ran = weft_core::signal::limits::ran_key("t1");
            assert!(last.counts.iter().any(|count| count.key == ran && count.hits == 1), "the last tick carries the minute's counts");
        }
        sleep(secs(30.0)).await;
        assert_eq!(ticks(&broker).len(), after.len(), "silence once idle");
    }

    /// After a long sleep the lease has lapsed: a run that comes then goes
    /// on at once, and the tick it woke goes out at once and renews the
    /// lease behind it, however slow the broker is to answer.
    #[tokio::test(start_paused = true)]
    async fn a_run_after_a_long_sleep_does_not_wait_and_its_tick_goes_out() {
        let holds = Arc::new(AtomicBool::new(false));
        let (door, broker) = ticking({
            let holds = holds.clone();
            move || holds.load(Ordering::SeqCst)
        })
        .await;
        sleep(secs(20.0)).await;
        assert!(ticks(&broker).is_empty(), "asleep since it started");
        *broker.tick_takes.lock().unwrap() = secs(2.0);
        holds.store(true, Ordering::SeqCst);
        let asked = Instant::now();
        door.wake();
        assert_eq!(asked.elapsed(), Duration::ZERO, "the run waited for nothing");
        sleep(secs(0.5)).await;
        assert_eq!(ticks(&broker).len(), 1, "the tick it woke went out at once");
    }

    /// The broker being away when the worker wakes refuses nothing: the
    /// call is let in, and the tick tries again each second.
    #[tokio::test(start_paused = true)]
    async fn a_lapsed_lease_and_a_failing_tick_refuse_no_call() {
        let (door, broker) = ticking(|| false).await;
        sleep(secs(20.0)).await;
        broker.tick_fails.store(true, Ordering::SeqCst);
        let Arrived::Run(_admitted, _) = arrive(&door, &HeaderMap::new()).await else { panic!("admitted") };
        sleep(secs(3.5)).await;
        assert!(ticks(&broker).len() >= 4, "a failed tick is tried again each second");
    }

    /// A wake left while the loop was ticking does not keep it awake: the
    /// door still goes quiet after one last tick.
    #[tokio::test(start_paused = true)]
    async fn a_wake_while_ticking_leaves_no_extra_tick() {
        let holds = Arc::new(AtomicBool::new(true));
        let (door, broker) = ticking({
            let holds = holds.clone();
            move || holds.load(Ordering::SeqCst)
        })
        .await;
        sleep(secs(2.5)).await;
        door.wake();
        door.wake();
        holds.store(false, Ordering::SeqCst);
        let before = ticks(&broker).len();
        sleep(secs(30.0)).await;
        assert_eq!(ticks(&broker).len(), before + 1, "one last tick, then silence");
    }

    /// What the worker holds (a run it claimed, a record on its way) keeps
    /// the door ticking as long as it does.
    #[tokio::test(start_paused = true)]
    async fn the_door_ticks_while_the_worker_holds_work() {
        let holds = Arc::new(AtomicBool::new(false));
        let (door, broker) = ticking({
            let holds = holds.clone();
            move || holds.load(Ordering::SeqCst)
        })
        .await;
        sleep(secs(5.0)).await;
        holds.store(true, Ordering::SeqCst);
        door.wake();
        sleep(secs(3.5)).await;
        let held = ticks(&broker).len();
        assert!(held >= 4, "a tick at once, then one a second: {held}");
        holds.store(false, Ordering::SeqCst);
        sleep(secs(1.5)).await;
        let after = ticks(&broker).len();
        sleep(secs(30.0)).await;
        assert_eq!(ticks(&broker).len(), after, "silence once it holds nothing");
    }

    /// A refusal is a count too: it wakes the tick, is reported once, and
    /// the door goes quiet again.
    #[tokio::test(start_paused = true)]
    async fn a_refusal_while_idle_is_reported_once() {
        let (door, broker) = ticking(|| false).await;
        sleep(secs(10.0)).await;
        let minute = the_minute();
        let mut headers = HeaderMap::new();
        headers.insert(weft_core::instance::INSTANCE_TOKEN_HEADER, "nope".parse().unwrap());
        assert!(matches!(arrive(&door, &headers).await, Arrived::Answer(_)), "refused");
        sleep(secs(30.0)).await;
        let ticks = ticks(&broker);
        assert_eq!(ticks.len(), 1, "one tick for the refusal, then silence");
        if ticks[0].window_start == minute {
            assert!(ticks[0].counts.iter().any(|count| count.key.starts_with("t:-:")), "the refused token is counted");
        }
    }

    /// A call after a sleep is decided at once on what this copy last
    /// heard, never after a wait (the overshoot the limits module doc
    /// settles): another copy ran the entry's one run of the minute
    /// meanwhile, so the first call goes over by one, and the tick it woke
    /// brings that count in for the next call.
    #[tokio::test(start_paused = true)]
    async fn a_call_after_a_sleep_is_decided_at_once_and_its_tick_corrects_the_next() {
        let mut trigger = route("t1", "route", "x");
        armed(&mut trigger).spec.limits = weft_core::signal::EntryLimits { per_minute: Some(1), ..Default::default() };
        let broker = FakeDoorBroker::new(vec![trigger]);
        let door = door(broker.clone());
        door.start_ticks(&tokio::runtime::Handle::current(), Box::new(|| UnderWay::Nothing));
        sleep(secs(20.0)).await;
        assert!(ticks(&broker).is_empty(), "asleep since it started");

        // Meanwhile another copy ran the minute's one run.
        let minute = the_minute();
        let ran = weft_broker_client::protocol::DoorCount { key: "e:t1".into(), hits: 1 };
        *broker.heard.lock().unwrap() = (vec![ran], 2);
        *broker.tick_takes.lock().unwrap() = secs(2.0);
        let asked = Instant::now();
        let Arrived::Run(_admitted, _) = arrive(&door, &HeaderMap::new()).await else { panic!("admitted on what it last heard") };
        assert_eq!(asked.elapsed(), Duration::ZERO, "the call waited for nothing");
        sleep(secs(3.0)).await;
        if the_minute() != minute {
            return; // crossed into a new minute, which counts from nothing
        }
        assert!(matches!(arrive(&door, &HeaderMap::new()).await, Arrived::Answer(_)), "the next call is refused on the count its tick heard");
    }

    /// While the door ticks every second, a call is decided on what it heard
    /// last and waits for nothing, however slow the broker is right now.
    #[tokio::test(start_paused = true)]
    async fn a_call_while_ticking_waits_for_no_tick() {
        let holds = Arc::new(AtomicBool::new(true));
        let (door, broker) = ticking({
            let holds = holds.clone();
            move || holds.load(Ordering::SeqCst)
        })
        .await;
        sleep(secs(3.5)).await;
        *broker.tick_takes.lock().unwrap() = secs(2.0);
        let asked = Instant::now();
        let Arrived::Run(_admitted, _) = arrive(&door, &HeaderMap::new()).await else { panic!("admitted") };
        assert_eq!(asked.elapsed(), Duration::ZERO, "no wait while ticking");
    }

    /// A run of the door that only waits outside lets the door sleep after
    /// a tick that vouches for the worker until the earliest wait gives up,
    /// even though the run is still going (and still counted); a later
    /// wait, or a shorter one, vouches again before the door sleeps on; once
    /// the run moves, the door ticks at once and vouches no more. None of
    /// that says the lease got shorter: only a stop does, once.
    #[tokio::test(start_paused = true)]
    async fn a_door_whose_only_run_waits_vouches_until_its_wait_ends_and_sleeps() {
        let quiet = Arc::new(std::sync::Mutex::new(UnderWay::Working));
        let (door, broker) = ticking_with({
            let quiet = quiet.clone();
            move || *quiet.lock().unwrap()
        })
        .await;
        let wait_until = |vouch_until| *quiet.lock().unwrap() = UnderWay::OnlyQuiet { vouch_until };
        let Arrived::Run(_admitted, _) = arrive(&door, &HeaderMap::new()).await else { panic!("admitted") };
        sleep(secs(3.5)).await;
        wait_until(5_000);
        sleep(secs(30.0)).await;
        let asleep = ticks(&broker);
        let last = asleep.last().unwrap();
        assert_eq!(last.vouch_until, Some(5_000), "the last tick vouched until the wait ends");
        assert_eq!(last.in_flight.get("t1"), Some(&1), "the waiting run is still going, and counted");
        assert_eq!(asleep.iter().filter(|tick| tick.vouch_until.is_some()).count(), 1, "one vouching tick, then sleep");

        wait_until(9_000);
        door.wake();
        sleep(secs(30.0)).await;
        let again = ticks(&broker);
        assert_eq!(again.len(), asleep.len() + 1, "a later wait vouches once more, then sleep");
        assert_eq!(again.last().unwrap().vouch_until, Some(9_000));

        wait_until(7_000);
        door.wake();
        sleep(secs(30.0)).await;
        let shorter = ticks(&broker);
        assert_eq!(shorter.len(), again.len() + 1, "a shorter wait vouches again too");
        assert_eq!(shorter.last().unwrap().vouch_until, Some(7_000));

        *quiet.lock().unwrap() = UnderWay::Working;
        door.wake();
        sleep(secs(2.5)).await;
        let moved = ticks(&broker);
        assert!(moved.len() >= shorter.len() + 2, "the run moved: the door ticks at once, then every second");
        assert!(moved[shorter.len()..].iter().all(|tick| tick.vouch_until.is_none()));
        assert!(!moved.iter().any(|tick| tick.shortens), "a wait that moves or vouches again rings nothing");

        wait_until(8_000);
        door.wake();
        sleep(secs(30.0)).await;
        let vouched = ticks(&broker);
        assert_eq!(vouched.last().unwrap().vouch_until, Some(8_000), "asleep again, vouched for");
        *quiet.lock().unwrap() = UnderWay::Stopping;
        door.wake();
        sleep(secs(2.5)).await;
        let stopping = &ticks(&broker)[vouched.len()..];
        assert!(stopping.len() >= 2, "told to stop: the door ticks at once, then every second");
        assert!(stopping.iter().all(|tick| tick.vouch_until.is_none()));
        assert_eq!(stopping.iter().map(|tick| tick.shortens).collect::<Vec<_>>()[..2], [true, false], "the first tick of the stop says the lease got shorter, once");
    }

    #[test]
    fn only_a_stop_after_a_vouch_shortens_a_lease() {
        assert!(shortens(Some(100), UnderWay::Stopping), "told to stop while vouched for");
        assert!(!shortens(None, UnderWay::Stopping), "nothing vouched, nothing to shorten");
        assert!(!shortens(Some(100), UnderWay::Working), "a wait that moves again");
        assert!(!shortens(Some(100), UnderWay::OnlyQuiet { vouch_until: 50 }), "a shorter wait");
    }

    #[test]
    fn what_a_worker_has_under_way_decides_whether_its_door_sleeps() {
        assert_eq!(UnderWay::of(false, 0, (0, None), 15), UnderWay::Working, "a record still to write");
        assert_eq!(UnderWay::of(true, 0, (0, None), 15), UnderWay::Nothing);
        assert_eq!(UnderWay::of(true, 2, (2, Some(100)), 15), UnderWay::OnlyQuiet { vouch_until: 115 }, "the earliest wait, and a lease past it");
        assert_eq!(UnderWay::of(true, 2, (1, Some(100)), 15), UnderWay::Working, "one run still moves");
        assert_eq!(UnderWay::of(true, 1, (2, Some(100)), 15), UnderWay::OnlyQuiet { vouch_until: 115 }, "a quiet run a moment after it left");
        let landed = |vouched_until| Some(Landed { nothing_in_flight: false, vouched_until });
        let idle = Some(Landed { nothing_in_flight: true, vouched_until: None });
        assert!(may_sleep(UnderWay::Nothing, idle, true, true));
        assert!(!may_sleep(UnderWay::Nothing, idle, false, true), "a run of the door still going");
        assert!(may_sleep(UnderWay::OnlyQuiet { vouch_until: 115 }, landed(Some(115)), false, true), "the door's own runs are the quiet ones");
        assert!(!may_sleep(UnderWay::OnlyQuiet { vouch_until: 115 }, landed(None), false, true), "no tick vouched yet");
        assert!(!may_sleep(UnderWay::OnlyQuiet { vouch_until: 200 }, landed(Some(115)), false, true), "a later wait is not vouched for yet");
        assert!(!may_sleep(UnderWay::OnlyQuiet { vouch_until: 100 }, landed(Some(115)), false, true), "nor a shorter one: a dead worker must not keep its lease past it");
        assert!(!may_sleep(UnderWay::OnlyQuiet { vouch_until: 115 }, None, false, true), "the last tick failed");
        assert!(!may_sleep(UnderWay::OnlyQuiet { vouch_until: 115 }, landed(Some(115)), false, false), "a count not reported yet");
        assert!(!may_sleep(UnderWay::Working, idle, true, true));
        assert!(!may_sleep(UnderWay::Stopping, idle, true, true), "a stopping worker ticks while it lives");
    }

    /// A wake that comes before the loop starts waiting is kept for it: the
    /// sleep that follows returns at once.
    #[tokio::test]
    async fn a_wake_before_the_sleep_is_not_lost() {
        let ticking = Ticking::new();
        ticking.wake();
        assert!(ticking.sleep().now_or_never().is_some(), "the wake left for the sleeper");
        assert!(ticking.sleep().now_or_never().is_none(), "one wake, one return");
    }
}
