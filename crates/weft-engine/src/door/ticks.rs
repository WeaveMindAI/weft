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
//! The lease is what the tick keeps: a run on record as this worker's is
//! read as lost once the lease lapsed and a margin as long again went by
//! (`weft_dispatcher::worker_door`). Every way a run comes onto this worker
//! wakes the tick, and the run goes on at once without waiting for it: the
//! tick it woke renews the lease a round trip later, long before the
//! margin runs out.

use std::time::Duration;

/// How often the loop ticks while something is under way (scaled).
pub(super) const TICK_EVERY: Duration = Duration::from_secs(1);

/// What the worker says it still holds, besides the door's own runs: true
/// while the tick has to go on.
pub type WorkUnderWay = Box<dyn Fn() -> bool + Send + Sync>;

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
        let broker = FakeDoorBroker::new(vec![route("t1", "route", "x")]);
        let door = door(broker.clone());
        door.start_ticks(Box::new(busy)).await.unwrap();
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
        assert_eq!(ticks(&broker).len(), 1, "only the first tick, at startup: nothing came since");

        let minute = the_minute();
        let Arrived::Run(admitted, _) = arrive(&door, &HeaderMap::new()).await else { panic!("admitted") };
        sleep(secs(3.5)).await;
        let busy = ticks(&broker);
        assert!(busy.len() >= 5, "the call is decided on what the copy last heard, then its tick goes out at once, then one a second: {}", busy.len());
        assert!(busy[2..].iter().all(|tick| tick.in_flight.get("t1") == Some(&1)), "every tick after the admission says the run is in flight");

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
        assert_eq!(ticks(&broker).len(), 1, "asleep since the first tick");
        *broker.tick_takes.lock().unwrap() = secs(2.0);
        holds.store(true, Ordering::SeqCst);
        let asked = Instant::now();
        door.wake();
        assert_eq!(asked.elapsed(), Duration::ZERO, "the run waited for nothing");
        sleep(secs(0.5)).await;
        assert_eq!(ticks(&broker).len(), 2, "the tick it woke went out at once");
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
        assert!(held >= 5, "a tick at once, then one a second: {held}");
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
        assert_eq!(ticks.len(), 2, "one tick for the refusal, then silence");
        if ticks[1].window_start == minute {
            assert!(ticks[1].counts.iter().any(|count| count.key.starts_with("t:-:")), "the refused token is counted");
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
        door.start_ticks(Box::new(|| false)).await.unwrap();
        sleep(secs(20.0)).await;
        assert_eq!(ticks(&broker).len(), 1, "asleep since the first tick");

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
