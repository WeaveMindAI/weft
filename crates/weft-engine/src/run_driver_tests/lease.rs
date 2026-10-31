    //! Layer 3: a node body that holds its thread for long, the way heavy
    //! work done inline in one poll does, delays none of its worker's door
    //! ticks. The ticks keep the worker's lease and run on the worker's io
    //! runtime (`EngineClients::io`), apart from the threads node bodies run
    //! on, so a slow body never loses its run.

    use std::sync::Mutex as StdMutex;
    use std::time::{Duration, Instant};

    use super::*;
    use super::engine_test_rig::{catalog, drive, test_manifest};
    use crate::door::fake::{door, FakeDoorBroker};
    use async_trait::async_trait;
    use serde_json::json;
    use weft_core::error::WeftResult;
    use weft_core::node::Node;
    use weft_core::{ProjectDefinition, RunContext};

    /// How many tick intervals the body holds its thread for.
    const SPIN_TICKS: u32 = 4;

    /// Holds its thread for [`SPIN_TICKS`] tick intervals in one poll,
    /// noting each moment a tick reached `broker` meanwhile, between the
    /// moments the spin started and ended.
    struct Spins {
        broker: Arc<FakeDoorBroker>,
        seen: Arc<StdMutex<Vec<Instant>>>,
    }
    test_manifest!(Spins, "Spins");
    #[async_trait]
    impl Node for Spins {
        async fn run(&self, _ctx: RunContext) -> WeftResult<()> {
            let started = Instant::now();
            let until = started + weft_core::time_scale::scaled(crate::door::ticks::TICK_EVERY) * SPIN_TICKS;
            let mut seen = vec![started];
            let mut ticks = self.broker.ticks.lock().unwrap().len();
            while Instant::now() < until {
                let now = self.broker.ticks.lock().unwrap().len();
                if now > ticks {
                    ticks = now;
                    seen.push(Instant::now());
                }
                std::hint::spin_loop();
            }
            seen.push(Instant::now());
            *self.seen.lock().unwrap() = seen;
            Ok(())
        }
    }

    fn project() -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [{
                "id": "spin", "nodeType": "Spins", "label": null, "config": null, "position": {"x": 0.0, "y": 0.0},
                "inputs": [], "outputs": [],
                "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []
            }],
            "edges": [],
            "groups": []
        }))
        .unwrap()
    }

    // As many runs as the main runtime has threads, each spinning one of
    // them: while the bodies spin, nothing on the main runtime moves, and
    // the ticks still land every interval.
    weft_core::stress_test!(
        name: a_body_holding_its_thread_delays_no_tick_of_the_lease,
        runs: 4,
        worker_threads: 4,
        async fn body() {
            let every = weft_core::time_scale::scaled(crate::door::ticks::TICK_EVERY);
            let broker = FakeDoorBroker::new(Vec::new());
            let door = door(broker.clone());
            // Where a worker's door ticks (`crate::worker::serve`).
            let io = crate::context::io_runtime().expect("the io runtime starts");
            door.start_ticks(&io, Box::new(|| crate::door::UnderWay::Working));
            let seen = Arc::new(StdMutex::new(Vec::new()));
            let cat = catalog(vec![("Spins", Box::new(Spins { broker: broker.clone(), seen: seen.clone() }))]);
            let (outcome, _) = drive(project(), cat, &["spin"]).await;
            assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
            let seen = seen.lock().unwrap().clone();
            // Between the spin's start, each tick that landed during it,
            // and its end: never two intervals without a tick.
            let gaps: Vec<Duration> = seen.windows(2).map(|pair| pair[1] - pair[0]).collect();
            assert!(seen.len() >= 2 + SPIN_TICKS as usize / 2, "ticks during the spin: {gaps:?}");
            assert!(gaps.iter().all(|gap| *gap < every * 2), "a tick waited for the body: {gaps:?}");
            drop(door);
        }
    );
