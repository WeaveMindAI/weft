//! Live-tier node-test composition: builds the [`weft_core::LiveRig`]
//! whose handle is the PRODUCTION [`RunnerHandle`], so a live test's
//! calls go through the real connection resolution, relaying, and
//! metering, exactly like a firing inside an execution.
//!
//! What differs from a real firing, and only this:
//!   - the journal is [`weft_journal::NoopJournal`]: a node test is
//!     not an execution, nothing folds it, and it must not fabricate
//!     execution rows;
//!   - emitted outputs are captured at the ctx seam instead of routed
//!     (there is no downstream graph);
//!   - each RUNNER carries one execution identity (execution id +
//!     color), so a test's spends attribute to one real color, however
//!     many times its body fires the rig.

use std::sync::{Arc, Mutex};

use weft_core::access::Access;
use weft_core::cancellation::CancellationFlag;
use weft_core::context::ContextHandle;
use weft_core::node_test::LiveHandleFactory;
use weft_core::{Color, LiveRig};

use crate::context::{BusCoordinator, EngineClients, RunnerHandle};

/// Composes live rigs over one broker-client bundle and settles their
/// debts when the runs are over. One runner serves every live test of
/// a session; call [`Self::settle`] before the process reports its
/// results so no leased connection and no in-flight cost record is
/// dropped.
pub struct LiveTestRunner {
    clients: EngineClients,
    pod_name: String,
    tenant_id: String,
    project_id: String,
    /// THE run's execution identity: the pre-registered color the
    /// spawning runtime supplied (it registered the color so the
    /// broker can scope the run), or a fresh mint. One runner serves
    /// one test run, so the runner IS one execution: every rig and
    /// every handle it mints carries this same color, and every spend
    /// attributes to it.
    color: Color,
    /// Every handle a rig minted, so `settle` can release the
    /// runtime-owned connections their bodies opened.
    handles: Arc<Mutex<Vec<Arc<RunnerHandle>>>>,
}

impl LiveTestRunner {
    /// `clients` should come from `EngineClients::from_broker`; the
    /// journal is replaced with the no-write impl here (see module
    /// doc), so callers hand in the production bundle unmodified.
    pub fn new(
        mut clients: EngineClients,
        pod_name: String,
        tenant_id: String,
        project_id: String,
        fixed_color: Option<Color>,
    ) -> Self {
        clients.journal = Arc::new(weft_journal::NoopJournal);
        Self {
            clients,
            pod_name,
            tenant_id,
            project_id,
            color: fixed_color.unwrap_or_else(Color::new_v4),
            handles: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// A rig for one live test: `connection_id` is the chosen grant
    /// for the test's declared `service`. The rig carries the runner's
    /// one execution identity, so a body that fires the rig several
    /// times attributes every spend to the same execution, exactly
    /// like several calls inside one firing would.
    pub fn rig(&self, connection_id: &str, service: &str) -> LiveRig {
        let access = Access::new(connection_id, service, None);
        let clients = self.clients.clone();
        let pod_name = self.pod_name.clone();
        let tenant_id = self.tenant_id.clone();
        let project_id = self.project_id.clone();
        let handles = self.handles.clone();
        let color = self.color;
        // ONE coordinator per rig, shared by every handle it mints:
        // the rig is one test, and a bus the test seeds (`rig.bus`)
        // must resolve from the handle `rig.run` mints, exactly like
        // two firings of one execution share the execution's registry.
        let bus_coordinator = BusCoordinator::new();
        let factory: LiveHandleFactory = Arc::new(move |node_type, declared| {
            let handle = Arc::new(RunnerHandle::new(
                format!("node-test-{}", color.simple()),
                project_id.clone(),
                color,
                "node-under-test".to_string(),
                node_type.to_string(),
                weft_core::frames::LoopFrames::default(),
                clients.clone(),
                pod_name.clone(),
                tenant_id.clone(),
                Arc::new(CancellationFlag::new()),
                bus_coordinator.clone(),
                declared,
            ));
            handles.lock().unwrap().push(handle.clone());
            Ok(handle as Arc<dyn ContextHandle>)
        });
        LiveRig::new(factory, access)
    }

    /// The execution color this run's cost is recorded under.
    pub fn color(&self) -> Color {
        self.color
    }

    /// Settle everything the runs left open: releases every leased
    /// connection, then blocks until every in-flight cost record has
    /// landed. MUST run before the process reports and exits; skipping
    /// it drops money. Returns one entry per release that failed
    /// (naming the grant, so an operator can release it by hand);
    /// empty means everything was released.
    pub async fn settle(&self) -> Vec<String> {
        let handles: Vec<Arc<RunnerHandle>> =
            std::mem::take(&mut *self.handles.lock().unwrap());
        let mut failures = Vec::new();
        for handle in handles {
            failures.extend(handle.close_opened_accesses().await);
        }
        self.clients.pending_costs.wait_zero().await;
        failures
    }
}
