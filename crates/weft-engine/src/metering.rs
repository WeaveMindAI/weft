//! The worker side of measured calls: the middleware stack behind an
//! opened connection's client.
//!
//! ONE composition serves every connection:
//!
//! ```text
//! client = base
//!        + MeteringMiddleware   when a meter is registered for the service
//!        + AuthMiddleware       always (the service's resolved auth steps)
//! ```
//!
//! For each request, the metering middleware
//!   1. routes it (straight to the service, or to the runtime's relay
//!      when the resolved credential carries one),
//!   2. on a direct billable route, runs the service's meter around it:
//!      prepare the request so its cost becomes reportable, TAP the response
//!      stream (the caller sees every chunk in real time; nothing is
//!      buffered or delayed), and
//!   3. when the response ends (cleanly or cut), resolves the meter's
//!      figure and records it durably on the execution's cost trail.
//!
//! The auth middleware runs INSIDE the metering one, so the meter
//! classifies the URL the node wrote while the credential lands on the
//! final request; the meter's own follow-up query rides a separate
//! signed-in client and never sees a credential. That composition is
//! what makes a sign-in that costs money measurable at all.
//!
//! A relayed call is not measured here: the relay is where the runtime's
//! own measuring happens, and this side's only job is to route the call
//! there. A worker-side figure is a MEASUREMENT, never a charge: the record
//! it enqueues is pinned `billed: false` and the broker refuses anything
//! else from a worker.
//!
//! The resolve + record run detached from the node's future (the call may
//! be cut by a cancel), tracked by [`PendingCostRecords`] so the pod never
//! exits while money is still being written down.

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{FutureExt, Stream, StreamExt};

use weft_core::error::{WeftError, WeftResult};
use weft_core::frames::LoopFrames;
use weft_core::Color;
use weft_providers::{
    CallObservation, FollowUp, MeasuredCost, ObservedCall, ProviderMeter, RouteClass,
};

// ---------- Pending-record tracking ----------

/// Counts cost resolutions still in flight process-wide, so the pod's exit
/// paths can refuse to die while a call's money is still being written
/// down. Incremented when a metered response ends (the resolve task is
/// spawned), decremented when its record has landed (or loudly failed).
pub struct PendingCostRecords {
    count: AtomicUsize,
    zero: tokio::sync::Notify,
}

impl PendingCostRecords {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { count: AtomicUsize::new(0), zero: tokio::sync::Notify::new() })
    }

    pub fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    fn begin(&self) {
        self.count.fetch_add(1, Ordering::SeqCst);
    }

    /// Release one token. A release with none held is a bookkeeping bug
    /// in a caller (one `end` too many): it is refused and logged rather
    /// than let the count wrap to the maximum, which would keep every
    /// `wait_zero` (the pod's exit gate) waiting for ever.
    fn end(&self) {
        let before = self.count.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| count.checked_sub(1));
        match before {
            Ok(1) => self.zero.notify_waiters(),
            Ok(_) => {}
            Err(_) => tracing::error!(
                target: "weft_engine::metering",
                "a cost record token was released with none held; the release is ignored"
            ),
        }
    }

    /// Resolve once no cost records are in flight.
    ///
    /// Every resolve is internally bounded (the follow-up client has a
    /// request timeout, the ledger poll a fixed budget). An OPEN charge
    /// holds a token until the execution that opened it ends
    /// (`OpenCharges::flush_color`); a report being read holds one of its
    /// own until the read returns (bounded the same way); and each record
    /// being written holds one until it is enqueued. So this returns as
    /// long as the executions being waited on have ended.
    pub async fn wait_zero(&self) {
        loop {
            // Arm BEFORE checking, so an `end` between the check and the
            // await cannot be missed: a `Notified` future is bound at
            // CREATION (tokio guarantees it completes for any
            // `notify_waiters` that fires after this line, even if the
            // future is first polled later), so the check-then-park window
            // is covered. Pinned by the wait_zero stress test below.
            let notified = self.zero.notified();
            if self.count() == 0 {
                return;
            }
            notified.await;
        }
    }
}

// ---------- Charges awaiting the response that states their amount ----------

/// What tells one open charge from every other the pod is holding: the
/// METER that opened it, and the id that meter chose.
///
/// The id alone is not enough. One pod holds the charges of every
/// service, every connection and every execution it is running, and the
/// id is a string a meter picks: `job-1` from one provider's example
/// meter collides with `job-1` from another's, and the collision was
/// resolved by booking one of the two as an unknown spend and letting
/// the survivor's report price it through the OTHER charge's sink, so a
/// real figure landed on the wrong execution's trail.
///
/// The service closes the collision BETWEEN meters, and that is all it
/// closes. Two executions on one pod whose ids come from the same meter
/// can still collide, because `report` is handed nothing but the meter
/// and the id (the response is the only thing that arrives, and it says
/// nothing about which run asked), so there is no color to key on at
/// lookup time. A meter that mints its own ids is what makes that
/// reachable, which is why the trait's own docs require an id the
/// PROVIDER assigned.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct ChargeKey {
    service: &'static str,
    id: String,
}

impl std::fmt::Display for ChargeKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.service, self.id)
    }
}

/// One billable call that spent money whose amount only a later response
/// states, held until a [`RouteClass::Reports`] response answers for it.
struct OpenCharge {
    /// Tells this charge from another that claimed the same id.
    ///
    /// A reused provider id must not let an old report claim a newer
    /// call's charge or book its figure against another execution.
    token: u64,
    /// Meter-owned state: seeded with the billable call's own observation
    /// data, folded by every report until one prices it.
    scratch: serde_json::Value,
    /// Which node's execution the spend belongs to. A charge outlives the
    /// call that opened it, so the sink that books it is carried with it
    /// rather than looked up later.
    sink: Arc<CostSink>,
}

/// One owner per charge, with reports queued in arrival order. A closing
/// execution drains received reports before declaring the amount unknown.
pub struct OpenCharges {
    next_token: std::sync::atomic::AtomicU64,
    by_id: std::sync::Mutex<std::collections::HashMap<ChargeKey, ChargeState>>,
}

struct ChargeState {
    charge: OpenCharge,
    reports: std::collections::VecDeque<ChargeReport>,
    processing: bool,
    closing: Option<String>,
}

struct ChargeReport {
    meter: &'static dyn ProviderMeter,
    path: String,
    observed: ObservedCall,
    follow_up: reqwest_middleware::ClientWithMiddleware,
    done: tokio::sync::oneshot::Sender<()>,
}

impl OpenCharges {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            next_token: std::sync::atomic::AtomicU64::new(0),
            by_id: std::sync::Mutex::new(std::collections::HashMap::new()),
        })
    }

    fn by_id(&self) -> std::sync::MutexGuard<'_, std::collections::HashMap<ChargeKey, ChargeState>> {
        self.by_id.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn count(&self) -> usize { self.by_id().len() }

    fn open(&self, service: &'static str, id: String, mut charge: OpenCharge) {
        let id = ChargeKey { service, id };
        charge.token = self.next_token.fetch_add(1, Ordering::SeqCst);
        charge.sink.pending.begin();
        let replaced = self.by_id().insert(id.clone(), ChargeState {
            charge, reports: Default::default(), processing: false, closing: None,
        });
        if let Some(replaced) = replaced {
            tracing::error!(target: "weft_engine::metering", charge = %id, unread = replaced.reports.len(),
                "two billable calls claimed the same charge id; booking the displaced call as unknown \
                 (any report queued for it goes unread; one being read right now still lands)");
            book_open_charge(replaced.charge, "displaced by a second call claiming the same id");
        }
    }

    /// Claim the report synchronously. Dropping this future never cancels
    /// accounting for a response we have already received.
    fn report(
        self: &Arc<Self>,
        meter: &'static dyn ProviderMeter,
        reported: &str,
        path: &str,
        observed: ObservedCall,
        follow_up: &reqwest_middleware::ClientWithMiddleware,
    ) -> impl std::future::Future<Output = ()> + use<> {
        let id = ChargeKey { service: meter.service(), id: reported.to_string() };
        let (done, received) = tokio::sync::oneshot::channel();
        let (known, start) = {
            let mut charges = self.by_id();
            match charges.get_mut(&id) {
                None => (false, None),
                Some(state) => {
                    state.reports.push_back(ChargeReport {
                        meter, path: path.to_string(), observed, follow_up: follow_up.clone(), done,
                    });
                    let start = if state.processing { None } else {
                        state.processing = true;
                        Some(state.charge.token)
                    };
                    (true, start)
                }
            }
        };
        if !known {
            // Either an ordinary re-read of a job already priced and
            // closed (a node may re-read a finished job as often as it
            // likes), or a figure that arrived after its execution ended
            // and the charge was booked as unknown. The two cannot be
            // told apart here, and the second loses the only number
            // anyone will ever have, so it is said out loud.
            tracing::warn!(
                target: "weft_engine::metering",
                charge = %id,
                "a report arrived for a charge that is no longer open; if its execution already \
                 ended, the spend stays on the trail as unknown"
            );
        }
        if let Some(token) = start {
            match tokio::runtime::Handle::try_current() {
                Ok(runtime) => {
                    let charges = self.clone();
                    runtime.spawn(async move { charges.drain_reports(id, token).await; });
                }
                Err(_) => {
                    let held = {
                        let mut charges = self.by_id();
                        if charges.get(&id).is_some_and(|state| state.charge.token == token) {
                            charges.remove(&id)
                        } else { None }
                    };
                    if let Some(held) = held {
                        book_open_charge(held.charge, "no runtime was available to read the received report");
                    }
                }
            }
        }
        async move { let _ = received.await; }
    }

    async fn drain_reports(&self, id: ChargeKey, token: u64) {
        loop {
            let next = {
                let mut charges = self.by_id();
                let Some(state) = charges.get_mut(&id).filter(|state| state.charge.token == token) else { return };
                match state.reports.pop_front() {
                    Some(report) => Ok((report, state.charge.scratch.clone(), state.charge.sink.clone())),
                    None => {
                        state.processing = false;
                        let why = state.closing.clone();
                        Err(why.map(|why| (charges.remove(&id).expect("owned charge").charge, why)))
                    }
                }
            };
            let (report, mut scratch, sink) = match next {
                Ok(next) => next,
                Err(held) => {
                    if let Some((charge, why)) = held { book_open_charge(charge, &why); }
                    return;
                }
            };
            // The read holds its own pending token: the charge's token can be
            // released under it (a second call displacing the charge books it
            // as unknown), and a figure this read still produces must not
            // land after `wait_zero` let the pod exit.
            sink.pending.begin();
            let result = std::panic::AssertUnwindSafe(report.meter.fold_report(
                &report.path, report.observed, &mut scratch,
                FollowUp { http: &report.follow_up, base_url: report.meter.base_url() },
            )).catch_unwind().await;
            let completed = {
                let mut charges = self.by_id();
                match charges.get_mut(&id).filter(|state| state.charge.token == token) {
                    Some(state) => {
                        state.charge.scratch = scratch;
                        match result {
                            Ok(None) => None,
                            result => {
                                let state = charges.remove(&id).expect("owned charge");
                                Some((state.charge, result, state.reports.len()))
                            }
                        }
                    }
                    None => {
                        // The charge is gone or re-tokened under us. Only
                        // `open` does that while a read is in progress
                        // (`close_where` defers, and `report`'s no-runtime
                        // removal only runs when nothing is processing): a
                        // second billable call claimed
                        // this id while the fold ran, and `open` booked our
                        // charge as unknown. If this report priced it, the
                        // figure goes on the trail too, loudly, rather than
                        // being thrown away.
                        match result {
                            Ok(Some(cost)) => {
                                tracing::warn!(target: "weft_engine::metering", charge = %id,
                                    "the amount for this charge arrived after a second call displaced it; it is \
                                     on the trail twice, once as unknown and once with the figure");
                                sink.clone().book_resolved(cost);
                            }
                            Ok(None) => {}
                            Err(_) => tracing::error!(target: "weft_engine::metering", charge = %id,
                                "the meter panicked while reading a cost report for a charge a second call had displaced"),
                        }
                        sink.pending.end();
                        let _ = report.done.send(());
                        return;
                    }
                }
            };
            if let Some((charge, result, unread)) = completed {
                match result {
                    Ok(Some(cost)) => {
                        let sink = charge.sink;
                        sink.clone().book_resolved(cost);
                        sink.pending.end();
                    }
                    Err(_) => {
                        tracing::error!(target: "weft_engine::metering", charge = %id,
                            "the meter panicked while reading a cost report");
                        book_open_charge(charge, "the meter panicked while reading a cost report");
                    }
                    Ok(None) => {
                        // `completed` is only `Some` for a priced or panicked
                        // read; never panic on the money path regardless.
                        tracing::error!(target: "weft_engine::metering", charge = %id,
                            "an incomplete report closed a charge; booking it as unknown");
                        book_open_charge(charge, "an incomplete report closed the charge");
                    }
                }
                if unread > 0 {
                    // Ordinarily re-reads of a job that is now priced. A
                    // provider that revised its figure on a later read
                    // would be revising into the void, so say how many.
                    tracing::warn!(target: "weft_engine::metering", charge = %id, unread,
                        "reports received after the one that priced this charge were not read");
                }
                sink.pending.end();
                let _ = report.done.send(());
                return;
            }
            sink.pending.end();
            let _ = report.done.send(());
        }
    }

    pub fn flush(&self, why: &str) { self.close_where(|_| true, why); }

    pub fn flush_color(&self, color: weft_core::Color, why: &str) {
        self.close_where(|charge| charge.sink.color == color, why);
    }

    /// Book every matching charge as unknown and drop it. A charge whose
    /// report is being read right now is not booked yet: the read may
    /// still price it, so it is marked to close when the read returns
    /// (`drain_reports`). Until then it holds its pending token, and a
    /// wait on `PendingCostRecords::wait_zero` (pod shutdown) waits for
    /// that read, which is bounded by the follow-up client's timeout.
    fn close_where(&self, matches: impl Fn(&OpenCharge) -> bool, why: &str) {
        let held = {
            let mut charges = self.by_id();
            let ids: Vec<_> = charges.iter_mut().filter_map(|(id, state)| {
                if !matches(&state.charge) { return None; }
                if state.processing {
                    tracing::info!(target: "weft_engine::metering", charge = %id,
                        "closing while its report is being read; it is booked when that read returns");
                    state.closing = Some(why.to_string());
                    None
                } else { Some(id.clone()) }
            }).collect();
            ids.into_iter().map(|id| charges.remove(&id).expect("selected charge").charge).collect::<Vec<_>>()
        };
        for charge in held { book_open_charge(charge, why); }
    }
}

/// Book one open charge as unknown: the spend happened, nothing ever
/// stated its amount.
fn book_open_charge(charge: OpenCharge, why: &str) {
    // `scratch` is meter-owned and a meter may replace it wholesale, so
    // it is not guaranteed to be an object. Indexing a non-object
    // `Value` panics, and this is reached from a destructor that can
    // run during an unwind, where a panic aborts the pod. Keep whatever
    // the meter left, under a key, rather than trusting its shape.
    let mut metadata = match charge.scratch {
        serde_json::Value::Object(_) => charge.scratch,
        other => serde_json::json!({ "meterState": other }),
    };
    metadata["resolution"] = serde_json::json!(format!(
        "the call spent, and no response ever stated what it cost ({why})"
    ));
    let model = metadata["model"].as_str().map(str::to_string);
    let sink = charge.sink.clone();
    // Book first, release the charge's token after: the count must not
    // pass through zero while a record is still to be enqueued.
    sink.clone().book_resolved(MeasuredCost { amount_usd: None, model, metadata });
    sink.pending.end();
}

// ---------- The cost sink (where a measured figure lands) ----------

/// Everything needed to book one call's measured cost to the execution's
/// durable cost trail: the task client to enqueue through, the firing the
/// spend belongs to, and the pending-records tracker.
pub struct CostSink {
    pub tasks: Arc<dyn weft_task_store::TaskStoreClient>,
    pub pending: Arc<PendingCostRecords>,
    /// Charges opened by a call whose amount a later response states.
    /// Pod-wide, because a charge outlives the call that opened it.
    pub open_charges: Arc<OpenCharges>,
    pub project_id: String,
    pub tenant_id: String,
    pub color: Color,
    pub node_id: String,
    pub frames: LoopFrames,
    pub service: String,
    /// Whose credential the connection rides; recorded on every figure
    /// so the cost trail says whose account spent.
    pub origin: weft_core::CredentialOwner,
}

impl CostSink {
    /// Book one finished observation's figure: resolve `cost` and write
    /// it down durably, detached from the caller's future (which may be
    /// aborted at any point) and tracked by the pending counter so the
    /// pod cannot exit while money is still being written. The one
    /// begin/spawn/record/end sequence behind every finalizer, HTTP and
    /// session alike; a session hands a ready future, an HTTP call the
    /// meter's resolve.
    pub(crate) fn book(
        self: Arc<Self>,
        cost: impl std::future::Future<Output = MeasuredCost> + Send + 'static,
    ) {
        self.pending.begin();
        let dedup_key = format!("metered_cost:{}", uuid::Uuid::new_v4());
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    let cost = cost.await;
                    self.record(dedup_key, cost).await;
                    self.pending.end();
                });
            }
            Err(_) => {
                // No runtime to spawn on (the process is tearing down
                // outside tokio): the record cannot be written. Say so
                // loudly; never drop money silently.
                self.pending.end();
                tracing::error!(
                    target: "weft_engine::metering",
                    "COST RECORD LOST for node {} ({}): the async runtime was already torn \
                     down when this call's figure was due, so it could not be written down",
                    self.node_id, self.service,
                );
            }
        }
    }

    /// Book a figure that is already resolved. The twin of [`Self::book`]
    /// for a charge whose amount arrived on a later response, where there
    /// is nothing left to await by the time it is known.
    pub(crate) fn book_resolved(self: Arc<Self>, cost: MeasuredCost) {
        self.book(async move { cost });
    }

    /// Enqueue the durable `RecordCost` task. One record per physical call
    /// (a replayed body that calls again spends again, and gets its own
    /// record), so the dedup key is minted per call and only guards
    /// enqueue retries. A record that cannot be enqueued after bounded
    /// retries is logged LOUDLY: the money trail is incomplete and says so.
    async fn record(&self, dedup_key: String, cost: MeasuredCost) {
        let payload = weft_task_store::RecordCostPayload {
            color: self.color.to_string(),
            node_id: self.node_id.clone(),
            frames: self.frames.clone(),
            service: self.service.clone(),
            model: cost.model,
            amount_usd: cost.amount_usd,
            billed: false,
            origin: self.origin,
            metadata: cost.metadata,
        };
        let payload_json = match serde_json::to_value(&payload) {
            Ok(v) => v,
            Err(e) => {
                tracing::error!(
                    target: "weft_engine::metering",
                    "COST RECORD LOST for node {} ({}): payload serialize failed: {e}",
                    self.node_id, self.service,
                );
                return;
            }
        };
        const ENQUEUE_ATTEMPTS: u32 = 3;
        for attempt in 1..=ENQUEUE_ATTEMPTS {
            let task = weft_task_store::NewTask {
                kind: weft_task_store::TaskKind::RecordCost.into(),
                target: weft_task_store::TaskTarget::Dispatcher,
                project_id: Some(self.project_id.clone()),
                dedup_key: Some(dedup_key.clone()),
                color: Some(self.color.to_string()),
                tenant_id: Some(self.tenant_id.clone()),
                target_pod_name: None,
                binary_hash: None,
                payload: payload_json.clone(),
            };
            match self.tasks.enqueue_dedup(task).await {
                Ok(_) => return,
                Err(e) if attempt < ENQUEUE_ATTEMPTS => {
                    tracing::warn!(
                        target: "weft_engine::metering",
                        "cost record enqueue failed (attempt {attempt}): {e:#}; retrying",
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
                Err(e) => {
                    tracing::error!(
                        target: "weft_engine::metering",
                        "COST RECORD LOST for node {} ({}): enqueue failed after retries: {e:#}",
                        self.node_id, self.service,
                    );
                }
            }
        }
    }
}

// ---------- The middleware ----------

/// Per-connection metering middleware; see the module docs.
pub struct MeteringMiddleware {
    /// The service's meter, when one is registered. `None` = the call
    /// passes through unmeasured (a service without a meter has no cost
    /// figure at all).
    meter: Option<&'static dyn ProviderMeter>,
    /// The runtime's relay for calls on this connection; `None` = direct.
    relay_url: Option<String>,
    /// A signed-in client for the meter's own follow-up query on the
    /// direct lane (the same auth the original call rode, on the
    /// bounded follow-up pool). The meter itself never sees a
    /// credential.
    follow_up: reqwest_middleware::ClientWithMiddleware,
    sink: Arc<CostSink>,
}

/// Build the signed-in (and, when a meter is registered, measured)
/// client for one opened connection: the ONE composition every
/// connection's calls ride. `steps` are the service's auth steps
/// already resolved against the connection's values. Fails loud when
/// the connection is relayed but no meter is registered for the
/// service: routing to a relay needs the service's base URL to strip,
/// and only a meter knows it.
pub fn connection_client(
    service: &str,
    steps: Vec<weft_core::access::client::AppliedStep>,
    relay_url: Option<&str>,
    sink: Arc<CostSink>,
) -> WeftResult<reqwest_middleware::ClientWithMiddleware> {
    let meter = weft_providers::meter_for(service);
    if relay_url.is_some() && meter.is_none() {
        return Err(WeftError::NodeExecution(format!(
            "the runtime relays calls for service '{service}', but no meter is registered \
             to route them by; connect your own credential on the node"
        )));
    }
    // A runtime-supplied credential is only ever sent on routes its
    // meter explicitly knows (the meter IS the allowlist: billable
    // routes carry the measurement, free routes are declared free).
    // Without a meter there is no allowlist, so the shared door does
    // not open, on either lane.
    if sink.origin == weft_core::CredentialOwner::Ours && meter.is_none() {
        return Err(WeftError::NodeExecution(format!(
            "service '{service}' offers a runtime credential but registers no meter; a \
             runtime credential only travels on meter-declared routes, so this door \
             cannot open. Connect your own credential on the node, or register a meter \
             for the service"
        )));
    }
    let follow_up = reqwest_middleware::ClientBuilder::new(follow_up_client().clone())
        .with(weft_core::access::client::AuthMiddleware::new(steps.clone()))
        .build();
    let middleware = MeteringMiddleware {
        meter,
        relay_url: relay_url.map(str::to_string),
        follow_up,
        sink,
    };
    // Metering OUTER (classifies the URL the node wrote, rewrites to
    // the relay), auth INNER (the credential lands on the final
    // request either lane).
    // The pool is the shared connection hygiene base (standard
    // redirect handling included, on both lanes).
    Ok(reqwest_middleware::ClientBuilder::new(
        weft_core::access::client::base_client().clone(),
    )
    .with(middleware)
    .with(weft_core::access::client::AuthMiddleware::new(steps))
    .build())
}

/// The client a meter's own follow-up query rides. Separate from the
/// shared pool on purpose: a follow-up must be bounded (the
/// pending-record tracker relies on every resolve finishing), so it
/// carries a total request timeout. Redirects are disabled because a
/// follow-up addresses the provider's own fixed origins (its base_url,
/// its rate catalog) and must never be bounced anywhere else.
fn follow_up_client() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("metering follow-up client")
    })
}

fn middleware_err(msg: String) -> reqwest_middleware::Error {
    reqwest_middleware::Error::Middleware(anyhow::anyhow!(msg))
}

#[async_trait::async_trait]
impl reqwest_middleware::Middleware for MeteringMiddleware {
    async fn handle(
        &self,
        mut req: reqwest::Request,
        extensions: &mut http::Extensions,
        next: reqwest_middleware::Next<'_>,
    ) -> reqwest_middleware::Result<reqwest::Response> {
        // The route this call addresses, relative to the provider's base.
        // `None` = the URL is not under the provider's API at all (or this
        // runtime has no meter to know where that is).
        let route = self.meter.and_then(|m| {
            weft_providers::route_under(m.base_url(), req.url().as_str())
                .map(|r| r.to_string())
        });

        if let Some(relay) = &self.relay_url {
            // Relayed lane: rebuild the URL against the relay and send.
            // The relay does the measuring; this side does none. The
            // rebuild is the shared one every relayed lane uses.
            let Some(meter) = self.meter else {
                // `connection_client` refuses a relay without a meter, so
                // this is a broken invariant rather than anything the
                // program did. Refused rather than panicked: this runs
                // inside a request, and a panic here enters the worker's
                // unwind path.
                return Err(middleware_err(
                    "this connection relays through the platform, which only meters what a meter \
                     describes, and no meter was attached; this is a bug in weft"
                        .to_string(),
                ));
            };
            // The SAME allowlist the direct lane applies below, and for
            // the same reason: a credential the runtime holds travels only
            // on routes its meter declares. The relay does the measuring,
            // but what may be ASKED is this side's rule, and the
            // design-time lane (`weft_broker::credential`) already checks
            // it here. Without this the two worker lanes disagreed about
            // what the meter-as-allowlist means, and an undeclared route
            // went out on the platform's key.
            if self.sink.origin == weft_core::CredentialOwner::Ours {
                weft_providers::ours_route_on(
                    meter,
                    &self.sink.service,
                    req.method().as_str(),
                    req.url().as_str(),
                )
                .map_err(middleware_err)?;
            }
            let relayed =
                weft_providers::relay_join(meter.base_url(), relay, req.url().as_str())
                    .map_err(middleware_err)?;
            *req.url_mut() = reqwest::Url::parse(&relayed).map_err(|e| {
                middleware_err(format!("relay URL {relayed:?} does not parse: {e}"))
            })?;
            return next.run(req, extensions).await;
        }

        // Direct lane. A RUNTIME credential (origin Ours) travels only
        // on routes its meter explicitly declares; the shared gate
        // refuses everything else loud BEFORE the credential is
        // attached. A key the USER holds is theirs to aim: unknown
        // routes pass through unmeasured as always.
        if self.sink.origin == weft_core::CredentialOwner::Ours {
            let Some(meter) = self.meter else {
                return Err(middleware_err(
                    "this connection's credential is the runtime's, which may only be spent \
                     through a meter, and no meter was attached; this is a bug in weft"
                        .to_string(),
                ));
            };
            weft_providers::ours_route_on(
                meter,
                &self.sink.service,
                req.method().as_str(),
                req.url().as_str(),
            )
            .map_err(middleware_err)?;
        }

        // Measure billable routes; everything else passes through
        // untouched (a free route, an unknown route on a key the user
        // holds, a provider with no meter).
        let (Some(meter), Some(route)) = (self.meter, route.as_deref()) else {
            return next.run(req, extensions).await;
        };
        // Two kinds of route are watched: the one that spends, and the one
        // that reports what an earlier spend came to. Everything else
        // passes through untouched.
        let class = meter.classify(req.method().as_str(), route);
        if !matches!(class, RouteClass::Billable(_) | RouteClass::Reports) {
            return next.run(req, extensions).await;
        }

        // Before a single byte of a billable call goes out, ask whether
        // it could be priced at all when it comes back. A call the meter
        // could never put a figure on is unmetered spend however it
        // ends, and the only moment that costs nothing to prevent is
        // this one.
        if matches!(class, RouteClass::Billable(_)) {
            let follow_up = FollowUp { http: &self.follow_up, base_url: meter.base_url() };
            if let Err(e) = meter.priceable(route, follow_up).await {
                return Err(middleware_err(format!(
                    "refusing a billable call on '{}': {e:#}",
                    self.sink.service,
                )));
            }
        }

        // Prepare the request so its cost becomes reportable. A body the
        // middleware cannot see (a streaming body) cannot be prepared, and
        // an unpreparable billable call would be an unmeasurable spend:
        // refuse it loud, before any bytes go out. Buffer the body instead.
        // A reporting route spends nothing, so it is only read, never
        // prepared.
        if matches!(class, RouteClass::Billable(_)) {
            match req.body().and_then(|b| b.as_bytes()) {
                Some(bytes) => {
                    if let Some(prepared) =
                        meter.prepare(route, bytes).map_err(|e| middleware_err(e.to_string()))?
                    {
                        let len = prepared.len();
                        *req.body_mut() = Some(reqwest::Body::from(prepared));
                        req.headers_mut().insert(
                            http::header::CONTENT_LENGTH,
                            http::HeaderValue::from(len),
                        );
                    }
                }
                None => {
                    return Err(middleware_err(format!(
                        "billable call on '{}' has a streaming body, which cannot be prepared \
                         for metering; send the body buffered (a byte payload, not a stream)",
                        self.sink.service,
                    )))
                }
            }
        }
        // The tap reads the response bytes as they pass; a compressed body
        // would be opaque to it. This client never negotiates compression
        // itself, but a caller-set header would; force identity.
        req.headers_mut().remove(http::header::ACCEPT_ENCODING);

        // Captured for the observer below: some routes price off the
        // REQUEST (its query's format, its body's text), and the
        // request is consumed by the send.
        let request_query = req.url().query().unwrap_or("").to_string();
        let request_bytes: Vec<u8> =
            req.body().and_then(|b| b.as_bytes()).map(|b| b.to_vec()).unwrap_or_default();

        let response = next.run(req, extensions).await?;

        // Tap the response: the caller sees every chunk in real time while
        // the observer reads what it needs in passing. When the stream
        // ends (or is cut, including by drop), the finalizer resolves the
        // cost and records it, detached from the caller's future.
        let mut observer = meter.observe(route, &request_query, &request_bytes);
        observer.on_status(response.status().as_u16());
        let status = response.status();
        let version = response.version();
        let headers = response.headers().clone();
        // The headers reach the observer BEFORE any chunk, because a
        // provider that states the charge out of band states it here
        // (fal's `X-Fal-Billable-Units`), and a meter that has the
        // provider's own figure never has to re-derive one.
        observer.on_headers(&headers);
        let tapped = TapStream {
            inner: response.bytes_stream().boxed(),
            finalizer: Some(Finalizer {
                observer,
                meter,
                route: route.to_string(),
                class,
                open_charges: self.sink.open_charges.clone(),
                follow_up: self.follow_up.clone(),
                sink: self.sink.clone(),
            }),
        };
        // The rebuild carries the wire-visible surface only (status,
        // version, headers); response extensions are process-local
        // bookkeeping and nothing downstream of a metered call reads them.
        let mut rebuilt = http::Response::builder().status(status).version(version);
        match rebuilt.headers_mut() {
            Some(h) => *h = headers,
            None => return Err(middleware_err("rebuild tapped response".into())),
        }
        let rebuilt = rebuilt
            .body(reqwest::Body::wrap_stream(tapped))
            .map_err(|e| middleware_err(format!("rebuild tapped response: {e}")))?;
        Ok(reqwest::Response::from(rebuilt))
    }
}

/// What a finished (or cut) observation needs to become a durable record.
struct Finalizer {
    observer: Box<dyn CallObservation>,
    meter: &'static dyn ProviderMeter,
    /// The call's route, carried to `resolve` as a fact.
    route: String,
    /// Whether this response spent money or reports on an earlier spend.
    class: RouteClass,
    /// The charges this pod is holding open, for a provider that states a
    /// call's amount on a later response.
    open_charges: Arc<OpenCharges>,
    /// The signed-in follow-up client (bounded pool + the connection's
    /// auth); the meter never sees a credential.
    follow_up: reqwest_middleware::ClientWithMiddleware,
    sink: Arc<CostSink>,
}

impl Finalizer {
    /// End the observation and book the figure through [`CostSink::book`]
    /// (detached, tracked, loud on loss). EVERY billable call resolves
    /// through its meter, fixed-priced routes included: whether a call
    /// was actually charged is provider-specific knowledge (a provider
    /// may answer 200 with a failure body it never bills, or bill a
    /// call it then refuses), so the declared price is the meter's
    /// input, never the middleware's verdict.
    fn finish(self, interrupted: bool) {
        let observed = self.observer.end(interrupted);
        let meter = self.meter;
        let route = self.route;
        let follow_up_http = self.follow_up;
        let open_charges = self.open_charges;
        let sink = self.sink;

        // A response that reports on an earlier spend books nothing of its
        // own: it closes the charge that spend opened.
        if matches!(self.class, RouteClass::Reports) {
            let Some(id) = meter.charge_reported_on(&route, &observed) else { return };
            drop(open_charges.report(meter, &id, &route, observed, &follow_up_http));
            return;
        }

        // A billable call whose amount only a later response states does
        // not resolve here. The charge is opened SYNCHRONOUSLY, before this
        // returns, so the caller's very next request cannot report on a
        // charge the pod is not yet holding.
        if let Some(id) = meter.opens_charge(&route, &observed) {
            // `data` is whatever the meter's `observe` returned, and the
            // documented default hands back the response body as it
            // parsed: a provider answering a top-level array or string
            // makes this a non-object, and indexing a non-object `Value`
            // panics. The panic would land inside a stream poll or a
            // destructor, so the shape is checked rather than assumed
            // (`book_open_charge` does the same for the same reason).
            let mut scratch = match observed.data {
                serde_json::Value::Object(_) => observed.data,
                other => serde_json::json!({ "meterState": other }),
            };
            scratch["route"] = serde_json::json!(route);
            open_charges.open(meter.service(), id, OpenCharge { token: 0, scratch, sink });
            return;
        }

        sink.book(async move {
            let follow_up = FollowUp {
                http: &follow_up_http,
                base_url: meter.base_url(),
            };
            meter.resolve(&route, observed, follow_up).await
        });
    }
}

/// The response-body tap: forwards every chunk untouched and unbuffered,
/// feeding the observer in passing. Finalizes exactly once: on clean end,
/// on stream error, or on drop (the caller hung up mid-stream).
struct TapStream {
    inner: BoxStream<'static, reqwest::Result<Bytes>>,
    finalizer: Option<Finalizer>,
}

impl Stream for TapStream {
    type Item = reqwest::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                if let Some(f) = self.finalizer.as_mut() {
                    f.observer.on_chunk(&chunk);
                }
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(e))) => {
                if let Some(f) = self.finalizer.take() {
                    f.finish(true);
                }
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                if let Some(f) = self.finalizer.take() {
                    f.finish(false);
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for TapStream {
    fn drop(&mut self) {
        if let Some(f) = self.finalizer.take() {
            f.finish(true);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use weft_providers::{ObservedCall, RouteClass};

    // ---- Rig: a recording task store, a test meter, a gated SSE server ----

    /// Records every enqueued task; everything else is unreachable in these
    /// tests.
    #[derive(Default)]
    pub(super) struct RecordingTaskStore {
        pub enqueued: Mutex<Vec<weft_task_store::NewTask>>,
    }

    #[async_trait::async_trait]
    impl weft_task_store::TaskStoreClient for RecordingTaskStore {
        async fn enqueue_dedup(
            &self,
            spec: weft_task_store::tasks::NewTask,
        ) -> anyhow::Result<weft_task_store::tasks::DedupOutcome> {
            self.enqueued.lock().unwrap().push(spec);
            Ok(weft_task_store::tasks::DedupOutcome::Inserted(uuid::Uuid::new_v4()))
        }
        async fn wait_for_terminal(
            &self,
            _task_id: uuid::Uuid,
            _timeout: std::time::Duration,
            _poll_interval: std::time::Duration,
        ) -> anyhow::Result<weft_task_store::tasks::TaskOutcome> {
            unreachable!("metering tests only enqueue")
        }
        async fn claim_one(
            &self,
            _pod_id: &str,
            _filter: weft_task_store::tasks::ClaimFilter,
        ) -> anyhow::Result<Option<weft_task_store::tasks::Task>> {
            Ok(None)
        }
        async fn requeue(&self, _task_id: uuid::Uuid, _pod_id: &str) -> anyhow::Result<bool> {
            Ok(true)
        }
        async fn heartbeat(&self, _task_id: uuid::Uuid, _pod_id: &str) -> anyhow::Result<bool> {
            Ok(true)
        }
        async fn complete(
            &self,
            _task_id: uuid::Uuid,
            _pod_id: &str,
            _result: serde_json::Value,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn fail(
            &self,
            _task_id: uuid::Uuid,
            _pod_id: &str,
            _error: String,
        ) -> anyhow::Result<()> {
            Ok(())
        }
    }

    /// A meter for a provider living at the test server: chat/completions
    /// billable, cost read from the LAST SSE `usage.cost` seen; an
    /// interrupted call with no usage resolves to an honest unknown.
    /// With `fixed_usd` set, the same route classifies Fixed instead and
    /// `resolve` answers the declared price from the observed status
    /// (the meter, not the middleware, decides whether a fixed-priced
    /// call was actually charged).
    struct TestMeter {
        base: &'static str,
        fixed_usd: Option<f64>,
    }

    struct TestObservation {
        scanner: weft_providers::sse::DataLineScanner,
        status: u16,
        cost: Option<f64>,
    }

    impl CallObservation for TestObservation {
        fn on_status(&mut self, status: u16) {
            self.status = status;
        }
        fn on_chunk(&mut self, bytes: &[u8]) {
            let cost = &mut self.cost;
            self.scanner.feed(bytes, |payload| {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(payload) {
                    if let Some(c) = v["usage"]["cost"].as_f64() {
                        *cost = Some(c);
                    }
                }
            });
        }
        fn end(self: Box<Self>, interrupted: bool) -> ObservedCall {
            ObservedCall {
                interrupted,
                status: self.status,
                data: serde_json::json!({ "cost": self.cost }),
            }
        }
    }

    #[async_trait::async_trait]
    impl ProviderMeter for TestMeter {
        fn service(&self) -> &'static str {
            "testprov"
        }
        fn base_url(&self) -> &'static str {
            self.base
        }
        fn classify(&self, method: &str, path: &str) -> RouteClass {
            match (method, path) {
                ("POST", "chat/completions") => RouteClass::Billable(match self.fixed_usd {
                    Some(usd) => weft_providers::Pricing::Fixed { usd },
                    None => weft_providers::Pricing::Metered,
                }),
                ("GET", "models") => RouteClass::Free,
                _ => RouteClass::Unknown,
            }
        }
        fn prepare(&self, _path: &str, body: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
            let mut parsed: serde_json::Value = serde_json::from_slice(body)?;
            parsed["usage"] = serde_json::json!({ "include": true });
            Ok(Some(serde_json::to_vec(&parsed)?))
        }
        async fn ceiling_usd(
            &self,
            _path: &str,
            _body: &[u8],
            _follow_up: FollowUp<'_>,
        ) -> anyhow::Result<f64> {
            Ok(1.0)
        }
        fn observe(&self, _path: &str, _query: &str, _request_body: &[u8]) -> Box<dyn CallObservation> {
            Box::new(TestObservation {
                scanner: weft_providers::sse::DataLineScanner::new(),
                status: 0,
                cost: None,
            })
        }
        async fn resolve(
            &self,
            _path: &str,
            observed: ObservedCall,
            _follow_up: FollowUp<'_>,
        ) -> MeasuredCost {
            if let Some(usd) = self.fixed_usd {
                let refused = !(200..300).contains(&observed.status);
                return MeasuredCost {
                    amount_usd: Some(if refused { 0.0 } else { usd }),
                    model: None,
                    metadata: serde_json::json!({
                        "resolution": if refused {
                            "provider refused the call; nothing billed"
                        } else {
                            "fixed route price"
                        },
                        "status": observed.status,
                    }),
                };
            }
            MeasuredCost {
                amount_usd: observed.data["cost"].as_f64(),
                model: None,
                metadata: serde_json::json!({ "interrupted": observed.interrupted }),
            }
        }
    }

    /// One-route SSE server: POST /api/v1/chat/completions answers with a
    /// body streamed from an mpsc of byte chunks the TEST controls, so the
    /// pacing of the stream is deterministic. Also records the request body
    /// it received (to assert `prepare` really rewrote the wire bytes).
    async fn spawn_sse_server() -> (
        String,                                                  // base url
        tokio::sync::mpsc::UnboundedSender<Bytes>,               // feed chunks
        Arc<Mutex<Vec<serde_json::Value>>>,                      // received bodies
    ) {
        use axum::routing::post;
        let (chunk_tx, chunk_rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
        let chunk_rx = Arc::new(tokio::sync::Mutex::new(Some(chunk_rx)));
        let received: Arc<Mutex<Vec<serde_json::Value>>> = Arc::default();
        let received_in = received.clone();
        let app = axum::Router::new().route(
            "/api/v1/chat/completions",
            post(move |body: axum::body::Bytes| {
                let chunk_rx = chunk_rx.clone();
                let received_in = received_in.clone();
                async move {
                    received_in
                        .lock()
                        .unwrap()
                        .push(serde_json::from_slice(&body).expect("json body"));
                    let rx = chunk_rx.lock().await.take().expect("one request per test");
                    let stream = tokio_stream::wrappers::UnboundedReceiverStream::new(rx)
                        .map(Ok::<_, std::convert::Infallible>);
                    axum::response::Response::builder()
                        .header("content-type", "text/event-stream")
                        .body(axum::body::Body::from_stream(stream))
                        .unwrap()
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}/api/v1", listener.local_addr().unwrap());
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (base, chunk_tx, received)
    }

    fn rig(
        base: &'static str,
        relay_url: Option<String>,
    ) -> (reqwest_middleware::ClientWithMiddleware, Arc<RecordingTaskStore>, Arc<PendingCostRecords>)
    {
        rig_owned(base, relay_url, weft_core::CredentialOwner::TheirOwn, None)
    }

    fn rig_owned(
        base: &'static str,
        relay_url: Option<String>,
        origin: weft_core::CredentialOwner,
        fixed_usd: Option<f64>,
    ) -> (reqwest_middleware::ClientWithMiddleware, Arc<RecordingTaskStore>, Arc<PendingCostRecords>)
    {
        let tasks = Arc::new(RecordingTaskStore::default());
        let pending = PendingCostRecords::new();
        let sink = CostSink {
            tasks: tasks.clone(),
            pending: pending.clone(),
            open_charges: OpenCharges::new(),
            project_id: "p1".into(),
            tenant_id: "t1".into(),
            color: uuid::Uuid::nil(),
            node_id: "node-x".into(),
            frames: LoopFrames::default(),
            service: "testprov".into(),
            origin,
        };
        // The follow-up client of the rig: same signed-in shape the
        // production composition builds (auth-free here; the test meter
        // makes no follow-up call).
        let follow_up = reqwest_middleware::ClientBuilder::new(reqwest::Client::new()).build();
        let middleware = MeteringMiddleware {
            meter: Some(Box::leak(Box::new(TestMeter { base, fixed_usd }))),
            relay_url,
            follow_up,
            sink: Arc::new(sink),
        };
        let client = reqwest_middleware::ClientBuilder::new(reqwest::Client::new())
            .with(middleware)
            .build();
        (client, tasks, pending)
    }

    pub(super) fn recorded_payloads(tasks: &RecordingTaskStore) -> Vec<weft_task_store::RecordCostPayload> {
        tasks
            .enqueued
            .lock()
            .unwrap()
            .iter()
            .map(|t| serde_json::from_value(t.payload.clone()).expect("record_cost payload"))
            .collect()
    }

    async fn wait_recorded(tasks: &RecordingTaskStore, pending: &PendingCostRecords) {
        pending.wait_zero().await;
        for _ in 0..100 {
            if !tasks.enqueued.lock().unwrap().is_empty() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    // L1-shaped stress pin for the pending counter's wakeup: `end()` firing
    // concurrently with `wait_zero` arming must never be missed (a lost
    // wakeup HANGS the await; the explicit timeout turns that hang into a
    // loud failure).
    weft_core::stress_test! {
        name: wait_zero_never_misses_a_concurrent_end,
        runs: 200,
        worker_threads: 4,
        async fn body() {
            // Hammered: the miss window is the instant between wait_zero's
            // count check and its park, so one begin/end pair rarely lands
            // in it; hundreds of pairs per iteration make a miss reliable.
            // Alternating record counts (1 and 3) also exercise the
            // notify-only-when-the-LAST-record-lands branch.
            for i in 0..400usize {
                let pending = PendingCostRecords::new();
                let records = if i % 2 == 0 { 1 } else { 3 };
                for _ in 0..records {
                    pending.begin();
                }
                let enders: Vec<_> = (0..records)
                    .map(|_| {
                        let ender = pending.clone();
                        tokio::spawn(async move { ender.end() })
                    })
                    .collect();
                tokio::time::timeout(std::time::Duration::from_secs(5), pending.wait_zero())
                    .await
                    .expect("wait_zero must observe the concurrent ends (a lost wakeup hangs)");
                assert_eq!(pending.count(), 0);
                for ender in enders {
                    ender.await.unwrap();
                }
            }
        }
    }

    /// L3, THE POINT OF THE FOLD: a SIGN-IN service with a registered
    /// meter. The one composed client applies the connection's auth
    /// steps (the wire really carries the header) AND measures the
    /// call, recording `origin: their-own, billed: false`. This case
    /// was unreachable before metering became credential-blind.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_signed_in_connection_is_measured_on_its_own_composed_client() {
        let (base, chunk_tx, received) = spawn_sse_server().await;
        let base: &'static str = Box::leak(base.into_boxed_str());
        // Register the test meter under a unique service name via the
        // production composition (not the hand-built rig).
        let meter: &'static TestMeter = Box::leak(Box::new(TestMeter { base, fixed_usd: None }));
        // `connection_client` looks meters up in the global registry;
        // inject through the same code path by registering.
        struct Registered;
        impl Registered {
            fn client(
                meter: &'static TestMeter,
                tasks: Arc<RecordingTaskStore>,
                pending: Arc<PendingCostRecords>,
            ) -> reqwest_middleware::ClientWithMiddleware {
                let steps = weft_core::access::client::resolve_steps(
                    &[weft_core::access::spec::AuthStep::Header {
                        name: "Authorization".into(),
                        value: weft_core::access::spec::Template::new("Bearer {token}"),
                    }],
                    &[("token".to_string(), "signed-in-token".to_string())]
                        .into_iter()
                        .collect(),
                )
                .unwrap();
                let sink = CostSink {
                    tasks,
                    pending,
                    open_charges: OpenCharges::new(),
                    project_id: "p1".into(),
                    tenant_id: "t1".into(),
                    color: uuid::Uuid::nil(),
                    node_id: "node-x".into(),
                    frames: LoopFrames::default(),
                    service: "testprov".into(),
                    origin: weft_core::CredentialOwner::TheirOwn,
                };
                // The exact production stack (metering outer, auth inner),
                // with the meter injected directly since the global
                // registry is keyed by the shipped services.
                let follow_up = reqwest_middleware::ClientBuilder::new(reqwest::Client::new())
                    .with(weft_core::access::client::AuthMiddleware::new(steps.clone()))
                    .build();
                reqwest_middleware::ClientBuilder::new(reqwest::Client::new())
                    .with(MeteringMiddleware {
                        meter: Some(meter),
                        relay_url: None,
                        follow_up,
                        sink: Arc::new(sink),
                    })
                    .with(weft_core::access::client::AuthMiddleware::new(steps))
                    .build()
            }
        }
        let tasks = Arc::new(RecordingTaskStore::default());
        let pending = PendingCostRecords::new();
        let client = Registered::client(meter, tasks.clone(), pending.clone());

        chunk_tx
            .send(Bytes::from("data: {\"usage\":{\"cost\":0.5}}\n\ndata: [DONE]\n\n"))
            .unwrap();
        drop(chunk_tx);
        let response = client
            .post(format!("{base}/chat/completions"))
            .json(&serde_json::json!({"model": "m", "messages": []}))
            .send()
            .await
            .expect("send");
        response.bytes().await.expect("body");

        wait_recorded(&tasks, &pending).await;
        let payloads = recorded_payloads(&tasks);
        assert_eq!(payloads.len(), 1, "the sign-in call was measured");
        assert_eq!(payloads[0].amount_usd, Some(0.5));
        assert!(!payloads[0].billed, "measured, never billed worker-side");
        assert_eq!(payloads[0].origin, weft_core::CredentialOwner::TheirOwn);
        // The wire really carried the auth step: the request reached
        // the server (it answered), and the recorded body proves the
        // prepared rewrite went out on the SAME request.
        assert_eq!(received.lock().unwrap()[0]["usage"]["include"], true);
    }

    /// A FIXED-price route still resolves THROUGH its meter (whether a
    /// fixed-priced call was charged is provider knowledge): the record
    /// carries the declared price and names the fixed resolution.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_fixed_route_books_its_declared_price_through_resolve() {
        let (base, chunk_tx, _received) = spawn_sse_server().await;
        let base: &'static str = Box::leak(base.into_boxed_str());
        let (client, tasks, pending) =
            rig_owned(base, None, weft_core::CredentialOwner::TheirOwn, Some(0.001));

        chunk_tx.send(Bytes::from("data: [DONE]\n\n")).unwrap();
        drop(chunk_tx);
        let response = client
            .post(format!("{base}/chat/completions"))
            .json(&serde_json::json!({"model": "m", "messages": []}))
            .send()
            .await
            .expect("send");
        response.bytes().await.expect("body");

        wait_recorded(&tasks, &pending).await;
        let payloads = recorded_payloads(&tasks);
        assert_eq!(payloads.len(), 1, "the fixed call was booked");
        assert_eq!(payloads[0].amount_usd, Some(0.001));
        assert_eq!(payloads[0].metadata["resolution"], "fixed route price");
    }

    /// A service with NO registered meter records nothing: the client
    /// signs in and passes through unmeasured.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_meterless_service_records_nothing() {
        let (base, chunk_tx, received) = spawn_sse_server().await;
        let steps = weft_core::access::client::resolve_steps(
            &[weft_core::access::spec::AuthStep::Header {
                name: "Authorization".into(),
                value: weft_core::access::spec::Template::new("Bearer {token}"),
            }],
            &[("token".to_string(), "tok-1".to_string())].into_iter().collect(),
        )
        .unwrap();
        let tasks = Arc::new(RecordingTaskStore::default());
        let pending = PendingCostRecords::new();
        let sink = CostSink {
            tasks: tasks.clone(),
            pending: pending.clone(),
            open_charges: OpenCharges::new(),
            project_id: "p1".into(),
            tenant_id: "t1".into(),
            color: uuid::Uuid::nil(),
            node_id: "node-x".into(),
            frames: LoopFrames::default(),
            service: "no_such_meterless_service".into(),
            origin: weft_core::CredentialOwner::TheirOwn,
        };
        let client =
            connection_client("no_such_meterless_service", steps, None, Arc::new(sink))
                .expect("builds");

        chunk_tx.send(Bytes::from("data: [DONE]\n\n")).unwrap();
        drop(chunk_tx);
        let response = client
            .post(format!("{base}/chat/completions"))
            .json(&serde_json::json!({"model": "m"}))
            .send()
            .await
            .expect("send");
        response.bytes().await.expect("body");
        pending.wait_zero().await;
        assert!(tasks.enqueued.lock().unwrap().is_empty(), "no meter = no record");
        // The auth step still applied.
        assert!(received.lock().unwrap()[0].get("usage").is_none(), "no meter = no prepare");

        // And a RELAYED connection without a meter is refused at build.
        let steps2 = Vec::new();
        let sink2 = CostSink {
            tasks: tasks.clone(),
            pending: pending.clone(),
            open_charges: OpenCharges::new(),
            project_id: "p1".into(),
            tenant_id: "t1".into(),
            color: uuid::Uuid::nil(),
            node_id: "node-x".into(),
            frames: LoopFrames::default(),
            service: "no_such_meterless_service".into(),
            origin: weft_core::CredentialOwner::Ours,
        };
        let err = connection_client(
            "no_such_meterless_service",
            steps2,
            Some("http://relay"),
            Arc::new(sink2),
        )
        .unwrap_err();
        assert!(err.to_string().contains("no meter is registered"), "{err}");
    }

    /// L3, the streaming tap: the caller receives chunk N in REAL TIME
    /// (while the server still holds the rest of the stream back), the
    /// request was prepared on the wire, and once the stream ends the
    /// meter's figure lands as a durable record, `billed: false`, attributed
    /// to the firing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_tap_forwards_chunks_in_real_time_and_records_the_measured_cost() {
        let (base, chunk_tx, received) = spawn_sse_server().await;
        let base: &'static str = Box::leak(base.into_boxed_str());
        let (client, tasks, pending) = rig(base, None);

        let response = client
            .post(format!("{base}/chat/completions"))
            .json(&serde_json::json!({"model": "m", "messages": []}))
            .send()
            .await
            .expect("send");
        // `prepare` rewrote the outgoing body: the accounting opt-in is on
        // the wire even though the caller never asked for it.
        assert_eq!(received.lock().unwrap()[0]["usage"]["include"], true);

        // Release ONE chunk; the caller must see it while the stream is
        // still open and the final (usage) chunk is still held back. That
        // is the not-buffered property: a buffering tap would block here
        // forever waiting for the end of the stream.
        chunk_tx.send(Bytes::from("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n")).unwrap();
        let mut stream = response.bytes_stream();
        let first = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
            .await
            .expect("chunk 1 must arrive while the stream is still open")
            .expect("stream open")
            .expect("chunk ok");
        assert!(std::str::from_utf8(&first).unwrap().contains("hi"));
        assert!(tasks.enqueued.lock().unwrap().is_empty(), "nothing recorded mid-stream");

        // Now the trailing usage chunk + end of stream.
        chunk_tx.send(Bytes::from("data: {\"usage\":{\"cost\":0.000031}}\n\ndata: [DONE]\n\n")).unwrap();
        drop(chunk_tx);
        while let Some(chunk) = stream.next().await {
            chunk.expect("chunk ok");
        }

        wait_recorded(&tasks, &pending).await;
        let payloads = recorded_payloads(&tasks);
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].amount_usd, Some(0.000031));
        assert!(!payloads[0].billed, "a worker-side figure is a measurement, never a charge");
        assert_eq!(payloads[0].node_id, "node-x");
        assert_eq!(payloads[0].service, "testprov");
        assert_eq!(pending.count(), 0);
    }

    /// L3, interruption: the caller DROPS the response mid-stream (a
    /// cancelled node). The tap still finalizes, and a cost the meter
    /// cannot resolve is recorded as an honest UNKNOWN, never as $0.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dropped_stream_still_records_and_an_unresolvable_cost_is_unknown_not_zero() {
        let (base, chunk_tx, _received) = spawn_sse_server().await;
        let base: &'static str = Box::leak(base.into_boxed_str());
        let (client, tasks, pending) = rig(base, None);

        let response = client
            .post(format!("{base}/chat/completions"))
            .json(&serde_json::json!({"model": "m", "messages": []}))
            .send()
            .await
            .expect("send");
        chunk_tx.send(Bytes::from("data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")).unwrap();
        let mut stream = response.bytes_stream();
        stream.next().await.expect("stream open").expect("chunk ok");
        // Hang up before the usage chunk ever arrives.
        drop(stream);

        wait_recorded(&tasks, &pending).await;
        let payloads = recorded_payloads(&tasks);
        assert_eq!(payloads.len(), 1, "an interrupted call still gets its record");
        assert_eq!(
            payloads[0].amount_usd, None,
            "an unresolvable cost is recorded as unknown, never booked as $0"
        );
        assert_eq!(payloads[0].metadata["interrupted"], true);
    }

    /// The runtime-credential allowlist: on origin Ours, an UNKNOWN
    /// route (or a URL off the provider's API) is refused loudly and
    /// nothing is sent, an explicitly FREE route passes; the same
    /// unknown route on the user's own key passes through unmeasured.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_runtime_credential_only_travels_on_declared_routes() {
        let (base, _chunk_tx, received) = spawn_sse_server().await;
        let leaked: &'static str = Box::leak(base.clone().into_boxed_str());
        let (ours, tasks, _pending) =
            rig_owned(leaked, None, weft_core::CredentialOwner::Ours, None);

        // Unknown route: refused, the server never sees it.
        let err = ours
            .post(format!("{base}/mystery"))
            .body("x")
            .send()
            .await
            .expect_err("an unknown route must refuse on a runtime credential");
        assert!(err.to_string().contains("not a route"), "{err}");
        // Off the provider's API entirely: refused too.
        let err = ours
            .post("https://elsewhere.invalid/steal")
            .send()
            .await
            .expect_err("an off-API URL must refuse on a runtime credential");
        assert!(err.to_string().contains("only travels"), "{err}");
        assert!(received.lock().unwrap().is_empty(), "nothing reached the server");
        assert!(tasks.enqueued.lock().unwrap().is_empty());

        // An explicitly FREE route passes: the middleware lets it out
        // (a refusal would surface as the send() erroring, exactly as
        // above; the test server's answer for the path is irrelevant).
        ours.get(format!("{base}/models")).send().await.expect("free route passes");

        // The SAME unknown route on the user's own key passes through.
        let (theirs, _tasks, _pending) = rig(leaked, None);
        theirs
            .post(format!("{base}/mystery"))
            .body("x")
            .send()
            .await
            .expect("an unknown route on the user's own key passes through");
    }

    /// L3, the relay lane: a call on a relayed access is REWRITTEN to the
    /// relay (path + query preserved) and NOT measured here (the relay is
    /// where the runtime measures). A call outside the provider's API
    /// cannot be relayed and fails loud.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_relayed_call_is_rewritten_to_the_relay_and_not_measured_here() {
        // The "relay" is just the same SSE server; what matters is WHERE
        // the request lands and that nothing is recorded on this side.
        let (relay_base, chunk_tx, received) = spawn_sse_server().await;
        // The provider's own base is somewhere the test never serves: if
        // the rewrite failed, the request would go there and error.
        let provider_base: &'static str = "https://provider.invalid/api/v1";
        let (client, tasks, pending) = rig(provider_base, Some(relay_base.clone()));

        chunk_tx.send(Bytes::from("data: [DONE]\n\n")).unwrap();
        drop(chunk_tx);
        let response = client
            .post(format!("{provider_base}/chat/completions?stream=true"))
            .json(&serde_json::json!({"model": "m"}))
            .send()
            .await
            .expect("send lands on the relay");
        assert!(response.status().is_success());
        assert_eq!(received.lock().unwrap().len(), 1, "the relay received the call");
        // Not prepared and not measured here: the relay does both.
        assert!(received.lock().unwrap()[0].get("usage").is_none());
        response.bytes().await.expect("body");
        pending.wait_zero().await;
        assert!(tasks.enqueued.lock().unwrap().is_empty(), "no record on the relayed lane");

        // Outside the provider's API: refused loud, nothing sent.
        let err = client
            .post("https://elsewhere.invalid/steal")
            .send()
            .await
            .expect_err("a non-provider URL cannot be relayed");
        assert!(err.to_string().contains("cannot be relayed"), "{err}");
    }

    /// L3, the redirect policy: STANDARD library handling, nothing
    /// custom. A redirect is followed (this is what un-broke
    /// redirect-serving endpoints like Google's CSV export), and the
    /// request's headers ride along per ordinary HTTP-client
    /// convention: a custom auth header reaches the hop's origin. The
    /// only convention exception is the library's own (the well-known
    /// `Authorization`/cookie headers drop when the host changes),
    /// which is every HTTP tool's behavior, not a weft rule.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_redirect_hop_carries_the_connections_auth_to_the_new_origin() {
        use axum::routing::get;

        // Target server (origin B): records the auth header it saw.
        let seen: Arc<Mutex<Option<String>>> = Arc::default();
        let seen_in = seen.clone();
        let target_app = axum::Router::new().route(
            "/landed",
            get(move |headers: axum::http::HeaderMap| {
                let seen_in = seen_in.clone();
                async move {
                    *seen_in.lock().unwrap() = headers
                        .get("x-custom-token")
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_string);
                    "ok"
                }
            }),
        );
        let target = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_url = format!("http://{}/landed", target.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(target, target_app).await.unwrap() });

        // Source server (origin A): answers 307 to origin B.
        let redirect_to = target_url.clone();
        let source_app = axum::Router::new().route(
            "/start",
            get(move || {
                let redirect_to = redirect_to.clone();
                async move {
                    axum::response::Response::builder()
                        .status(307)
                        .header("location", redirect_to)
                        .body(axum::body::Body::empty())
                        .unwrap()
                }
            }),
        );
        let source = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let source_url = format!("http://{}/start", source.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(source, source_app).await.unwrap() });

        // A custom auth header, forwarded across the hop per standard
        // client convention (only the well-known Authorization/cookie
        // headers are host-scoped).
        let steps = weft_core::access::client::resolve_steps(
            &[weft_core::access::spec::AuthStep::Header {
                name: "x-custom-token".into(),
                value: weft_core::access::spec::Template::new("tok-{token}"),
            }],
            &[("token".to_string(), "42".to_string())].into_iter().collect(),
        )
        .unwrap();
        let client = weft_core::access::client::authed_client(steps);

        let resp = client.get(&source_url).send().await.expect("follow the hop");
        assert!(resp.status().is_success());
        assert_eq!(
            seen.lock().unwrap().as_deref(),
            Some("tok-42"),
            "the hop re-applied the connection's auth on the new origin"
        );
    }

    /// The follow-up client never leaves its origin: a redirect answer
    /// comes back as a status, not a followed hop (the client carries
    /// the connection's auth middleware, which would re-apply auth on
    /// the hop's target).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_follow_up_client_does_not_follow_redirects() {
        use axum::routing::get;

        let landed: Arc<Mutex<bool>> = Arc::default();
        let landed_in = landed.clone();
        let target_app = axum::Router::new().route(
            "/landed",
            get(move || {
                let landed_in = landed_in.clone();
                async move {
                    *landed_in.lock().unwrap() = true;
                    "ok"
                }
            }),
        );
        let target = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_url = format!("http://{}/landed", target.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(target, target_app).await.unwrap() });

        let redirect_to = target_url.clone();
        let source_app = axum::Router::new().route(
            "/generation",
            get(move || {
                let redirect_to = redirect_to.clone();
                async move {
                    axum::response::Response::builder()
                        .status(307)
                        .header("location", redirect_to)
                        .body(axum::body::Body::empty())
                        .unwrap()
                }
            }),
        );
        let source = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let source_url = format!("http://{}/generation", source.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(source, source_app).await.unwrap() });

        let resp = follow_up_client().get(&source_url).send().await.expect("answers");
        assert_eq!(resp.status(), 307, "the redirect is an answer, not a hop");
        assert!(!*landed.lock().unwrap(), "the hop's target was never contacted");
    }
}

/// The pairing between a call that spends and the later response that
/// says what it cost. These pin the harness's half of it: the meter is a
/// stand-in for any queue-style provider, so what is tested here is the
/// language feature, not fal.
#[cfg(test)]
mod open_charge_tests {
    use super::tests::{recorded_payloads, RecordingTaskStore};
    use super::*;
    use weft_providers::{ObservedCall, Pricing};

    /// A provider shaped like fal's queue: the submit answers a ticket
    /// and no figure, and a later read states what the job came to.
    struct QueuedMeter;

    #[async_trait::async_trait]
    impl ProviderMeter for QueuedMeter {
        fn service(&self) -> &'static str {
            "queued"
        }
        fn base_url(&self) -> &'static str {
            "https://queued.example"
        }
        fn classify(&self, method: &str, _path: &str) -> RouteClass {
            match method {
                "POST" => RouteClass::Billable(Pricing::Metered),
                _ => RouteClass::Reports,
            }
        }
        fn prepare(&self, _p: &str, _b: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
            Ok(None)
        }
        fn observe(&self, _p: &str, _q: &str, _b: &[u8]) -> Box<dyn CallObservation> {
            unreachable!("these tests drive the charge pairing directly")
        }
        fn opens_charge(&self, _path: &str, observed: &ObservedCall) -> Option<String> {
            observed.data["requestId"].as_str().map(str::to_string)
        }
        fn charge_reported_on(&self, path: &str, _o: &ObservedCall) -> Option<String> {
            path.strip_prefix("requests/").map(str::to_string)
        }
        async fn fold_report(
            &self,
            _path: &str,
            observed: ObservedCall,
            scratch: &mut serde_json::Value,
            _f: FollowUp<'_>,
        ) -> Option<MeasuredCost> {
            // A provider may await a price lookup while another report or
            // execution close arrives. Exercise that scheduling boundary.
            tokio::task::yield_now().await;
            let units = observed.data["units"].as_f64()?;
            scratch["units"] = serde_json::json!(units);
            Some(MeasuredCost {
                amount_usd: Some(units * 2.0),
                model: scratch["model"].as_str().map(str::to_string),
                metadata: scratch.clone(),
            })
        }
        async fn resolve(
            &self,
            _p: &str,
            _o: ObservedCall,
            _f: FollowUp<'_>,
        ) -> MeasuredCost {
            unreachable!("a queued submit opens a charge instead of resolving")
        }
    }

    static QUEUED: QueuedMeter = QueuedMeter;

    fn sink(tasks: Arc<RecordingTaskStore>, pending: Arc<PendingCostRecords>) -> Arc<CostSink> {
        Arc::new(CostSink {
            tasks,
            pending,
            open_charges: OpenCharges::new(),
            project_id: "p1".into(),
            tenant_id: "t1".into(),
            color: uuid::Uuid::nil(),
            node_id: "node-x".into(),
            frames: LoopFrames::default(),
            service: "queued".into(),
            origin: weft_core::CredentialOwner::TheirOwn,
        })
    }

    /// A sink for one execution, sharing the pod's charge map.
    fn sink_on(
        tasks: Arc<RecordingTaskStore>,
        pending: Arc<PendingCostRecords>,
        open_charges: Arc<OpenCharges>,
        color: weft_core::Color,
    ) -> Arc<CostSink> {
        Arc::new(CostSink {
            tasks,
            pending,
            open_charges,
            project_id: "p1".into(),
            tenant_id: "t1".into(),
            color,
            node_id: "node-x".into(),
            frames: LoopFrames::default(),
            service: "queued".into(),
            origin: weft_core::CredentialOwner::TheirOwn,
        })
    }

    fn submitted(id: &str) -> ObservedCall {
        ObservedCall {
            interrupted: false,
            status: 200,
            data: serde_json::json!({ "requestId": id, "model": "m1" }),
        }
    }

    fn http() -> reqwest_middleware::ClientWithMiddleware {
        reqwest_middleware::ClientBuilder::new(reqwest::Client::new()).build()
    }

    /// The whole point: the submit books nothing (its amount does not
    /// exist yet), and the later read is what puts the figure on the
    /// trail, against the node that submitted.
    #[tokio::test]
    async fn the_read_that_reports_is_what_books_the_submit() {
        let tasks = Arc::new(RecordingTaskStore::default());
        let pending = PendingCostRecords::new();
        let sink = sink(tasks.clone(), pending.clone());
        let charges = sink.open_charges.clone();

        charges.open(QUEUED.service(), "req-1".into(), OpenCharge { token: 0, scratch: submitted("req-1").data, sink });
        assert_eq!(charges.count(), 1, "the charge is held between spend and figure");
        assert!(recorded_payloads(&tasks).is_empty(), "nothing is booked yet");

        let report = ObservedCall {
            interrupted: false,
            status: 200,
            data: serde_json::json!({ "units": 3.0 }),
        };
        charges.report(&QUEUED, "req-1", "requests/req-1", report, &http()).await;
        pending.wait_zero().await;

        assert_eq!(charges.count(), 0);
        let booked = recorded_payloads(&tasks);
        assert_eq!(booked.len(), 1);
        assert_eq!(booked[0].amount_usd, Some(6.0));
        assert_eq!(booked[0].node_id, "node-x");
        assert_eq!(booked[0].model.as_deref(), Some("m1"));
    }

    /// A report that does not finish the picture leaves the charge open,
    /// so a provider that answers across several reads is not booked off
    /// the first one.
    #[tokio::test]
    async fn an_inconclusive_report_leaves_the_charge_open() {
        let tasks = Arc::new(RecordingTaskStore::default());
        let pending = PendingCostRecords::new();
        let sink = sink(tasks.clone(), pending.clone());
        let charges = sink.open_charges.clone();
        charges.open(QUEUED.service(), "req-1".into(), OpenCharge { token: 0, scratch: submitted("req-1").data, sink });

        // No `units`: the job is still running.
        let pending_report = ObservedCall {
            interrupted: false,
            status: 200,
            data: serde_json::json!({ "status": "IN_PROGRESS" }),
        };
        charges.report(&QUEUED, "req-1", "requests/req-1", pending_report, &http()).await;

        assert_eq!(charges.count(), 1, "still waiting for the figure");
        assert!(recorded_payloads(&tasks).is_empty());
    }

    // Both responses arrive before shutdown; processing may interleave
    // with receipt and close. Neither the final figure nor the charge
    // can be lost when the execution closes immediately afterward.
    weft_core::stress_test! {
      name: received_reports_finish_before_execution_close,
      runs: 20,
      worker_threads: 4,
      async fn body() {
        let tasks = Arc::new(RecordingTaskStore::default());
        let charges = OpenCharges::new();
        let pending = PendingCostRecords::new();
        let color = uuid::Uuid::new_v4();
        let sink = sink_on(tasks.clone(), pending.clone(), charges.clone(), color);
        charges.open(QUEUED.service(), "req-1".into(), OpenCharge { token: 0, scratch: submitted("req-1").data, sink });

        drop(charges.report(&QUEUED, "req-1", "requests/req-1", ObservedCall {
            interrupted: false, status: 200, data: serde_json::json!({"status": "IN_PROGRESS"}),
        }, &http()));
        drop(charges.report(&QUEUED, "req-1", "requests/req-1", ObservedCall {
            interrupted: false, status: 200, data: serde_json::json!({"units": 3.0}),
        }, &http()));
        charges.flush_color(color, "the execution ended before the job was read back");
        pending.wait_zero().await;

        assert_eq!(charges.count(), 0);
        let booked = recorded_payloads(&tasks);
        assert_eq!(booked.len(), 1, "the mid-fold charge was written down");
        assert_eq!(booked[0].amount_usd, Some(6.0), "the final received figure wins before close");
      }
    }

    /// A second call claims the id while the first's report is being read:
    /// `open` books the first as unknown, and the figure the read still
    /// produces lands beside it, with every record written before the
    /// pending count reaches zero.
    // Current-thread on purpose: the one `yield_now` below runs the spawned
    // read exactly up to the meter's own await, which puts the displacing
    // `open` inside the fold window. A multi-thread runtime could let the
    // open win the race to the map first, which is the (logged) other case.
    #[tokio::test]
    async fn a_charge_displaced_mid_read_keeps_its_figure_on_the_trail() {
        let tasks = Arc::new(RecordingTaskStore::default());
        let charges = OpenCharges::new();
        let pending = PendingCostRecords::new();
        let color = weft_core::Color::new_v4();
        let sink = sink_on(tasks.clone(), pending.clone(), charges.clone(), color);
        charges.open(QUEUED.service(), "req-1".into(), OpenCharge { token: 0, scratch: submitted("req-1").data, sink: sink.clone() });
        // The fake meter yields once inside `fold_report`; the displacing
        // open lands in that window.
        let read = charges.report(&QUEUED, "req-1", "requests/req-1", ObservedCall {
            interrupted: false, status: 200, data: serde_json::json!({"units": 3.0}),
        }, &http());
        // Let the spawned read reach the meter's own await before displacing.
        tokio::task::yield_now().await;
        charges.open(QUEUED.service(), "req-1".into(), OpenCharge { token: 0, scratch: submitted("req-1").data, sink });
        read.await;
        charges.flush_color(color, "the execution ended");
        pending.wait_zero().await;
        assert_eq!(charges.count(), 0);
        let mut amounts: Vec<Option<f64>> = recorded_payloads(&tasks).iter().map(|record| record.amount_usd).collect();
        amounts.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(amounts, vec![None, None, Some(6.0)],
            "the displaced charge as unknown, the second charge as unknown at close, and the figure the read produced");
    }

    /// A figure that arrives after its execution closed the charge has
    /// nowhere to go: the charge is on the trail as unknown, exactly
    /// once, and the late report neither books again nor panics.
    #[tokio::test]
    async fn a_report_after_the_execution_closed_leaves_one_unknown_record() {
        let tasks = Arc::new(RecordingTaskStore::default());
        let charges = OpenCharges::new();
        let pending = PendingCostRecords::new();
        let color = weft_core::Color::new_v4();
        let sink = sink_on(tasks.clone(), pending.clone(), charges.clone(), color);
        charges.open(QUEUED.service(), "req-1".into(), OpenCharge { token: 0, scratch: submitted("req-1").data, sink });
        charges.flush_color(color, "the execution ended before the job was read back");
        charges.report(&QUEUED, "req-1", "requests/req-1", ObservedCall {
            interrupted: false, status: 200, data: serde_json::json!({"units": 3.0}),
        }, &http()).await;
        pending.wait_zero().await;
        assert_eq!(charges.count(), 0);
        let booked = recorded_payloads(&tasks);
        assert_eq!(booked.len(), 1);
        assert!(booked[0].amount_usd.is_none(), "spend with no figure is unknown, never zero");
    }

    #[tokio::test]
    async fn an_executions_charges_close_with_the_execution() {
        let tasks = Arc::new(RecordingTaskStore::default());
        let charges = OpenCharges::new();

        let mine_pending = PendingCostRecords::new();
        let color = uuid::Uuid::new_v4();
        let mine = sink_on(tasks.clone(), mine_pending.clone(), charges.clone(), color);
        charges.open(QUEUED.service(), "req-1".into(), OpenCharge { token: 0, scratch: submitted("req-1").data, sink: mine });

        // Another execution on the same pod, still going. Its own
        // pending tracker, so waiting for this execution's records does
        // not wait on a charge that is meant to stay open.
        let other = sink_on(tasks.clone(), PendingCostRecords::new(), charges.clone(), uuid::Uuid::new_v4());
        charges.open(QUEUED.service(), "req-2".into(), OpenCharge { token: 0, scratch: submitted("req-2").data, sink: other });

        charges.flush_color(color, "the execution ended before the job was read back");
        mine_pending.wait_zero().await;

        assert_eq!(charges.count(), 1, "the other execution's charge is untouched");
        let booked = recorded_payloads(&tasks);
        assert_eq!(booked.len(), 1);
        assert!(booked[0].amount_usd.is_none(), "spend with no figure is unknown, never zero");
    }

    /// Two SERVICES using the same job id are two charges.
    ///
    /// The id is a string a meter picks, and the shipped example meters
    /// mint `job-1`, so a pod running two providers at once had one
    /// displace the other: the loser was booked as an unknown spend it
    /// was not, and the winner's report priced it through the loser's
    /// sink, putting a real figure on another execution's trail.
    #[tokio::test]
    async fn one_job_id_under_two_services_is_two_charges() {
        let tasks = Arc::new(RecordingTaskStore::default());
        let charges = OpenCharges::new();
        let pending = PendingCostRecords::new();
        let color = uuid::Uuid::new_v4();

        let first = sink_on(tasks.clone(), pending.clone(), charges.clone(), color);
        charges.open("queued", "job-1".into(), OpenCharge { token: 0, scratch: submitted("job-1").data, sink: first });
        let second = sink_on(tasks.clone(), pending.clone(), charges.clone(), color);
        charges.open("otherprov", "job-1".into(), OpenCharge { token: 0, scratch: submitted("job-1").data, sink: second });

        assert_eq!(charges.count(), 2, "neither displaced the other");
        assert!(recorded_payloads(&tasks).is_empty(), "nothing was booked as displaced");

        charges.flush_color(color, "the execution ended");
        pending.wait_zero().await;
        assert_eq!(recorded_payloads(&tasks).len(), 2, "both spends are written down");
    }

    /// A job submitted and never read back still spent money. The flush
    /// is what stops that becoming a spend with no row at all.
    #[tokio::test]
    async fn a_charge_nobody_ever_reported_on_is_booked_as_unknown() {
        let tasks = Arc::new(RecordingTaskStore::default());
        let pending = PendingCostRecords::new();
        let sink = sink(tasks.clone(), pending.clone());
        let charges = sink.open_charges.clone();
        charges.open(QUEUED.service(), "req-1".into(), OpenCharge { token: 0, scratch: submitted("req-1").data, sink });

        charges.flush("the pod shut down");
        pending.wait_zero().await;

        let booked = recorded_payloads(&tasks);
        assert_eq!(booked.len(), 1);
        assert_eq!(booked[0].amount_usd, None, "recorded AS unknown, never as zero");
        assert_eq!(booked[0].model.as_deref(), Some("m1"));
        assert!(
            booked[0].metadata["resolution"].as_str().unwrap().contains("the pod shut down"),
            "the trail says why it has no figure"
        );
    }

    /// A call the meter could never price is refused before it is sent.
    /// This is the one moment the spend can still be prevented for free:
    /// afterwards the money is gone and the trail can only say so.
    #[tokio::test]
    async fn a_call_that_could_never_be_priced_is_refused_before_it_is_sent() {
        struct Unpriceable;
        #[async_trait::async_trait]
        impl ProviderMeter for Unpriceable {
            fn service(&self) -> &'static str {
                "unpriceable"
            }
            fn base_url(&self) -> &'static str {
                "https://unpriceable.example"
            }
            fn classify(&self, _m: &str, _p: &str) -> RouteClass {
                RouteClass::Billable(Pricing::Metered)
            }
            fn prepare(&self, _p: &str, _b: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
                Ok(None)
            }
            fn observe(&self, _p: &str, _q: &str, _b: &[u8]) -> Box<dyn CallObservation> {
                unreachable!("the call must never be sent")
            }
            async fn priceable(&self, path: &str, _f: FollowUp<'_>) -> anyhow::Result<()> {
                anyhow::bail!("no price is published for '{path}'")
            }
            async fn resolve(
                &self,
                _p: &str,
                _o: ObservedCall,
                _f: FollowUp<'_>,
            ) -> MeasuredCost {
                unreachable!("the call must never be sent")
            }
        }
        static UNPRICEABLE: Unpriceable = Unpriceable;

        let tasks = Arc::new(RecordingTaskStore::default());
        let pending = PendingCostRecords::new();
        let sink = sink(tasks.clone(), pending.clone());
        let client = reqwest_middleware::ClientBuilder::new(reqwest::Client::new())
            .with(MeteringMiddleware {
                meter: Some(&UNPRICEABLE),
                relay_url: None,
                follow_up: http(),
                sink,
            })
            .build();

        let err = client
            .post("https://unpriceable.example/generate")
            .json(&serde_json::json!({}))
            .send()
            .await
            .expect_err("an unpriceable billable call must be refused");
        let text = err.to_string();
        assert!(text.contains("no price is published"), "the refusal says why: {text}");
        assert!(recorded_payloads(&tasks).is_empty(), "nothing was spent, so nothing is booked");
    }

    /// A read naming no open charge books nothing: a node may re-read a
    /// finished job as often as it likes without spending twice.
    #[tokio::test]
    async fn re_reading_a_finished_job_books_nothing_further() {
        let tasks = Arc::new(RecordingTaskStore::default());
        let pending = PendingCostRecords::new();
        let sink = sink(tasks.clone(), pending.clone());
        let charges = sink.open_charges.clone();

        let report = ObservedCall {
            interrupted: false,
            status: 200,
            data: serde_json::json!({ "units": 3.0 }),
        };
        charges.report(&QUEUED, "req-unknown", "requests/req-unknown", report, &http()).await;
        pending.wait_zero().await;

        assert!(recorded_payloads(&tasks).is_empty());
    }
}
