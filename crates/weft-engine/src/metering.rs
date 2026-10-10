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
//!      figure and writes it on the run's record (`CostReported`).
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
//! it writes is pinned `billed: false`.
//!
//! The resolve + record run detached from the node's future (the call may
//! be cut by a cancel), tracked per run by [`PendingCostRecords`]: a run
//! waits for its spend to be written down before its ending is, so the
//! figure is on its record, before its ending. The token that keeps the
//! ending waiting is taken when the call goes out, not when its response
//! ends: a response body can outlive the node that asked for it (a client
//! library reading the stream on a task of its own finishes the body after
//! the node already returned), and a token taken only then would arrive
//! after the run had ended.
//!
//! That same library task is why aborting the node does not end a call:
//! the body lives on the library's task, not in the node's future. So the
//! tap itself stops reading a call's answer when the provider sends
//! nothing for [`ANSWER_SILENCE_LIMIT`] while it is waited on, or when the
//! call's run is cancelled (the run's own cancel flag, on the
//! [`CostSink`]). The reader then gets an error, which ends the library's
//! task; the call is booked as cut, naming why; and its token goes, so the
//! run's ending never waits on a provider that stays connected and silent.
//! A call the run's cancel catches before its answer even started is let
//! go the same way.

use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{FutureExt, Stream, StreamExt};

use weft_core::error::{WeftError, WeftResult};
use weft_core::frames::LoopFrames;
use weft_core::ExecutionId;
use weft_providers::{
    CallObservation, FollowUp, MeasuredCost, ObservedCall, ProviderMeter, RouteClass,
};

// ---------- Pending-record tracking ----------

/// Counts one run's spend not yet on its record, so the run's ending
/// waits while a call's money is still being written down. A token is
/// taken when a metered call goes out and travels with it: the response's
/// tap carries it, then the open charge or the figure being written, and
/// it is released once the figure is on the run's record (or loudly
/// failed).
pub struct PendingCostRecords {
    inner: Arc<weft_core::in_flight::InFlight>,
}

impl PendingCostRecords {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { inner: weft_core::in_flight::InFlight::new("cost record") })
    }

    #[cfg(test)]
    pub fn count(&self) -> usize {
        self.inner.count()
    }

    /// Take a token for spend not on record yet; it is released when it
    /// drops.
    pub(crate) fn hold(&self) -> weft_core::in_flight::InFlightToken {
        self.inner.token()
    }

    /// Resolve once no cost records are in flight.
    ///
    /// Every resolve is internally bounded (the follow-up client has a
    /// request timeout, the ledger poll a fixed budget). An OPEN charge
    /// holds a token until its run closes it
    /// (`OpenCharges::close_execution_id`, which a run ending calls, and
    /// which waits here); a report being read holds one of its own until the
    /// read returns (bounded the same way); and each figure being written
    /// holds one until it is on record.
    pub async fn wait_zero(&self) {
        self.inner.wait_zero().await;
    }
}

// ---------- Charges awaiting the response that states their amount ----------

/// What tells one open charge from every other the process is holding: the
/// METER that opened it, and the id that meter chose.
///
/// The id alone is not enough. One process holds the charges of every
/// service, every connection and every execution it is running, and the
/// id is a string a meter picks: `job-1` from one provider's example
/// meter collides with `job-1` from another's, and the collision was
/// resolved by booking one of the two as an unknown spend and letting
/// the survivor's report price it through the OTHER charge's sink, so a
/// real figure landed on the wrong execution's trail.
///
/// The service closes the collision BETWEEN meters, and that is all it
/// closes. Two executions on one process whose ids come from the same meter
/// can still collide, because `report` is handed nothing but the meter
/// and the id (the response is the only thing that arrives, and it says
/// nothing about which run asked), so there is no execution to key on at
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
    /// The billable call's own pending token, carried on until the charge
    /// is booked, so its run's ending waits for it.
    held: weft_core::in_flight::InFlightToken,
}

/// One owner per charge, with reports queued in arrival order. A closing
/// execution drains received reports before declaring the amount unknown.
pub struct OpenCharges {
    next_token: std::sync::atomic::AtomicU64,
    book: std::sync::Mutex<Book>,
}

/// The charges held open, and the runs whose ending already closed theirs.
/// One lock over both, so a charge cannot slip in between a run's close
/// and the mark that refuses it.
#[derive(Default)]
struct Book {
    by_id: std::collections::HashMap<ChargeKey, ChargeState>,
    /// Runs being ended (`OpenCharges::close_execution_id`). A response
    /// body can finish after its run's charges were closed (a client
    /// library reading the stream on a task of its own), and a charge it
    /// opened then would hold the run's token with nothing left to close
    /// it, so the ending would wait forever. Such a charge is booked as
    /// unknown at once instead.
    ending: std::collections::HashSet<ExecutionId>,
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
    follow_up: FollowUpLane,
    done: tokio::sync::oneshot::Sender<()>,
}

impl OpenCharges {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            next_token: std::sync::atomic::AtomicU64::new(0),
            book: std::sync::Mutex::new(Book::default()),
        })
    }

    fn book(&self) -> std::sync::MutexGuard<'_, Book> {
        self.book.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn count(&self) -> usize { self.book().by_id.len() }

    fn open(&self, service: &'static str, id: String, mut charge: OpenCharge) {
        let id = ChargeKey { service, id };
        charge.token = self.next_token.fetch_add(1, Ordering::SeqCst);
        let replaced = {
            let mut book = self.book();
            if book.ending.contains(&charge.sink.execution_id) {
                drop(book);
                tracing::warn!(target: "weft_engine::metering", charge = %id,
                    "a call opened a charge after its run began ending; booking it as unknown");
                book_open_charge(charge, "the call's response finished after its run began ending");
                return;
            }
            book.by_id.insert(id.clone(), ChargeState {
                charge, reports: Default::default(), processing: false, closing: None,
            })
        };
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
        follow_up: &FollowUpLane,
    ) -> impl std::future::Future<Output = ()> + use<> {
        let id = ChargeKey { service: meter.service(), id: reported.to_string() };
        let (done, received) = tokio::sync::oneshot::channel();
        let (known, start) = {
            let mut book = self.book();
            match book.by_id.get_mut(&id) {
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
                        let mut book = self.book();
                        if book.by_id.get(&id).is_some_and(|state| state.charge.token == token) {
                            book.by_id.remove(&id)
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
                let mut book = self.book();
                let charges = &mut book.by_id;
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
            // land after `wait_zero` let the process exit.
            let reading = sink.pending.hold();
            let result = std::panic::AssertUnwindSafe(report.meter.fold_report(
                &report.path, report.observed, &mut scratch,
                report.follow_up.follow_up(report.meter.base_url()),
            )).catch_unwind().await;
            let completed = {
                let mut book = self.book();
                let charges = &mut book.by_id;
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
                                sink.clone().book_resolved(reading, cost);
                            }
                            Ok(None) => {}
                            Err(_) => tracing::error!(target: "weft_engine::metering", charge = %id,
                                "the meter panicked while reading a cost report for a charge a second call had displaced"),
                        }
                        let _ = report.done.send(());
                        return;
                    }
                }
            };
            if let Some((charge, result, unread)) = completed {
                match result {
                    Ok(Some(cost)) => charge.sink.book_resolved(charge.held, cost),
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
                drop(reading);
                let _ = report.done.send(());
                return;
            }
            drop(reading);
            let _ = report.done.send(());
        }
    }

    pub fn flush(&self, why: &str) { self.close_where(|_| true, why); }

    /// End a run's spend: book every charge it still holds as unknown, then
    /// wait until each of its figures is on record. Until that wait returns,
    /// a charge the run opens late (a body that finished after this began)
    /// is booked as unknown at once rather than held. Once it returns no
    /// call of the run is left to open one, so the mark is dropped, and a
    /// run that paused can open charges again when it resumes.
    pub async fn close_execution_id(&self, execution_id: ExecutionId, pending: &PendingCostRecords, why: &str) {
        struct Ending<'a>(&'a OpenCharges, ExecutionId);
        impl Drop for Ending<'_> {
            fn drop(&mut self) { self.0.book().ending.remove(&self.1); }
        }
        self.book().ending.insert(execution_id);
        let _ending = Ending(self, execution_id);
        self.close_where(|charge| charge.sink.execution_id == execution_id, why);
        pending.wait_zero().await;
    }

    /// Book every matching charge as unknown and drop it. A charge whose
    /// report is being read right now is not booked yet: the read may
    /// still price it, so it is marked to close when the read returns
    /// (`drain_reports`). Until then it holds its pending token, and a
    /// wait on `PendingCostRecords::wait_zero` (process shutdown) waits for
    /// that read, which is bounded by the follow-up client's timeout.
    fn close_where(&self, matches: impl Fn(&OpenCharge) -> bool, why: &str) {
        let held = {
            let mut book = self.book();
            let charges = &mut book.by_id;
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
    // run during an unwind, where a panic aborts the process. Keep whatever
    // the meter left, under a key, rather than trusting its shape.
    let mut metadata = match charge.scratch {
        serde_json::Value::Object(_) => charge.scratch,
        other => serde_json::json!({ "meterState": other }),
    };
    metadata["resolution"] = serde_json::json!(format!(
        "the call spent, and no response ever stated what it cost ({why})"
    ));
    let model = metadata["model"].as_str().map(str::to_string);
    // The charge's token moves into the booking, so the count cannot pass
    // through zero while the figure is still to be written down.
    charge.sink.book_resolved(charge.held, MeasuredCost { amount_usd: None, model, metadata });
}

// ---------- The cost sink (where a measured figure lands) ----------

/// Everything needed to book one call's measured cost on the run's record:
/// the run's handle, the firing the spend belongs to, and what is still
/// being worked out (which the run waits for before it ends).
pub struct CostSink {
    /// The run's record (`crate::context::RunRecord::journal`).
    pub journal: Arc<dyn weft_journal::JournalClient>,
    /// The process's writer, which writes the figure down at once.
    pub writer: Arc<crate::journal_writer::WorkerJournal>,
    /// This worker's replica, which writes the run's record.
    pub replica: String,
    /// The run's spend still being worked out.
    pub pending: Arc<PendingCostRecords>,
    /// Charges opened by a call whose amount a later response states.
    /// process-wide, because a charge outlives the call that opened it.
    pub open_charges: Arc<OpenCharges>,
    pub execution_id: ExecutionId,
    pub node_id: String,
    pub frames: LoopFrames,
    pub service: String,
    /// Whose credential the connection rides; recorded on every figure
    /// so the cost trail says whose account spent.
    pub origin: weft_core::CredentialOwner,
    /// The run's cancel flag: a call still going when it trips is let go
    /// (see the module doc).
    pub cancellation: Arc<weft_core::cancellation::CancellationFlag>,
}

impl CostSink {
    /// Book one finished observation's figure: resolve `cost` and write
    /// it down durably, detached from the caller's future (which may be
    /// aborted at any point). `held` is the pending token the spend has
    /// carried since its call went out; it is released once the figure is
    /// on record, so the run cannot end while money is still being
    /// written. The one spawn/record sequence behind every finalizer, HTTP
    /// and session alike; a session hands a ready future, an HTTP call the
    /// meter's resolve.
    pub(crate) fn book(
        self: Arc<Self>,
        held: weft_core::in_flight::InFlightToken,
        cost: impl std::future::Future<Output = MeasuredCost> + Send + 'static,
    ) {
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    let cost = cost.await;
                    self.record(cost).await;
                    drop(held);
                });
            }
            Err(_) => {
                // No runtime to spawn on (the process is tearing down
                // outside tokio): the record cannot be written. Say so
                // loudly; never drop money silently.
                drop(held);
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
    pub(crate) fn book_resolved(self: Arc<Self>, held: weft_core::in_flight::InFlightToken, cost: MeasuredCost) {
        self.book(held, std::future::ready(cost));
    }

    /// Write the figure down: a `CostReported` on the run's record, one per
    /// physical call (a replayed body that calls again spends again), and
    /// on record at once whatever the run is kept as, since it is money:
    /// an unrecorded run leaves a note of itself for it
    /// (`weft_journal::unrecorded`). A figure that cannot be written is
    /// said LOUDLY: the money trail is incomplete and says so.
    async fn record(&self, cost: MeasuredCost) {
        let event = weft_journal::ExecEvent::CostReported {
            execution_id: self.execution_id,
            node_id: self.node_id.clone(),
            frames: self.frames.clone(),
            cost_id: uuid::Uuid::now_v7().to_string(),
            service: self.service.clone(),
            model: cost.model,
            amount_usd: cost.amount_usd,
            billed: false,
            origin: self.origin.clone(),
            metadata: cost.metadata,
            at_unix: crate::now_unix(),
        };
        let written = match self.journal.record_event(&event, Some(&self.replica)).await {
            Ok(()) => self.writer.record_first(self.execution_id).await,
            Err(e) => Err(e),
        };
        if let Err(e) = written {
            tracing::error!(
                target: "weft_engine::metering",
                execution_id = %self.execution_id,
                "COST RECORD LOST for node {} ({}): {e:#}",
                self.node_id, self.service,
            );
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
    /// What the meter's own follow-up queries ride (see [`FollowUpLane`]).
    follow_up: FollowUpLane,
    sink: Arc<CostSink>,
}

/// What a meter's own queries ride: a signed-in client on the direct lane
/// (the same auth the original call rode, on the bounded follow-up pool;
/// the meter itself never sees a credential), and what the worker holds
/// for its runs to share, where a meter keeps what it looked up.
#[derive(Clone)]
struct FollowUpLane {
    http: reqwest_middleware::ClientWithMiddleware,
    shared: Arc<weft_core::shared::Shared>,
}

impl FollowUpLane {
    fn follow_up<'a>(&'a self, base_url: &'a str) -> FollowUp<'a> {
        FollowUp { http: &self.http, base_url, shared: &self.shared }
    }
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
    shared: Arc<weft_core::shared::Shared>,
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
    if sink.origin.is_platform() && meter.is_none() {
        return Err(WeftError::NodeExecution(format!(
            "service '{service}' offers a runtime credential but registers no meter; a \
             runtime credential only travels on meter-declared routes, so this door \
             cannot open. Connect your own credential on the node, or register a meter \
             for the service"
        )));
    }
    let follow_up = FollowUpLane {
        http: reqwest_middleware::ClientBuilder::new(follow_up_client().clone())
            .with(weft_core::access::client::AuthMiddleware::new(steps.clone()))
            .build(),
        shared,
    };
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
            if self.sink.origin.is_platform() {
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
        if self.sink.origin.is_platform() {
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
            if let Err(e) = meter.priceable(route, self.follow_up.follow_up(meter.base_url())).await {
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

        // Named in the warning if the tap has to cut the call.
        let host = req.url().host_str().unwrap_or("").to_string();

        // The run's ending waits from here: the token rides the response's
        // tap into the figure's record, and a body still being read after
        // the node returned keeps it held. Until the answer starts, a
        // cancel of the run lets the call go (the send may be on a task
        // the node's abort does not reach). A run already cancelled sends
        // nothing: no call goes out, so nothing is booked.
        if self.sink.cancellation.is_cancelled() {
            return Err(middleware_err(format!(
                "the run was cancelled: the call to '{}' was not sent",
                self.sink.service
            )));
        }
        let unanswered = Unanswered {
            sink: self.sink.clone(),
            host: host.clone(),
            held: Some(self.sink.pending.hold()),
            spends: matches!(class, RouteClass::Billable(_)),
        };
        let response = tokio::select! {
            biased;
            sent = next.run(req, extensions) => match sent {
                Ok(response) => response,
                Err(e) => {
                    unanswered.failed(&e);
                    return Err(e);
                }
            },
            () = self.sink.cancellation.cancelled() => {
                return Err(middleware_err(format!("{CANCELLED_BEFORE_ANSWER}: the call to '{}' was let go", self.sink.service)));
            }
        };
        let held = unanswered.answered();

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
            inner: Some(response.bytes_stream().boxed()),
            host,
            watch: None,
            finalizer: Some(Finalizer {
                observer,
                meter,
                route: route.to_string(),
                class,
                open_charges: self.sink.open_charges.clone(),
                follow_up: self.follow_up.clone(),
                sink: self.sink.clone(),
                held,
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
    /// The charges this process is holding open, for a provider that states a
    /// call's amount on a later response.
    open_charges: Arc<OpenCharges>,
    /// What the meter's own follow-up queries ride.
    follow_up: FollowUpLane,
    sink: Arc<CostSink>,
    /// The call's pending token, taken when it went out.
    held: weft_core::in_flight::InFlightToken,
}

impl Finalizer {
    /// End the observation and book the figure through [`CostSink::book`]
    /// (detached, tracked, loud on loss). EVERY billable call resolves
    /// through its meter, fixed-priced routes included: whether a call
    /// was actually charged is provider-specific knowledge (a provider
    /// may answer 200 with a failure body it never bills, or bill a
    /// call it then refuses), so the declared price is the meter's
    /// input, never the middleware's verdict.
    ///
    /// `cut` is why the tap stopped reading the answer, when it did: it
    /// lands on the figure (under `cut`), whatever the meter makes of the
    /// part it saw.
    fn finish(self, interrupted: bool, cut: Option<String>) {
        let Finalizer { observer, meter, route, class, open_charges, follow_up, sink, held } = self;
        let observed = observer.end(interrupted);

        // A response that reports on an earlier spend books nothing of its
        // own: it closes the charge that spend opened, which holds its own
        // run's token, so this call's token is released here.
        if matches!(class, RouteClass::Reports) {
            let Some(id) = meter.charge_reported_on(&route, &observed) else { return };
            drop(open_charges.report(meter, &id, &route, observed, &follow_up));
            return;
        }

        // A billable call whose amount only a later response states does
        // not resolve here. The charge is opened SYNCHRONOUSLY, before this
        // returns, so the caller's very next request cannot report on a
        // charge the process is not yet holding.
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
            if let Some(cut) = cut {
                scratch["cut"] = serde_json::json!(cut);
            }
            open_charges.open(meter.service(), id, OpenCharge { token: 0, scratch, sink, held });
            return;
        }

        sink.book(held, async move {
            let mut cost = meter.resolve(&route, observed, follow_up.follow_up(meter.base_url())).await;
            if let Some(cut) = cut {
                // Shape-checked like `scratch` above, for the same reason.
                if !cost.metadata.is_object() {
                    cost.metadata = serde_json::json!({ "meterState": cost.metadata });
                }
                cost.metadata["cut"] = serde_json::json!(cut);
            }
            cost
        });
    }
}

/// How long a metered call's answer may send nothing while it is waited
/// on before the tap stops reading it (see the module doc). Real time,
/// never scaled: it waits on a provider, not on weft
/// (`weft_core::time_scale`). A streamed answer sends every token. The
/// wait for an answer's status and headers is before the tap exists, so
/// a provider that takes minutes to answer at all is not affected; one
/// that answers its headers and then sends nothing for this long is.
pub(crate) const ANSWER_SILENCE_LIMIT: Duration = Duration::from_secs(60);

/// Why a call cancelled before its answer started was let go.
const CANCELLED_BEFORE_ANSWER: &str = "the run was cancelled before the provider answered";

/// A metered call between going out and its answer starting: the call's
/// pending token, and what a call let go here books. A call that spends
/// and ends here without an answer (its run cancelled, its caller gone, its
/// send failed after the connection was made) may have been charged, so it
/// is booked as an unknown spend, never as nothing. Only a call that never
/// reached the provider books nothing.
struct Unanswered {
    sink: Arc<CostSink>,
    host: String,
    held: Option<weft_core::in_flight::InFlightToken>,
    /// A billable route; a reporting route spends nothing.
    spends: bool,
}

impl Unanswered {
    /// The answer started: its tap carries the token on.
    fn answered(mut self) -> weft_core::in_flight::InFlightToken {
        self.held.take().expect("the token is only taken here or by `failed`")
    }

    /// The send failed, and the error goes back to the caller. A failure to
    /// connect means the provider never saw the call: nothing is booked.
    /// Any other (a timeout, a reset once the request went out) may come
    /// after the provider started work it bills, so it is booked as unknown.
    fn failed(mut self, error: &reqwest_middleware::Error) {
        if never_connected(error) {
            self.held = None;
            return;
        }
        self.book_unknown(&format!("the call failed before the provider answered: {error}"));
    }

    /// Book the call as an unknown spend, naming `why`, and release its
    /// token once that is on record. A reporting route spends nothing.
    fn book_unknown(&mut self, why: &str) {
        let Some(held) = self.held.take() else { return };
        if !self.spends {
            return;
        }
        tracing::warn!(
            target: "weft_engine::metering",
            execution_id = %self.sink.execution_id,
            node = %self.sink.node_id,
            service = %self.sink.service,
            host = %self.host,
            "{why}; its cost is booked as unknown",
        );
        let cost = MeasuredCost {
            amount_usd: None,
            model: None,
            metadata: serde_json::json!({ "resolution": format!("unknown: {why}") }),
        };
        self.sink.clone().book_resolved(held, cost);
    }
}

impl Drop for Unanswered {
    fn drop(&mut self) {
        let why = if self.sink.cancellation.is_cancelled() {
            CANCELLED_BEFORE_ANSWER
        } else {
            "the call was dropped before the provider answered"
        };
        self.book_unknown(why);
    }
}

/// Whether a send failed before any connection to the provider was made:
/// a connect error anywhere in its chain.
fn never_connected(error: &reqwest_middleware::Error) -> bool {
    let mut cause: Option<&(dyn std::error::Error + 'static)> = match error {
        reqwest_middleware::Error::Reqwest(e) => Some(e),
        reqwest_middleware::Error::Middleware(e) => Some(e.as_ref()),
    };
    while let Some(e) = cause {
        if e.downcast_ref::<reqwest::Error>().is_some_and(reqwest::Error::is_connect) {
            return true;
        }
        cause = e.source();
    }
    false
}

/// Why the tap stopped reading an answer.
#[derive(Debug, Clone, Copy)]
enum Cut {
    Silent,
    Cancelled,
}

impl std::fmt::Display for Cut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Cut::Silent => write!(f, "the provider sent nothing for {}s", ANSWER_SILENCE_LIMIT.as_secs()),
            Cut::Cancelled => f.write_str("the run was cancelled before the call's answer finished"),
        }
    }
}

impl std::error::Error for Cut {}

/// What the tap's reader is handed: the body's own error, or a [`Cut`].
type TapError = Box<dyn std::error::Error + Send + Sync>;

/// The response-body tap: forwards every chunk untouched and unbuffered,
/// feeding the observer in passing. Finalizes exactly once: on clean end,
/// on stream error, on a cut, or on drop (the caller hung up mid-stream).
struct TapStream {
    /// `None` once the tap cut the answer: the body is dropped, and its
    /// connection with it.
    inner: Option<BoxStream<'static, reqwest::Result<Bytes>>>,
    finalizer: Option<Finalizer>,
    /// The host the call went to, named if the tap cuts it.
    host: String,
    /// What can cut the answer, made the first time the reader waits.
    watch: Option<Watch>,
}

/// The two ways a waited-on answer is cut (see the module doc).
struct Watch {
    cancelled: Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
    silence: Pin<Box<tokio::time::Sleep>>,
    /// The reader is waiting, since the silence deadline was last set: a
    /// chunk clears it, and the next wait sets the deadline again.
    waiting: bool,
}

impl TapStream {
    /// Nothing is ready for the reader: whether the answer is to be cut.
    /// The silence counts from when the reader started waiting, so a
    /// reader that was busy elsewhere never cuts an answer that waited
    /// for it.
    fn stalled(&mut self, cx: &mut Context<'_>) -> Option<Cut> {
        let cancellation = &self.finalizer.as_ref()?.sink.cancellation;
        let watch = self.watch.get_or_insert_with(|| {
            let cancellation = cancellation.clone();
            Watch {
                cancelled: Box::pin(async move { cancellation.cancelled().await }),
                silence: Box::pin(tokio::time::sleep(ANSWER_SILENCE_LIMIT)),
                waiting: true,
            }
        });
        if !watch.waiting {
            watch.waiting = true;
            watch.silence.as_mut().reset(tokio::time::Instant::now() + ANSWER_SILENCE_LIMIT);
        }
        if watch.cancelled.poll_unpin(cx).is_ready() {
            return Some(Cut::Cancelled);
        }
        if watch.silence.poll_unpin(cx).is_ready() {
            return Some(Cut::Silent);
        }
        None
    }

    /// Stop reading the answer: drop the body, book the call as cut, and
    /// hand the reader the reason as the body's error.
    fn cut(&mut self, cut: Cut) -> Poll<Option<Result<Bytes, TapError>>> {
        self.inner = None;
        self.watch = None;
        if let Some(f) = self.finalizer.take() {
            tracing::warn!(
                target: "weft_engine::metering",
                execution_id = %f.sink.execution_id,
                node = %f.sink.node_id,
                service = %f.sink.service,
                host = %self.host,
                "{cut}: weft stopped reading the call's answer, and its cost is booked from what arrived",
            );
            f.finish(true, Some(cut.to_string()));
        }
        Poll::Ready(Some(Err(Box::new(cut))))
    }
}

impl Stream for TapStream {
    type Item = Result<Bytes, TapError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(inner) = self.inner.as_mut() else { return Poll::Ready(None) };
        match inner.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                if let Some(watch) = self.watch.as_mut() {
                    watch.waiting = false;
                }
                if let Some(f) = self.finalizer.as_mut() {
                    f.observer.on_chunk(&chunk);
                }
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(e))) => {
                if let Some(f) = self.finalizer.take() {
                    f.finish(true, None);
                }
                Poll::Ready(Some(Err(Box::new(e))))
            }
            Poll::Ready(None) => {
                if let Some(f) = self.finalizer.take() {
                    f.finish(false, None);
                }
                Poll::Ready(None)
            }
            Poll::Pending => match self.stalled(cx) {
                Some(cut) => self.cut(cut),
                None => Poll::Pending,
            },
        }
    }
}

impl Drop for TapStream {
    fn drop(&mut self) {
        if let Some(f) = self.finalizer.take() {
            f.finish(true, None);
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::Mutex;
    use weft_providers::{ObservedCall, RouteClass};

    // ---- Rig: a recording task store, a test meter, a gated SSE server ----

    /// Keeps every event a run's record is handed.
    #[derive(Default)]
    pub(crate) struct RecordedCosts {
        pub events: Mutex<Vec<weft_journal::ExecEvent>>,
    }

    #[async_trait::async_trait]
    impl weft_journal::JournalClient for RecordedCosts {
        async fn record_event(&self, event: &weft_journal::ExecEvent, _replica: Option<&str>) -> anyhow::Result<()> {
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }
        async fn events_for_execution_id(&self, _execution_id: ExecutionId) -> anyhow::Result<Vec<weft_journal::ExecEvent>> {
            unreachable!("metering tests only write")
        }
    }

    /// A writer over a record nothing reads: the sinks write through
    /// [`RecordedCosts`], and ask the writer to write their run first,
    /// which it does not write.
    pub(crate) fn test_writer() -> Arc<crate::journal_writer::WorkerJournal> {
        crate::journal_writer::WorkerJournal::start(
            Arc::new(crate::test_record::FakeRecord::default()),
            Default::default(),
            &tokio::runtime::Handle::current(),
        )
    }

    /// A figure as the run's record holds it.
    pub(crate) struct Booked {
        pub node_id: String,
        pub service: String,
        pub model: Option<String>,
        pub amount_usd: Option<f64>,
        pub billed: bool,
        pub origin: weft_core::CredentialOwner,
        pub metadata: serde_json::Value,
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
    ) -> (reqwest_middleware::ClientWithMiddleware, Arc<RecordedCosts>, Arc<PendingCostRecords>)
    {
        rig_owned(base, relay_url, weft_core::CredentialOwner::Author, None, weft_core::cancellation::CancellationFlag::new_arc())
    }

    fn rig_owned(
        base: &'static str,
        relay_url: Option<String>,
        origin: weft_core::CredentialOwner,
        fixed_usd: Option<f64>,
        cancellation: Arc<weft_core::cancellation::CancellationFlag>,
    ) -> (reqwest_middleware::ClientWithMiddleware, Arc<RecordedCosts>, Arc<PendingCostRecords>)
    {
        let tasks = Arc::new(RecordedCosts::default());
        let pending = PendingCostRecords::new();
        let sink = CostSink {
            journal: tasks.clone(),
            writer: test_writer(),
            replica: "w".into(),
            pending: pending.clone(),
            open_charges: OpenCharges::new(),
            execution_id: uuid::Uuid::nil(),
            node_id: "node-x".into(),
            frames: LoopFrames::default(),
            service: "testprov".into(),
            origin,
            cancellation,
        };
        // The follow-up client of the rig: same signed-in shape the
        // production composition builds (auth-free here; the test meter
        // makes no follow-up call).
        let follow_up = FollowUpLane {
            http: reqwest_middleware::ClientBuilder::new(reqwest::Client::new()).build(),
            shared: weft_core::shared::Shared::new(std::time::Duration::MAX),
        };
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

    pub(crate) fn recorded_payloads(tasks: &RecordedCosts) -> Vec<Booked> {
        tasks
            .events
            .lock()
            .unwrap()
            .iter()
            .map(|event| match event {
                weft_journal::ExecEvent::CostReported { node_id, service, model, amount_usd, billed, origin, metadata, .. } => Booked {
                    node_id: node_id.clone(),
                    service: service.clone(),
                    model: model.clone(),
                    amount_usd: *amount_usd,
                    billed: *billed,
                    origin: origin.clone(),
                    metadata: metadata.clone(),
                },
                other => panic!("a sink writes costs only: {other:?}"),
            })
            .collect()
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
                let held: Vec<_> = (0..records).map(|_| pending.hold()).collect();
                let enders: Vec<_> = held
                    .into_iter()
                    .map(|token| tokio::spawn(async move { drop(token) }))
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
                tasks: Arc<RecordedCosts>,
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
                    journal: tasks,
                    writer: test_writer(),
                    replica: "w".into(),
                    pending,
                    open_charges: OpenCharges::new(),
                    execution_id: uuid::Uuid::nil(),
                    node_id: "node-x".into(),
                    frames: LoopFrames::default(),
                    service: "testprov".into(),
                    origin: weft_core::CredentialOwner::Author,
                    cancellation: weft_core::cancellation::CancellationFlag::new_arc(),
                };
                // The exact production stack (metering outer, auth inner),
                // with the meter injected directly since the global
                // registry is keyed by the shipped services.
                let follow_up = FollowUpLane {
                    http: reqwest_middleware::ClientBuilder::new(reqwest::Client::new())
                        .with(weft_core::access::client::AuthMiddleware::new(steps.clone()))
                        .build(),
                    shared: weft_core::shared::Shared::new(std::time::Duration::MAX),
                };
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
        let tasks = Arc::new(RecordedCosts::default());
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

        pending.wait_zero().await;
        let payloads = recorded_payloads(&tasks);
        assert_eq!(payloads.len(), 1, "the sign-in call was measured");
        assert_eq!(payloads[0].amount_usd, Some(0.5));
        assert!(!payloads[0].billed, "measured, never billed worker-side");
        assert_eq!(payloads[0].origin, weft_core::CredentialOwner::Author);
        // The wire really carried the auth step: the request reached
        // the server (it answered), and the recorded body proves the
        // prepared rewrite went out on the SAME request.
        assert_eq!(received.lock().unwrap()[0]["usage"]["include"], true);
    }

    // L3, the order a client library that reads the body on a task of its
    // own produces (an LLM client parsing the SSE stream, say): the node has
    // its answer once the usage chunk arrives and returns, while that task
    // is still reading the stream's tail, so the tap finishes after the node
    // is gone. The run's ending must still wait for the call's figure and
    // find it on record once it opens. Stress-looped: the reader task, the
    // tap and the gate meet on a shared multi-thread runtime.
    weft_core::stress_test! {
        name: a_body_read_after_the_node_returned_still_holds_the_runs_ending,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let (base, chunk_tx, _received) = spawn_sse_server().await;
            let base: &'static str = Box::leak(base.into_boxed_str());
            let (client, tasks, pending) = rig(base, None);

            // The answer and its usage arrive; the stream's tail has not.
            chunk_tx.send(Bytes::from("data: {\"usage\":{\"cost\":0.25}}\n\n")).unwrap();
            let response = client
                .post(format!("{base}/chat/completions"))
                .json(&serde_json::json!({"model": "m", "messages": []}))
                .send()
                .await
                .expect("send");
            let (answer_tx, answer_rx) = tokio::sync::oneshot::channel();
            let reader = tokio::spawn(async move {
                let mut body = response.bytes_stream();
                let first = body.next().await.expect("a chunk").expect("readable");
                let _ = answer_tx.send(first);
                while let Some(chunk) = body.next().await {
                    chunk.expect("readable");
                }
            });
            let answer = answer_rx.await.expect("the node got its answer");
            assert!(String::from_utf8_lossy(&answer).contains("0.25"));

            // The node returned and the run ends: its gate stays shut while
            // the tap still holds the call's spend.
            let ending = pending.wait_zero();
            tokio::pin!(ending);
            assert!(
                futures::poll!(&mut ending).is_pending(),
                "the run's ending opened while its call's body was still being read"
            );

            // The tail arrives and the reader finishes the body.
            chunk_tx.send(Bytes::from("data: [DONE]\n\n")).unwrap();
            drop(chunk_tx);
            tokio::time::timeout(std::time::Duration::from_secs(10), ending)
                .await
                .expect("the ending opens once the figure is on record");
            let payloads = recorded_payloads(&tasks);
            assert_eq!(payloads.len(), 1, "the figure is on record before the ending");
            assert_eq!(payloads[0].amount_usd, Some(0.25));
            reader.await.unwrap();
        }
    }

    /// A FIXED-price route still resolves THROUGH its meter (whether a
    /// fixed-priced call was charged is provider knowledge): the record
    /// carries the declared price and names the fixed resolution.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_fixed_route_books_its_declared_price_through_resolve() {
        let (base, chunk_tx, _received) = spawn_sse_server().await;
        let base: &'static str = Box::leak(base.into_boxed_str());
        let (client, tasks, pending) =
            rig_owned(base, None, weft_core::CredentialOwner::Author, Some(0.001), weft_core::cancellation::CancellationFlag::new_arc());

        chunk_tx.send(Bytes::from("data: [DONE]\n\n")).unwrap();
        drop(chunk_tx);
        let response = client
            .post(format!("{base}/chat/completions"))
            .json(&serde_json::json!({"model": "m", "messages": []}))
            .send()
            .await
            .expect("send");
        response.bytes().await.expect("body");

        pending.wait_zero().await;
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
        let tasks = Arc::new(RecordedCosts::default());
        let pending = PendingCostRecords::new();
        let sink = CostSink {
            journal: tasks.clone(),
            writer: test_writer(),
            replica: "w".into(),
            pending: pending.clone(),
            open_charges: OpenCharges::new(),
            execution_id: uuid::Uuid::nil(),
            node_id: "node-x".into(),
            frames: LoopFrames::default(),
            service: "no_such_meterless_service".into(),
            origin: weft_core::CredentialOwner::Author,
            cancellation: weft_core::cancellation::CancellationFlag::new_arc(),
        };
        let client =
            connection_client("no_such_meterless_service", steps, None, Arc::new(sink), weft_core::shared::Shared::new(std::time::Duration::MAX))
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
        assert!(tasks.events.lock().unwrap().is_empty(), "no meter = no record");
        // The auth step still applied.
        assert!(received.lock().unwrap()[0].get("usage").is_none(), "no meter = no prepare");

        // And a RELAYED connection without a meter is refused at build.
        let steps2 = Vec::new();
        let sink2 = CostSink {
            journal: tasks.clone(),
            writer: test_writer(),
            replica: "w".into(),
            pending: pending.clone(),
            open_charges: OpenCharges::new(),
            execution_id: uuid::Uuid::nil(),
            node_id: "node-x".into(),
            frames: LoopFrames::default(),
            service: "no_such_meterless_service".into(),
            origin: weft_core::CredentialOwner::Platform,
            cancellation: weft_core::cancellation::CancellationFlag::new_arc(),
        };
        let err = connection_client(
            "no_such_meterless_service",
            steps2,
            Some("http://relay"),
            Arc::new(sink2),
            weft_core::shared::Shared::new(std::time::Duration::MAX),
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
        assert!(tasks.events.lock().unwrap().is_empty(), "nothing recorded mid-stream");

        // Now the trailing usage chunk + end of stream.
        chunk_tx.send(Bytes::from("data: {\"usage\":{\"cost\":0.000031}}\n\ndata: [DONE]\n\n")).unwrap();
        drop(chunk_tx);
        while let Some(chunk) = stream.next().await {
            chunk.expect("chunk ok");
        }

        pending.wait_zero().await;
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

        pending.wait_zero().await;
        let payloads = recorded_payloads(&tasks);
        assert_eq!(payloads.len(), 1, "an interrupted call still gets its record");
        assert_eq!(
            payloads[0].amount_usd, None,
            "an unresolvable cost is recorded as unknown, never booked as $0"
        );
        assert_eq!(payloads[0].metadata["interrupted"], true);
    }

    /// A tap over `body` for a call of the run whose cancel flag is
    /// `cancellation`, as the middleware builds it once the answer started,
    /// booking into the returned record.
    fn tap_over(
        body: impl Stream<Item = reqwest::Result<Bytes>> + Send + 'static,
        cancellation: Arc<weft_core::cancellation::CancellationFlag>,
    ) -> (TapStream, Arc<RecordedCosts>, Arc<PendingCostRecords>, Arc<OpenCharges>) {
        let tasks = Arc::new(RecordedCosts::default());
        let pending = PendingCostRecords::new();
        let open_charges = OpenCharges::new();
        let sink = Arc::new(CostSink {
            journal: tasks.clone(),
            writer: test_writer(),
            replica: "w".into(),
            pending: pending.clone(),
            open_charges: open_charges.clone(),
            execution_id: uuid::Uuid::nil(),
            node_id: "node-x".into(),
            frames: LoopFrames::default(),
            service: "testprov".into(),
            origin: weft_core::CredentialOwner::Author,
            cancellation,
        });
        let meter: &'static TestMeter = Box::leak(Box::new(TestMeter { base: "http://provider.test/api/v1", fixed_usd: None }));
        let follow_up = FollowUpLane {
            http: reqwest_middleware::ClientBuilder::new(reqwest::Client::new()).build(),
            shared: weft_core::shared::Shared::new(Duration::MAX),
        };
        let tap = TapStream {
            inner: Some(body.boxed()),
            finalizer: Some(Finalizer {
                observer: meter.observe("chat/completions", "", b""),
                meter,
                route: "chat/completions".into(),
                class: RouteClass::Billable(weft_providers::Pricing::Metered),
                open_charges: open_charges.clone(),
                follow_up,
                sink,
                held: pending.hold(),
            }),
            host: "provider.test".into(),
            watch: None,
        };
        (tap, tasks, pending, open_charges)
    }

    /// What a client library's own reading task does with an answer: read
    /// it to its end or its first error, then drop it.
    async fn read_to_end(mut tap: TapStream) -> Result<usize, String> {
        let mut chunks = 0;
        while let Some(chunk) = tap.next().await {
            chunk.map_err(|e| e.to_string())?;
            chunks += 1;
        }
        Ok(chunks)
    }

    const CONTENT: &str = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n";

    /// A provider that stays connected and sends nothing: once the reader
    /// waited the silence limit, the tap cuts the answer, the reader gets
    /// the reason as an error (so the library's task ends), the call is
    /// booked unknown with that reason, and the run's ending returns.
    #[tokio::test(start_paused = true)]
    async fn a_stalled_answer_is_cut_after_the_silence_limit_and_the_run_ends() {
        let body = futures::stream::iter([Ok(Bytes::from(CONTENT))]).chain(futures::stream::pending());
        let (tap, tasks, pending, charges) = tap_over(body, weft_core::cancellation::CancellationFlag::new_arc());
        let started = tokio::time::Instant::now();
        let library = tokio::spawn(read_to_end(tap));

        charges.close_execution_id(uuid::Uuid::nil(), &pending, "the run ended").await;
        assert!(started.elapsed() >= ANSWER_SILENCE_LIMIT, "cut once the silence limit passed, not before");
        assert_eq!(library.await.unwrap(), Err(Cut::Silent.to_string()), "the reader is told why");
        let payloads = recorded_payloads(&tasks);
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].amount_usd, None, "nothing stated the cost: unknown, never $0");
        assert_eq!(payloads[0].metadata["cut"], "the provider sent nothing for 60s");
        assert_eq!(payloads[0].metadata["interrupted"], true);
    }

    /// A cancel of the run cuts an answer still being read at once, on a
    /// task the node's abort never reaches, and the run's ending returns.
    #[tokio::test(start_paused = true)]
    async fn a_cancelled_runs_answer_is_cut_at_once_and_the_run_ends() {
        let cancellation = weft_core::cancellation::CancellationFlag::new_arc();
        let body = futures::stream::iter([Ok(Bytes::from(CONTENT))]).chain(futures::stream::pending());
        let (tap, tasks, pending, charges) = tap_over(body, cancellation.clone());
        let library = tokio::spawn(read_to_end(tap));
        tokio::task::yield_now().await;

        let started = tokio::time::Instant::now();
        cancellation.cancel_because(weft_core::exec::CancelCause::User);
        charges.close_execution_id(uuid::Uuid::nil(), &pending, "the run was cancelled").await;
        assert!(started.elapsed() < ANSWER_SILENCE_LIMIT, "cut by the cancel, not by the silence limit");
        assert_eq!(library.await.unwrap(), Err(Cut::Cancelled.to_string()));
        let payloads = recorded_payloads(&tasks);
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].amount_usd, None);
        assert_eq!(payloads[0].metadata["cut"], "the run was cancelled before the call's answer finished");
    }

    /// A live answer whose gaps each stay under the limit (together far
    /// over it), read by a reader that is then busy elsewhere for longer
    /// than the limit, is never cut and books its stated cost.
    #[tokio::test(start_paused = true)]
    async fn a_slow_but_live_answer_is_never_cut() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<reqwest::Result<Bytes>>();
        let (mut tap, tasks, pending, _charges) =
            tap_over(tokio_stream::wrappers::UnboundedReceiverStream::new(rx), weft_core::cancellation::CancellationFlag::new_arc());
        let gap = ANSWER_SILENCE_LIMIT - Duration::from_secs(1);
        tokio::spawn(async move {
            for _ in 0..3 {
                tokio::time::sleep(gap).await;
                tx.send(Ok(Bytes::from(CONTENT))).unwrap();
            }
            // Sent while the reader is busy elsewhere.
            tokio::time::sleep(ANSWER_SILENCE_LIMIT).await;
            tx.send(Ok(Bytes::from("data: {\"usage\":{\"cost\":0.000031}}\n\n"))).unwrap();
        });
        for _ in 0..3 {
            tap.next().await.expect("open").expect("a live chunk is never cut");
        }
        tokio::time::sleep(ANSWER_SILENCE_LIMIT * 2).await;
        tap.next().await.expect("open").expect("the chunk that waited for the reader");
        assert!(tap.next().await.is_none(), "the answer ends cleanly");

        pending.wait_zero().await;
        let payloads = recorded_payloads(&tasks);
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].amount_usd, Some(0.000031));
        assert!(payloads[0].metadata.get("cut").is_none(), "nothing was cut");
    }

    /// A call whose answer has not even started when its run is cancelled
    /// is let go at once (its send may be on a task the node's abort does
    /// not reach), and booked as an unknown spend.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cancel_lets_go_of_a_call_still_waiting_for_its_answer() {
        let app = axum::Router::new()
            .route("/api/v1/chat/completions", axum::routing::post(std::future::pending::<&'static str>));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base: &'static str = Box::leak(format!("http://{}/api/v1", listener.local_addr().unwrap()).into_boxed_str());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let cancellation = weft_core::cancellation::CancellationFlag::new_arc();
        let (client, tasks, pending) = rig_owned(base, None, weft_core::CredentialOwner::Author, None, cancellation.clone());

        let send = tokio::spawn(async move {
            client.post(format!("{base}/chat/completions")).json(&serde_json::json!({"model": "m", "messages": []})).send().await.map(drop)
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while pending.count() == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the call went out");
        cancellation.cancel_because(weft_core::exec::CancelCause::User);
        let err = tokio::time::timeout(Duration::from_secs(5), send).await.expect("let go at once").unwrap().unwrap_err();
        assert!(format!("{err:?}").contains(CANCELLED_BEFORE_ANSWER), "{err:?}");
        tokio::time::timeout(Duration::from_secs(5), pending.wait_zero()).await.expect("the run's ending is not held");
        let payloads = recorded_payloads(&tasks);
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].amount_usd, None);
        assert_eq!(payloads[0].metadata["resolution"], format!("unknown: {CANCELLED_BEFORE_ANSWER}"));
    }

    /// A billable call on a run already cancelled is refused before it is
    /// sent: the provider never sees it, and nothing is booked.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_call_on_a_cancelled_run_is_not_sent_and_books_nothing() {
        let (base, _chunk_tx, received) = spawn_sse_server().await;
        let base: &'static str = Box::leak(base.into_boxed_str());
        let cancellation = weft_core::cancellation::CancellationFlag::new_arc();
        cancellation.cancel_because(weft_core::exec::CancelCause::User);
        let (client, tasks, pending) = rig_owned(base, None, weft_core::CredentialOwner::Author, None, cancellation);

        let err = client.post(format!("{base}/chat/completions")).json(&serde_json::json!({"model": "m", "messages": []})).send().await.unwrap_err();
        assert!(format!("{err:?}").contains("was not sent"), "{err:?}");
        pending.wait_zero().await;
        assert!(recorded_payloads(&tasks).is_empty(), "no call went out, so nothing is booked");
        assert!(received.lock().unwrap().is_empty(), "the provider never saw it");
    }

    /// A send that never connected books nothing; one that failed once the
    /// request reached the provider (here: it hung up without answering)
    /// may have been billed, so it is booked as an unknown spend.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_send_books_unknown_only_once_the_provider_was_reached() {
        let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let closed_base: &'static str = Box::leak(format!("http://{}/api/v1", closed.local_addr().unwrap()).into_boxed_str());
        drop(closed);
        let (client, tasks, pending) = rig(closed_base, None);
        let err = client.post(format!("{closed_base}/chat/completions")).json(&serde_json::json!({"model": "m", "messages": []})).send().await.unwrap_err();
        assert!(never_connected(&err), "{err:?}");
        pending.wait_zero().await;
        assert!(recorded_payloads(&tasks).is_empty(), "a call that never connected books nothing");

        let hangs_up = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hangs_up_base: &'static str = Box::leak(format!("http://{}/api/v1", hangs_up.local_addr().unwrap()).into_boxed_str());
        tokio::spawn(async move {
            use tokio::io::AsyncReadExt as _;
            let (mut socket, _) = hangs_up.accept().await.unwrap();
            let mut request = [0u8; 1024];
            let _ = socket.read(&mut request).await;
        });
        let (client, tasks, pending) = rig(hangs_up_base, None);
        let err = client.post(format!("{hangs_up_base}/chat/completions")).json(&serde_json::json!({"model": "m", "messages": []})).send().await.unwrap_err();
        assert!(!never_connected(&err), "{err:?}");
        tokio::time::timeout(Duration::from_secs(5), pending.wait_zero()).await.expect("booked and released");
        let payloads = recorded_payloads(&tasks);
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].amount_usd, None);
        let resolution = payloads[0].metadata["resolution"].as_str().unwrap();
        assert!(resolution.starts_with("unknown: the call failed before the provider answered"), "{resolution}");
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
            rig_owned(leaked, None, weft_core::CredentialOwner::Platform, None, weft_core::cancellation::CancellationFlag::new_arc());

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
        assert!(tasks.events.lock().unwrap().is_empty());

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
        assert!(tasks.events.lock().unwrap().is_empty(), "no record on the relayed lane");

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
    use super::tests::{recorded_payloads, test_writer, RecordedCosts};
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

    fn sink(tasks: Arc<RecordedCosts>, pending: Arc<PendingCostRecords>) -> Arc<CostSink> {
        Arc::new(CostSink {
            journal: tasks,
            writer: test_writer(),
            replica: "w".into(),
            pending,
            open_charges: OpenCharges::new(),
            execution_id: uuid::Uuid::nil(),
            node_id: "node-x".into(),
            frames: LoopFrames::default(),
            service: "queued".into(),
            origin: weft_core::CredentialOwner::Author,
            cancellation: weft_core::cancellation::CancellationFlag::new_arc(),
        })
    }

    /// A sink for one execution, sharing the process's charge map.
    fn sink_on(
        tasks: Arc<RecordedCosts>,
        pending: Arc<PendingCostRecords>,
        open_charges: Arc<OpenCharges>,
        execution_id: weft_core::ExecutionId,
    ) -> Arc<CostSink> {
        Arc::new(CostSink {
            journal: tasks,
            writer: test_writer(),
            replica: "w".into(),
            pending,
            open_charges,
            execution_id,
            node_id: "node-x".into(),
            frames: LoopFrames::default(),
            service: "queued".into(),
            origin: weft_core::CredentialOwner::Author,
            cancellation: weft_core::cancellation::CancellationFlag::new_arc(),
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

    fn lane() -> FollowUpLane {
        FollowUpLane { http: http(), shared: weft_core::shared::Shared::new(std::time::Duration::MAX) }
    }

    /// The whole point: the submit books nothing (its amount does not
    /// exist yet), and the later read is what puts the figure on the
    /// trail, against the node that submitted.
    #[tokio::test]
    async fn the_read_that_reports_is_what_books_the_submit() {
        let tasks = Arc::new(RecordedCosts::default());
        let pending = PendingCostRecords::new();
        let sink = sink(tasks.clone(), pending.clone());
        let charges = sink.open_charges.clone();

        charges.open(QUEUED.service(), "req-1".into(), OpenCharge { token: 0, scratch: submitted("req-1").data, held: sink.pending.hold(), sink });
        assert_eq!(charges.count(), 1, "the charge is held between spend and figure");
        assert!(recorded_payloads(&tasks).is_empty(), "nothing is booked yet");

        let report = ObservedCall {
            interrupted: false,
            status: 200,
            data: serde_json::json!({ "units": 3.0 }),
        };
        charges.report(&QUEUED, "req-1", "requests/req-1", report, &lane()).await;
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
        let tasks = Arc::new(RecordedCosts::default());
        let pending = PendingCostRecords::new();
        let sink = sink(tasks.clone(), pending.clone());
        let charges = sink.open_charges.clone();
        charges.open(QUEUED.service(), "req-1".into(), OpenCharge { token: 0, scratch: submitted("req-1").data, held: sink.pending.hold(), sink });

        // No `units`: the job is still running.
        let pending_report = ObservedCall {
            interrupted: false,
            status: 200,
            data: serde_json::json!({ "status": "IN_PROGRESS" }),
        };
        charges.report(&QUEUED, "req-1", "requests/req-1", pending_report, &lane()).await;

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
        let tasks = Arc::new(RecordedCosts::default());
        let charges = OpenCharges::new();
        let pending = PendingCostRecords::new();
        let execution_id = uuid::Uuid::new_v4();
        let sink = sink_on(tasks.clone(), pending.clone(), charges.clone(), execution_id);
        charges.open(QUEUED.service(), "req-1".into(), OpenCharge { token: 0, scratch: submitted("req-1").data, held: sink.pending.hold(), sink });

        drop(charges.report(&QUEUED, "req-1", "requests/req-1", ObservedCall {
            interrupted: false, status: 200, data: serde_json::json!({"status": "IN_PROGRESS"}),
        }, &lane()));
        drop(charges.report(&QUEUED, "req-1", "requests/req-1", ObservedCall {
            interrupted: false, status: 200, data: serde_json::json!({"units": 3.0}),
        }, &lane()));
        charges.close_execution_id(execution_id, &pending, "the execution ended before the job was read back").await;

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
        let tasks = Arc::new(RecordedCosts::default());
        let charges = OpenCharges::new();
        let pending = PendingCostRecords::new();
        let execution_id = weft_core::ExecutionId::new_v4();
        let sink = sink_on(tasks.clone(), pending.clone(), charges.clone(), execution_id);
        charges.open(QUEUED.service(), "req-1".into(), OpenCharge { token: 0, scratch: submitted("req-1").data, held: sink.pending.hold(), sink: sink.clone() });
        // The fake meter yields once inside `fold_report`; the displacing
        // open lands in that window.
        let read = charges.report(&QUEUED, "req-1", "requests/req-1", ObservedCall {
            interrupted: false, status: 200, data: serde_json::json!({"units": 3.0}),
        }, &lane());
        // Let the spawned read reach the meter's own await before displacing.
        tokio::task::yield_now().await;
        charges.open(QUEUED.service(), "req-1".into(), OpenCharge { token: 0, scratch: submitted("req-1").data, held: sink.pending.hold(), sink });
        read.await;
        charges.close_execution_id(execution_id, &pending, "the execution ended").await;
        assert_eq!(charges.count(), 0);
        let mut amounts: Vec<Option<f64>> = recorded_payloads(&tasks).iter().map(|record| record.amount_usd).collect();
        amounts.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(amounts, vec![None, None, Some(6.0)],
            "the displaced charge as unknown, the second charge as unknown at close, and the figure the read produced");
    }

    /// A response body that finishes after its run began ending (a client
    /// library reading the stream on its own task) opens its charge too
    /// late for the close to see it. The charge carries the call's token,
    /// taken when the call went out, so held open it would keep the ending
    /// waiting forever; it is booked as unknown at once and the ending
    /// completes.
    #[tokio::test]
    async fn a_charge_opened_after_its_run_began_ending_is_booked_and_the_ending_completes() {
        let tasks = Arc::new(RecordedCosts::default());
        let charges = OpenCharges::new();
        let pending = PendingCostRecords::new();
        let execution_id = weft_core::ExecutionId::new_v4();
        let sink = sink_on(tasks.clone(), pending.clone(), charges.clone(), execution_id);
        // The call went out: its token is taken, its body still streaming.
        let held = sink.pending.hold();

        let ending = charges.close_execution_id(execution_id, &pending, "the run ended before the job was read back");
        tokio::pin!(ending);
        assert!(futures::poll!(&mut ending).is_pending(), "the ending waits on the call still in flight");

        // The body finishes now, and its meter opens a charge.
        charges.open(QUEUED.service(), "req-1".into(), OpenCharge { token: 0, scratch: submitted("req-1").data, held, sink });
        tokio::time::timeout(std::time::Duration::from_secs(5), ending)
            .await
            .expect("a charge opened after the close must not hold the ending open");

        assert_eq!(charges.count(), 0, "nothing is left held for a run that ended");
        let booked = recorded_payloads(&tasks);
        assert_eq!(booked.len(), 1);
        assert!(booked[0].amount_usd.is_none(), "spend with no figure is unknown, never zero");
        assert!(charges.book().ending.is_empty(), "the mark goes once the ending's wait returned");
    }

    // The same late open racing the close from another thread: whichever
    // side wins the lock, the charge is booked and the ending completes.
    weft_core::stress_test! {
        name: a_late_charge_racing_the_close_never_holds_the_ending,
        runs: 200,
        worker_threads: 4,
        async fn body() {
            let tasks = Arc::new(RecordedCosts::default());
            let charges = OpenCharges::new();
            let pending = PendingCostRecords::new();
            let execution_id = weft_core::ExecutionId::new_v4();
            let sink = sink_on(tasks.clone(), pending.clone(), charges.clone(), execution_id);
            let held = sink.pending.hold();
            let opener = {
                let charges = charges.clone();
                tokio::spawn(async move {
                    charges.open(QUEUED.service(), "req-1".into(), OpenCharge { token: 0, scratch: submitted("req-1").data, held, sink });
                })
            };
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                charges.close_execution_id(execution_id, &pending, "the run ended"),
            )
            .await
            .expect("the ending completes whichever side won");
            opener.await.unwrap();
            assert_eq!(charges.count(), 0);
            let booked = recorded_payloads(&tasks);
            assert_eq!(booked.len(), 1);
            assert!(booked[0].amount_usd.is_none());
        }
    }

    /// A figure that arrives after its execution closed the charge has
    /// nowhere to go: the charge is on the trail as unknown, exactly
    /// once, and the late report neither books again nor panics.
    #[tokio::test]
    async fn a_report_after_the_execution_closed_leaves_one_unknown_record() {
        let tasks = Arc::new(RecordedCosts::default());
        let charges = OpenCharges::new();
        let pending = PendingCostRecords::new();
        let execution_id = weft_core::ExecutionId::new_v4();
        let sink = sink_on(tasks.clone(), pending.clone(), charges.clone(), execution_id);
        charges.open(QUEUED.service(), "req-1".into(), OpenCharge { token: 0, scratch: submitted("req-1").data, held: sink.pending.hold(), sink });
        charges.close_execution_id(execution_id, &pending, "the execution ended before the job was read back").await;
        charges.report(&QUEUED, "req-1", "requests/req-1", ObservedCall {
            interrupted: false, status: 200, data: serde_json::json!({"units": 3.0}),
        }, &lane()).await;
        pending.wait_zero().await;
        assert_eq!(charges.count(), 0);
        let booked = recorded_payloads(&tasks);
        assert_eq!(booked.len(), 1);
        assert!(booked[0].amount_usd.is_none(), "spend with no figure is unknown, never zero");
    }

    #[tokio::test]
    async fn an_executions_charges_close_with_the_execution() {
        let tasks = Arc::new(RecordedCosts::default());
        let charges = OpenCharges::new();

        let mine_pending = PendingCostRecords::new();
        let execution_id = uuid::Uuid::new_v4();
        let mine = sink_on(tasks.clone(), mine_pending.clone(), charges.clone(), execution_id);
        charges.open(QUEUED.service(), "req-1".into(), OpenCharge { token: 0, scratch: submitted("req-1").data, held: mine.pending.hold(), sink: mine });

        // Another execution on the same process, still going. Its own
        // pending tracker, so waiting for this execution's records does
        // not wait on a charge that is meant to stay open.
        let other = sink_on(tasks.clone(), PendingCostRecords::new(), charges.clone(), uuid::Uuid::new_v4());
        charges.open(QUEUED.service(), "req-2".into(), OpenCharge { token: 0, scratch: submitted("req-2").data, held: other.pending.hold(), sink: other });

        charges.close_execution_id(execution_id, &mine_pending, "the execution ended before the job was read back").await;

        assert_eq!(charges.count(), 1, "the other execution's charge is untouched");
        let booked = recorded_payloads(&tasks);
        assert_eq!(booked.len(), 1);
        assert!(booked[0].amount_usd.is_none(), "spend with no figure is unknown, never zero");
    }

    /// Two SERVICES using the same job id are two charges.
    ///
    /// The id is a string a meter picks, and the shipped example meters
    /// mint `job-1`, so a process running two providers at once had one
    /// displace the other: the loser was booked as an unknown spend it
    /// was not, and the winner's report priced it through the loser's
    /// sink, putting a real figure on another execution's trail.
    #[tokio::test]
    async fn one_job_id_under_two_services_is_two_charges() {
        let tasks = Arc::new(RecordedCosts::default());
        let charges = OpenCharges::new();
        let pending = PendingCostRecords::new();
        let execution_id = uuid::Uuid::new_v4();

        let first = sink_on(tasks.clone(), pending.clone(), charges.clone(), execution_id);
        charges.open("queued", "job-1".into(), OpenCharge { token: 0, scratch: submitted("job-1").data, held: first.pending.hold(), sink: first });
        let second = sink_on(tasks.clone(), pending.clone(), charges.clone(), execution_id);
        charges.open("otherprov", "job-1".into(), OpenCharge { token: 0, scratch: submitted("job-1").data, held: second.pending.hold(), sink: second });

        assert_eq!(charges.count(), 2, "neither displaced the other");
        assert!(recorded_payloads(&tasks).is_empty(), "nothing was booked as displaced");

        charges.close_execution_id(execution_id, &pending, "the execution ended").await;
        assert_eq!(recorded_payloads(&tasks).len(), 2, "both spends are written down");
    }

    /// A job submitted and never read back still spent money. The flush
    /// is what stops that becoming a spend with no row at all.
    #[tokio::test]
    async fn a_charge_nobody_ever_reported_on_is_booked_as_unknown() {
        let tasks = Arc::new(RecordedCosts::default());
        let pending = PendingCostRecords::new();
        let sink = sink(tasks.clone(), pending.clone());
        let charges = sink.open_charges.clone();
        charges.open(QUEUED.service(), "req-1".into(), OpenCharge { token: 0, scratch: submitted("req-1").data, held: sink.pending.hold(), sink });

        charges.flush("the replica shut down");
        pending.wait_zero().await;

        let booked = recorded_payloads(&tasks);
        assert_eq!(booked.len(), 1);
        assert_eq!(booked[0].amount_usd, None, "recorded AS unknown, never as zero");
        assert_eq!(booked[0].model.as_deref(), Some("m1"));
        assert!(
            booked[0].metadata["resolution"].as_str().unwrap().contains("the replica shut down"),
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

        let tasks = Arc::new(RecordedCosts::default());
        let pending = PendingCostRecords::new();
        let sink = sink(tasks.clone(), pending.clone());
        let client = reqwest_middleware::ClientBuilder::new(reqwest::Client::new())
            .with(MeteringMiddleware {
                meter: Some(&UNPRICEABLE),
                relay_url: None,
                follow_up: lane(),
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
        let tasks = Arc::new(RecordedCosts::default());
        let pending = PendingCostRecords::new();
        let sink = sink(tasks.clone(), pending.clone());
        let charges = sink.open_charges.clone();

        let report = ObservedCall {
            interrupted: false,
            status: 200,
            data: serde_json::json!({ "units": 3.0 }),
        };
        charges.report(&QUEUED, "req-unknown", "requests/req-unknown", report, &lane()).await;
        pending.wait_zero().await;

        assert!(recorded_payloads(&tasks).is_empty());
    }
}
