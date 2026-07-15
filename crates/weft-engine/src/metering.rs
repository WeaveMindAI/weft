//! The worker side of metered paid calls: the middleware behind
//! `ctx.metered_client`.
//!
//! For each request on a provider access, the middleware
//!   1. routes it (straight to the provider, or to the deployment's relay
//!      when the access carries one),
//!   2. on a direct billable route, runs the provider's meter around it:
//!      prepare the request so its cost becomes reportable, TAP the response
//!      stream (the caller sees every chunk in real time; nothing is
//!      buffered or delayed), and
//!   3. when the response ends (cleanly or cut), resolves the meter's
//!      figure and records it durably on the execution's cost trail.
//!
//! A relayed call is not measured here: the relay is where the deployment's
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
use futures::{Stream, StreamExt};

use weft_core::error::{WeftError, WeftResult};
use weft_core::frames::LoopFrames;
use weft_core::Color;
use weft_providers::{CallObservation, FollowUp, MeasuredCost, ProviderMeter, RouteClass};

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

    fn end(&self) {
        if self.count.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.zero.notify_waiters();
        }
    }

    /// Resolve once no cost records are in flight. Every resolve is
    /// internally bounded (the follow-up client has a request timeout and
    /// the ledger poll a fixed budget), so this always returns.
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

// ---------- The cost sink (where a measured figure lands) ----------

/// Everything needed to book one call's measured cost to the execution's
/// durable cost trail: the task client to enqueue through, the firing the
/// spend belongs to, and the pending-records tracker.
pub struct CostSink {
    pub tasks: Arc<dyn weft_task_store::TaskStoreClient>,
    pub pending: Arc<PendingCostRecords>,
    pub project_id: String,
    pub tenant_id: String,
    pub color: Color,
    pub node_id: String,
    pub frames: LoopFrames,
    pub provider: String,
    /// Whose key the access rides; recorded on every figure so the cost
    /// trail says whose key spent.
    pub origin: weft_core::AccessOrigin,
}

impl CostSink {
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
            service: self.provider.clone(),
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
                    self.node_id, self.provider,
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
                        self.node_id, self.provider,
                    );
                }
            }
        }
    }
}

// ---------- The middleware ----------

/// Per-access metering middleware; see the module docs.
pub struct MeteringMiddleware {
    /// The provider's meter, when this runtime ships one. `None` = the
    /// call passes through unmeasured (a provider without a meter has no
    /// cost figure at all).
    meter: Option<&'static dyn ProviderMeter>,
    /// The deployment's relay for calls on this access; `None` = direct.
    relay_url: Option<String>,
    /// The access credential, for the meter's own follow-up query on the
    /// direct lane.
    credential: String,
    sink: Arc<CostSink>,
}

/// Build the metered client for one access. Fails loud when the access is
/// relayed but this runtime has no meter for the provider: routing to a
/// relay needs the provider's base URL to strip, and only a meter knows it.
pub fn metered_client(
    provider: &str,
    credential: &str,
    relay_url: Option<&str>,
    sink: CostSink,
) -> WeftResult<reqwest_middleware::ClientWithMiddleware> {
    let meter = weft_providers::meter_for(provider);
    if relay_url.is_some() && meter.is_none() {
        return Err(WeftError::NodeExecution(format!(
            "the deployment relays calls for provider '{provider}', but this runtime has no \
             provider definition to route them by; set your own key on the node"
        )));
    }
    let middleware = MeteringMiddleware {
        meter,
        relay_url: relay_url.map(str::to_string),
        credential: credential.to_string(),
        sink: Arc::new(sink),
    };
    Ok(reqwest_middleware::ClientBuilder::new(base_client().clone())
        .with(middleware)
        .build())
}

/// The shared connection pool under every metered client. No total timeout
/// (streams run long); the connect timeout bounds a dead host. Redirects
/// disabled: a redirect could re-aim a spliced credential at another host.
fn base_client() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("metered base client")
    })
}

/// The client a meter's own follow-up query rides. Separate from the
/// metered pool on purpose: a follow-up must be bounded (the pending-record
/// tracker relies on every resolve finishing), so it carries a total
/// request timeout.
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
            // Relayed lane: rebuild the URL against the relay and send. The
            // relay does the measuring; this side does none.
            let Some(route) = route else {
                return Err(middleware_err(format!(
                    "this call ({}) is not under provider '{}''s API ({}), so the deployment \
                     cannot relay it; calls on a deployment-granted access must address the \
                     provider's own API",
                    req.url(),
                    self.sink.provider,
                    self.meter.map(|m| m.base_url()).unwrap_or("<unknown>"),
                )));
            };
            let query = req.url().query().map(|q| format!("?{q}")).unwrap_or_default();
            let relayed = format!("{}/{}{}", relay.trim_end_matches('/'), route, query);
            *req.url_mut() = reqwest::Url::parse(&relayed).map_err(|e| {
                middleware_err(format!("relay URL {relayed:?} does not parse: {e}"))
            })?;
            return next.run(req, extensions).await;
        }

        // Direct lane: measure billable routes; everything else passes
        // through untouched (a free route, an unknown route on a key the
        // user holds, a provider with no meter).
        let (Some(meter), Some(route)) = (self.meter, route.as_deref()) else {
            return next.run(req, extensions).await;
        };
        if !matches!(meter.classify(req.method().as_str(), route), RouteClass::Billable(_)) {
            return next.run(req, extensions).await;
        }

        // Prepare the request so its cost becomes reportable. A body the
        // middleware cannot see (a streaming body) cannot be prepared, and
        // an unpreparable billable call would be an unmeasurable spend:
        // refuse it loud, before any bytes go out. Buffer the body instead.
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
                    self.sink.provider,
                )))
            }
        }
        // The tap reads the response bytes as they pass; a compressed body
        // would be opaque to it. This client never negotiates compression
        // itself, but a caller-set header would; force identity.
        req.headers_mut().remove(http::header::ACCEPT_ENCODING);

        let response = next.run(req, extensions).await?;

        // Tap the response: the caller sees every chunk in real time while
        // the observer reads what it needs in passing. When the stream
        // ends (or is cut, including by drop), the finalizer resolves the
        // cost and records it, detached from the caller's future.
        let mut observer = meter.observe();
        observer.on_status(response.status().as_u16());
        let status = response.status();
        let version = response.version();
        let headers = response.headers().clone();
        let tapped = TapStream {
            inner: response.bytes_stream().boxed(),
            finalizer: Some(Finalizer {
                observer,
                meter,
                credential: self.credential.clone(),
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
    credential: String,
    sink: Arc<CostSink>,
}

impl Finalizer {
    /// End the observation and spawn the resolve + record, tracked by the
    /// pending counter so the pod cannot exit under it. Detached on
    /// purpose: the caller's future may be aborted at any point, and the
    /// money must still be written down.
    fn finish(self, interrupted: bool) {
        let observed = self.observer.end(interrupted);
        let meter = self.meter;
        let credential = self.credential;
        let sink = self.sink;
        sink.pending.begin();
        let dedup_key = format!("metered_cost:{}", uuid::Uuid::new_v4());
        let sink_on_no_runtime = sink.clone();
        let task = async move {
            let follow_up = FollowUp {
                http: follow_up_client(),
                base_url: meter.base_url(),
                credential: &credential,
            };
            let cost = meter.resolve(observed, follow_up).await;
            sink.record(dedup_key, cost).await;
            sink.pending.end();
        };
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(task);
            }
            Err(_) => {
                // No runtime to spawn on (the process is tearing down
                // outside tokio): the record cannot be written. Say so
                // loudly; never drop money silently.
                sink_on_no_runtime.pending.end();
                tracing::error!(
                    target: "weft_engine::metering",
                    "COST RECORD LOST for provider '{}': the async runtime was already torn \
                     down when this call's cost resolution was due (a stream dropped during \
                     process shutdown), so the figure could not be written down",
                    sink_on_no_runtime.provider,
                );
            }
        }
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
    struct RecordingTaskStore {
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
    struct TestMeter {
        base: &'static str,
    }

    struct TestObservation {
        scanner: weft_providers::sse::DataLineScanner,
        cost: Option<f64>,
    }

    impl CallObservation for TestObservation {
        fn on_status(&mut self, _status: u16) {}
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
                data: serde_json::json!({ "cost": self.cost }),
            }
        }
    }

    #[async_trait::async_trait]
    impl ProviderMeter for TestMeter {
        fn provider(&self) -> &'static str {
            "testprov"
        }
        fn base_url(&self) -> &'static str {
            self.base
        }
        fn classify(&self, method: &str, path: &str) -> RouteClass {
            match (method, path) {
                ("POST", "chat/completions") => {
                    RouteClass::Billable(weft_providers::Pricing::Metered)
                }
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
            _http: &reqwest::Client,
        ) -> anyhow::Result<f64> {
            Ok(1.0)
        }
        fn observe(&self) -> Box<dyn CallObservation> {
            Box::new(TestObservation {
                scanner: weft_providers::sse::DataLineScanner::new(),
                cost: None,
            })
        }
        async fn resolve(&self, observed: ObservedCall, _follow_up: FollowUp<'_>) -> MeasuredCost {
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
        let tasks = Arc::new(RecordingTaskStore::default());
        let pending = PendingCostRecords::new();
        let sink = CostSink {
            tasks: tasks.clone(),
            pending: pending.clone(),
            project_id: "p1".into(),
            tenant_id: "t1".into(),
            color: uuid::Uuid::nil(),
            node_id: "node-x".into(),
            frames: LoopFrames::default(),
            provider: "testprov".into(),
            origin: weft_core::AccessOrigin::UserProvided,
        };
        let middleware = MeteringMiddleware {
            meter: Some(Box::leak(Box::new(TestMeter { base }))),
            relay_url,
            credential: "sk-key".into(),
            sink: Arc::new(sink),
        };
        let client = reqwest_middleware::ClientBuilder::new(reqwest::Client::new())
            .with(middleware)
            .build();
        (client, tasks, pending)
    }

    fn recorded_payloads(tasks: &RecordingTaskStore) -> Vec<weft_task_store::RecordCostPayload> {
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

    /// L3, the relay lane: a call on a relayed access is REWRITTEN to the
    /// relay (path + query preserved) and NOT measured here (the relay is
    /// where the deployment measures). A call outside the provider's API
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
        assert!(err.to_string().contains("cannot relay"), "{err}");
    }
}
