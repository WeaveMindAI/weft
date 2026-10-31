//! Workers on Cloud Run.
//!
//! Each project runs as one Cloud Run service (`names::worker_service`),
//! as the project's own service account: one account per project, so a
//! project's workers can prove which project they are and nothing else.
//! Each program and worker settings the project runs is a revision of that
//! service, under a tag of its own (`names::worker_tag`) whose address
//! weft's own calls use, until the dispatcher settles the project's
//! workers on other settings for the same program and it is deleted
//! (`Runner::settle`: which settings are current is the dispatcher's to
//! say, from what the project has stored, never this runner's to guess
//! from the revisions); the project's callers reach the service's own
//! address, whose traffic goes to the project's front
//! (`Runner::front`). Instances kept warm (`min_instances`) are the
//! front's alone: the service holds them (Cloud Run splits a service's
//! minimum by traffic, so a revision taking none keeps none), and a
//! project that takes no calls keeps none. Anybody may call the service (its invoker check is
//! off): the worker checks its callers itself, and weft's own calls carry
//! the project's key, which the worker derives from its project secret.
//!
//! Cloud Run cuts one request at 60 minutes, so a run lives inside that;
//! the worker stops it shortly before and says how a pause gives it a
//! fresh hour.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};
use weft_platform_traits::config::GcpPlatform;
use weft_platform_traits::runner::{PlatformRefused, WorkerWontStart};
use weft_platform_traits::{Patience, Runner, WorkerEndpoint, WorkerSettings, WorkerStarting, WorkerTarget};

use crate::api::{is_status, Google};
use crate::names;

/// The longest one request may run on the Cloud Run services weft makes:
/// a project's workers here, and weft's own roles (its Terraform). The
/// broker tells every line to it this limit, and a worker whose runs only
/// wait stops before its line reaches it (`weft_broker_client::line`).
// SYNC: REQUEST_CAP <-> deploy/terraform/gcp/serverless.tf (google_cloud_run_v2_service.role timeout)
pub const REQUEST_CAP: Duration = Duration::from_secs(3600);

/// How long Cloud Run lets a stopping instance run after it sends
/// SIGTERM, before it kills it. Cloud Run's own figure, not one weft sets.
pub const STOP_GRACE: Duration = Duration::from_secs(10);

/// How long a bring-up waits for a deploy in flight to settle, and how
/// often it looks. Cloud Run settles a revision in minutes; past this the
/// deploy is stuck, and saying so beats waiting on it.
const SETTLE_WAIT: Duration = Duration::from_secs(15 * 60);
const SETTLE_POLL: Duration = Duration::from_secs(5);

/// How long a [`Patience::Brief`] caller (a delivery, a live caller)
/// waits on a bring-up before it hears [`WorkerStarting`]. Well under a delivery's
/// lease, so a delivery is given back before its lease can lapse and hand
/// the same run out twice.
const CALLER_HOLD: Duration = Duration::from_secs(20);

/// A bring-up's outcome as every caller sharing it reads it: the address,
/// or the failure ([`Fault`]).
type Landed = Option<Result<String, Fault>>;

/// A failed bring-up as every caller sharing it reads it (an
/// `anyhow::Error` cannot be shared): the failure in words, and what kind
/// of failure each caller hears it as.
#[derive(Debug, Clone)]
struct Fault {
    words: String,
    kind: FaultKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FaultKind {
    /// Google refused weft's call ([`REFUSING_STATUSES`]), heard as
    /// [`PlatformRefused`].
    Refused,
    /// The revision itself failed to start (`RevisionStanding::Failed`),
    /// heard as [`WorkerWontStart`].
    WontStart,
    /// Anything else: no answer, a timeout.
    Other,
}

/// What Google answers when it refuses weft's call rather than failing to
/// handle it: an invalid request, a call it would not authenticate,
/// permission denied. A 429 is not one: Google answers it for a passing
/// rate limit as much as for a quota, so it counts as no answer, whose
/// tries stop and wait for something that can fix it instead of ending
/// the run.
const REFUSING_STATUSES: [u16; 3] = [400, 401, 403];

impl Fault {
    fn of(e: &anyhow::Error) -> Self {
        let kind = if WorkerWontStart::is(e) {
            FaultKind::WontStart
        } else if REFUSING_STATUSES.iter().any(|status| is_status(e, *status)) {
            FaultKind::Refused
        } else {
            FaultKind::Other
        };
        Self { words: format!("{e:#}"), kind }
    }

    fn into_error(self) -> anyhow::Error {
        match self.kind {
            FaultKind::Refused => anyhow::Error::new(PlatformRefused { why: self.words }),
            FaultKind::WontStart => anyhow::Error::new(WorkerWontStart { why: self.words }),
            FaultKind::Other => anyhow::anyhow!(self.words),
        }
    }
}

/// The bring-ups in flight in this process, one per service name
/// and spec: however many deliveries and live callers want the same one,
/// one task deploys it and polls Cloud Run while it settles, and each
/// caller waits on that task's outcome for as long as it may. The task
/// runs on its own, so a caller that stops waiting stops nothing.
struct Flights {
    inflight: parking_lot::Mutex<HashMap<(String, String), tokio::sync::watch::Receiver<Landed>>>,
    /// How long a [`Patience::Brief`] caller waits ([`CALLER_HOLD`]; a
    /// test shortens it).
    brief: Duration,
}

impl Flights {
    fn new(brief: Duration) -> Self {
        Self { inflight: Default::default(), brief }
    }

    /// Join the bring-up of `key`, starting it with `start` when none is
    /// in flight.
    fn join<F>(self: &Arc<Self>, key: (String, String), start: impl FnOnce() -> F) -> tokio::sync::watch::Receiver<Landed>
    where
        F: std::future::Future<Output = anyhow::Result<String>> + Send + 'static,
    {
        let mut inflight = self.inflight.lock();
        if let Some(rx) = inflight.get(&key) {
            return rx.clone();
        }
        let (tx, rx) = tokio::sync::watch::channel(None);
        inflight.insert(key.clone(), rx.clone());
        drop(inflight);
        let bring_up = start();
        // One line when a bring-up starts and one when it lands, however
        // many callers wait on it: a caller only logs a real failure.
        tracing::info!(target: "weft_platform_gcp::runner", name = %key.0, "bringing up the program's worker");
        let started = std::time::Instant::now();
        let landing = Landing { flights: self.clone(), key };
        tokio::spawn(async move {
            let landed = bring_up.await.map_err(|e| Fault::of(&e));
            let (name, took) = (&landing.key.0, started.elapsed());
            match &landed {
                Ok(at) => tracing::info!(target: "weft_platform_gcp::runner", %name, ?took, %at, "the program's worker is up"),
                Err(e) => tracing::warn!(target: "weft_platform_gcp::runner", %name, ?took, error = %e.words, kind = ?e.kind, "bringing up the program's worker failed"),
            }
            // Out of the map before the outcome is sent, so a caller that
            // reads the outcome and asks again starts afresh.
            drop(landing);
            let _ = tx.send(Some(landed));
        });
        rx
    }

    /// Wait on a bring-up as `patience` says: to its end, or for at most
    /// the brief hold, after which the answer is [`WorkerStarting`].
    async fn wait(&self, mut rx: tokio::sync::watch::Receiver<Landed>, patience: Patience, what: &str) -> anyhow::Result<String> {
        let landed = async {
            match rx.wait_for(Option::is_some).await {
                Ok(landed) => landed.clone().expect("waited for an outcome"),
                Err(_) => Err(Fault { words: format!("the bring-up of {what} stopped without an outcome"), kind: FaultKind::Other }),
            }
        };
        let landed = match patience {
            Patience::ToTheEnd => landed.await,
            Patience::Brief => tokio::time::timeout(self.brief, landed)
                .await
                .map_err(|_| anyhow::Error::new(WorkerStarting { what: what.to_string() }))?,
        };
        landed.map_err(Fault::into_error)
    }
}

/// Takes a bring-up out of [`Flights`] when its task ends, a panic
/// included, so a name is never stuck on a task that is gone.
struct Landing {
    flights: Arc<Flights>,
    key: (String, String),
}

impl Drop for Landing {
    fn drop(&mut self) {
        self.flights.inflight.lock().remove(&self.key);
    }
}

/// How a revision of a project's service stands.
#[derive(Debug, PartialEq, Eq)]
enum RevisionStanding {
    /// Ready to take calls.
    Ready,
    /// Still coming up.
    Settling,
    /// It failed only because the project's new account had not spread
    /// yet: a fresh attempt gets past it.
    Due,
    /// It failed for a reason a fresh attempt would hit again (the image
    /// crashes at start, a bad setting). Carries Cloud Run's own words.
    Failed(String),
}

/// How `found` (a Cloud Run v2 revision) stands. Cloud Run puts its
/// readiness and, when it did not come up, the failure in `conditions`
/// (`type`, `state`, `reason`, `message`).
fn revision_standing(found: &Value) -> RevisionStanding {
    let conditions: Vec<&Value> = found.get("conditions").and_then(Value::as_array).into_iter().flatten().collect();
    let ready = conditions.iter().find(|c| c.get("type").and_then(Value::as_str) == Some("Ready"));
    let state = ready.and_then(|c| c.get("state")).and_then(Value::as_str);
    if found.get("reconciling").and_then(Value::as_bool) == Some(true)
        || matches!(state, None | Some("CONDITION_PENDING" | "CONDITION_RECONCILING" | "STATE_UNSPECIFIED"))
    {
        return RevisionStanding::Settling;
    }
    if state == Some("CONDITION_SUCCEEDED") {
        return RevisionStanding::Ready;
    }
    let mut reasons = Vec::new();
    let mut messages = Vec::new();
    for c in conditions.iter().filter(|c| c.get("state").and_then(Value::as_str) == Some("CONDITION_FAILED")) {
        for field in ["reason", "revisionReason"] {
            if let Some(r) = c.get(field).and_then(Value::as_str) {
                reasons.push(r.to_string());
            }
        }
        if let Some(m) = c.get("message").and_then(Value::as_str).filter(|m| !m.is_empty()) {
            if !messages.iter().any(|seen: &String| seen == m) {
                messages.push(m.to_string());
            }
        }
    }
    let message = messages.join("; ");
    if reasons.iter().any(|r| r == "SECRETS_ACCESS_CHECK_FAILED") || crate::accounts::says_account_not_spread(&message) {
        return RevisionStanding::Due;
    }
    RevisionStanding::Failed(match (message.is_empty(), reasons.is_empty()) {
        (false, _) => message,
        (true, false) => format!("Cloud Run reports it failed ({})", reasons.join(", ")),
        (true, true) => "Cloud Run reports it failed and gives no reason".to_string(),
    })
}

/// The revision the tag `tag` names on the service `found`, as its
/// traffic says.
fn tagged_revision(found: &Value, tag: &str) -> Option<String> {
    found
        .get("traffic")
        .and_then(Value::as_array)?
        .iter()
        .find(|t| t.get("tag").and_then(Value::as_str) == Some(tag))
        .and_then(|t| t.get("revision"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// The address of the tag `tag` on the service `found`, once Cloud Run
/// serves it.
fn tag_address(found: &Value, tag: &str) -> Option<String> {
    found
        .get("trafficStatuses")
        .and_then(Value::as_array)?
        .iter()
        .find(|t| t.get("tag").and_then(Value::as_str) == Some(tag))
        .and_then(|t| t.get("uri"))
        .and_then(Value::as_str)
        .filter(|uri| !uri.is_empty())
        .map(str::to_string)
}

/// The service's traffic with `revision` under `tag` (replacing whatever
/// the tag named before), every other tag kept. `front` sends every call
/// to that revision; otherwise the calls keep going where they went, and
/// the first revision of a new service takes them all.
fn traffic_with(found: Option<&Value>, tag: &str, revision: &str, front: bool) -> Value {
    let mut traffic: Vec<Value> = found
        .and_then(|f| f.get("traffic"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|t| t.get("tag").and_then(Value::as_str) != Some(tag))
        .collect();
    let takes_all = front || traffic.iter().all(|t| t.get("percent").and_then(Value::as_u64).unwrap_or(0) == 0);
    if takes_all {
        for t in traffic.iter_mut() {
            t["percent"] = json!(0);
        }
    }
    traffic.push(json!({
        "type": "TRAFFIC_TARGET_ALLOCATION_TYPE_REVISION",
        "revision": revision,
        "tag": tag,
        "percent": if takes_all { 100 } else { 0 },
    }));
    Value::Array(traffic.into_iter().filter(written_back).collect())
}

/// Whether every call to the service `found` goes to `revision`, as its
/// traffic says (Cloud Run leaves a zero out of what it answers).
fn takes_every_call(found: &Value, revision: &str) -> bool {
    let percent = |t: &Value| t.get("percent").and_then(Value::as_u64).unwrap_or(0);
    let traffic = found.get("traffic").and_then(Value::as_array).into_iter().flatten();
    let mut all = 0;
    for t in traffic {
        if t.get("revision").and_then(Value::as_str) == Some(revision) {
            all += percent(t);
        } else if percent(t) > 0 {
            return false;
        }
    }
    all == 100
}

/// The revisions of the service `found` that the tag `tag` supersedes,
/// once Cloud Run serves `tag`: every one of `revisions` (the service's,
/// as Cloud Run v2 lists them) running the same image (`image_hash`)
/// under another tag, that is with other settings. Each keeps instances
/// of its own, under its own `maxInstanceCount`, for as long as it is
/// there. Nothing goes before `tag` serves: the calls would have nowhere
/// to go. Whether `tag`'s settings are the current ones is its caller's to
/// know ([`Runner::settle`]): nothing on the service says so, since a
/// dispatcher copy holding settings from before a change can deploy a
/// revision with them after the current one. A revision of another image
/// is left to `Runner::forget_image`.
fn superseded(found: &Value, revisions: &[Value], tag: &str, image_hash: &str) -> Vec<String> {
    if tagged_revision(found, tag).is_none() || tag_address(found, tag).is_none() {
        return Vec::new();
    }
    revisions
        .iter()
        .filter(|r| r.pointer("/labels/weft-image").and_then(Value::as_str) == Some(image_hash))
        .filter(|r| revision_tag(r).is_some_and(|other| other != tag))
        .filter_map(revision_name)
        .map(str::to_string)
        .collect()
}

/// The ones of `going` that can be deleted: all but the revision the
/// service `found`'s template names, which Cloud Run refuses to delete
/// (it is the service's latest). Taken out of the traffic like the rest,
/// it takes no calls, scales to zero, and goes with the next settling once
/// a newer write named another revision in the template.
fn deletable(found: &Value, going: Vec<String>) -> Vec<String> {
    let template = found.pointer("/template/revision").and_then(Value::as_str);
    going.into_iter().filter(|g| Some(g.as_str()) != template).collect()
}

/// The tags (their `weft-tag` labels) of the ones of `revisions` named
/// in `going`: the tags whose remembered addresses retiring them ends.
fn tags_of(revisions: &[Value], going: &[String]) -> Vec<String> {
    revisions
        .iter()
        .filter(|r| revision_name(r).is_some_and(|name| going.iter().any(|g| g == name)))
        .filter_map(revision_tag)
        .map(str::to_string)
        .collect()
}

/// A Cloud Run v2 revision's short name (the last part of its `name`).
fn revision_name(revision: &Value) -> Option<&str> {
    revision.get("name").and_then(Value::as_str).and_then(|n| n.rsplit('/').next())
}

/// The tag weft labeled a Cloud Run v2 revision with.
fn revision_tag(revision: &Value) -> Option<&str> {
    revision.pointer("/labels/weft-tag").and_then(Value::as_str)
}

/// The service `found`'s traffic with every entry naming one of `going`
/// taken out and the calls they took handed to the tag `to`, or `None`
/// when no entry names one. `going` runs the same program as `to`
/// ([`superseded`]), so its callers are answered the same.
fn traffic_handed_over(found: &Value, going: &[String], to: &str) -> Option<Value> {
    let percent = |t: &Value| t.get("percent").and_then(Value::as_u64).unwrap_or(0);
    let traffic: Vec<Value> = found.get("traffic").and_then(Value::as_array).cloned().unwrap_or_default();
    let (leaving, mut kept): (Vec<Value>, Vec<Value>) =
        traffic.into_iter().partition(|t| t.get("revision").and_then(Value::as_str).is_some_and(|r| going.iter().any(|g| g == r)));
    if leaving.is_empty() {
        return None;
    }
    let handed: u64 = leaving.iter().map(percent).sum();
    if let Some(t) = kept.iter_mut().find(|t| t.get("tag").and_then(Value::as_str) == Some(to)) {
        t["percent"] = json!(percent(t) + handed);
    }
    Some(Value::Array(kept.into_iter().filter(written_back).collect()))
}

/// Whether Cloud Run takes a traffic entry back as written: one that
/// names its revision, or the "latest" one. An entry whose revision was
/// deleted by hand names none.
fn written_back(entry: &Value) -> bool {
    entry.get("revision").and_then(Value::as_str).is_some()
        || entry.get("type").and_then(Value::as_str) == Some("TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST")
}

/// What this process last saw deployed and usable for one tag of one
/// service. Only an optimization: Cloud Run stays the one record, so an
/// entry is dropped whenever a call shows its address is gone
/// ([`weft_platform_traits::WorkerCall::address_is_gone`]), its project
/// retires or its image goes. A sibling dispatcher copy holding a stale
/// entry costs one failed call, after which it asks Cloud Run again. A
/// failed deploy is never remembered: every copy reads it from Cloud Run.
struct Known {
    address: String,
    project: uuid::Uuid,
    image_hash: String,
}

/// The port a worker serves on (Cloud Run sets `PORT` to it).
const WORKER_PORT: u16 = 8080;

/// Cloning shares every map: a clone is the same runner, handed to a
/// bring-up task that outlives the call that started it.
#[derive(Clone)]
pub struct CloudRunRunner {
    google: Google,
    gcp: GcpPlatform,
    /// The broker's address as a worker reaches it.
    broker_url: String,
    install: weft_core::infra::Install,
    /// What the install allows at its public edge, which a worker's door
    /// holds its callers to.
    edge: weft_platform_traits::config::EdgeConfig,
    /// The install's caller-ticket secret, decoded: what each project's own
    /// secret is derived from (`weft_core::caller_token::ProjectSecret`). A
    /// worker is given only its project's.
    install_secret: Arc<Vec<u8>>,
    /// One change at a time per service in this process. Only spares
    /// duplicate work and needless conflicts: every change is made against
    /// the service as Cloud Run holds it (its `etag`), since another
    /// dispatcher copy may change it too.
    changing: Arc<parking_lot::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    /// Keyed by service and tag. Spares a Cloud Run read on every call to
    /// a project's workers.
    known: Arc<parking_lot::Mutex<HashMap<(String, String), Known>>>,
    flights: Arc<Flights>,
}

impl CloudRunRunner {
    pub fn new(
        google: Google,
        gcp: GcpPlatform,
        broker_url: String,
        install: weft_core::infra::Install,
        edge: weft_platform_traits::config::EdgeConfig,
        install_secret: Vec<u8>,
    ) -> Self {
        Self {
            google,
            gcp,
            broker_url,
            install,
            edge,
            install_secret: Arc::new(install_secret),
            changing: Arc::default(),
            known: Arc::default(),
            flights: Arc::new(Flights::new(CALLER_HOLD)),
        }
    }

    fn run_base(&self) -> String {
        format!("https://run.googleapis.com/v2/projects/{}/locations/{}", self.gcp.project, self.gcp.region)
    }

    fn service_url(&self, project: uuid::Uuid) -> String {
        format!("{}/services/{}", self.run_base(), names::worker_service(project))
    }

    /// The tag of `target`'s revisions.
    fn tag_of(target: &WorkerTarget) -> String {
        use sha2::{Digest, Sha256};
        let settings = serde_json::to_vec(&target.settings).expect("worker settings serialize");
        let digest: String = Sha256::digest(&settings).iter().take(2).map(|b| format!("{b:02x}")).collect();
        names::worker_tag(&target.image, &digest)
    }

    /// `project`'s own secret, which its workers hold.
    fn secret_of(&self, project: uuid::Uuid) -> weft_core::caller_token::ProjectSecret {
        weft_core::caller_token::ProjectSecret::of(&self.install_secret, project)
    }

    /// The account `project`'s workers run as. It needs no grant: what a
    /// worker holds of weft's comes in its environment.
    async fn ensure_account(&self, project: uuid::Uuid) -> anyhow::Result<String> {
        crate::accounts::ensure_project_account(&self.google, &self.gcp, project, &[]).await
    }

    // SYNC: the worker's environment <-> crates/weft-compiler/src/codegen.rs (write_main_rs Args),
    //       crates/weft-platform-local/src/runner.rs (worker_env),
    //       crates/weft-core/src/caller_token.rs (ProjectSecret::from_env),
    //       crates/weft-engine/src/worker.rs (identity_from_env),
    //       crates/weft-engine/src/test_runner.rs (Args::Serve run_cap_secs, the test server's WEFT_RUN_CAP_SECS),
    //       crates/weft-core/src/needs.rs (NEEDS_FROM_ENV)
    fn container(&self, target: &WorkerTarget) -> Value {
        let secret = self.secret_of(target.project);
        let mut env = vec![
            json!({ "name": "WEFT_PROJECT_ID", "value": target.project.to_string() }),
            json!({ "name": "WEFT_TENANT_ID", "value": target.tenant }),
            json!({ "name": "WEFT_BROKER_URL", "value": self.broker_url }),
            json!({ "name": "WEFT_WORKER_IDENTITY", "value": "gcp-metadata" }),
            json!({ "name": "WEFT_PROJECT_SECRET", "value": secret.to_hex() }),
            // Google's front end appends the caller's address, the way it
            // does for the install's own public door.
            json!({ "name": "WEFT_TRUSTED_HOPS", "value": self.edge.trusted_proxy_hops.public.to_string() }),
            json!({ "name": "WEFT_INVALID_TOKENS_PER_MINUTE", "value": weft_platform_traits::config::invalid_tokens_env(self.edge.invalid_tokens_per_minute) }),
            json!({ "name": weft_core::needs::NEEDS_FROM_ENV, "value": weft_core::needs::NeedsFrom::Image.env_value() }),
        ];
        env.extend(target.settings.worker_env().into_iter().map(|(name, value)| json!({ "name": name, "value": value })));
        if let Some(binary_hash) = &target.binary_hash {
            env.push(json!({ "name": "WEFT_BINARY_HASH", "value": binary_hash }));
        }
        env.push(json!({ "name": "WEFT_RUN_CAP_SECS", "value": REQUEST_CAP.as_secs().to_string() }));
        // Cloud Run may stop an instance no request holds open, so a run
        // left with no caller moves under weft's own held call.
        env.push(json!({ "name": "WEFT_RESUME_WHEN_CALLER_LEAVES", "value": "true" }));
        let mut c = json!({
            "image": target.image,
            "env": env,
            "resources": {
                "limits": {
                    "cpu": target.settings.cpu.as_deref().unwrap_or(WorkerSettings::CLOUD_DEFAULT_CPU),
                    "memory": target.settings.memory,
                },
            },
        });
        c["ports"] = json!([{ "containerPort": WORKER_PORT }]);
        c["resources"]["cpuIdle"] = json!(!target.settings.cpu_always_allocated);
        c["resources"]["startupCpuBoost"] = json!(target.settings.startup_boost);
        c
    }

    fn vpc(&self) -> Value {
        json!({
            "networkInterfaces": [{
                "network": self.gcp.network,
                "subnetwork": self.gcp.subnet,
            }],
            // Only the install's private addresses go through the network;
            // the internet stays the internet.
            "egress": "PRIVATE_RANGES_ONLY",
        })
    }

    fn labels(&self, project: uuid::Uuid) -> Value {
        json!({
            weft_core::infra::INSTALL_LABEL: self.install.label_value(),
            "weft-project": project.simple().to_string(),
        })
    }

    /// The revision `revision` running `target`, as the service's template.
    fn revision_template(&self, target: &WorkerTarget, account: &str, revision: &str) -> Value {
        let s: &WorkerSettings = &target.settings;
        json!({
            "revision": revision,
            "labels": { "weft-image": names::image_hash(&target.image), "weft-tag": Self::tag_of(target) },
            "serviceAccount": account,
            // Warm instances are the service's, for its front (`front`).
            "scaling": { "minInstanceCount": 0, "maxInstanceCount": s.max_instances },
            "maxInstanceRequestConcurrency": s.concurrency,
            "timeout": format!("{}s", REQUEST_CAP.as_secs()),
            "vpcAccess": self.vpc(),
            "containers": [self.container(target)],
        })
    }

    /// The project's service as weft writes it: open to every caller (the
    /// worker checks them), with `template` and `traffic`, `warm` instances
    /// kept for whatever takes its calls, against the version `etag` names
    /// when it changes one Cloud Run holds.
    fn service_body(&self, project: uuid::Uuid, template: Value, traffic: Value, warm: u64, etag: Option<&str>) -> Value {
        let mut body = json!({
            "labels": self.labels(project),
            "ingress": "INGRESS_TRAFFIC_ALL",
            "invokerIamDisabled": true,
            "scaling": { "minInstanceCount": warm },
            "template": template,
            "traffic": traffic,
        });
        if let Some(etag) = etag {
            body["etag"] = json!(etag);
        }
        body
    }

    /// Write `body` to the service at `url` (Cloud Run's `allowMissing`
    /// makes one call create or update it) and wait for it. A conflict
    /// (another copy changed it since it was read) is the caller's to read
    /// again and retry.
    async fn write_service(&self, url: &str, body: &Value) -> anyhow::Result<()> {
        let op = self.google.patch(&format!("{url}?allowMissing=true"), body).await?;
        self.google.wait("https://run.googleapis.com/v2", op).await.map(|_| ())
    }

    /// The gate one service's changes take in this process.
    fn gate(&self, service: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.changing.lock().entry(service.to_string()).or_default().clone()
    }

    /// Let go of a service's gate once no change waits on it.
    fn ungate(&self, service: &str, gate: &Arc<tokio::sync::Mutex<()>>) {
        let mut changing = self.changing.lock();
        if changing.get(service).is_some_and(|g| Arc::ptr_eq(g, gate) && Arc::strong_count(g) == 2) {
            changing.remove(service);
        }
    }

    /// The address of `target`'s revision on its project's service,
    /// deploying it when there is none; waited on as `patience` says.
    /// Past the memo, the work is one shared bring-up per service and tag
    /// ([`Flights`]).
    async fn ensure(&self, target: &WorkerTarget, patience: Patience) -> anyhow::Result<String> {
        let key = (names::worker_service(target.project), Self::tag_of(target));
        if let Some(known) = self.known.lock().get(&key) {
            return Ok(known.address.clone());
        }
        let this = self.clone();
        let owned = target.clone();
        let what = format!("{}'s revision {}", key.0, key.1);
        let rx = self.flights.join(key.clone(), move || async move { this.bring_up(&owned).await });
        self.flights.wait(rx, patience, &what).await
    }

    /// The one bring-up of `target`'s revision: deploy it when due, wait
    /// for it to come up, and remember its address.
    async fn bring_up(&self, target: &WorkerTarget) -> anyhow::Result<String> {
        let service = names::worker_service(target.project);
        let tag = Self::tag_of(target);
        let url = self.service_url(target.project);
        let account = self.ensure_account(target.project).await?;
        let started = tokio::time::Instant::now();
        loop {
            anyhow::ensure!(
                started.elapsed() < SETTLE_WAIT,
                "{service}'s revision {tag} did not come up within {}s at Cloud Run; look at it in the console",
                SETTLE_WAIT.as_secs()
            );
            let found = self.google.get_opt(&url).await?;
            let revision = found.as_ref().and_then(|f| tagged_revision(f, &tag));
            let standing = match &revision {
                Some(revision) => match self.google.get_opt(&format!("{url}/revisions/{revision}")).await? {
                    Some(found) => revision_standing(&found),
                    None => RevisionStanding::Due,
                },
                None => RevisionStanding::Due,
            };
            match standing {
                RevisionStanding::Ready => match found.as_ref().and_then(|f| tag_address(f, &tag)) {
                    Some(address) => {
                        self.known.lock().insert(
                            (service, tag),
                            Known { address: address.clone(), project: target.project, image_hash: names::image_hash(&target.image) },
                        );
                        return Ok(address);
                    }
                    // Cloud Run gives a tag its address a moment after
                    // the revision is up.
                    None => tokio::time::sleep(SETTLE_POLL).await,
                },
                RevisionStanding::Settling => tokio::time::sleep(SETTLE_POLL).await,
                RevisionStanding::Failed(why) => {
                    return Err(anyhow::Error::new(WorkerWontStart {
                        why: format!("the program's worker ({service}, revision {tag}) failed to start: {why}"),
                    }));
                }
                RevisionStanding::Due => {
                    let gate = self.gate(&service);
                    let written = {
                        let _one = gate.lock().await;
                        // Read again under the gate: another bring-up of
                        // this process may have just written the service.
                        let found = self.google.get_opt(&url).await?;
                        let revision = format!("{service}-{tag}-{}", random_suffix());
                        let template = self.revision_template(target, &account, &revision);
                        let traffic = traffic_with(found.as_ref(), &tag, &revision, false);
                        let etag = found.as_ref().and_then(|f| f.get("etag")).and_then(Value::as_str);
                        let body = self.service_body(target.project, template, traffic, found.as_ref().map_or(0, warm_of), etag);
                        crate::accounts::until_account_is_known(|| self.write_service(&url, &body)).await
                    };
                    self.ungate(&service, &gate);
                    // Another copy changed the service since it was read,
                    // or is changing it now: read it again in a moment.
                    match written {
                        Err(e) if is_status(&e, 409) || is_status(&e, 412) => tokio::time::sleep(SETTLE_POLL).await,
                        other => other.map_err(|e| e.context(format!("deploy {service}'s revision {tag} for project {}", target.project)))?,
                    }
                }
            }
        }
    }

    /// Every call to the project's service goes to `target`'s revision.
    async fn send_traffic_to(&self, target: &WorkerTarget) -> anyhow::Result<String> {
        let service = names::worker_service(target.project);
        let tag = Self::tag_of(target);
        let url = self.service_url(target.project);
        let gate = self.gate(&service);
        let sent = async {
            let _one = gate.lock().await;
            loop {
                let found = self.google.get_opt(&url).await?.ok_or_else(|| anyhow::anyhow!("{service} is gone"))?;
                let revision = tagged_revision(&found, &tag).ok_or_else(|| anyhow::anyhow!("{service} has no revision tagged {tag}"))?;
                let warm = u64::from(target.settings.min_instances);
                if takes_every_call(&found, &revision) && warm_of(&found) == warm {
                    return found.get("uri").and_then(Value::as_str).map(str::to_string).ok_or_else(|| anyhow::anyhow!("{service} has no address"));
                }
                let traffic = traffic_with(Some(&found), &tag, &revision, true);
                let template = found.get("template").cloned().ok_or_else(|| anyhow::anyhow!("{service} has no template"))?;
                let body = self.service_body(target.project, template, traffic, warm, found.get("etag").and_then(Value::as_str));
                match self.write_service(&url, &body).await {
                    // Another copy is changing the service: read it again
                    // in a moment.
                    Err(e) if is_status(&e, 409) || is_status(&e, 412) => tokio::time::sleep(SETTLE_POLL).await,
                    written => written?,
                }
            }
        }
        .await;
        self.ungate(&service, &gate);
        sent
    }

    /// Retire every revision `target`'s revision supersedes on its
    /// project's service ([`superseded`]), once `target`'s tag serves: out
    /// of its traffic first (Cloud Run refuses to delete a revision it
    /// routes to), its calls handed to `target`'s, then deleted, which ends
    /// its instances and whatever they hold (database connections,
    /// memory); a worker told to stop hands its recorded runs back. The
    /// template's revision is taken out of the traffic and kept
    /// ([`deletable`]). Decided from the service as Cloud Run holds it, so
    /// whatever one attempt leaves the next retires.
    async fn retire_superseded(&self, target: &WorkerTarget) -> anyhow::Result<()> {
        let service = names::worker_service(target.project);
        let tag = Self::tag_of(target);
        let image_hash = names::image_hash(&target.image);
        let url = self.service_url(target.project);
        let revisions = self.revisions(&url).await?;
        let gate = self.gate(&service);
        let untagged = async {
            let _one = gate.lock().await;
            loop {
                let Some(found) = self.google.get_opt(&url).await? else { return Ok((Vec::new(), Vec::new())) };
                let going = superseded(&found, &revisions, &tag, &image_hash);
                let Some(traffic) = traffic_handed_over(&found, &going, &tag) else {
                    return Ok((going.clone(), deletable(&found, going)));
                };
                let template = found.get("template").cloned().ok_or_else(|| anyhow::anyhow!("{service} has no template"))?;
                let body = self.service_body(target.project, template, traffic, warm_of(&found), found.get("etag").and_then(Value::as_str));
                match self.write_service(&url, &body).await {
                    // Another copy is changing the service: read it again
                    // in a moment.
                    Err(e) if is_status(&e, 409) || is_status(&e, 412) => tokio::time::sleep(SETTLE_POLL).await,
                    written => return written.map(|()| (going.clone(), deletable(&found, going))),
                }
            }
        }
        .await;
        self.ungate(&service, &gate);
        let (untagged, deleting) = untagged?;
        // Out of the traffic, their tags' addresses no longer answer.
        let retired = tags_of(&revisions, &untagged);
        self.known.lock().retain(|(s, t), _| *s != service || !retired.contains(t));
        // Every one is tried, so the error names all that are left.
        let mut left = Vec::new();
        for revision in deleting {
            let deleted = match self.google.delete(&format!("{url}/revisions/{revision}")).await {
                Ok(Some(op)) => self.google.wait("https://run.googleapis.com/v2", op).await.map(|_| ()),
                Ok(None) => Ok(()),
                Err(e) => Err(e),
            };
            if let Err(e) = deleted {
                left.push(format!("{revision}: {e:#}"));
            }
        }
        anyhow::ensure!(
            left.is_empty(),
            "{} (to end them now, delete each with `gcloud run revisions delete <revision> --project {} --region {}`)",
            left.join("; "),
            self.gcp.project,
            self.gcp.region,
        );
        Ok(())
    }

    /// Every revision of the service at `url`, as Cloud Run v2 lists them.
    async fn revisions(&self, url: &str) -> anyhow::Result<Vec<Value>> {
        let mut out = Vec::new();
        let mut page: Option<String> = None;
        loop {
            let mut query = vec![("pageSize", "100".to_string())];
            if let Some(token) = page.take() {
                query.push(("pageToken", token));
            }
            let listed = self.google.get_query(&format!("{url}/revisions"), &query).await?;
            out.extend(listed.get("revisions").and_then(Value::as_array).into_iter().flatten().cloned());
            page = listed.get("nextPageToken").and_then(Value::as_str).map(str::to_string);
            if page.is_none() {
                return Ok(out);
            }
        }
    }

    /// The names of the services labeled `label=value`.
    async fn labeled(&self, label: &str, value: &str) -> anyhow::Result<Vec<String>> {
        let mut out = Vec::new();
        let mut page: Option<String> = None;
        loop {
            let mut query = vec![("pageSize", "100".to_string())];
            if let Some(token) = page.take() {
                query.push(("pageToken", token));
            }
            let listed = self.google.get_query(&format!("{}/services", self.run_base()), &query).await?;
            for item in listed.get("services").and_then(Value::as_array).into_iter().flatten() {
                let labels = item.get("labels");
                let ours = labels.and_then(|l| l.get(weft_core::infra::INSTALL_LABEL)).and_then(Value::as_str) == Some(self.install.label_value());
                if ours && labels.and_then(|l| l.get(label)).and_then(Value::as_str) == Some(value) {
                    if let Some(name) = item.get("name").and_then(Value::as_str) {
                        out.push(name.to_string());
                    }
                }
            }
            page = listed.get("nextPageToken").and_then(Value::as_str).map(str::to_string);
            if page.is_none() {
                return Ok(out);
            }
        }
    }

    async fn delete_all(&self, names: Vec<String>) -> anyhow::Result<()> {
        for name in names {
            if let Some(op) = self.google.delete(&format!("https://run.googleapis.com/v2/{name}")).await? {
                self.google.wait("https://run.googleapis.com/v2", op).await?;
            }
        }
        Ok(())
    }

    /// Take every revision of the service `name` (full resource name)
    /// running an image of digest `image_hash` out of its traffic, and
    /// delete them. One that takes calls is the project's front, which
    /// never runs an image nothing references: it is left alone.
    async fn forget_revisions(&self, name: &str, image_hash: &str) -> anyhow::Result<()> {
        let url = format!("https://run.googleapis.com/v2/{name}");
        let going: Vec<String> = self
            .revisions(&url)
            .await?
            .iter()
            .filter(|r| r.pointer("/labels/weft-image").and_then(Value::as_str) == Some(image_hash))
            .filter_map(|r| r.get("name").and_then(Value::as_str).and_then(|n| n.rsplit('/').next()).map(str::to_string))
            .collect();
        if going.is_empty() {
            return Ok(());
        }
        loop {
            let Some(found) = self.google.get_opt(&url).await? else { return Ok(()) };
            let traffic: Vec<Value> = found.get("traffic").and_then(Value::as_array).cloned().unwrap_or_default();
            if traffic.iter().any(|t| {
                t.get("percent").and_then(Value::as_u64).unwrap_or(0) > 0
                    && t.get("revision").and_then(Value::as_str).is_some_and(|r| going.iter().any(|g| g == r))
            }) {
                tracing::warn!(target: "weft_platform_gcp::runner", service = %name, "an image being forgotten still takes the project's calls; its revisions stay");
                return Ok(());
            }
            let kept: Vec<Value> =
                traffic.iter().filter(|t| !t.get("revision").and_then(Value::as_str).is_some_and(|r| going.iter().any(|g| g == r))).cloned().collect();
            if kept.len() == traffic.len() {
                break;
            }
            let template = found.get("template").cloned().ok_or_else(|| anyhow::anyhow!("{name} has no template"))?;
            let project = found
                .pointer("/labels/weft-project")
                .and_then(Value::as_str)
                .and_then(|p| uuid::Uuid::parse_str(p).ok())
                .ok_or_else(|| anyhow::anyhow!("{name} carries no readable weft-project label"))?;
            let body = self.service_body(project, template, Value::Array(kept), warm_of(&found), found.get("etag").and_then(Value::as_str));
            match self.write_service(&url, &body).await {
                Err(e) if is_status(&e, 409) || is_status(&e, 412) => tokio::time::sleep(SETTLE_POLL).await,
                written => {
                    written?;
                    break;
                }
            }
        }
        for revision in going {
            match self.google.delete(&format!("{url}/revisions/{revision}")).await {
                Ok(Some(op)) => {
                    self.google.wait("https://run.googleapis.com/v2", op).await?;
                }
                Ok(None) => {}
                // The service's own template still names it (its latest
                // revision): it goes with the next one.
                Err(e) if is_status(&e, 400) || is_status(&e, 409) || is_status(&e, 412) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

/// How many instances the service `found` keeps warm.
fn warm_of(found: &Value) -> u64 {
    found.pointer("/scaling/minInstanceCount").and_then(Value::as_u64).unwrap_or(0)
}

/// A few random characters that make each deploy attempt a revision of its
/// own: a revision's name is final, so a retry after a failure that
/// settles on its own (a new account's access to the secret spreading)
/// needs a new one.
fn random_suffix() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..4].to_string()
}

#[async_trait]
impl Runner for CloudRunRunner {
    async fn prepare(&self, target: &WorkerTarget) -> anyhow::Result<()> {
        self.ensure(target, Patience::ToTheEnd).await.map(|_| ())
    }

    async fn endpoint(&self, target: &WorkerTarget, patience: Patience) -> anyhow::Result<WorkerEndpoint> {
        let base_url = self.ensure(target, patience).await?;
        Ok(WorkerEndpoint { base_url, bearer: self.secret_of(target.project).worker_door_key(), hold: None })
    }

    async fn front(&self, target: &WorkerTarget, _port: Option<u16>) -> anyhow::Result<weft_core::projects::ProjectAddress> {
        self.ensure(target, Patience::ToTheEnd).await?;
        let url = self.send_traffic_to(target).await?;
        Ok(weft_core::projects::ProjectAddress::Serving { url })
    }

    async fn settle(&self, target: &WorkerTarget) -> anyhow::Result<()> {
        self.retire_superseded(target).await
    }

    // The service keeps no warm instance any more and scales to zero on
    // its own; its traffic stays where it was, and its routes answer that
    // they take no calls.
    async fn let_front_go(&self, project: uuid::Uuid) -> anyhow::Result<()> {
        let service = names::worker_service(project);
        let url = self.service_url(project);
        let gate = self.gate(&service);
        let cooled = async {
            let _one = gate.lock().await;
            loop {
                let Some(found) = self.google.get_opt(&url).await? else { return Ok(()) };
                if warm_of(&found) == 0 {
                    return Ok(());
                }
                let template = found.get("template").cloned().ok_or_else(|| anyhow::anyhow!("{service} has no template"))?;
                let traffic = found.get("traffic").cloned().unwrap_or_else(|| json!([]));
                let body = self.service_body(project, template, traffic, 0, found.get("etag").and_then(Value::as_str));
                match self.write_service(&url, &body).await {
                    Err(e) if is_status(&e, 409) || is_status(&e, 412) => tokio::time::sleep(SETTLE_POLL).await,
                    written => return written,
                }
            }
        }
        .await;
        self.ungate(&service, &gate);
        cooled
    }

    fn forget_address(&self, target: &WorkerTarget) {
        self.known.lock().remove(&(names::worker_service(target.project), Self::tag_of(target)));
    }

    async fn kill_workers(&self, _tenant: &str, project: uuid::Uuid) -> anyhow::Result<()> {
        let value = project.simple().to_string();
        let all = self.labeled("weft-project", &value).await.map_err(|e| anyhow::anyhow!(
            "could not list the project's worker services ({e:#}); find them with `gcloud run services list \
             --project {} --region {} --filter=metadata.labels.weft-project={value}` and delete each with \
             `gcloud run services delete <name> --project {} --region {}`",
            self.gcp.project, self.gcp.region, self.gcp.project, self.gcp.region,
        ))?;
        self.known.lock().retain(|_, k| k.project != project);
        // Deleting a service stops its instances outright. Every one is
        // tried, so the error names all that are left.
        let mut left = Vec::new();
        for name in all {
            if let Err(e) = self.delete_all(vec![name.clone()]).await {
                left.push((name.rsplit('/').next().unwrap_or(&name).to_string(), e));
            }
        }
        if left.is_empty() {
            return Ok(());
        }
        let names: Vec<&str> = left.iter().map(|(name, _)| name.as_str()).collect();
        let why: Vec<String> = left.iter().map(|(name, e)| format!("{name}: {e:#}")).collect();
        anyhow::bail!(
            "the project's worker services {} are still running ({}); delete them by hand with \
             `gcloud run services delete <name> --project {} --region {}`",
            names.join(", "), why.join("; "), self.gcp.project, self.gcp.region,
        )
    }

    async fn retire(&self, _tenant: &str, project: uuid::Uuid) -> anyhow::Result<()> {
        crate::accounts::revoke_project_account(&self.google, &self.gcp, project).await
    }

    async fn forget_image(&self, image: &str) -> anyhow::Result<()> {
        let hash = names::image_hash(image);
        self.known.lock().retain(|_, k| k.image_hash != hash);
        for service in self.labeled(weft_core::infra::INSTALL_LABEL, self.install.label_value()).await? {
            if service.rsplit('/').next().is_some_and(|name| name.starts_with("wk-")) {
                self.forget_revisions(&service, &hash).await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gcp() -> GcpPlatform {
        GcpPlatform {
            name: "weft".into(),
            project: "acme".into(),
            region: "us-central1".into(),
            zone: "us-central1-a".into(),
            network: "weft".into(),
            subnet: "weft-sub".into(),
            artifact_registry: "us-central1-docker.pkg.dev/acme/weft".into(),
            build_bucket: "acme-weft-builds".into(),
            builder_service_account: "weft-builder@acme.iam.gserviceaccount.com".into(),
            builder_base_image: "base".into(),
            tasks_queue: "weft-wakes".into(),
            wake_attempts: 20,
            ticks_queue: "weft-ticks".into(),
            news_topic: "weft-news".into(),
            holders_topic: "weft-holders".into(),
            core_service_account: "weft-core@acme.iam.gserviceaccount.com".into(),
            holder_pool: "projects/acme/locations/us-central1/workerPools/weft-holder".into(),
            dispatcher_service: "weft-role-dispatcher".into(),
            runtime_image: "r".into(),
            deployer_service_account: "d".into(),
            frontend_service_account: "f".into(),
            workload_identity_provider: "w".into(),
            infra_network_tag: "weft-infra".into(),
            build_machine: None,
            alerts: true,
        }
    }

    fn edge() -> weft_platform_traits::config::EdgeConfig {
        weft_platform_traits::config::EdgeConfig {
            trusted_proxy_hops: weft_platform_traits::config::ProxyHops { public: 1, outside: 1, domains: 2 },
            invalid_tokens_per_minute: Some(30),
        }
    }

    fn target(settings: WorkerSettings) -> WorkerTarget {
        WorkerTarget { tenant: "local".into(), project: uuid::Uuid::from_u128(3), image: "us-central1-docker.pkg.dev/acme/weft/weft-worker:ab".into(), binary_hash: Some("ab".into()), settings }
    }

    fn runner(broker: &str) -> CloudRunRunner {
        CloudRunRunner::new(
            Google::new(Arc::new(crate::metadata::MetadataTokens::new())),
            gcp(),
            broker.into(),
            weft_core::infra::Install::default_install(),
            edge(),
            b"test-secret-32-bytes-aaaaaaaaaaa".to_vec(),
        )
    }

    /// A revision keeps no warm instance of its own, whatever the settings:
    /// those are the front's (`service_body`), so a revision that stops
    /// taking calls stops costing anything.
    #[test]
    fn a_revision_keeps_no_warm_instance_of_its_own() {
        let r = runner("b");
        let t = r.revision_template(&target(WorkerSettings { min_instances: 2, ..WorkerSettings::default() }), "a", "rev-1");
        assert_eq!(t["scaling"]["minInstanceCount"], 0);
    }

    #[test]
    fn a_revision_carries_every_worker_lever_and_scales_to_zero_by_default() {
        let r = runner("http://10.10.0.2:14113/broker");
        let t = r.revision_template(&target(WorkerSettings::default()), "wp-x@acme.iam.gserviceaccount.com", "rev-1");
        assert_eq!(t["revision"], "rev-1");
        assert_eq!(t["scaling"]["minInstanceCount"], 0);
        assert_eq!(t["scaling"]["maxInstanceCount"], 10);
        assert_eq!(t["maxInstanceRequestConcurrency"], 80);
        assert_eq!(t["timeout"], "3600s");
        assert_eq!(t["serviceAccount"], "wp-x@acme.iam.gserviceaccount.com");
        assert_eq!(t["vpcAccess"]["networkInterfaces"][0]["subnetwork"], "weft-sub", "workers leave from the install's main subnet");
        let c = &t["containers"][0];
        assert_eq!(c["resources"]["cpuIdle"], true);
        let env = c["env"].as_array().unwrap();
        let var = |n: &str| env.iter().find(|e| e["name"] == n).cloned().unwrap();
        assert_eq!(var("WEFT_BROKER_URL")["value"], "http://10.10.0.2:14113/broker");
        assert!(env.iter().all(|e| e["name"] != "WEFT_INSTALL_URL"));
        let secret = weft_core::caller_token::ProjectSecret::of(b"test-secret-32-bytes-aaaaaaaaaaa", uuid::Uuid::from_u128(3));
        assert_eq!(var("WEFT_PROJECT_SECRET")["value"], secret.to_hex(), "a worker holds its project's secret");
        assert!(env.iter().all(|e| e["name"] != "WEFT_CALLER_TOKEN_SECRET"), "and never the install's");
        assert_eq!(var("WEFT_RUN_CAP_SECS")["value"], "3600");
        assert_eq!(var("WEFT_RESUME_WHEN_CALLER_LEAVES")["value"], "true");
    }

    /// Anybody may call a project's service: its workers check their
    /// callers themselves.
    #[test]
    fn a_projects_service_is_open_to_every_caller() {
        let r = runner("b");
        let body = r.service_body(uuid::Uuid::from_u128(3), json!({}), json!([]), 2, Some("e1"));
        assert_eq!(body["invokerIamDisabled"], true);
        assert_eq!(body["scaling"]["minInstanceCount"], 2, "warm instances are the service's, for its front");
        assert_eq!(body["ingress"], "INGRESS_TRAFFIC_ALL");
        assert_eq!(body["etag"], "e1", "a change is made against the version it read");
        assert_eq!(body["labels"]["weft-project"], uuid::Uuid::from_u128(3).simple().to_string());
    }

    /// Another program and other settings are another tag; the same are
    /// the same.
    #[test]
    fn a_tag_names_the_program_and_its_settings() {
        let one = target(WorkerSettings::default());
        assert_eq!(CloudRunRunner::tag_of(&one), CloudRunRunner::tag_of(&one.clone()));
        let mut other_image = one.clone();
        other_image.image.push('c');
        assert_ne!(CloudRunRunner::tag_of(&one), CloudRunRunner::tag_of(&other_image));
        let mut other_settings = one.clone();
        other_settings.settings.min_instances = 1;
        assert_ne!(CloudRunRunner::tag_of(&one), CloudRunRunner::tag_of(&other_settings));
    }

    /// A new revision takes no calls from the front unless it is the
    /// first, or made the front; every other tag stays reachable.
    #[test]
    fn traffic_keeps_every_tag_and_moves_only_for_a_front() {
        let first = traffic_with(None, "v1", "svc-v1-aaaa", false);
        assert_eq!(first, json!([{ "type": "TRAFFIC_TARGET_ALLOCATION_TYPE_REVISION", "revision": "svc-v1-aaaa", "tag": "v1", "percent": 100 }]));
        let found = json!({ "traffic": first });
        let second = traffic_with(Some(&found), "v2", "svc-v2-bbbb", false);
        assert_eq!(second[0]["percent"], 100, "the front keeps its calls");
        assert_eq!(second[1]["percent"], 0);
        assert!(!takes_every_call(&json!({ "traffic": second.clone() }), "svc-v2-bbbb"));
        let moved = traffic_with(Some(&json!({ "traffic": second })), "v2", "svc-v2-bbbb", true);
        assert!(takes_every_call(&json!({ "traffic": moved.clone() }), "svc-v2-bbbb"));
        assert_eq!(moved.as_array().unwrap().len(), 2, "the old tag stays reachable");
        // Cloud Run leaves a zero out of what it answers.
        assert!(takes_every_call(&json!({ "traffic": [{ "revision": "r2", "percent": 100 }, { "revision": "r1", "tag": "v1" }] }), "r2"));
        // A retried deploy of one tag replaces what the tag named.
        let retried = traffic_with(Some(&json!({ "traffic": moved })), "v2", "svc-v2-cccc", false);
        assert_eq!(retried.as_array().unwrap().iter().filter(|t| t["tag"] == "v2").count(), 1);
    }

    /// A Cloud Run v2 revision named `name`, labeled as weft labels the
    /// revision running the image of digest `image` under `tag`.
    fn listed(name: &str, image: &str, tag: &str) -> Value {
        json!({
            "name": format!("projects/acme/locations/us-central1/services/wk-x/revisions/{name}"),
            "labels": { "weft-image": image, "weft-tag": tag },
        })
    }

    /// Once the settled settings serve, every revision of the same program
    /// under other settings goes, whatever traffic it held and however new
    /// it is; another program's is left to `forget_image`, and the settled
    /// tag's own (an earlier attempt at it included) stays.
    #[test]
    fn settling_supersedes_every_revision_of_the_same_program_under_other_settings() {
        let found = json!({
            "template": { "revision": "r-new" },
            "traffic": [
                { "revision": "r-old", "tag": "vaaaa0001", "percent": 100 },
                { "revision": "r-older", "tag": "vaaaa0002" },
                { "revision": "r-other", "tag": "vbbbb0001" },
                { "revision": "r-new", "tag": "vaaaa0003" },
            ],
            "trafficStatuses": [{ "revision": "r-new", "tag": "vaaaa0003", "uri": "https://vaaaa0003---wk-x-1.a.run.app" }],
        });
        let revisions = [
            listed("r-old", "aaaa", "vaaaa0001"),
            listed("r-older", "aaaa", "vaaaa0002"),
            // Left untagged by an attempt that did not retire it.
            listed("r-untagged", "aaaa", "vaaaa0004"),
            listed("r-other", "bbbb", "vbbbb0001"),
            listed("r-new", "aaaa", "vaaaa0003"),
            // An earlier attempt at the settled tag itself.
            listed("r-new-try", "aaaa", "vaaaa0003"),
        ];
        let going = superseded(&found, &revisions, "vaaaa0003", "aaaa");
        assert_eq!(going, ["r-old", "r-older", "r-untagged"]);
        assert_eq!(tags_of(&revisions, &going), ["vaaaa0001", "vaaaa0002", "vaaaa0004"], "only the retired tags' addresses are forgotten");
        let traffic = traffic_handed_over(&found, &going, "vaaaa0003").unwrap();
        assert_eq!(
            traffic,
            json!([{ "revision": "r-other", "tag": "vbbbb0001" }, { "revision": "r-new", "tag": "vaaaa0003", "percent": 100 }]),
            "the calls the other settings took go to the settled ones"
        );
        assert!(takes_every_call(&json!({ "traffic": traffic }), "r-new"));
        assert_eq!(deletable(&found, going), ["r-old", "r-older", "r-untagged"]);
        assert_eq!(traffic_handed_over(&found, &["r-untagged".to_string()], "vaaaa0003"), None, "nothing to take out of the traffic");
    }

    /// Nothing goes before the settled settings serve: the calls would
    /// have nowhere to go.
    #[test]
    fn nothing_is_superseded_before_the_settled_settings_serve() {
        let revisions = [listed("r-old", "aaaa", "vaaaa0001"), listed("r-new", "aaaa", "vaaaa0003")];
        let tagged = json!({
            "traffic": [{ "revision": "r-old", "tag": "vaaaa0001", "percent": 100 }, { "revision": "r-new", "tag": "vaaaa0003" }],
        });
        assert!(superseded(&tagged, &revisions, "vaaaa0003", "aaaa").is_empty(), "no address yet");
        let untagged = json!({
            "traffic": [{ "revision": "r-old", "tag": "vaaaa0001", "percent": 100 }],
            "trafficStatuses": [{ "revision": "r-old", "tag": "vaaaa0001", "uri": "https://vaaaa0001---wk-x-1.a.run.app" }],
        });
        assert!(superseded(&untagged, &revisions, "vaaaa0003", "aaaa").is_empty(), "not in the traffic yet");
    }

    /// A revision deployed after the settled one with other settings (a
    /// dispatcher copy that still held the settings from before a change)
    /// is the service's template, which Cloud Run refuses to delete: it is
    /// taken out of the traffic, its calls handed to the settled settings,
    /// and kept for a later settling.
    #[test]
    fn the_templates_revision_leaves_the_traffic_and_is_kept() {
        let found = json!({
            "template": { "revision": "r-stale" },
            "traffic": [{ "revision": "r-stale", "tag": "vaaaa0001", "percent": 100 }, { "revision": "r-new", "tag": "vaaaa0003" }],
            "trafficStatuses": [{ "revision": "r-new", "tag": "vaaaa0003", "uri": "https://vaaaa0003---wk-x-1.a.run.app" }],
        });
        let revisions = [listed("r-new", "aaaa", "vaaaa0003"), listed("r-stale", "aaaa", "vaaaa0001")];
        let going = superseded(&found, &revisions, "vaaaa0003", "aaaa");
        assert_eq!(going, ["r-stale"]);
        let traffic = traffic_handed_over(&found, &going, "vaaaa0003").unwrap();
        assert!(takes_every_call(&json!({ "traffic": traffic }), "r-new"));
        assert!(deletable(&found, going).is_empty());
    }

    #[test]
    fn a_failed_revision_redeploys_only_while_the_account_spreads() {
        let revision = |state: &str, reason: &str, message: &str| {
            json!({ "conditions": [{ "type": "Ready", "state": state, "reason": reason, "message": message }] })
        };
        assert_eq!(revision_standing(&revision("CONDITION_SUCCEEDED", "", "")), RevisionStanding::Ready);
        assert_eq!(revision_standing(&revision("CONDITION_RECONCILING", "", "")), RevisionStanding::Settling);
        assert_eq!(revision_standing(&json!({})), RevisionStanding::Settling, "no condition yet");
        assert_eq!(revision_standing(&revision("CONDITION_FAILED", "SECRETS_ACCESS_CHECK_FAILED", "")), RevisionStanding::Due);
        assert_eq!(
            revision_standing(&revision("CONDITION_FAILED", "UNKNOWN", "Permission denied on secret: projects/acme/secrets/s/versions/latest")),
            RevisionStanding::Due
        );
        assert_eq!(
            revision_standing(&revision("CONDITION_FAILED", "UNKNOWN", "The user-provided container failed to start and listen on the port")),
            RevisionStanding::Failed("The user-provided container failed to start and listen on the port".into())
        );
        assert_eq!(
            revision_standing(&revision("CONDITION_FAILED", "CONTAINER_MISSING", "")),
            RevisionStanding::Failed("Cloud Run reports it failed (CONTAINER_MISSING)".into())
        );
    }

    #[test]
    fn a_tags_revision_and_address_are_read_off_the_service() {
        let found = json!({
            "traffic": [{ "revision": "r1", "tag": "v1", "percent": 100 }],
            "trafficStatuses": [{ "revision": "r1", "tag": "v1", "uri": "https://v1---wk-x-1.a.run.app" }],
        });
        assert_eq!(tagged_revision(&found, "v1").as_deref(), Some("r1"));
        assert_eq!(tag_address(&found, "v1").as_deref(), Some("https://v1---wk-x-1.a.run.app"));
        assert_eq!(tagged_revision(&found, "v2"), None);
    }

    /// However many callers want one bring-up, it runs once and every one
    /// of them reads its outcome; a caller that stops waiting hears
    /// `WorkerStarting` and stops nothing; once it lands, the next call
    /// starts afresh.
    #[tokio::test]
    async fn one_bring_up_per_key_shared_by_every_caller() {
        let flights = Arc::new(Flights::new(Duration::from_millis(20)));
        let runs = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (go, gate) = tokio::sync::oneshot::channel::<()>();
        let key = || ("svc".to_string(), "s".to_string());
        let start = |runs: Arc<std::sync::atomic::AtomicUsize>, gate: tokio::sync::oneshot::Receiver<()>| {
            move || async move {
                runs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                gate.await.ok();
                Ok("https://svc".to_string())
            }
        };
        let first = flights.join(key(), start(runs.clone(), gate));
        let (_unused, never) = tokio::sync::oneshot::channel::<()>();
        let second = flights.join(key(), start(runs.clone(), never));
        let early = flights.wait(second.clone(), Patience::Brief, "svc").await.unwrap_err();
        assert!(WorkerStarting::is(&early), "{early:#}");
        // A patient caller is still waiting well past the brief hold, and
        // hears the address once the bring-up lands.
        let patient = tokio::spawn({
            let flights = flights.clone();
            async move { flights.wait(first, Patience::ToTheEnd, "svc").await }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!patient.is_finished(), "a patient caller waits past the brief hold");
        go.send(()).unwrap();
        assert_eq!(patient.await.unwrap().unwrap(), "https://svc");
        assert_eq!(flights.wait(second, Patience::Brief, "svc").await.unwrap(), "https://svc");
        assert_eq!(runs.load(std::sync::atomic::Ordering::SeqCst), 1, "the second caller joined the first bring-up");
        assert!(flights.inflight.lock().is_empty(), "a landed bring-up leaves the map");
        let failed = flights.join(key(), || async { anyhow::bail!("the revision crashed") });
        let crashed = flights.wait(failed, Patience::ToTheEnd, "svc").await.unwrap_err();
        assert_eq!(format!("{crashed:#}"), "the revision crashed");
        assert!(!PlatformRefused::is(&crashed), "a failure Google did not refuse is no refusal");
        assert!(!WorkerWontStart::is(&crashed), "nor a worker that cannot start");
        let failed = flights.join(key(), || async {
            Err(anyhow::Error::new(WorkerWontStart { why: "the container exited".into() }))
        });
        let wont = flights.wait(failed, Patience::ToTheEnd, "svc").await.unwrap_err();
        assert!(WorkerWontStart::is(&wont), "a revision that failed to start reaches every caller as such: {wont:#}");
        assert!(format!("{wont:#}").contains("the container exited"), "{wont:#}");
    }

    /// Google refusing weft's call, under whatever context the bring-up
    /// added, reaches every caller sharing it as `PlatformRefused`; Google
    /// failing to handle it (a 503) does not.
    #[tokio::test]
    async fn google_refusing_the_bring_up_reaches_its_callers_as_a_refusal() {
        let flights = Arc::new(Flights::new(Duration::from_millis(20)));
        let answered = |status: u16| {
            let flights = flights.clone();
            async move {
                let rx = flights.join(("svc".to_string(), status.to_string()), move || async move {
                    Err(anyhow::Error::new(crate::api::ApiError { status, body: "no".into() }).context("deploy svc's revision"))
                });
                flights.wait(rx, Patience::ToTheEnd, "svc").await.unwrap_err()
            }
        };
        for status in REFUSING_STATUSES {
            let refused = answered(status).await;
            assert!(PlatformRefused::is(&refused), "{status}: {refused:#}");
            assert!(format!("{refused:#}").contains(&format!("Google answered {status}")), "{refused:#}");
        }
        assert!(!PlatformRefused::is(&answered(503).await));
        assert!(!PlatformRefused::is(&answered(429).await), "a rate limit is no answer, not a refusal");
    }
}
