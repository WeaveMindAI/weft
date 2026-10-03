//! Workers on Cloud Run.
//!
//! Each project image runs as a Cloud Run service of its own
//! (`names::worker_service`), scaling to zero unless the project keeps
//! copies warm, as the project's own service account: one account per
//! project, so a project's workers can prove which project they are and
//! nothing else. Only weft's own service account may invoke the service,
//! so the worker trusts any call that reaches it (`WEFT_WORKER_DOOR=
//! platform`). A long run is an execution of a Cloud Run job on the same
//! image, running `--run <execution_id>`.
//!
//! Cloud Run cuts one request at 60 minutes, so a short run lives inside
//! that; the worker stops itself shortly before it and says which setting
//! lifts it.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};
use weft_platform_traits::config::GcpPlatform;
use weft_platform_traits::{Patience, Runner, WorkerEndpoint, WorkerSettings, WorkerStarting, WorkerTarget};

use crate::accounts::{add_binding, Access};
use crate::api::{is_status, Google};
use crate::names;

/// The longest one request may run on Cloud Run.
const REQUEST_CAP: Duration = Duration::from_secs(3600);

/// The longest a Cloud Run job's task may run.
const JOB_CAP_SECS: u64 = 7 * 24 * 3600;

/// How long a bring-up waits for a deploy in flight to settle, and how
/// often it looks. Cloud Run settles a revision in minutes; past this the
/// deploy is stuck, and saying so beats waiting on it.
const SETTLE_WAIT: Duration = Duration::from_secs(15 * 60);
const SETTLE_POLL: Duration = Duration::from_secs(5);

/// How long a [`Patience::Brief`] caller (a delivery, a live caller)
/// waits on a bring-up before it hears [`WorkerStarting`]. Well under a delivery's
/// lease, so a delivery is given back before its lease can lapse and hand
/// the same execution out twice.
const CALLER_HOLD: Duration = Duration::from_secs(20);

/// A bring-up's outcome as every caller sharing it reads it: the address,
/// or the failure in words (an `anyhow::Error` cannot be shared).
type Landed = Option<Result<String, String>>;

/// The bring-ups in flight in this process, one per service or job name
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
            let landed = bring_up.await.map_err(|e| format!("{e:#}"));
            let (name, took) = (&landing.key.0, started.elapsed());
            match &landed {
                Ok(at) => tracing::info!(target: "weft_platform_gcp::runner", %name, ?took, %at, "the program's worker is up"),
                Err(e) => tracing::warn!(target: "weft_platform_gcp::runner", %name, ?took, error = %e, "bringing up the program's worker failed"),
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
                Err(_) => Err(format!("the bring-up of {what} stopped without an outcome")),
            }
        };
        let landed = match patience {
            Patience::ToTheEnd => landed.await,
            Patience::Brief => tokio::time::timeout(self.brief, landed)
                .await
                .map_err(|_| anyhow::Error::new(WorkerStarting { what: what.to_string() }))?,
        };
        landed.map_err(|e| anyhow::anyhow!(e))
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

/// The label naming the spec a service or job stands deployed at.
const SPEC_LABEL: &str = "weft-spec";

/// A short fingerprint of a service or job body, as a label value.
fn spec_of(body: &Value) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(body.to_string().as_bytes()).iter().take(16).map(|b| format!("{b:02x}")).collect()
}

/// `body` carrying its own spec label. The label rides the same write as
/// the body it names, so whatever Cloud Run holds is labelled with the
/// spec it runs, whichever of two racing copies wrote last: a label
/// written apart from its body could name the other copy's.
fn labelled(body: &Value, spec: &str) -> Value {
    let mut out = body.clone();
    out["labels"][SPEC_LABEL] = json!(spec);
    out
}

/// The annotation on a service's revision template that makes each deploy
/// attempt a template of its own.
const ATTEMPT_ANNOTATION: &str = "weft-attempt";

/// `body` as one deploy attempt writes it. Cloud Run makes a new revision
/// only when the template changes, so a retry of a byte-identical body
/// after a revision failed for a reason that settles on its own (a new
/// account's access to the secret not spread yet) would leave the service
/// on the failed revision for good. A fresh value in the template makes
/// every attempt a new revision. It is added after [`spec_of`] read the
/// body, so the spec names what is deployed and never the attempt. A job
/// has no revisions (each execution reads its current template), so it is
/// written as is.
fn attempt(body: &Value, kind: Kind) -> Value {
    let mut out = body.clone();
    if kind == Kind::Service {
        out["template"]["annotations"][ATTEMPT_ANNOTATION] = json!(uuid::Uuid::new_v4().simple().to_string());
    }
    out
}

/// What a deployed Cloud Run resource is to weft.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// Called over HTTP at its own address; only weft may invoke it.
    Service,
    /// Started by resource URL (`:run`), never called.
    Job,
}

impl Kind {
    /// Where weft reaches the resource at `url`, read off what Cloud Run
    /// holds for it.
    fn address(self, url: &str, found: &Value) -> Option<String> {
        match self {
            Kind::Service => found.get("uri").and_then(Value::as_str).map(str::to_string),
            Kind::Job => Some(url.to_string()),
        }
    }
}

/// Where a service or job Cloud Run holds stands against the spec weft
/// wants it at.
#[derive(Debug, PartialEq, Eq)]
enum Standing {
    /// At the spec, ready, and (a service) serving its latest revision.
    Ready,
    /// A deploy (another dispatcher copy's, or an earlier one) is still
    /// settling. Redeploying now would replace that copy's revision with
    /// ours mid-flight, so the caller waits and reads again.
    Settling,
    /// A deploy is due: another spec, or a latest
    /// revision that failed only because the project's new account had
    /// not spread yet (which a fresh attempt gets past).
    Due,
    /// At the spec, and its latest attempt failed for a reason a redeploy
    /// of the same spec would hit again (the image crashes at start, a
    /// bad setting). Carries Cloud Run's own words.
    Failed(String),
}

/// How `found` (a Cloud Run v2 service or job) stands against `spec`.
/// Cloud Run puts a resource's readiness and, when it did not reach a
/// serving state, the failure in `terminalCondition` (`state`, `reason`,
/// `message`); `conditions` holds its sub-resources' (the revision's).
fn standing(found: &Value, spec: &str, kind: Kind) -> Standing {
    if found.pointer(&format!("/labels/{SPEC_LABEL}")).and_then(Value::as_str) != Some(spec) {
        return Standing::Due;
    }
    let terminal = found.get("terminalCondition");
    let state = terminal.and_then(|c| c.get("state")).and_then(Value::as_str);
    if found.get("reconciling").and_then(Value::as_bool) == Some(true)
        || matches!(state, None | Some("CONDITION_PENDING" | "CONDITION_RECONCILING" | "STATE_UNSPECIFIED"))
    {
        return Standing::Settling;
    }
    // An older revision still serving while the latest failed runs other
    // settings than this spec's, so it is no more usable than none.
    let serves_latest = kind == Kind::Job || found.get("latestReadyRevision") == found.get("latestCreatedRevision");
    if state == Some("CONDITION_SUCCEEDED") && serves_latest {
        return Standing::Ready;
    }
    let failed = terminal
        .into_iter()
        .chain(found.get("conditions").and_then(Value::as_array).into_iter().flatten())
        .filter(|c| c.get("state").and_then(Value::as_str) == Some("CONDITION_FAILED"));
    let mut reasons = Vec::new();
    let mut messages = Vec::new();
    for c in failed {
        for field in ["reason", "revisionReason", "executionReason"] {
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
        return Standing::Due;
    }
    Standing::Failed(match (message.is_empty(), reasons.is_empty()) {
        (false, _) => message,
        (true, false) => format!("Cloud Run reports it failed ({})", reasons.join(", ")),
        (true, true) => "its latest revision is not the one serving, and Cloud Run gives no reason".to_string(),
    })
}

/// What this process last saw deployed and usable under one name. Only
/// an optimization: Cloud Run stays the one record, so an entry is
/// dropped whenever a call shows its address is gone
/// ([`weft_platform_traits::WorkerCall::address_is_gone`]), its project
/// retires or its image goes, and a changed spec never matches it. A
/// sibling dispatcher copy holding a stale entry costs one failed call,
/// after which it asks Cloud Run again. A failed deploy is never
/// remembered: every copy reads it from Cloud Run.
struct Known {
    spec: String,
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
    /// One deploy at a time per service or job in this process (two specs
    /// of one name included), while deploys are in flight. Only spares
    /// duplicate work: whether one is deployed is read from Cloud Run
    /// under the gate, since another dispatcher copy may have changed or
    /// deleted it.
    deploying: Arc<parking_lot::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    /// Keyed by service or job name. Spares a Cloud Run read on every
    /// call to a project's workers.
    known: Arc<parking_lot::Mutex<HashMap<String, Known>>>,
    flights: Arc<Flights>,
}

impl CloudRunRunner {
    pub fn new(google: Google, gcp: GcpPlatform, broker_url: String, install: weft_core::infra::Install) -> Self {
        Self {
            google,
            gcp,
            broker_url,
            install,
            deploying: Arc::default(),
            known: Arc::default(),
            flights: Arc::new(Flights::new(CALLER_HOLD)),
        }
    }

    fn run_base(&self) -> String {
        format!("https://run.googleapis.com/v2/projects/{}/locations/{}", self.gcp.project, self.gcp.region)
    }

    async fn ensure_account(&self, project: uuid::Uuid) -> anyhow::Result<String> {
        crate::accounts::ensure_project_account(&self.google, &self.gcp, project, &[Access::CallerTokenSecret]).await
    }

    /// Create or update the service or job at `url` to `body`, whole
    /// (Cloud Run's `allowMissing` makes one call do both, so dispatcher
    /// copies deploying the same one at once never collide on "already
    /// exists"), and wait for it. Each try is its own [`attempt`].
    async fn upsert(&self, url: &str, body: &Value, kind: Kind) -> anyhow::Result<()> {
        let call = format!("{url}?allowMissing=true");
        // One bounded wait covers both things that settle on their own: a
        // new project account (and its grants) reaching Cloud Run, and
        // another copy's change to the same one still going (409).
        crate::accounts::until_settled(
            |e| is_status(e, 409),
            || async {
                let op = self.google.patch(&call, &attempt(body, kind)).await?;
                self.google.wait("https://run.googleapis.com/v2", op).await
            },
        )
        .await
        .map(|_| ())
    }

    fn container(&self, target: &WorkerTarget, long: bool) -> Value {
        let mut env = vec![
            json!({ "name": "WEFT_PROJECT_ID", "value": target.project.to_string() }),
            json!({ "name": "WEFT_TENANT_ID", "value": target.tenant }),
            json!({ "name": "WEFT_BROKER_URL", "value": self.broker_url }),
            json!({ "name": "WEFT_WORKER_DOOR", "value": "platform" }),
            json!({ "name": "WEFT_WORKER_IDENTITY", "value": "gcp-metadata" }),
            json!({ "name": "WEFT_CALLER_TOKEN_SECRET", "valueSource": { "secretKeyRef": { "secret": self.gcp.caller_token_secret, "version": "latest" } } }),
        ];
        if !long {
            env.push(json!({ "name": "WEFT_SHORT_RUN_CAP_SECS", "value": REQUEST_CAP.as_secs().to_string() }));
        }
        let mut c = json!({
            "image": target.image,
            "env": env,
            "resources": {
                "limits": { "cpu": target.settings.cpu, "memory": target.settings.memory },
            },
        });
        if !long {
            c["ports"] = json!([{ "containerPort": WORKER_PORT }]);
            c["resources"]["cpuIdle"] = json!(!target.settings.cpu_always_allocated);
            c["resources"]["startupCpuBoost"] = json!(target.settings.startup_boost);
        }
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

    fn labels(&self, target: &WorkerTarget) -> Value {
        json!({
            weft_core::infra::INSTALL_LABEL: self.install.label_value(),
            "weft-project": target.project.simple().to_string(),
            "weft-image": names::image_hash(&target.image),
        })
    }

    fn service_body(&self, target: &WorkerTarget, account: &str) -> Value {
        let s: &WorkerSettings = &target.settings;
        json!({
            "labels": self.labels(target),
            "ingress": "INGRESS_TRAFFIC_ALL",
            "invokerIamDisabled": false,
            "template": {
                "serviceAccount": account,
                "scaling": { "minInstanceCount": s.min_instances, "maxInstanceCount": s.max_instances },
                "maxInstanceRequestConcurrency": s.concurrency,
                "timeout": format!("{}s", REQUEST_CAP.as_secs()),
                "vpcAccess": self.vpc(),
                "containers": [self.container(target, false)],
            },
        })
    }


    /// Let weft's own account call the service at `url`. A grant cannot
    /// precede the service it is on, so it follows the deploy; and a copy
    /// stopped between the two leaves a service at its spec that weft may
    /// not call, which is why every read that finds one at its spec runs
    /// this too (a policy read when the grant is there already).
    async fn let_weft_call(&self, url: &str, kind: Kind) -> anyhow::Result<()> {
        if kind == Kind::Service {
            add_binding(&self.google, url, "roles/run.invoker", &format!("serviceAccount:{}", self.gcp.core_service_account)).await?;
        }
        Ok(())
    }

    /// Where the service or job `name` at `url` is reached, when it
    /// stands deployed at `spec` and weft may call it; `None` when a
    /// deploy is due. A spec whose latest revision failed for a lasting
    /// reason is an error, read from Cloud Run on every call (so every
    /// dispatcher copy sees it) until a new build changes the spec:
    /// deploying the same spec again would only fail again.
    ///
    /// A resource still settling at the spec is waited on (bounded: an
    /// internal wait on Cloud Run, never on the user) and read again. Only
    /// a bring-up task runs this ([`Flights`]), so one poller per name
    /// reads Cloud Run however many callers wait.
    async fn usable_at(&self, name: &str, url: &str, spec: &str, kind: Kind) -> anyhow::Result<Option<String>> {
        let started = tokio::time::Instant::now();
        loop {
            let Some(found) = self.google.get_opt(url).await? else { return Ok(None) };
            match standing(&found, spec, kind) {
                Standing::Settling if started.elapsed() < SETTLE_WAIT => tokio::time::sleep(SETTLE_POLL).await,
                Standing::Settling => {
                    anyhow::bail!("{name} is still settling after {}s at Cloud Run; look at its latest revision in the console", SETTLE_WAIT.as_secs())
                }
                Standing::Due => return Ok(None),
                Standing::Failed(why) => {
                    anyhow::bail!("the program's worker ({name}) failed to start: {why}; fix the program and build again")
                }
                Standing::Ready => {
                    self.let_weft_call(url, kind).await?;
                    return kind
                        .address(url, &found)
                        .map(Some)
                        .ok_or_else(|| anyhow::anyhow!("{name} is ready but Cloud Run gives it no address"));
                }
            }
        }
    }

    /// What this process last saw deployed as `name` at `spec`.
    fn remembered(&self, name: &str, spec: &str) -> Option<String> {
        self.known.lock().get(name).filter(|k| k.spec == spec).map(|k| k.address.clone())
    }

    fn remember(&self, name: &str, target: &WorkerTarget, spec: &str, address: &str) {
        self.known.lock().insert(
            name.to_string(),
            Known { spec: spec.to_string(), address: address.to_string(), project: target.project, image_hash: names::image_hash(&target.image) },
        );
    }

    /// Deploy (or update) the service or job `name` at `url` to `body`,
    /// answering its address.
    ///
    /// The resource carries a label naming the spec it was deployed at,
    /// written in the same call as the body it names (see [`labelled`]).
    ///
    /// Past the memo, the work is one shared bring-up per name and spec
    /// ([`Flights`]); `patience` is how long this caller waits on it.
    async fn ensure(
        &self,
        target: &WorkerTarget,
        name: &str,
        url: &str,
        body: &Value,
        kind: Kind,
        patience: Patience,
    ) -> anyhow::Result<String> {
        let spec = spec_of(body);
        if let Some(known) = self.remembered(name, &spec) {
            return Ok(known);
        }
        let this = self.clone();
        let (target, owned_name, url, body, owned_spec) = (target.clone(), name.to_string(), url.to_string(), body.clone(), spec.clone());
        let rx = self.flights.join((name.to_string(), spec), move || async move {
            this.bring_up(&target, &owned_name, &url, &body, &owned_spec, kind).await
        });
        self.flights.wait(rx, patience, name).await
    }

    /// The one bring-up of `name` at `spec`: deploy it when due, wait for
    /// it to settle, and remember its address.
    async fn bring_up(&self, target: &WorkerTarget, name: &str, url: &str, body: &Value, spec: &str, kind: Kind) -> anyhow::Result<String> {
        let gate = self.deploying.lock().entry(name.to_string()).or_default().clone();
        let deployed = self.deploy_once(target, name, url, body, spec, kind, &gate).await;
        // The gate is only for deploys in flight; the map keeps nothing
        // once the last of them is done.
        let mut deploying = self.deploying.lock();
        if deploying.get(name).is_some_and(|g| Arc::ptr_eq(g, &gate) && Arc::strong_count(g) == 2) {
            deploying.remove(name);
        }
        drop(deploying);
        let at = deployed?;
        self.remember(name, target, spec, &at);
        Ok(at)
    }

    #[allow(clippy::too_many_arguments)]
    async fn deploy_once(
        &self,
        target: &WorkerTarget,
        name: &str,
        url: &str,
        body: &Value,
        spec: &str,
        kind: Kind,
        gate: &tokio::sync::Mutex<()>,
    ) -> anyhow::Result<String> {
        let _one = gate.lock().await;
        if let Some(at) = self.usable_at(name, url, spec, kind).await? {
            return Ok(at);
        }
        self.ensure_account(target.project).await?;
        // A service at its spec whose latest revision failed while the
        // project's account was still spreading lands here too, and the
        // fresh attempt redeploys it (see [`standing`]).
        self.upsert(url, &labelled(body, spec), kind)
            .await
            .map_err(|e| e.context(format!("deploy {name} for the workers of project {}", target.project)))?;
        self.usable_at(name, url, spec, kind)
            .await?
            .ok_or_else(|| anyhow::anyhow!("{name} is deployed but has no address or is not ready"))
    }

    /// Deploy (or update) the service for `target`, answering its URL,
    /// waiting on the bring-up as `patience` says.
    async fn ensure_service(&self, target: &WorkerTarget, patience: Patience) -> anyhow::Result<String> {
        target.settings.validate().map_err(|e| anyhow::anyhow!("project {}: {e}", target.project))?;
        let name = names::worker_service(target.project, &target.image);
        let url = format!("{}/services/{name}", self.run_base());
        let account = names::project_account_email(target.project, &self.gcp.project);
        let body = self.service_body(target, &account);
        self.ensure(target, &name, &url, &body, Kind::Service, patience).await
    }

    /// Deploy (or update) the long-run job for `target`, answering its
    /// resource URL.
    async fn ensure_job(&self, target: &WorkerTarget, patience: Patience) -> anyhow::Result<String> {
        let name = names::worker_job(target.project, &target.image);
        let url = format!("{}/jobs/{name}", self.run_base());
        let account = names::project_account_email(target.project, &self.gcp.project);
        let body = self.job_body(target, &account);
        self.ensure(target, &name, &url, &body, Kind::Job, patience).await
    }

    fn job_body(&self, target: &WorkerTarget, account: &str) -> Value {
        json!({
            "labels": self.labels(target),
            "template": {
                "taskCount": 1,
                "template": {
                    "serviceAccount": account,
                    "timeout": format!("{JOB_CAP_SECS}s"),
                    // A long run that fails is the run's own ending; a
                    // retry would start it again from nothing.
                    "maxRetries": 0,
                    "vpcAccess": self.vpc(),
                    "containers": [self.container(target, true)],
                },
            },
        })
    }

    /// Forget what this process remembered about deployments matching
    /// `drop`, so the memo never outlives what it describes.
    fn forget_where(&self, drop: impl Fn(&Known) -> bool) {
        self.known.lock().retain(|_, k| !drop(k));
    }

    /// The names of the services and jobs labeled `label=value`.
    async fn labeled(&self, kind: &str, label: &str, value: &str) -> anyhow::Result<Vec<String>> {
        let mut out = Vec::new();
        let mut page: Option<String> = None;
        loop {
            let mut query = vec![("pageSize", "100".to_string())];
            if let Some(token) = page.take() {
                query.push(("pageToken", token));
            }
            let listed = self.google.get_query(&format!("{}/{kind}", self.run_base()), &query).await?;
            for item in listed.get(kind).and_then(Value::as_array).into_iter().flatten() {
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
}

#[async_trait]
impl Runner for CloudRunRunner {
    async fn prepare(&self, target: &WorkerTarget) -> anyhow::Result<()> {
        self.ensure_service(target, Patience::ToTheEnd).await.map(|_| ())
    }

    async fn endpoint(&self, target: &WorkerTarget, patience: Patience) -> anyhow::Result<WorkerEndpoint> {
        let base_url = self.ensure_service(target, patience).await?;
        let bearer = self.google.tokens().id_token(&base_url).await?;
        Ok(WorkerEndpoint { base_url, bearer, hold: None })
    }

    fn forget_address(&self, target: &WorkerTarget) {
        self.known.lock().remove(&names::worker_service(target.project, &target.image));
    }

    async fn start_long(&self, target: &WorkerTarget, execution_id: uuid::Uuid, patience: Patience) -> anyhow::Result<()> {
        let job = self.ensure_job(target, patience).await?;
        let started = self
            .google
            .post(
                &format!("{job}:run"),
                &json!({ "overrides": { "containerOverrides": [{ "args": ["--run", execution_id.to_string()] }] } }),
            )
            .await;
        if started.as_ref().is_err_and(|e| is_status(e, 404)) {
            // Deleted behind this process's back: the next start deploys it.
            self.known.lock().remove(&names::worker_job(target.project, &target.image));
        }
        started.map_err(|e| e.context(format!("start the long run {execution_id}")))?;
        Ok(())
    }

    async fn retire(&self, _tenant: &str, project: uuid::Uuid) -> anyhow::Result<()> {
        let value = project.simple().to_string();
        let mut all = self.labeled("services", "weft-project", &value).await?;
        all.extend(self.labeled("jobs", "weft-project", &value).await?);
        self.forget_where(|k| k.project == project);
        self.delete_all(all).await?;
        crate::accounts::revoke_project_account(&self.google, &self.gcp, project).await
    }

    async fn forget_image(&self, image: &str) -> anyhow::Result<()> {
        let hash = names::image_hash(image);
        let mut all = self.labeled("services", "weft-image", &hash).await?;
        all.extend(self.labeled("jobs", "weft-image", &hash).await?);
        self.forget_where(|k| k.image_hash == hash);
        self.delete_all(all).await
    }

    fn short_run_cap(&self) -> Option<Duration> {
        Some(REQUEST_CAP)
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
            tasks_queue: "weft-wakes".into(),
            core_service_account: "weft-core@acme.iam.gserviceaccount.com".into(),
            holder_pool: "projects/acme/locations/us-central1/workerPools/weft-holder".into(),
            dispatcher_service: "weft-role-dispatcher".into(),
            runtime_image: "r".into(),
            deployer_service_account: "d".into(),
            frontend_service_account: "f".into(),
            workload_identity_provider: "w".into(),
            caller_token_secret: "weft-caller-token-secret".into(),
            infra_network_tag: "weft-infra".into(),
        }
    }

    fn target(settings: WorkerSettings) -> WorkerTarget {
        WorkerTarget { tenant: "local".into(), project: uuid::Uuid::from_u128(3), image: "us-central1-docker.pkg.dev/acme/weft/weft-worker:ab".into(), settings }
    }

    #[test]
    fn a_service_carries_every_worker_lever_and_scales_to_zero_by_default() {
        let r = CloudRunRunner::new(Google::new(Arc::new(crate::metadata::MetadataTokens::new())), gcp(), "http://10.10.0.2:14113/broker".into(), weft_core::infra::Install::default_install());
        let body = r.service_body(&target(WorkerSettings::default()), "wp-x@acme.iam.gserviceaccount.com");
        let t = &body["template"];
        assert_eq!(t["scaling"]["minInstanceCount"], 0);
        assert_eq!(t["scaling"]["maxInstanceCount"], 10);
        assert_eq!(t["maxInstanceRequestConcurrency"], 20);
        assert_eq!(t["timeout"], "3600s");
        assert_eq!(t["serviceAccount"], "wp-x@acme.iam.gserviceaccount.com");
        assert_eq!(t["vpcAccess"]["networkInterfaces"][0]["subnetwork"], "weft-sub", "workers leave from the install's main subnet");
        let c = &t["containers"][0];
        assert_eq!(c["resources"]["cpuIdle"], true);
        let env = c["env"].as_array().unwrap();
        let var = |n: &str| env.iter().find(|e| e["name"] == n).cloned().unwrap();
        assert_eq!(var("WEFT_WORKER_DOOR")["value"], "platform");
        assert_eq!(var("WEFT_BROKER_URL")["value"], "http://10.10.0.2:14113/broker");
        assert_eq!(var("WEFT_CALLER_TOKEN_SECRET")["valueSource"]["secretKeyRef"]["secret"], "weft-caller-token-secret");
        assert_eq!(var("WEFT_SHORT_RUN_CAP_SECS")["value"], "3600");
    }

    /// The label is part of the body written, and the spec it names is
    /// the body without it, so a read compares like with like.
    #[test]
    fn the_spec_label_rides_the_body_it_names() {
        let r = CloudRunRunner::new(Google::new(Arc::new(crate::metadata::MetadataTokens::new())), gcp(), "b".into(), weft_core::infra::Install::default_install());
        for body in [r.service_body(&target(WorkerSettings::default()), "a"), r.job_body(&target(WorkerSettings::default()), "a")] {
            let spec = spec_of(&body);
            let written = labelled(&body, &spec);
            assert_eq!(written["labels"][SPEC_LABEL], spec.as_str());
            assert_eq!(written["labels"]["weft-project"], body["labels"]["weft-project"]);
            assert_eq!(written["template"], body["template"]);
        }
    }

    /// Two attempts at one body differ in the service's template (so each
    /// is a new revision) and agree on everything else, the spec label
    /// included. A job is written as is.
    #[test]
    fn every_attempt_is_a_new_revision_of_the_same_spec() {
        let r = CloudRunRunner::new(Google::new(Arc::new(crate::metadata::MetadataTokens::new())), gcp(), "b".into(), weft_core::infra::Install::default_install());
        let body = labelled(&r.service_body(&target(WorkerSettings::default()), "a"), "s");
        let (one, two) = (attempt(&body, Kind::Service), attempt(&body, Kind::Service));
        assert_ne!(one["template"], two["template"]);
        assert_eq!(one["labels"], body["labels"]);
        let strip = |mut v: Value| {
            v["template"].as_object_mut().unwrap().remove("annotations");
            v
        };
        assert_eq!(strip(one), body);
        let job = r.job_body(&target(WorkerSettings::default()), "a");
        assert_eq!(attempt(&job, Kind::Job), job);
    }

    fn service(state: &str, ready: &str, created: &str, conditions: Value) -> Value {
        json!({
            "labels": { SPEC_LABEL: "s" },
            "terminalCondition": { "type": "Ready", "state": state },
            "latestReadyRevision": ready,
            "latestCreatedRevision": created,
            "conditions": conditions,
        })
    }

    #[test]
    fn a_failed_revision_redeploys_only_while_the_account_spreads() {
        let failed_rev = |reason: &str, message: &str| {
            json!([{ "type": "RoutesReady", "state": "CONDITION_FAILED", "reason": reason, "message": message }])
        };
        assert_eq!(standing(&service("CONDITION_SUCCEEDED", "r1", "r1", json!([])), "s", Kind::Service), Standing::Ready);
        assert_eq!(standing(&service("CONDITION_SUCCEEDED", "r1", "r1", json!([])), "other", Kind::Service), Standing::Due, "another spec");
        let mut settling = service("CONDITION_RECONCILING", "r1", "r2", json!([]));
        assert_eq!(standing(&settling, "s", Kind::Service), Standing::Settling, "another copy's deploy in flight");
        let mut reconciling = service("CONDITION_SUCCEEDED", "r1", "r1", json!([]));
        reconciling["reconciling"] = json!(true);
        assert_eq!(standing(&reconciling, "s", Kind::Service), Standing::Settling);
        settling["terminalCondition"]["state"] = json!("CONDITION_PENDING");
        assert_eq!(standing(&settling, "s", Kind::Service), Standing::Settling);
        assert_eq!(standing(&settling, "other", Kind::Service), Standing::Due, "a settling deploy of another spec is replaced");
        let spreading = service("CONDITION_FAILED", "", "r1", failed_rev("SECRETS_ACCESS_CHECK_FAILED", ""));
        assert_eq!(standing(&spreading, "s", Kind::Service), Standing::Due);
        let spreading = service("CONDITION_FAILED", "", "r1", failed_rev("UNKNOWN", "Permission denied on secret: projects/acme/secrets/s/versions/latest"));
        assert_eq!(standing(&spreading, "s", Kind::Service), Standing::Due);
        let crash = service("CONDITION_FAILED", "", "r1", failed_rev("UNKNOWN", "The user-provided container failed to start and listen on the port"));
        assert_eq!(standing(&crash, "s", Kind::Service), Standing::Failed("The user-provided container failed to start and listen on the port".into()));
        let older_serves = service("CONDITION_FAILED", "r1", "r2", failed_rev("CONTAINER_MISSING", ""));
        assert_eq!(standing(&older_serves, "s", Kind::Service), Standing::Failed("Cloud Run reports it failed (CONTAINER_MISSING)".into()));
        let older_serves_quietly = service("CONDITION_SUCCEEDED", "r1", "r2", json!([]));
        assert!(matches!(standing(&older_serves_quietly, "s", Kind::Service), Standing::Failed(_)), "an older revision runs other settings");
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
        assert_eq!(format!("{:#}", flights.wait(failed, Patience::ToTheEnd, "svc").await.unwrap_err()), "the revision crashed");
    }

    #[test]
    fn a_long_run_has_no_port_and_no_short_cap() {
        let r = CloudRunRunner::new(Google::new(Arc::new(crate::metadata::MetadataTokens::new())), gcp(), "b".into(), weft_core::infra::Install::default_install());
        let c = r.container(&target(WorkerSettings::default()), true);
        assert!(c.get("ports").is_none());
        assert!(!c["env"].as_array().unwrap().iter().any(|e| e["name"] == "WEFT_SHORT_RUN_CAP_SECS"));
    }
}
