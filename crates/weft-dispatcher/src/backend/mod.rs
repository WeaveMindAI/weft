//! Pluggable backends.
//!
//! `WorkerBackend` spawns worker pods: `K8sWorkerBackend` is the production impl,
//! with a subprocess impl for tests. `ImageBuilder` is a capability seam: turn a
//! staged build context into a pullable worker image. When a build daemon is
//! supplied an impl is present; when worker images are built ahead of time there
//! is no impl and no build task is ever enqueued.
//!
//! Infra provisioning is not a backend here: the dispatcher routes
//! intent through the `infra_lifecycle_command` table, and the
//! per-tenant infra supervisor pod claims those rows and runs
//! kubectl. The dispatcher itself never shells kubectl for user
//! infra.

pub mod k8s_worker;

pub use k8s_worker::K8sWorkerBackend;

use std::path::PathBuf;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[async_trait]
pub trait WorkerBackend: Send + Sync {
    /// Spawn a worker Pod for `spec.project_id`'s pool. The Pod claims
    /// any pending `target=worker` task scoped to its project; one Pod
    /// multiplexes many concurrent executions. Cold-start path called
    /// by the dispatcher when no live Pod exists for the project.
    ///
    /// Pod name is chosen by the caller (deterministic from the
    /// spawn task id) so a partial-success retry collides on the
    /// same name instead of creating a second Pod.
    async fn spawn_pod(
        &self,
        pod_name: &str,
        spec: SpawnPodSpec,
    ) -> anyhow::Result<WorkerHandle>;

    async fn kill_pod(&self, pod_name: String, namespace: String) -> anyhow::Result<()>;
}

/// Spec for spawning a worker Pod. The Pod runs the content-addressed
/// worker image (`weft-worker:<binary_hash>`, deduped across projects /
/// tenants), claims tasks for that project, and scale-to-zeros when idle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpawnPodSpec {
    pub project_id: String,
    pub tenant: String,
    /// The PROJECT namespace (`wft-project-<tenant>-<project>`), not
    /// the tenant namespace. Workers live in the project namespace
    /// so cross-project lateral access is contained at the k8s
    /// boundary.
    pub namespace: String,
    /// Dispatcher Pod id stamped on the worker_pod row for traceability.
    pub owner_dispatcher: String,
    /// Binary hash that identifies which worker image to pull.
    /// None when the project has never been built/registered with
    /// a hash by the CLI; backends should fail loudly in that case
    /// rather than fall back to `:latest` so a misconfigured CLI
    /// doesn't silently spawn the wrong image.
    pub binary_hash: Option<String>,
    /// Hex-encoded HMAC secret the worker verifies live-connection
    /// routing tokens with (the dispatcher signs them with the same
    /// secret). Empty string when live connections aren't provisioned;
    /// the worker then rejects every token loud (never accepts unsigned).
    pub caller_token_secret_hex: String,
}

#[derive(Debug, Clone)]
pub struct WorkerHandle {
    /// k8s pod name minted by the spawn (worker_pod PRIMARY KEY).
    pub pod_name: String,
}

/// Turns a staged build context into a pullable worker image. The seam is
/// split-phase: `start` submits and returns a job id immediately; `poll` asks "is
/// that job done?". The split lets a build survive a dispatcher restart: the job
/// id is persisted before we poll, so a restarted dispatcher re-polls by the
/// recorded id rather than losing track of a build still running.
///
/// The dispatcher compiles + stages the context itself, so the builder only runs
/// the build of a ready context. The builder NEVER calls back into the dispatcher.
#[async_trait]
pub trait ImageBuilder: Send + Sync {
    /// Submit a staged context for building + pushing to `req.image_ref`. Returns
    /// the builder's job id immediately (the build runs asynchronously on the
    /// builder's side). The caller persists the returned `external_build_id`
    /// before polling so a restart can recover.
    async fn start(&self, req: BuildRequest) -> anyhow::Result<BuildHandle>;

    /// Ask the builder whether `handle`'s job is done. Recoverable from the
    /// recorded id alone (a restarted dispatcher re-polls without re-`start`).
    async fn poll(&self, handle: &BuildHandle) -> anyhow::Result<BuildStatus>;

    /// Whether `image_ref` ALREADY exists in the registry. The check-then-build
    /// skip: worker images are content-addressed (`weft-worker:<binary_hash>`), so
    /// if a byte-identical build already happened (same nodes + same code, even by
    /// a different tenant), the image is already there and the build is skipped
    /// entirely. Querying the registry is builder-specific (it owns the registry
    /// auth), so it lives on the builder, not the dispatcher.
    async fn image_exists(&self, image_ref: &str) -> anyhow::Result<bool>;

    /// Interrupt `handle`'s in-flight build: stop the build work (kill the
    /// builder process / session) so a subsequent `poll` reports
    /// `BuildStatus::Cancelled`. Idempotent; cancelling an already-terminal
    /// job is a no-op. Best-effort on the builder side (a cancel that races
    /// completion leaves the job Succeeded, which `poll` reports honestly).
    async fn cancel(&self, handle: &BuildHandle) -> anyhow::Result<()>;
}

/// The control point a `ProjectBuilder` calls around REAL build work, so the
/// dispatcher's `building` transition only engages when something actually
/// builds (a pure cache-hit verb never flips the marker, so concurrent runs on
/// an up-to-date project never serialize against each other).
///
/// The mechanism behind the gate is weft's (the project row's `transition`
/// marker + heartbeat + the stuck-transition reaper); the KNOWLEDGE of "a real
/// build is starting" is the builder's. This trait is the seam between them.
#[async_trait]
pub trait BuildGate: Send + Sync {
    /// Called once, just before the first actual image build is submitted.
    /// Errs when the project cannot enter the `building` transition right now
    /// (another verb is already building, or the lifecycle is mid-flip); the
    /// builder aborts with that error and the verb surfaces it.
    async fn begin(&self) -> anyhow::Result<()>;

    /// Whether the user requested cancellation of this build. Polled by the
    /// builder's await loop; on `true` the builder interrupts the in-flight
    /// build (`ImageBuilder::cancel`) and errs with a cancellation message.
    async fn cancel_requested(&self) -> anyhow::Result<bool>;
}

/// Ensures a project's LATEST saved source is built + registered so it is runnable,
/// building on demand if it is not. This is the seam a verb (`run` / `activate` /
/// infra start) consults BEFORE acting, so clicking a verb on a not-yet-built (or
/// edited-since-built) project just works: the verb triggers the build.
///
/// The default is NO impl (`ensure_built` is `None` on the state): when the CLI
/// compiles + builds + registers before it calls the verb, the dispatcher's
/// stored `running_definition_hash` is already current and there is nothing to
/// do. An impl that builds from an uploaded source tree compiles the latest tree,
/// builds any stale image, and registers, so the verb no longer 412s on a stub.
/// Idempotent: a no-op when the running build already matches the latest source,
/// so a verb on an up-to-date project pays only a cheap freshness check.
#[async_trait]
pub trait ProjectBuilder: Send + Sync {
    /// Make `project_id`'s latest saved source runnable: build (if stale) + register.
    /// Returns when the project has a current `running_definition_hash`, or errs with
    /// a user-facing reason (compile error, build failure, cancelled) the verb
    /// surfaces. `gate` MUST be consulted around real build work: call
    /// `gate.begin()` before the first image build is submitted (abort on Err),
    /// and poll `gate.cancel_requested()` while awaiting a build (interrupt +
    /// err on true). A pure cache-hit path never touches the gate.
    async fn ensure_built(
        &self,
        project_id: uuid::Uuid,
        gate: &dyn BuildGate,
    ) -> anyhow::Result<()>;

    /// Best-effort interrupt of any in-flight build for `project_id` (kill the
    /// builder job so the driving verb's await loop sees it terminal quickly).
    /// The DURABLE cancel signal is the project row's `cancelling_build`
    /// transition, which the driving pod polls via its gate; this call just
    /// shortens the wait when the cancel lands on the same pod that runs the
    /// build. `Ok(true)` iff a local in-flight build was found and interrupted.
    async fn cancel_build(&self, project_id: uuid::Uuid) -> anyhow::Result<bool>;
}

/// A staged build context to submit. `context_dir` is the directory the
/// compiler produced (`weft_compiler::build::build_project` → the staged
/// context: generated Dockerfile + generated crate + referenced node source);
/// the builder tars it and runs `docker build -f context_dir/Dockerfile`.
/// `image_ref` is the registry-qualified, CONTENT-addressed target tag
/// (`<registry>/weft-worker:<binary_hash>`) the builder pushes to and the
/// worker later pulls. The dispatcher mints it once (registry URL + binary
/// hash) so push and pull read one source of truth.
#[derive(Debug, Clone)]
pub struct BuildRequest {
    pub project_id: String,
    pub tenant: String,
    pub context_dir: PathBuf,
    pub image_ref: String,
}

/// The builder's own job id (e.g. a build daemon's job id), persisted on
/// the `image_build` row so the poll survives a dispatcher restart.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildHandle {
    pub external_build_id: String,
}

/// The result of polling a build job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildStatus {
    /// Still running on the builder's side; poll again later.
    Pending,
    /// Pushed `image_ref`; `image_digest` is the registry content digest.
    Succeeded { image_digest: String },
    /// The build failed; `reason` is the builder's error, surfaced to status.
    Failed { reason: String },
    /// The build was interrupted by `ImageBuilder::cancel`. Terminal; the
    /// awaiting verb surfaces "build cancelled" rather than a failure.
    Cancelled,
}

/// A dumb in-memory `ImageBuilder` for layer-3 tests: records every call and
/// returns scripted results. No business logic (no real build), just records +
/// replays, per the testing-pyramid fakes rule.
#[cfg(any(test, feature = "test-helpers"))]
#[derive(Default)]
pub struct FakeImageBuilder {
    inner: std::sync::Mutex<FakeImageBuilderInner>,
}

#[cfg(any(test, feature = "test-helpers"))]
#[derive(Default)]
struct FakeImageBuilderInner {
    /// Image refs that `image_exists` reports present (the check-then-build hit set).
    existing: std::collections::HashSet<String>,
    /// The status `poll` returns, keyed by external_build_id. Absent => Pending.
    poll_results: std::collections::HashMap<String, BuildStatus>,
    /// The build id `start` mints next (incremented per start).
    next_id: u64,
    /// Append-only call log for assertions.
    starts: Vec<BuildRequest>,
    polls: Vec<String>,
    exists_checks: Vec<String>,
    cancels: Vec<String>,
}

#[cfg(any(test, feature = "test-helpers"))]
impl FakeImageBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark an image ref as already present (so `image_exists` returns true and
    /// the executor takes the check-then-build skip path).
    pub fn set_image_exists(&self, image_ref: &str) {
        self.inner.lock().unwrap().existing.insert(image_ref.to_string());
    }

    /// Script the status `poll` returns for a given build id.
    pub fn set_poll_result(&self, external_build_id: &str, status: BuildStatus) {
        self.inner
            .lock()
            .unwrap()
            .poll_results
            .insert(external_build_id.to_string(), status);
    }

    pub fn starts(&self) -> Vec<BuildRequest> {
        self.inner.lock().unwrap().starts.clone()
    }

    pub fn polls(&self) -> Vec<String> {
        self.inner.lock().unwrap().polls.clone()
    }

    pub fn exists_checks(&self) -> Vec<String> {
        self.inner.lock().unwrap().exists_checks.clone()
    }

    pub fn cancels(&self) -> Vec<String> {
        self.inner.lock().unwrap().cancels.clone()
    }
}

#[cfg(any(test, feature = "test-helpers"))]
#[async_trait]
impl ImageBuilder for FakeImageBuilder {
    async fn start(&self, req: BuildRequest) -> anyhow::Result<BuildHandle> {
        let mut inner = self.inner.lock().unwrap();
        inner.next_id += 1;
        let id = format!("fake-build-{}", inner.next_id);
        inner.starts.push(req);
        Ok(BuildHandle { external_build_id: id })
    }

    async fn poll(&self, handle: &BuildHandle) -> anyhow::Result<BuildStatus> {
        let mut inner = self.inner.lock().unwrap();
        inner.polls.push(handle.external_build_id.clone());
        Ok(inner
            .poll_results
            .get(&handle.external_build_id)
            .cloned()
            .unwrap_or(BuildStatus::Pending))
    }

    async fn image_exists(&self, image_ref: &str) -> anyhow::Result<bool> {
        let mut inner = self.inner.lock().unwrap();
        inner.exists_checks.push(image_ref.to_string());
        Ok(inner.existing.contains(image_ref))
    }

    async fn cancel(&self, handle: &BuildHandle) -> anyhow::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.cancels.push(handle.external_build_id.clone());
        // Record + flip: a cancelled fake job polls Cancelled from here on,
        // matching the production contract.
        inner
            .poll_results
            .insert(handle.external_build_id.clone(), BuildStatus::Cancelled);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_handle_wire_round_trips() {
        // The handle is persisted on the image_build row (its external_build_id)
        // and reconstructed by the sweep, so its wire shape is load-bearing.
        let h = BuildHandle { external_build_id: "build-op-42".into() };
        let v = serde_json::to_value(&h).unwrap();
        assert_eq!(v["external_build_id"], "build-op-42");
        let back: BuildHandle = serde_json::from_value(v).unwrap();
        assert_eq!(back.external_build_id, "build-op-42");
    }

    #[tokio::test]
    async fn fake_builder_records_and_replays() {
        let b = FakeImageBuilder::new();
        // image_exists reflects the configured set.
        b.set_image_exists("reg/weft-worker:present");
        assert!(b.image_exists("reg/weft-worker:present").await.unwrap());
        assert!(!b.image_exists("reg/weft-worker:absent").await.unwrap());
        assert_eq!(b.exists_checks().len(), 2);

        // start mints an id and records the request.
        let handle = b
            .start(BuildRequest {
                project_id: "p".into(),
                tenant: "t".into(),
                context_dir: std::path::PathBuf::from("/tmp/ctx"),
                image_ref: "reg/weft-worker:x".into(),
            })
            .await
            .unwrap();
        assert_eq!(b.starts().len(), 1);

        // poll returns Pending until scripted, then the scripted terminal status.
        assert_eq!(b.poll(&handle).await.unwrap(), BuildStatus::Pending);
        b.set_poll_result(
            &handle.external_build_id,
            BuildStatus::Succeeded { image_digest: "sha256:abc".into() },
        );
        assert_eq!(
            b.poll(&handle).await.unwrap(),
            BuildStatus::Succeeded { image_digest: "sha256:abc".into() }
        );
    }
}
