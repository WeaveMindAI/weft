//! Where a project's workers run, and how weft reaches them.
//!
//! A worker is the project's compiled program serving HTTP. Weft does not
//! keep workers running and hand them work from a queue: it CALLS a worker
//! for each run (`POST /run/<run_id>`, or the caller's own
//! forwarded request for a live one), and the worker answers when the
//! run ends or waits on something outside it. That is the whole
//! protocol, and it is weft's, the same on every platform (see
//! `weft_engine::worker_server`).
//!
//! What differs per platform is only where that HTTP server lives and how
//! to get an address for it, which is what this trait answers: a process
//! on the machine started on demand, or a Cloud Run service.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use weft_core::workers::WorkerSettings;

/// Everything the platform needs to run one project's workers at one
/// image. A project can have several images live at once (a new build
/// while older runs still resume on the image they started on), so
/// the image is part of the target, not a property of the project.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerTarget {
    pub tenant: String,
    pub project: uuid::Uuid,
    /// The worker to run, by its image ref (content-addressed by its
    /// binary hash): an image on the cloud, a program compiled on the
    /// machine on a local install (`weft_core::builds::WORKER_REPO`).
    pub image: String,
    /// The program's binary the worker holds, whose routes the worker
    /// serves; `None` for a node-test server, which serves no routes.
    pub binary_hash: Option<String>,
    pub settings: WorkerSettings,
}

/// Where each project's folder is on this machine: its folder working
/// copy's path, when that working copy is on this machine. A platform
/// whose workers run as processes on the machine runs each in its
/// project's folder, and asks only when it starts one.
#[async_trait]
pub trait ProjectFolders: Send + Sync {
    /// `project`'s folder on this machine; `None` when it has none here.
    async fn folder(&self, project: uuid::Uuid) -> anyhow::Result<Option<std::path::PathBuf>>;
}

/// The exit code a worker ends with when it cannot listen on its port
/// because another program holds it, after printing
/// [`WORKER_PORT_TAKEN_LINE`]: how a runner that started it on a port it
/// was asked for tells a taken port ([`PortTaken`]) from any other end.
/// The worker's own bind (`weft_engine::listen`) prints and exits with these.
pub const WORKER_PORT_TAKEN_EXIT: i32 = 98;

/// The line a worker prints, followed by its address, when its port is
/// held by another program, just before it exits with
/// [`WORKER_PORT_TAKEN_EXIT`].
pub const WORKER_PORT_TAKEN_LINE: &str = "weft worker: the port is held by another program:";

/// The header weft's own calls to a worker carry their credential in.
///
/// Not `Authorization`: a live caller's request is forwarded to the worker
/// with its headers intact (they are the program's data), and a caller's
/// own `Authorization` must reach the program untouched. Not Cloud Run's
/// `X-Serverless-Authorization` either: a project's service takes calls
/// from anybody, and the platform reads that one as its own. The worker
/// takes it out of every call before the program sees it.
// SYNC: WORKER_AUTH_HEADER <-> crates/weft-engine/src/worker.rs (WeftCredential::admits_headers)
pub const WORKER_AUTH_HEADER: &str = "x-weft-worker-key";

/// The header a worker's server sets on every answer it gives, so weft
/// can tell an answer from the program (a route's own 404 included) from
/// one the platform gave in its place (Cloud Run's 404 for a service that
/// is gone, a proxy's 502). Only an answer without it can say the address
/// is gone ([`WorkerCall::address_is_gone`]).
// SYNC: WORKER_ANSWER_HEADER <-> crates/weft-engine/src/worker.rs (mark_worker_answer)
pub const WORKER_ANSWER_HEADER: &str = "x-weft-worker";

/// How one call to a worker's address ended, as far as the address goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerCall {
    /// No answer came back. `connect` is a failure to reach the address
    /// at all (no such host, connection refused), as opposed to one that
    /// broke later (a timeout, a cut body).
    NoAnswer { connect: bool },
    /// Something answered with `status`; `from_worker` is whether it
    /// carried [`WORKER_ANSWER_HEADER`].
    Answered { status: u16, from_worker: bool },
}

impl WorkerCall {
    /// A call that got no answer back.
    pub fn no_answer(e: &reqwest::Error) -> Self {
        WorkerCall::NoAnswer { connect: e.is_connect() }
    }

    /// A call answered with `status` and `headers`.
    pub fn answered(status: reqwest::StatusCode, headers: &reqwest::header::HeaderMap) -> Self {
        WorkerCall::Answered { status: status.as_u16(), from_worker: headers.contains_key(WORKER_ANSWER_HEADER) }
    }

    /// Whether this call is evidence that the address no longer leads to
    /// the project's workers, so a remembered copy of it must go.
    ///
    /// Only evidence counts: a platform's own "busy" answers (Cloud Run's
    /// 429 when no instance is free, its 503 while scaling up, a proxy's
    /// 502) say nothing about the address, and forgetting on them would
    /// make every overflowing request of a burst pay a platform lookup.
    /// What does count is an address that cannot be reached at all and
    /// the platform's own 404 (the service was deleted). The platform's
    /// 403 does not: the address is still right, and forgetting it would
    /// make every call pay two admin reads for as long as the refusal
    /// lasts. It reaches the caller as [`WorkerCall::platform_refused`].
    pub fn address_is_gone(self) -> bool {
        match self {
            WorkerCall::NoAnswer { connect } => connect,
            WorkerCall::Answered { status, from_worker } => !from_worker && status == 404,
        }
    }

    /// The platform itself refused weft's call (Cloud Run's 403: weft's
    /// account may not invoke the service), as opposed to the program
    /// answering 403. A caller names this in its error, since the program
    /// never saw the call.
    pub fn platform_refused(self) -> bool {
        matches!(self, WorkerCall::Answered { status: 403, from_worker: false })
    }
}

/// How long a caller of [`Runner::endpoint`] will wait on a bring-up of the project's workers. The caller says it,
/// so no caller loops on [`WorkerStarting`] to wait longer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Patience {
    /// Hold for the platform's short hold, then hear [`WorkerStarting`]
    /// while the bring-up goes on. For a caller that can give its work
    /// back and ask again (a delivery under a lease, a live caller).
    Brief,
    /// Wait until the bring-up lands or fails. For a caller whose whole
    /// job is this one call (a node test).
    ToTheEnd,
}

/// The answer [`Runner::endpoint`] gives a
/// [`Patience::Brief`] caller when the project's workers are being
/// brought up (a deploy or its revision still settling) and did not
/// become ready within the platform's short hold. Nothing failed: the bring-up goes on without the caller, so a
/// caller gives its work back and asks again later instead of holding it
/// through a wait of minutes. [`WorkerStarting::is`] recognizes it.
#[derive(Debug)]
pub struct WorkerStarting {
    /// What is starting, in the platform's own name for it.
    pub what: String,
}

impl std::fmt::Display for WorkerStarting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the program's worker ({}) is still starting", self.what)
    }
}

impl std::error::Error for WorkerStarting {}

impl WorkerStarting {
    /// Whether `e` is (or wraps) this answer.
    pub fn is(e: &anyhow::Error) -> bool {
        e.downcast_ref::<WorkerStarting>().is_some()
    }
}

/// The answer [`Runner::endpoint`] gives when the platform refused weft's
/// own call to bring up or find the project's workers (permission denied,
/// a quota reached, a request it calls invalid): a definite no, unlike an
/// API that timed out or never answered. `why` is the platform's answer in
/// words. [`PlatformRefused::is`] recognizes it.
#[derive(Debug)]
pub struct PlatformRefused {
    pub why: String,
}

impl std::fmt::Display for PlatformRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the platform refused weft's call: {}", self.why)
    }
}

impl std::error::Error for PlatformRefused {}

impl PlatformRefused {
    /// Whether `e` is (or wraps) this answer.
    pub fn is(e: &anyhow::Error) -> bool {
        e.downcast_ref::<PlatformRefused>().is_some()
    }
}

/// The answer [`Runner::endpoint`] gives when the workers of the build
/// asked for cannot run at all: started, they exit (a local process, a
/// Cloud Run revision that failed), or the build's program is gone. A definite answer about that build, which no
/// retry changes: what runs on it ends at once and says so, rather than
/// waiting for a worker that will never come. [`WorkerWontStart::is`]
/// recognizes it.
#[derive(Debug)]
pub struct WorkerWontStart {
    /// Why, in the platform's words (its last lines, its reason).
    pub why: String,
}

impl std::fmt::Display for WorkerWontStart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the worker of this build cannot start: {}", self.why)
    }
}

impl std::error::Error for WorkerWontStart {}

impl WorkerWontStart {
    /// Whether `e` is (or wraps) this answer.
    pub fn is(e: &anyhow::Error) -> bool {
        e.downcast_ref::<WorkerWontStart>().is_some()
    }
}

/// The port [`Runner::front`] was asked to publish is held by another
/// program on the machine. [`PortTaken::of`] recognizes it.
#[derive(Debug)]
pub struct PortTaken {
    pub port: u16,
}

impl std::fmt::Display for PortTaken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "port {} is held by another program", self.port)
    }
}

impl std::error::Error for PortTaken {}

impl PortTaken {
    /// The port, when `e` is (or wraps) this answer.
    pub fn of(e: &anyhow::Error) -> Option<u16> {
        e.downcast_ref::<PortTaken>().map(|taken| taken.port)
    }
}

/// Where to reach a project's worker server right now, and with what.
pub struct WorkerEndpoint {
    /// Base URL of the worker's HTTP server (`/_weft/...`, and the live
    /// caller paths).
    pub base_url: String,
    /// The bearer token the worker accepts from weft.
    pub bearer: String,
    /// Kept for as long as the caller talks to the worker: a platform
    /// that stops idle workers itself (the local one) counts these, so a
    /// worker is never stopped under a call it handed out.
    pub hold: Option<Box<dyn std::any::Any + Send + Sync>>,
}

impl WorkerEndpoint {
    /// The value of [`WORKER_AUTH_HEADER`] on a call to this worker.
    pub fn auth_value(&self) -> String {
        worker_auth_value(&self.bearer)
    }
}

/// The value of [`WORKER_AUTH_HEADER`] on a call carrying the worker key
/// `key`.
pub fn worker_auth_value(key: &str) -> String {
    format!("Bearer {key}")
}

/// The worker key a [`WORKER_AUTH_HEADER`] value carries (the reverse of
/// [`worker_auth_value`]).
pub fn worker_auth_key(value: &str) -> Option<&str> {
    value.strip_prefix("Bearer ")
}

impl std::fmt::Debug for WorkerEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerEndpoint").field("base_url", &self.base_url).finish_non_exhaustive()
    }
}

#[async_trait]
pub trait Runner: Send + Sync {
    /// Make the project's workers run `target` (its image and settings).
    /// Idempotent; called when a project activates and whenever its
    /// image or settings change.
    async fn prepare(&self, target: &WorkerTarget) -> anyhow::Result<()>;

    /// An address to call the project's workers at, starting one if the
    /// platform needs that done by hand. A platform whose workers take
    /// minutes to come up waits on it as `patience` says: a
    /// [`Patience::Brief`] caller hears [`WorkerStarting`] after a short
    /// hold rather than being held through the whole bring-up. The
    /// platform refusing the call that brings them up or finds them is
    /// [`PlatformRefused`]; any other error is the platform giving no
    /// answer.
    async fn endpoint(&self, target: &WorkerTarget, patience: Patience) -> anyhow::Result<WorkerEndpoint>;

    /// How a call to the address [`Runner::endpoint`] gave for `target`
    /// went. Every caller of a worker reports every call here, and the one
    /// rule of [`WorkerCall::address_is_gone`] decides whether the address
    /// is forgotten.
    fn call_ended(&self, target: &WorkerTarget, call: WorkerCall) {
        if call.address_is_gone() {
            self.forget_address(target);
        }
    }

    /// The address remembered for `target` is gone. A platform that
    /// remembers addresses to spare itself lookups forgets this one, so
    /// the next `endpoint` asks the platform afresh. Platforms that
    /// remember nothing ignore it.
    fn forget_address(&self, target: &WorkerTarget) {
        let _ = target;
    }

    /// Make `target` the program the project's callers reach at its own
    /// address, straight, with nothing of weft's in between: on a cloud,
    /// the project's service sends its traffic to `target`'s revision; on
    /// a machine, `target`'s worker publishes the project's `port` itself
    /// and is kept running. Whatever served the address before stops (its
    /// runs leave the way they do when the platform stops a worker), so a
    /// caller arriving in that moment is refused and tries again.
    /// Answers the address. A `port` another program holds is
    /// [`PortTaken`], and nothing is started. Idempotent.
    async fn front(&self, target: &WorkerTarget, port: Option<u16>) -> anyhow::Result<weft_core::projects::ProjectAddress>;

    /// `target` is what the project's workers run now: its image with the
    /// settings the project has stored, read from them by the caller, not
    /// from a copy that may predate a change. Let go of whatever the
    /// platform keeps running of that image under other settings (on a
    /// cloud, the revisions it deployed for them, their calls handed to
    /// `target`'s). Only the caller can say which settings are current:
    /// what the platform holds cannot, since a caller holding settings
    /// from before a change may bring them up again after it. Does
    /// nothing until `target` serves. An error names what is left; by
    /// then `target` already serves, so the caller logs it rather than
    /// failing the change that led here, and what is left takes no calls
    /// and goes at the next settling. Idempotent.
    async fn settle(&self, target: &WorkerTarget) -> anyhow::Result<()>;

    /// Stop serving the project's own address: nothing of it takes calls
    /// any more (every trigger it has was wiped). Its workers go once idle.
    async fn let_front_go(&self, project: uuid::Uuid) -> anyhow::Result<()>;

    /// Kill every worker the project has, outright (the runs on them die
    /// with them), and remove what runs them (a service). What removing a project does before it erases a row:
    /// a worker left alive would keep serving from what it remembers of a
    /// project that is gone. An error names what still runs and how to
    /// stop it by hand. Idempotent.
    async fn kill_workers(&self, tenant: &str, project: uuid::Uuid) -> anyhow::Result<()>;

    /// Remove what the platform keeps for the project once its workers
    /// are gone (on a cloud, the account its workers ran as). Idempotent.
    async fn retire(&self, tenant: &str, project: uuid::Uuid) -> anyhow::Result<()>;

    /// Remove what the platform keeps running from `image` (a process,
    /// a service revision), for an image about to be deleted because
    /// nothing references it. Idempotent.
    async fn forget_image(&self, image: &str) -> anyhow::Result<()>;
}

#[cfg(any(test, feature = "test-helpers"))]
pub mod fake {
    use super::*;
    use parking_lot::Mutex;

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum RunnerCall {
        Prepare(WorkerTarget),
        Endpoint { project: uuid::Uuid },
        Front { target: WorkerTarget, port: Option<u16> },
        Settle(WorkerTarget),
        LetFrontGo { project: uuid::Uuid },
        KillWorkers { project: uuid::Uuid },
        Retire { project: uuid::Uuid },
        ForgetImage(String),
    }

    /// Answers the folders it was given; a project it was not given has
    /// none.
    #[derive(Default)]
    pub struct FakeProjectFolders {
        folders: Mutex<std::collections::HashMap<uuid::Uuid, std::path::PathBuf>>,
    }

    impl FakeProjectFolders {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn set(&self, project: uuid::Uuid, folder: impl Into<std::path::PathBuf>) {
            self.folders.lock().insert(project, folder.into());
        }
    }

    #[async_trait]
    impl ProjectFolders for FakeProjectFolders {
        async fn folder(&self, project: uuid::Uuid) -> anyhow::Result<Option<std::path::PathBuf>> {
            Ok(self.folders.lock().get(&project).cloned())
        }
    }

    /// Records every call; hands out `base_url` as the endpoint.
    pub struct FakeRunner {
        pub base_url: String,
        calls: Mutex<Vec<RunnerCall>>,
    }

    impl FakeRunner {
        pub fn new(base_url: impl Into<String>) -> Self {
            Self { base_url: base_url.into(), calls: Mutex::new(Vec::new()) }
        }

        pub fn calls(&self) -> Vec<RunnerCall> {
            self.calls.lock().clone()
        }
    }

    #[async_trait]
    impl Runner for FakeRunner {
        async fn prepare(&self, target: &WorkerTarget) -> anyhow::Result<()> {
            self.calls.lock().push(RunnerCall::Prepare(target.clone()));
            Ok(())
        }
        async fn endpoint(&self, target: &WorkerTarget, _patience: Patience) -> anyhow::Result<WorkerEndpoint> {
            self.calls.lock().push(RunnerCall::Endpoint { project: target.project });
            Ok(WorkerEndpoint { base_url: self.base_url.clone(), bearer: "fake".into(), hold: None })
        }
        async fn front(&self, target: &WorkerTarget, port: Option<u16>) -> anyhow::Result<weft_core::projects::ProjectAddress> {
            self.calls.lock().push(RunnerCall::Front { target: target.clone(), port });
            Ok(weft_core::projects::ProjectAddress::Serving { url: self.base_url.clone() })
        }
        async fn settle(&self, target: &WorkerTarget) -> anyhow::Result<()> {
            self.calls.lock().push(RunnerCall::Settle(target.clone()));
            Ok(())
        }
        async fn let_front_go(&self, project: uuid::Uuid) -> anyhow::Result<()> {
            self.calls.lock().push(RunnerCall::LetFrontGo { project });
            Ok(())
        }
        async fn kill_workers(&self, _tenant: &str, project: uuid::Uuid) -> anyhow::Result<()> {
            self.calls.lock().push(RunnerCall::KillWorkers { project });
            Ok(())
        }
        async fn retire(&self, _tenant: &str, project: uuid::Uuid) -> anyhow::Result<()> {
            self.calls.lock().push(RunnerCall::Retire { project });
            Ok(())
        }
        async fn forget_image(&self, image: &str) -> anyhow::Result<()> {
            self.calls.lock().push(RunnerCall::ForgetImage(image.to_string()));
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_evidence_the_address_is_gone_forgets_it() {
        let gone = |call: WorkerCall| call.address_is_gone();
        assert!(gone(WorkerCall::NoAnswer { connect: true }));
        assert!(!gone(WorkerCall::NoAnswer { connect: false }), "a timeout says nothing about the address");
        assert!(gone(WorkerCall::Answered { status: 404, from_worker: false }), "the platform's 404: the service is gone");
        assert!(!gone(WorkerCall::Answered { status: 403, from_worker: false }), "the platform's 403 is a refusal at a right address");
        assert!(WorkerCall::Answered { status: 403, from_worker: false }.platform_refused());
        assert!(!WorkerCall::Answered { status: 403, from_worker: true }.platform_refused(), "the program's own 403");
        for busy in [429, 502, 503, 500] {
            assert!(!gone(WorkerCall::Answered { status: busy, from_worker: false }), "{busy} is the platform being busy");
        }
        assert!(!gone(WorkerCall::Answered { status: 404, from_worker: true }), "a route's own 404");
    }
}
