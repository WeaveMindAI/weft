//! Where a project's workers run, and how weft reaches them.
//!
//! A worker is the project's compiled program serving HTTP. Weft does not
//! keep workers running and hand them work from a queue: it CALLS a worker
//! for each execution (`POST /run/<execution_id>`, or the caller's own
//! forwarded request for a live one), and the worker answers when the
//! execution ends or waits on something outside it. That is the whole
//! protocol, and it is weft's, the same on every platform (see
//! `weft_engine::worker_server`).
//!
//! What differs per platform is only where that HTTP server lives and how
//! to get an address for it, which is what this trait answers: a local
//! container started on demand, or a Cloud Run service.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Everything the platform needs to run one project's workers at one
/// image. A project can have several images live at once (a new build
/// while older executions still resume on the image they started on), so
/// the image is part of the target, not a property of the project.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerTarget {
    pub tenant: String,
    pub project: uuid::Uuid,
    /// The worker image to run (content-addressed by its binary hash).
    pub image: String,
    /// The program's binary the image holds, whose routes the worker
    /// serves; `None` for a node-test server, which serves no routes.
    pub binary_hash: Option<String>,
    pub settings: WorkerSettings,
}

/// The per-project worker levers. Every one has a default that serves a
/// person running their own things; each can be raised per project.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerSettings {
    /// Copies kept running when idle. 0 scales to zero (a cold start on
    /// the first call after a quiet stretch); 1 or more never waits for
    /// one, and is billed while idle on a cloud.
    #[serde(default)]
    pub min_instances: u32,
    /// Most copies at once.
    #[serde(default = "WorkerSettings::default_max_instances")]
    pub max_instances: u32,
    /// How many calls Cloud Run sends one copy at once; a local install
    /// does not use it.
    #[serde(default = "WorkerSettings::default_concurrency")]
    pub concurrency: u32,
    /// CPUs per copy on a cloud, as the platform counts them ("1", "2",
    /// "0.5"); unset, one CPU on Cloud Run
    /// ([`WorkerSettings::CLOUD_DEFAULT_CPU`]). A local install does not
    /// cap a copy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<String>,
    /// Memory per copy on a cloud ("512Mi", "2Gi"). A local install does
    /// not cap a copy.
    #[serde(default = "WorkerSettings::default_memory")]
    pub memory: String,
    /// Extra CPU while a copy starts, where the platform offers it.
    #[serde(default = "WorkerSettings::default_startup_boost")]
    pub startup_boost: bool,
    /// Keep CPU on a copy between calls, where the platform throttles it
    /// otherwise (Cloud Run bills CPU only while a request is open). Needed
    /// when a program keeps working after it answered a live caller; it
    /// bills the copy for as long as it is up.
    #[serde(default)]
    pub cpu_always_allocated: bool,
    /// How long a copy holds a live thing its runs share (`ctx.shared`: a
    /// pool of database connections, ready Python processes) once nothing
    /// used it, in seconds. Longer saves rebuilding it after a quiet
    /// stretch; shorter lets it go sooner. The worker reads it from
    /// [`SHARED_IDLE_ENV`].
    #[serde(default = "WorkerSettings::default_shared_idle_seconds")]
    pub shared_idle_seconds: u64,
    /// The most runs one copy takes at once. A call past it waits for a
    /// run to end (`max_queue_wait_seconds`) instead of being refused.
    /// Unset, a copy takes as many as its memory holds at a MiB each, and
    /// never fewer than 64.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_runs_at_once: Option<u32>,
    /// How long a call waits for one of a copy's runs to end when it takes
    /// all it may, in seconds, before it is answered busy (`503`).
    #[serde(default = "WorkerSettings::default_max_queue_wait_seconds")]
    pub max_queue_wait_seconds: u64,
}

/// The variable a worker reads [`WorkerSettings::shared_idle_seconds`]
/// from; every platform sets it on every worker it starts.
pub const SHARED_IDLE_ENV: &str = "WEFT_SHARED_IDLE_SECS";

/// The variable a worker reads [`WorkerSettings::max_runs_at_once`] from,
/// set only when the setting is.
pub const MAX_RUNS_AT_ONCE_ENV: &str = "WEFT_MAX_RUNS_AT_ONCE";

/// The variable a worker reads [`WorkerSettings::max_queue_wait_seconds`]
/// from; every platform sets it on every worker it starts.
pub const MAX_QUEUE_WAIT_ENV: &str = "WEFT_MAX_QUEUE_WAIT_SECS";

impl WorkerSettings {
    /// The variables a platform sets on every worker it starts for these
    /// settings, each with its value.
    pub fn worker_env(&self) -> Vec<(&'static str, String)> {
        let mut env = vec![
            (SHARED_IDLE_ENV, self.shared_idle_seconds.to_string()),
            (MAX_QUEUE_WAIT_ENV, self.max_queue_wait_seconds.to_string()),
        ];
        if let Some(most) = self.max_runs_at_once {
            env.push((MAX_RUNS_AT_ONCE_ENV, most.to_string()));
        }
        env
    }
}

impl WorkerSettings {
    fn default_shared_idle_seconds() -> u64 {
        300
    }
    fn default_max_queue_wait_seconds() -> u64 {
        30
    }
    fn default_max_instances() -> u32 {
        10
    }
    /// Cloud Run's own default for a service: a run mostly waits (on a
    /// model, a database, a person), so one copy serves many, and when its
    /// CPU fills the platform adds copies. A copy nearly out of memory turns
    /// new work away itself (`weft_engine`'s memory guard) rather than
    /// taking a run that would bring the others down.
    fn default_concurrency() -> u32 {
        80
    }
    /// What a copy gets on a cloud whose services need a CPU count when
    /// the project and the install set none.
    pub const CLOUD_DEFAULT_CPU: &'static str = "1";
    fn default_memory() -> String {
        "1Gi".into()
    }
    fn default_startup_boost() -> bool {
        true
    }

    /// Refuse a shape no platform could run, naming the lever.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_instances == 0 {
            return Err("workers.max_instances is 0: nothing could ever run; set it to 1 or more".into());
        }
        if self.min_instances > self.max_instances {
            return Err(format!(
                "workers.min_instances ({}) is above workers.max_instances ({}); lower the first or raise the second",
                self.min_instances, self.max_instances
            ));
        }
        if self.concurrency == 0 {
            return Err("workers.concurrency is 0: a copy could serve nothing; set it to 1 or more".into());
        }
        if self.cpu.as_deref().is_some_and(|cpu| cpu.trim().is_empty()) {
            return Err("workers.cpu is empty; set a CPU count (\"1\", \"0.5\") or unset it for the platform's own".into());
        }
        if self.memory.trim().is_empty() {
            return Err("workers.memory must be set".into());
        }
        if self.max_runs_at_once == Some(0) {
            return Err("workers.max_runs_at_once is 0: a copy could take no run; set it to 1 or more, or unset it to follow the copy's memory".into());
        }
        Ok(())
    }
}

/// What a project may set of [`WorkerSettings`] for itself: a lever
/// replaces the install's setting ([`WorkerSettings::with`]) where a
/// project sets it. Listed here once; the overrides struct, the lever
/// names and every per-lever operation follow from the list, and
/// `every_setting_is_a_lever` holds it to [`WorkerSettings`]' fields.
macro_rules! worker_levers {
    ($($lever:ident: $setting:ty),* $(,)?) => {
        /// A project's own worker levers: each one it sets replaces the
        /// install's ([`WorkerSettings::with`]); unset ones follow the
        /// install.
        #[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct WorkerOverrides {
            $(
                #[serde(default, skip_serializing_if = "Option::is_none")]
                pub $lever: Option<<$setting as Lever>::Set>,
            )*
        }

        impl WorkerOverrides {
            /// Whether no lever is set.
            pub fn is_empty(&self) -> bool {
                *self == Self::default()
            }

            /// Every lever's name, as the flags, the API and a `weft.toml`
            /// spell it.
            pub const LEVERS: &'static [&'static str] = &[$(stringify!($lever)),*];

            /// `self` with every lever `o` sets in its place.
            pub fn merged(&self, o: &WorkerOverrides) -> Self {
                Self { $($lever: o.$lever.clone().or_else(|| self.$lever.clone()),)* }
            }

            /// Put the lever named `name` back on the install's. Refused,
            /// listing the levers, for a name that is not one.
            pub fn unset(&mut self, name: &str) -> Result<(), String> {
                $(if name == stringify!($lever) {
                    self.$lever = None;
                    return Ok(());
                })*
                Err(format!("'{name}' is not a worker lever; the levers are {}", Self::LEVERS.join(", ")))
            }

            /// Whether the lever named `name` is set here.
            pub fn sets(&self, name: &str) -> bool {
                $(if name == stringify!($lever) {
                    return self.$lever.is_some();
                })*
                false
            }
        }

        impl WorkerSettings {
            /// These settings with `o`'s set levers in their place.
            pub fn with(&self, o: &WorkerOverrides) -> Self {
                Self { $($lever: self.$lever.overridden(&o.$lever),)* }
            }
        }
    };
}

worker_levers! {
    min_instances: u32,
    max_instances: u32,
    concurrency: u32,
    cpu: Option<String>,
    memory: String,
    startup_boost: bool,
    cpu_always_allocated: bool,
    shared_idle_seconds: u64,
    max_runs_at_once: Option<u32>,
    max_queue_wait_seconds: u64,
}

/// A setting a project can override: what it sets, and the setting once
/// it did.
pub trait Lever: Clone {
    /// What a project sets.
    type Set: Clone;
    fn overridden(&self, by: &Option<Self::Set>) -> Self;
}

macro_rules! plain_lever {
    ($($ty:ty),*) => {$(
        impl Lever for $ty {
            type Set = $ty;
            fn overridden(&self, by: &Option<$ty>) -> Self {
                by.clone().unwrap_or_else(|| self.clone())
            }
        }
    )*};
}
plain_lever!(u32, u64, bool, String);

/// A setting the install may leave to the platform (`cpu`) or to the
/// copy (`max_runs_at_once`): a project sets a value, never that default
/// back.
macro_rules! optional_lever {
    ($($ty:ty),*) => {$(
        impl Lever for Option<$ty> {
            type Set = $ty;
            fn overridden(&self, by: &Option<$ty>) -> Self {
                by.clone().or_else(|| self.clone())
            }
        }
    )*};
}
optional_lever!(String, u32);

/// `GET/PUT /projects/{id}/workers`: a project's worker levers, where
/// each one comes from, and what its workers run with.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkersResponse {
    /// What the install gives every project.
    pub install: WorkerSettings,
    /// What this project sets for itself.
    pub project: WorkerOverrides,
    /// What its workers run with.
    pub effective: WorkerSettings,
}

impl Default for WorkerSettings {
    fn default() -> Self {
        Self {
            min_instances: 0,
            max_instances: Self::default_max_instances(),
            concurrency: Self::default_concurrency(),
            cpu: None,
            memory: Self::default_memory(),
            startup_boost: Self::default_startup_boost(),
            cpu_always_allocated: false,
            shared_idle_seconds: Self::default_shared_idle_seconds(),
            max_runs_at_once: None,
            max_queue_wait_seconds: Self::default_max_queue_wait_seconds(),
        }
    }
}

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
    /// hold rather than being held through the whole bring-up.
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

    /// Stop serving the project's own address: nothing of it takes calls
    /// any more (every trigger it has was wiped). Its workers go once idle.
    async fn let_front_go(&self, project: uuid::Uuid) -> anyhow::Result<()>;

    /// Remove everything the platform keeps for the project's workers
    /// (a service, stopped containers). Idempotent.
    async fn retire(&self, tenant: &str, project: uuid::Uuid) -> anyhow::Result<()>;

    /// Remove what the platform keeps running from `image` (a container,
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
        LetFrontGo { project: uuid::Uuid },
        Retire { project: uuid::Uuid },
        ForgetImage(String),
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
        async fn let_front_go(&self, project: uuid::Uuid) -> anyhow::Result<()> {
            self.calls.lock().push(RunnerCall::LetFrontGo { project });
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

    fn every_lever_set() -> WorkerOverrides {
        WorkerOverrides {
            min_instances: Some(1),
            max_instances: Some(2),
            concurrency: Some(3),
            cpu: Some("2".into()),
            memory: Some("2Gi".into()),
            startup_boost: Some(false),
            cpu_always_allocated: Some(true),
            shared_idle_seconds: Some(60),
            max_runs_at_once: Some(200),
            max_queue_wait_seconds: Some(5),
        }
    }

    #[test]
    fn the_lever_names_are_the_wire_names() {
        let wire = serde_json::to_value(every_lever_set()).unwrap();
        let keys: Vec<&str> = wire.as_object().unwrap().keys().map(String::as_str).collect();
        let mut levers = WorkerOverrides::LEVERS.to_vec();
        levers.sort_unstable();
        let mut keys = keys;
        keys.sort_unstable();
        assert_eq!(keys, levers);
        let full = every_lever_set();
        assert!(WorkerOverrides::LEVERS.iter().all(|lever| full.sets(lever)));
    }

    #[test]
    fn every_setting_is_a_lever() {
        let settings = WorkerSettings { cpu: Some("1".into()), max_runs_at_once: Some(100), ..WorkerSettings::default() };
        let wire = serde_json::to_value(settings).unwrap();
        let mut keys: Vec<&str> = wire.as_object().unwrap().keys().map(String::as_str).collect();
        keys.sort_unstable();
        let mut levers = WorkerOverrides::LEVERS.to_vec();
        levers.sort_unstable();
        assert_eq!(keys, levers);
    }

    #[test]
    fn a_set_lever_replaces_the_setting_and_an_unset_one_keeps_it() {
        let install = WorkerSettings::default();
        let with = install.with(&WorkerOverrides { cpu: Some("2".into()), concurrency: Some(4), ..Default::default() });
        assert_eq!(with, WorkerSettings { cpu: Some("2".into()), concurrency: 4, ..install.clone() });
        assert_eq!(install.with(&WorkerOverrides::default()), install);
    }

    #[test]
    fn unsetting_every_lever_leaves_nothing_and_a_wrong_name_is_refused() {
        let mut o = every_lever_set();
        for lever in WorkerOverrides::LEVERS {
            o.unset(lever).unwrap();
        }
        assert_eq!(o, WorkerOverrides::default());
        let refused = o.unset("gpus").unwrap_err();
        assert!(refused.contains("'gpus' is not a worker lever"), "{refused}");
    }

    #[test]
    fn a_merge_keeps_what_the_new_levers_leave_unset() {
        let base = WorkerOverrides { cpu: Some("1".into()), concurrency: Some(4), ..Default::default() };
        let merged = base.merged(&WorkerOverrides { cpu: Some("2".into()), ..Default::default() });
        assert_eq!(merged, WorkerOverrides { cpu: Some("2".into()), concurrency: Some(4), ..Default::default() });
    }

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

    #[test]
    fn worker_settings_default_and_round_trip() {
        let s: WorkerSettings = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(s, WorkerSettings::default());
        assert!(s.validate().is_ok());
        let v = serde_json::to_value(&s).unwrap();
        assert_eq!(serde_json::from_value::<WorkerSettings>(v).unwrap(), s);
        assert!(serde_json::from_value::<WorkerSettings>(serde_json::json!({ "minInstances": 1 })).is_err());
    }

    #[test]
    fn a_projects_levers_replace_only_what_they_set() {
        let o: WorkerOverrides = serde_json::from_value(serde_json::json!({ "min_instances": 1, "memory": "2Gi" })).unwrap();
        let s = WorkerSettings::default().with(&o);
        assert_eq!((s.min_instances, s.memory.as_str()), (1, "2Gi"));
        assert_eq!(s.max_instances, WorkerSettings::default().max_instances);
        assert_eq!(WorkerSettings::default().with(&WorkerOverrides::default()), WorkerSettings::default());
        assert!(serde_json::from_value::<WorkerOverrides>(serde_json::json!({ "region": "x" })).is_err());
    }

    #[test]
    fn impossible_worker_settings_are_refused_naming_the_lever() {
        let mut s = WorkerSettings { min_instances: 3, max_instances: 2, ..Default::default() };
        assert!(s.validate().unwrap_err().contains("workers.min_instances"));
        s = WorkerSettings { max_instances: 0, ..Default::default() };
        assert!(s.validate().unwrap_err().contains("workers.max_instances"));
        s = WorkerSettings { concurrency: 0, ..Default::default() };
        assert!(s.validate().unwrap_err().contains("workers.concurrency"));
    }
}
