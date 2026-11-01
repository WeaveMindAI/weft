//! The translation layer's contract: one trait per platform capability.
//!
//! The same program and the same node code run unchanged on a laptop and
//! on any cloud. Everything that differs between those places is behind a
//! trait here, with one implementation per platform
//! (`weft-platform-local`, `weft-platform-gcp`). Someone adding a platform
//! writes one implementation of each and touches nothing else: not the
//! language, not the signal kinds, not the ctx.
//!
//! Layout:
//!   - `runner`: where a project's workers run and how to reach them.
//!   - `infra_host`: where a project's infrastructure runs.
//!   - `images`: building images and keeping them.
//!   - `needs`: what the machine workers run on lacks of what nodes need.
//!   - `alarm`: waking a role at a time.
//!   - `identity`: who is calling, on every internal call.
//!   - `roles`: weft's own roles and where each one runs.
//!   - `config`: the install's one config file.
//!   - `object_store`: files.
//!   - `frontends`: hosting a project's frontend on the install's cloud.
//!   - `domains`: the door in front of the install's own domains.
//!   - `holder_pool`: how many holders keep held connections open.
//!   - `holders`: what holders hear, and how a dead one is noticed.
//!   - `news`: telling the install's other copies what happened to a run,
//!     without the database.
//!   - `clock`: time, abstracted so tests advance it deterministically.
//!   - `unit_agent`: what weft asks the agent beside an infra unit.
//!
//! Test builds enable the `test-helpers` feature to also pull in the
//! fakes; the gate keeps them out of release binaries.

pub mod alarm;
pub mod clock;
pub mod config;
pub mod domains;
pub mod frontends;
pub mod holder_pool;
pub mod holders;
pub mod identity;
pub mod images;
pub mod infra_host;
pub mod needs;
pub mod news;
pub mod object_store;
pub mod roles;
pub mod runner;
pub mod unit_agent;

pub use alarm::{Alarm, Delivery, Wake, WakeCall, WakeQueue, WakeRefusal};
pub use clock::{Clock, SystemClock};
pub use config::{InstallConfig, PlatformConfig};
pub use identity::{CallerIdentity, FixedToken, IdentityRefused, IdentityTokens, Principal};
pub use domains::DomainHosting;
pub use frontends::{FrontendHosting, FrontendSite, HostedFrontend};
pub use holder_pool::HolderPool;
pub use holders::{AlarmLiveness, HeldCheck, HolderHeard, HolderLiveness, HolderNews, HolderNote, OneProcessHolds, HELD_CHECK_PATH};
pub use images::{BuildHandle, BuildRequest, BuildStatus, ImageBuilder, ImageDeleted, Staging};
pub use infra_host::{EndpointAt, InfraHost, RestartCause, UnitObservation, UnitRestart, UnitRunState};
pub use needs::MachineNeeds;
pub use news::{Listening, MayMiss, News, NewsHeard, NewsKind, RunNews};
pub use object_store::{
    object_store_for, CopyProgress, GetLink, ObjectEntry, ObjectStore, ObjectStoreConfig, PresignAudience,
    S3ObjectStore, SharedObjectStore, Unfollowed,
};
pub use roles::{CoreRole, Placement, RoleAddresses, RolePlacement, Vantage};
pub use runner::{worker_auth_key, worker_auth_value, Patience, PortTaken, ProjectFolders, Runner, WorkerCall, WorkerEndpoint, WorkerStarting, WorkerTarget, WorkerWontStart, WORKER_ANSWER_HEADER, WORKER_AUTH_HEADER, WORKER_PORT_TAKEN_EXIT, WORKER_PORT_TAKEN_LINE};
// A worker target carries its levers, so they are this crate's words too.
pub use weft_core::workers::{WorkerOverrides, WorkerSettings, WorkersResponse, MAX_QUEUE_WAIT_ENV, MAX_RUNS_AT_ONCE_ENV, SHARED_IDLE_ENV};

/// The `tracing` target of the line weft logs, once, when its own work is
/// stuck (`weft_task_store::unanswered`): the platform turns it into an
/// alert ([`InstallConfig::alert_after`]), and keeps it out of the
/// process's ordinary log.
pub const ALERT_TARGET: &str = "weft_alert";
#[cfg(any(test, feature = "test-helpers"))]
pub use alarm::fake::FakeAlarm;
#[cfg(any(test, feature = "test-helpers"))]
pub use clock::FakeClock;
#[cfg(any(test, feature = "test-helpers"))]
pub use domains::fake::FakeDomainHosting;
#[cfg(any(test, feature = "test-helpers"))]
pub use frontends::fake::{FakeFrontendHosting, FrontendCall};
#[cfg(any(test, feature = "test-helpers"))]
pub use holder_pool::fake::FakeHolderPool;
#[cfg(any(test, feature = "test-helpers"))]
pub use holders::fake::FakeHolderNews;
#[cfg(any(test, feature = "test-helpers"))]
pub use images::FakeImageBuilder;
#[cfg(any(test, feature = "test-helpers"))]
pub use news::fake::FakeNews;
#[cfg(any(test, feature = "test-helpers"))]
pub use object_store::fake::{FakeCall as ObjectStoreFakeCall, FakeObjectStore};
#[cfg(any(test, feature = "test-helpers"))]
pub use runner::fake::{FakeProjectFolders, FakeRunner, RunnerCall};
#[cfg(any(test, feature = "test-helpers"))]
pub use needs::fake::FakeMachineNeeds;
#[cfg(any(test, feature = "test-helpers"))]
pub use infra_host::fake::{FakeInfraHost, HostCall};
