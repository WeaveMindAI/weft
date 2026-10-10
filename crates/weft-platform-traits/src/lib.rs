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
//!   - `alarm`: waking a role at a time.
//!   - `identity`: who is calling, on every internal call.
//!   - `roles`: weft's own roles and where each one runs.
//!   - `config`: the install's one config file.
//!   - `object_store`: files.
//!   - `frontends`: hosting a project's frontend on the install's cloud.
//!   - `domains`: the door in front of the install's own domains.
//!   - `holder_pool`: how many holders keep held connections open.
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
pub mod identity;
pub mod images;
pub mod infra_host;
pub mod object_store;
pub mod roles;
pub mod runner;
pub mod unit_agent;

pub use alarm::{Alarm, Wake, WakeCall, WakeRefusal};
pub use clock::{Clock, SystemClock};
pub use config::{InstallConfig, PlatformConfig};
pub use identity::{CallerIdentity, FixedToken, IdentityRefused, IdentityTokens, Principal};
pub use domains::DomainHosting;
pub use frontends::{FrontendHosting, FrontendSite, HostedFrontend};
pub use holder_pool::HolderPool;
pub use images::{BuildHandle, BuildRequest, BuildStatus, ImageBuilder, ImageDeleted, Staging};
pub use infra_host::{EndpointAt, InfraHost, UnitObservation, UnitRunState};
pub use object_store::{
    object_store_for, ObjectEntry, ObjectStore, ObjectStoreConfig, PresignAudience,
    S3ObjectStore, SharedObjectStore,
};
pub use roles::{CoreRole, Placement, RoleAddresses, RolePlacement, Vantage};
pub use runner::{worker_auth_key, worker_auth_value, Patience, PortTaken, Runner, WorkerCall, WorkerEndpoint, WorkerOverrides, WorkerSettings, WorkerStarting, WorkerTarget, WorkersResponse, MAX_QUEUE_WAIT_ENV, MAX_RUNS_AT_ONCE_ENV, SHARED_IDLE_ENV, WORKER_ANSWER_HEADER, WORKER_AUTH_HEADER};
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
pub use images::FakeImageBuilder;
#[cfg(any(test, feature = "test-helpers"))]
pub use object_store::fake::{FakeCall as ObjectStoreFakeCall, FakeObjectStore};
#[cfg(any(test, feature = "test-helpers"))]
pub use runner::fake::{FakeRunner, RunnerCall};
#[cfg(any(test, feature = "test-helpers"))]
pub use infra_host::fake::{FakeInfraHost, HostCall};
