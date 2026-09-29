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
//!   - `clock`: time, abstracted so tests advance it deterministically.
//!   - `drain`: waiting for a project's running executions to finish
//!     before a disruptive lifecycle step.
//!   - `unit_agent`: what weft asks the agent beside an infra unit.
//!
//! Test builds enable the `test-helpers` feature to also pull in the
//! fakes; the gate keeps them out of release binaries.

pub mod alarm;
pub mod clock;
pub mod config;
pub mod drain;
pub mod identity;
pub mod images;
pub mod infra_host;
pub mod object_store;
pub mod roles;
pub mod runner;
pub mod unit_agent;

pub use alarm::{Alarm, Wake, WakeCall};
pub use clock::{Clock, SystemClock};
pub use config::{InstallConfig, PlatformConfig};
pub use drain::{drain_until_zero, DrainOutcome, DRAIN_POLL_INTERVAL};
pub use identity::{CallerIdentity, FixedToken, IdentityRefused, IdentityTokens, Principal};
pub use images::{BuildHandle, BuildRequest, BuildStatus, ImageBuilder, ImageDeleted};
pub use infra_host::{EndpointAt, InfraHost, UnitObservation, UnitRunState};
pub use object_store::{
    object_store_for, ObjectEntry, ObjectStore, ObjectStoreConfig, PresignAudience,
    S3ObjectStore, SharedObjectStore,
};
pub use roles::{CoreRole, Kick, Placement, RoleAddresses, RolePlacement, Vantage};
pub use runner::{Patience, Runner, WorkerCall, WorkerEndpoint, WorkerOverrides, WorkerSettings, WorkerStarting, WorkerTarget, WORKER_ANSWER_HEADER, WORKER_AUTH_HEADER};
#[cfg(any(test, feature = "test-helpers"))]
pub use alarm::fake::FakeAlarm;
#[cfg(any(test, feature = "test-helpers"))]
pub use clock::FakeClock;
#[cfg(any(test, feature = "test-helpers"))]
pub use images::FakeImageBuilder;
#[cfg(any(test, feature = "test-helpers"))]
pub use object_store::fake::{FakeCall as ObjectStoreFakeCall, FakeObjectStore};
#[cfg(any(test, feature = "test-helpers"))]
pub use runner::fake::{FakeRunner, RunnerCall};
#[cfg(any(test, feature = "test-helpers"))]
pub use roles::fake::FakeKick;
#[cfg(any(test, feature = "test-helpers"))]
pub use infra_host::fake::{FakeInfraHost, HostCall};
