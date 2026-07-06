//! Shared trait surface + production impls for cross-cutting platform
//! concerns. Multiple subsystems (supervisor, listener, dispatcher,
//! worker) need k8s access and a clock; this crate is where those
//! traits live so each subsystem can wire production or fake impls
//! without depending on a sibling subsystem's internals.
//!
//! Layout:
//!   - `kube`: k8s API surface (split into reader / writer / full).
//!   - `clock`: time + sleep, abstracted so tests can advance time
//!     deterministically.
//!   - `mem_pressure`: this pod's memory-usage fraction, the saturation
//!     signal both pooled pods (listener, supervisor) spill load on.
//!
//! Production builds link the real impls (`KubectlClient`,
//! `SystemClock`, `CgroupMemPressure`). Test builds enable the
//! `test-helpers` feature to also pull in the fakes. The feature gate
//! keeps fakes out of release binaries.

pub mod clock;
pub mod drain;
pub mod kube;
pub mod mem_pressure;
pub mod object_store;

// Re-export the trait surface at the crate root so consumers can
// write `use weft_platform_traits::{Clock, KubeClient, ...};`
// without leaking the internal module layout. Fakes stay gated.
pub use clock::{Clock, SystemClock};
pub use drain::{drain_until_zero, DrainOutcome, DRAIN_POLL_INTERVAL};
pub use kube::{
    DeleteOpts, KubeClient, KubeReader, KubeWriter, WorkloadKind, WorkloadReplicaState,
};
pub use mem_pressure::{
    is_saturated, plan_memory_scaledown, CgroupMemPressure, MemPressure, PoolPodLoad,
    SATURATION_MEM_FRACTION,
};
pub use object_store::{
    object_store_from_env, ObjectEntry, ObjectStore, ObjectStoreConfig, PresignAudience,
    S3ObjectStore, SharedObjectStore,
};
#[cfg(any(test, feature = "test-helpers"))]
pub use object_store::fake::{FakeCall as ObjectStoreFakeCall, FakeObjectStore};
#[cfg(any(test, feature = "test-helpers"))]
pub use clock::FakeClock;
#[cfg(any(test, feature = "test-helpers"))]
pub use kube::FakeKube;
#[cfg(any(test, feature = "test-helpers"))]
pub use mem_pressure::FakeMemPressure;
