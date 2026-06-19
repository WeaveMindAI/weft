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
//!
//! Production builds link the real impls (`KubectlClient`,
//! `SystemClock`). Test builds enable the `test-helpers` feature to
//! also pull in `FakeKube` + `FakeClock`. The feature gate keeps
//! fakes out of release binaries.

pub mod clock;
pub mod kube;

// Re-export the trait surface at the crate root so consumers can
// write `use weft_platform_traits::{Clock, KubeClient, ...};`
// without leaking the internal module layout. Fakes stay gated.
pub use clock::{Clock, SystemClock};
pub use kube::{
    DeleteOpts, KubeClient, KubeReader, KubeWriter, WorkloadKind, WorkloadReplicaState,
};
#[cfg(any(test, feature = "test-helpers"))]
pub use clock::FakeClock;
#[cfg(any(test, feature = "test-helpers"))]
pub use kube::FakeKube;
