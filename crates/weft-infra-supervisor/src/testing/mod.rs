//! Test rig for the supervisor. Composes the supervisor's fakes
//! plus the platform-traits fakes (`FakeKube`, `FakeClock`) into a
//! single struct that wires `SupervisorState` against them.
//!
//! Pattern to extend:
//!   - Tests construct `SupervisorTestRig::new()`.
//!   - Seed via `rig.broker.add_project(...)`, `rig.kube.set_workloads(...)`.
//!   - Drive via `rig.tick_health().await` or `rig.tick_lifecycle().await`.
//!   - Advance time via `rig.advance(Duration)`.
//!   - Assert via `rig.broker.events()`, `rig.kube.scale_calls()`, etc.
//!
//! The rig deliberately exposes its fakes (`rig.broker`, `rig.kube`,
//! `rig.clock`) as `pub` fields rather than hiding them behind
//! getters: tests should be cheap to write and noisy is fine.

mod rig;
pub use rig::SupervisorTestRig;
