//! Test rig for the supervisor. Composes the supervisor's fakes plus the
//! platform-traits fakes (`FakeInfraHost`, `FakeClock`) into a single
//! struct that wires `SupervisorState` against them.
//!
//! Pattern to extend:
//!   - Tests construct `SupervisorTestRig::new()`.
//!   - Seed via `rig.broker.add_project(...)`, `rig.host.set_state(...)`.
//!   - Drive via `rig.tick_health().await` or `rig.tick_lifecycle().await`.
//!   - Advance time via `rig.advance(Duration)`.
//!   - Assert via `rig.broker.events()`, `rig.host.calls()`, etc.
//!
//! The rig deliberately exposes its fakes (`rig.broker`, `rig.host`,
//! `rig.clock`) as `pub` fields rather than hiding them behind getters:
//! tests should be cheap to write and noisy is fine.

mod rig;
pub use rig::SupervisorTestRig;
