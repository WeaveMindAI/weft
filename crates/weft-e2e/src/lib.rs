//! # weft-e2e: the Layer-4 end-to-end test rig
//!
//! This crate drives a REAL running weft cluster (dispatcher + listener +
//! worker pods on kind) and asserts behavior through the dispatcher's public
//! API, exactly as a user or the outside world would. It is the Layer-4 tier
//! of the testing pyramid: real binaries, real network, real backing services.
//!
//! ## How to run
//!
//! ```text
//! cargo test -p weft-e2e --features e2e -- --test-threads=1
//! ```
//!
//! The `e2e` feature is OFF by default, so `cargo test --workspace` compiles
//! this crate but runs none of its cluster-touching tests. The rig invokes
//! `setup.sh` once at suite start to bring the cluster to current code, then
//! each test prepares an isolated project, drives it, and asserts.
//!
//! ## The toolkit (this library)
//!
//! - [`client`] : HTTP client over the dispatcher API + `weft` CLI shell-out.
//! - [`ensure`] : suite-shared "bring the system up on current code" step.
//! - [`event`]  : the execution replay event stream, typed-accessor over JSON.
//! - [`project`]: fixture -> isolated project lifecycle (prepare/build/run/rm).
//! - [`run`]    : start a run, wait for it to settle, fetch its replay.
//! - [`assert`] : intent assertions over a settled run (output / skip / loop).
//! - [`signal`] : discover + fire triggers the outside world calls IN.
//! - [`live`]   : the live-caller handshake + HTTP/WS exchange.
//! - [`human`]  : human-in-the-loop forms (discover, answer, assert resume).
//! - [`fakes`]  : throwaway servers for triggers the system dials OUT to.
//! - [`infra`]  : infra node lifecycle (start, poll, drive, terminate).
//! - [`storage`]: stored-file list / download / assert + sweep.
//! - [`bus`]    : bus conversation assertions over the event log.
//!
//! ## Tests (the `tests/` directory)
//!
//! Each test file targets one subsystem. A test prepares a fixture, drives it
//! via the toolkit, and asserts via [`run::SettledRun`]. The fixtures (real
//! weft projects) live under `fixtures/`.

pub mod assert;
pub mod bus;
pub mod client;
pub mod ensure;
pub mod event;
pub mod fakes;
pub mod human;
pub mod infra;
pub mod live;
// The platform layer reaches behind the public API (direct Postgres + kubectl)
// to observe and drive what the SYSTEM does underneath a program. It needs the
// cluster's Postgres, so it (and its `sqlx` dep) compile ONLY under `e2e`. None
// of it ships; see the module docs.
#[cfg(feature = "e2e")]
pub mod platform;
pub mod project;
pub mod run;
pub mod signal;
pub mod storage;

pub use client::{cli, cli_ok, poll_until, Dispatcher};
#[cfg(feature = "e2e")]
pub use platform::Platform;
pub use ensure::up;
pub use event::{Event, Replay};
pub use project::Project;
pub use run::SettledRun;
