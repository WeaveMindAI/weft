//! # weft-e2e: the Layer-4 end-to-end test rig
//!
//! This crate drives a REAL running weft install (the runtime process and
//! the worker processes it starts) and asserts behavior through the dispatcher's public
//! API, exactly as a user or the outside world would. It is the Layer-4 tier
//! of the testing pyramid: real binaries, real network, real backing services.
//!
//! ## How to run
//!
//! ```text
//! scripts/run-e2e.sh
//! ```
//!
//! The runner brings the install to current code once, then runs the test
//! files side by side. The `e2e` feature is OFF by default, so
//! `cargo test --workspace` compiles this crate but runs none of its
//! install-touching tests. How to write a test that runs well beside the
//! others is in the crate's README.
//!
//! ## The toolkit (this library)
//!
//! - [`client`] : HTTP client over the dispatcher API + `weft` CLI shell-out.
//! - [`ensure`] : reach the install under test (the runner brought it up).
//! - [`cell`]   : a whole install of a test's own.
//! - [`event`]  : the run replay event stream, typed-accessor over JSON.
//! - [`project`]: fixture -> isolated project lifecycle (prepare/build/run/rm).
//! - [`teardown`]: the guard that removes a project on a pass and keeps it on a fail.
//! - [`kept`]   : the ledger of what tests made, so the next run removes what a failed one kept.
//! - `cleanup`  : removing what failed runs kept (`scripts/run-e2e.sh --clean`).
//! - [`run`]    : start a run, wait for it to settle, fetch its replay.
//! - [`assert`] : intent assertions over a settled run (output / skip / loop).
//! - [`signal`] : discover + fire triggers the outside world calls IN.
//! - [`live`]   : a live caller's HTTP/WS exchange.
//! - [`human`]  : human-in-the-loop forms (discover, answer, assert resume).
//! - [`fakes`]  : throwaway servers for triggers the system dials OUT to.
//! - [`infra`]  : infra node lifecycle (start, poll, terminate).
//! - [`display`]: read what a node is showing, through an outside client's door.
//! - [`access`] : connect a service through the dispatcher's store, as the editor does.
//! - [`status`] : project status + available-actions observation.
//! - [`storage`]: stored-file list / download / assert + sweep.
//! - [`bus`]    : bus conversation assertions over the event log.
//! - `platform` : worker processes, containers and the database, behind the API.
//!
//! ## Tests (the `tests/` directory)
//!
//! Each test file targets one subsystem. A test prepares a fixture, drives it
//! via the toolkit, and asserts via [`run::SettledRun`]. The fixtures (real
//! weft projects) live under `fixtures/`.
//!
//! ## Reuse by another harness
//!
//! The API-driving toolkit ([`client::Dispatcher`] + the modules built on it:
//! [`run`], [`assert`], [`event`], [`storage`], [`signal`], [`live`]) is
//! deliberately auth-agnostic: `Dispatcher` carries the operator key it
//! sends (the install's own key here, read from its secrets). A harness that
//! holds another key can depend on THIS crate and reuse that toolkit, handing
//! it its key ([`client::Dispatcher::with_key`]; everything API + token, no
//! CLI). What stays specific to this harness and
//! is NOT reused: [`ensure`] and [`cell`] (installs on this machine),
//! `platform` (worker processes and containers on this machine), and [`project::Project`] (shells out to the
//! `weft` CLI).

pub mod access;
pub mod assert;
pub mod bus;
pub mod cell;
// Removing what failed runs kept (`scripts/run-e2e.sh --clean`). Reaches
// behind the API through the platform layer, so `e2e` only.
#[cfg(feature = "e2e")]
pub mod cleanup;
pub mod client;
pub mod display;
pub mod ensure;
pub mod event;
pub mod fakes;
pub mod human;
pub mod infra;
pub mod kept;
pub mod live;
// The platform layer reaches behind the public API (worker processes, Docker,
// a direct database) to observe and drive what the SYSTEM does underneath a
// program, so it compiles ONLY under `e2e`. None of it ships; see the module
// docs.
#[cfg(feature = "e2e")]
pub mod platform;
pub mod project;
pub mod run;
pub mod signal;
pub mod status;
pub mod storage;
pub mod teardown;

pub use client::{cli, cli_ok, poll_until, poll_until_describing, time_left, Dispatcher, TEST_LIMIT};
#[cfg(feature = "e2e")]
pub use platform::Platform;
pub use cell::Cell;
pub use ensure::up;
pub use event::{Event, Replay};
pub use project::Project;
pub use run::SettledRun;
pub use teardown::Teardown;
