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
//! scripts/run-e2e.sh
//! ```
//!
//! The runner brings the cluster to current code once, then runs the test
//! files side by side. The `e2e` feature is OFF by default, so
//! `cargo test --workspace` compiles this crate but runs none of its
//! cluster-touching tests. How to write a test that runs well beside the
//! others is in the crate's README.
//!
//! ## The toolkit (this library)
//!
//! - [`client`] : HTTP client over the dispatcher API + `weft` CLI shell-out.
//! - [`ensure`] : reach the default install (the runner brought it up).
//! - [`cell`]   : a whole install of a test's own, with its own pace.
//! - [`event`]  : the execution replay event stream, typed-accessor over JSON.
//! - [`project`]: fixture -> isolated project lifecycle (prepare/build/run/rm).
//! - [`run`]    : start a run, wait for it to settle, fetch its replay.
//! - [`assert`] : intent assertions over a settled run (output / skip / loop).
//! - [`signal`] : discover + fire triggers the outside world calls IN.
//! - [`live`]   : the live-caller handshake + HTTP/WS exchange.
//! - [`human`]  : human-in-the-loop forms (discover, answer, assert resume).
//! - [`fakes`]  : throwaway servers for triggers the system dials OUT to.
//! - [`infra`]  : infra node lifecycle (start, poll, drive, terminate).
//! - [`status`] : project status + available-actions observation.
//! - [`storage`]: stored-file list / download / assert + sweep.
//! - [`bus`]    : bus conversation assertions over the event log.
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
//! deliberately auth-agnostic: `Dispatcher` carries an optional
//! [`client::AuthProvider`] (None here, where there is no login). A harness that
//! needs tokens can depend on THIS crate and reuse that toolkit, injecting a
//! provider that signs a token per request (everything API + token, no CLI). What
//! stays specific to this harness and is NOT reused: [`ensure`] and [`cell`]
//! (installs on kind), `platform` (kind/Postgres direct), and
//! [`project::Project`] (shells out to the `weft` CLI). This harness is
//! unauthenticated by construction (its authenticator only ever issues `local`).

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
// The platform layer reaches behind the public API (direct Postgres + kubectl)
// to observe and drive what the SYSTEM does underneath a program. It needs the
// cluster's Postgres, so it (and its `sqlx` dep) compile ONLY under `e2e`. None
// of it ships; see the module docs.
#[cfg(feature = "e2e")]
pub mod platform;
pub mod project;
pub mod run;
pub mod signal;
pub mod status;
pub mod storage;
// The teardown guard (clean-on-pass / keep-and-warn-on-fail) is backing-agnostic,
// so both the local CLI fixture (`project::Project`) and an HTTP fixture in
// another harness share ONE definition of the policy via this guard.
pub mod teardown;

pub use client::{cli, cli_ok, poll_until, poll_until_describing, tail, Dispatcher};
#[cfg(feature = "e2e")]
pub use platform::Platform;
pub use cell::Cell;
pub use ensure::up;
pub use event::{Event, Replay};
pub use project::Project;
pub use run::SettledRun;
pub use teardown::Teardown;
