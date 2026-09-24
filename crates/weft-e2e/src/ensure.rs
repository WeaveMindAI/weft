//! Reaching the install a test runs against.
//!
//! The runner (`scripts/run-e2e.sh`) brings the default install to current
//! code ONCE, before any test starts, so a test only has to confirm the
//! dispatcher answers. Nothing here wipes anything: every test cleans up
//! exactly what it created when it passes, and keeps it when it fails, so
//! tests running side by side never touch each other's state. What a failed
//! run left behind is removed when the next run starts (or by
//! `scripts/run-e2e.sh --clean`).

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};

use crate::client::{poll_until, Dispatcher};

/// The default install's dispatcher, once it answers `/health`. Called at the
/// START of every test that runs on the default install; a test that needs
/// its own install starts a [`crate::cell::Cell`] instead.
pub async fn up() -> Result<Dispatcher> {
    load_repo_env();
    let disp = Dispatcher::from_env()?;
    wait_healthy(&disp).await?;
    Ok(disp)
}

/// Load the repo-root `.env` into this test process's environment (the same
/// uncommitted file the daemon's setup reads provider keys from), so a test
/// that spends on a real key finds it without the operator re-exporting it
/// into the test shell. Never overrides an already-set var, and a missing
/// `.env` is fine (a test needing a key fails loudly on its own). Only under
/// the `e2e` feature: `.env` loading is meaningless without the live system.
fn load_repo_env() {
    #[cfg(feature = "e2e")]
    if let Ok(root) = repo_root() {
        let _ = dotenvy::from_path(root.join(".env"));
    }
}

/// Read a GROUP of external-service variables a test needs together,
/// or announce ONE skip naming the whole shortfall and return `None`.
/// Loads the repo-root `.env` first, so the check is order-independent:
/// a test may (and should) gate on its variables BEFORE the expensive
/// [`up`], and a single-test binary must not depend on a sibling test
/// having loaded `.env` for it. `what` names the service the group
/// belongs to; the returned values are in `vars` order (destructure
/// with `<[String; N]>::try_from`).
pub fn env_group_or_skip(what: &str, vars: &[&str]) -> Option<Vec<String>> {
    load_repo_env();
    let mut present = Vec::new();
    let mut missing = Vec::new();
    let mut values = Vec::new();
    for &var in vars {
        match std::env::var(var) {
            Ok(v) if !v.trim().is_empty() => {
                present.push(var);
                values.push(v);
            }
            _ => missing.push(var),
        }
    }
    if missing.is_empty() {
        Some(values)
    } else {
        eprintln!("SKIPPED {what}: missing {missing:?} (have {present:?})");
        None
    }
}

/// Single-variable form of [`env_group_or_skip`] (one implementation).
pub fn env_or_skip(var: &str) -> Option<String> {
    env_group_or_skip(var, &[var]).map(|mut values| values.pop().expect("one var, one value"))
}

/// Absolute path to the repo root (where `setup.sh` lives). Resolved from this
/// crate's manifest dir (`<root>/crates/weft-e2e`) so it is correct regardless
/// of the cwd the test binary runs in. Fails loud if `setup.sh` is not found,
/// which means the layout changed and the rig must be updated.
pub(crate) fn repo_root() -> Result<PathBuf> {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let root = manifest
        .parent()
        .and_then(Path::parent)
        .context("locate repo root from CARGO_MANIFEST_DIR")?
        .to_path_buf();
    let setup = root.join("setup.sh");
    if !setup.is_file() {
        bail!(
            "setup.sh not found at {}; repo layout changed, update weft-e2e::ensure::repo_root",
            setup.display()
        );
    }
    Ok(root)
}

/// Poll the dispatcher's `/health` until it answers `ok`. A dispatcher that
/// was just rolled, or a cell's that just started, can take a beat to accept
/// connections, so every test confirms reachability before it proceeds.
pub(crate) async fn wait_healthy(disp: &Dispatcher) -> Result<()> {
    poll_until(
        &format!("dispatcher /health at {} to answer ok", disp.base()),
        Duration::from_secs(60),
        Duration::from_millis(500),
        || {
            let disp = disp.clone();
            async move {
                match disp.get_raw("/health").await {
                    Ok((status, body)) if status.is_success() && body.trim() == "ok" => {
                        Ok(Some(()))
                    }
                    // Reachable-but-not-ok, or not-yet-reachable: keep waiting.
                    Ok(_) => Ok(None),
                    Err(_) => Ok(None),
                }
            }
        },
    )
    .await
}
