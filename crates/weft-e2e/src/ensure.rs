//! Suite-shared "ensure the system is up and on current code" step.
//!
//! Per the rig's design, EVERY run invokes `setup.sh` once (it is idempotent:
//! a no-op when the cluster already runs the current code, a rebuild + rollout
//! when code changed). We run it a single time per test process via an async
//! latch, then wait for the dispatcher to answer `/health`, so the first test
//! pays the bring-up and the rest start instantly against a warm system.
//!
//! If setup or the health wait fails, every test that calls [`up`] fails with
//! the same clear error rather than each test independently flailing against a
//! down system.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use tokio::sync::OnceCell;

use crate::client::{cli_ok, poll_until, Dispatcher};

/// Latches the one-time bring-up. `Ok(())` once setup + health succeeded;
/// re-evaluated on failure so a transient first failure does not permanently
/// poison the suite (the next test retries the bring-up).
static READY: OnceCell<()> = OnceCell::const_new();

/// Ensure the weft system is up and running current code, exactly once per
/// test process. Returns a [`Dispatcher`] client pointed at it. Safe to call
/// from every test; only the first call does the work.
pub async fn up() -> Result<Dispatcher> {
    READY
        .get_or_try_init(|| async { bring_up().await })
        .await?;
    Dispatcher::from_env()
}

/// Run `setup.sh` to bring the cluster to current code, wait for the
/// dispatcher to be reachable, then sweep any leftover state from earlier
/// runs. Factored out of [`up`] so the latch wraps the whole bring-up as
/// one unit (so the sweep runs exactly once per suite, before any test).
async fn bring_up() -> Result<()> {
    let root = repo_root()?;
    run_setup(&root).await?;
    wait_healthy().await?;
    sweep_leftovers().await
}

/// Remove state left behind by EARLIER runs, once at suite startup. A
/// failed test deliberately leaves its project (and namespace, pods,
/// clones) up so the failure can be inspected; this sweep is what keeps
/// those stragglers from accumulating across runs and overloading the
/// cluster (idle namespaces + pods slow pod scheduling, which can starve
/// a fresh fixture's readiness). Run at the START, not as an aggressive
/// per-test teardown, so the just-failed run's state survives for
/// post-mortem but the NEXT run begins clean.
///
/// Two kinds of leftover:
///   - leftover PROJECTS: removed via the real `DELETE /projects/{id}`
///     path (forced), which deactivates, terminates infra, deletes the
///     namespace, and drops the rows, exactly as `weft rm --force` does;
///   - leftover pooled-pod CLONES a scale-down test stood up: swept via
///     the platform layer (kubectl + the registry rows).
async fn sweep_leftovers() -> Result<()> {
    let disp = Dispatcher::from_env()?;
    // Leftover projects. `GET /projects` lists them; delete each forced
    // (skip the 120s supervisor-terminate wait, the project is going).
    let projects: Vec<serde_json::Value> = disp.get_json("/projects").await?;
    for p in &projects {
        if let Some(id) = p.get("id").and_then(|v| v.as_str()) {
            disp.delete(&format!("/projects/{id}?force=true"))
                .await
                .with_context(|| format!("sweep leftover project {id}"))?;
        }
    }
    // Leftover pooled-pod clones (reaches behind the API via kubectl +
    // Postgres, the platform layer's job). Connect once for the sweep.
    // The platform layer is `e2e`-gated (it pulls in sqlx), so this part
    // compiles only under the feature; the project sweep above needs only
    // the HTTP client and stays feature-independent.
    #[cfg(feature = "e2e")]
    crate::platform::Platform::connect()
        .await?
        .sweep_e2e_clones()
        .await?;
    Ok(())
}

/// Absolute path to the repo root (where `setup.sh` lives). Resolved from this
/// crate's manifest dir (`<root>/crates/weft-e2e`) so it is correct regardless
/// of the cwd the test binary runs in. Fails loud if `setup.sh` is not found,
/// which means the layout changed and the rig must be updated.
fn repo_root() -> Result<PathBuf> {
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

/// Invoke `./setup.sh` at the repo root. No component flags: the default run
/// builds the CLI + refreshes the daemon (rebuilding images + rolling pods only
/// when code changed), which is exactly "bring the system to current code".
/// Inherits the rig's environment so any `WEFT_*` overrides flow through.
async fn run_setup(root: &Path) -> Result<()> {
    let out = tokio::process::Command::new("./setup.sh")
        .current_dir(root)
        .output()
        .await
        .with_context(|| format!("spawn ./setup.sh in {}", root.display()))?;
    if !out.status.success() {
        bail!(
            "setup.sh failed (exit {:?})\n--- stdout ---\n{}\n--- stderr ---\n{}",
            out.status.code(),
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(())
}

/// Poll the dispatcher's `/health` until it answers `ok`. setup.sh already
/// waits for rollouts, but the port-forward it (re)establishes can take a beat
/// to accept connections, so we confirm reachability before any test proceeds.
async fn wait_healthy() -> Result<()> {
    let disp = Dispatcher::from_env()?;
    poll_until(
        "dispatcher /health to answer ok",
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

/// Confirm the `weft` CLI is on PATH and reports a daemon. Optional belt-and-
/// suspenders a test can call; [`up`] already guarantees readiness via
/// `/health`. Kept because the CLI's own view ("port-forward up") catches a
/// class of "API reachable but CLI misconfigured" mismatch the raw health
/// check cannot.
pub async fn cli_sees_daemon(root: &Path) -> Result<()> {
    let out = cli_ok(root, &["daemon", "status"]).await?;
    if out.contains("running") {
        Ok(())
    } else {
        bail!("`weft daemon status` did not report running:\n{out}")
    }
}
