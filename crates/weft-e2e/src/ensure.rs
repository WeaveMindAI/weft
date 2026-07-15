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

/// Ensure the weft system is up and running current code, then CLEAN the cluster
/// to an empty baseline, and return a [`Dispatcher`] client pointed at it. Called
/// at the START of every test.
///
/// The bring-up (`setup.sh` + health wait) is expensive and idempotent, so it runs
/// exactly ONCE per test process, latched. The SWEEP, in contrast, runs on EVERY
/// call (every test): the contract is "each test begins against a clean cluster,
/// even if a PRIOR test crashed and left state behind". A failed test still leaves
/// its own state up for post-mortem and halts the run (the runner stops at the first
/// failure); the sweep only ever wipes state the CURRENT test is about to own, at
/// its own start, so it never destroys the evidence of the failure that stopped the
/// previous run.
pub async fn up() -> Result<Dispatcher> {
    load_repo_env();
    READY
        .get_or_try_init(|| async { bring_up().await })
        .await?;
    sweep_leftovers().await?;
    Dispatcher::from_env()
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

/// Run `setup.sh` to bring the cluster to current code and wait for the dispatcher
/// to be reachable. Latched to run once per process (the sweep is NOT here: it runs
/// per-test in [`up`]).
async fn bring_up() -> Result<()> {
    let root = repo_root()?;
    run_setup(&root).await?;
    wait_healthy().await
}

/// Wipe the cluster to an EMPTY baseline: every leftover project + every pooled-pod
/// clone. Run at the START of each test (see [`up`]), so a test always begins clean
/// regardless of what a prior crashed run left. It does NOT run as a per-test
/// TEARDOWN: a failed test's state survives (the runner halts on failure), and the
/// next test's start-sweep is what reclaims it.
///
/// Two kinds of leftover:
///   - leftover PROJECTS: removed via the real `DELETE /projects/{id}`
///     path (forced), which deactivates, terminates infra, deletes the
///     namespace, and drops the rows, exactly as `weft rm --force` does;
///   - leftover pooled-pod CLONES a scale-down test stood up: swept via
///     the platform layer (kubectl + the registry rows).
pub async fn sweep_leftovers() -> Result<()> {
    // Leftover PROJECTS for the (`local`) dispatcher.
    clean_projects(&Dispatcher::from_env()?).await?;
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

/// Delete EVERY project visible to `disp` (its tenant's projects: `GET /projects`
/// is tenant-scoped by the caller's identity), each forced so the delete skips the
/// 120s supervisor-terminate wait. The ONE shared "wipe a tenant's projects to
/// empty" primitive: called with the `local` dispatcher here, and (by a harness
/// that has tokens) once per tenant with that tenant's authed dispatcher.
/// HTTP-only, so it is feature-independent.
pub async fn clean_projects(disp: &Dispatcher) -> Result<()> {
    let projects: Vec<serde_json::Value> = disp.get_json("/projects").await?;
    for p in &projects {
        if let Some(id) = p.get("id").and_then(|v| v.as_str()) {
            disp.delete(&format!("/projects/{id}?force=true"))
                .await
                .with_context(|| format!("sweep leftover project {id}"))?;
        }
    }
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

/// Invoke `./setup.sh --cli --daemon` at the repo root: build the CLI + refresh
/// the daemon (rebuild images + roll pods only when code changed), which is
/// exactly "bring the BACKEND to current code". We pass `--cli --daemon`
/// explicitly rather than the bare default so setup.sh SKIPS the VS Code
/// extension build: the editor extension is a frontend artifact irrelevant to a
/// backend e2e run, and its TypeScript compile must never gate whether the rig
/// can stand the cluster up (a parked or mid-refactor extension would otherwise
/// abort every backend test). Inherits the rig's environment so any `WEFT_*`
/// overrides flow through.
async fn run_setup(root: &Path) -> Result<()> {
    let out = tokio::process::Command::new("./setup.sh")
        .args(["--cli", "--daemon"])
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
