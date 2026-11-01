//! Removing what failed runs kept.
//!
//! A passing test removes what it made, and a failing one keeps it so you
//! can look. This is the other half: once you are done looking, it removes
//! everything any e2e run kept, then reclaims unreferenced images (below). `scripts/run-e2e.sh` runs
//! it at the start of every run (and alone with `--clean`), never while
//! tests are running (the runner holds a lock for that), because a test in
//! flight holds exactly the same kind of state.
//!
//! What counts as kept, and how it is told apart from yours, all of it on
//! the install under test ([`crate::ensure::install_under_test`]) and none
//! of it on any other, so a run on one branch never removes what another
//! branch's run kept:
//!   - cells the ledger still lists ([`crate::kept`]): every cell's name
//!     starts with `e2e`, but so does every other install's runs' cells;
//!   - projects whose name starts with
//!     [`crate::project::E2E_PROJECT_PREFIX`], which every fixture's is;
//!   - connections the ledger still lists;
//!   - this install's fixture copies under the temp directory
//!     ([`crate::project::fixture_copies_dir`]);
//!   - and, once those are gone, `weft clean --images --all`, which is NOT
//!     limited to e2e: it removes every worker program no project of the
//!     install references (yours included, once nothing runs them), and
//!     the builder bases, node-test images, runtime images, standard
//!     workers and worker compile caches that no install on the machine
//!     records using, and every image and compile cache of an install no
//!     longer on the machine. While some install has no record yet, it
//!     removes nothing of the recorded kinds, and it never removes a cache
//!     a build is using right now.

use anyhow::{Context, Result};

use crate::client::cli_ok;

/// Remove everything e2e runs kept. See the module docs.
pub async fn clean_kept() -> Result<()> {
    let root = crate::ensure::repo_root()?;
    for cell in crate::kept::kept_cells()? {
        println!("removing cell {cell}");
        let install = weft_core::infra::Install::named(&cell).map_err(anyhow::Error::msg)?;
        crate::cell::daemon(&install, "remove").await?;
        crate::kept::cell_gone(&cell)?;
    }

    let disp = crate::ensure::up().await?;
    let projects: Vec<weft_core::projects::ProjectSummary> = disp.get_json("/projects").await?;
    for p in &projects {
        let (name, id) = (&p.name, &p.id);
        if name.starts_with(crate::project::E2E_PROJECT_PREFIX) {
            println!("removing project {name} ({id})");
            // Forced: a kept project may sit in any state, and the force
            // skips waiting on a supervisor to terminate its infra.
            disp.delete(&format!("/projects/{id}?force=true"))
                .await
                .with_context(|| format!("remove kept project {id}"))?;
        }
    }

    // A grant the ledger lists may be gone already (removed by hand), which
    // is as good as removed: only the ones the install still holds are
    // deleted.
    let held: Vec<serde_json::Value> = disp.get_json("/access/grants").await?;
    let held: std::collections::HashSet<&str> =
        held.iter().filter_map(|g| g.get("id").and_then(|v| v.as_str())).collect();
    for grant in crate::kept::kept_grants()? {
        if held.contains(grant.as_str()) {
            println!("removing connection {grant}");
            disp.delete(&format!("/access/grants/{grant}"))
                .await
                .with_context(|| format!("remove kept connection {grant}"))?;
        }
        crate::kept::grant_gone(&grant)?;
    }

    remove_fixture_copies()?;
    cli_ok(&disp, &root, &["clean", "--images", "--all"]).await?;
    Ok(())
}

/// The fixture copies this install's tests made
/// ([`crate::project::fixture_copies_dir`]).
fn remove_fixture_copies() -> Result<()> {
    let dir = crate::project::fixture_copies_dir()?;
    match std::fs::remove_dir_all(&dir) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("remove {}", dir.display())),
    }
}
