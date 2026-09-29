//! Removing what failed runs kept.
//!
//! A passing test removes what it made, and a failing one keeps it so you
//! can look. This is the other half: once you are done looking, it removes
//! everything any e2e run kept, then reclaims unreferenced images (below). `scripts/run-e2e.sh` runs
//! it at the start of every run (and alone with `--clean`), never while
//! tests are running (the runner holds a lock for that), because a test in
//! flight holds exactly the same kind of state.
//!
//! What counts as kept, and how it is told apart from yours:
//!   - cells: named installs whose name starts with
//!     [`crate::cell::CELL_NAME_PREFIX`];
//!   - projects on the default install whose name starts with
//!     [`crate::project::E2E_PROJECT_PREFIX`], which every fixture's is;
//!   - connections on the default install the ledger still lists
//!     ([`crate::kept`]);
//!   - the fixture copies under the temp directory (`weft-e2e-*`);
//!   - and, once those are gone, `weft clean --images --all`, which is NOT
//!     limited to e2e: on this machine it removes every worker image no
//!     project on the default install references (yours included, once
//!     nothing runs them), dangling worker leftovers, infra images no
//!     project references, every builder base and runtime but the current
//!     one, and the worker compile caches of a retired key that sat unused.

use anyhow::{Context, Result};

use crate::client::cli_ok;

/// Remove everything e2e runs kept. See the module docs.
pub async fn clean_kept() -> Result<()> {
    let root = crate::ensure::repo_root()?;
    for cell in kept_cells().await? {
        println!("removing cell {cell}");
        let out = tokio::process::Command::new("weft")
            .args(["daemon", "remove"])
            .env(weft_core::infra::INSTANCE_ENV, &cell)
            .stdin(std::process::Stdio::null())
            .output()
            .await
            .context("spawn `weft daemon remove`")?;
        anyhow::ensure!(
            out.status.success(),
            "removing the cell {cell} failed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    let disp = crate::ensure::up().await?;
    let projects: Vec<serde_json::Value> = disp.get_json("/projects").await?;
    for p in &projects {
        let name = p.get("name").and_then(|v| v.as_str()).unwrap_or_default();
        let Some(id) = p.get("id").and_then(|v| v.as_str()) else { continue };
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

/// The names of every cell on this machine, read off the named installs'
/// directories.
async fn kept_cells() -> Result<Vec<String>> {
    let dir = crate::ensure::install_dir(&weft_core::infra::Instance::default_install()).join("installs");
    let names = match std::fs::read_dir(&dir) {
        Ok(entries) => entries
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(anyhow::Error::from(e).context(format!("read {}", dir.display()))),
    };
    Ok(cells_in(names))
}

/// Pure half of [`kept_cells`].
fn cells_in(names: Vec<String>) -> Vec<String> {
    names
        .into_iter()
        .filter(|name| name.starts_with(crate::cell::CELL_NAME_PREFIX))
        .filter(|name| weft_core::infra::Instance::named(name).is_ok())
        .collect()
}

/// The fixture copies tests made under the temp directory
/// (`project::unique_tempdir`).
fn remove_fixture_copies() -> Result<()> {
    let tmp = std::env::temp_dir();
    for entry in std::fs::read_dir(&tmp).with_context(|| format!("read {}", tmp.display()))? {
        let entry = entry?;
        if entry.file_name().to_string_lossy().starts_with("weft-e2e-") && entry.path().is_dir() {
            std::fs::remove_dir_all(entry.path())
                .with_context(|| format!("remove {}", entry.path().display()))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn only_cells_are_taken_for_cells() {
        let names = ["e2e1234abcd", "mine", "e2eshort", "e2e-bad"].map(String::from).to_vec();
        assert_eq!(super::cells_in(names), vec!["e2e1234abcd", "e2eshort"]);
    }
}
