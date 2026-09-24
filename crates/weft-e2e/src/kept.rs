//! The ledger of what tests made that carries no name of its own to find it
//! by later.
//!
//! A project is found by its `e2e_` name and a cell by its `e2e` name, but a
//! connection (an access grant) is named by the service it connects to, so
//! nothing on it tells a test's from yours. Each one a test makes on the
//! default install is written here while it lives, and struck off when the
//! test removes it; whatever a failed test left in the ledger is what the
//! next run of `scripts/run-e2e.sh` removes before it starts. A cell's grants go with the cell.
//!
//! The runner says where the ledger lives (`WEFT_E2E_KEPT_DIR`), outside the
//! per-run log directory, so a new run does not forget what the last one
//! kept.

use std::path::PathBuf;

use anyhow::{Context, Result};

use crate::client::Dispatcher;

/// The variable the runner names the ledger's directory in.
// SYNC: WEFT_E2E_KEPT_DIR <-> scripts/run-e2e.sh
pub const KEPT_DIR_ENV: &str = "WEFT_E2E_KEPT_DIR";

/// A grant `disp`'s install now holds for this test.
pub(crate) fn grant_made(disp: &Dispatcher, grant_id: &str) -> Result<()> {
    if disp.instance().name().is_some() {
        return Ok(());
    }
    default_install_grant_made(grant_id)
}

/// A grant the default install now holds for this test (one seeded straight
/// into its database).
pub(crate) fn default_install_grant_made(grant_id: &str) -> Result<()> {
    let dir = grants_dir()?;
    std::fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
    std::fs::write(dir.join(grant_id), b"").with_context(|| format!("record grant {grant_id}"))
}

/// The test removed the grant.
pub(crate) fn grant_gone(grant_id: &str) -> Result<()> {
    let path = grants_dir()?.join(grant_id);
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("strike grant {grant_id} off the ledger")),
    }
}

/// Every grant the ledger still holds: what failed tests kept. Read
/// only by the sweep, which reaches the cluster, so `e2e` only.
#[cfg(feature = "e2e")]
pub(crate) fn kept_grants() -> Result<Vec<String>> {
    let dir = grants_dir()?;
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("read {}", dir.display())),
    };
    entries
        .map(|e| Ok(e?.file_name().to_string_lossy().into_owned()))
        .collect()
}

fn grants_dir() -> Result<PathBuf> {
    let root = std::env::var(KEPT_DIR_ENV).map_err(|_| {
        anyhow::anyhow!(
            "{KEPT_DIR_ENV} is not set: e2e tests run through scripts/run-e2e.sh, which \
             says where the ledger of what they keep lives"
        )
    })?;
    Ok(PathBuf::from(root).join("grants"))
}
