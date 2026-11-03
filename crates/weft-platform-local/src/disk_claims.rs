//! Which install takes a disk volume named before infra names carried the
//! install, when weft cannot tell on its own (`infra_names::plan_disk`).
//!
//! One file per volume in the machine's `MachineLayout::disk_claims`,
//! named after the volume and holding the install's label: `weft infra
//! take-disk` writes it, the install's next start of that infra carries
//! the volume over and removes it. One folder for the whole machine, so
//! the first claim is the only one.

use std::io::Write as _;
use std::path::Path;

/// The install that claimed the volume `volume`, if one did.
pub fn claimed_by(dir: &Path, volume: &str) -> anyhow::Result<Option<String>> {
    match std::fs::read_to_string(dir.join(volume)) {
        Ok(install) => Ok(Some(install.trim().to_string())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(anyhow::anyhow!("read the claim on the volume {volume} in {}: {e}", dir.display())),
    }
}

/// Say that `install` takes the volume `volume`. Refused when another
/// install claimed it first; claiming it again for the same one is fine.
pub fn claim(dir: &Path, volume: &str, install: &str) -> anyhow::Result<()> {
    std::fs::create_dir_all(dir).map_err(|e| anyhow::anyhow!("create {}: {e}", dir.display()))?;
    let path = dir.join(volume);
    match std::fs::OpenOptions::new().write(true).create_new(true).open(&path) {
        Ok(mut file) => file.write_all(install.as_bytes()).map_err(|e| anyhow::anyhow!("write {}: {e}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => match claimed_by(dir, volume)? {
            Some(already) if already == install => Ok(()),
            already => anyhow::bail!(
                "install '{}' already claimed the volume {volume}; if it should go to '{install}' instead, delete {} and run this again",
                already.unwrap_or_default(),
                path.display()
            ),
        },
        Err(e) => Err(anyhow::anyhow!("write {}: {e}", path.display())),
    }
}

/// Forget the claim on `volume`, once it is carried over.
pub fn release(dir: &Path, volume: &str) -> anyhow::Result<()> {
    match std::fs::remove_file(dir.join(volume)) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(anyhow::anyhow!("remove the claim on the volume {volume} in {}: {e}", dir.display())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_claim_holds_until_it_is_released() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(claimed_by(dir.path(), "wi-db-1-data").unwrap(), None);
        claim(dir.path(), "wi-db-1-data", "alpha").unwrap();
        claim(dir.path(), "wi-db-1-data", "alpha").unwrap();
        let refused = claim(dir.path(), "wi-db-1-data", "beta").unwrap_err().to_string();
        assert!(refused.contains("'alpha' already claimed"), "{refused}");
        assert_eq!(claimed_by(dir.path(), "wi-db-1-data").unwrap().as_deref(), Some("alpha"));
        release(dir.path(), "wi-db-1-data").unwrap();
        release(dir.path(), "wi-db-1-data").unwrap();
        claim(dir.path(), "wi-db-1-data", "beta").unwrap();
    }
}
