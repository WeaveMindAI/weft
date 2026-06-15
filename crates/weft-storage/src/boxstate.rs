//! Box-level durable state: the capability secret, the shared-name
//! grant table, and the last-resize stamp. Tiny, rarely written.
//!
//! Persistence model: the state file is REPLICATED to every backing
//! disk with a monotonic version; reads take the highest version
//! found. This survives any single disk's evacuation/release without
//! a "which disk owns the state" special case. A write bumps the
//! version and rewrites every disk; partial success is consistent by
//! construction (newest version wins) and logged loudly, total
//! failure errors.

use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use base64::Engine;
use serde::{Deserialize, Serialize};

use crate::disk::DiskPoolOps;
use crate::index::BOXSTATE_PATH;

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BoxState {
    pub version: u64,
    /// Base64 of the 32-byte HMAC secret all capabilities are minted
    /// and validated with. Minted once at first boot, never rotated
    /// implicitly (teardown only happens at zero bytes, when no
    /// outstanding capability has anything to fetch).
    pub secret_b64: String,
    /// `(project_id, shared_name)` pairs that have used a shared
    /// space. Audit/listing surface; recorded on first use.
    pub grants: BTreeSet<(String, String)>,
    /// Unix seconds of the last completed grow/shrink. Persisted so
    /// the shrink cooldown survives the pod restart a resize itself
    /// causes (an in-RAM cooldown would be wiped by every shrink).
    pub last_resize_at_unix: Option<i64>,
}

impl BoxState {
    pub fn secret(&self) -> Result<Vec<u8>> {
        B64.decode(&self.secret_b64).context("boxstate secret is not valid base64")
    }

    fn fresh() -> Result<Self> {
        let mut secret = [0u8; 32];
        getrandom::getrandom(&mut secret)
            .map_err(|e| anyhow!("OS RNG unavailable for capability secret: {e}"))?;
        Ok(Self {
            version: 0,
            secret_b64: B64.encode(secret),
            grants: BTreeSet::new(),
            last_resize_at_unix: None,
        })
    }
}

/// Read the newest replica across all disks; mint-and-persist a
/// fresh state when none exists (first boot of a new box).
pub async fn load_or_init(pool: &Arc<dyn DiskPoolOps>) -> Result<BoxState> {
    let mut newest: Option<BoxState> = None;
    // A single corrupt replica must NOT brick the box: replication
    // exists precisely so a healthy newer replica on another disk
    // wins. Skip+log a corrupt one and keep scanning. Only a TOTAL
    // loss (replicas existed but every one failed to parse) is fatal,
    // because then the box cannot trust its capability secret.
    let mut seen_any = false;
    let mut all_corrupt = true;
    for disk in pool.disks().await? {
        if let Some(bytes) = pool.read_small(&disk.name, BOXSTATE_PATH).await? {
            seen_any = true;
            match serde_json::from_slice::<BoxState>(&bytes) {
                Ok(state) => {
                    all_corrupt = false;
                    if newest.as_ref().map(|n| state.version > n.version).unwrap_or(true) {
                        newest = Some(state);
                    }
                }
                Err(e) => tracing::error!(
                    target: "weft_storage::boxstate",
                    disk = %disk.name,
                    error = %e,
                    "corrupt boxstate replica skipped; a healthy replica on another disk wins"
                ),
            }
        }
    }
    if seen_any && all_corrupt {
        anyhow::bail!(
            "every boxstate replica is corrupt; the box cannot trust its capability secret. \
             Inspect {BOXSTATE_PATH} on each disk"
        );
    }
    match newest {
        Some(state) => {
            // Re-replicate to EVERY disk on boot. A disk added by a
            // grow (the pod restarts to mount it, so its first boot
            // is here) arrives with no boxstate replica; this gives
            // it one immediately, so the capability secret never
            // depends on a single disk surviving every future
            // evacuation. Idempotent (same version, same content).
            persist(pool, &state, &BTreeSet::new()).await?;
            Ok(state)
        }
        None => {
            let state = BoxState::fresh()?;
            persist(pool, &state, &BTreeSet::new()).await?;
            Ok(state)
        }
    }
}

/// Replicate `state` to every disk EXCEPT those in `skip`. Errors
/// only when no eligible disk took the write; a partial success is
/// consistent (newest version wins on the next load) but logged
/// loudly per failed disk.
///
/// `skip` exists for evacuation: a disk that is draining (about to be
/// removed) must NOT be written to NOR counted toward success. Without
/// this, a write that lands ONLY on the dying disk would report
/// success and then be lost when the disk is released, defeating the
/// whole point of re-replicating before removal. Steady-state callers
/// pass an empty set.
pub async fn persist(
    pool: &Arc<dyn DiskPoolOps>,
    state: &BoxState,
    skip: &std::collections::BTreeSet<String>,
) -> Result<()> {
    let bytes = bytes::Bytes::from(serde_json::to_vec(state).expect("boxstate serializes"));
    let mut ok = 0usize;
    let mut eligible = 0usize;
    let disks = pool.disks().await?;
    for disk in disks.iter().filter(|d| !skip.contains(&d.name)) {
        eligible += 1;
        match pool
            .write_file(&disk.name, BOXSTATE_PATH, weft_core::storage::bytes_stream(bytes.clone()))
            .await
        {
            Ok(_) => ok += 1,
            Err(e) => tracing::warn!(
                target: "weft_storage::boxstate",
                disk = %disk.name,
                error = %e,
                "boxstate replica write failed (version wins on next load)"
            ),
        }
    }
    if ok == 0 {
        return Err(anyhow!(
            "boxstate write failed on every eligible disk ({eligible} of {} tried, \
             {} skipped as draining); refusing to continue with unpersisted state",
            disks.len(),
            skip.len()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::FakeDiskPool;

    #[tokio::test]
    async fn init_mints_secret_and_replicates() {
        let pool = FakeDiskPool::new();
        pool.add_disk("disk-0", 1 << 20);
        pool.add_disk("disk-1", 1 << 20);
        let pool: Arc<dyn DiskPoolOps> = pool.clone();
        let s = load_or_init(&pool).await.unwrap();
        assert_eq!(s.secret().unwrap().len(), 32);
        // Reload returns the SAME secret (replicas found).
        let s2 = load_or_init(&pool).await.unwrap();
        assert_eq!(s, s2);
    }

    #[tokio::test]
    async fn newest_version_wins_across_disks() {
        let fake = FakeDiskPool::new();
        fake.add_disk("disk-0", 1 << 20);
        let pool: Arc<dyn DiskPoolOps> = fake.clone();
        let mut s = load_or_init(&pool).await.unwrap();
        // Bump + persist, then add a disk that still has NO replica;
        // load must return the bumped version, not re-init.
        s.version += 1;
        s.grants.insert(("p1".into(), "team".into()));
        persist(&pool, &s, &BTreeSet::new()).await.unwrap();
        fake.add_disk("disk-1", 1 << 20);
        let loaded = load_or_init(&pool).await.unwrap();
        assert_eq!(loaded, s);
    }

    #[tokio::test]
    async fn persist_skips_draining_disks_and_errors_if_no_eligible() {
        // Fresh pool, NO init (so neither disk has a replica yet), to
        // observe exactly which disks this persist writes.
        let fake = FakeDiskPool::new();
        fake.add_disk("disk-0", 1 << 20);
        fake.add_disk("disk-1", 1 << 20);
        let pool: Arc<dyn DiskPoolOps> = fake.clone();
        let s = BoxState::fresh().unwrap();

        // Persist while disk-0 is draining: disk-0 must NOT receive the
        // replica (it is about to be released), disk-1 must.
        let skip: BTreeSet<String> = ["disk-0".to_string()].into_iter().collect();
        persist(&pool, &s, &skip).await.unwrap();
        assert!(
            pool.read_small("disk-0", BOXSTATE_PATH).await.unwrap().is_none(),
            "draining disk must not receive the boxstate replica"
        );
        assert!(pool.read_small("disk-1", BOXSTATE_PATH).await.unwrap().is_some());

        // Every disk skipped -> no eligible disk took the write -> error
        // (never a silent success that would lose the state).
        let skip_all: BTreeSet<String> =
            ["disk-0".to_string(), "disk-1".to_string()].into_iter().collect();
        assert!(persist(&pool, &s, &skip_all).await.is_err());
    }
}
