//! The resize watcher: grows the pool before it fills, shrinks it
//! (evacuate one disk, release the PVC) after a SUSTAINED surplus.
//! The decision is a pure function (Layer 1); the watcher loop wires
//! it to the store + clock and is exercised by the Layer-3 rig.
//!
//! Guards: grow is immediate (running out of space blocks users);
//! shrink needs the surplus to hold through a dwell window AND a
//! cooldown since the last resize (persisted in boxstate, so the pod
//! restart a resize causes doesn't wipe it). One resize at a time by
//! construction: a single watcher per box, and the dispatcher
//! serializes disk add/remove per tenant on its side.

use std::sync::Arc;

use crate::config::{
    GROW_FREE_THRESHOLD_FRACTION, RESIZE_TICK_INTERVAL, SHRINK_COOLDOWN, SHRINK_DWELL,
    SHRINK_HEADROOM_FRACTION,
};
use crate::disk::DiskInfo;
use crate::store::Store;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResizeDecision {
    None,
    Grow,
    /// Evacuate the shrink candidate disk, then release its PVC. The
    /// watcher recomputes the candidate from the same disk snapshot
    /// `decide` saw, so the decision stays a bare variant.
    Shrink,
}

/// Everything the pure decision reads. Built by the watcher tick.
#[derive(Debug, Clone)]
pub struct ResizeInputs {
    pub disks: Vec<DiskInfo>,
    pub draining: usize,
    /// Per-tenant backing-disk unit size (bytes); from the profile.
    pub disk_unit_bytes: u64,
    /// Unix seconds now / of the last resize / since when the shrink
    /// condition has held continuously (None = not currently held).
    pub now_unix: i64,
    pub last_resize_at_unix: Option<i64>,
    pub surplus_since_unix: Option<i64>,
}

/// Would dropping the emptiest disk still leave `headroom` free?
/// (Pure helper; also picks the shrink candidate.)
pub fn shrink_candidate(disks: &[DiskInfo], disk_unit_bytes: u64) -> Option<&DiskInfo> {
    if disks.len() <= 1 {
        return None;
    }
    let total_free: u64 = disks.iter().map(|d| d.free_bytes).sum();
    // Candidate = the disk with the most free space (least to move).
    let candidate = disks.iter().max_by_key(|d| d.free_bytes)?;
    let headroom = (disk_unit_bytes as f64 * SHRINK_HEADROOM_FRACTION) as u64;
    let used_on_candidate = candidate.total_bytes - candidate.free_bytes;
    // After release: the candidate's used bytes move onto the rest's
    // free space; require the remainder to keep `headroom`.
    let free_after = total_free
        .saturating_sub(candidate.free_bytes)
        .saturating_sub(used_on_candidate);
    if free_after >= headroom {
        Some(candidate)
    } else {
        None
    }
}

/// The pure resize decision.
pub fn decide(inputs: &ResizeInputs) -> ResizeDecision {
    // Never overlap resizes: a draining disk means an evacuation is
    // in progress (or resuming); let it finish.
    if inputs.draining > 0 {
        return ResizeDecision::None;
    }
    let total_free: u64 = inputs.disks.iter().map(|d| d.free_bytes).sum();
    let grow_threshold = (inputs.disk_unit_bytes as f64 * GROW_FREE_THRESHOLD_FRACTION) as u64;
    if total_free < grow_threshold {
        return ResizeDecision::Grow;
    }
    if shrink_candidate(&inputs.disks, inputs.disk_unit_bytes).is_none() {
        return ResizeDecision::None;
    }
    // Dwell: the surplus must have held continuously.
    let Some(since) = inputs.surplus_since_unix else {
        return ResizeDecision::None;
    };
    if inputs.now_unix - since < SHRINK_DWELL.as_secs() as i64 {
        return ResizeDecision::None;
    }
    // Cooldown since the last resize.
    if let Some(last) = inputs.last_resize_at_unix {
        if inputs.now_unix - last < SHRINK_COOLDOWN.as_secs() as i64 {
            return ResizeDecision::None;
        }
    }
    ResizeDecision::Shrink
}

/// The watcher. `tick()` is public so tests step one iteration;
/// `run_loop` wraps it with the clock's sleep.
pub struct ResizeWatcher {
    store: Arc<Store>,
    disk_unit_bytes: u64,
    /// Unix seconds since when the shrink condition has held
    /// continuously. In-RAM: a restart resets the dwell (conservative:
    /// delays a shrink, never rushes one).
    surplus_since_unix: Option<i64>,
}

impl ResizeWatcher {
    pub fn new(store: Arc<Store>, disk_unit_bytes: u64) -> Self {
        Self { store, disk_unit_bytes, surplus_since_unix: None }
    }

    pub async fn tick(&mut self) -> anyhow::Result<()> {
        let now = self.store.clock().now_unix();
        let disks = self.store.pool().disks().await?;
        let draining = self.store.draining_disks().await;

        // Resume an interrupted evacuation before anything else: a
        // draining disk must finish draining (the marker persists
        // across restarts exactly for this).
        if let Some(disk) = draining.iter().next().cloned() {
            tracing::info!(target: "weft_storage::resize", disk = %disk, "resuming evacuation");
            self.store.evacuate(&disk).await?;
            self.store.stamp_resize().await?;
            return Ok(());
        }

        // Track the dwell window.
        let surplus_now = shrink_candidate(&disks, self.disk_unit_bytes).is_some();
        self.surplus_since_unix = match (surplus_now, self.surplus_since_unix) {
            (false, _) => None,
            (true, None) => Some(now),
            (true, Some(s)) => Some(s),
        };

        let inputs = ResizeInputs {
            disks: disks.clone(),
            draining: draining.len(),
            disk_unit_bytes: self.disk_unit_bytes,
            now_unix: now,
            last_resize_at_unix: self.store.last_resize_at_unix().await,
            surplus_since_unix: self.surplus_since_unix,
        };
        match decide(&inputs) {
            ResizeDecision::None => Ok(()),
            ResizeDecision::Grow => {
                tracing::info!(
                    target: "weft_storage::resize",
                    free = inputs.disks.iter().map(|d| d.free_bytes).sum::<u64>(),
                    "pool low on space; requesting a disk"
                );
                self.store.pool().request_disk_add().await?;
                self.store.stamp_resize().await?;
                Ok(())
            }
            ResizeDecision::Shrink => {
                let candidate = shrink_candidate(&disks, self.disk_unit_bytes)
                    .expect("decide() returned Shrink so a candidate exists")
                    .name
                    .clone();
                tracing::info!(
                    target: "weft_storage::resize",
                    disk = %candidate,
                    "sustained surplus; evacuating a disk to release it"
                );
                self.surplus_since_unix = None;
                self.store.evacuate(&candidate).await?;
                self.store.stamp_resize().await?;
                Ok(())
            }
        }
    }

    pub async fn run_loop(mut self) {
        loop {
            if let Err(e) = self.tick().await {
                tracing::error!(target: "weft_storage::resize", error = %e, "resize tick failed");
            }
            self.store.clock().sleep(RESIZE_TICK_INTERVAL).await;
        }
    }
}

/// The kept-file expiry sweep loop (same wrap-a-tick shape).
pub async fn run_expiry_loop(store: Arc<Store>) {
    loop {
        if let Err(e) = store.expiry_sweep().await {
            tracing::error!(target: "weft_storage::expiry", error = %e, "expiry sweep failed");
        }
        store.clock().sleep(crate::config::EXPIRY_SWEEP_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn disk(name: &str, free: u64, total: u64) -> DiskInfo {
        DiskInfo { name: name.into(), free_bytes: free, total_bytes: total }
    }

    const GI: u64 = 1 << 30;

    fn inputs(disks: Vec<DiskInfo>) -> ResizeInputs {
        ResizeInputs {
            disks,
            draining: 0,
            disk_unit_bytes: 10 * GI,
            now_unix: 100_000,
            last_resize_at_unix: None,
            surplus_since_unix: Some(0),
        }
    }

    #[test]
    fn grows_when_free_below_threshold() {
        // 2 GiB free < half a 10 GiB unit.
        let i = inputs(vec![disk("disk-0", 2 * GI, 10 * GI)]);
        assert_eq!(decide(&i), ResizeDecision::Grow);
    }

    #[test]
    fn idles_in_the_healthy_band() {
        // 6 GiB free: above grow threshold, not enough surplus to
        // drop a disk and keep headroom.
        let i = inputs(vec![disk("disk-0", 6 * GI, 10 * GI)]);
        assert_eq!(decide(&i), ResizeDecision::None);
    }

    #[test]
    fn shrinks_after_dwell_when_a_disk_is_droppable() {
        // Two nearly-empty disks: drop one, plenty of headroom left.
        let i = ResizeInputs {
            surplus_since_unix: Some(100_000 - SHRINK_DWELL.as_secs() as i64),
            ..inputs(vec![disk("disk-0", 9 * GI, 10 * GI), disk("disk-1", 10 * GI, 10 * GI)])
        };
        assert_eq!(decide(&i), ResizeDecision::Shrink);
    }

    #[test]
    fn dwell_not_elapsed_blocks_shrink() {
        let i = ResizeInputs {
            surplus_since_unix: Some(100_000 - SHRINK_DWELL.as_secs() as i64 + 1),
            ..inputs(vec![disk("disk-0", 9 * GI, 10 * GI), disk("disk-1", 10 * GI, 10 * GI)])
        };
        assert_eq!(decide(&i), ResizeDecision::None);
    }

    #[test]
    fn surplus_interrupted_resets_the_window() {
        // The watcher models the thrash case (delete then re-download
        // a big file) as surplus_since=None when the condition does
        // not currently hold; decide never shrinks then.
        let i = ResizeInputs {
            surplus_since_unix: None,
            ..inputs(vec![disk("disk-0", 9 * GI, 10 * GI), disk("disk-1", 10 * GI, 10 * GI)])
        };
        assert_eq!(decide(&i), ResizeDecision::None);
    }

    #[test]
    fn cooldown_blocks_shrink() {
        let i = ResizeInputs {
            surplus_since_unix: Some(0),
            last_resize_at_unix: Some(100_000 - SHRINK_COOLDOWN.as_secs() as i64 + 1),
            ..inputs(vec![disk("disk-0", 9 * GI, 10 * GI), disk("disk-1", 10 * GI, 10 * GI)])
        };
        assert_eq!(decide(&i), ResizeDecision::None);
    }

    #[test]
    fn never_drops_the_last_disk() {
        let i = inputs(vec![disk("disk-0", 10 * GI, 10 * GI)]);
        assert_eq!(decide(&i), ResizeDecision::None);
    }

    #[test]
    fn in_progress_drain_blocks_everything() {
        let i = ResizeInputs {
            draining: 1,
            ..inputs(vec![disk("disk-0", GI / 2, 10 * GI), disk("disk-1", 10 * GI, 10 * GI)])
        };
        assert_eq!(decide(&i), ResizeDecision::None);
    }

    #[test]
    fn candidate_is_the_emptiest_disk() {
        let disks = vec![
            disk("disk-0", 2 * GI, 10 * GI),
            disk("disk-1", 10 * GI, 10 * GI),
            disk("disk-2", 5 * GI, 10 * GI),
        ];
        assert_eq!(shrink_candidate(&disks, 10 * GI).unwrap().name, "disk-1");
    }
}
