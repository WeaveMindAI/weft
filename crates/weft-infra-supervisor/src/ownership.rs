//! Ownership loop. The single site that claims + renews this
//! supervisor's EXCLUSIVE leases over project infrastructure.
//!
//! Each tick reports this pod's memory pressure to the broker's
//! `sync_ownership`, which (atomically) records the pressure, renews
//! every project this pod already owns, and claims a BATCH of more
//! unowned-or-expired projects' infra ONLY while the pod is below the
//! shared memory saturation threshold (a saturated pod keeps what it
//! owns but takes on no more, so the dispatcher spawns another
//! supervisor). Both work loops (lifecycle, health) then act ONLY on the
//! owned set (read via `owned_projects`), so two supervisors never run
//! kubectl against the same project. A crashed supervisor stops
//! renewing; its leases expire after `infra_owner_lease_secs` and a live
//! supervisor adopts the projects on a later tick.
//! What a tick changed for this pod (projects taken on, projects lost)
//! goes to both work loops as an `OwnershipChange`, so each stops acting
//! on a lost project the moment the loss is known.
//!
//! Why a dedicated loop (not folded into the work loops): claiming is an
//! ownership-BREADTH change, while the work loops only ACT on what is
//! owned. Keeping the claim here makes "how many projects this pod owns"
//! change in exactly one place, never as a side effect of doing work.

use std::collections::HashSet;

use anyhow::{anyhow, Result};
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;

use crate::SupervisorState;

/// What one ownership tick changed for this pod, for the work loops.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OwnershipChange {
    /// Projects the tick took on (freshly claimed, or this pod's own
    /// lapsed lease revived). A command issued on one while nobody held
    /// it woke no claim of this pod's, so the lifecycle loop asks again.
    pub claimed: Vec<Uuid>,
    /// Projects this pod no longer owns. Another pod runs their commands
    /// now, so the lifecycle loop stops whatever it was running for them.
    pub lost: Vec<Uuid>,
}

impl OwnershipChange {
    fn is_empty(&self) -> bool {
        self.claimed.is_empty() && self.lost.is_empty()
    }
}

/// Tick for as long as the pod lives, handing every change to each work
/// loop in `changes` (lifecycle and health).
pub async fn run_loop(state: SupervisorState, changes: Vec<UnboundedSender<OwnershipChange>>) -> Result<()> {
    let mut owned = HashSet::new();
    loop {
        match tick(&state, &mut owned).await {
            Ok(Some(change)) => {
                for loop_changes in &changes {
                    loop_changes
                        .send(change.clone())
                        .map_err(|_| anyhow!("a work loop is gone; it no longer follows ownership changes"))?;
                }
            }
            Ok(None) => {}
            Err(e) => tracing::warn!(error = %e, "ownership tick failed"),
        }
        tokio::select! {
            () = state.clock.sleep(state.ownership_interval) => {}
            () = state.ownership_wanted.notified() => {}
        }
    }
}

/// One ownership tick: renew + claim, log the owned breadth, update
/// `owned` (what this pod owned after the previous tick) and hand back
/// what changed, if anything. Exposed so integration tests can step it
/// one tick at a time.
pub async fn tick(state: &SupervisorState, owned: &mut HashSet<Uuid>) -> Result<Option<OwnershipChange>> {
    let pressure = state.mem_pressure.fraction();
    let synced = state
        .broker
        .sync_ownership(&state.pod_name, pressure)
        .await?;
    tracing::debug!(
        pod = %state.pod_name,
        owned = synced.owned.len(),
        claimed = synced.claimed.len(),
        mem_pressure = pressure,
        "ownership synced (renewed + claimed a batch while under saturation)"
    );
    let now: HashSet<Uuid> = synced.owned.iter().map(|p| p.project_id).collect();
    let mut lost: Vec<Uuid> = owned.difference(&now).copied().collect();
    lost.sort_unstable();
    *owned = now;
    let change = OwnershipChange { claimed: synced.claimed, lost };
    Ok((!change.is_empty()).then_some(change))
}
