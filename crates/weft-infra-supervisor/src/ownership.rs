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
//! renewing; its leases expire after `INFRA_OWNER_LEASE_SECS` and a live
//! supervisor adopts the projects on a later tick.
//!
//! Why a dedicated loop (not folded into the work loops): claiming is an
//! ownership-BREADTH change, while the work loops only ACT on what is
//! owned. Keeping the claim here makes "how many projects this pod owns"
//! change in exactly one place, never as a side effect of doing work.

use anyhow::Result;

use crate::SupervisorState;

pub async fn run_loop(state: SupervisorState) -> Result<()> {
    loop {
        if let Err(e) = tick(&state).await {
            tracing::warn!(error = %e, "ownership tick failed");
        }
        state.clock.sleep(state.poll_interval).await;
    }
}

/// One ownership tick: renew + claim, log the owned breadth. Exposed so
/// integration tests can step it one tick at a time.
pub async fn tick(state: &SupervisorState) -> Result<()> {
    let pressure = state.mem_pressure.fraction();
    let owned = state
        .broker
        .sync_ownership(&state.pod_name, pressure)
        .await?;
    tracing::debug!(
        pod = %state.pod_name,
        owned = owned.len(),
        mem_pressure = pressure,
        "ownership synced (renewed + claimed a batch while under saturation)"
    );
    Ok(())
}
