//! Ownership loop. The single site that claims + renews this
//! supervisor's EXCLUSIVE leases over project infrastructure.
//!
//! Each tick calls the broker's `sync_ownership`, which (atomically)
//! renews every project this supervisor already owns and claims a BATCH
//! of more unowned-or-expired projects' infra. Both work loops
//! (lifecycle, health) then act ONLY on the owned set (read via
//! `owned_projects`), so two supervisors never act on the same project's
//! infrastructure at once. A crashed supervisor stops renewing; its
//! leases expire after `infra_owner_lease_secs` and a live supervisor
//! adopts the projects on a later tick.
//!
//! Each tick also sweeps what the host holds for copies that are gone
//! for good (their project was removed, or the program no longer
//! declares them), deleting all of it, the disks a node listed in
//! `keepOnTerminate` included. The sweep only touches a project this
//! supervisor holds the lease of, so the tick also asks for the lease of
//! every project the host holds copies of, infra or not.
//! What a tick changed for this supervisor (projects taken on, projects lost)
//! goes to both work loops as an `OwnershipChange`, so each stops acting
//! on a lost project the moment the loss is known.
//!
//! Why a dedicated loop (not folded into the work loops): claiming is an
//! ownership-BREADTH change, while the work loops only ACT on what is
//! owned. Keeping the claim here makes "how many projects this supervisor owns"
//! change in exactly one place, never as a side effect of doing work.

use std::collections::{BTreeMap, HashSet};

use anyhow::{anyhow, Result};
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;
use weft_core::infra::NodeRef;

use crate::SupervisorState;

/// What one ownership tick changed for this supervisor, for the work loops.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OwnershipChange {
    /// Projects the tick took on (freshly claimed, or this supervisor's own
    /// lapsed lease revived). A command issued on one while nobody held
    /// it woke no claim of this supervisor's, so the lifecycle loop asks again.
    pub claimed: Vec<Uuid>,
    /// Projects this supervisor no longer owns. Another runs their commands
    /// now, so the lifecycle loop stops whatever it was running for them.
    pub lost: Vec<Uuid>,
}

impl OwnershipChange {
    fn is_empty(&self) -> bool {
        self.claimed.is_empty() && self.lost.is_empty()
    }
}

/// Tick for as long as the process lives, handing every change to each work
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

/// One ownership tick: list what the host holds, renew + claim (the
/// held projects included), sweep the gone copies under the leases just
/// renewed, update `owned` (what this supervisor owned after the previous
/// tick) and hand back what changed, if anything. Exposed so integration
/// tests can step it one tick at a time.
pub async fn tick(state: &SupervisorState, owned: &mut HashSet<Uuid>) -> Result<Option<OwnershipChange>> {
    // A failed listing skips this tick's sweep but never the renewal:
    // losing every lease over a host hiccup would hand the projects over.
    let held = match state.host.copies().await {
        Ok(copies) => Some(copies),
        Err(e) => {
            tracing::warn!(error = %format!("{e:#}"), "listing what the host holds failed; the next tick sweeps");
            None
        }
    };
    let mut held_projects: Vec<Uuid> = held.iter().flatten().map(|c| c.project).collect();
    held_projects.sort_unstable();
    held_projects.dedup();
    let synced = state.broker.sync_ownership(&state.instance, &held_projects).await?;
    tracing::debug!(
        instance = %state.instance,
        owned = synced.owned.len(),
        claimed = synced.claimed.len(),
        "ownership synced (renewed + claimed a batch)"
    );
    if let Some(copies) = held {
        sweep_gone_copies(state, copies).await;
    }
    let now: HashSet<Uuid> = synced.owned.iter().map(|p| p.project_id).collect();
    let mut lost: Vec<Uuid> = owned.difference(&now).copied().collect();
    lost.sort_unstable();
    *owned = now;
    let change = OwnershipChange { claimed: synced.claimed, lost };
    Ok((!change.is_empty()).then_some(change))
}

/// Delete everything the host holds (`copies`, its listing) for a copy
/// that is gone for good.
///
/// A terminate keeps the disks a node listed in `keepOnTerminate`, so
/// the next start of the same copy adopts them, and its row goes. That
/// is right while the copy can come back, and a leak once it cannot:
/// its project was removed (with `--force` not even waiting for the
/// terminate), or the program no longer declares it (the node was
/// removed, or changed sides between shared and per member), whether
/// the orphan reap terminated it or it was already down with no row
/// left to reap. The host is the only place such a copy still shows,
/// so the sweep starts from the host's listing, and the broker judges
/// which of those copies are gone. A copy still holding a row is never
/// gone here: a user terminate or the reap owns it until the row goes.
///
/// A copy's id is derived from (project, node, member), so a node added
/// back names the very copy a judgment called gone, and an apply
/// adopting its kept disks must never overlap its deletion. Two guards,
/// both held from the judgment through the deletion:
/// - The project's `infra_owner` lease. The broker judges an existing
///   project only for its owner, the one supervisor that can be applying
///   it (the tick claimed the lease of every project the host holds, so
///   an unleased one is judged by whoever took it). Each gone copy is
///   judged again right before its deletion, the same fence a lifecycle
///   write takes right before its host call, so a lease that moved in
///   between aborts the rest of the project's deletions. What remains is
///   the window every fenced host call has: between that last check and
///   the host call, which a new owner can only enter after this
///   supervisor stopped renewing for a whole lease.
/// - The project's lock (`ProjectLocks`), the one every lifecycle command
///   of this process holds, so this supervisor's own apply of the project
///   cannot interleave. A project a command holds is skipped this tick
///   rather than waited on.
///
/// A removed project (no row) is judged for anyone: nothing can apply it
/// until it is registered again, which the per-copy check right before
/// each deletion also catches, down to the same window.
///
/// One project or copy failing is logged and the rest still go.
/// Idempotent: a copy another supervisor deleted first is simply gone
/// from the next listing.
pub async fn sweep_gone_copies(state: &SupervisorState, copies: Vec<NodeRef>) {
    let mut by_project: BTreeMap<Uuid, Vec<NodeRef>> = BTreeMap::new();
    for copy in copies {
        by_project.entry(copy.project).or_default().push(copy);
    }
    for (project, copies) in by_project {
        let Some(_project) = state.project_locks.try_lock(project) else {
            tracing::debug!(project_id = %project, "a lifecycle command holds the project; its gone copies wait for the next tick");
            continue;
        };
        if let Err(e) = sweep_project(state, project, &copies).await {
            tracing::warn!(project_id = %project, error = %format!("{e:#}"), "sweeping the project's gone copies failed; the next tick tries again");
        }
    }
}

/// One project's sweep, under its lock: judge its copies, then delete
/// each gone one after judging it again (the lease fence).
async fn sweep_project(state: &SupervisorState, project: Uuid, copies: &[NodeRef]) -> Result<()> {
    let Some(gone) = state.broker.gone_copies(&state.instance, project, copies).await? else {
        tracing::debug!(project_id = %project, "another supervisor holds the project's lease; its copies are its to sweep");
        return Ok(());
    };
    for copy in copies.iter().filter(|c| gone.contains(&c.instance)) {
        match state.broker.gone_copies(&state.instance, project, std::slice::from_ref(copy)).await? {
            None => {
                tracing::info!(project_id = %project, "the project's lease moved mid-sweep; leaving its copies to the new owner");
                return Ok(());
            }
            Some(still) if still.is_empty() => continue,
            Some(_) => {}
        }
        tracing::info!(
            project_id = %copy.project,
            node = %copy.node,
            instance = %copy.instance,
            "the copy is gone for good; deleting what the host still holds for it, kept disks included"
        );
        if let Err(e) = state.host.terminate(copy, &[]).await {
            tracing::warn!(
                project_id = %copy.project,
                node = %copy.node,
                instance = %copy.instance,
                error = %format!("{e:#}"),
                "deleting a gone copy failed; the next tick tries again"
            );
        }
    }
    Ok(())
}
