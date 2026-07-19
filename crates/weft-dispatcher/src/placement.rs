//! Pod placement and sandbox policy seams.
//!
//! Two decisions about WHERE/HOW a worker pod runs:
//!
//!   - `PlacementPolicy`: which kubernetes namespace a project's WORKER lands
//!     in. The default rule is purely structural (a project with infra gets its
//!     own namespace next to its infra; a no-infra project shares the worker pool
//!     namespace).
//!
//!   - `SandboxPolicy`: which `runtimeClassName` (if any) a WORKER pod runs
//!     under, i.e. whether it is sandboxed. The default runs pods on the host
//!     runtime (no sandbox). A sandbox runtime is the right choice for shared-pool
//!     workers, where many tenants coexist in one namespace and the namespace
//!     alone does not isolate them.
//!
//! Both are `Arc<dyn>` on `DispatcherState`, with the structural / no-sandbox
//! defaults.

use std::sync::Arc;

use async_trait::async_trait;

/// Reclaims a deleted project's stored data: the single project-delete cleanup
/// hook. Run as the project is removed (BEFORE the project row is dropped, so a
/// row-cascade can't strand bytes a content tree references).
///
/// The DEFAULT impl (`WipeProjectFiles`) frees the project's `project/`-scoped
/// runtime files from the object store (present whenever a bucket backs
/// `ctx.storage`). One hook, one role: "free this deleted project's stored
/// data." (Runtime files NOT under the `project/` scope, i.e. `shared/`, are the
/// owner's and deliberately outlive the project; this never touches them.)
///
/// Must be idempotent: a `weft rm` retry replays it, and an error aborts the
/// delete so the operator retries rather than leaving stranded data.
#[async_trait]
pub trait ProjectReclaimer: Send + Sync {
    async fn reclaim(
        &self,
        state: &crate::state::DispatcherState,
        tenant: &str,
        project_id: uuid::Uuid,
    ) -> anyhow::Result<()>;
}

/// Decides the kubernetes namespace a project's worker pod runs in.
pub trait PlacementPolicy: Send + Sync {
    /// The worker namespace for a project. `attached_to_infra` is the resolved
    /// placement fact (the project's CURRENT source declares infra AND live
    /// infra state exists; see `resolve_worker_placement`, the ONLY producer
    /// of this input); `tenant` and `project_id` are available to key isolation.
    /// The single source of truth for worker placement: every spawn, DNS
    /// computation, and teardown routes through this so there is no second answer
    /// to "where does this worker live."
    fn worker_namespace(&self, attached_to_infra: bool, tenant: &str, project_id: &str) -> String;
}

/// The resolved answer to "where does this project's worker live RIGHT NOW".
/// Carries only what callers consume: the tenant and the resolved namespace. The
/// `attached_to_infra` fact is computed inside the resolver to feed the policy and
/// is already baked into `namespace`; it is not re-exposed (nothing re-derives
/// placement from it).
#[derive(Debug, Clone)]
pub struct ResolvedPlacement {
    pub tenant: crate::tenant::TenantId,
    pub namespace: String,
}

/// THE single resolver for worker placement. Every spawn path
/// (cold-start, live-connection, worker replacement) routes through
/// here so there is exactly one rule and it cannot fork:
///
///   worker namespace = project namespace IFF
///     the CURRENT source declares infra (the `has_infra` column,
///     refreshed on every register/sync; orphaned live infra whose
///     node was deleted from source does NOT count, so a no-infra
///     graph genuinely runs in the shared pool, unlinked)
///   AND the project's own namespace exists (the `project_namespace`
///     column, stamped only after the namespace actually landed and
///     cleared when it is torn down). Namespace existence, NOT
///     `infra_node` rows, is the anchor: infra Pods are reachable
///     ONLY from inside the project namespace (the namespace's
///     ingress policy admits same-namespace workers and nothing
///     else), so every worker that may talk to infra, INCLUDING the
///     InfraSetup provisioning execution that calls the endpoints it
///     just applied, must run there. `/infra/sync` creates the
///     namespace before it starts the InfraSetup, so provisioning
///     resolves here already.
///
/// `Ok(None)` = project unregistered (caller decides skip vs error).
pub async fn resolve_worker_placement(
    state: &crate::state::DispatcherState,
    project_id: &str,
) -> anyhow::Result<Option<ResolvedPlacement>> {
    let Some(declares_infra) = state.projects.project_has_infra(project_id).await? else {
        return Ok(None);
    };
    let attached_to_infra = declares_infra
        && state
            .projects
            .project_namespace(project_id)
            .await?
            .is_some_and(|ns| !ns.is_empty());
    let tenant = state.tenant_router.tenant_for_project(project_id).await?;
    let namespace =
        state
            .placement
            .worker_namespace(attached_to_infra, tenant.as_str(), project_id);
    Ok(Some(ResolvedPlacement { tenant, namespace }))
}

/// Decides the `runtimeClassName` (the sandbox runtime) for a worker pod, or
/// `None` to run on the host runtime (no sandbox). Only worker pods land as pods
/// through this seam, so only worker pods are sandboxed here.
pub trait SandboxPolicy: Send + Sync {
    /// The runtime class for a worker pod landing in `namespace`, or `None` for
    /// the host runtime. Keying on `namespace` allows sandboxing only the
    /// shared-pool workers (where the namespace does not isolate tenants) and
    /// leaving own-namespace workers on the host runtime, without a code fork.
    fn runtime_class(&self, namespace: &str) -> Option<String>;
}

/// The structural placement: a project with infra gets its own namespace (its
/// worker sits next to its infra pods); a no-infra project shares the worker
/// pool namespace. The default rule.
pub struct LocalPlacementPolicy;

impl PlacementPolicy for LocalPlacementPolicy {
    fn worker_namespace(&self, attached_to_infra: bool, tenant: &str, project_id: &str) -> String {
        // Delegate to the canonical structural rule (one body, with its own
        // tests in `project_namespace`).
        crate::project_namespace::worker_namespace(attached_to_infra, tenant, project_id)
    }
}

/// The no-sandbox default: every pod runs on the host runtime (per-pod
/// sandboxing is not applied).
pub struct NoSandbox;

impl SandboxPolicy for NoSandbox {
    fn runtime_class(&self, _namespace: &str) -> Option<String> {
        None
    }
}

/// The default reclaimer: free the project's per-project storage from the
/// object store: its `project/`-scoped runtime files (persistent state a
/// running node wrote) AND its `asset/`-scoped published assets (the sync's
/// derived copies of source-referenced media). Both are tied to the project's
/// lifetime by design and go away with it. `shared/`-scoped files are the
/// owner's, not the project's, and are deliberately left untouched (they
/// outlive the project). This runtime-files plane always exists, so it is the
/// default.
pub struct WipeProjectFiles;

#[async_trait]
impl ProjectReclaimer for WipeProjectFiles {
    async fn reclaim(
        &self,
        state: &crate::state::DispatcherState,
        tenant: &str,
        project_id: uuid::Uuid,
    ) -> anyhow::Result<()> {
        // Both prefixes through the validated constructors: a hand-built
        // format string would skip the segment grammar that keeps a wipe
        // inside its owner boundary.
        let project = project_id.to_string();
        let project_files =
            weft_core::storage::key::ParsedKey::project_prefix(tenant, &project)
                .map_err(|e| anyhow::anyhow!("project wipe prefix: {e}"))?;
        let assets = weft_core::storage::key::ParsedKey::asset_prefix(tenant, &project)
            .map_err(|e| anyhow::anyhow!("asset wipe prefix: {e}"))?;
        crate::storage::wipe_prefix(state, &project_files).await?;
        crate::storage::wipe_prefix(state, &assets).await?;
        Ok(())
    }
}

pub fn local_placement_policy() -> Arc<dyn PlacementPolicy> {
    Arc::new(LocalPlacementPolicy)
}

pub fn no_sandbox() -> Arc<dyn SandboxPolicy> {
    Arc::new(NoSandbox)
}

/// The default project reclaimer: wipe the project's runtime files.
pub fn default_reclaimer() -> Arc<dyn ProjectReclaimer> {
    Arc::new(WipeProjectFiles)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::project_namespace::{name_for, SHARED_WORKER_NAMESPACE};

    #[test]
    fn local_placement_matches_the_structural_rule() {
        let p = LocalPlacementPolicy;
        // No infra: the shared namespace, regardless of tenant/project.
        assert_eq!(p.worker_namespace(false, "local", "p1"), SHARED_WORKER_NAMESPACE);
        assert_eq!(p.worker_namespace(false, "tenant-xyz", "p2"), SHARED_WORKER_NAMESPACE);
        // Infra: the project's own namespace.
        assert_eq!(p.worker_namespace(true, "local", "p1"), name_for("local", "p1"));
        // A no-infra and an infra project never share a namespace.
        assert_ne!(
            p.worker_namespace(false, "local", "p1"),
            p.worker_namespace(true, "local", "p1")
        );
    }

    #[test]
    fn no_sandbox_never_sets_a_runtime_class() {
        let s = NoSandbox;
        assert_eq!(s.runtime_class(SHARED_WORKER_NAMESPACE), None);
        assert_eq!(s.runtime_class("wft-project-x--y"), None);
    }
}
