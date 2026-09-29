//! Freeing what a deleted project stored.

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
        // Every prefix through the validated constructors: a hand-built
        // format string would skip the segment grammar that keeps a wipe
        // inside its owner boundary.
        let project = project_id.to_string();
        let project_files =
            weft_core::storage::key::ParsedKey::project_prefix(tenant, &project)
                .map_err(|e| anyhow::anyhow!("project wipe prefix: {e}"))?;
        let assets = weft_core::storage::key::ParsedKey::asset_prefix(tenant, &project)
            .map_err(|e| anyhow::anyhow!("asset wipe prefix: {e}"))?;
        // Every member's files: a member id is only a name inside its
        // project, so they go with the project.
        let members = weft_core::storage::key::ParsedKey::members_prefix(tenant, &project)
            .map_err(|e| anyhow::anyhow!("member wipe prefix: {e}"))?;
        crate::storage::wipe_prefix(state, &project_files).await?;
        crate::storage::wipe_prefix(state, &assets).await?;
        crate::storage::wipe_prefix(state, &members).await?;
        Ok(())
    }
}

/// The default project reclaimer: wipe the project's runtime files.
pub fn default_reclaimer() -> Arc<dyn ProjectReclaimer> {
    Arc::new(WipeProjectFiles)
}
