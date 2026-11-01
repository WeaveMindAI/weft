//! Freeing what a deleted project stored.

use std::sync::Arc;

use async_trait::async_trait;

/// Reclaims a deleted project's stored data: the single project-delete cleanup
/// hook. Run as the project is removed (BEFORE the project row is dropped, so a
/// row-cascade can't strand bytes a content tree references).
///
/// The DEFAULT impl (`WipeProjectFiles`) frees the project's `project/`- and
/// `instance/`-scoped runtime files from the object store (present whenever a bucket backs
/// `ctx.storage`). One hook, one role: "free this deleted project's stored
/// data." (Its runs' files go with the runs' records, erased with the
/// project; its instances' files live inside it and go with it; `shared/`
/// files belong to a space, not the project, and outlive it: only a person
/// or their deadline deletes them, so this never touches them.)
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
/// running node wrote) and every instance's files, which live inside the
/// project and go away with it. Its references to the tenant's assets are
/// dropped, so an asset no other project of the tenant references starts its
/// countdown; one another project still references stays. `shared/`-scoped
/// files are a space's, not the project's, and are left untouched. This
/// runtime-files plane always exists, so it is the default.
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
        let removed = weft_core::storage::key::ParsedKey::removed_with_project(tenant, &project)
            .map_err(|e| anyhow::anyhow!("project wipe prefix: {e}"))?;
        for prefix in &removed {
            crate::storage::wipe_prefix(state, prefix).await?;
        }
        crate::storage::set_asset_references(
            state,
            tenant,
            weft_core::storage::AssetReferencesRequest { project, keys: Vec::new(), kept: Vec::new() },
        )
        .await?;
        Ok(())
    }
}

/// The default project reclaimer: wipe the project's and its instances'
/// runtime files, and drop its asset references.
pub fn default_reclaimer() -> Arc<dyn ProjectReclaimer> {
    Arc::new(WipeProjectFiles)
}
