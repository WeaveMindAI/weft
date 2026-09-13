//! Prepare and save trigger settings without arming listeners.

use super::Ctx;
use crate::progress::ActionVerb;

pub async fn run(ctx: Ctx, project: Option<String>, node_set: weft_compiler::codegen::NodeSet) -> anyhow::Result<()> {
    let inner = ctx.clone();
    ctx.with_progress(ActionVerb::Bake, |progress| async move {
        let (client, id) = match project {
            Some(id) => (inner.client(), id),
            None => {
                let handle = super::ensure::ensure_registered(&inner, &progress, node_set).await?;
                (handle.client, handle.id)
            }
        };
        let path = format!("/projects/{id}/bake");
        progress.dispatcher_call_start(&path);
        let result: serde_json::Value = client.post_json(&path, &serde_json::json!({})).await?;
        progress.dispatcher_call_done(serde_json::json!({"project_id": id}));
        progress.complete_with("trigger settings saved", result);
        Ok(())
    }).await
}
