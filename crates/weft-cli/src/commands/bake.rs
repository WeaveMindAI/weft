//! Prepare and save trigger settings without arming listeners. The
//! setup runs on a worker of the registered build; nothing running is
//! touched.

use anyhow::Context;
use super::Ctx;
use crate::progress::ActionVerb;

pub async fn run(
    ctx: Ctx,
    project: Option<String>,
    node_set: weft_core::builds::NodeSet,
    scope: weft_core::activation::ActivationScope,
) -> anyhow::Result<()> {
    let inner = ctx.clone();
    ctx.with_progress(ActionVerb::Bake, |progress| async move {
        let (client, id) = match project {
            Some(id) => (inner.client()?, id),
            None => {
                let handle = super::ensure::ensure_registered(&inner, &progress, node_set).await?;
                (handle.client, handle.id)
            }
        };
        let path = format!("/projects/{id}/bake");
        let body = weft_core::activation::BakeRequest { scope };
        progress.dispatcher_call_start(&path);
        // It answers once each trigger's setup run ended, however long.
        let result: serde_json::Value = client.waiting_on_its_work().post_json(&path, &serde_json::to_value(&body)?).await?;
        progress.dispatcher_call_done(serde_json::json!({"projectId": id}));
        // The install's own reading of the version it baked, whichever way
        // it was named.
        let broken: Vec<weft_core::project::Broken> = match result.get("broken") {
            Some(broken) => serde_json::from_value(broken.clone()).context("read the parts of the bake that do not compile")?,
            None => Vec::new(),
        };
        super::ensure::warn_broken(&progress, &broken);
        progress.complete_with("trigger settings saved", result);
        Ok(())
    }).await
}
