//! Prepare and save trigger settings without arming listeners. The
//! setup runs on a worker, so a worker built from an older image is
//! replaced on the way, and `--running-policy` says what happens to
//! its running executions, exactly as on `weft activate`.

use super::ensure::{parse_running_choice, running_choice_fields};
use super::Ctx;
use crate::progress::ActionVerb;

pub async fn run(
    ctx: Ctx,
    project: Option<String>,
    node_set: weft_compiler::codegen::NodeSet,
    running_policy: Option<String>,
    drain_timeout: Option<u64>,
) -> anyhow::Result<()> {
    let inner = ctx.clone();
    ctx.with_progress(ActionVerb::Bake, |progress| async move {
        // Parsed before anything is built: a misspelt policy, or a cap
        // with nothing to bound, is refused here, not after a build.
        let (running_policy, drain_timeout) =
            parse_running_choice(running_policy.as_deref(), drain_timeout)?;
        let (client, id) = match project {
            Some(id) => (inner.client(), id),
            None => {
                let handle = super::ensure::ensure_registered(&inner, &progress, node_set).await?;
                (handle.client, handle.id)
            }
        };
        let path = format!("/projects/{id}/bake");
        let body = serde_json::Value::Object(running_choice_fields(running_policy, drain_timeout));
        progress.drain_wait(&body, drain_timeout);
        progress.dispatcher_call_start(&path);
        let result: serde_json::Value = client.post_json(&path, &body).await?;
        progress.dispatcher_call_done(serde_json::json!({"project_id": id}));
        progress.complete_with("trigger settings saved", result);
        Ok(())
    }).await
}
