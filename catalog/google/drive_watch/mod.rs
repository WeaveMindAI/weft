//! GoogleDriveWatch: fires when a watched Drive file or folder
//! changes.
//!
//!   - `setup_trigger`: register one event subscription on the
//!     `drive_changes` topic, carrying the target file id as the
//!     subscribe call's parameter and the change-kind filter as
//!     predicates over the topic's named fields. The language owns
//!     the watch channel (minting, renewal before expiry, stopping it
//!     at deactivate); this node never sees any of it.
//!
//!   - `run`: the push only says "this file changed"; ask Drive what
//!     it looks like NOW and fan the answer.

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde_json::Value;

use weft::signal::{Predicate, PredicateOp, ProviderEvents};
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct GoogleDriveWatchNode;

#[async_trait]
impl Node for GoogleDriveWatchNode {
    async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let target: String = ctx.inputs.get("target")?;
        let changes: String = ctx.inputs.get("changes")?;

        let mut filters = vec![
            // The provider's first push after subscribing is a plain
            // "the channel is live" confirmation, not a change.
            Predicate {
                field: "state".into(),
                op: PredicateOp::Neq,
                value: Some("sync".into()),
            },
        ];
        // The `changed` header carries a comma list of what changed
        // (content, properties, children, ...); narrow with a
        // contains filter when the user asked for one kind.
        if changes != "any" {
            filters.push(Predicate {
                field: "changed".into(),
                op: PredicateOp::Contains,
                value: Some(changes),
            });
        }

        ctx.register_signal(
            ProviderEvents::new(&account, "drive_changes", filters)
                .with_params(BTreeMap::from([("target".to_string(), target)])),
        )
        .await
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // The push carries only headers (state, what changed, the
        // resource id); the file's current shape is a follow-up read.
        let data = Value::Object(ctx.wake.object()?.clone());
        let mut out = ctx.fan_declared(&data);

        let target: String = ctx.inputs.get("target")?;
        let account: Access = ctx.inputs.get("account")?;
        let http = ctx.client(&account).await?;
        let file = weft::access::client::get_json(
            &http,
            &format!(
                "https://www.googleapis.com/drive/v3/files/{target}?fields=id,name,mimeType,modifiedTime,lastModifyingUser(displayName)&supportsAllDrives=true"
            ),
            "read the changed file",
        )
        .await?;
        let text = |v: Option<&Value>| v.and_then(Value::as_str).unwrap_or_default().to_string();
        out = out
            .set("fileId", text(file.get("id")))
            .set("name", text(file.get("name")))
            .set("mimeType", text(file.get("mimeType")))
            .set("modifiedTime", text(file.get("modifiedTime")))
            .set(
                "modifiedBy",
                text(file.get("lastModifyingUser").and_then(|u| u.get("displayName"))),
            );
        ctx.pulse_downstream(out).await
    }
}
