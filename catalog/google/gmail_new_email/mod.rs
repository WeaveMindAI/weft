//! GmailNewEmail: fires once per new message matching a Gmail search
//! query. A delta poll in seen-set mode (message ids are opaque and
//! newest-first, so a high-water mark cannot order them); activation
//! starts from the current mailbox state, history never replays.
//! Each fire fetches the full message and emits the same decomposed
//! fields GmailGet does, attachments included.

use async_trait::async_trait;
use serde_json::Value;

use weft::signal::{DeltaMode, PollDelta, PollEndpoint};
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::gmail::{read_message, API};

#[derive(NodeManifest)]
pub struct GmailNewEmailNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for GmailNewEmailNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let query: String = ctx.inputs.get_or("query", String::new())?;
        let interval: f64 = ctx.inputs.get("intervalSecs")?;

        let mut url = format!("{API}/messages?maxResults=25");
        if !query.trim().is_empty() {
            url.push_str(&format!("&q={}", urlencoding::encode(query.trim())));
        }
        ctx.register_signal(PollEndpoint {
            url,
            interval_secs: interval as u64,
            delta: Some(PollDelta {
                items: "messages".into(),
                cursor_field: Some("id".into()),
                mode: DeltaMode::Set,
                cursor_param: None,
            }),
            access: Some(weft::primitive::AccessRef::from(&account)),
            filters: Vec::new(),
        })
        .await
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let include_attachments: bool = ctx.inputs.get("includeAttachments")?;

        let wake = ctx.wake.record()?;
        let id = wake
            .pointer("/item/id")
            .and_then(Value::as_str)
            .node_err("the poll wake carries no message id")?
            .to_string();

        let http = ctx.client(&account).await?;
        let read = read_message(&ctx, &http, &id, include_attachments).await?;
        ctx.pulse_downstream(read.into_output().set("id", id)).await
    }
}
