//! GmailNewEmail: fires once per new message matching a Gmail search
//! query. A delta poll in seen-set mode (message ids are opaque and
//! newest-first, so a high-water mark cannot order them); activation
//! starts from the current mailbox state, history never replays.
//! Each fire fetches the full message and emits the same decomposed
//! fields GmailGet does, attachments included.

use async_trait::async_trait;

use weft::access::client::url;
use weft::signal::{DeltaMode, PollDelta, PollEndpoint, PollPaging};
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

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

    async fn setup_trigger(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let query: Option<String> = ctx.inputs.opt("query")?;
        let interval: u64 = ctx.inputs.get("intervalSecs")?;

        // Gmail's largest page, so a burst takes as few pages as it can;
        // the poll follows `nextPageToken` until it meets a message it
        // has already seen.
        let url = url(API, &["messages"])?
            .query("maxResults", 500)
            .query_opt("q", query.as_deref().map(str::trim));
        ctx.register_signal(PollEndpoint {
            url: url.into(),
            interval_secs: interval,
            delta: Some(PollDelta {
                items: "messages".into(),
                cursor_field: Some("id".into()),
                mode: DeltaMode::Set,
                cursor_param: None,
                paging: Some(PollPaging::query("nextPageToken", "pageToken")),
            }),
            access: Some(weft::primitive::AccessRef::from(&account)),
            ..Default::default()
        })
        .await
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let include_attachments: bool = ctx.inputs.get("includeAttachments")?;
        let max_body_chars: usize = ctx.inputs.get("maxBodyChars")?;
        let id: String = ctx.wake.nested("item")?.get("id")?;

        let http = ctx.client(&account).await?;
        let read = read_message(&ctx, &http, &id, include_attachments, max_body_chars).await?;
        ctx.pulse_downstream(read.into_output().set("id", id)).await
    }
}
