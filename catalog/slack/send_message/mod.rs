//! SlackSendMessage: post (or schedule) a message on the connected
//! workspace's bot. One node covers the whole send surface: channel or
//! direct message, plain text and/or Block Kit blocks, a thread reply,
//! and a scheduled send. The compile-time scope check already proved
//! the wired access ticks `chat:write`; the store re-checks at
//! resolution (the drift backstop); Slack's own error is the last
//! resort.
//!
//! Destination: exactly one of `channel` / `user`. A user destination
//! opens the direct-message conversation first (`conversations.open`)
//! and posts into it, so the caller never handles DM channel ids.
//!
//! Content: `text`, `blocks`, or both (Slack uses `text` as the
//! notification fallback when `blocks` are present).
//!
//! Timing: `postAt` (epoch seconds) switches to
//! `chat.scheduleMessage`; a scheduled message has no permalink yet,
//! so the node emits `scheduledId` instead of `ts`/`permalink`.
//!
//! A post that succeeds always emits `ts`/`channel`: the permalink is
//! a best-effort second read whose failure is logged, never a node
//! failure (the message is already up; failing here would invite a
//! double-posting retry).

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::context::LogLevel;
use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

use super::api;

#[derive(NodeManifest)]
pub struct SlackSendMessageNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for SlackSendMessageNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let channel: Option<String> = ctx.inputs.opt("channel")?;
        let user: Option<String> = ctx.inputs.opt("user")?;
        let text: Option<String> = ctx.inputs.opt("text")?;
        let blocks: Option<Value> = ctx.inputs.opt("blocks")?;
        let thread_ts: Option<String> = ctx.inputs.opt("threadTs")?;
        // Read as an integer directly: a fractional schedule time is a
        // loud type error, never a silent truncation.
        let post_at: Option<i64> = ctx.inputs.opt("postAt")?;

        let destination = match (channel, user) {
            (Some(c), None) => c,
            (None, Some(u)) => {
                // A user destination is a DM: open (or reuse) the
                // conversation with them and post into its channel id.
                let opened =
                    api::call(&ctx, &access, "conversations.open", json!({ "users": u })).await?;
                api::required_str(&opened["channel"], "conversations.open", "id")?.to_string()
            }
            (Some(_), Some(_)) => {
                weft::node_bail!("pick ONE destination: a channel or a user, not both")
            }
            (None, None) => weft::node_bail!("pick a destination: a channel or a user"),
        };

        let mut payload = json!({ "channel": destination });
        api::set_content(&mut payload, text, blocks, "send")?;
        if let Some(t) = thread_ts {
            payload["thread_ts"] = json!(t);
        }

        if let Some(at) = post_at {
            payload["post_at"] = json!(at);
            let answer = api::call(&ctx, &access, "chat.scheduleMessage", payload).await?;
            let id = api::required_str(&answer, "chat.scheduleMessage", "scheduled_message_id")?;
            return ctx
                .pulse_downstream(NodeOutput::new().set("scheduledId", id.to_string()))
                .await;
        }

        let answer = api::call(&ctx, &access, "chat.postMessage", payload).await?;
        let ts = api::required_str(&answer, "chat.postMessage", "ts")?.to_string();
        let posted_channel = api::required_str(&answer, "chat.postMessage", "channel")?.to_string();
        // The permalink is a separate read; `chat.getPermalink` sits in
        // Slack's most generous rate tier (Tier 4), so fetching it on
        // every post never throttles ahead of the posting itself. The
        // message is already visible at this point, so a permalink
        // failure must never fail the node (a retry would post it
        // twice): log the error loudly and leave `permalink` un-emitted
        // (the engine closes it at termination; downstream reads
        // absence).
        let permalink = api::get(
            &ctx,
            &access,
            "chat.getPermalink",
            &[("channel", posted_channel.clone()), ("message_ts", ts.clone())],
        )
        .await
        .and_then(|link| {
            api::required_str(&link, "chat.getPermalink", "permalink").map(str::to_string)
        });

        let mut out = NodeOutput::new().set("ts", ts).set("channel", posted_channel);
        match permalink {
            Ok(link) => out = out.set("permalink", link),
            Err(e) => {
                ctx.log(
                    LogLevel::Error,
                    format!("the message posted, but reading its permalink failed: {e}"),
                )
                .await?;
            }
        }
        ctx.pulse_downstream(out).await
    }
}
