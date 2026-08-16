//! TelegramReceiveMessage: fires once per message sent to the bot.
//! Registers a delta poll on `getUpdates` with the update_id
//! high-water mark (update ids are increasing integers, so the
//! max-cursor mode is exact) and the acknowledged-cursor `offset`
//! parameter: Telegram only discards a queued update once a later
//! `offset` is sent, so each poll confirms the previous batch and the
//! queue drains instead of pinning at its oldest 100 entries.
//! Activation starts from now: the priming poll sends `offset=-1`
//! (Telegram's drain idiom: answer only the newest queued update,
//! discard the rest), so a deep pre-activation backlog never replays;
//! restarts resume from the durable cursor and never re-fire history.

use async_trait::async_trait;
use serde_json::Value;

use weft::node::NodeOutput;
use weft::signal::{PollDelta, PollEndpoint, Predicate};
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct TelegramReceiveMessageNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for TelegramReceiveMessageNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let chat_id: Option<String> = ctx.inputs.opt("chatId")?;
        let keyword: Option<String> = ctx.inputs.opt("keyword")?;
        let interval: f64 = ctx.inputs.get("intervalSecs")?;

        // The fire payload is `{ item: <update> }`; predicates walk
        // into the update with dotted paths.
        let mut filters = vec![Predicate::exists("item.message")];
        if let Some(c) = chat_id.filter(|c| !c.trim().is_empty()) {
            filters.push(Predicate::eq("item.message.chat.id", c));
        }
        if let Some(k) = keyword.filter(|k| !k.trim().is_empty()) {
            filters.push(Predicate::contains("item.message.text", k));
        }

        // The URL is generic; the connection's path-prefix step aims
        // it at /bot<token>/getUpdates.
        ctx.register_signal(PollEndpoint {
            url: "https://api.telegram.org/getUpdates?allowed_updates=[\"message\"]&limit=100"
                .into(),
            interval_secs: interval as u64,
            delta: Some(PollDelta {
                items: "result".into(),
                cursor_field: Some("update_id".into()),
                mode: Default::default(),
                // Telegram wants `last_update_id + 1` as the offset;
                // priming sends -1 (drain: only the newest update).
                cursor_param: Some(weft::signal::CursorParam {
                    name: "offset".into(),
                    offset: 1,
                    prime: Some(-1),
                }),
            }),
            access: Some(weft::primitive::AccessRef::from(&account)),
            filters,
            ..Default::default()
        })
        .await
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let wake = ctx.wake.record()?;
        let message = &wake["item"]["message"];
        let text = message["text"].as_str().unwrap_or_default().to_string();
        let chat_id = message["chat"]["id"].as_i64().node_err("the update carries no chat id")?;
        let user = message
            .pointer("/from/username")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let message_id =
            message["message_id"].as_i64().node_err("the update carries no message_id")?;
        ctx.pulse_downstream(
            NodeOutput::new()
                .set("text", text)
                .set("chatId", chat_id.to_string())
                .set("user", user)
                .set("messageId", message_id)
                .set("message", message.clone()),
        )
        .await
    }
}
