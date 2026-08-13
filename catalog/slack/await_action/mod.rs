//! SlackAwaitAction: the approval pattern. Post a message with
//! buttons, park the execution, resume when someone clicks, say which
//! button and who.
//!
//! Flow:
//!   1. Mint a correlation id (journaled via `ctx.run`, so replays
//!      reuse the same id).
//!   2. Post the message: a section with the text, an actions block
//!      with one button per entry, and message METADATA carrying the
//!      correlation id. Slack echoes the metadata inside the
//!      interaction payload, which is where the `interactions` topic's
//!      `callbackId` field reads it back out.
//!   3. `await_signal` on the connection's `interactions` topic,
//!      predicated on that exact id: the execution parks (the worker
//!      is released) until the click arrives through the service's
//!      interactivity delivery (webhook or the app's Socket Mode).
//!   4. On resume, replace the original message through `chat.update`
//!      on the posted (channel, ts) the node already holds (buttons
//!      must not stay clickable after the decision, and this handle,
//!      unlike the payload's `response_url`, is always present) and
//!      pulse who clicked what.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::signal::{Predicate, ProviderEvents};
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::api;

#[derive(NodeManifest)]
pub struct SlackAwaitActionNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for SlackAwaitActionNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let channel: String = ctx.inputs.get("channel")?;
        let text: String = ctx.inputs.get("text")?;
        let buttons: Value = ctx.inputs.get("buttons")?;
        let confirmed_text: Option<String> = ctx.inputs.opt("confirmedText")?;

        let entries = buttons
            .as_array()
            .filter(|b| !b.is_empty())
            .node_err("buttons must be a non-empty list of { id, label } objects")?;
        let mut elements = Vec::new();
        for b in entries {
            let id = b["id"].as_str().node_err("every button needs an id")?;
            let label = b["label"].as_str().node_err("every button needs a label")?;
            let mut button = json!({
                "type": "button",
                "text": { "type": "plain_text", "text": label },
                "action_id": id,
                "value": id,
            });
            // Slack styles: "primary" (green) / "danger" (red); absent
            // = default grey. Passed through verbatim so Slack's own
            // error names an invalid style.
            if let Some(style) = b.get("style").and_then(Value::as_str) {
                button["style"] = json!(style);
            }
            elements.push(button);
        }

        let callback = ctx
            .run("mint_callback", || async {
                Ok(json!(uuid::Uuid::new_v4().to_string()))
            })
            .await?;
        let callback = callback
            .as_str()
            .node_err("the journaled callback id is not a string")?
            .to_string();

        let blocks = json!([
            { "type": "section", "text": { "type": "mrkdwn", "text": text } },
            { "type": "actions", "block_id": callback, "elements": elements },
        ]);
        // `ctx.run` journals the post: a replay after the park must
        // not post the message again.
        let cb = callback.clone();
        let post_text = text.clone();
        let posted = {
            let ctx = &ctx;
            let access = &access;
            ctx.run("post_message", move || async move {
                api::call(
                    ctx,
                    access,
                    "chat.postMessage",
                    json!({
                        "channel": channel,
                        "text": post_text,
                        "blocks": blocks,
                        "metadata": {
                            "event_type": "weft_await_action",
                            "event_payload": { "callback": cb }
                        }
                    }),
                )
                .await
            })
            .await?
        };
        let posted_ts = api::required_str(&posted, "chat.postMessage", "ts")?.to_string();
        let posted_channel = api::required_str(&posted, "chat.postMessage", "channel")?.to_string();

        let click = ctx
            .await_signal(ProviderEvents::new(
                &access,
                "interactions",
                vec![Predicate::eq("callbackId", callback)],
            ))
            .await?;

        // The decision fields are load-bearing: an approval workflow
        // must never branch on a silently-empty action or attribute a
        // decision to nobody. The display name genuinely may be absent.
        let action = click["action"]
            .as_str()
            .node_err("the interaction payload carries no action")?
            .to_string();
        let user = click["user"]
            .as_str()
            .node_err("the interaction payload carries no user")?
            .to_string();
        let user_name = click["userName"].as_str().unwrap_or_default().to_string();

        // Retire the buttons: a decided approval must not stay
        // clickable. `chat.update` on the posted (channel, ts) uses
        // the connection the node already holds, so the retire step
        // never depends on a payload field Slack may omit. The HUMAN
        // DECISION above is the node's load-bearing output, so a
        // failed retire (bot removed from the channel, message
        // deleted) logs loudly and still delivers the decision: a
        // stale button is recoverable, a discarded approval is not.
        // Journaled via ctx.run so a replay never re-updates.
        let label = entries
            .iter()
            .find(|b| b["id"].as_str() == Some(action.as_str()))
            .and_then(|b| b["label"].as_str())
            .unwrap_or(action.as_str());
        let final_text = confirmed_text
            .clone()
            .unwrap_or_else(|| format!("{text}\n> *{label}* chosen by <@{user}>"));
        let retire = {
            let ctx = &ctx;
            let access = &access;
            let posted_channel = &posted_channel;
            let posted_ts = &posted_ts;
            ctx.run("retire_buttons", move || async move {
                api::call(
                    ctx,
                    access,
                    "chat.update",
                    json!({
                        "channel": posted_channel,
                        "ts": posted_ts,
                        "text": final_text,
                        "blocks": [
                            { "type": "section",
                              "text": { "type": "mrkdwn", "text": final_text } }
                        ],
                    }),
                )
                .await
            })
            .await
        };
        if let Err(e) = retire {
            // Best-effort log: even a failing log channel must not
            // stop the decision below from delivering (losing a
            // recorded human approval to telemetry would be the exact
            // failure the non-fatal retire exists to prevent).
            let _ = ctx
                .log(
                    weft::context::LogLevel::Error,
                    format!(
                        "could not retire the approval buttons (the decision below \
                         still delivered): {e}"
                    ),
                )
                .await;
        }

        ctx.pulse_downstream(
            weft::node::NodeOutput::new()
                .set("action", action)
                .set("user", user)
                .set("userName", user_name)
                .set("ts", posted_ts),
        )
        .await
    }
}
