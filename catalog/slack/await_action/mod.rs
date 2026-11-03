//! SlackAwaitAction: the approval pattern. Post a message with
//! buttons, park the run, resume when someone clicks, say which
//! button and who.
//!
//! Flow:
//!   1. Mint a correlation id (kept in the node's state, so the body
//!      run again after the park reuses the same id).
//!   2. Post the message: a section with the text, an actions block
//!      with one button per entry, and message METADATA carrying the
//!      correlation id. Slack echoes the metadata inside the
//!      interaction payload, which is where the `interactions` topic's
//!      `callbackId` field reads it back out.
//!   3. Wait on the connection's `interactions` topic, predicated on
//!      that exact id: the run parks (the worker is released) until the
//!      click arrives through the service's interactivity delivery
//!      (webhook or the app's Socket Mode).
//!   4. On resume, the body runs from its top: the state holds the id
//!      and the posted message, so it posts nothing and comes straight
//!      back to the wait, which hands the click over. Then it replaces
//!      the original message through `chat.update` on the posted
//!      (channel, ts) the state holds (buttons must not stay clickable
//!      after the decision, and this handle, unlike the payload's
//!      `response_url`, is always present) and pulses who clicked what.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use weft::access::client::required_str;
use weft::signal::{Predicate, ProviderEvents};
use weft::{Access, RunContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::api;

/// One approval button, as the `buttons` port's type declares it.
#[derive(Deserialize)]
struct ActionButton {
    id: String,
    label: String,
    style: Option<String>,
}

/// Where the approval stands: the id minted, the message posted, the
/// click answered. Each filled field is a step the body skips when it
/// runs again after the park.
#[derive(Default, Serialize, Deserialize)]
struct Approval {
    callback: Option<String>,
    posted: Option<Posted>,
    click: Option<Value>,
}

/// The posted message, which the retire edits.
#[derive(Clone, Serialize, Deserialize)]
struct Posted {
    channel: String,
    ts: String,
}

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

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let channel: String = ctx.inputs.get("channel")?;
        let text: String = ctx.inputs.get("text")?;
        let buttons: Vec<ActionButton> = ctx.inputs.get("buttons")?;
        let confirmed_text: Option<String> = ctx.inputs.opt("confirmedText")?;

        let mut elements = Vec::new();
        for b in &buttons {
            let mut button = json!({
                "type": "button",
                "text": { "type": "plain_text", "text": b.label },
                "action_id": b.id,
                "value": b.id,
            });
            // Slack styles: "primary" (green) / "danger" (red); absent
            // = default grey. Passed through verbatim so Slack's own
            // error names an invalid style.
            if let Some(style) = &b.style {
                button["style"] = json!(style);
            }
            elements.push(button);
        }

        let mut state = ctx.keep(Approval::default()).await?;
        // Its own step, apart from the post: a post sent again after a
        // crash carries the same id, so a click on either message answers
        // the wait.
        let callback = state
            .once(|s| &mut s.callback, async |_| { Ok(uuid::Uuid::new_v4().to_string()) })
            .await?;

        let blocks = json!([
            { "type": "section", "text": { "type": "mrkdwn", "text": text } },
            { "type": "actions", "block_id": callback, "elements": elements },
        ]);
        // The connection opens inside the step, so a body run again after
        // the park opens nothing before the wait.
        let posted = state
            .once(|s| &mut s.posted, async |_| {
                let client = ctx.client(&access).await?;
                let posted = api::call(
                    &client,
                    "chat.postMessage",
                    json!({
                        "channel": channel,
                        "text": text,
                        "blocks": blocks,
                        "metadata": {
                            "event_type": "weft_await_action",
                            "event_payload": { "callback": callback }
                        }
                    }),
                )
                .await?;
                Ok(Posted {
                    channel: required_str(&posted, "chat.postMessage", "channel")?.to_string(),
                    ts: required_str(&posted, "chat.postMessage", "ts")?.to_string(),
                })
            })
            .await?;

        let click = state
            .wait(
                |s| &mut s.click,
                ProviderEvents::new(&access, "interactions", vec![Predicate::eq("callbackId", callback)]),
            )
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
        // Nothing after it can pause, so only a crash before the
        // decision is sent runs it again, writing the same text.
        let label = buttons
            .iter()
            .find(|b| b.id == action)
            .map_or(action.as_str(), |b| b.label.as_str());
        let final_text = confirmed_text
            .clone()
            .unwrap_or_else(|| format!("{text}\n> *{label}* chosen by <@{user}>"));
        // Opened after the park, not before it: the click may come days
        // later, and this call needs a fresh sign-in then.
        let retire: WeftResult<Value> = async {
            let client = ctx.client(&access).await?;
            api::call(
                &client,
                "chat.update",
                json!({
                    "channel": posted.channel,
                    "ts": posted.ts,
                    "text": final_text,
                    "blocks": [
                        { "type": "section",
                          "text": { "type": "mrkdwn", "text": final_text } }
                    ],
                }),
            )
            .await
        }
        .await;
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
                .set("ts", posted.ts),
        )
        .await
    }
}

