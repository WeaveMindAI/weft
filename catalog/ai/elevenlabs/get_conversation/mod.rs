//! ElevenLabsGetConversation: read an agent conversation: waits for
//! it to finish by default (a call takes as long as it takes; no
//! deadline, cancelling the execution cancels the wait), then emits
//! the transcript, the analysis, and optionally the call audio.

use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::access::client::get_json;
use weft::context::LogLevel;
use weft::node::NodeOutput;
use weft::storage::{KeepTtl, StorageScope};
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::elevenlabs::API;

#[derive(NodeManifest)]
pub struct ElevenLabsGetConversationNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ElevenLabsGetConversationNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let conversation: String = ctx.inputs.get("conversationId")?;
        let wait: bool = ctx.inputs.get("waitUntilDone")?;
        let include_audio: bool = ctx.inputs.get("includeAudio")?;

        let http = ctx.client(&account).await?;
        let url = format!("{API}/convai/conversations/{conversation}");
        let mut polls: u64 = 0;
        let (status, answer): (String, Value) = loop {
            let answer =
                get_json(&http, &url, "elevenlabs: read the conversation").await?;
            let status = answer["status"]
                .as_str()
                .node_err("elevenlabs: the conversation answered no status")?
                .to_string();
            match status.as_str() {
                "done" | "failed" => break (status, answer),
                "initiated" | "in-progress" | "processing" if !wait => break (status, answer),
                // Still ringing, talking, or post-processing. A
                // breadcrumb every ~12 polls keeps a long call legible.
                "initiated" | "in-progress" | "processing" => {
                    polls += 1;
                    if polls % 12 == 0 {
                        ctx.log(
                            LogLevel::Info,
                            format!("conversation {conversation} still {status}"),
                        )
                        .await?;
                    }
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                other => weft::node_bail!(
                    "elevenlabs answered an unexpected status '{other}' for conversation \
                     {conversation}"
                ),
            }
        };

        let transcript: Vec<Value> = answer["transcript"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|t| json!({ "role": t["role"], "message": t["message"] }))
            .collect();
        let text = transcript
            .iter()
            .filter_map(|t| {
                let m = t["message"].as_str()?;
                Some(format!("{}: {m}", t["role"].as_str().unwrap_or("?")))
            })
            .collect::<Vec<_>>()
            .join("\n");

        let mut out = NodeOutput::new()
            .set("status", status)
            .set("transcript", json!(transcript))
            .set("text", text)
            .set("conversation", answer.clone());
        // The analysis only exists once elevenlabs has processed the
        // finished call; until then these ports stay unset.
        if let Some(summary) = answer["analysis"]["transcript_summary"].as_str() {
            out = out.set("summary", summary);
        }
        if let Some(successful) = answer["analysis"]["call_successful"].as_str() {
            out = out.set("successful", successful);
        }
        if include_audio {
            let resp = http
                .get(format!("{url}/audio"))
                .send()
                .await
                .node_err("elevenlabs: download the call audio")?;
            let stored = ctx
                .storage(StorageScope::Execution)
                .put_response(
                    resp,
                    "elevenlabs: download the call audio",
                    None,
                    &format!("call_{conversation}.mp3"),
                    // The recording is the run's product: keep it past
                    // the run (default 30-day access-bumped TTL).
                    Some(KeepTtl::Default),
                )
                .await?;
            out = out.set("audio", stored.to_value());
        }
        ctx.pulse_downstream(out).await
    }
}
