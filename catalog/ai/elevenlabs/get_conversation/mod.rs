//! ElevenLabsGetConversation: read an agent conversation: waits for
//! it to finish by default, parked without holding a worker (a call
//! takes as long as it takes; no deadline, cancelling the run
//! cancels the wait), then emits the transcript as a stored JSON file,
//! the analysis (the summary, the outcome, the data collection and
//! criteria results), the call's length and cost, and optionally the
//! whole conversation and the OpenTelemetry trace (JSON files too) and
//! the call audio.

use async_trait::async_trait;
use serde_json::{json, Map, Value};

use weft::access::client::get_json;
use weft::node::NodeOutput;
use weft::signal::{PollEndpoint, Predicate};
use weft::storage::{StorageScope};
use weft::{Access, RunContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::elevenlabs::{store_json, API};

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

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let conversation: String = ctx.inputs.get("conversationId")?;
        let wait: bool = ctx.inputs.get("waitUntilDone")?;
        let include_audio: bool = ctx.inputs.get("includeAudio")?;

        let http = ctx.client(&account).await?;
        // The id is typed text that becomes a path segment: encoded, it
        // can only ever name a conversation.
        let url = format!("{API}/convai/conversations/{}", urlencoding::encode(&conversation));
        // Waiting parks on the conversation and resumes on any status
        // that is not in flight (still ringing, talking, or
        // post-processing), so a status this node does not know ends the
        // wait and is refused below instead of being waited on forever.
        let answer = if wait {
            ctx.await_signal(PollEndpoint {
                url: url.clone(),
                interval_secs: 5,
                access: Some(weft::primitive::AccessRef::from(&account)),
                filters: vec![
                    Predicate::neq("status", "initiated"),
                    Predicate::neq("status", "in-progress"),
                    Predicate::neq("status", "processing"),
                ],
                ..Default::default()
            })
            .await?
        } else {
            get_json(&http, &url, "read the ElevenLabs conversation").await?
        };
        let status = answer["status"]
            .as_str()
            .node_err("ElevenLabs answered the conversation with no status")?
            .to_string();
        match status.as_str() {
            "done" | "failed" => {}
            "initiated" | "in-progress" | "processing" if !wait => {}
            other => weft::node_bail!(
                "elevenlabs answered an unexpected status '{other}' for conversation \
                 {conversation}"
            ),
        }

        let turns: Vec<Value> = answer["transcript"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|t| json!({ "role": t["role"], "message": t["message"] }))
            .collect();
        let text = turns
            .iter()
            .filter_map(|t| {
                let m = t["message"].as_str()?;
                Some(format!("{}: {m}", t["role"].as_str().unwrap_or("?")))
            })
            .collect::<Vec<_>>()
            .join("\n");
        // The transcript grows with the call, past the 100 KB a wire
        // carries, so it always travels as a file.
        let transcript = store_json(
            &ctx,
            &json!({ "text": text, "turns": turns }),
            &format!("conversation_{conversation}_transcript.json"),
        )
        .await?;

        let mut out = NodeOutput::new().set("status", status).set("transcript", transcript);
        // The analysis only exists once elevenlabs has processed the
        // finished call; until then its ports stay unset.
        let analysis = &answer["analysis"];
        if let Some(summary) = analysis["transcript_summary"].as_str() {
            out = out.set("summary", summary);
        }
        if let Some(title) = analysis["call_summary_title"].as_str() {
            out = out.set("summaryTitle", title);
        }
        if let Some(successful) = analysis["call_successful"].as_str() {
            out = out.set("successful", successful);
        }
        if let Some(score) = number(&analysis["call_success_score"]) {
            out = out.set("successScore", score);
        }
        if let Some(results) = analysis["data_collection_results"].as_object() {
            out = out.set("dataCollection", by_id(results, "value"));
        }
        if let Some(results) = analysis["evaluation_criteria_results"].as_object() {
            out = out.set("evaluation", by_id(results, "result"));
        }
        let metadata = &answer["metadata"];
        if let Some(duration) = number(&metadata["call_duration_secs"]) {
            out = out.set("durationSecs", duration);
        }
        if let Some(reason) = metadata["termination_reason"].as_str().filter(|r| !r.is_empty()) {
            out = out.set("terminationReason", reason);
        }
        if let Some(cost) = number(&metadata["cost_fiat"]) {
            out = out.set("costUsd", cost);
        }
        // The whole object holds everything the ports above carry and
        // more, so it is stored only for a program that reads it.
        if ctx.is_output_wired("conversation") {
            let stored = store_json(&ctx, &answer, &format!("conversation_{conversation}.json")).await?;
            out = out.set("conversation", stored);
        }

        // The trace is asked for on its own read, after the wait, so the
        // looks while the call runs never carry it.
        if ctx.is_output_wired("otlpTrace") {
            let traced = get_json(&http, &format!("{url}?format=opentelemetry"), "read the ElevenLabs conversation's trace")
                .await?;
            let trace = traced
                .get("otlp_traces")
                .filter(|trace| trace.is_object())
                .node_err(format!("elevenlabs answered no otlp_traces for conversation {conversation}"))?;
            let stored = store_json(&ctx, trace, &format!("conversation_{conversation}_otlp.json")).await?;
            out = out.set("otlpTrace", stored);
        }
        if include_audio {
            let resp = http
                .get(format!("{url}/audio"))
                .send()
                .await
                .node_err("download the call audio from ElevenLabs")?;
            let stored = ctx
                .storage(StorageScope::Run)
                .put_response(
                    resp,
                    "download the call audio from ElevenLabs",
                    None,
                    &format!("call_{conversation}.mp3"),
                    None,
                )
                .await?;
            out = out.set("audio", stored.to_value());
        }
        ctx.pulse_downstream(out).await
    }
}

/// The number ElevenLabs answered, as it answered it, or `None` when it
/// answered none (a null, or nothing).
fn number(value: &Value) -> Option<Value> {
    value.is_number().then(|| value.clone())
}

/// An analysis results object (id to result) as id to the one field a
/// program reads off each result: `value` for a data collection item,
/// `result` for a criterion.
fn by_id(results: &Map<String, Value>, field: &str) -> Value {
    results.iter().map(|(id, result)| (id.clone(), result[field].clone())).collect::<Map<String, Value>>().into()
}
