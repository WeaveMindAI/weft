//! ElevenLabsGetConversation: read an agent conversation: waits for
//! it to finish by default, parked without holding a worker (a call
//! takes as long as it takes; no deadline, cancelling the run
//! cancels the wait), then emits the transcript as a stored JSON file,
//! the analysis (the summary, the outcome, the data collection and
//! criteria results), the call's length and cost, and optionally the
//! whole conversation and the OpenTelemetry trace (JSON files too) and
//! the call audio.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use weft::access::client::{checked_send, get_json, url};
use weft::node::NodeOutput;
use weft::signal::{PollEndpoint, Predicate};
use weft::storage::{StorageScope};
use weft::{Access, RunContext, Node, NodeErrExt, NodeManifest, WeftResult};

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

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let conversation: String = ctx.inputs.get("conversationId")?;
        let wait: bool = ctx.inputs.get("waitUntilDone")?;
        let include_audio: bool = ctx.inputs.get("includeAudio")?;

        let http = ctx.client(&account).await?;
        let address = url(API, &["convai", "conversations", &conversation])?;
        // Waiting parks on the conversation and resumes on any status
        // that is not in flight (still ringing, talking, or
        // post-processing), so a status this node does not know ends the
        // wait and is refused below instead of being waited on forever.
        // The wait carries only the status, which the state keeps, so a
        // run resumed after it waits no more; the conversation itself
        // (its transcript can run past 100 KB) is read after the wait,
        // and a finished conversation reads the same however often.
        let mut state = ctx.keep(Waited::default()).await?;
        let answer = if wait {
            let poll = PollEndpoint {
                url: address.to_string(),
                interval_secs: 5,
                access: Some(weft::primitive::AccessRef::from(&account)),
                filters: vec![
                    Predicate::neq("status", "initiated"),
                    Predicate::neq("status", "in-progress"),
                    Predicate::neq("status", "processing"),
                ],
                carry: vec!["status".into()],
                ..Default::default()
            };
            state.wait(|s| &mut s.ended, poll).await?;
            get_json(&http, address.as_str(), "read the finished ElevenLabs conversation").await?
        } else {
            get_json(&http, address.as_str(), "read the ElevenLabs conversation").await?
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
        let storage = ctx.storage(StorageScope::Run);
        let transcript = storage
            .put_json(&json!({ "text": text, "turns": turns }), &format!("conversation_{conversation}_transcript.json"), None)
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
            let stored = storage.put_json(&answer, &format!("conversation_{conversation}.json"), None).await?;
            out = out.set("conversation", stored);
        }

        // The trace is asked for on its own read, after the wait, so the
        // looks while the call runs never carry it.
        if ctx.is_output_wired("otlpTrace") {
            let traced = get_json(
                &http,
                address.clone().query("format", "opentelemetry").as_str(),
                "read the ElevenLabs conversation's trace",
            )
            .await?;
            let trace = traced
                .get("otlp_traces")
                .filter(|trace| trace.is_object())
                .node_err(format!("elevenlabs answered no otlp_traces for conversation {conversation}"))?;
            let stored = storage.put_json(trace, &format!("conversation_{conversation}_otlp.json"), None).await?;
            out = out.set("otlpTrace", stored);
        }
        if include_audio {
            let what = "download the call audio from ElevenLabs";
            let resp = checked_send(http.get(address.segment("audio")?.as_str()), what).await?;
            let stored = storage.put_response(resp, what, None, &format!("call_{conversation}.mp3"), None).await?;
            out = out.set("audio", stored.to_value());
        }
        ctx.pulse_downstream(out).await
    }
}

/// What the node keeps: the status its wait ended on, when it waits.
#[derive(Default, Serialize, Deserialize)]
struct Waited {
    ended: Option<Value>,
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
