//! ElevenLabsDub: dub a recording into another language (translated,
//! re-voiced to match the original speakers). Long-running: submit
//! the dubbing job, park on its status without holding a worker (a dub
//! the user asked for takes as long as it takes, no deadline;
//! cancelling the execution cancels the wait), then download and store
//! the dubbed audio.

use async_trait::async_trait;
use serde_json::json;

use weft::access::client::{json_call, Multipart};
use weft::node::NodeOutput;
use weft::signal::{PollEndpoint, Predicate};
use weft::storage::{FileHandle, KeepTtl, StorageScope};
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::elevenlabs::API;

#[derive(NodeManifest)]
pub struct ElevenLabsDubNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ElevenLabsDubNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let file: FileHandle = ctx.inputs.get("file")?;
        let target_lang: String = ctx.inputs.get("targetLang")?;
        let source_lang: Option<String> = ctx.inputs.opt("sourceLang")?;
        let num_speakers: Option<f64> = ctx.inputs.opt("numSpeakers")?;
        let drop_background: bool = ctx.inputs.get("dropBackgroundAudio")?;

        let http = ctx.client(&account).await?;
        // Starting the dub is the paid call, and the body replays from
        // the top when the wait resumes: journaled, the replay reads the
        // dubbing id back instead of starting a second dub (and never
        // reads the recording again).
        let started = ctx
            .run("elevenlabs_start_dub", || async {
                let (meta, bytes) =
                    ctx.storage(StorageScope::Execution).get_bytes(&file).await?;
                let mut form = Multipart::form_data()
                    .text("target_lang", &target_lang)
                    .file("file", &meta.filename, &meta.mime_type, bytes);
                if let Some(lang) = &source_lang {
                    form = form.text("source_lang", lang);
                }
                if let Some(n) = num_speakers {
                    form = form.text("num_speakers", &(n as u64).to_string());
                }
                if drop_background {
                    form = form.text("drop_background_audio", "true");
                }
                let (content_type, body) = form.build();
                let answer = json_call(
                    http.post(format!("{API}/dubbing"))
                        .header("content-type", content_type)
                        .body(body),
                    "elevenlabs: start the dub",
                )
                .await?;
                let job = answer["dubbing_id"]
                    .as_str()
                    .node_err("elevenlabs: the dub start carries no dubbing_id")?;
                Ok(json!({ "job": job, "filename": meta.filename }))
            })
            .await?;
        let job = started["job"]
            .as_str()
            .node_err("the journaled dub start has no job")?
            .to_string();
        let filename =
            started["filename"].as_str().node_err("the journaled dub start has no filename")?;

        // Resume on any status that is not in flight, so a status this
        // node does not know ends the wait and is refused below instead
        // of being waited on forever.
        let status = ctx
            .await_signal(PollEndpoint {
                url: format!("{API}/dubbing/{job}"),
                interval_secs: 5,
                access: Some(weft::primitive::AccessRef::from(&account)),
                filters: vec![
                    Predicate::neq("status", "preparing"),
                    Predicate::neq("status", "dubbing"),
                ],
                ..Default::default()
            })
            .await?;
        match status["status"].as_str().unwrap_or_default() {
            "dubbed" => {}
            "failed" => weft::node_bail!(
                "the dub failed: {}",
                status["error"].as_str().unwrap_or("no detail")
            ),
            other => weft::node_bail!(
                "elevenlabs answered an unexpected status '{other}' for dub {job}"
            ),
        }

        let resp = http
            .get(format!("{API}/dubbing/{job}/audio/{target_lang}"))
            .send()
            .await
            .node_err("elevenlabs: download the dubbed audio")?;
        let stored = ctx
            .storage(StorageScope::Execution)
            .put_response(
                resp,
                "elevenlabs: download the dubbed audio",
                None,
                &format!("dubbed_{target_lang}_{filename}"),
                // The dubbed audio is the run's product: keep it past
                // the run (default 30-day access-bumped TTL).
                Some(KeepTtl::Default),
            )
            .await?;
        // The dub PROJECT stays on the account (re-downloadable,
        // editable in the studio); its id makes it addressable.
        ctx.pulse_downstream(NodeOutput::stored_file(stored).set("dubbingId", job)).await
    }
}
