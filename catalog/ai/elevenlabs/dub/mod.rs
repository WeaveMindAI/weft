//! ElevenLabsDub: dub a recording into another language (translated,
//! re-voiced to match the original speakers). Long-running: submit
//! the dubbing job, wait on its status (a dub the user asked for
//! takes as long as it takes, no deadline; cancelling the execution
//! cancels the wait), then download and store the dubbed audio.

use std::time::Duration;

use async_trait::async_trait;

use weft::access::client::{get_json, json_call, Multipart};
use weft::context::LogLevel;
use weft::node::NodeOutput;
use weft::storage::{FileHandle, StorageScope};
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

        let (meta, bytes) = ctx.storage(StorageScope::Execution).get_bytes(&file).await?;
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

        let http = ctx.client(&account).await?;
        let started = json_call(
            http.post(format!("{API}/dubbing"))
                .header("content-type", content_type)
                .body(body),
            "elevenlabs: start the dub",
        )
        .await?;
        let job = started["dubbing_id"]
            .as_str()
            .node_err("elevenlabs: the dub start carries no dubbing_id")?
            .to_string();

        // Wait on the job. A breadcrumb every ~12 polls keeps a long
        // dub legible in the logs.
        let mut polls: u64 = 0;
        loop {
            let status = get_json(
                &http,
                &format!("{API}/dubbing/{job}"),
                "elevenlabs: read the dub status",
            )
            .await?;
            match status["status"].as_str().unwrap_or_default() {
                "dubbed" => break,
                "failed" => weft::node_bail!(
                    "the dub failed: {}",
                    status["error"].as_str().unwrap_or("no detail")
                ),
                "preparing" | "dubbing" => {
                    polls += 1;
                    if polls % 12 == 0 {
                        ctx.log(LogLevel::Info, format!("dub {job} still running")).await?;
                    }
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                other => weft::node_bail!(
                    "elevenlabs answered an unexpected status '{other}' for dub {job}"
                ),
            }
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
                &format!("dubbed_{target_lang}_{}", meta.filename),
                None,
            )
            .await?;
        // The dub PROJECT stays on the account (re-downloadable,
        // editable in the studio); its id makes it addressable.
        ctx.pulse_downstream(NodeOutput::stored_file(stored).set("dubbingId", job)).await
    }
}
