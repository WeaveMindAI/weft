//! ElevenLabsVideoFromReferences: a prompt and reference material
//! (subjects, styles, clips, sounds) to one video through the Image &
//! Video API.

use async_trait::async_trait;
use serde_json::json;

use weft::node::NodeOutput;
use weft::storage::FileHandle;
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use super::flows::{generate, is_veo, video_request, Job, Media, References};

#[derive(NodeManifest)]
pub struct ElevenLabsVideoFromReferencesNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ElevenLabsVideoFromReferencesNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let model: String = ctx.inputs.get("model")?;
        let subjects: Vec<FileHandle> = ctx.inputs.list("subjects")?;
        let styles: Vec<FileHandle> = ctx.inputs.list("styles")?;
        let videos: Vec<FileHandle> = ctx.inputs.list("videos")?;
        let audios: Vec<FileHandle> = ctx.inputs.list("audios")?;
        // Only Veo gives a reference image a role; `styles` on any other
        // model is refused by the metadata rule before the body runs.
        let veo = is_veo(&model);
        let mut body = video_request(&ctx)?;

        let http = ctx.client(&account).await?;
        let refs = References::new(&ctx, &http, &account);
        let mut job = ctx.keep(Job::default()).await?;
        let video = generate(&ctx, &mut job, &account, &http, Media::Video, || async {
            let images = if veo {
                let mut images = Vec::with_capacity(subjects.len() + styles.len());
                for (files, role) in [(&subjects, "subject"), (&styles, "style")] {
                    for image in refs.of_each(files).await? {
                        images.push(json!({ "image": image, "role": role }));
                    }
                }
                images
            } else {
                refs.of_each(&subjects).await?
            };
            if !images.is_empty() {
                body["images"] = json!(images);
            }
            // Veo takes no clips or sounds: it refuses them itself with a
            // 422 naming the field, before anything is charged.
            if !videos.is_empty() {
                body["videos"] = json!(refs.of_each(&videos).await?);
            }
            if !audios.is_empty() {
                body["audios"] = json!(refs.of_each(&audios).await?);
            }
            Ok(body)
        })
        .await?;
        ctx.pulse_downstream(NodeOutput::new().set("video", video)).await
    }
}
