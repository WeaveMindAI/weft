//! FalEditImage: change an existing image with a text instruction
//! (edit, restyle, inpaint with an optional mask) through fal's
//! queue. The knobs target the blessed edit model; `params` carries
//! model-specific extras.

use async_trait::async_trait;
use serde_json::{json, Map, Value};

use weft::node::NodeOutput;
use weft::storage::{FileHandle, StorageScope};
use weft::{Access, RunContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::fal::{image_url, merge_params, run_queued, Queued};

#[derive(NodeManifest)]
pub struct FalEditImageNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for FalEditImageNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let model: String = ctx.inputs.get("model")?;
        let prompt: String = ctx.inputs.get("prompt")?;
        let image: FileHandle = ctx.inputs.get("image")?;
        let mask: Option<FileHandle> = ctx.inputs.opt("mask")?;
        let params = ctx.inputs.opt::<Map<String, Value>>("params")?;

        // Two families, two spellings of the same argument: the flux
        // kontext line takes `image_url` (one string), OpenAI's edit
        // endpoint takes `image_urls` (a list) and answers 422 without
        // it. Send both; each model reads the key it knows and ignores
        // the other. A model that refuses the one it does not know is not
        // a dead end: `params: { "image_urls": null }` takes it back off,
        // which is what a null extra means.
        let storage = ctx.storage(StorageScope::Run);
        let source = storage.external_file(&image).await?.url;
        let mut payload = json!({
            "prompt": prompt,
            "image_url": source.clone(),
            "image_urls": [source],
        });
        if let Some(mask) = &mask {
            payload["mask_url"] = json!(storage.external_file(mask).await?.url);
        }
        merge_params(&mut payload, params.as_ref());

        let http = ctx.client(&account).await?;
        let mut job = ctx.keep(Queued::default()).await?;
        let answer =
            run_queued(&mut job, &account, &http, &model, &payload, "fal: edit the image").await?;

        let url = image_url(&answer).node_err("fal answered no image for this edit")?;

        let ty = ctx.output_type("image")?;
        let stored = ctx
            .storage(StorageScope::Run)
            .internalize(&Value::String(url.to_string()), &ty, None)
            .await?;
        ctx.pulse_downstream(NodeOutput::new().set("image", stored)).await
    }
}
