//! LinkOptions self-tests: the lifetime lands on the value and on every
//! link made from it, and nothing is stored.

use serde_json::json;

use weft::storage::{FileHandle, StorageScope};
use weft::{FakeRig, NodeTest, WeftResult};

use super::LinkOptionsNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("the_lifetime_rides_the_value_and_stores_nothing", sets),
        NodeTest::fake("empty_minutes_is_the_default_lifetime", default),
        NodeTest::fake("a_video_stays_a_video", video_stays_video),
        NodeTest::fake("a_blob_holding_a_picture_stays_a_blob", blob_stays_blob),
    ]
}

async fn sets(rig: FakeRig) -> WeftResult<()> {
    let file = rig.store_file("clip.mp4", "video/mp4", vec![1, 2, 3]);
    let outcome = rig.run(&LinkOptionsNode, json!({ "file": file.clone(), "minutes": 120 })).await.ok()?;
    let out = &outcome.outputs["file"];
    assert_eq!(out["__weft_video__"]["linkSecs"], json!(7200));
    assert_eq!(out["__weft_video__"]["key"], file["__weft_video__"]["key"], "the same file");
    assert_eq!(rig.stored_files(&StorageScope::Run)?.len(), 1, "nothing new was stored");
    // A link made from it, by any node, lives that long.
    let link = rig
        .run(&LinkProbe, json!({ "file": out.clone() }))
        .await
        .ok()?;
    assert!(link.outputs["url"].as_str().unwrap().ends_with("?ttl=7200"), "{}", link.outputs["url"]);
    Ok(())
}

async fn default(rig: FakeRig) -> WeftResult<()> {
    let mut file = rig.store_file("a.txt", "text/plain", b"x".to_vec());
    file["__weft_blob__"]["linkSecs"] = json!(60);
    let outcome = rig.run(&LinkOptionsNode, json!({ "file": file })).await.ok()?;
    assert!(outcome.outputs["file"]["__weft_blob__"].get("linkSecs").is_none(), "back to the default");
    Ok(())
}

async fn video_stays_video(rig: FakeRig) -> WeftResult<()> {
    let video = rig.store_file("clip.mp4", "video/mp4", vec![0]);
    rig.output_type("file", weft::WeftType::parse("Video").expect("parses"));
    let outcome = rig.run(&LinkOptionsNode, json!({ "file": video, "minutes": 5 })).await.ok()?;
    assert!(outcome.outputs["file"].get("__weft_video__").is_some());
    Ok(())
}

/// The kind is the one that came in, never one read again from the mime:
/// a png carried as a Blob goes out a Blob.
async fn blob_stays_blob(rig: FakeRig) -> WeftResult<()> {
    let picture = rig.store_file("cat.png", "image/png", vec![0]);
    let blob = json!({ "__weft_blob__": picture["__weft_image__"].clone() });
    let outcome = rig.run(&LinkOptionsNode, json!({ "file": blob, "minutes": 5 })).await.ok()?;
    let out = &outcome.outputs["file"];
    assert_eq!(out["__weft_blob__"]["linkSecs"], json!(300), "{out}");
    assert!(out.get("__weft_image__").is_none(), "{out}");
    Ok(())
}

/// A node that hands out a file's external link, as a Reply would.
struct LinkProbe;

impl weft::NodeManifest for LinkProbe {
    fn manifest(&self) -> &'static weft::NodeMetadata {
        static MANIFEST: std::sync::OnceLock<weft::NodeMetadata> = std::sync::OnceLock::new();
        MANIFEST.get_or_init(|| {
            serde_json::from_value(json!({
                "type": "LinkProbe",
                "label": "Link probe",
                "description": "Hands out a file's external link.",
                "inputs": [{ "name": "file", "type": "File", "required": true }],
                "outputs": [{ "name": "url", "type": "String" }]
            }))
            .expect("the probe's manifest")
        })
    }
}

#[async_trait::async_trait]
impl weft::Node for LinkProbe {
    async fn run(&self, ctx: weft::RunContext) -> WeftResult<()> {
        let file: FileHandle = ctx.inputs.get("file")?;
        let link = ctx.storage(StorageScope::Run).external_link(&file).await?;
        ctx.pulse_downstream(weft::node::NodeOutput::new().set("url", link.url)).await
    }
}
