//! FalEditImage self-tests.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult, WeftType};

use super::FalEditImageNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("edits_and_stores_the_image", edits),
        NodeTest::fake("params_null_drops_a_key", params_null_drops_a_key),
        NodeTest::live("one_real_edit", "fal", live_edit),
    ]
}

/// One real edit of a freshly minted sample image, on the blessed
/// per-image-priced edit model. fal stores nothing on the account, so
/// there is nothing to clean.
async fn live_edit(rig: LiveRig) -> WeftResult<()> {
    let conn = rig.connect().await?;
    let (mime, bytes) = crate::testing::sample_image(&conn).await?;
    let image = rig.store_file("seed.png", &mime, bytes).await?;
    let outcome = rig
        .run(
            &FalEditImageNode,
            json!({
                "account": rig.access("fal"),
                "image": image,
                "prompt": "make the square red",
                "model": "fal-ai/flux-pro/kontext",
            }),
        )
        .await
        .ok()?;
    assert!(outcome.output("image")?.is_object(), "the edit lands as a stored file");
    Ok(())
}

async fn edits(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/fal-ai/flux-pro/kontext", json!({ "request_id": "req-2" }));
    rig.respond(
        "GET",
        "/fal-ai/flux-pro/requests/req-2/status",
        json!({ "status": "COMPLETED" }),
    );
    rig.respond(
        "GET",
        "/fal-ai/flux-pro/requests/req-2",
        json!({ "images": [{ "url": "data:image/png;base64,ZWRpdA==" }] }),
    );
    rig.output_type("image", WeftType::parse("Image").expect("parses"));
    let image = rig.store_file("photo.png", "image/png", b"png-bytes".to_vec());
    let outcome = rig
        .run(
            &FalEditImageNode,
            json!({
                "account": rig.access("fal"),
                "image": image,
                "prompt": "make it night",
                "model": "fal-ai/flux-pro/kontext",
            }),
        )
        .await
        .ok()?;
    assert!(outcome.outputs["image"].is_object(), "the edit lands as a stored file");

    let sent = &rig.requests()[0];
    let body = sent.body.as_ref().expect("json payload");
    assert_eq!(body["prompt"], json!("make it night"));
    assert!(
        body["image_url"].as_str().is_some_and(|u| u.starts_with("data:image/png")),
        "the input image rides inline when no public link exists: {}",
        body["image_url"]
    );
    // OpenAI's edit endpoint reads `image_urls` and answers 422
    // without it, so the same image goes out under both spellings.
    assert_eq!(
        body["image_urls"],
        json!([body["image_url"].as_str().expect("image_url is a string")]),
        "image_urls carries the same image as image_url"
    );
    assert!(body.get("mask_url").is_none(), "no mask key without a mask");
    Ok(())
}

/// A model that refuses the spelling it does not know is not a dead end:
/// naming the key `null` in `params` takes it off the request.
async fn params_null_drops_a_key(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/fal-ai/flux-pro/kontext", json!({ "request_id": "req-3" }));
    rig.respond(
        "GET",
        "/fal-ai/flux-pro/requests/req-3/status",
        json!({ "status": "COMPLETED" }),
    );
    rig.respond(
        "GET",
        "/fal-ai/flux-pro/requests/req-3",
        json!({ "images": [{ "url": "data:image/png;base64,ZWRpdA==" }] }),
    );
    rig.output_type("image", WeftType::parse("Image").expect("parses"));
    let image = rig.store_file("photo.png", "image/png", b"png-bytes".to_vec());
    rig.run(
        &FalEditImageNode,
        json!({
            "account": rig.access("fal"),
            "image": image,
            "prompt": "make it night",
            "model": "fal-ai/flux-pro/kontext",
            "params": { "image_urls": null },
        }),
    )
    .await
    .ok()?;
    let sent = rig.requests();
    let body = sent[0].body.as_ref().expect("json payload");
    assert!(body.get("image_urls").is_none(), "a null extra removes the key: {body}");
    assert!(body["image_url"].as_str().is_some(), "the spelling this family reads still goes out");
    Ok(())
}
