//! DecisionBox self-tests: the box from the answer, and the refusal of
//! a state with no image before anything is paid.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::super::testing::{answer_decision, decision_request, live_provider, provider, ty};
use super::DecisionBoxNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("hands_back_the_box_and_its_confidence", finds),
        NodeTest::fake("no_image_is_refused_before_the_call", no_image),
        NodeTest::fake("a_box_missing_an_edge_fails", missing_edge),
        NodeTest::fake("audio_alone_is_no_image_to_look_in", audio_only),
        NodeTest::fake("a_box_with_top_below_bottom_fails", upside_down),
        NodeTest::fake("a_box_with_left_past_right_fails", mirrored),
        NodeTest::fake("a_box_past_the_image_fails", past_the_image),
        NodeTest::live("one_real_box", "openrouter", live_box),
    ]
}

async fn finds(rig: FakeRig) -> WeftResult<()> {
    answer_decision(
        &rig,
        json!({ "type": "box", "box": { "top": 0.11, "bottom": 0.44, "left": 0.55, "right": 0.7 }, "confidence": 0.63 }),
    );
    rig.input_type("photo", ty("Image"));
    let photo = rig.store_file("photo.png", "image/png", b"png".to_vec());
    let outcome = rig
        .run(
            &DecisionBoxNode,
            json!({ "provider": provider(&rig), "instructions": "the red square in `photo`", "photo": photo }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["box"], json!({ "top": 0.11, "bottom": 0.44, "left": 0.55, "right": 0.7 }));
    assert_eq!(outcome.outputs["confidence"], json!(0.63));
    let body = decision_request(&rig);
    assert_eq!(body["questions"]["answer"], json!({ "type": "box", "instructions": "the red square in `photo`" }));
    Ok(())
}

async fn no_image(rig: FakeRig) -> WeftResult<()> {
    rig.input_type("note", ty("String"));
    let err = rig
        .run(
            &DecisionBoxNode,
            json!({ "provider": provider(&rig), "instructions": "the product", "note": "no picture here" }),
        )
        .await
        .failure()?;
    assert!(err.contains("no image to look in"), "{err}");
    assert!(rig.requests().is_empty(), "nothing was paid for");
    Ok(())
}

async fn missing_edge(rig: FakeRig) -> WeftResult<()> {
    answer_decision(&rig, json!({ "type": "box", "box": { "top": 0.1, "bottom": 0.4, "left": 0.5 }, "confidence": 0.6 }));
    rig.input_type("photo", ty("Image"));
    let photo = rig.store_file("photo.png", "image/png", b"png".to_vec());
    let err = rig
        .run(&DecisionBoxNode, json!({ "provider": provider(&rig), "instructions": "the square", "photo": photo }))
        .await
        .failure()?;
    assert!(err.contains("carries no `right`"), "{err}");
    Ok(())
}

async fn audio_only(rig: FakeRig) -> WeftResult<()> {
    rig.input_type("call", ty("Audio"));
    let call = rig.store_file("call.mp3", "audio/mpeg", b"mp3".to_vec());
    let err = rig
        .run(&DecisionBoxNode, json!({ "provider": provider(&rig), "instructions": "the speaker", "call": call }))
        .await
        .failure()?;
    assert!(err.contains("no image to look in"), "{err}");
    assert!(rig.requests().is_empty(), "nothing was paid for");
    Ok(())
}

async fn upside_down(rig: FakeRig) -> WeftResult<()> {
    refuses_box(rig, json!({ "top": 0.6, "bottom": 0.2, "left": 0.1, "right": 0.4 })).await
}

async fn mirrored(rig: FakeRig) -> WeftResult<()> {
    refuses_box(rig, json!({ "top": 0.1, "bottom": 0.4, "left": 0.5, "right": 0.2 })).await
}

async fn past_the_image(rig: FakeRig) -> WeftResult<()> {
    refuses_box(rig, json!({ "top": 0.1, "bottom": 1.3, "left": 0.1, "right": 0.4 })).await
}

/// The node fails on an answer carrying `found` as its box.
async fn refuses_box(rig: FakeRig, found: serde_json::Value) -> WeftResult<()> {
    answer_decision(&rig, json!({ "type": "box", "box": found, "confidence": 0.6 }));
    rig.input_type("photo", ty("Image"));
    let photo = rig.store_file("photo.png", "image/png", b"png".to_vec());
    let err = rig
        .run(&DecisionBoxNode, json!({ "provider": provider(&rig), "instructions": "the square", "photo": photo }))
        .await
        .failure()?;
    assert!(err.contains("a box that is no box"), "{found}: {err}");
    Ok(())
}

/// A 400x300 white PNG with a red square at x 200..300, y 50..150: the
/// box is left 0.5, right 0.75, top 0.17, bottom 0.5.
fn square_png() -> Vec<u8> {
    let (w, h) = (400u32, 300u32);
    let mut raw = Vec::with_capacity(((w * 3 + 1) * h) as usize);
    for y in 0..h {
        raw.push(0);
        for x in 0..w {
            let red = (200..300).contains(&x) && (50..150).contains(&y);
            raw.extend_from_slice(if red { &[220, 20, 20] } else { &[255, 255, 255] });
        }
    }
    png(w, h, &raw)
}

/// An uncompressed (stored-deflate) RGB PNG of `raw` scanlines.
fn png(w: u32, h: u32, raw: &[u8]) -> Vec<u8> {
    fn crc(bytes: &[u8]) -> u32 {
        let mut c = 0xffff_ffffu32;
        for b in bytes {
            c ^= u32::from(*b);
            for _ in 0..8 {
                c = if c & 1 == 1 { 0xedb8_8320 ^ (c >> 1) } else { c >> 1 };
            }
        }
        !c
    }
    fn adler(bytes: &[u8]) -> u32 {
        let (mut a, mut b) = (1u32, 0u32);
        for x in bytes {
            a = (a + u32::from(*x)) % 65521;
            b = (b + a) % 65521;
        }
        (b << 16) | a
    }
    fn chunk(out: &mut Vec<u8>, kind: &[u8], data: &[u8]) {
        out.extend_from_slice(&(data.len() as u32).to_be_bytes());
        let mut body = kind.to_vec();
        body.extend_from_slice(data);
        out.extend_from_slice(&body);
        out.extend_from_slice(&crc(&body).to_be_bytes());
    }
    let mut zlib = vec![0x78, 0x01];
    let blocks: Vec<&[u8]> = raw.chunks(65535).collect();
    for (i, block) in blocks.iter().enumerate() {
        zlib.push(u8::from(i + 1 == blocks.len()));
        let len = block.len() as u16;
        zlib.extend_from_slice(&len.to_le_bytes());
        zlib.extend_from_slice(&(!len).to_le_bytes());
        zlib.extend_from_slice(block);
    }
    zlib.extend_from_slice(&adler(raw).to_be_bytes());
    let mut header = w.to_be_bytes().to_vec();
    header.extend_from_slice(&h.to_be_bytes());
    header.extend_from_slice(&[8, 2, 0, 0, 0]);
    let mut out = b"\x89PNG\r\n\x1a\n".to_vec();
    chunk(&mut out, b"IHDR", &header);
    chunk(&mut out, b"IDAT", &zlib);
    chunk(&mut out, b"IEND", &[]);
    out
}

async fn live_box(rig: LiveRig) -> WeftResult<()> {
    rig.input_type("photo", ty("Image"));
    let photo = rig.store_file("square.png", "image/png", square_png()).await?;
    let outcome = rig
        .run(
            &DecisionBoxNode,
            json!({
                "provider": live_provider(&rig, "typesafe/jev-1.14"),
                "instructions": "the red square in `photo`",
                "photo": photo,
            }),
        )
        .await
        .ok()?;
    let found = &outcome.outputs["box"];
    // Each edge within a tenth of the image of the square's own.
    for (edge, truth) in [("left", 0.5), ("right", 0.75), ("top", 50.0 / 300.0), ("bottom", 0.5)] {
        let at = found[edge].as_f64().unwrap_or(-1.0);
        assert!((at - truth).abs() <= 0.1, "the box's {edge} sits on the square's: {found}");
    }
    Ok(())
}
