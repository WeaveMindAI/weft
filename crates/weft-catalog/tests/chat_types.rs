//! The declared chat types mirror minillmlib's serde exactly (apart
//! from media slots, which hold stored-file values in stored form).
//! This test builds REAL minillmlib messages, serializes them, swaps
//! each media slot for a stored-file value through the same type-driven
//! walk the runtime uses, and validates the result against the
//! catalog's declared `ChatHistory`. A drift on either side (a lib
//! field the declaration misses, a declaration field the lib never
//! writes) fails here instead of at a user's provider call.
// SYNC: (exercises) catalog/ai/openrouter/metadata.json types <->
//       MiniLLMLibRS/src/message serde

use minillmlib::{
    AudioData, ContentPart, ImageData, Media, Message, MessageContent, VideoData,
};
use weft_catalog::{stdlib_root, FsCatalog};
use weft_core::weft_type::{FileKind, WeftType};

fn marker(kind: FileKind, mime: &str) -> serde_json::Value {
    WeftType::file_marker(kind, serde_json::json!({ "key": "k1", "mimeType": mime }))
}

#[test]
fn declared_chat_history_accepts_real_minillmlib_messages() {
    let catalog = FsCatalog::discover(&stdlib_root()).expect("stdlib catalog");
    let history_ty = catalog
        .type_registry()
        .scoped(|| WeftType::parse("ChatHistory"))
        .expect("ChatHistory is declared in the stdlib catalog");

    // A conversation exercising every declared field: plain text, a
    // named message, multimodal parts (image with detail + dimensions,
    // audio with duration, video), tool calls, a tool result, and a
    // cache breakpoint.
    let mut with_tools = Message::assistant("checking the weather");
    with_tools.tool_calls = Some(vec![minillmlib::ToolCall::new(
        "call-1",
        "get_weather",
        r#"{"city":"Paris"}"#,
    )]);
    let mut cached = Message::system("you are terse");
    cached.cache_breakpoint = true;
    let messages = vec![
        cached,
        Message::user("hello").with_name("alice"),
        Message::user(MessageContent::with_media(
            "what is in these?",
            &[
                Media::Image(
                    ImageData::from_url("https://x.example/a.png").with_dimensions(800, 600),
                ),
                Media::Audio(AudioData::from_bytes(&[0u8; 4], "mp3").with_duration(3.5)),
                Media::Video(VideoData::from_url("https://x.example/c.mp4").with_duration(9.0)),
            ],
        )),
        with_tools,
        Message::tool("call-1", "15 degrees"),
        Message::assistant(MessageContent::parts(vec![
            ContentPart::text("here"),
            ContentPart::image(&ImageData::from_url("data:image/png;base64,aGk=")),
        ])),
    ];
    let serialized = serde_json::to_value(&messages).expect("messages serialize");

    // Swap every media slot (the lib serialized URLs / base64 data)
    // for a stored-file value, through the SAME walk the runtime's
    // internalize uses, then the stored form must validate.
    let slots = weft_core::storage::media::media_slots(&serialized, &history_ty);
    // `media_slots` dedupes by serialized form, so this counts DISTINCT
    // media values: the two url-images, the audio, and the video.
    assert_eq!(slots.len(), 4, "four distinct media values: {slots:?}");
    // Pick each replacement's kind from the slot's OWN content (never
    // by position: the traversal order is not this test's contract).
    let replacements = slots
        .iter()
        .map(|slot| {
            let text = slot.as_str().expect("a media slot the lib wrote is a string");
            let (kind, mime) = if text.contains(".mp4") {
                (FileKind::Video, "video/mp4")
            } else if text.contains("image") || text.contains(".png") {
                (FileKind::Image, "image/png")
            } else {
                (FileKind::Audio, "audio/mp3")
            };
            (slot.to_string(), marker(kind, mime))
        })
        .collect();
    let stored =
        weft_core::storage::media::substitute_media(&serialized, &history_ty, &replacements);

    history_ty
        .validate_value(&stored)
        .expect("the stored form of a real minillmlib conversation is a valid ChatHistory");

    // Strictness holds: a field the lib never writes is refused.
    let mut drifted = stored.clone();
    drifted[0]["surprise"] = serde_json::json!(1);
    let err = history_ty.validate_value(&drifted).unwrap_err();
    assert!(err.contains("surprise"), "{err}");

    // And the raw (pre-swap) form is NOT valid stored form: media slots
    // must hold stored-file values, never URLs or base64.
    assert!(history_ty.validate_value(&serialized).is_err());
}
