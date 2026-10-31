//! DecisionYesNo self-tests, and through it the state every decision
//! node builds from its added ports (`decision.rs`): field order,
//! records, files read as text, media sent as labelled items, and the
//! loud refusals before anything is paid.

use serde_json::{json, Value};

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::super::decision::{audio_format, file_content, layout, PORT_TYPES};
use super::super::testing::{answer_decision, decision_request, live_provider, provider, ty, CHEAP_MODEL};
use super::DecisionYesNoNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("media_makes_the_state_an_array_after_the_fields", layout_with_media),
        NodeTest::basic("audio_formats_come_from_the_mime_type", audio_formats),
        NodeTest::basic("every_decision_node_holds_its_ports_to_port_types", ports_held_to_port_types),
        NodeTest::basic("a_file_is_read_as_json_or_text_and_nothing_else", file_contents),
        NodeTest::fake("asks_a_noul_about_the_ports_in_written_order", asks_in_written_order),
        NodeTest::fake("yes_follows_the_threshold", yes_follows_threshold),
        NodeTest::fake("both_criteria_ride_the_question", both_criteria),
        NodeTest::fake("one_criterion_alone_is_refused", one_criterion),
        NodeTest::fake("a_probability_at_the_threshold_is_yes", at_threshold),
        NodeTest::fake("the_routing_block_rides_the_request", routing_knobs),
        NodeTest::fake("an_image_port_is_sent_after_a_label", image_sent),
        NodeTest::fake("a_record_is_sent_as_it_is", record_sent_whole),
        NodeTest::fake("a_file_inside_a_record_is_refused", file_inside_a_record),
        NodeTest::fake("a_list_of_images_is_sent_item_by_item", list_of_images),
        NodeTest::fake("a_list_of_text_files_is_a_list_of_contents", list_of_text_files),
        NodeTest::fake("an_audio_port_rides_as_base64_with_its_format", audio_sent),
        NodeTest::fake("a_video_port_is_sent_as_a_video_part", video_sent),
        NodeTest::fake("an_image_given_as_a_data_url_is_sent_too", data_url_sent),
        NodeTest::fake("a_json_file_naming_a_stored_file_is_refused", json_file_naming_a_file),
        NodeTest::fake("text_and_json_files_are_sent_as_their_content", files_as_content),
        NodeTest::fake("a_pdf_is_refused_before_the_call", pdf_refused),
        NodeTest::fake("a_stored_file_inside_a_json_value_is_refused", undeclared_file),
        NodeTest::fake("a_web_address_in_an_image_port_is_refused", web_address_refused),
        NodeTest::fake("an_optional_port_nothing_reached_is_left_out", optional_port_left_out),
        NodeTest::fake("nothing_arrived_is_refused", nothing_arrived),
        NodeTest::fake("a_provider_of_another_kind_is_refused", wrong_provider_kind),
        NodeTest::fake("an_answer_of_another_type_fails", answer_of_another_type),
        NodeTest::fake("an_answer_with_no_type_fails", answer_with_no_type),
        NodeTest::fake("an_answer_with_no_probability_fails", answer_with_no_noul),
        NodeTest::fake("a_stopped_run_sends_nothing", stopped_before_the_call),
        NodeTest::fake("a_refused_call_is_caught_on_error", refused_call_caught),
        NodeTest::live("one_real_yes_no", "openrouter", live_yes_no),
    ]
}

/// The type bound every question node's metadata writes is the one the
/// state builder reads (`PORT_TYPES`): a metadata.json carries no SYNC
/// marker, so this is what keeps the two from drifting.
fn ports_held_to_port_types() -> WeftResult<()> {
    use weft::node::{Condition, NodeManifest};
    let expected: Vec<weft::WeftType> = PORT_TYPES.iter().map(|t| ty(t)).collect();
    let manifests = [
        DecisionYesNoNode.manifest(),
        super::super::decision_choice::DecisionChoiceNode.manifest(),
        super::super::decision_score::DecisionScoreNode.manifest(),
        super::super::decision_box::DecisionBoxNode.manifest(),
    ];
    for meta in manifests {
        let bound = meta.validate.iter().find_map(|rule| match &rule.when {
            Condition::Not { of } => match of.as_ref() {
                Condition::CustomInputsFit { one_of } => Some(one_of.clone()),
                _ => None,
            },
            _ => None,
        });
        assert_eq!(bound.as_deref(), Some(expected.as_slice()), "{}", meta.node_type);
    }
    Ok(())
}

fn layout_with_media() -> WeftResult<()> {
    let mut fields = serde_json::Map::new();
    fields.insert("title".into(), json!("Red square"));
    assert_eq!(layout(fields.clone(), Vec::new()), json!({ "title": "Red square" }), "no media: the object");
    let part = json!({ "type": "image_url", "image_url": { "url": "data:image/png;base64,AA==" } });
    assert_eq!(
        layout(fields, vec![json!("`photo`:"), part.clone()]),
        json!([{ "title": "Red square" }, "`photo`:", part]),
    );
    assert_eq!(
        layout(serde_json::Map::new(), vec![json!("`photo`:"), part.clone()]),
        json!(["`photo`:", part]),
        "no field left: only the media"
    );
    Ok(())
}

fn audio_formats() -> WeftResult<()> {
    for (mime, format) in [
        ("audio/wav", "wav"),
        ("audio/x-wav", "wav"),
        ("audio/wave", "wav"),
        ("audio/mpeg", "mpeg"),
        ("audio/mp4", "mp4"),
        ("audio/x-m4a", "m4a"),
        ("audio/ogg; codecs=opus", "ogg"),
        ("AUDIO/FLAC", "flac"),
    ] {
        assert_eq!(audio_format("clip", mime)?, format, "{mime}");
    }
    let err = audio_format("clip", "audio/amr").expect_err("amr is not taken").to_string();
    assert!(err.contains("audio/amr") && err.contains("`clip`"), "{err}");
    Ok(())
}

fn file_contents() -> WeftResult<()> {
    assert_eq!(file_content("doc", "application/json", br#"{"a":[1]}"#)?, json!({ "a": [1] }));
    assert_eq!(file_content("doc", "application/ld+json", br#"[1]"#)?, json!([1]));
    assert_eq!(file_content("doc", "text/plain; charset=utf-8", b"hello")?, json!("hello"));
    assert_eq!(file_content("doc", "text/markdown", b"# h")?, json!("# h"));
    let pdf = file_content("doc", "application/pdf", b"%PDF").expect_err("a pdf is not text").to_string();
    assert!(pdf.contains("application/pdf") && pdf.contains("`doc`"), "{pdf}");
    let broken = file_content("doc", "application/json", b"{").expect_err("broken json").to_string();
    assert!(broken.contains("does not parse"), "{broken}");
    let binary = file_content("doc", "text/plain", &[0xff, 0xfe]).expect_err("not utf-8").to_string();
    assert!(binary.contains("not UTF-8"), "{binary}");
    Ok(())
}

fn noul(p: f64) -> Value {
    json!({ "type": "noul", "noul": p })
}

async fn asks_in_written_order(rig: FakeRig) -> WeftResult<()> {
    answer_decision(&rig, noul(0.96));
    rig.input_type("ticket", ty("String"));
    rig.input_type("customerTier", ty("String"));
    let outcome = rig
        .run(
            &DecisionYesNoNode,
            json!({
                "provider": provider(&rig),
                "instructions": "Is the customer reporting a software defect?",
                "ticket": "The checkout page is blank after I click Pay.",
                "customerTier": "enterprise",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["probability"], json!(0.96));
    assert_eq!(outcome.outputs["yes"], json!(true));
    let body = decision_request(&rig);
    assert_eq!(body["model"], json!("test/decider"));
    assert_eq!(
        body["questions"],
        json!({ "answer": { "type": "noul", "instructions": "Is the customer reporting a software defect?" } }),
        "no criteria written, none sent"
    );
    assert_eq!(
        body["state"],
        json!({ "ticket": "The checkout page is blank after I click Pay.", "customerTier": "enterprise" })
    );
    let text = rig.requests()[0].body_text.clone().expect("a body");
    assert!(
        text.find("\"ticket\"").expect("ticket sent") < text.find("\"customerTier\"").expect("tier sent"),
        "the fields keep the order the ports were written in: {text}"
    );
    assert!(body.get("provider").is_none(), "no routing knob set, no routing sent");
    Ok(())
}

async fn yes_follows_threshold(rig: FakeRig) -> WeftResult<()> {
    answer_decision(&rig, noul(0.6));
    rig.input_type("text", ty("String"));
    let outcome = rig
        .run(
            &DecisionYesNoNode,
            json!({ "provider": provider(&rig), "instructions": "Spam?", "threshold": 0.7, "text": "hi" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["probability"], json!(0.6));
    assert_eq!(outcome.outputs["yes"], json!(false), "0.6 is under a 0.7 threshold");
    Ok(())
}

async fn both_criteria(rig: FakeRig) -> WeftResult<()> {
    answer_decision(&rig, noul(0.2));
    rig.input_type("text", ty("String"));
    let outcome = rig
        .run(
            &DecisionYesNoNode,
            json!({
                "provider": provider(&rig),
                "instructions": "Is it a bug?",
                "whenTrue": "Broken behavior.",
                "whenFalse": "A question or a feature request.",
                "text": "Can you add dark mode?",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["yes"], json!(false), "0.2 is under the default 0.5");
    let body = decision_request(&rig);
    assert_eq!(
        body["questions"]["answer"]["criteria"],
        json!({ "true": "Broken behavior.", "false": "A question or a feature request." })
    );
    Ok(())
}

async fn one_criterion(rig: FakeRig) -> WeftResult<()> {
    rig.input_type("text", ty("String"));
    let err = rig
        .run(
            &DecisionYesNoNode,
            json!({ "provider": provider(&rig), "instructions": "Bug?", "whenTrue": "Broken.", "text": "x" }),
        )
        .await
        .failure()?;
    assert!(err.contains("whenTrue and whenFalse go together"), "{err}");
    let err = rig
        .run(
            &DecisionYesNoNode,
            json!({ "provider": provider(&rig), "instructions": "Bug?", "whenFalse": "Fine.", "text": "x" }),
        )
        .await
        .failure()?;
    assert!(err.contains("whenTrue and whenFalse go together"), "whenFalse alone too: {err}");
    assert!(rig.requests().is_empty(), "nothing was sent");
    Ok(())
}

async fn at_threshold(rig: FakeRig) -> WeftResult<()> {
    answer_decision(&rig, noul(0.7));
    rig.input_type("text", ty("String"));
    let outcome = rig
        .run(
            &DecisionYesNoNode,
            json!({ "provider": provider(&rig), "instructions": "Spam?", "threshold": 0.7, "text": "hi" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["yes"], json!(true), "the threshold is where yes starts");
    Ok(())
}

async fn routing_knobs(rig: FakeRig) -> WeftResult<()> {
    answer_decision(&rig, noul(0.5));
    rig.input_type("text", ty("String"));
    let mut provider = provider(&rig);
    provider["routing"] = json!({ "order": ["perplexity"], "allow_fallbacks": false });
    rig.run(&DecisionYesNoNode, json!({ "provider": provider, "instructions": "Bug?", "text": "x" }))
        .await
        .ok()?;
    let body = decision_request(&rig);
    assert_eq!(body["provider"], json!({ "order": ["perplexity"], "allow_fallbacks": false }));
    Ok(())
}

async fn image_sent(rig: FakeRig) -> WeftResult<()> {
    answer_decision(&rig, noul(0.9));
    rig.input_type("title", ty("String"));
    rig.input_type("photo", ty("Image"));
    let photo = rig.store_file("photo.png", "image/png", b"png-bytes".to_vec());
    rig.run(
        &DecisionYesNoNode,
        json!({
            "provider": provider(&rig),
            "instructions": "Does `photo` show the item in `title`?",
            "title": "Red square sticker",
            "photo": photo,
        }),
    )
    .await
    .ok()?;
    let body = decision_request(&rig);
    assert_eq!(
        body["state"],
        json!([
            { "title": "Red square sticker" },
            "`photo`:",
            { "type": "image_url", "image_url": { "url": "data:image/png;base64,cG5nLWJ5dGVz" } },
        ]),
        "the image follows the object, after its label"
    );
    let text = rig.requests()[0].body_text.clone().expect("a body");
    assert!(!text.contains("__weft_"), "no stored-file value leaves weft");
    Ok(())
}

async fn record_sent_whole(rig: FakeRig) -> WeftResult<()> {
    answer_decision(&rig, noul(0.9));
    rig.input_type("listing", ty("{ title: String, tags: List[String] }"));
    rig.run(
        &DecisionYesNoNode,
        json!({
            "provider": provider(&rig),
            "instructions": "Is `listing.title` about stickers?",
            "listing": { "title": "Red square sticker", "tags": ["red"] },
        }),
    )
    .await
    .ok()?;
    let body = decision_request(&rig);
    assert_eq!(body["state"], json!({ "listing": { "title": "Red square sticker", "tags": ["red"] } }));
    Ok(())
}

/// The compiler refuses this port's type (`custom_inputs_fit`); run as it
/// stands, the node refuses it too, before anything is read or paid.
async fn file_inside_a_record(rig: FakeRig) -> WeftResult<()> {
    rig.input_type("listing", ty("{ title: String, photo: Image }"));
    let photo = rig.store_file("front.jpg", "image/jpeg", b"front".to_vec());
    let err = rig
        .run(
            &DecisionYesNoNode,
            json!({
                "provider": provider(&rig),
                "instructions": "Does `listing.photo` match `listing.title`?",
                "listing": { "title": "Red square sticker", "photo": photo },
            }),
        )
        .await
        .failure()?;
    assert!(err.contains("a value made of JSON, a file, or a list of files"), "{err}");
    assert!(rig.requests().is_empty(), "nothing was sent");
    Ok(())
}

async fn list_of_images(rig: FakeRig) -> WeftResult<()> {
    answer_decision(&rig, noul(0.9));
    rig.input_type("photos", ty("List[Image]"));
    let a = rig.store_file("a.png", "image/png", b"a".to_vec());
    let b = rig.store_file("b.png", "image/png", b"b".to_vec());
    rig.run(
        &DecisionYesNoNode,
        json!({ "provider": provider(&rig), "instructions": "Same item in both?", "photos": [a, b] }),
    )
    .await
    .ok()?;
    let body = decision_request(&rig);
    assert_eq!(
        body["state"],
        json!([
            "`photos[0]`:",
            { "type": "image_url", "image_url": { "url": "data:image/png;base64,YQ==" } },
            "`photos[1]`:",
            { "type": "image_url", "image_url": { "url": "data:image/png;base64,Yg==" } },
        ]),
        "a port of only images leaves no object behind"
    );
    Ok(())
}

async fn list_of_text_files(rig: FakeRig) -> WeftResult<()> {
    answer_decision(&rig, noul(0.9));
    rig.input_type("pages", ty("List[File]"));
    let a = rig.store_file("a.txt", "text/plain", b"Page one.".to_vec());
    let b = rig.store_file("b.json", "application/json", br#"{"page":2}"#.to_vec());
    rig.run(&DecisionYesNoNode, json!({ "provider": provider(&rig), "instructions": "Spam?", "pages": [a, b] }))
        .await
        .ok()?;
    let body = decision_request(&rig);
    assert_eq!(body["state"], json!({ "pages": ["Page one.", { "page": 2 }] }));
    Ok(())
}

async fn audio_sent(rig: FakeRig) -> WeftResult<()> {
    answer_decision(&rig, noul(0.1));
    rig.input_type("call", ty("Audio"));
    let call = rig.store_file("call.wav", "audio/wav", b"RIFF".to_vec());
    rig.run(
        &DecisionYesNoNode,
        json!({ "provider": provider(&rig), "instructions": "Is the caller angry?", "call": call }),
    )
    .await
    .ok()?;
    let body = decision_request(&rig);
    assert_eq!(
        body["state"],
        json!(["`call`:", { "type": "input_audio", "input_audio": { "data": "UklGRg==", "format": "wav" } }])
    );
    Ok(())
}

async fn video_sent(rig: FakeRig) -> WeftResult<()> {
    answer_decision(&rig, noul(0.9));
    rig.input_type("clip", ty("Video"));
    let clip = rig.store_file("clip.mp4", "video/mp4", b"mp4".to_vec());
    rig.run(&DecisionYesNoNode, json!({ "provider": provider(&rig), "instructions": "A cat?", "clip": clip }))
        .await
        .ok()?;
    let body = decision_request(&rig);
    assert_eq!(
        body["state"],
        json!(["`clip`:", { "type": "video_url", "video_url": { "url": "data:video/mp4;base64,bXA0" } }])
    );
    Ok(())
}

async fn data_url_sent(rig: FakeRig) -> WeftResult<()> {
    answer_decision(&rig, noul(0.9));
    rig.input_type("photo", ty("Image"));
    rig.run(
        &DecisionYesNoNode,
        json!({ "provider": provider(&rig), "instructions": "Red?", "photo": "data:image/png;base64,cG5n" }),
    )
    .await
    .ok()?;
    let body = decision_request(&rig);
    assert_eq!(
        body["state"],
        json!(["`photo`:", { "type": "image_url", "image_url": { "url": "data:image/png;base64,cG5n" } }])
    );
    Ok(())
}

async fn json_file_naming_a_file(rig: FakeRig) -> WeftResult<()> {
    rig.input_type("export", ty("File"));
    let inner = rig.store_file("photo.png", "image/png", b"png".to_vec());
    let export = rig.store_file("export.json", "application/json", serde_json::to_vec(&json!({ "photo": inner })).expect("json"));
    let err = rig
        .run(&DecisionYesNoNode, json!({ "provider": provider(&rig), "instructions": "Ok?", "export": export }))
        .await
        .failure()?;
    assert!(err.contains("`export` is a JSON file naming a stored file"), "{err}");
    assert!(rig.requests().is_empty(), "nothing was sent");
    Ok(())
}

async fn files_as_content(rig: FakeRig) -> WeftResult<()> {
    answer_decision(&rig, noul(0.9));
    rig.input_type("contract", ty("File"));
    rig.input_type("customer", ty("File"));
    let contract = rig.store_file("contract.txt", "text/plain", b"The supplier pays all fees.".to_vec());
    let customer = rig.store_file("customer.json", "application/json", br#"{"plan":"pro"}"#.to_vec());
    rig.run(
        &DecisionYesNoNode,
        json!({
            "provider": provider(&rig),
            "instructions": "Does `contract` make `customer` pay any fee?",
            "contract": contract,
            "customer": customer,
        }),
    )
    .await
    .ok()?;
    let body = decision_request(&rig);
    assert_eq!(
        body["state"],
        json!({ "contract": "The supplier pays all fees.", "customer": { "plan": "pro" } }),
        "a text file is its text and a JSON file its value, in place"
    );
    Ok(())
}

async fn pdf_refused(rig: FakeRig) -> WeftResult<()> {
    rig.input_type("contract", ty("File"));
    let contract = rig.store_file("contract.pdf", "application/pdf", b"%PDF-1.7".to_vec());
    let err = rig
        .run(
            &DecisionYesNoNode,
            json!({ "provider": provider(&rig), "instructions": "Fees?", "contract": contract }),
        )
        .await
        .failure()?;
    assert!(err.contains("`contract` is a application/pdf file"), "{err}");
    assert!(rig.requests().is_empty(), "nothing was sent");
    Ok(())
}

async fn undeclared_file(rig: FakeRig) -> WeftResult<()> {
    rig.input_type("payload", ty("JsonDict"));
    let photo = rig.store_file("photo.png", "image/png", b"png".to_vec());
    let err = rig
        .run(
            &DecisionYesNoNode,
            json!({ "provider": provider(&rig), "instructions": "Ok?", "payload": { "photo": photo } }),
        )
        .await
        .failure()?;
    assert!(err.contains("holds a stored file inside a value"), "{err}");
    assert!(rig.requests().is_empty(), "the stored-file value never left weft");
    Ok(())
}

async fn web_address_refused(rig: FakeRig) -> WeftResult<()> {
    rig.input_type("photo", ty("Image"));
    let err = rig
        .run(
            &DecisionYesNoNode,
            json!({
                "provider": provider(&rig),
                "instructions": "Red?",
                "photo": "https://example.com/photo.png",
            }),
        )
        .await
        .failure()?;
    assert!(err.contains("decision models never fetch one"), "{err}");
    assert!(rig.requests().is_empty(), "nothing was sent");
    Ok(())
}

async fn optional_port_left_out(rig: FakeRig) -> WeftResult<()> {
    answer_decision(&rig, noul(0.3));
    rig.input_type("ticket", ty("String"));
    rig.input_type("screenshot", ty("Image | Null"));
    rig.run(
        &DecisionYesNoNode,
        json!({ "provider": provider(&rig), "instructions": "Bug?", "ticket": "It crashes." }),
    )
    .await
    .ok()?;
    let body = decision_request(&rig);
    assert_eq!(body["state"], json!({ "ticket": "It crashes." }), "the silent port is not a field");
    Ok(())
}

async fn nothing_arrived(rig: FakeRig) -> WeftResult<()> {
    rig.input_type("ticket", ty("String | Null"));
    let err = rig
        .run(&DecisionYesNoNode, json!({ "provider": provider(&rig), "instructions": "Bug?" }))
        .await
        .failure()?;
    assert!(err.contains("holds anything to judge"), "{err}");
    assert!(rig.requests().is_empty(), "nothing was sent");
    Ok(())
}

async fn wrong_provider_kind(rig: FakeRig) -> WeftResult<()> {
    rig.input_type("text", ty("String"));
    let err = rig
        .run(
            &DecisionYesNoNode,
            json!({
                "provider": { "kind": "anthropic", "model": "m", "account": rig.access("anthropic") },
                "instructions": "Bug?",
                "text": "x",
            }),
        )
        .await
        .failure()?;
    assert!(err.contains("wire an OpenRouter Decision Model (got 'anthropic')"), "{err}");
    assert!(rig.requests().is_empty(), "nothing was sent");
    Ok(())
}

async fn answer_of_another_type(rig: FakeRig) -> WeftResult<()> {
    answer_decision(&rig, json!({ "type": "choice", "choice": "a" }));
    rig.input_type("text", ty("String"));
    let err = rig
        .run(&DecisionYesNoNode, json!({ "provider": provider(&rig), "instructions": "Bug?", "text": "x" }))
        .await
        .failure()?;
    assert!(err.contains("answered a choice to a noul question"), "{err}");
    Ok(())
}

async fn answer_with_no_type(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/api/v1/systemone", json!({ "answers": {} }));
    rig.input_type("text", ty("String"));
    let err = rig
        .run(&DecisionYesNoNode, json!({ "provider": provider(&rig), "instructions": "Bug?", "text": "x" }))
        .await
        .failure()?;
    assert!(err.contains("answered nothing to a noul question"), "{err}");
    Ok(())
}

async fn answer_with_no_noul(rig: FakeRig) -> WeftResult<()> {
    answer_decision(&rig, json!({ "type": "noul" }));
    rig.input_type("text", ty("String"));
    let err = rig
        .run(&DecisionYesNoNode, json!({ "provider": provider(&rig), "instructions": "Bug?", "text": "x" }))
        .await
        .failure()?;
    assert!(err.contains("carries no `noul`"), "{err}");
    Ok(())
}

async fn stopped_before_the_call(rig: FakeRig) -> WeftResult<()> {
    answer_decision(&rig, noul(0.9));
    rig.input_type("text", ty("String"));
    rig.stop_after_calls(0);
    let outcome = rig
        .run(&DecisionYesNoNode, json!({ "provider": provider(&rig), "instructions": "Bug?", "text": "x" }))
        .await;
    assert!(matches!(outcome.result, Err(weft::WeftError::Cancelled)), "{:?}", outcome.result);
    assert!(rig.requests().is_empty(), "a run already stopped sends nothing");
    assert!(outcome.outputs.is_empty(), "a stopped run emits nothing");
    Ok(())
}

async fn refused_call_caught(rig: FakeRig) -> WeftResult<()> {
    rig.respond_status(
        "POST",
        "/api/v1/systemone",
        400,
        json!({ "error": { "code": 400, "message": "Perplexity does not accept \"input_audio\" parts in state" } }),
    );
    rig.wire_output("_error");
    rig.input_type("text", ty("String"));
    let outcome = rig
        .run(&DecisionYesNoNode, json!({ "provider": provider(&rig), "instructions": "Bug?", "text": "x" }))
        .await
        .ok()?;
    let error = outcome.output("_error")?.as_str().expect("a message").to_string();
    assert!(error.contains("does not accept"), "the provider's own words reach `_error`: {error}");
    assert!(!outcome.outputs.contains_key("probability"), "a caught failure emits no probability");
    Ok(())
}

async fn live_yes_no(rig: LiveRig) -> WeftResult<()> {
    rig.input_type("ticket", ty("String"));
    let provider = live_provider(&rig, CHEAP_MODEL);
    let bug = rig
        .run(
            &DecisionYesNoNode,
            json!({
                "provider": provider,
                "instructions": "Is the customer reporting a software defect?",
                "ticket": "My checkout page shows a blank screen after I click Pay. I tried two browsers.",
            }),
        )
        .await
        .ok()?;
    assert_eq!(bug.outputs["yes"], json!(true), "a broken checkout is a defect: {:?}", bug.outputs);
    Ok(())
}
