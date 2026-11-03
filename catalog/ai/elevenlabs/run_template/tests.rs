//! ElevenLabsRunTemplate self-tests: the weft type each template kind
//! takes, how each port value goes, the check of the ports against the
//! template before anything runs, the parked wait, and each output
//! landing on its port.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use weft::storage::FileHandle;
use weft::{fixture_spec, FakeRig, LiveRig, NodeTest, WeftResult, WeftType};

use crate::testing::{emitted_file, inline};

use super::{
    input_mismatches, input_value, output_mismatches, output_type, pick_version, weft_type,
    ElevenLabsRunTemplateNode,
};

// The routes of the template every fake test runs: its read, the run's
// create, and the run.
const TEMPLATE: &str = "/v1/flows/templates/tmpl_1";
const READ: &str = "/v1/flows/templates/tmpl_1?versions_per_template=10";
const RUNS: &str = "/v1/flows/templates/tmpl_1/runs";
const RUN: &str = "/v1/flows/templates/tmpl_1/runs/run1";

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("each_template_kind_maps_to_the_weft_type_that_carries_it", kinds),
        NodeTest::basic("each_value_goes_the_way_its_template_kind_takes_it", values),
        NodeTest::basic("every_mismatch_is_named_in_one_list", every_mismatch),
        NodeTest::basic("the_version_flagged_latest_runs_when_none_is_pinned", latest),
        NodeTest::fake(
            "every_input_goes_as_its_template_type_and_every_output_lands_on_its_port",
            every_kind,
        ),
        NodeTest::fake("a_fraction_on_a_whole_number_input_is_refused_before_anything_runs", fraction_refused),
        NodeTest::fake("an_array_of_images_sends_each_as_a_reference", array_of_images),
        NodeTest::fake("a_pinned_version_is_checked_against_its_own_ports_and_sent", pinned),
        NodeTest::fake("a_template_input_with_no_port_is_refused_before_anything_runs", missing_port),
        NodeTest::fake("a_port_the_template_does_not_take_is_refused", extra_port),
        NodeTest::fake("a_port_declared_with_a_type_that_does_not_fit_is_refused", wrong_type),
        NodeTest::fake("an_output_the_template_does_not_produce_is_refused", unknown_output),
        NodeTest::fake("an_unknown_pinned_version_is_refused_listing_the_runnable_ones", unknown_version),
        NodeTest::fake("a_template_with_no_runnable_version_is_refused", no_version),
        NodeTest::fake("a_failed_output_fails_the_run_naming_it", failed),
        NodeTest::fake("a_failed_run_comes_out_on_error_when_it_is_wired", failed_caught),
        NodeTest::fake("a_resumed_run_never_reads_the_template_or_starts_a_run_twice", resumed),
        NodeTest::live("one_real_run_of_a_prompt_to_image_template", "elevenlabs", live_run).with_fixture(
            fixture_spec(
                "ELEVENLABS_TEMPLATE_ID",
                "Template id",
                "A published Flows template in the connected workspace with one string input named `prompt` and \
                 one image output named `image`. The test runs it once, which bills that template's credits.",
            ),
        ),
    ]
}

fn ty(text: &str) -> WeftType {
    WeftType::parse(text).expect("a weft type")
}

fn port(id: &str, schema: Value) -> Value {
    json!({ "id": id, "content_schema": schema })
}

fn kinds() -> WeftResult<()> {
    for (kind, weft) in [
        ("string", "String"),
        ("voice", "String"),
        ("number", "Number"),
        ("integer", "Number"),
        ("boolean", "Boolean"),
        ("image", "Image"),
        ("video", "Video"),
        ("audio", "Audio"),
    ] {
        assert_eq!(weft_type(&json!({ "type": kind })), Ok(ty(weft)), "{kind}");
    }
    assert_eq!(weft_type(&json!({ "type": "array", "items": { "type": "image" } })), Ok(ty("List[Image]")));
    assert!(weft_type(&json!({ "type": "object", "properties": {} })).is_err(), "a run cannot be given an object");
    assert!(
        weft_type(&json!({ "type": "array", "items": { "type": "object" } })).is_err(),
        "nor a list of objects"
    );

    assert_eq!(output_type(&json!({ "type": "image" })), Ok(ty("Image")));
    let list = output_type(&json!({ "type": "array", "items": { "type": "image" } })).unwrap_err();
    assert!(list.starts_with("a list"), "no template hands a list back yet: {list}");
    assert!(output_type(&json!({ "type": "object" })).is_err(), "nor an object");
    Ok(())
}

fn values() -> WeftResult<()> {
    let photo = json!({ "__weft_image__": { "key": "k1" } });
    let side = json!({ "__weft_image__": { "key": "k2" } });
    // Each file goes as whatever the caller answers for it; here, an
    // asset named by the file's key.
    let mut reference = |file: FileHandle| -> WeftResult<Value> {
        match file {
            FileHandle::Key { key, .. } => Ok(json!({ "type": "asset", "asset_id": key })),
            other => panic!("a key-backed file: {other:?}"),
        }
    };
    let mut sent = |schema: Value, value: Value| input_value(&schema, &value, &mut reference);

    assert_eq!(sent(json!({ "type": "string" }), json!("a corgi"))?, json!("a corgi"));
    assert_eq!(
        sent(json!({ "type": "voice" }), json!("v1"))?,
        json!({ "type": "voice", "voice_id": "v1" })
    );
    assert_eq!(sent(json!({ "type": "number" }), json!(0.5))?, json!(0.5));
    assert_eq!(sent(json!({ "type": "integer" }), json!(3))?, json!(3));
    assert_eq!(sent(json!({ "type": "boolean" }), json!(true))?, json!(true));
    assert_eq!(sent(json!({ "type": "image" }), photo.clone())?, json!({ "type": "asset", "asset_id": "k1" }));
    assert_eq!(
        sent(json!({ "type": "array", "items": { "type": "image" } }), json!([photo, side]))?,
        json!([{ "type": "asset", "asset_id": "k1" }, { "type": "asset", "asset_id": "k2" }])
    );
    assert_eq!(
        sent(json!({ "type": "array", "items": { "type": "string" } }), json!(["a", "b"]))?,
        json!(["a", "b"])
    );
    assert_eq!(
        sent(json!({ "type": "array", "items": { "type": "string" } }), json!("a"))?,
        json!(["a"]),
        "a list port carrying one item sends a one-item list"
    );
    Ok(())
}

fn every_mismatch() -> WeftResult<()> {
    let inputs = vec![
        port("prompt", json!({ "type": "string" })),
        port("reference", json!({ "type": "image", "title": "Product photo" })),
        port("meta", json!({ "type": "object", "properties": {} })),
        port("count", json!({ "type": "integer" })),
    ];
    let declared = BTreeMap::from([
        ("prompt".to_string(), ty("String")),
        ("count".to_string(), ty("String")),
        ("style".to_string(), ty("String")),
    ]);
    let problems = input_mismatches(&inputs, &declared);
    assert_eq!(problems.len(), 4, "{problems:#?}");
    let says = |text: &str| problems.iter().any(|p| p.contains(text));
    assert!(says("`reference` (Product photo) has no port: declare `reference: Image`"), "{problems:#?}");
    assert!(says("`meta` is an object"), "{problems:#?}");
    assert!(says("`count` is declared `String`, which does not fit the template's integer input: declare `count: Number`"), "{problems:#?}");
    assert!(
        says("`style` is not an input of the template (its inputs: prompt: String, reference: Image, meta: object, count: Number)"),
        "{problems:#?}"
    );

    let outputs = vec![
        port("product_still", json!({ "type": "image" })),
        port("frames", json!({ "type": "array", "items": { "type": "image" } })),
    ];
    let declared = BTreeMap::from([
        ("product_still".to_string(), ty("String")),
        ("frames".to_string(), ty("List[Image]")),
        ("demo".to_string(), ty("Video")),
    ]);
    let problems = output_mismatches(&outputs, &declared);
    assert_eq!(problems.len(), 3, "{problems:#?}");
    let says = |text: &str| problems.iter().any(|p| p.contains(text));
    assert!(says("`product_still` is declared `String`, which the template's image output does not fit: declare `product_still: Image`"), "{problems:#?}");
    assert!(says("`frames` is a list"), "{problems:#?}");
    assert!(says("`demo` is not an output of the template (its outputs: product_still: Image, frames: List[Image])"), "{problems:#?}");
    Ok(())
}

fn latest() -> WeftResult<()> {
    let flagged = json!({ "versions": [
        { "version_id": "ver_3", "is_latest": false },
        { "version_id": "ver_2", "is_latest": true },
    ] });
    assert_eq!(pick_version("tmpl_1", &flagged, None)?["version_id"], "ver_2");
    let unflagged = json!({ "versions": [
        { "version_id": "ver_3", "is_latest": false },
        { "version_id": "ver_2", "is_latest": false },
    ] });
    assert_eq!(pick_version("tmpl_1", &unflagged, None)?["version_id"], "ver_3", "else the newest listed");
    Ok(())
}

// ----- Fake: canned template reads and runs ---------------------------

/// A template version with these ports.
fn version(id: &str, latest: bool, inputs: Vec<Value>, outputs: Vec<Value>) -> Value {
    json!({
        "version_id": id,
        "published_at_unix": 1_760_000_000,
        "is_latest": latest,
        "inputs": inputs,
        "outputs": outputs,
    })
}

/// A product-shot version: a prompt and a photo in, a still out.
fn product_shot(id: &str, latest: bool) -> Value {
    version(
        id,
        latest,
        vec![
            port("prompt", json!({ "type": "string", "title": "Prompt" })),
            port("reference", json!({ "type": "image", "title": "Product photo" })),
        ],
        vec![port("product_still", json!({ "type": "image", "title": "Final image" }))],
    )
}

/// The template read answers these versions, newest first.
fn template_has(rig: &FakeRig, versions: Vec<Value>) {
    rig.respond(
        "GET",
        READ,
        json!({ "id": "tmpl_1", "name": "Product shot", "has_more_versions": false, "versions": versions }),
    );
}

/// The ports a program running the product shot declares, and the
/// inputs it gives them.
fn product_shot_ports(rig: &FakeRig) -> Value {
    rig.input_type("prompt", ty("String"));
    rig.input_type("reference", ty("Image"));
    rig.output_type("product_still", ty("Image"));
    let photo = rig.store_file("photo.png", "image/png", b"photo".to_vec());
    inputs(rig, json!({ "prompt": "a corgi on a surfboard", "reference": photo }))
}

fn inputs(rig: &FakeRig, ports: Value) -> Value {
    let mut base = json!({ "account": rig.access("elevenlabs"), "template": "tmpl_1" });
    for (k, v) in ports.as_object().expect("the ports' values are an object") {
        base[k] = v.clone();
    }
    base
}

/// A completed image output, downloadable from its content URL.
fn image_done(id: &str) -> Value {
    json!({
        "type": "image",
        "id": id,
        "status": "completed",
        "content_url": format!("https://storage.googleapis.com/generations/{id}"),
        "content_mime_type": "image/png",
    })
}

/// Canned answers for a run that ends `status`: the create answers
/// `run1`, the wait ends on the status, and the read answers `outputs`.
fn run_ends(rig: &FakeRig, status: &str, outputs: Value) {
    rig.respond(
        "POST",
        RUNS,
        json!({ "id": "run1", "template_id": "tmpl_1", "version_id": "ver_1", "status": "pending", "outputs": {} }),
    );
    rig.signal(json!({ "status": status }));
    rig.respond(
        "GET",
        RUN,
        json!({ "id": "run1", "template_id": "tmpl_1", "version_id": "ver_1", "status": status, "outputs": outputs }),
    );
}

/// A run whose product still completes, and the still's download.
fn still_completes(rig: &FakeRig) {
    run_ends(rig, "completed", json!({ "product_still": image_done("g1") }));
    rig.respond_raw("GET", "/generations/g1", 200, "image/png", b"still".to_vec());
}

/// The JSON body the node started its run with.
fn run_body(rig: &FakeRig) -> Value {
    rig.requests()
        .into_iter()
        .find(|r| r.method == "POST" && r.path == RUNS)
        .and_then(|r| r.body)
        .expect("the node started a run")
}

fn started(rig: &FakeRig) -> bool {
    rig.requests().iter().any(|r| r.method == "POST" && r.path == RUNS)
}

async fn fraction_refused(rig: FakeRig) -> WeftResult<()> {
    template_has(&rig, vec![version("ver_1", true, vec![port("count", json!({ "type": "integer" }))], vec![])]);
    rig.input_type("count", ty("Number"));
    let err = rig.run(&ElevenLabsRunTemplateNode, inputs(&rig, json!({ "count": 2.5 }))).await.failure()?;
    assert!(err.starts_with("input error"), "{err}");
    assert!(err.contains("`count` takes whole numbers, and got 2.5"), "{err}");
    assert!(!started(&rig), "nothing runs");
    Ok(())
}

async fn every_kind(rig: FakeRig) -> WeftResult<()> {
    template_has(
        &rig,
        vec![version(
            "ver_1",
            true,
            vec![
                port("prompt", json!({ "type": "string" })),
                port("reference", json!({ "type": "image" })),
                port("narrator", json!({ "type": "voice" })),
                port("count", json!({ "type": "integer" })),
                port("tags", json!({ "type": "array", "items": { "type": "string" } })),
            ],
            vec![port("product_still", json!({ "type": "image" })), port("title", json!({ "type": "string" }))],
        )],
    );
    run_ends(
        &rig,
        "completed",
        json!({
            "product_still": image_done("g1"),
            "title": { "type": "string", "id": "g2", "status": "completed", "content": "Ride the wave." },
        }),
    );
    rig.respond_raw("GET", "/generations/g1", 200, "image/png", b"still".to_vec());
    for (name, weft) in [
        ("prompt", "String"),
        ("reference", "Image"),
        ("narrator", "String"),
        ("count", "Number"),
        ("tags", "List[String]"),
    ] {
        rig.input_type(name, ty(weft));
    }
    rig.output_type("product_still", ty("Image"));
    rig.output_type("title", ty("String"));
    let photo = rig.store_file("photo.png", "image/png", b"photo".to_vec());
    let outcome = rig
        .run(
            &ElevenLabsRunTemplateNode,
            inputs(
                &rig,
                json!({
                    "prompt": "a corgi on a surfboard",
                    "reference": photo,
                    "narrator": "v1",
                    // A wire carries a whole number as a float.
                    "count": 3.0,
                    "tags": ["a", "b"],
                }),
            ),
        )
        .await
        .ok()?;

    assert_eq!(
        run_body(&rig),
        json!({
            "inputs": {
                "prompt": "a corgi on a surfboard",
                "reference": inline("image/png", b"photo"),
                "narrator": { "type": "voice", "voice_id": "v1" },
                "count": 3,
                "tags": ["a", "b"],
            },
            "version_id": "ver_1",
        }),
        "nothing is pinned, yet the run names the latest version, the one whose ports were checked"
    );
    let (bytes, filename, mime) = emitted_file(&rig, &outcome.outputs, "product_still")?;
    assert_eq!(bytes, b"still");
    assert_eq!(filename, "image.png");
    assert_eq!(mime, "image/png");
    assert_eq!(outcome.output("title")?, &json!("Ride the wave."));

    // The wait parks on the run, looks every 10 seconds (a template can
    // make videos), and carries only the status.
    let awaited = rig.awaited_signals();
    assert_eq!(awaited.len(), 1, "one wait");
    assert_eq!(awaited[0].config["url"], json!(format!("https://api.elevenlabs.io{RUN}")));
    assert_eq!(awaited[0].config["interval_secs"], json!(10));
    assert_eq!(awaited[0].config["carry"], json!(["status"]));
    Ok(())
}

async fn array_of_images(rig: FakeRig) -> WeftResult<()> {
    template_has(
        &rig,
        vec![version(
            "ver_1",
            true,
            vec![port("shots", json!({ "type": "array", "items": { "type": "image" } }))],
            vec![port("product_still", json!({ "type": "image" }))],
        )],
    );
    still_completes(&rig);
    rig.input_type("shots", ty("List[Image]"));
    rig.output_type("product_still", ty("Image"));
    let front = rig.store_file("front.png", "image/png", b"front".to_vec());
    let side = rig.store_file("side.jpg", "image/jpeg", b"side".to_vec());
    rig.run(&ElevenLabsRunTemplateNode, inputs(&rig, json!({ "shots": [front, side] }))).await.ok()?;
    assert_eq!(
        run_body(&rig)["inputs"]["shots"],
        json!([inline("image/png", b"front"), inline("image/jpeg", b"side")])
    );
    Ok(())
}

async fn pinned(rig: FakeRig) -> WeftResult<()> {
    // The latest version also takes a photo, the pinned one only a
    // prompt: the node's one input port fits the pinned version alone.
    template_has(
        &rig,
        vec![
            product_shot("ver_2", true),
            version(
                "ver_1",
                false,
                vec![port("prompt", json!({ "type": "string" }))],
                vec![port("product_still", json!({ "type": "image" }))],
            ),
        ],
    );
    still_completes(&rig);
    rig.input_type("prompt", ty("String"));
    rig.output_type("product_still", ty("Image"));
    rig.run(&ElevenLabsRunTemplateNode, inputs(&rig, json!({ "prompt": "a corgi", "version": "ver_1" })))
        .await
        .ok()?;
    assert_eq!(run_body(&rig), json!({ "inputs": { "prompt": "a corgi" }, "version_id": "ver_1" }));
    Ok(())
}

async fn missing_port(rig: FakeRig) -> WeftResult<()> {
    template_has(&rig, vec![product_shot("ver_1", true)]);
    rig.input_type("prompt", ty("String"));
    rig.output_type("product_still", ty("Image"));
    // A program mistake: `_error` being wired does not catch it.
    rig.wire_output("_error");
    let err = rig
        .run(&ElevenLabsRunTemplateNode, inputs(&rig, json!({ "prompt": "a corgi" })))
        .await
        .failure()?;
    assert!(err.contains("`reference` (Product photo) has no port: declare `reference: Image`"), "{err}");
    assert!(!started(&rig), "nothing ran, so nothing was paid");
    Ok(())
}

async fn extra_port(rig: FakeRig) -> WeftResult<()> {
    template_has(&rig, vec![product_shot("ver_1", true)]);
    let mut given = product_shot_ports(&rig);
    rig.input_type("style", ty("String"));
    given["style"] = json!("noir");
    let err = rig.run(&ElevenLabsRunTemplateNode, given).await.failure()?;
    assert!(
        err.contains("the port `style` is not an input of the template (its inputs: prompt: String, reference: Image)"),
        "{err}"
    );
    assert!(!started(&rig));
    Ok(())
}

async fn wrong_type(rig: FakeRig) -> WeftResult<()> {
    template_has(&rig, vec![product_shot("ver_1", true)]);
    rig.input_type("prompt", ty("String"));
    rig.input_type("reference", ty("String"));
    rig.output_type("product_still", ty("Image"));
    let err = rig
        .run(&ElevenLabsRunTemplateNode, inputs(&rig, json!({ "prompt": "a corgi", "reference": "photo.png" })))
        .await
        .failure()?;
    assert!(
        err.contains(
            "the port `reference` is declared `String`, which does not fit the template's image input: declare \
             `reference: Image`"
        ),
        "{err}"
    );
    assert!(!started(&rig));
    Ok(())
}

async fn unknown_output(rig: FakeRig) -> WeftResult<()> {
    template_has(&rig, vec![product_shot("ver_1", true)]);
    let given = product_shot_ports(&rig);
    rig.output_type("demo", ty("Video"));
    let err = rig.run(&ElevenLabsRunTemplateNode, given).await.failure()?;
    assert!(
        err.contains("the port `demo` is not an output of the template (its outputs: product_still: Image)"),
        "{err}"
    );
    assert!(!started(&rig));
    Ok(())
}

async fn unknown_version(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "GET",
        READ,
        json!({
            "id": "tmpl_1",
            "name": "Product shot",
            "has_more_versions": true,
            "versions": [product_shot("ver_3", true), product_shot("ver_2", false)],
        }),
    );
    let mut given = product_shot_ports(&rig);
    given["version"] = json!("ver_0");
    let err = rig.run(&ElevenLabsRunTemplateNode, given).await.failure()?;
    assert!(err.contains("has no runnable version ver_0; pin one of ver_3, ver_2"), "{err}");
    assert!(err.contains("10 newest"), "an older version cannot be checked, and the refusal says why: {err}");
    assert!(!started(&rig));
    Ok(())
}

async fn no_version(rig: FakeRig) -> WeftResult<()> {
    template_has(&rig, vec![]);
    let given = product_shot_ports(&rig);
    let err = rig.run(&ElevenLabsRunTemplateNode, given).await.failure()?;
    assert!(err.contains("has no version the API can run"), "{err}");
    assert!(err.contains("a model it does not serve through the API"), "the refusal says why: {err}");
    assert!(!started(&rig));
    Ok(())
}

async fn failed(rig: FakeRig) -> WeftResult<()> {
    template_has(&rig, vec![product_shot("ver_1", true)]);
    run_ends(
        &rig,
        "failed",
        json!({ "product_still": {
            "type": "image",
            "id": "g1",
            "status": "failed",
            "failure_reason": "moderated",
            "error_message": "The prompt was rejected by content moderation. You were not charged for this generation.",
        } }),
    );
    let given = product_shot_ports(&rig);
    let err = rig.run(&ElevenLabsRunTemplateNode, given).await.failure()?;
    assert!(err.contains("product_still: moderated: The prompt was rejected by content moderation."), "{err}");
    assert!(err.contains("run1"), "the error names the run: {err}");
    Ok(())
}

async fn failed_caught(rig: FakeRig) -> WeftResult<()> {
    template_has(&rig, vec![product_shot("ver_1", true)]);
    run_ends(
        &rig,
        "failed",
        json!({ "product_still": {
            "type": "image",
            "id": "g1",
            "status": "failed",
            "failure_reason": "timeout",
            "error_message": "Timed out while processing.",
        } }),
    );
    rig.wire_output("_error");
    let given = product_shot_ports(&rig);
    let outcome = rig.run(&ElevenLabsRunTemplateNode, given).await.ok()?;
    let error = outcome.output("_error")?.as_str().expect("error is a string").to_string();
    assert!(error.contains("product_still: timeout"), "{error}");
    assert!(!outcome.outputs.contains_key("product_still"), "a caught failure emits no still");
    Ok(())
}

async fn resumed(rig: FakeRig) -> WeftResult<()> {
    template_has(&rig, vec![product_shot("ver_1", true)]);
    still_completes(&rig);
    let given = product_shot_ports(&rig);
    rig.run(&ElevenLabsRunTemplateNode, given.clone()).await.ok()?;
    rig.run(&ElevenLabsRunTemplateNode, given).await.ok()?;
    let sent = rig.requests();
    let reads = sent.iter().filter(|r| r.method == "GET" && r.path == TEMPLATE).count();
    let runs = sent.iter().filter(|r| r.method == "POST" && r.path == RUNS).count();
    assert_eq!(reads, 1, "the resumed body finds the job in its state, so it never reads the template again");
    assert_eq!(runs, 1, "and never pays for a second run");
    assert_eq!(rig.awaited_signals().len(), 1, "nor waits again on a job whose end it already has");
    Ok(())
}

/// Runs a real prompt-to-image template once, which bills whatever
/// that template's generations cost. Nothing stays on the account.
async fn live_run(rig: LiveRig) -> WeftResult<()> {
    let template = rig.fixture("ELEVENLABS_TEMPLATE_ID")?;
    rig.input_type("prompt", ty("String"));
    rig.output_type("image", ty("Image"));
    let outcome = rig
        .run(
            &ElevenLabsRunTemplateNode,
            json!({
                "account": rig.access("elevenlabs"),
                "template": template,
                "prompt": "a single red circle on a white background",
            }),
        )
        .await
        .ok()?;
    let file = weft::storage::StoredFile::from_value(outcome.output("image")?)?;
    assert!(file.mime_type.starts_with("image/"), "{}", file.mime_type);
    assert!(file.size_bytes > 1_000, "a real image is more than a kilobyte: {}", file.size_bytes);
    Ok(())
}
