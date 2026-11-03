//! ElevenLabsDub self-tests: the project create, the language target,
//! the parked wait and the stored dub, against canned answers.

use serde_json::{json, Value};

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use crate::testing::emitted_file;

use super::{ElevenLabsDubNode, Settings};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("creates_the_project_with_its_language_waits_stores_the_dub_and_owes_the_project_delete", dubs),
        NodeTest::fake("a_project_the_program_reads_is_kept", kept),
        NodeTest::fake("every_term_and_the_cloning_strength_reach_the_requests", every_setting),
        NodeTest::fake("a_failed_dub_fails_with_elevenlabs_message", failed),
        NodeTest::fake("a_failed_project_is_named_with_its_own_cause", failed_project),
        NodeTest::fake("a_failed_dub_comes_out_on_error_when_it_is_wired", failed_caught),
        NodeTest::fake("a_language_that_cannot_be_added_owes_the_paid_project_delete", language_refused),
        NodeTest::fake("a_language_that_cannot_be_added_leaves_a_kept_project_named", language_refused_kept),
        NodeTest::fake("a_create_that_names_no_language_owes_the_paid_project_delete", no_language_named),
        NodeTest::fake("a_wait_given_up_owes_the_project_delete", given_up),
        NodeTest::fake("a_kept_project_survives_a_wait_given_up", given_up_kept),
        NodeTest::fake("a_replacement_voice_is_logged_and_the_dub_still_lands", replacement_voice),
        NodeTest::fake("a_warning_from_preparing_the_source_is_logged_too", preparation_warning),
        NodeTest::fake("a_resumed_run_never_creates_a_second_project", resumed),
        NodeTest::fake("an_unknown_status_fails_loud", unknown_status),
        NodeTest::fake("a_link_goes_instead_of_a_file_and_names_the_dub", source_url),
        NodeTest::fake("a_transcript_with_translations_starts_its_language_on_the_create", transcript_and_reference),
        NodeTest::basic("every_setting_rides_the_create_and_nothing_nobody_gave", create_fields),
        NodeTest::fake("translations_with_a_cloning_strength_are_refused_before_anything_is_sent", translations_with_cloning),
        NodeTest::live("one_real_short_dub_to_spanish", "elevenlabs", live_dub),
    ]
}

const TARGET_PATH: &str = "/v1/dubbing/project/p1/language/l1";
const PROJECT_PATH: &str = "/v1/dubbing/project/p1";

/// The inputs of a dub of `talk.mp3` into Spanish, plus `extra`.
fn inputs(rig: &FakeRig, extra: Value) -> Value {
    let mut base = json!({
        "account": rig.access("elevenlabs"),
        "file": rig.store_file("talk.mp3", "audio/mpeg", b"english speech".to_vec()),
        "targetLang": "es",
    });
    for (k, v) in extra.as_object().expect("extra inputs are an object") {
        base[k] = v.clone();
    }
    base
}

/// Canned answers up to the wait: the create answers project `p1` with
/// its target `l1` (the language rides on the create), adding a language
/// on its own also answers `l1`, and the wait ends on `status`.
fn starts(rig: &FakeRig, status: &str) {
    rig.respond(
        "POST",
        "/v1/dubbing/project",
        json!({ "project_id": "p1", "status": "queued", "model_id": "dubbing_v1", "language_ids": ["l1"] }),
    );
    rig.respond(
        "POST",
        "/v1/dubbing/project/p1/language",
        json!({ "language_id": "l1", "project_id": "p1", "target_language": "es", "status": "queued" }),
    );
    rig.signal(json!({ "status": status }));
}

/// Whether weft owes the project's delete when the node ends, however it
/// ends, through the node's own access.
fn owes_delete(rig: &FakeRig) -> bool {
    let project = format!("https://api.elevenlabs.io{PROJECT_PATH}");
    rig.owed_calls().iter().any(|(call, access)| {
        call.url == project
            && call.method == weft::signal::CancelMethod::Delete
            && call.when == weft::signal::SendWhen::Always
            && access.is_some()
    })
}

/// Canned answers for a dub that completes with `warnings`, its FLAC
/// served at a signed storage URL with a generic content type.
fn completes(rig: &FakeRig, warnings: Value) {
    completes_with(rig, warnings, json!([]));
}

/// A completed dub whose target carries `warnings` and whose project
/// carries `project_warnings` (what came up preparing the source).
fn completes_with(rig: &FakeRig, warnings: Value, project_warnings: Value) {
    starts(rig, "completed");
    rig.respond(
        "GET",
        TARGET_PATH,
        json!({
            "language_id": "l1",
            "project_id": "p1",
            "target_language": "es",
            "status": "completed",
            "outputs": { "lossless_audio": "https://storage.googleapis.com/dubs/l1.flac" },
            "error": null,
            "warnings": warnings,
        }),
    );
    rig.respond(
        "GET",
        PROJECT_PATH,
        json!({ "project_id": "p1", "status": "ready", "warnings": project_warnings }),
    );
    rig.respond_raw("GET", "/dubs/l1.flac", 200, "application/octet-stream", b"fLaC spanish".to_vec());
}

/// Canned answers for a dub target that ends `failed` with `error`.
fn fails(rig: &FakeRig, error: Value) {
    starts(rig, "failed");
    rig.respond(
        "GET",
        TARGET_PATH,
        json!({ "language_id": "l1", "project_id": "p1", "status": "failed", "error": error, "warnings": [] }),
    );
}

/// The create the node sent. Its form streams (the recording goes out
/// of storage as it is sent), so the rig cannot read it: the fields are
/// checked on [`Settings::form`] instead.
fn create(rig: &FakeRig) -> weft::SentRequest {
    rig.requests()
        .into_iter()
        .find(|r| r.method == "POST" && r.path == "/v1/dubbing/project")
        .expect("the node sent a multipart create")
}

/// The fields `settings` puts on the create, with `source_url` when given.
fn fields(settings: &Settings<'_>, source_url: Option<&str>) -> WeftResult<String> {
    let (_, body) = settings.form(source_url)?.build();
    Ok(String::from_utf8_lossy(&body).into_owned())
}

/// A create of a Spanish dub on weft's default model, with nothing else.
fn plain() -> Settings<'static> {
    Settings {
        model: "dubbing_v1",
        source_language: None,
        target_language: Some("es"),
        keyterms: &[],
        transcript: None,
        reference: None,
    }
}

/// The JSON body of the language POST.
fn language_body(rig: &FakeRig) -> Value {
    rig.requests()
        .into_iter()
        .find(|r| r.method == "POST" && r.path == "/v1/dubbing/project/p1/language")
        .and_then(|r| r.body)
        .expect("the node added a language with a JSON body")
}

fn language_posts(rig: &FakeRig) -> usize {
    rig.requests().iter().filter(|r| r.method == "POST" && r.path == "/v1/dubbing/project/p1/language").count()
}

/// Whether the multipart `form` holds a text field `name` set to `value`.
fn has_field(form: &str, name: &str, value: &str) -> bool {
    form.contains(&format!("name=\"{name}\"\r\n\r\n{value}\r\n"))
}

async fn dubs(rig: FakeRig) -> WeftResult<()> {
    completes(&rig, json!([]));
    let outcome = rig.run(&ElevenLabsDubNode, inputs(&rig, json!({ "sourceLang": "en" }))).await.ok()?;

    assert!(create(&rig).body_streamed, "the recording streams out of storage, never held whole");
    assert_eq!(language_posts(&rig), 0, "no language added on its own");

    // The wait parks on the language target, signed by the connection,
    // ends on anything not in flight, and carries only the status.
    let awaited = rig.awaited_signals();
    assert_eq!(awaited.len(), 1, "one wait");
    assert_eq!(awaited[0].kind, "poll_endpoint");
    assert_eq!(awaited[0].config["url"], json!(format!("https://api.elevenlabs.io{TARGET_PATH}")));
    assert_eq!(awaited[0].config["interval_secs"], json!(10));
    assert_eq!(awaited[0].config["carry"], json!(["status"]));
    assert!(awaited[0].access.is_some(), "each look is signed by the ElevenLabs connection");
    assert_eq!(
        awaited[0].match_predicates,
        vec![
            weft::signal::Predicate::neq("status", "queued"),
            weft::signal::Predicate::neq("status", "processing"),
        ]
    );

    let (bytes, filename, mime) = emitted_file(&rig, &outcome.outputs, "file")?;
    assert_eq!(bytes, b"fLaC spanish");
    assert_eq!(filename, "dubbed_es_talk.flac");
    assert_eq!(mime, "audio/flac", "stored as FLAC, never as the download's generic header");
    assert!(owes_delete(&rig), "nothing reads `projectId`, so the project goes once the node ends");
    assert!(!rig.requests().iter().any(|r| r.method == "DELETE"), "the node deletes nothing itself");
    Ok(())
}

/// The create's fields: the model, the languages, each term as its own
/// field, the transcript as a JSON file, the reference and the link, and
/// nothing nobody gave.
fn create_fields() -> WeftResult<()> {
    let form = fields(&Settings { source_language: Some("en"), ..plain() }, None)?;
    assert!(has_field(&form, "model_id", "dubbing_v1"), "weft's default model: {form}");
    assert!(has_field(&form, "source_language", "en"), "{form}");
    assert!(
        has_field(&form, "target_language", "es"),
        "the language rides on the create, so one ElevenLabs does not dub is refused before any charge: {form}"
    );
    assert!(!form.contains("name=\"keyterms\""), "no term nobody gave: {form}");
    assert!(!form.contains("name=\"source_url\""), "{form}");

    let terms = ["Weavemind".to_string(), "weft".to_string()];
    let form = fields(&Settings { model: "dubbing_v2", target_language: None, keyterms: &terms, ..plain() }, None)?;
    assert!(has_field(&form, "model_id", "dubbing_v2"), "{form}");
    assert_eq!(form.matches("name=\"keyterms\"").count(), 2, "one field per term: {form}");
    assert!(has_field(&form, "keyterms", "Weavemind") && has_field(&form, "keyterms", "weft"), "{form}");
    assert!(!form.contains("name=\"source_language\""), "no language given is detected: {form}");
    assert!(
        !form.contains("name=\"target_language\""),
        "a cloning strength can only ride on a language added on its own: {form}"
    );

    let transcript = json!({ "segments": [{ "start": 0.0, "end": 1.5, "text": "Hello.", "translation": "Hola." }] });
    let link = "https://example.com/media/talk.mp4?sig=abc";
    let form = fields(&Settings { transcript: Some(&transcript), reference: Some("episode-12"), ..plain() }, Some(link))?;
    assert!(
        form.contains("name=\"transcript\"; filename=\"transcript.json\"\r\nContent-Type: application/json"),
        "ElevenLabs takes the transcript as a JSON file: {form}"
    );
    let document = form.split("filename=\"transcript.json\"").nth(1).expect("a transcript part");
    assert!(document.contains(r#""translation":"Hola.""#), "the document goes as given: {form}");
    assert!(has_field(&form, "reference", "episode-12"), "{form}");
    assert!(has_field(&form, "source_url", link), "ElevenLabs fetches the link itself: {form}");
    Ok(())
}

async fn kept(rig: FakeRig) -> WeftResult<()> {
    completes(&rig, json!([]));
    rig.wire_output("projectId");
    let outcome = rig.run(&ElevenLabsDubNode, inputs(&rig, json!({}))).await.ok()?;
    assert_eq!(outcome.outputs["projectId"], json!("p1"));
    assert!(!owes_delete(&rig), "a project the program reads is the program's to keep");
    Ok(())
}

async fn every_setting(rig: FakeRig) -> WeftResult<()> {
    completes(&rig, json!([]));
    rig.run(
        &ElevenLabsDubNode,
        inputs(
            &rig,
            json!({
                "model": "dubbing_v2",
                "keyterms": ["Weavemind", "weft"],
                // A wired number arrives as a float.
                "cloningStrength": 3.0,
            }),
        ),
    )
    .await
    .ok()?;
    assert_eq!(
        language_body(&rig),
        json!({ "target_language": "es", "voice_settings": { "cloning_strength": 3 } }),
        "the strength goes as a whole number"
    );
    Ok(())
}

async fn failed(rig: FakeRig) -> WeftResult<()> {
    let message = "The transcript could not be translated.";
    fails(&rig, json!({ "code": "translation_failed", "message": message, "retryable": false }));
    let err = rig.run(&ElevenLabsDubNode, inputs(&rig, json!({}))).await.failure()?;
    assert!(err.contains("translation_failed"), "{err}");
    assert!(err.contains(message), "{err}");
    assert!(err.contains("p1"), "the error names the project: {err}");
    Ok(())
}

async fn failed_project(rig: FakeRig) -> WeftResult<()> {
    fails(&rig, json!({ "code": "project_failed", "message": "The project failed.", "retryable": false }));
    rig.respond(
        "GET",
        "/v1/dubbing/project/p1",
        json!({
            "project_id": "p1",
            "status": "failed",
            "error": { "code": "no_speech", "message": "No speech was found in the file.", "retryable": false },
        }),
    );
    let err = rig.run(&ElevenLabsDubNode, inputs(&rig, json!({}))).await.failure()?;
    assert!(err.contains("no_speech"), "{err}");
    assert!(err.contains("No speech was found in the file."), "the project's own cause: {err}");
    assert!(err.contains("p1"), "{err}");
    Ok(())
}

async fn failed_caught(rig: FakeRig) -> WeftResult<()> {
    fails(&rig, json!({ "code": "translation_failed", "message": "The transcript could not be translated." }));
    rig.wire_output("_error");
    let outcome = rig.run(&ElevenLabsDubNode, inputs(&rig, json!({}))).await.ok()?;
    let error = outcome.output("_error")?.as_str().expect("error is a string").to_string();
    assert!(error.contains("translation_failed"), "{error}");
    assert!(!outcome.outputs.contains_key("file"), "a caught failure emits no file");
    Ok(())
}

/// A language added on its own (a cloning strength is set) that
/// ElevenLabs refuses: the canned answers and the inputs.
fn refuse_the_language(rig: &FakeRig) -> Value {
    rig.respond("POST", "/v1/dubbing/project", json!({ "project_id": "p1", "status": "queued", "language_ids": [] }));
    rig.respond_status(
        "POST",
        "/v1/dubbing/project/p1/language",
        422,
        json!({ "detail": { "message": "Unsupported target language: xx" } }),
    );
    inputs(rig, json!({ "targetLang": "xx", "model": "dubbing_v2", "cloningStrength": 5 }))
}

async fn language_refused(rig: FakeRig) -> WeftResult<()> {
    let given = refuse_the_language(&rig);
    let err = rig.run(&ElevenLabsDubNode, given).await.failure()?;
    assert!(err.contains("422"), "{err}");
    assert!(err.contains("Unsupported target language: xx"), "{err}");
    assert!(err.contains("p1") && err.contains("it is deleted"), "the paid project is named: {err}");
    assert!(owes_delete(&rig), "nothing reads `projectId`, so the project goes");
    assert!(rig.awaited_signals().is_empty(), "a target that was never added is never waited on");
    Ok(())
}

async fn language_refused_kept(rig: FakeRig) -> WeftResult<()> {
    let given = refuse_the_language(&rig);
    rig.wire_output("projectId");
    let err = rig.run(&ElevenLabsDubNode, given).await.failure()?;
    assert!(err.contains("p1") && err.contains("stays on the account"), "{err}");
    assert!(!owes_delete(&rig), "a project the program reads is the program's to keep");
    Ok(())
}

async fn no_language_named(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v1/dubbing/project", json!({ "project_id": "p1", "status": "queued", "language_ids": [] }));
    let err = rig.run(&ElevenLabsDubNode, inputs(&rig, json!({}))).await.failure()?;
    assert!(err.contains("did not say it started the dub into 'es'"), "{err}");
    assert!(err.contains("p1") && err.contains("it is deleted"), "{err}");
    assert!(owes_delete(&rig), "nothing reads `projectId`, so the paid project goes");
    assert!(rig.awaited_signals().is_empty(), "nothing to wait on");
    Ok(())
}

/// A create with its language, then a wait that gives up.
fn gives_up(rig: &FakeRig) {
    rig.respond(
        "POST",
        "/v1/dubbing/project",
        json!({ "project_id": "p1", "status": "queued", "model_id": "dubbing_v1", "language_ids": ["l1"] }),
    );
    rig.signal_given_up();
}

async fn given_up(rig: FakeRig) -> WeftResult<()> {
    gives_up(&rig);
    let err = rig.run(&ElevenLabsDubNode, inputs(&rig, json!({}))).await.failure()?;
    assert!(err.contains("given up"), "{err}");
    assert!(owes_delete(&rig), "nothing will read the dub, so its project goes");
    Ok(())
}

async fn given_up_kept(rig: FakeRig) -> WeftResult<()> {
    gives_up(&rig);
    rig.wire_output("projectId");
    let err = rig.run(&ElevenLabsDubNode, inputs(&rig, json!({}))).await.failure()?;
    assert!(err.contains("given up"), "{err}");
    assert!(!owes_delete(&rig), "a project the program reads is the program's to keep");
    Ok(())
}

async fn replacement_voice(rig: FakeRig) -> WeftResult<()> {
    completes(
        &rig,
        json!([{
            "type": "voices_not_permitted",
            "speaker_ids": ["speaker_0"],
            "message": "Cloning this speaker's voice is not permitted.",
        }]),
    );
    let outcome = rig.run(&ElevenLabsDubNode, inputs(&rig, json!({}))).await.ok()?;
    let (bytes, _, _) = emitted_file(&rig, &outcome.outputs, "file")?;
    assert_eq!(bytes, b"fLaC spanish", "the dub still lands");
    let logs = rig.logs();
    assert_eq!(logs.len(), 1, "{logs:?}");
    assert_eq!(logs[0].0, weft::context::LogLevel::Warn);
    assert!(logs[0].1.contains("speaker_0"), "{}", logs[0].1);
    assert!(logs[0].1.contains("Cloning this speaker's voice is not permitted."), "{}", logs[0].1);
    Ok(())
}

async fn preparation_warning(rig: FakeRig) -> WeftResult<()> {
    completes_with(
        &rig,
        json!([]),
        json!([{ "type": "voices_not_permitted", "speaker_ids": ["speaker_1"], "message": "Not permitted." }]),
    );
    rig.run(&ElevenLabsDubNode, inputs(&rig, json!({}))).await.ok()?;
    let logs = rig.logs();
    assert_eq!(logs.len(), 1, "{logs:?}");
    assert!(logs[0].1.contains("speaker_1"), "{}", logs[0].1);
    Ok(())
}

async fn resumed(rig: FakeRig) -> WeftResult<()> {
    completes(&rig, json!([]));
    let given = inputs(&rig, json!({}));
    rig.run(&ElevenLabsDubNode, given.clone()).await.ok()?;
    rig.run(&ElevenLabsDubNode, given).await.ok()?;
    let posts = |path: &str| rig.requests().iter().filter(|r| r.method == "POST" && r.path == path).count();
    assert_eq!(posts("/v1/dubbing/project"), 1, "the resumed body finds the project in its state instead of paying twice");
    assert_eq!(rig.awaited_signals().len(), 1, "and does not wait again on a dub whose end it already has");
    let saved = rig.saved_state(&ElevenLabsDubNode).expect("a saved state");
    assert!(saved["dubbed"].is_object(), "the stored dub is in the state, so the project is never read again: {saved}");
    let reads = rig.requests().iter().filter(|r| r.method == "GET" && r.path == TARGET_PATH).count();
    assert_eq!(reads, 1, "the finished dub was read once");
    Ok(())
}

async fn unknown_status(rig: FakeRig) -> WeftResult<()> {
    starts(&rig, "stale");
    rig.respond("GET", TARGET_PATH, json!({ "language_id": "l1", "project_id": "p1", "status": "stale" }));
    let err = rig.run(&ElevenLabsDubNode, inputs(&rig, json!({}))).await.failure()?;
    assert!(err.contains("unexpected status 'stale'"), "{err}");
    assert!(err.contains("p1"), "{err}");
    Ok(())
}

async fn source_url(rig: FakeRig) -> WeftResult<()> {
    completes(&rig, json!([]));
    let mut given = inputs(&rig, json!({ "sourceUrl": "https://example.com/media/talk.mp4?sig=abc" }));
    given.as_object_mut().expect("inputs are an object").remove("file");
    let outcome = rig.run(&ElevenLabsDubNode, given).await.ok()?;
    let (_, filename, _) = emitted_file(&rig, &outcome.outputs, "file")?;
    assert_eq!(filename, "dubbed_es_talk.flac", "named after the link's last path segment");
    Ok(())
}

/// A transcript with translations seeds the language the create starts,
/// so a dub that carries one runs without adding a language on its own.
async fn transcript_and_reference(rig: FakeRig) -> WeftResult<()> {
    completes(&rig, json!([]));
    rig.wire_output("projectId");
    let transcript = json!({ "segments": [{ "start": 0.0, "end": 1.5, "text": "Hello.", "translation": "Hola." }] });
    rig.run(
        &ElevenLabsDubNode,
        inputs(&rig, json!({ "sourceLang": "en", "transcript": transcript, "reference": "episode-12" })),
    )
    .await
    .ok()?;
    assert_eq!(language_posts(&rig), 0, "the translations seed the language the create starts");
    Ok(())
}

async fn translations_with_cloning(rig: FakeRig) -> WeftResult<()> {
    let transcript = json!({ "segments": [{ "start": 0.0, "end": 1.0, "text": "Hi.", "translation": "Hola." }] });
    let given = inputs(
        &rig,
        json!({ "sourceLang": "en", "transcript": transcript, "model": "dubbing_v2", "cloningStrength": 5 }),
    );
    let err = rig.run(&ElevenLabsDubNode, given).await.failure()?;
    assert!(err.starts_with("input error") && err.contains("translations"), "{err}");
    assert!(rig.requests().is_empty(), "nothing is sent, so nothing is paid for");
    Ok(())
}

/// The cheapest real dub: a couple of seconds of English speech into
/// Spanish on Dubbing v1 (expect a few minutes of processing). Nothing
/// reads `projectId`, so weft owes its delete when the node ends; the
/// rig records that call rather than sending it, so the test deletes the
/// project itself, and repeated runs never pile projects up.
async fn live_dub(rig: LiveRig) -> WeftResult<()> {
    let conn = rig.connect().await?;
    let voice = crate::testing::first_voice_id(&conn).await?;
    let sample = crate::testing::speech_sample(&conn, &voice, "Hello, this is a test.").await?;
    let file = rig.store_file("english.mp3", "audio/mpeg", sample).await?;
    let outcome = rig
        .run(
            &ElevenLabsDubNode,
            json!({
                "account": rig.access("elevenlabs"),
                "file": file,
                "model": "dubbing_v1",
                "targetLang": "es",
                "sourceLang": "en",
            }),
        )
        .await
        .ok()?;
    let project = outcome.output("projectId")?.as_str().expect("project id").to_string();
    let url = format!("{}/dubbing/project/{project}", crate::elevenlabs::API);
    crate::testing::delete(&conn, &url, "delete the test dub project").await?;
    assert_eq!(outcome.output("mimeType")?, &json!("audio/flac"));
    let filename = outcome.output("filename")?.as_str().unwrap_or_default().to_string();
    assert_eq!(filename, "dubbed_es_english.flac");
    let size = outcome.output("sizeBytes")?.as_f64().unwrap_or(0.0);
    assert!(size > 1_000.0, "the dubbed clip is real audio: {size}");
    Ok(())
}
