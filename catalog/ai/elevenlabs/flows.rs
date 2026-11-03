//! The Image & Video API the generation nodes share: turning the
//! graph's files into ElevenLabs media references, submitting a job,
//! waiting for it without holding a worker, and storing what it made.
//!
//! ElevenLabs runs every image, video and template run as a job. The
//! create answers an id at once (`pending`), the job moves to
//! `generating`, and it ends `completed` (with a signed `content_url`
//! that lasts about an hour) or `failed` (with a `failure_reason`; a
//! failed job is not charged). Parameters a model does not take never
//! start a job: the create itself answers 422, before anything is paid.

use std::future::Future;

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use weft::access::client::{get_json, json_call, post_json, url, Multipart, StoredBodyExt};
use weft::signal::{CancelCall, PollEndpoint, Predicate};
use weft::storage::{FileHandle, StorageScope};
use weft::{node_error, Access, Kept, RunContext, NodeErrExt, WeftResult};

use super::elevenlabs::API;

type Client = weft::reqwest_middleware::ClientWithMiddleware;

/// The largest file ElevenLabs takes inline in a request (25 MB once
/// decoded). A bigger one goes up as a temporary asset instead.
const INLINE_MAX_BYTES: u64 = 25_000_000;

/// What a generation route makes.
#[derive(Clone, Copy)]
pub enum Media {
    Image,
    Video,
}

impl Media {
    /// The route segment, and the kind of file the output is.
    fn noun(self) -> &'static str {
        match self {
            Media::Image => "image",
            Media::Video => "video",
        }
    }

    /// Seconds between two looks at a running job. ElevenLabs asks for
    /// no more than one look every 2 seconds on an image (weft's floor
    /// is 5) and every 10 on a video.
    fn poll_secs(self) -> u64 {
        match self {
            Media::Image => 5,
            Media::Video => 10,
        }
    }
}

/// The type ElevenLabs takes inline for a file of `mime` and `len`
/// bytes, spelled the way the API spells it, or `None` when the file
/// has to go up as an asset (a type outside the inline list, or more
/// than 25 MB).
pub fn inline_mime(mime: &str, len: u64) -> Option<&'static str> {
    if len > INLINE_MAX_BYTES {
        return None;
    }
    let bare = mime.split(';').next().unwrap_or_default().trim().to_ascii_lowercase();
    Some(match bare.as_str() {
        "image/jpeg" | "image/jpg" | "image/pjpeg" => "image/jpeg",
        "image/png" => "image/png",
        "image/webp" => "image/webp",
        "image/heic" => "image/heic",
        "image/heif" => "image/heif",
        "audio/mpeg" | "audio/mp3" => "audio/mpeg",
        "audio/wav" | "audio/x-wav" | "audio/wave" | "audio/vnd.wave" => "audio/wav",
        "video/mp4" => "video/mp4",
        "video/quicktime" => "video/quicktime",
        "video/webm" => "video/webm",
        _ => return None,
    })
}

/// The stored name for an output of `kind` (`image`, `video`, `audio`)
/// served as `mime`: `image.png`, `video.mp4`, `audio.mp3`.
pub fn output_filename(kind: &str, mime: &str) -> String {
    let subtype = mime
        .split(';')
        .next()
        .and_then(|m| m.split('/').nth(1))
        .and_then(|s| s.split('+').next())
        .map(str::trim)
        .unwrap_or_default();
    let ext = match subtype {
        "jpeg" => "jpg",
        "quicktime" => "mov",
        "mpeg" if kind == "audio" => "mp3",
        "" => "bin",
        other => other,
    };
    format!("{kind}.{ext}")
}

/// Set `key` on a request body when the person set it. Every model
/// refuses a field it does not take, so a setting nobody gave never
/// travels.
pub fn set_some<T: Serialize>(body: &mut Value, key: &str, value: Option<T>) {
    if let Some(v) = value {
        body[key] = json!(v);
    }
}

/// Turns the graph's files into the media references one request
/// carries. A file ElevenLabs does not take inline goes up as a
/// temporary asset, whose delete weft owes from then on, however the node
/// ends (`ctx.after_node`): a job reads its assets until it ends, and the
/// node ends after the job.
pub struct References<'a> {
    ctx: &'a RunContext,
    http: &'a Client,
    account: &'a Access,
}

impl<'a> References<'a> {
    pub fn new(ctx: &'a RunContext, http: &'a Client, account: &'a Access) -> Self {
        Self { ctx, http, account }
    }

    /// The reference for one file: its bytes inline when ElevenLabs
    /// takes it that way, otherwise an asset uploaded to the workspace
    /// for this job and named by its id.
    pub async fn of(&self, file: &FileHandle) -> WeftResult<Value> {
        let storage = self.ctx.storage(StorageScope::Run);
        let body = storage.body(file).await?;
        if let Some(mime) = inline_mime(body.mime_type(), body.len()) {
            // Inline, the bytes go base64 inside the JSON, so they are read
            // whole (25 MB at most).
            drop(body);
            let (_, bytes) = storage.get_bytes(file).await?;
            return Ok(json!({
                "type": "inline_base64",
                "mime_type": mime,
                "content_base64": base64::engine::general_purpose::STANDARD.encode(&bytes),
            }));
        }
        let name = body.filename().to_string();
        let form = Multipart::form_data().text("name", &name).stored("asset", body);
        let answer = json_call(
            self.http.post(url(API, &["assets"])?.as_str()).multipart(form),
            &format!("upload '{name}' to ElevenLabs as a temporary asset"),
        )
        .await?;
        let id = answer["asset_id"].as_str().node_err("ElevenLabs answered no asset_id for the uploaded asset")?;
        self.ctx.after_node(CancelCall::delete(url(API, &["assets", id])?).always(), self.account).await?;
        Ok(json!({ "type": "asset", "asset_id": id }))
    }

    /// Each file's reference, in order.
    pub async fn of_each(&self, files: &[FileHandle]) -> WeftResult<Vec<Value>> {
        let mut refs = Vec::with_capacity(files.len());
        for file in files {
            refs.push(self.of(file).await?);
        }
        Ok(refs)
    }
}

/// Where a job stands, the state of a node that runs one: nothing yet,
/// accepted (its id), or ended (the status the wait ended on). A run
/// that pauses while ElevenLabs works comes back with the job, so the
/// paid create is never sent twice.
#[derive(Default, Serialize, Deserialize)]
pub struct Job {
    submitted: Option<String>,
    ended: Option<Value>,
}

/// Build a request with `build` (which turns files into references)
/// and POST it to `url`, once: a resumed body finds the job's id in its
/// state instead of paying for a second one. `no_id` is the refusal when
/// the answer names no job.
pub async fn submit<F, Fut>(state: &mut Kept<Job>, http: &Client, url: &str, what: &str, no_id: &str, build: F) -> WeftResult<String>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = WeftResult<Value>>,
{
    state
        .once(|s| &mut s.submitted, async |_| {
            let body = build().await?;
            let answer = post_json(http, url, &body, what).await?;
            Ok(answer["id"].as_str().node_err(no_id)?.to_string())
        })
        .await
}

/// Park until the job at `url` leaves `pending` and `generating`,
/// without holding a worker, and answer the job read again now.
///
/// The wait carries only `status`; the read after it carries the rest,
/// with content URLs signed now rather than whenever the wait ended,
/// and none of them lands in the state. Any status that is not in
/// flight ends the wait, so one this package does not know is refused
/// by the caller instead of waited on for ever.
pub async fn finish(state: &mut Kept<Job>, account: &Access, http: &Client, url: &str, poll_secs: u64) -> WeftResult<Value> {
    let poll = PollEndpoint {
        url: url.to_string(),
        interval_secs: poll_secs,
        access: Some(weft::primitive::AccessRef::from(account)),
        filters: vec![Predicate::neq("status", "pending"), Predicate::neq("status", "generating")],
        carry: vec!["status".into()],
        ..Default::default()
    };
    state.wait(|s| &mut s.ended, poll).await?;
    get_json(http, url, "read the finished ElevenLabs job").await
}

/// The failure ElevenLabs reported on a finished job or output.
pub fn failure(what: &str, job: &Value) -> weft::WeftError {
    node_error(format!(
        "{what} failed ({}): {}",
        job["failure_reason"].as_str().unwrap_or("no reason given"),
        job["error_message"].as_str().unwrap_or("no message")
    ))
}

/// Download a completed output of `kind` (`image`, `video`, `audio`)
/// from the signed URL ElevenLabs handed back into the run's storage,
/// and answer the stored file to emit. The file is stored as the type
/// ElevenLabs reported on the job, not whatever the download's header
/// says: everything downstream reads the stored type as the truth about
/// the bytes, and it decides which kind of file the graph sees.
pub async fn store_output(ctx: &RunContext, kind: &str, output: &Value) -> WeftResult<Value> {
    let url = output["content_url"].as_str().node_err("ElevenLabs answered a finished output with no content_url")?;
    let mime = output["content_mime_type"]
        .as_str()
        .node_err("ElevenLabs answered a finished output with no content_mime_type")?;
    if mime.split('/').next() != Some(kind) {
        weft::node_bail!("elevenlabs reported the finished {kind} as {mime}, which is no {kind}");
    }
    ctx.storage(StorageScope::Run).put_from_url(url, Some(mime), Some(&output_filename(kind, mime)), None).await
}

/// Make one generation: POST what `build` returns to the route for
/// `media`, wait for the job, and answer the stored output.
pub async fn generate<F, Fut>(
    ctx: &RunContext,
    state: &mut Kept<Job>,
    account: &Access,
    http: &Client,
    media: Media,
    build: F,
) -> WeftResult<Value>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = WeftResult<Value>>,
{
    let noun = media.noun();
    let id = submit(
        state,
        http,
        url(API, &["flows", noun])?.as_str(),
        &format!("start the ElevenLabs {noun} generation"),
        "ElevenLabs answered no generation id when the generation was started",
        build,
    )
    .await?;
    let job = finish(state, account, http, url(API, &["flows", noun, &id])?.as_str(), media.poll_secs()).await?;
    match job["status"].as_str().unwrap_or_default() {
        "completed" => store_output(ctx, noun, &job).await,
        "failed" => Err(failure(&format!("the {noun} generation {id}"), &job)),
        other => weft::node_bail!("elevenlabs answered an unexpected status '{other}' for the {noun} generation {id}"),
    }
}

/// Whether `model` is one of Google's Veo models, which spell some
/// fields their own way (reference images carry a role, the top
/// resolution is `4K`).
pub fn is_veo(model: &str) -> bool {
    model.starts_with("veo-")
}

/// The request every prompt-driven video model shares, read from the
/// node's inputs: the model, the prompt, and the output settings the
/// person set (`durationSecs`, `aspectRatio`, `resolution`,
/// `generateAudio`, `negativePrompt`, `promptAsWritten`, `seed`).
pub fn video_request(ctx: &RunContext) -> WeftResult<Value> {
    let model: String = ctx.inputs.get("model")?;
    let prompt: String = ctx.inputs.get("prompt")?;
    let generate_audio: bool = ctx.inputs.get("generateAudio")?;
    let as_written: bool = ctx.inputs.get("promptAsWritten")?;
    let duration: Option<u64> = ctx.inputs.opt("durationSecs")?;
    let aspect: Option<String> = ctx.inputs.opt("aspectRatio")?;
    let resolution: Option<String> = ctx.inputs.opt("resolution")?;
    let negative: Option<String> = ctx.inputs.opt("negativePrompt")?;
    let seed: Option<u64> = ctx.inputs.opt("seed")?;

    let resolution = resolution.map(|r| video_resolution(&model, &r));
    let mut body = json!({ "model_id": model, "prompt": prompt, "generate_audio": generate_audio });
    set_some(&mut body, "duration_secs", duration);
    set_some(&mut body, "aspect_ratio", aspect);
    set_some(&mut body, "resolution", resolution);
    set_some(&mut body, "negative_prompt", negative);
    // Veo rewrites the prompt unless told not to, and only Veo takes the
    // field, so it travels only when the person ticked the box.
    set_some(&mut body, "enhance_prompt", as_written.then_some(false));
    set_some(&mut body, "seed", seed);
    Ok(body)
}

/// Veo spells the top video resolution `4K` and Seedance `4k`. The node
/// offers one list for every model, so it takes either spelling and
/// sends the model's own.
pub fn video_resolution(model: &str, resolution: &str) -> String {
    if resolution.eq_ignore_ascii_case("4k") {
        if is_veo(model) { "4K" } else { "4k" }.to_string()
    } else {
        resolution.to_string()
    }
}
