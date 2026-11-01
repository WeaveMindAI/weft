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
use std::sync::Mutex;

use base64::Engine as _;
use serde::Serialize;
use serde_json::{json, Value};

use weft::access::client::{get_json, json_call, post_json, send_error, Multipart};
use weft::context::LogLevel;
use weft::reqwest::StatusCode;
use weft::signal::{PollEndpoint, Predicate};
use weft::storage::{FileHandle, StorageScope, StoredFile};
use weft::{node_error, Access, RunContext, NodeErrExt, WeftError, WeftResult};

use super::elevenlabs::API;

type Client = weft::reqwest_middleware::ClientWithMiddleware;

/// The largest file ElevenLabs takes inline in a request (25 MB once
/// decoded). A bigger one goes up as a temporary asset instead.
const INLINE_MAX_BYTES: usize = 25_000_000;

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
pub fn inline_mime(mime: &str, len: usize) -> Option<&'static str> {
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
/// carries, and remembers every temporary asset it had to upload, so
/// the node removes them once the job no longer needs them.
pub struct References<'a> {
    ctx: &'a RunContext,
    http: &'a Client,
    uploads: Mutex<Vec<String>>,
}

impl<'a> References<'a> {
    pub fn new(ctx: &'a RunContext, http: &'a Client) -> Self {
        Self { ctx, http, uploads: Mutex::new(Vec::new()) }
    }

    /// The reference for one file: its bytes inline when ElevenLabs
    /// takes it that way, otherwise an asset uploaded to the workspace
    /// for this job and named by its id.
    pub async fn of(&self, file: &FileHandle) -> WeftResult<Value> {
        let (meta, bytes) = self.ctx.storage(StorageScope::Run).get_bytes(file).await?;
        if let Some(mime) = inline_mime(&meta.mime_type, bytes.len()) {
            return Ok(json!({
                "type": "inline_base64",
                "mime_type": mime,
                "content_base64": base64::engine::general_purpose::STANDARD.encode(&bytes),
            }));
        }
        let (content_type, body) = Multipart::form_data()
            .text("name", &meta.filename)
            .file("asset", &meta.filename, &meta.mime_type, bytes.to_vec())
            .build();
        let answer = json_call(
            self.http.post(format!("{API}/assets")).header("content-type", content_type).body(body),
            &format!("upload '{}' to ElevenLabs as a temporary asset", meta.filename),
        )
        .await?;
        let id = answer["asset_id"]
            .as_str()
            .node_err("ElevenLabs answered no asset_id for the uploaded asset")?
            .to_string();
        self.uploads.lock().expect("no holder of this lock panics").push(id.clone());
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

    fn uploads(&self) -> Vec<String> {
        self.uploads.lock().expect("no holder of this lock panics").clone()
    }
}

/// A job ElevenLabs accepted: its create answer, and the temporary
/// assets it reads.
pub struct Submitted {
    pub answer: Value,
    uploads: Vec<String>,
}

/// Build a request with `build` (which turns files into references
/// through `refs`) and POST it to `url`, once: the step is journaled,
/// so the body replaying after the wait reads the answer back instead
/// of paying for a second job. A request that fails takes its
/// temporary uploads with it.
pub async fn submit<F, Fut>(
    ctx: &RunContext,
    http: &Client,
    refs: &References<'_>,
    url: &str,
    what: &str,
    build: F,
) -> WeftResult<Submitted>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = WeftResult<Value>>,
{
    let journaled = ctx
        .run("elevenlabs_submit", || async {
            let sent = async {
                let body = build().await?;
                post_json(http, url, &body, what).await
            }
            .await;
            match sent {
                Ok(answer) => Ok(json!({ "answer": answer, "uploads": refs.uploads() })),
                Err(e) => {
                    remove_uploads(ctx, http, &refs.uploads()).await?;
                    Err(e)
                }
            }
        })
        .await?;
    let uploads = journaled["uploads"]
        .as_array()
        .node_err("the journaled submit has no uploads")?
        .iter()
        .filter_map(|id| id.as_str().map(str::to_string))
        .collect();
    Ok(Submitted { answer: journaled["answer"].clone(), uploads })
}

/// Park until the job at `url` leaves `pending` and `generating`,
/// without holding a worker, then remove the job's temporary uploads
/// (it no longer reads them, whatever its outcome) and answer the job
/// read again now.
///
/// The wait carries only `status`; the read after it carries the rest,
/// with content URLs signed now rather than whenever the wait ended,
/// and none of them lands in the journal. Any status that is not in
/// flight ends the wait, so one this package does not know is refused
/// by the caller instead of waited on for ever. A run stopped while it
/// waits runs none of this, so its temporary uploads stay in the
/// workspace's assets.
pub async fn finish(
    ctx: &RunContext,
    account: &Access,
    http: &Client,
    url: &str,
    poll_secs: u64,
    submitted: &Submitted,
) -> WeftResult<Value> {
    let ended = ctx
        .await_signal(PollEndpoint {
            url: url.to_string(),
            interval_secs: poll_secs,
            access: Some(weft::primitive::AccessRef::from(account)),
            filters: vec![Predicate::neq("status", "pending"), Predicate::neq("status", "generating")],
            carry: vec!["status".into()],
            ..Default::default()
        })
        .await;
    match ended {
        Ok(_) => {}
        // The run gave the job up without its answer (the wait ran out
        // of time, or a person skipped it): nothing will read the job
        // again, so its uploads go now.
        Err(e @ (WeftError::WaitGaveUp(_) | WeftError::WaitSkipped(_))) => {
            remove_uploads(ctx, http, &submitted.uploads).await?;
            return Err(e);
        }
        // Anything else leaves the job its files, the park above all:
        // the body stops here while ElevenLabs works and replays from
        // the top once the job has ended.
        Err(e) => return Err(e),
    }
    remove_uploads(ctx, http, &submitted.uploads).await?;
    get_json(http, url, "read the finished ElevenLabs job").await
}

/// Delete the temporary assets a job read. One already gone counts as
/// removed (the body can replay after a crash that followed the
/// delete). One that cannot be removed never fails the node, since the
/// job it served is already paid for: it is named on the node's log,
/// with where to delete it, because it stays in the workspace and
/// counts against its storage.
async fn remove_uploads(ctx: &RunContext, http: &Client, uploads: &[String]) -> WeftResult<()> {
    for id in uploads {
        let refused = match http.delete(format!("{API}/assets/{id}")).send().await {
            Ok(resp) if resp.status().is_success() || resp.status() == StatusCode::NOT_FOUND => continue,
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                format!("ElevenLabs answered {status}: {}", body.chars().take(300).collect::<String>())
            }
            Err(e) => send_error(e),
        };
        ctx.log(
            LogLevel::Warn,
            format!(
                "the temporary asset {id} is still in the ElevenLabs workspace ({refused}); delete it in the \
                 Assets section of ElevenLabs, where it counts against the workspace's storage"
            ),
        )
        .await?;
    }
    Ok(())
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
/// into the run's storage, and answer the stored
/// file to emit. The file is stored as the type ElevenLabs reported on
/// the job, not whatever the download's header says: everything
/// downstream reads the stored type as the truth about the bytes, and
/// it decides which kind of file the graph sees.
pub async fn store_output(ctx: &RunContext, kind: &str, output: &Value) -> WeftResult<Value> {
    let url = output["content_url"].as_str().node_err("ElevenLabs answered a finished output with no content_url")?;
    let mime = output["content_mime_type"]
        .as_str()
        .node_err("ElevenLabs answered a finished output with no content_mime_type")?;
    if mime.split('/').next() != Some(kind) {
        weft::node_bail!("elevenlabs reported the finished {kind} as {mime}, which is no {kind}");
    }
    let stored = store_download(ctx, url, mime, &output_filename(kind, mime)).await?;
    Ok(stored.to_value())
}

/// Download a finished output from the signed `url` ElevenLabs handed
/// back into the run's storage, as `mime` (the type
/// ElevenLabs stated, never the download's header: everything
/// downstream reads the stored type as the truth about the bytes) under
/// `filename`.
pub async fn store_download(ctx: &RunContext, url: &str, mime: &str, filename: &str) -> WeftResult<StoredFile> {
    let what = format!("download {filename} from ElevenLabs");
    // The URL is signed by ElevenLabs and served by a storage host: it
    // goes out on the plain client, so the account's key never travels
    // with it.
    let resp = ctx.http().get(url).send().await.map_err(|e| node_error(format!("{what}: {}", send_error(e))))?;
    ctx.storage(StorageScope::Run).put_response(resp, &what, Some(mime), filename, None).await
}

/// Make one generation: POST what `build` returns to the route for
/// `media`, wait for the job, and answer the stored output.
pub async fn generate<F, Fut>(
    ctx: &RunContext,
    account: &Access,
    http: &Client,
    refs: &References<'_>,
    media: Media,
    build: F,
) -> WeftResult<Value>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = WeftResult<Value>>,
{
    let noun = media.noun();
    let route = format!("{API}/flows/{noun}");
    let submitted =
        submit(ctx, http, refs, &route, &format!("start the ElevenLabs {noun} generation"), build).await?;
    let id = submitted.answer["id"]
        .as_str()
        .node_err("ElevenLabs answered no generation id when the generation was started")?
        .to_string();
    let job = finish(ctx, account, http, &format!("{route}/{id}"), media.poll_secs(), &submitted).await?;
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
    let duration: Option<f64> = ctx.inputs.opt("durationSecs")?;
    let aspect: Option<String> = ctx.inputs.opt("aspectRatio")?;
    let resolution: Option<String> = ctx.inputs.opt("resolution")?;
    let negative: Option<String> = ctx.inputs.opt("negativePrompt")?;
    let seed: Option<f64> = ctx.inputs.opt("seed")?;

    let resolution = resolution.map(|r| video_resolution(&model, &r));
    let mut body = json!({ "model_id": model, "prompt": prompt, "generate_audio": generate_audio });
    // `durationSecs` and `seed` take whole numbers only (their widgets'
    // step, which the runtime holds a wired value to as well), so both
    // casts are exact.
    set_some(&mut body, "duration_secs", duration.map(|d| d as u64));
    set_some(&mut body, "aspect_ratio", aspect);
    set_some(&mut body, "resolution", resolution);
    set_some(&mut body, "negative_prompt", negative);
    // Veo rewrites the prompt unless told not to, and only Veo takes the
    // field, so it travels only when the person ticked the box.
    set_some(&mut body, "enhance_prompt", as_written.then_some(false));
    set_some(&mut body, "seed", seed.map(|s| s as u64));
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
