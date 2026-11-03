//! ElevenLabsDub: dub a recording into another language, translated and
//! re-voiced to match the original speakers, and store the dubbed audio.
//! Left out on purpose: `webhook_ids`, since the node waits on the dub
//! itself, and the language target's `translations`, since a segment id
//! they could name only exists, within one run, in a `transcript` the
//! program supplies, whose segments carry their translation already.

use std::path::Path;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use weft::access::client::{get_json, json_call, post_json, Multipart};
use weft::context::LogLevel;
use weft::node::NodeOutput;
use weft::signal::{PollEndpoint, Predicate};
use weft::storage::{FileHandle, StorageScope, StoredFile};
use weft::{node_error, Access, RunContext, Node, NodeErrExt, NodeManifest, WeftError, WeftResult};

use super::elevenlabs::{link_filename, API};
use super::flows::{delete_quietly, store_download, wait_for_job};

type Client = weft::reqwest_middleware::ClientWithMiddleware;

#[derive(NodeManifest)]
pub struct ElevenLabsDubNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ElevenLabsDubNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let file: Option<FileHandle> = ctx.inputs.opt("file")?;
        let source_url: Option<String> = ctx.inputs.opt("sourceUrl")?;
        let target: String = ctx.inputs.get("targetLang")?;
        let source: Option<String> = ctx.inputs.opt("sourceLang")?;
        let model: String = ctx.inputs.get("model")?;
        let keyterms: Vec<String> = ctx.inputs.list("keyterms")?;
        let transcript: Option<Value> = ctx.inputs.opt("transcript")?;
        let reference: Option<String> = ctx.inputs.opt("reference")?;
        // A whole number: the widget's step holds a wired value to whole
        // numbers too.
        let cloning: Option<u64> = ctx.inputs.opt("cloningStrength")?;
        // The project is the program's only when it reads `projectId`.
        // Otherwise nothing would ever use it again, and it would sit on
        // the account (the install's own, on the built-in connection)
        // until somebody deleted it, so the node deletes it once done.
        let keep = ctx.is_output_wired("projectId");
        // Everything ElevenLabs would refuse once the project has taken
        // its up-front charge, refused here before anything is sent (the
        // compiler already refuses the written shapes; this is the floor
        // for wired ones).
        let recording = match (file, source_url) {
            (Some(file), None) => Recording::File(file),
            (None, Some(url)) => Recording::Url(url),
            (Some(_), Some(_)) => {
                return Err(WeftError::Input("both `file` and `sourceUrl` are given: keep one of them".into()))
            }
            (None, None) => return Err(WeftError::Input("there is nothing to dub: give a `file` or a `sourceUrl`".into())),
        };
        if let Some(refusal) = refusal(&model, cloning.is_some(), source.is_some(), transcript.as_ref()) {
            return Err(WeftError::Input(refusal));
        }

        let http = ctx.client(&account).await?;
        let projects = format!("{API}/dubbing/project");

        let mut state = ctx.keep(Dub::default()).await?;

        // Creating the project is paid (one language's dub up front), so
        // it runs once: a run resumed after the wait finds the project in
        // its state instead of paying for a second one (and never reads
        // the recording again). The language rides on the create whenever
        // it can, so a language ElevenLabs does not dub is refused before
        // anything is charged; a cloning strength can only be set on a
        // language added on its own.
        let created = state
            .once(|s| &mut s.created, async |_| {
                let mut form = Multipart::form_data().text("model_id", &model);
                let filename = match &recording {
                    Recording::File(file) => {
                        let (meta, bytes) = ctx.storage(StorageScope::Run).get_bytes(file).await?;
                        form = form.file("file", &meta.filename, &meta.mime_type, bytes);
                        meta.filename
                    }
                    Recording::Url(url) => {
                        form = form.text("source_url", url);
                        link_filename(url, "source")
                    }
                };
                if let Some(lang) = &source {
                    form = form.text("source_language", lang);
                }
                if cloning.is_none() {
                    form = form.text("target_language", &target);
                }
                // ElevenLabs reads a list field as the field repeated.
                for term in &keyterms {
                    form = form.text("keyterms", term);
                }
                if let Some(transcript) = &transcript {
                    let document = serde_json::to_vec(transcript).node_err("serialize the transcript")?;
                    form = form.file("transcript", "transcript.json", "application/json", document);
                }
                if let Some(reference) = &reference {
                    form = form.text("reference", reference);
                }
                let (content_type, body) = form.build();
                let answer = json_call(
                    http.post(&projects).header("content-type", content_type).body(body),
                    "create the ElevenLabs dub project",
                )
                .await?;
                let project = answer["project_id"]
                    .as_str()
                    .node_err("ElevenLabs created the dub project but its answer has no project_id")?
                    .to_string();
                let language = answer["language_ids"][0].as_str().map(str::to_string);
                Ok(Created { project, filename, language })
            })
            .await?;
        let Created { project, filename, language } = created;

        let language = match (language, cloning) {
            (Some(language), None) => language,
            (None, None) => {
                let fate = project_fate(&ctx, &http, &projects, &project, keep).await?;
                weft::node_bail!(
                    "ElevenLabs created the dub project {project} but did not say it started the dub into \
                     '{target}'. Creating the project already cost one language's dub, and {fate}"
                )
            }
            // A step of its own: by now the project holds its prepaid
            // language, so a failure here says what became of it.
            (_, Some(strength)) => {
                state
                    .once(|s| &mut s.added, async |_| {
                        let body = json!({ "target_language": target, "voice_settings": { "cloning_strength": strength } });
                        let url = format!("{projects}/{project}/language");
                        let added =
                            post_json(&http, &url, &body, &format!("add '{target}' to the ElevenLabs dub project")).await;
                        let answer = match added {
                            Ok(answer) => answer,
                            Err(e) => {
                                let fate = project_fate(&ctx, &http, &projects, &project, keep).await?;
                                return Err(node_error(format!(
                                    "adding '{target}' to the dub project {project} failed: {e}. Creating the project \
                                     already cost one language's dub, and {fate}"
                                )));
                            }
                        };
                        Ok(answer["language_id"]
                            .as_str()
                            .node_err("ElevenLabs added the language but its answer has no language_id")?
                            .to_string())
                    })
                    .await?
            }
        };
        let target_url = format!("{projects}/{project}/language/{language}");

        // Resume on any status that is not in flight, so one this node
        // does not know ends the wait and is refused below instead of
        // being waited on for ever. The wait carries only the status; the
        // read after it carries the rest, with the download URL signed
        // now rather than whenever the wait ended (it lasts an hour).
        let poll = PollEndpoint {
            url: target_url.clone(),
            interval_secs: 10,
            access: Some(weft::primitive::AccessRef::from(&account)),
            filters: vec![Predicate::neq("status", "queued"), Predicate::neq("status", "processing")],
            carry: vec!["status".into()],
            ..Default::default()
        };
        wait_for_job(&mut state, |s| &mut s.ended, poll, || async {
            if !keep {
                discard(&ctx, &http, &projects, &project).await?;
            }
            Ok(())
        })
        .await?;

        // Kept, so a body run again after a crash that followed the
        // delete below finds the stored dub in its state instead of
        // reading a project that is gone.
        let dubbed = state
            .once(|s| &mut s.dubbed, async |_| finished(&ctx, &http, &projects, &project, &target, &target_url, &filename).await)
            .await;
        if !keep {
            discard(&ctx, &http, &projects, &project).await?;
        }
        ctx.pulse_downstream(NodeOutput::stored_file(dubbed?).set("projectId", project)).await
    }
}

/// Where the dub stands: the project created (and the language it
/// started), the language added on its own when a cloning strength is
/// set, the status the wait ended on, and the dubbed audio stored.
#[derive(Default, Serialize, Deserialize)]
struct Dub {
    created: Option<Created>,
    added: Option<String>,
    ended: Option<Value>,
    dubbed: Option<StoredFile>,
}

/// The dub project the create made.
#[derive(Clone, Serialize, Deserialize)]
struct Created {
    project: String,
    /// The recording's name, which names the dubbed file.
    filename: String,
    /// The language the create started, when it started one.
    language: Option<String>,
}

/// Where the recording to dub comes from.
enum Recording {
    /// A file in the run's storage, uploaded with the create.
    File(FileHandle),
    /// A public link ElevenLabs fetches the recording from itself.
    Url(String),
}

/// What ElevenLabs would refuse, or what would pay for nothing, said
/// before the project is created, or `None` when the dub can go.
fn refusal(model: &str, cloning: bool, source_language: bool, transcript: Option<&Value>) -> Option<String> {
    // ElevenLabs offers a cloning strength on Dubbing v2 only, and it
    // goes on a language added after the project has taken its charge.
    if cloning && model != "dubbing_v2" {
        return Some(format!(
            "`cloningStrength` is a dubbing_v2 setting, and this dub uses '{model}': set `model` to dubbing_v2, \
             or leave `cloningStrength` empty"
        ));
    }
    let Some(transcript) = transcript else { return None };
    if !source_language {
        return Some("ElevenLabs needs `sourceLang` with a `transcript`: say which language the transcript is in".into());
    }
    // A transcript's translations seed the language the create starts,
    // and a cloning strength makes the node add that language on its own.
    let translated = transcript["segments"]
        .as_array()
        .is_some_and(|segments| segments.iter().any(|s| s.get("translation").is_some()));
    if cloning && translated {
        return Some(
            "the `transcript` carries translations, which ElevenLabs only takes for a language started with the \
             project, and `cloningStrength` needs the language added on its own: drop the translations, or leave \
             `cloningStrength` empty"
                .into(),
        );
    }
    None
}

/// Read the dub target that just ended and store its audio, or fail with
/// what ElevenLabs said went wrong.
async fn finished(
    ctx: &RunContext,
    http: &Client,
    projects: &str,
    project: &str,
    target: &str,
    target_url: &str,
    filename: &str,
) -> WeftResult<StoredFile> {
    let dub = get_json(http, target_url, "read the finished ElevenLabs dub").await?;
    match dub["status"].as_str().unwrap_or_default() {
        "completed" => {}
        // The target only says its project failed; the project carries
        // the cause.
        "failed" if dub["error"]["code"] == "project_failed" => {
            let parent =
                get_json(http, &format!("{projects}/{project}"), "read the failed ElevenLabs dub project").await?;
            return Err(failure(&format!("the dub project {project}"), &parent["error"]));
        }
        "failed" => return Err(failure(&format!("the dub into '{target}' of project {project}"), &dub["error"])),
        other => weft::node_bail!(
            "ElevenLabs answered an unexpected status '{other}' for the dub into '{target}' of project {project}"
        ),
    }
    // The target carries its own warnings; what came up while the source
    // was prepared is reported on the project.
    let parent = get_json(http, &format!("{projects}/{project}"), "read the finished ElevenLabs dub project").await?;
    let lines: Vec<String> = [&parent["warnings"], &dub["warnings"]]
        .into_iter()
        .filter_map(Value::as_array)
        .flatten()
        .map(warning_line)
        .collect();
    for line in lines {
        ctx.log(LogLevel::Warn, line).await?;
    }
    let url = dub["outputs"]["lossless_audio"]
        .as_str()
        .node_err("ElevenLabs reports the dub finished but gives no outputs.lossless_audio to download")?;
    let stem = Path::new(filename).file_stem().and_then(|s| s.to_str()).unwrap_or(filename);
    store_download(ctx, url, "audio/flac", &format!("dubbed_{target}_{stem}.flac")).await
}

/// Delete a dub project nobody keeps ([`delete_quietly`]).
async fn discard(ctx: &RunContext, http: &Client, projects: &str, project: &str) -> WeftResult<()> {
    delete_quietly(ctx, http, &format!("{projects}/{project}"), |refused| {
        format!("the dub project {project} is still on the ElevenLabs account ({refused}); delete it from that account")
    })
    .await
}

/// What became of a project whose dub could not start: kept on the
/// account (`keep`), where its prepaid dub serves the first language
/// added, or deleted. Its words finish the failure's sentence.
async fn project_fate(ctx: &RunContext, http: &Client, projects: &str, project: &str, keep: bool) -> WeftResult<&'static str> {
    if keep {
        return Ok("it stays on the account behind this node's key, where the first language you add uses that prepaid dub; or delete it there");
    }
    discard(ctx, http, projects, project).await?;
    Ok("it was deleted")
}

/// The failure ElevenLabs reported in an `error` object (a dub target's
/// or a project's), with whether running the same input again can help.
fn failure(what: &str, error: &Value) -> WeftError {
    let again = match error["retryable"].as_bool() {
        Some(true) => ". ElevenLabs says running the same input again could succeed",
        Some(false) => ". ElevenLabs says the same input will fail the same way again",
        None => "",
    };
    node_error(format!(
        "{what} failed ({}): {}{again}",
        error["code"].as_str().unwrap_or("no code given"),
        error["message"].as_str().unwrap_or("no message")
    ))
}

/// One warning on a finished dub as a log line, quoting ElevenLabs.
fn warning_line(warning: &Value) -> String {
    let message = warning["message"].as_str().unwrap_or("no message");
    if warning["type"] == "voices_not_permitted" {
        let speakers = warning["speaker_ids"]
            .as_array()
            .map(|ids| ids.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(", "))
            .unwrap_or_default();
        format!("the dub used a replacement voice for speakers [{speakers}]: {message}")
    } else {
        format!("ElevenLabs warned about the dub ({}): {message}", warning["type"].as_str().unwrap_or("untyped"))
    }
}
