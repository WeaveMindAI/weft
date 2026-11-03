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

use weft::access::client::{get_json, json_call, post_json, url, Multipart, StoredBodyExt, Url};
use weft::context::LogLevel;
use weft::node::NodeOutput;
use weft::signal::{CancelCall, PollEndpoint, Predicate};
use weft::storage::{name_in_url, FileHandle, StorageScope, StoredFile};
use weft::{node_error, Access, RunContext, Node, NodeErrExt, NodeManifest, WeftError, WeftResult};

use super::elevenlabs::API;

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
        // until somebody deleted it, so weft deletes it once the node
        // ends, however it ends.
        let keep = ctx.is_output_wired("projectId");
        // The rules refuse both `file` and `sourceUrl`, and a node given
        // neither is skipped.
        let recording = match file {
            Some(file) => Recording::File(file),
            None => Recording::Url(source_url.node_err("the dub has neither `file` nor `sourceUrl`")?),
        };
        if cloning.is_some() && transcript.as_ref().is_some_and(translated) {
            weft::input_bail!(
                "the `transcript` carries translations, which ElevenLabs only takes for a language started with the \
                 project, and `cloningStrength` needs the language added on its own: drop the translations, or leave \
                 `cloningStrength` empty"
            );
        }

        let http = ctx.client(&account).await?;
        let projects = url(API, &["dubbing", "project"])?;

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
                let settings = Settings {
                    model: &model,
                    source_language: source.as_deref(),
                    target_language: cloning.is_none().then_some(target.as_str()),
                    keyterms: &keyterms,
                    transcript: transcript.as_ref(),
                    reference: reference.as_deref(),
                };
                let (form, filename) = match &recording {
                    Recording::File(file) => {
                        let body = ctx.storage(StorageScope::Run).body(file).await?;
                        let filename = body.filename().to_string();
                        (settings.form(None)?.stored("file", body), filename)
                    }
                    Recording::Url(link) => (settings.form(Some(link))?, name_in_url(link).unwrap_or("source").to_string()),
                };
                let answer =
                    json_call(http.post(projects.as_str()).multipart(form), "create the ElevenLabs dub project").await?;
                let project = answer["project_id"]
                    .as_str()
                    .node_err("ElevenLabs created the dub project but its answer has no project_id")?
                    .to_string();
                // Owed from now on, so a project nobody keeps goes whether
                // the node ends done, failed, cancelled or given up.
                if !keep {
                    ctx.after_node(CancelCall::delete(projects.clone().segment(&project)?).always(), &account).await?;
                }
                let language = answer["language_ids"][0].as_str().map(str::to_string);
                Ok(Created { project, filename, language })
            })
            .await?;
        let Created { project, filename, language } = created;

        let language = match (language, cloning) {
            (Some(language), None) => language,
            (None, None) => {
                let fate = project_fate(keep);
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
                        let address = projects.clone().segment(&project)?.segment("language")?;
                        let added = post_json(&http, address.as_str(), &body, &format!("add '{target}' to the ElevenLabs dub project"))
                            .await;
                        let answer = match added {
                            Ok(answer) => answer,
                            Err(e) => {
                                let fate = project_fate(keep);
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
        let target_url = projects.clone().segment(&project)?.segment("language")?.segment(&language)?;

        // Resume on any status that is not in flight, so one this node
        // does not know ends the wait and is refused below instead of
        // being waited on for ever. The wait carries only the status; the
        // read after it carries the rest, with the download URL signed
        // now rather than whenever the wait ended (it lasts an hour).
        let poll = PollEndpoint {
            url: target_url.to_string(),
            interval_secs: 10,
            access: Some(weft::primitive::AccessRef::from(&account)),
            filters: vec![Predicate::neq("status", "queued"), Predicate::neq("status", "processing")],
            carry: vec!["status".into()],
            ..Default::default()
        };
        state.wait(|s| &mut s.ended, poll).await?;

        // Kept, so a body run again after a crash finds the stored dub in
        // its state instead of downloading it again.
        let dubbed = state
            .once(|s| &mut s.dubbed, async |_| finished(&ctx, &http, &projects, &project, &target, &target_url, &filename).await)
            .await?;
        ctx.pulse_downstream(NodeOutput::stored_file(dubbed).set("projectId", project)).await
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

/// The create's settings, as the node was given them.
pub struct Settings<'a> {
    pub model: &'a str,
    pub source_language: Option<&'a str>,
    /// The language the create starts, unless a cloning strength makes
    /// the node add it on its own.
    pub target_language: Option<&'a str>,
    pub keyterms: &'a [String],
    pub transcript: Option<&'a Value>,
    pub reference: Option<&'a str>,
}

impl Settings<'_> {
    /// The create's form, all but a stored recording, which goes after
    /// it streamed out of storage: every setting given, and the public
    /// `source_url` ElevenLabs fetches the recording from when there is
    /// one.
    pub fn form(&self, source_url: Option<&str>) -> WeftResult<Multipart> {
        let mut form = Multipart::form_data().text("model_id", self.model);
        if let Some(link) = source_url {
            form = form.text("source_url", link);
        }
        if let Some(lang) = self.source_language {
            form = form.text("source_language", lang);
        }
        if let Some(target) = self.target_language {
            form = form.text("target_language", target);
        }
        // ElevenLabs reads a list field as the field repeated.
        for term in self.keyterms {
            form = form.text("keyterms", term);
        }
        if let Some(transcript) = self.transcript {
            let document = serde_json::to_vec(transcript).node_err("serialize the transcript")?;
            form = form.file("transcript", "transcript.json", "application/json", document);
        }
        if let Some(reference) = self.reference {
            form = form.text("reference", reference);
        }
        Ok(form)
    }
}

/// Whether a `transcript` carries translations, which seed the language
/// the create starts (a cloning strength makes the node add that
/// language on its own instead, so the two cannot go together).
fn translated(transcript: &Value) -> bool {
    transcript["segments"].as_array().is_some_and(|segments| segments.iter().any(|s| s.get("translation").is_some()))
}

/// Read the dub target that just ended and store its audio, or fail with
/// what ElevenLabs said went wrong.
async fn finished(
    ctx: &RunContext,
    http: &Client,
    projects: &Url,
    project: &str,
    target: &str,
    target_url: &Url,
    filename: &str,
) -> WeftResult<StoredFile> {
    let dub = get_json(http, target_url.as_str(), "read the finished ElevenLabs dub").await?;
    let project_url = projects.clone().segment(project)?;
    match dub["status"].as_str().unwrap_or_default() {
        "completed" => {}
        // The target only says its project failed; the project carries
        // the cause.
        "failed" if dub["error"]["code"] == "project_failed" => {
            let parent = get_json(http, project_url.as_str(), "read the failed ElevenLabs dub project").await?;
            return Err(failure(&format!("the dub project {project}"), &parent["error"]));
        }
        "failed" => return Err(failure(&format!("the dub into '{target}' of project {project}"), &dub["error"])),
        other => weft::node_bail!(
            "ElevenLabs answered an unexpected status '{other}' for the dub into '{target}' of project {project}"
        ),
    }
    // The target carries its own warnings; what came up while the source
    // was prepared is reported on the project.
    let parent = get_json(http, project_url.as_str(), "read the finished ElevenLabs dub project").await?;
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
    // Stored as the lossless FLAC ElevenLabs names it, whatever type the
    // signed download is served under.
    let stored = ctx
        .storage(StorageScope::Run)
        .put_from_url(url, Some("audio/flac"), Some(&format!("dubbed_{target}_{stem}.flac")), None)
        .await?;
    StoredFile::from_value(&stored)
}

/// What becomes of a project whose dub could not start: kept on the
/// account (`keep`), where its prepaid dub serves the first language
/// added, or deleted when the node ends. Its words finish the failure's
/// sentence.
fn project_fate(keep: bool) -> &'static str {
    if keep {
        "it stays on the account behind this node's key, where the first language you add uses that prepaid dub; or delete it there"
    } else {
        "it is deleted"
    }
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
