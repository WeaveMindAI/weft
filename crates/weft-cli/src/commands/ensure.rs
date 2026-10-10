//! Shared helper: discover the cwd project, compile it, and have the
//! install build it. Every mutating project-scoped command (`run`,
//! `activate`, `bake`, `resync`, the infra verbs, `build`) calls this first
//! so users don't have to remember `weft build` as a prerequisite.
//!
//! Semantics:
//!   - Compile here first: a mistake in the program is reported at once,
//!     with every diagnostic, before anything is uploaded.
//!   - Snapshot: every covered file the tenant's assets lack is uploaded
//!     straight to the bucket; the manifest names the version.
//!   - Resolve the `@asset` refs only this machine can read (a file outside
//!     the project, a URL) and send them with the version.
//!   - `POST /projects/{id}/builds`: the install fetches the version back,
//!     compiles it, and starts a build of every stale image with its own
//!     builder, answering at once with those builds. The install registers
//!     the version itself once they are built; this command follows it
//!     until it is registered or failed. Nothing is built or loaded on this
//!     machine, and nothing this machine computed is believed.
//!   - Registering a build is UNCONDITIONAL even while executions run on
//!     the previous image: every worker task is stamped with the image it
//!     was enqueued for and only claimable by a process baked from it, so
//!     in-flight work finishes on the old processes while new work flows to
//!     fresh, current-image processes.

use anyhow::{Context, Result};

use weft_core::builds::{BuildsUnderway, BuiltProgram, ImageBuild, VersionBuildState, VersionBuildStatus};
use weft_core::projects::{BuildState, BuildStateResponse};

use super::Ctx;
use crate::client::DispatcherClient;
use crate::progress::Progress;

/// User's local choice for how to handle in-flight Fire executions
/// when a `run` lands with a stale binary or a `deactivate` lands with
/// running execs. The type lives in weft-core (one definition shared
/// with the broker/dispatcher wire protocol); re-exported here so
/// every CLI verb keeps importing it from the gate that consumes it.
pub use weft_core::{RunningChoice, RunningPolicy};

/// Parse an optional `--running-policy` CLI flag value, mapping an
/// unrecognized value to a uniform error. `None` stays `None` (the
/// verb prompts or defaults). One parser for every verb's
/// `--running-policy` so the accepted set can't drift.
pub fn parse_running_policy_flag(flag: Option<&str>) -> Result<Option<RunningPolicy>> {
    match flag {
        None => Ok(None),
        Some(s) => RunningPolicy::parse(&s.to_ascii_lowercase())
            .map(Some)
            .ok_or_else(|| {
                anyhow::anyhow!("invalid --running-policy '{s}'; expected 'wait' or 'cancel'")
            }),
    }
}

/// The running-policy answer a verb puts on the wire: the flag parsed
/// (cancel unless it says wait) and the drain cap, which only ever
/// rides a wait. A `--drain-timeout` beside cancel is refused rather
/// than dropped: it would bound nothing, and a flag that changes
/// nothing is one the person believes did. One reading for every verb
/// that takes the pair (`activate`, `deactivate`, `resync`, the infra
/// verbs), so they cannot disagree about what the cap means.
pub fn parse_running_choice(
    policy: Option<&str>,
    drain_timeout: Option<u64>,
) -> Result<(RunningPolicy, Option<u64>)> {
    let policy = parse_running_policy_flag(policy)?.unwrap_or_default();
    if policy == RunningPolicy::Cancel && drain_timeout.is_some() {
        anyhow::bail!(
            "--drain-timeout bounds a wait, and the running policy is cancel (the default); \
             pass `--running-policy wait` with it, or drop it"
        );
    }
    Ok((policy, drain_timeout))
}

/// The running-work answer as every verb sends it: the policy always
/// (so the wire never guesses), the cap when one was given.
pub fn running_choice(policy: RunningPolicy, drain_timeout: Option<u64>) -> RunningChoice {
    RunningChoice { running_policy: Some(policy), drain_timeout_secs: drain_timeout }
}

pub struct ProjectHandle {
    pub id: String,
    pub name: String,
    pub client: DispatcherClient,
    /// Published sources verified against the files used to compile this build.
    pub manifest: super::versions::Manifest,
    /// What the install built and registered: the compiled program and its
    /// three authoritative hashes (binary = worker image identity,
    /// definition = runtime shape / resync drift, infra = infra closure /
    /// upgrade drift). Downstream verbs name the build by these.
    pub built: BuiltProgram,
}

/// What a person is told when a build moved infra places onto a new
/// image: only a copy started from now on gets it.
fn replaced_infra_images_note(places: &[String], on: Option<&str>) -> Option<String> {
    if places.is_empty() {
        return None;
    }
    Some(format!(
        "this build changed the image of infra {}: a copy started from now on gets the new image, \
         and a copy already running keeps its own until you run `{}` \
         (with `--instance <id>` for one instance's copy)",
        places.join(", "),
        super::weft_on(on, "infra upgrade")
    ))
}

impl ProjectHandle {
    pub fn binary_hash(&self) -> &str {
        &self.built.binary_hash
    }
    pub fn definition_hash(&self) -> &str {
        &self.built.definition_hash
    }

    /// This build as an activate or a resync names it (no reactivate
    /// choice: the caller sets one when it has one).
    pub fn activation_target(&self) -> weft_core::activation::ActivationTarget {
        weft_core::activation::ActivationTarget { build: self.built.named(), reactivate_choice: None }
    }
}

/// How often a version build being followed is asked where it is, and
/// how often an ask the install could not be reached for is sent again.
const BUILD_LOOK_EVERY: std::time::Duration = std::time::Duration::from_secs(5);

/// How often a build that goes on says so.
const BUILD_SAY_EVERY: std::time::Duration = std::time::Duration::from_secs(30);

/// The image builds a version build waits on, as this command follows
/// them: the ones still running, the images that finished, and how the
/// others failed.
#[derive(Default)]
struct Following {
    /// By the name weft minted for each build.
    pending: std::collections::BTreeMap<String, Followed>,
    /// The images this command saw built, in the order they finished.
    built: Vec<String>,
    /// How each build that did not succeed ended, in the order they did.
    failures: Vec<String>,
}

/// One image build being followed.
struct Followed {
    image: String,
    /// How long into the command it was first seen.
    since: std::time::Duration,
    /// The builder's id for it, once said; whether its log was said too.
    said: Option<(String, bool)>,
    /// Its log's address, once known: a failure names it.
    log_url: Option<String>,
}

/// What a look at one build has to be said.
#[derive(Debug, PartialEq)]
enum Said {
    /// It runs on the builder (said again once its log's address comes).
    Building(weft_core::projects::BuildInFlight),
    /// It finished, about `seconds` after it was first seen.
    Built { image: String, seconds: u64 },
}

impl Following {
    /// Follow `builds`, the image builds a 202 named.
    fn new(builds: Vec<ImageBuild>, elapsed: std::time::Duration) -> Self {
        let pending = builds
            .into_iter()
            .map(|build| (build.name, Followed { image: build.image, since: elapsed, said: None, log_url: None }))
            .collect();
        Self { pending, ..Self::default() }
    }

    /// Take in what the install answered about the build `name`, and
    /// answer what is to be said about it.
    fn note(&mut self, name: &str, answer: &BuildStateResponse, elapsed: std::time::Duration) -> Vec<Said> {
        let Some(followed) = self.pending.get_mut(name) else { return Vec::new() };
        let mut said = Vec::new();
        if answer.log_url.is_some() {
            followed.log_url.clone_from(&answer.log_url);
        }
        if let Some(build) = &answer.build {
            let with_log = answer.log_url.is_some();
            let say = match &followed.said {
                None => true,
                Some((_, log_said)) => !log_said && with_log,
            };
            if say {
                followed.said = Some((build.clone(), with_log));
                said.push(Said::Building(weft_core::projects::BuildInFlight {
                    image: followed.image.clone(),
                    build: build.clone(),
                    started_at_unix: answer.started_at_unix.unwrap_or_default(),
                    log_url: answer.log_url.clone(),
                }));
            }
        }
        let log = followed.log_url.as_ref().map(|url| format!("\nits log: {url}")).unwrap_or_default();
        let image = followed.image.clone();
        let ended = match answer.state {
            Some(BuildState::Running) => return said,
            Some(BuildState::Succeeded) => {
                said.push(Said::Built { image: image.clone(), seconds: elapsed.saturating_sub(followed.since).as_secs() });
                self.built.push(image);
                None
            }
            Some(BuildState::Failed) => Some(format!(
                "the build of {image} failed:\n{}{log}",
                answer.reason.as_deref().unwrap_or("the builder gave no reason")
            )),
            Some(BuildState::Cancelled) => Some(format!("the build of {image} was cancelled")),
            None => Some(format!("the build of {image} is no longer on record")),
        };
        self.failures.extend(ended);
        self.pending.remove(name);
        said
    }

    /// The images building right now, as far as this command has said.
    fn building(&self) -> Vec<String> {
        self.pending.values().filter(|f| f.said.is_some()).map(|f| f.image.clone()).collect()
    }
}

/// Follow the version build `build` of project `id` until the install
/// registered it, saying what each image build is doing on the way, and
/// answer what registered. A version that failed, was cancelled, or was
/// superseded by a newer build ends the command with why. While the
/// install is out of reach the builds go on there, so the command says so
/// and keeps looking, for as long as it takes; any other failure to look
/// (a refusal, a body it cannot read) ends the command with it.
async fn follow_version(
    client: &DispatcherClient,
    id: &str,
    build: uuid::Uuid,
    progress: &Progress,
    following: &mut Following,
    started: std::time::Instant,
) -> Result<BuiltProgram> {
    let mut said = started.elapsed();
    let mut out_of_reach = false;
    loop {
        tokio::time::sleep(BUILD_LOOK_EVERY).await;
        let elapsed = started.elapsed();
        let due = elapsed.saturating_sub(said) >= BUILD_SAY_EVERY;
        let answer = match version_state(client, id, build).await {
            Ok(answer) => answer,
            Err(e) => {
                let why = look_again_after(e).map_err(|e| e.context("read where the build is"))?;
                // Said at once when the install goes out of reach, then on
                // the usual beat while it stays out.
                if !out_of_reach || due {
                    said = elapsed;
                    progress.build_unreachable(elapsed, &why);
                }
                out_of_reach = true;
                continue;
            }
        };
        for image in &answer.images {
            for say in following.note(&image.build.name, &image.state, started.elapsed()) {
                match say {
                    Said::Building(build) => progress.build_image(&build),
                    Said::Built { image, seconds } => progress.build_image_done(&image, seconds),
                }
            }
        }
        let why = || answer.reason.clone().unwrap_or_else(|| "the install gave no reason".to_string());
        match answer.state {
            VersionBuildStatus::Registered => {
                return answer.program.context("the install says the version registered, but answered no program");
            }
            VersionBuildStatus::Waiting => {}
            VersionBuildStatus::Failed if !following.failures.is_empty() => {
                anyhow::bail!("the build failed:\n{}", following.failures.join("\n\n"))
            }
            VersionBuildStatus::Failed => anyhow::bail!("the build failed:\n{}", why()),
            VersionBuildStatus::Cancelled => anyhow::bail!("the build was cancelled"),
            VersionBuildStatus::Superseded => anyhow::bail!("this build will not be registered: {}", why()),
        }
        // Back in reach: said at once, so nothing goes on reading as out
        // of reach.
        let back = std::mem::take(&mut out_of_reach);
        if back || due {
            said = elapsed;
            progress.build_wait(elapsed, &following.building(), &following.built);
        }
    }
}

/// A request to the install that failed: `Ok` with why, to say, when the
/// install is only out of reach for now and the request is to be sent
/// again: its connection failed on the way
/// ([`crate::client::connection_failed`]), or the install (or the gateway
/// in front of it) answered that it cannot serve this right now (a 502,
/// 503 or 504: down, starting, or unable to read its database). Both
/// requests this is read on (asking for the build, which joins the one
/// already asked for, and reading where it is) are safe to send again. The
/// error itself when anything else answered.
fn look_again_after(e: anyhow::Error) -> Result<String> {
    use reqwest::StatusCode;
    let busy = crate::client::refusal(&e).is_some_and(|refused| {
        matches!(refused.status(), StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT)
    });
    if busy || crate::client::connection_failed(&e) {
        return Ok(format!("{e:#}"));
    }
    Err(e)
}

/// A build ask that failed: `Ok` with why, to say, when it is to be sent
/// again ([`look_again_after`], and, once it was `sent_again`, the first
/// ask still starting the builds, [`crate::client::Marked::BuildBusy`]);
/// the error itself otherwise.
fn ask_again_after(e: anyhow::Error, sent_again: bool) -> Result<String> {
    let first_still_starting =
        sent_again && crate::client::refusal(&e).is_some_and(|refused| refused.marked() == Some(crate::client::Marked::BuildBusy));
    if first_still_starting {
        return Ok(format!("{e:#}"));
    }
    look_again_after(e)
}

/// Where the version build `build` of project `id` is.
async fn version_state(client: &DispatcherClient, id: &str, build: uuid::Uuid) -> Result<VersionBuildState> {
    let answer = client.get_json(&format!("/projects/{id}/builds/{build}")).await?;
    serde_json::from_value(answer).context("read a build's state")
}

/// What the build ask answered.
enum Asked {
    /// Every image was there: the version registered.
    Registered(Box<BuiltProgram>),
    /// The install registers it once these builds end.
    Underway(BuildsUnderway),
}

/// Ask the install to build `body`, sending the ask again while the
/// install is out of reach ([`look_again_after`]): a second ask joins the
/// version build the first one may have started. Once an ask was sent
/// again, the install answering that the project's builds are being
/// started right now ([`crate::client::Marked::BuildBusy`]) is that first
/// ask still starting them, so it is waited out too; on the first ask
/// that answer is somebody else's build, and ends the command.
async fn ask_to_build(
    client: &DispatcherClient,
    path: &str,
    body: &serde_json::Value,
    progress: &Progress,
    started: std::time::Instant,
) -> Result<Asked> {
    let mut said: Option<std::time::Duration> = None;
    let mut sent_again = false;
    loop {
        let asked = client.post_json_success(path, body).await.and_then(|(status, text)| match status {
            202 => Ok(Asked::Underway(serde_json::from_str(&text).context("read the builds the install started")?)),
            _ => Ok(Asked::Registered(Box::new(serde_json::from_str(&text).context("read the build's answer")?))),
        });
        let e = match asked {
            Ok(asked) => return Ok(asked),
            Err(e) => e,
        };
        let why = ask_again_after(e, sent_again).map_err(|e| e.context("the build failed"))?;
        sent_again = true;
        let elapsed = started.elapsed();
        if said.is_none_or(|said| elapsed.saturating_sub(said) >= BUILD_SAY_EVERY) {
            said = Some(elapsed);
            progress.build_unreachable(elapsed, &why);
        }
        tokio::time::sleep(BUILD_LOOK_EVERY).await;
    }
}

/// Make sure the install knows this project, WITHOUT building anything.
/// Cheap and a no-op when it already does.
///
/// For a verb a person can reasonably reach for before anything has run
/// on that install, and that needs only the project to exist there, never
/// a build of it: a checkpoint (the version tree lives under the project's
/// id), a connection, an option, a frontend. Making them run first would
/// build a worker image for nothing.
pub async fn ensure_project_known(ctx: &Ctx) -> Result<()> {
    let project = ctx.project()?;
    let client = ctx.client()?;
    let body = weft_core::projects::DeclareRequest { id: project.id(), name: project.manifest.package.name.clone() };
    client
        .post_json("/projects", &serde_json::to_value(&body)?)
        .await
        .context("declare the project to the install")?;
    Ok(())
}

/// Discover + compile the cwd project and have the install build and
/// register it (see the module doc). Nothing here parks, drains, or
/// prompts.
pub async fn ensure_registered(
    ctx: &Ctx,
    progress: &Progress,
    node_set: weft_core::builds::NodeSet,
) -> Result<ProjectHandle> {
    let compiled = compile_project(ctx, progress)?;
    build_compiled(ctx, progress, node_set, compiled).await
}

pub struct CompiledProject {
    pub definition: weft_core::ProjectDefinition,
    sources: super::versions::Manifest,
}

/// Compile without publishing files or building an image. Commands validate
/// their requested operation against this same definition before registration.
pub fn compile_project(ctx: &Ctx, progress: &Progress) -> Result<CompiledProject> {
    let project = ctx.project()?;
    let sources = super::versions::local_manifest(project)?;

    // Compile + enrich once; both hashes are scoped to the referenced
    // / infra-closure nodes, so they need the definition + catalog.
    // Cheap, and downstream code after ensure_registered needs these
    // anyway. Use the diagnostic-bearing loader so a compile failure
    // can fire a structured progress error (the editor's action-bar
    // modal renders per-diagnostic info) instead of a single
    // flattened string.
    let (definition, catalog) = match weft_compiler::hash::load_enriched_project_with_diagnostics(project) {
        Ok(pair) => pair,
        Err(weft_compiler::hash::CompileLoadError::Read(msg)) => {
            anyhow::bail!("{msg}");
        }
        Err(weft_compiler::hash::CompileLoadError::Diagnostics(diags)) => {
            return Err(compile_failure(project, progress, &diags));
        }
    };
    // The program's own rules (a trigger inside a loop, a wire into a
    // trigger, ...) are checked here, before any verb looks at its cut or
    // spec: the diagnostic that names the node and says why is what the
    // user reads, never a later refusal about a cut it caused. The build
    // validates again on its own path; this is the same check, earlier.
    let diags = weft_compiler::validate::validate_with_mode(
        &definition,
        &catalog,
        weft_compiler::validate::ValidationMode::Structural,
    );
    if diags.iter().any(|d| matches!(d.severity, weft_compiler::Severity::Error)) {
        return Err(compile_failure(project, progress, &diags));
    }
    Ok(CompiledProject { definition, sources })
}

/// Report compile diagnostics both ways at once and hand back the error
/// to return: a structured progress event for the editor, and the same
/// per-line rendering in the error text for a terminal.
fn compile_failure(
    project: &weft_compiler::project::Project,
    progress: &Progress,
    diags: &[weft_compiler::Diagnostic],
) -> anyhow::Error {
    let summary = diags
        .iter()
        .find(|d| matches!(d.severity, weft_compiler::Severity::Error))
        .map(|d| d.message.clone())
        .unwrap_or_else(|| "compile failed".to_string());
    // The editor's action-bar modal renders one entry per
    // diagnostic from this structured event; the location's
    // `file` is the diagnostic's own source file (an @include's
    // nodes keep their file's coordinates), falling back to
    // main.weft, so a click jumps to the right buffer.
    // SYNC: diagnostic -> ActionErrorDiagnostic mapping <->
    //       extension-vscode/src/preflight.ts preflightBlock
    let main_weft = project.main_weft();
    let json_diags: Vec<serde_json::Value> = diags
        .iter()
        .map(|d| {
            let severity = match d.severity {
                weft_compiler::Severity::Error => "error",
                weft_compiler::Severity::Warning => "warning",
                weft_compiler::Severity::Info => "info",
                weft_compiler::Severity::Hint => "info",
            };
            serde_json::json!({
                "severity": severity,
                "code": d.code,
                "message": d.message,
                "location": {
                    "file": d.file.clone().unwrap_or_else(|| main_weft.to_string_lossy().into_owned()),
                    "line": d.line,
                    "column": d.column,
                },
            })
        })
        .collect();
    progress.structured_error(serde_json::json!({
        "message": summary,
        "what": "Compiling project",
        "stage": "compile",
        "diagnostics": json_diags,
    }));
    // In TTY mode there's no action-bar to read the structured
    // event, so the error itself must carry the per-line
    // locations: one `line:column message` per error, the same
    // rendering the catalog/parse path produces. A bare
    // "compile failed with N diagnostic(s)" would force the
    // user back to the editor to find WHERE.
    anyhow::anyhow!("compile failed:\n{}", weft_compiler::render_diagnostics(diags))
}

/// Upload the version the compile read, and have the install build it.
pub async fn build_compiled(
    ctx: &Ctx,
    progress: &Progress,
    node_set: weft_core::builds::NodeSet,
    compiled: CompiledProject,
) -> Result<ProjectHandle> {
    let CompiledProject { definition, sources } = compiled;
    let project = ctx.project()?;
    let client = ctx.client()?;
    let manifest = super::versions::snapshot(&client, project, !progress.json).await?;
    anyhow::ensure!(manifest == sources,
        "project files changed after compilation; rerun the command to build and record the same sources");

    // The `@asset` refs only this machine can read, resolved here and sent
    // with the version (a referenced file the tenant's assets lack is
    // uploaded on the way).
    let resolutions =
        crate::commands::assets::asset_resolutions(&client, &project.root, &definition, true).await?;

    let id = project.id().to_string();
    let path = format!("/projects/{id}/builds");
    let body = weft_core::builds::VersionBuildRequest {
        name: project.manifest.package.name.clone(),
        manifest: manifest.clone(),
        node_set,
        assets: resolutions.map.clone(),
    };
    progress.build_start(&project.manifest.package.name);
    progress.dispatcher_call_start(&path);
    let started = std::time::Instant::now();
    // The ask answers at once: the version registered (200), or the image
    // builds it waits on (202), which the install registers it after.
    let (built, following) = match ask_to_build(&client, &path, &serde_json::to_value(&body)?, progress, started).await? {
        Asked::Registered(built) => (*built, Following::default()),
        Asked::Underway(underway) => {
            let mut following = Following::new(underway.images, started.elapsed());
            let built = follow_version(&client, &id, underway.build, progress, &mut following, started).await?;
            (built, following)
        }
    };
    progress.dispatcher_call_done(serde_json::json!({ "project_id": id }));
    progress.build_done(&project.manifest.package.name, &following.built);
    if let Some(note) = replaced_infra_images_note(&built.replaced_infra_images, ctx.on()) {
        progress.warn(&note);
    }

    // The version's blobs and the assets the built program uses are the
    // project's current references; everything else starts expiring.
    for warning in crate::commands::assets::publish_references(&client, &built.definition, &resolutions, Some(&manifest)).await? {
        progress.warn(&warning);
    }
    // The install built the files as they were when this command read
    // them; if the disk moved meanwhile, the build registered is already
    // behind it, and the next edit would look already live.
    anyhow::ensure!(
        super::versions::local_manifest(project)? == manifest,
        "project files changed while building, so the version this built is already \
         behind your disk. Run the command again (it rebuilds only what moved). If several \
         people or agents edit at once, wait for them to finish first"
    );

    Ok(ProjectHandle {
        id,
        name: project.manifest.package.name.clone(),
        client,
        manifest,
        built,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn answer(state: Option<BuildState>) -> BuildStateResponse {
        BuildStateResponse { state, ..Default::default() }
    }

    fn secs(n: u64) -> std::time::Duration {
        std::time::Duration::from_secs(n)
    }

    /// A build is said once the builder names it, again once its log's
    /// address comes, and built at its end, with how long it took.
    #[test]
    fn a_followed_build_is_said_as_it_goes() {
        let mut following = Following::new(vec![ImageBuild { image: "worker".into(), name: "b1".into() }], secs(2));
        assert!(following.note("b1", &answer(Some(BuildState::Running)), secs(5)).is_empty(), "not on the builder yet");
        let named = BuildStateResponse { build: Some("cb-1".into()), started_at_unix: Some(7), ..answer(Some(BuildState::Running)) };
        let said = following.note("b1", &named, secs(10));
        assert!(matches!(&said[..], [Said::Building(b)] if b.build == "cb-1" && b.log_url.is_none()), "{said:?}");
        assert!(following.note("b1", &named, secs(15)).is_empty(), "said once");
        let with_log = BuildStateResponse { log_url: Some("https://log".into()), ..named.clone() };
        let said = following.note("b1", &with_log, secs(20));
        assert!(matches!(&said[..], [Said::Building(b)] if b.log_url.as_deref() == Some("https://log")), "{said:?}");
        assert_eq!(following.building(), ["worker"]);
        let done = BuildStateResponse { state: Some(BuildState::Succeeded), ..with_log };
        assert_eq!(following.note("b1", &done, secs(62)), [Said::Built { image: "worker".into(), seconds: 60 }]);
        assert!(following.pending.is_empty());
        assert_eq!(following.built, ["worker"]);
        assert!(following.failures.is_empty());
        assert!(following.note("b1", &done, secs(67)).is_empty(), "a build that ended is said once");
    }

    /// Every way a build can end other than built is a failure that names
    /// its image, the reason and the log when there are some.
    #[test]
    fn a_build_that_did_not_succeed_says_why() {
        let mut following = Following::new(
            vec![
                ImageBuild { image: "worker".into(), name: "b1".into() },
                ImageBuild { image: "db".into(), name: "b2".into() },
                ImageBuild { image: "cache".into(), name: "b3".into() },
            ],
            secs(0),
        );
        let failed = BuildStateResponse {
            reason: Some("cargo: error[E0425]".into()),
            log_url: Some("https://log/1".into()),
            ..answer(Some(BuildState::Failed))
        };
        following.note("b1", &failed, secs(5));
        following.note("b2", &answer(Some(BuildState::Cancelled)), secs(5));
        following.note("b3", &answer(None), secs(5));
        assert!(following.pending.is_empty());
        assert_eq!(following.failures[0], "the build of worker failed:\ncargo: error[E0425]\nits log: https://log/1");
        assert_eq!(following.failures[1], "the build of db was cancelled");
        assert_eq!(following.failures[2], "the build of cache is no longer on record");
    }

    /// A look the install refused, or answered with something unreadable,
    /// ends the follow with that answer; one that never reached the
    /// install, or reached only its gateway saying it is down, is looked
    /// at again.
    #[tokio::test]
    async fn a_refused_look_ends_the_follow_and_an_unreachable_one_looks_again() {
        use crate::client::fake::{closed, install, Then};
        let build = uuid::Uuid::nil();
        let look = |base: String| async move { version_state(&DispatcherClient::new(base, None), "p", build).await.unwrap_err() };

        let refused = look(install(Then::Answer("HTTP/1.1 401 Unauthorized\r\ncontent-length: 11\r\n\r\nwrong key\r\n")).await).await;
        let e = look_again_after(refused).unwrap_err();
        assert_eq!(format!("{e:#}"), "wrong key");

        let unreadable = look(install(Then::Answer("HTTP/1.1 200 OK\r\ncontent-length: 6\r\n\r\n{\"x\":1")).await).await;
        assert!(look_again_after(unreadable).is_err(), "an answer it cannot read is not the install being away");

        let why = look_again_after(look(closed()).await).unwrap();
        assert!(why.contains("did not answer"), "{why}");
        let reset = look(install(Then::Reset).await).await;
        assert!(look_again_after(reset).is_ok());
        let down = look(install(Then::Answer("HTTP/1.1 502 Bad Gateway\r\ncontent-length: 0\r\n\r\n")).await).await;
        assert!(look_again_after(down).is_ok());
        let busy = look(install(Then::Answer("HTTP/1.1 503 Service Unavailable\r\ncontent-length: 0\r\n\r\n")).await).await;
        assert!(look_again_after(busy).is_ok());
    }

    /// The project's builds being started right now ends a first ask (it
    /// is somebody else's build), and is waited out once the ask was sent
    /// again (it is this command's own first ask, whose answer was lost).
    #[tokio::test]
    async fn a_busy_project_ends_a_first_ask_and_is_waited_out_once_sent_again() {
        use crate::client::fake::{install, Then};
        let busy = || async {
            let base = install(Then::Answer(
                "HTTP/1.1 409 Conflict\r\nx-weft-build-busy: 1\r\ncontent-length: 16\r\n\r\ncannot build now",
            ))
            .await;
            DispatcherClient::new(base, None).post_json_success("/projects/p/builds", &serde_json::json!({})).await.unwrap_err()
        };
        assert!(ask_again_after(busy().await, false).is_err());
        assert!(ask_again_after(busy().await, true).is_ok());
        let cancelled = DispatcherClient::new(
            install(Then::Answer("HTTP/1.1 409 Conflict\r\ncontent-length: 15\r\n\r\nbuild cancelled")).await,
            None,
        )
        .post_json_success("/projects/p/builds", &serde_json::json!({}))
        .await
        .unwrap_err();
        assert!(ask_again_after(cancelled, true).is_err(), "only the busy marker is waited out");
    }
}
