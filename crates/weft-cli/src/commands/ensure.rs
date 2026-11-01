//! Shared helper: sync this folder with its working copy and have the
//! install build it. Every mutating project-scoped command (`run`,
//! `activate`, `bake`, `resync`, the infra verbs, `build`) calls this first
//! so users don't have to remember `weft build` as a prerequisite.
//!
//! Semantics:
//!   - Sync first (`super::copy::this_copy`): the folder's edits go up to
//!     its working copy, so the build is of what the folder holds.
//!   - `POST /projects/{id}/builds` naming that working copy: the install
//!     stamps it into a version, compiles it, resolves its `@asset`s, holds
//!     the run the build is for (when there is one) to the program, and
//!     starts a build of every stale image with its own builder, answering
//!     at once with those builds. A program that does not compile, or a run
//!     that could not start, is refused there, before any image is built.
//!     The install registers the version itself once its images are built;
//!     this command follows it until it is registered or failed. Nothing is
//!     compiled, built or loaded on this machine.
//!   - A part of the program that does not compile (an included file that
//!     does not parse, a node that did not load) only breaks that part: the
//!     command goes on, a run that needs the part is refused when it is
//!     cut, and once the command did what it came for, every broken part is
//!     printed as a warning ([`ProjectHandle::warn_broken`]).
//!   - Registering a build is UNCONDITIONAL even while runs run on
//!     the previous image: every worker task is stamped with the image it
//!     was enqueued for and only claimable by a process baked from it, so
//!     in-flight work finishes on the old processes while new work flows to
//!     fresh, current-image processes.

use anyhow::{Context, Result};

use weft_core::builds::{BuildRequest, BuildsUnderway, BuiltProgram, ImageBuild, VersionBuildState, VersionBuildStatus};
use weft_core::projects::{BuildState, BuildStateResponse};

use super::Ctx;
use crate::client::DispatcherClient;
use crate::progress::{Progress, SayAgain};

pub struct ProjectHandle {
    pub id: String,
    pub name: String,
    pub client: DispatcherClient,
    /// This folder's working copy, as the sync before the build left it.
    pub copy: weft_core::working_copy::WorkingCopy,
    /// The version the install stamped the working copy as and built.
    pub version: String,
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

    /// Say every part of the build that does not compile, once the command
    /// did what it came for: whatever it ran did not need them (a cut that
    /// needs one is refused, naming it), and they are never hidden. An
    /// included file that does not parse may hold a trigger nobody can see
    /// until it does, so the line never claims the command missed nothing.
    /// The parts are the install's own reading of the version, not this
    /// machine's.
    pub fn warn_broken(&self, progress: &Progress) {
        warn_broken(progress, &self.built.definition.broken);
        for package in &self.built.left_out {
            progress.warn(&format!(
                "the worker was built without it, so nothing that uses its nodes runs: {}",
                package.reason(self.built.worker_log.as_deref())
            ));
        }
    }
}

/// [`ProjectHandle::warn_broken`] for a verb that kept only the parts.
pub fn warn_broken(progress: &Progress, broken: &[weft_core::project::Broken]) {
    for line in broken_warnings(broken) {
        progress.warn(&line);
    }
}

/// The warning lines for `broken`, the parts of a build that do not
/// compile: one per part, its reason's own lines kept under it.
fn broken_warnings(broken: &[weft_core::project::Broken]) -> Vec<String> {
    broken
        .iter()
        .map(|part| format!("does not compile, so nothing that needs it runs: {}", part.why.replace('\n', "\n  ")))
        .collect()
}

/// How often a version build being followed is asked where it is, and
/// how often an ask the install could not be reached for is sent again.
const BUILD_LOOK_EVERY: std::time::Duration = std::time::Duration::from_secs(5);

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
    let mut say = SayAgain::said_at(started.elapsed());
    let mut out_of_reach = false;
    loop {
        tokio::time::sleep(BUILD_LOOK_EVERY).await;
        let elapsed = started.elapsed();
        let due = say.due(elapsed);
        let answer = match version_state(client, id, build).await {
            Ok(answer) => answer,
            Err(e) => {
                let why = look_again_after(e).map_err(|e| e.context("read where the build is"))?;
                // Said at once when the install goes out of reach, then on
                // the usual beat while it stays out.
                if !out_of_reach || due {
                    say.said(elapsed);
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
            say.said(elapsed);
            progress.build_wait(elapsed, &following.building(), &following.built);
        }
    }
}

/// A request to the install that failed: `Ok` with why, to say, when the
/// install is only out of reach for now and the request is to be sent
/// again: its connection failed on the way, or it was taken and not
/// answered within its time limit ([`crate::client::connection_failed`],
/// [`crate::client::NoAnswerInTime`]), or the gateway in front of it
/// answered that it cannot reach it right now (a 502, 503 or 504 the
/// install did not give itself: down or starting). Every request this is
/// read on (asking for the build, which joins the one already asked for,
/// reading where it is, looking at a task) is safe to send again, which
/// only its caller knows. The client already waits out, for every
/// request, the answers that are safe to send again whatever the request
/// ([`crate::client::busy_answer`]: the platform's 429, the install's own
/// 503), so those never reach here, and this covers the rest. The error
/// itself when anything else answered, the install's own 500 or 502
/// included: that one does not lift by waiting (its database refusing for
/// good, its storage down), and looking again on it would follow forever.
// SYNC: look_again_after <-> extension-vscode/src/dispatcher.ts (worthTryingAgain)
pub(crate) fn look_again_after(e: anyhow::Error) -> Result<String> {
    use reqwest::StatusCode;
    let busy = crate::client::refusal(&e).is_some_and(|refused| {
        !refused.is_the_installs()
            && matches!(refused.status(), StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT)
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
    let mut say = SayAgain::default();
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
        if say.due(elapsed) {
            say.said(elapsed);
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

/// Sync the cwd project's folder and have the install build it with
/// `node_set` and register it (see the module doc). Nothing here parks,
/// drains, or prompts.
pub async fn ensure_registered(
    ctx: &Ctx,
    progress: &Progress,
    node_set: weft_core::builds::NodeSet,
) -> Result<ProjectHandle> {
    ensure_built(ctx, progress, BuildRequest { node_set, ..Default::default() }).await
}

/// [`ensure_registered`] for the build `request` asks (a new root, the
/// run it is for); the working copy it builds is this folder's.
pub async fn ensure_built(ctx: &Ctx, progress: &Progress, mut request: BuildRequest) -> Result<ProjectHandle> {
    let super::copy::FolderCopy { project_id: id, client, copy } = super::copy::this_copy(ctx, Some(progress)).await?;
    request.working_copy = weft_core::working_copy::WorkingCopyRef { working_copy: Some(copy.id) };
    let name = ctx.project()?.manifest.package.name.clone();
    let path = format!("/projects/{id}/builds");
    progress.build_start(&name);
    progress.dispatcher_call_start(&path);
    let started = std::time::Instant::now();
    // The ask answers at once: the version registered (200), or the image
    // builds it waits on (202), which the install registers it after.
    let asked = ask_to_build(&client, &path, &serde_json::to_value(&request)?, progress, started).await.inspect_err(|e| {
        if let Some(text) = compile_failure(e) {
            progress.structured_error(serde_json::json!({ "message": text, "what": "Compiling project", "stage": "compile" }));
        }
    })?;
    let (built, following) = match asked {
        Asked::Registered(built) => (*built, Following::default()),
        Asked::Underway(underway) => {
            let mut following = Following::new(underway.images, started.elapsed());
            let built = follow_version(&client, &id, underway.build, progress, &mut following, started).await?;
            (built, following)
        }
    };
    progress.dispatcher_call_done(serde_json::json!({ "projectId": id }));
    progress.build_done(&name, &following.built);
    if let Some(note) = replaced_infra_images_note(&built.replaced_infra_images, ctx.on()) {
        progress.warn(&note);
    }
    let version = built.version.clone();
    Ok(ProjectHandle { id, name, client, copy, version, built })
}

/// The install's words when the build was refused because the program as
/// a whole does not compile: what its 422 says from `compile failed:` on,
/// one `line:column message` per diagnostic.
// SYNC: "compile failed:" <-> crates/weft-dispatcher/src/build/source.rs compile
fn compile_failure(e: &anyhow::Error) -> Option<String> {
    let refused = crate::client::refusal(e).filter(|refused| refused.status() == reqwest::StatusCode::UNPROCESSABLE_ENTITY)?;
    let text = refused.to_string();
    let at = text.find("compile failed:")?;
    Some(text[at..].to_string())
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

    /// The install's refusal of a program that does not compile is told
    /// apart from every other failed build, so the editor shows it as a
    /// compile failure.
    #[tokio::test]
    async fn a_compile_failure_is_read_off_the_install_refusal() {
        use crate::client::fake::{install, Then};
        let ask = |answer: &'static str| async move {
            DispatcherClient::new(install(Then::Answer(answer)).await, None)
                .post_json_success("/projects/p/builds", &serde_json::json!({}))
                .await
                .unwrap_err()
                .context("the build failed")
        };
        let failed = ask("HTTP/1.1 422 Unprocessable Entity\r\ncontent-length: 39\r\n\r\nbuild: compile failed:\n3:1 unknown node").await;
        assert_eq!(compile_failure(&failed).as_deref(), Some("compile failed:\n3:1 unknown node"));
        let other = ask("HTTP/1.1 422 Unprocessable Entity\r\ncontent-length: 14\r\n\r\nunknown assets").await;
        assert_eq!(compile_failure(&other), None);
        let busy = ask("HTTP/1.1 409 Conflict\r\ncontent-length: 15\r\n\r\ncompile failed:").await;
        assert_eq!(compile_failure(&busy), None, "only the install's 422 is a compile failure");
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
        // A look the install took and never answered (its copy stopped
        // behind a gateway holding the connection) is looked at again
        // once its limit is up.
        let held = DispatcherClient::new(install(Then::Hold).await, None).answering_within(std::time::Duration::from_millis(200));
        let why = look_again_after(version_state(&held, "p", build).await.unwrap_err()).unwrap();
        assert!(why.contains("did not answer within"), "{why}");
        let down = look(install(Then::Answer("HTTP/1.1 502 Bad Gateway\r\ncontent-length: 0\r\n\r\n")).await).await;
        assert!(look_again_after(down).is_ok());
        // The gateway's 503 (the install down or starting) to the build ask
        // (a write this command knows is safe to send again) is read here.
        let busy = DispatcherClient::new(install(Then::Answer("HTTP/1.1 503 Service Unavailable\r\ncontent-length: 0\r\n\r\n")).await, None)
            .post_json_success("/projects/p/builds", &serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(look_again_after(busy).is_ok());
        // The install's own 500 (its database refusing for good) ends the
        // follow with its words.
        let for_good = look(
            install(Then::Answer("HTTP/1.1 500 Internal Server Error\r\nx-weft-install: 1\r\ncontent-length: 14\r\n\r\nquota exceeded"))
                .await,
        )
        .await;
        assert_eq!(format!("{:#}", look_again_after(for_good).unwrap_err()), "quota exceeded");
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
