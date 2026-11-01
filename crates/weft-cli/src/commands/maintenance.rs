//! `weft maintenance [status] | on [--running wait|cancel] | off | check`:
//! everything on the install switched off while weft itself is updated
//! (`weft_core::maintenance`).
//!
//! `on` takes down every trigger that is on and stops every infra copy that
//! runs, then (under `--running wait`, the default) waits until the runs
//! going have finished and the copies are stopped. `off` builds each of
//! those projects again on this weft, starts those copies on that build,
//! then brings those triggers back, followed to its end.
//! `check` is what `./setup.sh` and the GCP install workflow run before
//! replacing anything: it fails while something is on.

use std::time::Duration;

use weft_core::builds::VersionBuildState;
use weft_core::maintenance::{LiftAnswer, LiftDoing, LiftProgress, LiftReport, MaintenanceOn, MaintenanceOnRequest, MaintenanceState, ProjectLifted};
use weft_core::projects::BuildState;
use weft_core::take_down::Running;

use super::Ctx;

pub enum MaintenanceAction {
    Status,
    On { running: Option<Running> },
    Off,
    /// With `key_stdin`, the operator key comes on stdin (CI); `update`
    /// is the update's own step, as the refusal names it.
    Check { key_stdin: bool, update: String },
}

/// How long `on` waits between two looks at what is still going down.
const LOOK_EVERY: Duration = Duration::from_secs(3);

/// How long one look at a lift waits on the install: short, since each
/// look also reads where it is.
const LIFT_LOOK: Duration = Duration::from_secs(1);

pub async fn run(ctx: Ctx, action: MaintenanceAction) -> anyhow::Result<()> {
    match action {
        MaintenanceAction::Status => status(&ctx).await,
        MaintenanceAction::On { running } => on(&ctx, running).await,
        MaintenanceAction::Off => off(&ctx).await,
        MaintenanceAction::Check { key_stdin, update } => check(&ctx, key_stdin, &update).await,
    }
}

async fn read_state(client: &crate::client::DispatcherClient) -> anyhow::Result<MaintenanceState> {
    crate::client::read_answer(client.get_json("/install/maintenance").await?, "the install's maintenance")
}

/// `weft maintenance` / `weft maintenance status`.
async fn status(ctx: &Ctx) -> anyhow::Result<()> {
    let state = read_state(&ctx.client()?).await?;
    if ctx.json_out(&state)? {
        return Ok(());
    }
    match state.since_unix {
        Some(since) => {
            println!("in maintenance since {}; `{}` lifts it", when(since), ctx.weft("maintenance off"));
            if state.left.is_empty() {
                println!("  it brings nothing back: nothing was on when it began");
            }
            for project in &state.left {
                println!("  {} ({}): brings back {}", project.name, project.id, left_words(&project.copies, &project.triggers));
            }
        }
        None => {
            println!("not in maintenance");
            // What a project could not bring back when the last one was
            // lifted stays down, and is that project's own to bring back.
            for project in &state.left {
                println!(
                    "  {} ({}): still down since the last maintenance: {}; fix what kept it down, then `{}` in its folder \
                     brings it back",
                    project.name,
                    project.id,
                    left_words(&project.copies, &project.triggers),
                    ctx.weft("resync")
                );
            }
        }
    }
    let on = state.going.words();
    println!("on now: {}", if on.is_empty() { "nothing" } else { &on });
    Ok(())
}

/// `weft maintenance on`.
async fn on(ctx: &Ctx, running: Option<Running>) -> anyhow::Result<()> {
    let client = ctx.client()?;
    let request = MaintenanceOnRequest { running };
    // The install answers once every project is taken down, which can
    // take a while with infra to stop.
    let spinner = (!ctx.json()).then(crate::spinner::Spinner::start);
    if let Some(spinner) = &spinner {
        spinner.show_one("taking down every trigger and stopping every infra copy", crate::progress::now_unix() as i64);
    }
    let answer: MaintenanceOn =
        // It goes through every project of the install before it answers.
        crate::client::read_answer(
            client.waiting_on_its_work().post_json("/install/maintenance", &serde_json::to_value(&request)?).await?,
            "what `weft maintenance on` did",
        )?;
    drop(spinner);
    if !ctx.json() {
        println!("in maintenance since {}", when(answer.since_unix));
        for project in &answer.projects {
            let mut did = Vec::new();
            if !project.triggers.is_empty() {
                did.push(format!("took down {}", weft_core::activation::OwnerTriggers::list_words(&project.triggers)));
            }
            if !project.copies.is_empty() {
                did.push(format!("stopped infra {}", copy_list(&project.copies)));
            }
            if project.runs_cancelled > 0 {
                did.push(format!("cancelled {} run(s)", project.runs_cancelled));
            }
            if !did.is_empty() {
                println!("  {} ({}): {}", project.name, project.id, did.join("; "));
            }
            for refused in &project.refused {
                eprintln!("  {} ({}): still on: {refused}", project.name, project.id);
            }
        }
    }
    // What goes down on its own (a run finishing, a copy stopping) is
    // waited for, however long it takes: the update needs it over.
    // Ctrl+C stops the waiting, never what it waits for.
    if request.running() == Running::Wait {
        let spinner = (!ctx.json()).then(crate::spinner::Spinner::start);
        let since = crate::progress::now_unix() as i64;
        let mut said = false;
        loop {
            let state = read_state(&client).await?;
            if !state.going.still_going_down() {
                break;
            }
            if !said {
                let cancel = if state.going.runs > 0 {
                    format!("; `{}` cancels the runs", ctx.weft("maintenance on --running cancel"))
                } else {
                    String::new()
                };
                eprintln!("waiting for what is still going down (Ctrl+C stops waiting{cancel})");
                said = true;
            }
            if let Some(spinner) = &spinner {
                spinner.show_one(format!("still going down: {}", state.going.words()), since);
            }
            tokio::time::sleep(LOOK_EVERY).await;
        }
    }
    ctx.json_out(&answer)?;
    check_left(ctx, &answer)?;
    if !ctx.json() {
        println!("the install is in maintenance: update it now, then `{}` brings back what this took down", ctx.weft("maintenance off"));
    }
    Ok(())
}

/// Fail when `on` left something on, which another `on` takes down once
/// what held it is over.
fn check_left(ctx: &Ctx, answer: &MaintenanceOn) -> anyhow::Result<()> {
    if answer.left_something_on() {
        anyhow::bail!(
            "the install is in maintenance, but some of it is still on (said above); once that is over, run `{}` again (it adds what is on and clears nothing)",
            ctx.weft("maintenance on")
        );
    }
    Ok(())
}

/// `weft maintenance off`.
async fn off(ctx: &Ctx) -> anyhow::Result<()> {
    let client = ctx.client()?;
    let answer: LiftAnswer =
        crate::client::read_answer(client.post_json("/install/maintenance/off", &serde_json::json!({})).await?, "the lift's answer")?;
    let task = match answer {
        LiftAnswer::NotOn => {
            if !ctx.json_out(&LiftReport { projects: Vec::new() })? {
                println!("the install is not in maintenance; nothing to bring back");
            }
            return Ok(());
        }
        LiftAnswer::UnderWay { task } => task,
    };
    let follow = format!("/install/maintenance/follow/{task}");
    if ctx.json() {
        let report: LiftReport =
            crate::client::read_answer(super::follow::follow_at(&client, &follow, LIFT_LOOK, |_| {}, async |_| Ok(())).await?, "the lift's report")?;
        ctx.json_out(&report)?;
        return Ok(());
    }
    eprintln!("building each project again on this weft, then starting the infra the maintenance stopped, then bringing back its triggers");
    let mut view = LiftView { spinner: crate::spinner::Spinner::start(), said: 0, logs_said: Vec::new() };
    let warn = |message: &str| eprintln!("{message}");
    let report = super::follow::follow_at(&client, &follow, LIFT_LOOK, warn, async |progress| view.going(&client, progress).await).await?;
    let report: LiftReport = crate::client::read_answer(report, "the lift's report")?;
    view.spinner.show(Vec::new());
    // What the lift finished after the last look.
    for project in &report.projects[view.said.min(report.projects.len())..] {
        view.say_lifted(project);
    }
    drop(view);
    let down: Vec<&str> = report.projects.iter().filter(|project| !project.failed.is_empty()).map(|project| project.name.as_str()).collect();
    println!("the maintenance is lifted");
    if !down.is_empty() {
        eprintln!(
            "{} stayed down, without holding anything else: fix what is said above in its folder, then `{}` there brings \
             back what the update took down (or `{}` what you want left off)",
            down.join(", "),
            ctx.weft("resync"),
            ctx.weft("deactivate")
        );
    }
    Ok(())
}

/// What `weft maintenance off` shows while the lift goes: each project
/// once the lift is done with it, the step it is on under a spinner, and
/// where each image it builds keeps its log.
struct LiftView {
    spinner: crate::spinner::Spinner,
    /// How many of the lift's done projects are said.
    said: usize,
    /// The image builds whose log is said.
    logs_said: Vec<String>,
}

impl LiftView {
    async fn going(&mut self, client: &crate::client::DispatcherClient, progress: Option<serde_json::Value>) -> anyhow::Result<()> {
        let Some(progress) = progress else { return Ok(()) };
        let progress: LiftProgress = crate::client::read_answer(progress, "where the lift is")?;
        for project in progress.done.iter().skip(self.said) {
            self.say_lifted(project);
        }
        self.said = self.said.max(progress.done.len());
        let mut lines = Vec::new();
        for at in &progress.going {
            let (label, log) = self.step(client, at).await;
            lines.push(crate::spinner::Shown { label, since_unix: at.since_unix, log });
        }
        self.spinner.show(lines);
        Ok(())
    }

    /// How `at` reads on its line, and the log beside it: for a build
    /// whose images build, which ones (each one's log said once).
    async fn step(&mut self, client: &crate::client::DispatcherClient, at: &weft_core::maintenance::LiftAt) -> (String, Option<String>) {
        let words = format!("{}: {}", at.name, at.doing.words());
        let LiftDoing::Building { build: Some(build) } = &at.doing else { return (words, None) };
        // A look the install could not answer leaves the line as the
        // step's words until the next.
        let answer = client.get_json(&format!("/projects/{}/builds/{build}", at.id)).await.ok();
        let Some(state) = answer.and_then(|answer| serde_json::from_value::<VersionBuildState>(answer).ok()) else { return (words, None) };
        let building: Vec<_> = state.images.iter().filter(|image| image.state.state == Some(BuildState::Running)).collect();
        if building.is_empty() {
            return (words, None);
        }
        for image in &building {
            if let Some(url) = &image.state.log_url {
                if !self.logs_said.contains(&image.build.name) {
                    self.logs_said.push(image.build.name.clone());
                    self.spinner.say(&format!("  {}: building image {}; its log: {url}", at.name, short_image(&image.build.image)), false);
                }
            }
        }
        let names: Vec<&str> = building.iter().map(|image| short_image(&image.build.image)).collect();
        (format!("{}: building image {}", at.name, names.join(", ")), building.iter().find_map(|image| image.state.log_url.clone()))
    }

    /// The lift is done with `project`: what came back, and what did not.
    fn say_lifted(&self, project: &ProjectLifted) {
        let (did, failed) = lifted_lines(project);
        if !did.is_empty() {
            self.spinner.say(&format!("  {} ({}): {}", project.name, project.id, did.join("; ")), false);
        }
        for failed in &failed {
            self.spinner.say(&format!("  {} ({}): did not come back: {failed}", project.name, project.id), true);
        }
    }
}

/// An image ref as a person reads it: its name, without where it is
/// pushed or its tag.
fn short_image(image: &str) -> &str {
    let name = image.rsplit('/').next().unwrap_or(image);
    name.split(':').next().unwrap_or(name)
}

/// What a lift did in one project, as lines: what came back (and what it
/// had nothing to do for), then what did not, each with why.
pub(crate) fn lifted_lines(project: &weft_core::maintenance::ProjectLifted) -> (Vec<String>, Vec<String>) {
    let mut did = Vec::new();
    if project.rebuilt {
        did.push("built again on this weft".to_string());
    }
    if !project.started.is_empty() {
        did.push(format!("started infra {}", copy_list(&project.started)));
    }
    if !project.turned_on.is_empty() {
        did.push(format!("turned on {}", weft_core::activation::OwnerTriggers::list_words(&project.turned_on)));
    }
    did.extend(project.skipped.iter().map(|skipped| format!("skipped {skipped}")));
    (did, project.failed.clone())
}

/// `weft maintenance check`: fail while something is on, so an update
/// never replaces a weft that is running something. An install that is not
/// set up or not running has nothing on; one whose weft predates the
/// maintenance cannot be asked, and is let through, saying so.
async fn check(ctx: &Ctx, key_stdin: bool, update: &str) -> anyhow::Result<()> {
    match ctx.install_access() {
        Ok(_) => {}
        // This machine's install with no address yet: never started.
        Err(e) if ctx.on().is_none() => {
            println!("this machine's install is not set up ({e:#}); nothing to check");
            return Ok(());
        }
        Err(e) => return Err(e),
    }
    let client = ctx.client_keyed(key_stdin)?;
    let state = match client.get_json("/install/maintenance").await {
        Ok(state) => crate::client::read_answer::<MaintenanceState>(state, "the install's maintenance")?,
        Err(e) => {
            if let Some(unreachable) = e.chain().find_map(|cause| cause.downcast_ref::<crate::client::InstallUnreachable>()) {
                println!("{} is not running; nothing to check", unreachable.named());
                return Ok(());
            }
            if crate::client::refusal(&e).is_some_and(|refused| predates_maintenance(refused.status())) {
                println!("the weft running this install predates `weft maintenance`, so whether something is on cannot be checked; going ahead");
                return Ok(());
            }
            return Err(e);
        }
    };
    // Asked by raw address (the cloud workflow), the person still acts on
    // the install through a project's target.
    let spell = |verb: &str| match ctx.dispatcher.is_some() && ctx.on().is_none() {
        true => format!("weft {verb} --on <target> (from a project that names it)"),
        false => ctx.weft(verb),
    };
    if let Some(why) = weft_core::maintenance::update_refusal(&state, spell, update) {
        anyhow::bail!("{why}");
    }
    if !ctx.json_out(&state)? {
        println!("nothing is on; the update can go ahead");
    }
    Ok(())
}

/// Whether `status`, a weft's answer to `GET /install/maintenance`, says
/// that weft predates `weft maintenance`, so it has no such route. One
/// without the route answers 404, or 405: until the maintenance came, the
/// dispatcher's catch-all public route (`/{*mount_path}`, which takes a
/// trigger's fire) served POST alone and answered any other method on any
/// path it did not know with 405.
fn predates_maintenance(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::NOT_FOUND || status == reqwest::StatusCode::METHOD_NOT_ALLOWED
}

/// A unix time as a person reads it.
fn when(unix: i64) -> String {
    chrono::DateTime::from_timestamp(unix, 0).map(|at| at.format("%Y-%m-%d %H:%M UTC").to_string()).unwrap_or_else(|| unix.to_string())
}

fn copy_list(copies: &[weft_core::infra::wire::CopyRef]) -> String {
    copies
        .iter()
        .map(|copy| match &copy.instance {
            Some(instance) => format!("'{}' of instance '{instance}'", copy.node),
            None => format!("'{}'", copy.node),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// What a lift brings back in one project, as a person reads it.
fn left_words(copies: &[weft_core::infra::wire::CopyRef], triggers: &[weft_core::activation::OwnerTriggers]) -> String {
    let mut parts = Vec::new();
    if !copies.is_empty() {
        parts.push(format!("infra {}", copy_list(copies)));
    }
    if !triggers.is_empty() {
        parts.push(format!("triggers {}", weft_core::activation::OwnerTriggers::list_words(triggers)));
    }
    parts.join(", then ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::activation::OwnerTriggers;
    use weft_core::infra::wire::CopyRef;
    use weft_core::instance::{InstanceId, Owner};

    #[test]
    fn an_image_is_said_by_its_name() {
        assert_eq!(short_image("weft-infra-studio:b0d68f"), "weft-infra-studio");
        assert_eq!(short_image("us-docker.pkg.dev/x/r/weft-worker:aa"), "weft-worker");
        assert_eq!(short_image("weft-worker"), "weft-worker");
    }

    #[test]
    fn a_weft_without_the_route_predates_the_maintenance() {
        assert!(predates_maintenance(reqwest::StatusCode::NOT_FOUND));
        assert!(predates_maintenance(reqwest::StatusCode::METHOD_NOT_ALLOWED), "an older weft's catch-all answers a GET with 405");
        for status in [reqwest::StatusCode::UNAUTHORIZED, reqwest::StatusCode::FORBIDDEN, reqwest::StatusCode::INTERNAL_SERVER_ERROR, reqwest::StatusCode::CONFLICT] {
            assert!(!predates_maintenance(status), "{status}");
        }
    }

    #[test]
    fn what_a_lift_brings_back_reads_infra_first() {
        let copies = [CopyRef { node: "db".into(), instance: None }, CopyRef { node: "cache".into(), instance: Some(InstanceId::new("ada").unwrap()) }];
        let triggers = [OwnerTriggers { owner: Owner::Shared, triggers: vec!["door".into()] }];
        let words = left_words(&copies, &triggers);
        assert!(words.starts_with("infra 'db', 'cache' of instance 'ada', then triggers"), "{words}");
        assert!(words.contains("'door'"), "{words}");
        assert_eq!(left_words(&[], &triggers), format!("triggers {}", OwnerTriggers::list_words(&triggers)));
    }
}
