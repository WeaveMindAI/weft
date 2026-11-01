//! Progress emitter. CLI verbs call into here to broadcast phase
//! events. In `--json` mode each emit prints one NDJSON line on
//! stdout; in human mode it prints a brief status line. The VS
//! Code extension parses the NDJSON stream and feeds each event
//! into its action-bar reducer.
//!
//! Adding a new phase: extend `Phase`, add an emit helper if the
//! call site wants something more typed than `emit(phase, detail)`,
//! and (extension side) add one match arm in the reducer. CLI
//! commands threading progress receive `&Progress` and call
//! `progress.<phase>(detail)`.
//!
//! Output schema (per line, on stdout):
//!   { "tsUnix": <u64>,
//!     "verb":    <ActionVerb>,
//!     "phase":   <Phase>,
//!     "detail":  <object | null> }
//!
//! The two modes never mix on one invocation: in `--json` mode stdout
//! carries ONLY the NDJSON event stream (human lines are skipped), and
//! in human mode stdout carries ONLY the human status lines (no
//! NDJSON). So `--json` stdout is a clean machine stream the extension
//! parses without filtering.

use serde::Serialize;
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

/// Which CLI verb is emitting events. Webview state machine
/// disambiguates so the bar can render the right label per phase.
// SYNC: ActionVerb <-> packages/weft-graph/src/protocol.ts ActionVerb, crates/weft-dispatcher/src/api/project.rs compute_available_actions
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionVerb {
    Run,
    Activate,
    Bake,
    /// Cancel an in-flight `activate` (status=Activating). Wipes
    /// partial trigger registrations and leaves those triggers down.
    CancelActivate,
    /// Cancel an in-flight build (transition=building): the install
    /// stops the project's image builds.
    CancelBuild,
    Deactivate,
    Resync,
    Build,
    InfraStart,
    InfraStop,
    InfraTerminate,
    InfraUpgrade,
    /// Cancel in-flight infra work: halt claimed lifecycle commands,
    /// cancel unclaimed ones, interrupt the InfraSetup provisioning
    /// run. HALT, not rollback: per-node partial state stays.
    InfraCancel,
    /// Multi-level project cleanup (deactivate + unregister + optional
    /// infra/journal/image/local wipes).
    Rm,
}

/// What a wait for the runs going is on ([`Progress::drain_wait`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainOn {
    /// The runs reaching the infra an infra verb takes down.
    Infra,
    /// The runs on the old version a resync replaces.
    Resync,
    /// The runs the triggers a deactivate takes down fired.
    Deactivate,
}

impl DrainOn {
    fn as_str(self) -> &'static str {
        match self {
            DrainOn::Infra => "infra",
            DrainOn::Resync => "resync",
            DrainOn::Deactivate => "deactivate",
        }
    }
}

/// All phases any verb can emit. Closed enum so the extension's
/// reducer covers every variant via match-exhaustiveness.
// SYNC: Phase <-> packages/weft-graph/src/protocol.ts CliPhase
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    /// The install started building the project. Detail carries
    /// `{ "project": <name> }`.
    BuildStart,
    /// The install finished building the project. Detail carries
    /// `{ "project": <name>, "built": [...] }`: the image refs it had to
    /// build (empty when every image was already there).
    BuildDone,
    /// One image of the build is building on the install's builder.
    /// Detail carries `{ "image", "build", "logUrl"? }`: the image ref,
    /// the builder's id for the build, and where its log is read.
    BuildImage,
    /// One image of the build was built (its build succeeded). Detail
    /// carries `{ "image", "seconds" }`: about how long it took.
    BuildImageDone,
    /// Periodic heartbeat while the install builds. Detail carries
    /// `{ "elapsedSeconds", "images": [...], "built": [...] }`: the images
    /// still building, and the ones this build has finished so far.
    BuildWait,
    /// The install cannot be reached to see where the build is (no
    /// answer, or its gateway says it is down); the command keeps looking.
    /// Detail carries `{ "elapsedSeconds", "message" }`: since the build
    /// began, and why the last look failed. The next `BuildWait` or
    /// `BuildImage` says the install answers again.
    BuildUnreachable,
    /// HTTP request to the dispatcher started.
    DispatcherCallStart,
    /// HTTP request to the dispatcher finished. Body in `detail`
    /// (e.g. `{ "runId": "..." }` for run, `{ "signalCount": N }`
    /// for activate).
    DispatcherCallDone,
    /// Infra provision started (one event covers every infra node
    /// in the verb; node ids in detail).
    InfraProvisionStart,
    InfraProvisionDone,
    /// The command is about to wait for the runs reaching the infra it
    /// takes down to end (`--running wait`), however long they take.
    /// Emitted before the call that waits, because the wait itself
    /// happens on the dispatcher: without it the command looks hung.
    DrainWait,
    /// The run is being prepared on the install, and what its start is
    /// doing, said once it changes and every [`SAY_AGAIN_EVERY`] while it
    /// lasts. Detail carries `{ "elapsedSeconds", "doing" }`.
    RunStarting,
    /// Trigger registration started (signals about to be wired up).
    TriggerRegisterStart,
    /// The activation goes on, and what it waits on changed. Detail
    /// carries `{ "elapsedSeconds", "waiting" }`: since it was claimed,
    /// and what it waits on now ("its setup run <id>").
    TriggerRegisterWait,
    TriggerRegisterDone,
    /// Waiting on a long-running infra supervisor command (stop /
    /// terminate / start), which can depend on draining in-flight
    /// runs and so is unbounded. Detail:
    /// `{ "verb": "...", "elapsedSeconds": S }`.
    InfraWait,
    /// Something the person should know that is not a failure: the verb
    /// carries on and still ends `Complete`. Detail carries
    /// `{ "message": "..." }`.
    Warning,
    /// CLI verb finished cleanly.
    Complete,
    /// CLI verb failed. Detail carries `{ "message": "..." }`.
    /// CLI exits non-zero AFTER emitting this so the extension can
    /// distinguish a clean error from an unexpected crash.
    Error,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Event<'a> {
    ts_unix: u64,
    /// Absent on the one event a verb with no progress channel (a
    /// reader such as `events` or `logs`) emits: its failure, through
    /// [`report_plain_error`].
    #[serde(skip_serializing_if = "Option::is_none")]
    verb: Option<ActionVerb>,
    phase: Phase,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<&'a Value>,
}

/// What a failure that never reached this machine's daemon says, first,
/// before the transport noise.
///
/// Worth its own sentence because the three reasons a request does not
/// arrive (the daemon is not started, this machine only has the editor
/// extension, the address is wrong) are all fixed by the person rather
/// than by the program, and none of them is what "error sending request
/// for url" reads like.
///
/// A host wrapping the CLI does NOT match on this sentence: the same
/// answer rides the `--json` error event as `daemonUnreachable`, which
/// is a fact rather than wording, so this line is free to be rephrased.
const DAEMON_UNREACHABLE: &str = "the weft daemon is not answering";

/// The install a request never reached, when one did not: nothing was
/// refused, nothing answered. A status the install returns, however bad,
/// is not this.
fn unreachable_install(e: &anyhow::Error) -> Option<&crate::client::InstallUnreachable> {
    e.chain().find_map(|cause| cause.downcast_ref::<crate::client::InstallUnreachable>())
}

/// The one report for a failure that reached `main` unreported: a verb
/// with no progress channel, or a failure before a channel existed.
/// Same shape as a channel's own error, so every verb fails the same
/// way: under `--json` one `phase: "error"` event on stdout, otherwise
/// one lowercase `error:` line on stderr. The cause chain rides along
/// in the message (`{e:#}`), since readers build their errors from
/// context and the chain is where the "why" lives.
pub fn report_plain_error(json: bool, e: &anyhow::Error) {
    let detail = error_detail(e);
    if json {
        let ev = Event { ts_unix: now_unix(), verb: None, phase: Phase::Error, detail: Some(&detail) };
        println!("{}", serde_json::to_string(&ev).expect("Event serializes"));
    } else {
        eprintln!("error: {}", detail["message"].as_str().expect("error_detail carries a message"));
    }
}

/// The `detail` of the `error` event a failure becomes, on every path
/// that reports one: the message (with the cause chain, `{e:#}`, since
/// that is where the "why" lives), plus the facts a host acts on
/// without matching the wording.
// SYNC: daemonUnreachable, installUnreachable <-> extension-vscode/src/cli.ts CliFailure
pub fn error_detail(e: &anyhow::Error) -> Value {
    let unreachable = unreachable_install(e);
    let local = unreachable.is_some_and(|install| install.is_local());
    let message = match unreachable {
        Some(_) if local => format!(
            "{DAEMON_UNREACHABLE}. Start it with `weft daemon start`, or install it if this \
             machine only has the editor extension. ({e:#})"
        ),
        // A remote install: nothing on this machine would start it, so
        // the daemon's advice would send the person the wrong way. The
        // chain names it (`InstallUnreachable`'s own words).
        Some(_) => format!("{e:#}; check the network and run the command again"),
        None => format!("{e:#}"),
    };
    serde_json::json!({
        "message": message,
        // This machine's daemon did not answer: a host may offer to start it.
        "daemonUnreachable": local,
        // Any install did not answer, this machine's or a remote one.
        "installUnreachable": unreachable.is_some(),
    })
}

/// One emitter per CLI invocation. Cheap to clone; commands thread
/// it through their helper functions so any layer can emit.
#[derive(Clone)]
pub struct Progress {
    pub json: bool,
    pub verb: ActionVerb,
    /// The `--on <target>` the command was given, so a line that names a
    /// command to run names it for the install the person is acting on
    /// ([`Progress::weft`]).
    on: Option<String>,
    /// Shared latch tracking whether ANY error event has already been
    /// emitted on this verb. Lets `with_progress` skip the
    /// auto-error trap when the body already emitted a structured
    /// error (so the editor sees the structured one, not a flattened
    /// duplicate that would overwrite it).
    error_emitted: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

/// An error a verb's progress channel has already shown the user. It
/// wraps the original so `{:?}` and `source()` still reach the cause,
/// and `main` recognises it to exit non-zero without printing the same
/// message a second time (once as the `error:` phase, once as anyhow's
/// `Error:` report).
#[derive(Debug)]
pub struct Reported(pub anyhow::Error);

impl std::fmt::Display for Reported {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for Reported {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

impl Progress {
    pub fn new(verb: ActionVerb, json: bool, on: Option<String>) -> Self {
        Self {
            json,
            verb,
            on,
            error_emitted: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// True if any error event has already been emitted on this
    /// emitter. Used by `with_progress` to avoid double-emitting
    /// when the body already produced a structured error.
    pub fn has_emitted_error(&self) -> bool {
        self.error_emitted.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// `weft <verb>` as a line of this command names it: with the `--on
    /// <target>` the command was given (`crate::commands::weft_on`).
    pub fn weft(&self, verb: &str) -> String {
        crate::commands::weft_on(self.on.as_deref(), verb)
    }

    /// Core emit. Most call sites prefer the typed helpers below
    /// for compile-time safety on which phases each verb produces.
    pub fn emit(&self, phase: Phase, detail: Option<Value>) {
        let ev = Event {
            ts_unix: now_unix(),
            verb: Some(self.verb),
            phase,
            detail: detail.as_ref(),
        };
        if self.json {
            // Single line per event; flush so the extension reads
            // them in order even when stdout buffering is enabled
            // (line-buffered terminals behave differently from
            // pipe-buffered subprocesses).
            println!("{}", serde_json::to_string(&ev).expect("Event serializes"));
            use std::io::Write;
            let _ = std::io::stdout().flush();
        } else {
            // Human-readable single-line status. Skips noisy phases
            // (start/done pairs collapse, complete is silent) so
            // the terminal doesn't fill with chatter. Warnings and
            // errors go where a shell expects them, on stderr, so a
            // script capturing stdout gets the result and nothing
            // else; every other line is progress and stays on stdout.
            if let Some(line) = human_line(&ev, &|verb| self.weft(verb)) {
                match ev.phase {
                    Phase::Warning | Phase::Error => eprintln!("{line}"),
                    _ => println!("{line}"),
                }
            }
        }
    }

    // SYNC: the details' "project" and "built" keys <-> packages/weft-graph/src/protocol.ts CliPhase
    pub fn build_start(&self, project: &str) {
        self.emit(Phase::BuildStart, Some(serde_json::json!({ "project": project })));
    }

    /// An image of the build started building on the install's builder.
    pub fn build_image(&self, build: &weft_core::projects::BuildInFlight) {
        self.emit(
            Phase::BuildImage,
            Some(serde_json::json!({ "image": build.image, "build": build.build, "logUrl": build.log_url })),
        );
    }

    /// An image of the build finished, about `seconds` after it started.
    pub fn build_image_done(&self, image: &str, seconds: u64) {
        self.emit(Phase::BuildImageDone, Some(serde_json::json!({ "image": image, "seconds": seconds })));
    }

    /// The build goes on: `images` still building, `built` finished so
    /// far, `elapsed` since it began.
    pub fn build_wait(&self, elapsed: std::time::Duration, images: &[String], built: &[String]) {
        self.emit(
            Phase::BuildWait,
            Some(serde_json::json!({ "elapsedSeconds": elapsed.as_secs(), "images": images, "built": built })),
        );
    }

    /// The last look at the build could not reach the install, `why`;
    /// `elapsed` since the build began.
    /// SYNC: the build_unreachable detail <-> packages/weft-graph/src/webview/lib/components/project/ActionBar.svelte (build_unreachable)
    pub fn build_unreachable(&self, elapsed: std::time::Duration, why: &str) {
        self.emit(
            Phase::BuildUnreachable,
            Some(serde_json::json!({ "elapsedSeconds": elapsed.as_secs(), "message": why })),
        );
    }

    /// `built`: the image refs the install had to build (empty when every
    /// image was already there).
    pub fn build_done(&self, project: &str, built: &[String]) {
        self.emit(Phase::BuildDone, Some(serde_json::json!({ "project": project, "built": built })));
    }

    /// Say that the call about to be made waits for the runs going, when
    /// it does (`running: wait`), and on which: the ones reaching the infra
    /// it takes down, the ones on the old version a resync replaces, or the
    /// ones fired by the triggers a deactivate takes down.
    pub fn drain_wait(&self, running: weft_core::Running, on: DrainOn) {
        if running == weft_core::Running::Wait {
            self.emit(Phase::DrainWait, Some(serde_json::json!({ "on": on.as_str() })));
        }
    }

    /// Where a resync is (`waits_for`, as its task said it), `elapsed` into
    /// following it: the owner's triggers it turns back on and what that
    /// waits on, or the runs on the old version it waits for, each with
    /// its start, or none once every one named before has ended (a run a
    /// worker took and has not put on record yet is waited for unnamed, so
    /// none named is not none left).
    // SYNC: the drain_wait detail's waitsFor <-> packages/weft-graph/src/protocol.ts CliPhase (drain_wait)
    pub fn resync_waits_for(&self, going: &weft_core::deactivation::ResyncGoing, elapsed: std::time::Duration) {
        self.emit(
            Phase::DrainWait,
            Some(serde_json::json!({
                "on": DrainOn::Resync.as_str(),
                "waitsFor": going.progress.waits_for,
                "turningOn": going.turning_on,
                "elapsedSeconds": elapsed.as_secs(),
            })),
        );
    }

    /// The run being started is at `stage`, `elapsed` since it was asked
    /// for.
    // SYNC: the run_starting detail <-> packages/weft-graph/src/protocol.ts CliPhase (run_starting), packages/weft-graph/src/webview/lib/components/project/ActionBar.svelte (run_starting)
    pub fn run_starting(&self, stage: &weft_core::versions::RunStarting, elapsed: std::time::Duration) {
        self.emit(Phase::RunStarting, Some(serde_json::json!({ "elapsedSeconds": elapsed.as_secs(), "doing": stage.describe() })));
    }

    pub fn dispatcher_call_start(&self, path: &str) {
        self.emit(
            Phase::DispatcherCallStart,
            Some(serde_json::json!({ "path": path })),
        );
    }

    pub fn dispatcher_call_done(&self, detail: Value) {
        self.emit(Phase::DispatcherCallDone, Some(detail));
    }

    pub fn infra_provision_start(&self, node_ids: &[String]) {
        self.emit(
            Phase::InfraProvisionStart,
            Some(serde_json::json!({ "nodeIds": node_ids })),
        );
    }

    pub fn infra_provision_done(&self) {
        self.emit(Phase::InfraProvisionDone, None);
    }

    pub fn trigger_register_start(&self) {
        self.emit(Phase::TriggerRegisterStart, None);
    }

    /// Still waiting on an infra verb, and what the copies starting under
    /// it are doing now (`weft infra status`'s progress lines), when any.
    /// SYNC: the infra_wait detail <-> packages/weft-graph/src/webview/lib/components/project/ActionBar.svelte (infra_wait)
    pub fn infra_wait(&self, verb: &str, elapsed_seconds: u64, doing: &[String]) {
        self.emit(
            Phase::InfraWait,
            Some(serde_json::json!({
                "verb": verb,
                "elapsedSeconds": elapsed_seconds,
                "doing": doing,
            })),
        );
    }

    /// The activation goes on, and waits on `progress.waiting` now.
    /// SYNC: the trigger_register_wait detail <-> packages/weft-graph/src/webview/lib/components/project/ActionBar.svelte (trigger_register_wait)
    pub fn trigger_register_wait(&self, progress: &weft_core::infra::wire::ChangeProgress) {
        self.emit(
            Phase::TriggerRegisterWait,
            Some(serde_json::json!({
                "elapsedSeconds": (progress.as_of_unix - progress.since_unix).max(0),
                "waiting": progress.waiting,
            })),
        );
    }

    pub fn trigger_register_done(&self) {
        self.emit(Phase::TriggerRegisterDone, None);
    }

    /// Something the person should see that is not a failure.
    ///
    /// On the progress channel, so a verb that rides it never has to
    /// print beside its own NDJSON (one stdout cannot be both).
    pub fn warn(&self, message: &str) {
        self.emit(Phase::Warning, Some(serde_json::json!({ "message": message })));
    }

    pub fn complete(&self, summary: &str) {
        self.emit(
            Phase::Complete,
            Some(serde_json::json!({ "summary": summary })),
        );
    }

    /// `complete` carrying the verb's own answer.
    ///
    /// A verb on the progress channel must not also print a bare JSON
    /// document to stdout: the two share one stream, and a reader
    /// parsing NDJSON gets a line with no phase while a reader parsing
    /// one document gets neither. The result rides the completion
    /// instead.
    pub fn complete_with(&self, summary: &str, result: serde_json::Value) {
        self.emit(
            Phase::Complete,
            Some(serde_json::json!({ "summary": summary, "result": result })),
        );
    }

    /// The verb failed with `e`: one `error` event carrying
    /// [`error_detail`].
    pub fn error(&self, e: &anyhow::Error) {
        self.error_emitted.store(true, std::sync::atomic::Ordering::SeqCst);
        self.emit(Phase::Error, Some(error_detail(e)));
    }

    /// Structured error variant: lets the verb describe WHAT failed,
    /// WHICH STAGE, and (optionally) pack a list of per-item
    /// diagnostics. The editor's action-bar error modal reads these
    /// fields directly. Use this when there's more context than a
    /// single message: compile failures, multi-error operations, etc.
    /// The webview tolerates missing fields (falls back to
    /// `error(message)` behavior).
    pub fn structured_error(&self, detail: serde_json::Value) {
        self.error_emitted.store(true, std::sync::atomic::Ordering::SeqCst);
        self.emit(Phase::Error, Some(detail));
    }
}

/// How many of the runs a resync waits for are named on its line; the
/// rest are counted.
const RUNS_NAMED: usize = 5;

/// What a resync waits for, at `now_unix`, as one line: the runs going on
/// the old version (oldest first, each with how long it has been going)
/// and the ways out, or, when none is named, that the triggers come back
/// next: a run a worker took and has not put on record yet is waited for
/// without being named (`weft_core::deactivation::ResyncProgress`), and the
/// task names none while it takes the triggers down or brings them back,
/// so an empty list never says which. `weft` spells a command for the
/// install the person acts on.
pub(crate) fn resync_wait_line(runs: &[weft_core::deactivation::RunGoing], now_unix: i64, weft: impl Fn(&str) -> String) -> String {
    if runs.is_empty() {
        return format!("no run on the old version is named now; the triggers come back on the new version next (`{}` gives the resync up)", weft("cancel-activate"));
    }
    let mut named: Vec<String> = runs
        .iter()
        .take(RUNS_NAMED)
        .map(|run| format!("{} (going {})", run.run_id, elapsed_text(now_unix.saturating_sub(run.since_unix).max(0).unsigned_abs())))
        .collect();
    if runs.len() > RUNS_NAMED {
        named.push(format!("and {} more", runs.len() - RUNS_NAMED));
    }
    let counted = if runs.len() == 1 { "1 run on the old one ends".to_string() } else { format!("{} runs on the old one end", runs.len()) };
    format!(
        "the new version takes over once {counted}: {}; `{}` ends one now, `{}` gives the resync up",
        named.join(", "),
        weft("stop <run>"),
        weft("cancel-activate")
    )
}

/// The build's heartbeat: what is building, what is built, and, between
/// images, that the next one has not reached the builder yet (the line
/// never names nothing).
fn build_wait_line(elapsed: u64, building: &[&str], built: &[&str]) -> String {
    let so_far = elapsed_text(elapsed);
    match (building, built) {
        ([], []) => format!("  still building ({so_far} so far): the first image has not started on the builder yet"),
        ([], built) => format!(
            "  still building ({so_far} so far): built {}, the next has not started on the builder yet",
            built.join(", ")
        ),
        (building, []) => format!("  still building {} ({so_far} so far)", building.join(", ")),
        (building, built) => {
            format!("  still building {} ({so_far} so far); built: {}", building.join(", "), built.join(", "))
        }
    }
}

/// `seconds` as a person reads a wait: `45s`, `1m51s`.
pub(crate) fn elapsed_text(seconds: u64) -> String {
    if seconds >= 60 {
        format!("{}m{:02}s", seconds / 60, seconds % 60)
    } else {
        format!("{seconds}s")
    }
}

pub(crate) fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock past UNIX_EPOCH")
        .as_secs()
}

/// Render a human-mode status line. Returns None for phases that
/// shouldn't surface in plain text mode (paired _done events are
/// implicit; complete is summary-only). `weft` spells a command for the
/// install the person acts on.
fn human_line(ev: &Event<'_>, weft: &dyn Fn(&str) -> String) -> Option<String> {
    Some(match ev.phase {
        Phase::BuildStart => format!(
            "building {}",
            ev.detail
                .and_then(|d| d.get("project"))
                .and_then(|v| v.as_str())
                .expect("build_start always names the project")
        ),
        Phase::BuildImage => {
            let field = |k: &str| ev.detail.and_then(|d| d.get(k)).and_then(|v| v.as_str());
            let image = field("image").unwrap_or("an image");
            let build = field("build").unwrap_or("?");
            match field("logUrl") {
                Some(log) => format!("  building {image} as {build}; its log: {log}"),
                None => format!("  building {image} as {build}"),
            }
        }
        Phase::BuildImageDone => {
            let image = ev.detail.and_then(|d| d.get("image")).and_then(|v| v.as_str()).unwrap_or("an image");
            let seconds = ev.detail.and_then(|d| d.get("seconds")).and_then(|v| v.as_u64()).unwrap_or(0);
            format!("  built {image} in about {}", elapsed_text(seconds))
        }
        Phase::BuildWait => {
            let elapsed = ev.detail.and_then(|d| d.get("elapsedSeconds")).and_then(|v| v.as_u64()).unwrap_or(0);
            let list = |k: &'static str| -> Vec<&str> {
                ev.detail
                    .and_then(|d| d.get(k))
                    .and_then(|v| v.as_array())
                    .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
                    .unwrap_or_default()
            };
            build_wait_line(elapsed, &list("images"), &list("built"))
        }
        Phase::BuildUnreachable => {
            let elapsed = ev.detail.and_then(|d| d.get("elapsedSeconds")).and_then(|v| v.as_u64()).unwrap_or(0);
            let why = ev.detail.and_then(|d| d.get("message")).and_then(|v| v.as_str()).unwrap_or("no answer");
            format!(
                "  cannot reach the install to see the build ({} so far): {why}\n  \
                 the build may still be going on there; looking again (Ctrl+C to stop following)",
                elapsed_text(elapsed)
            )
        }
        Phase::InfraProvisionStart => "provisioning infra".to_string(),
        // Where a resync is, said each time it changes and again every so
        // often while it does not.
        Phase::DrainWait if ev.detail.and_then(|d| d.get("waitsFor")).is_some() => {
            let runs: Vec<weft_core::deactivation::RunGoing> =
                ev.detail.and_then(|d| d.get("waitsFor")).and_then(|runs| serde_json::from_value(runs.clone()).ok()).unwrap_or_default();
            let elapsed = ev.detail.and_then(|d| d.get("elapsedSeconds")).and_then(Value::as_u64).unwrap_or(0);
            let turning_on: Option<weft_core::infra::wire::ChangeProgress> =
                ev.detail.and_then(|d| d.get("turningOn")).and_then(|on| serde_json::from_value(on.clone()).ok());
            let doing = match turning_on {
                Some(on) => format!("turning the triggers back on {}", on.describe(on.as_of_unix)),
                None => resync_wait_line(&runs, now_unix() as i64, weft),
            };
            format!("  still resyncing (elapsed {}): {doing}", elapsed_text(elapsed))
        }
        Phase::RunStarting => {
            let field = |k: &str| ev.detail.and_then(|d| d.get(k));
            let elapsed = field("elapsedSeconds").and_then(Value::as_u64).unwrap_or(0);
            let doing = field("doing").and_then(Value::as_str).unwrap_or("preparing it");
            format!("  starting the run ({} so far): {doing}", elapsed_text(elapsed))
        }
        Phase::DrainWait => match ev.detail.as_ref().and_then(|d| d.get("on")).and_then(Value::as_str) {
            Some("resync") => format!(
                "the triggers that are on and behind go down now, and the new version takes over once any run \
                 still going on the old one ends, however long it takes (`{}` ends one now, `{}` \
                 gives the resync up; Ctrl+C stops following, the resync goes on)",
                weft("stop <run>"),
                weft("cancel-activate")
            ),
            Some("deactivate") => format!(
                "the triggers go down now; waiting for the runs they fired to end, however long they take \
                 (`{}` ends one now; Ctrl+C stops waiting, the triggers stay down)",
                weft("stop <run>")
            ),
            _ => format!(
                "any run reaching the infra it takes down is waited for first, however long it takes \
                 (`{}` ends one now, `{}` calls the command off)",
                weft("stop <run>"),
                weft("infra cancel")
            ),
        },
        Phase::TriggerRegisterStart => "registering triggers".to_string(),
        Phase::TriggerRegisterWait => {
            let elapsed = ev.detail.and_then(|d| d.get("elapsedSeconds")).and_then(Value::as_u64).unwrap_or(0);
            let waiting = ev.detail.and_then(|d| d.get("waiting")).and_then(Value::as_str).unwrap_or("its next step");
            format!(
                "still turning on (elapsed {elapsed}s), waiting on: {waiting} (Ctrl+C stops following, the activation goes on; \
                 `{}` stops it)",
                weft("cancel-activate")
            )
        }
        Phase::InfraWait => {
            let verb = ev
                .detail
                .and_then(|d| d.get("verb"))
                .and_then(|v| v.as_str())
                .unwrap_or("command");
            let elapsed = ev
                .detail
                .and_then(|d| d.get("elapsedSeconds"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let doing: Vec<&str> = ev
                .detail
                .and_then(|d| d.get("doing"))
                .and_then(|v| v.as_array())
                .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
                .unwrap_or_default();
            let mut line = format!("still waiting on infra {verb} (elapsed {elapsed}s; Ctrl+C to back out)");
            for d in doing {
                line.push_str(&format!("\n  {d}"));
            }
            line
        }
        Phase::Warning => format!(
            "warning: {}",
            ev.detail
                .and_then(|d| d.get("message"))
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
        ),
        Phase::Complete => return ev
            .detail
            .and_then(|d| d.get("summary"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        Phase::Error => format!(
            "error: {}",
            ev.detail
                .and_then(|d| d.get("message"))
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
        ),
        // Done counterparts and the catch-alls are silent in human mode.
        _ => return None,
    })
}

/// How often a wait that goes on says it still does: a build, a sync of
/// the folder, a busy install.
pub const SAY_AGAIN_EVERY: std::time::Duration = std::time::Duration::from_secs(30);

/// When a wait that goes on is to say so again: at once when nothing was
/// said yet, then once every [`SAY_AGAIN_EVERY`]. Times are how long into
/// the wait, from whatever start the caller measures from.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SayAgain {
    said: Option<std::time::Duration>,
}

impl SayAgain {
    /// A wait whose first line was said `at` (its start, announced).
    pub fn said_at(at: std::time::Duration) -> Self {
        Self { said: Some(at) }
    }

    /// Whether a line is due `elapsed` into the wait.
    pub fn due(&self, elapsed: std::time::Duration) -> bool {
        self.said.is_none_or(|said| elapsed.saturating_sub(said) >= SAY_AGAIN_EVERY)
    }

    /// A line was said `elapsed` into the wait.
    pub fn said(&mut self, elapsed: std::time::Duration) {
        self.said = Some(elapsed);
    }
}

/// What a wait that goes on said last (`T`, where the thing waited on says
/// it is) and when: a line is due at once when where it is changed, and
/// otherwise every [`SAY_AGAIN_EVERY`] ([`SayAgain`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SaidLast<T> {
    what: T,
    again: SayAgain,
}

impl<T: PartialEq> SaidLast<T> {
    /// A wait whose start, at `what`, was announced `at` into it.
    pub fn announced(what: T, at: std::time::Duration) -> Self {
        Self { what, again: SayAgain::said_at(at) }
    }

    /// `now`, heard `elapsed` into the wait, when it is to be said (it then
    /// is what was said last); nothing when it is not.
    pub fn hear(&mut self, now: T, elapsed: std::time::Duration) -> Option<&T> {
        if self.what == now && !self.again.due(elapsed) {
            return None;
        }
        self.what = now;
        self.again.said(elapsed);
        Some(&self.what)
    }
}

/// `work`'s answer. Every `every` until it comes, `look` runs with how long
/// it has been, to say what is happening meanwhile: a call that takes
/// minutes is never silent. The one shape of every such wait (a build, an
/// infra start, a frontend's service), so each says what it sees and
/// nothing else.
pub async fn while_waiting<T>(
    work: impl std::future::Future<Output = T>,
    every: std::time::Duration,
    mut look: impl AsyncFnMut(std::time::Duration),
) -> T {
    let started = std::time::Instant::now();
    let mut work = std::pin::pin!(work);
    let mut timer = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
    loop {
        tokio::select! {
            done = &mut work => return done,
            _ = timer.tick() => look(started.elapsed()).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{build_wait_line, error_detail, SaidLast, SayAgain, SAY_AGAIN_EVERY};

    /// Every wait for the runs going names its commands for the install
    /// the command was given (`--on <target>`), never the local one.
    #[test]
    fn a_drain_wait_names_its_commands_for_the_install_acted_on() {
        let progress = super::Progress::new(super::ActionVerb::Resync, false, Some("cloud".into()));
        let weft = |verb: &str| progress.weft(verb);
        let line = |detail: serde_json::Value| {
            let ev = super::Event { ts_unix: 0, verb: None, phase: super::Phase::DrainWait, detail: Some(&detail) };
            super::human_line(&ev, &weft).expect("a drain wait is said")
        };
        for on in ["resync", "deactivate", "infra"] {
            let said = line(serde_json::json!({ "on": on }));
            assert!(said.contains("`weft stop <run> --on cloud`"), "{on}: {said}");
            assert!(!said.contains("`weft stop <run>`"), "{on}: {said}");
        }
        assert!(line(serde_json::json!({ "on": "infra" })).contains("`weft infra cancel --on cloud`"));
        let waits = serde_json::json!({ "on": "resync", "waitsFor": [{ "runId": uuid::Uuid::nil(), "sinceUnix": 0 }] });
        assert!(line(waits).contains("`weft cancel-activate --on cloud`"));
    }

    /// What a resync waits for reads as the runs, oldest first with how
    /// long each has gone, at most a few named, and the ways out; none
    /// named says the triggers come back next.
    #[test]
    fn a_resync_says_which_runs_it_waits_for() {
        let run = |n: u128, since_unix| weft_core::deactivation::RunGoing { run_id: uuid::Uuid::from_u128(n), since_unix };
        let weft = |verb: &str| format!("weft {verb} --on cloud");
        let line = super::resync_wait_line(&[run(1, 1000), run(2, 1180)], 1200, weft);
        assert_eq!(
            line,
            format!(
                "the new version takes over once 2 runs on the old one end: {} (going 3m20s), {} (going 20s); \
                 `weft stop <run> --on cloud` ends one now, `weft cancel-activate --on cloud` gives the resync up",
                uuid::Uuid::from_u128(1),
                uuid::Uuid::from_u128(2)
            )
        );
        let many: Vec<_> = (0..8).map(|n| run(n, 1200)).collect();
        let line = super::resync_wait_line(&many, 1200, weft);
        assert!(line.starts_with("the new version takes over once 8 runs") && line.contains("(going 0s), and 3 more;"), "{line}");
        assert_eq!(super::resync_wait_line(&[run(1, 1300)], 1200, weft).matches("going 0s").count(), 1, "a clock behind the install's reads 0s");
        assert_eq!(
            super::resync_wait_line(&[], 1200, weft),
            "no run on the old version is named now; the triggers come back on the new version next \
             (`weft cancel-activate --on cloud` gives the resync up)",
            "an empty list never says the old version is done: a run not on record yet is waited for unnamed"
        );
    }

    /// A wait says so at once when nothing was said, then again only once
    /// a whole beat passed since the last line.
    #[test]
    fn a_wait_says_so_at_once_then_once_a_beat() {
        let secs = std::time::Duration::from_secs;
        let mut say = SayAgain::default();
        assert!(say.due(secs(0)), "nothing said yet");
        say.said(secs(2));
        assert!(!say.due(secs(2)));
        assert!(!say.due(secs(2) + SAY_AGAIN_EVERY - secs(1)));
        assert!(say.due(secs(2) + SAY_AGAIN_EVERY));
        let announced = SayAgain::said_at(secs(5));
        assert!(!announced.due(secs(6)), "a wait whose start was announced waits a beat");
        assert!(announced.due(secs(5) + SAY_AGAIN_EVERY));
        assert!(!announced.due(secs(1)), "a time before the last line is never due");
    }

    /// A resync's follower says where it is at once when that changed, and
    /// otherwise again once a beat passed since it last spoke, with how
    /// long it has followed: never silent for minutes while the triggers
    /// come back.
    #[test]
    fn a_resync_says_where_it_is_when_it_changes_and_once_a_beat() {
        let secs = std::time::Duration::from_secs;
        let run = |n: u128| weft_core::deactivation::RunGoing { run_id: uuid::Uuid::from_u128(n), since_unix: 1000 };
        let waiting = weft_core::deactivation::ResyncProgress { waits_for: vec![run(1)] };
        let none = weft_core::deactivation::ResyncProgress::default();
        let mut said = SaidLast::announced(none.clone(), secs(0));
        assert_eq!(said.hear(none.clone(), secs(25)), None, "the drain-wait line just said it");
        assert_eq!(said.hear(waiting.clone(), secs(26)), Some(&waiting), "a change is said at once");
        assert_eq!(said.hear(waiting.clone(), secs(26) + SAY_AGAIN_EVERY - secs(1)), None);
        assert_eq!(said.hear(none.clone(), secs(40)), Some(&none), "the runs ending is a change");
        assert_eq!(said.hear(none.clone(), secs(40) + SAY_AGAIN_EVERY), Some(&none), "unchanged, said again after a beat");
        assert_eq!(said.hear(none.clone(), secs(41) + SAY_AGAIN_EVERY), None, "and not again until the next beat");

        let progress = super::Progress::new(super::ActionVerb::Resync, false, None);
        let weft = |verb: &str| progress.weft(verb);
        let detail = serde_json::json!({ "on": "resync", "waitsFor": [], "elapsedSeconds": 330 });
        let ev = super::Event { ts_unix: 0, verb: None, phase: super::Phase::DrainWait, detail: Some(&detail) };
        assert_eq!(
            super::human_line(&ev, &weft).expect("where a resync is is said"),
            "  still resyncing (elapsed 5m30s): no run on the old version is named now; the triggers come back on the new \
             version next (`weft cancel-activate` gives the resync up)"
        );
        // Turning an owner's triggers back on, it says for how long and
        // what that waits on.
        let detail = serde_json::json!({
            "on": "resync", "waitsFor": [], "elapsedSeconds": 90,
            "turningOn": { "sinceUnix": 100, "asOfUnix": 160, "waiting": "its setup run r1" }
        });
        let ev = super::Event { ts_unix: 0, verb: None, phase: super::Phase::DrainWait, detail: Some(&detail) };
        let line = super::human_line(&ev, &weft).expect("where a resync is is said");
        assert!(line.starts_with("  still resyncing (elapsed 1m30s): turning the triggers back on ") && line.contains("its setup run r1"), "{line}");
    }

    /// The wait line names what it knows in every state, never just the time.
    #[test]
    fn a_build_wait_says_what_is_building_and_what_is_done() {
        assert_eq!(build_wait_line(160, &[], &[]), "  still building (2m40s so far): the first image has not started on the builder yet");
        assert_eq!(build_wait_line(30, &[], &["worker"]), "  still building (30s so far): built worker, the next has not started on the builder yet");
        assert_eq!(build_wait_line(30, &["db"], &[]), "  still building db (30s so far)");
        assert_eq!(build_wait_line(30, &["db"], &["worker"]), "  still building db (30s so far); built: worker");
    }

    /// A request that never reached this machine's daemon says to start
    /// it, and flags it for a host that can; one that never reached a
    /// remote install names that install, says to check the network, and
    /// never offers the daemon.
    #[tokio::test]
    async fn an_unreachable_install_is_named_by_where_it_is() {
        // A port just let go of: nothing listens, the connection is refused.
        let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
        let local = crate::client::DispatcherClient::new(format!("http://{closed}"), None);
        let e = local.get_json("/install").await.unwrap_err().context("read the install");
        let detail = error_detail(&e);
        assert_eq!(detail["daemonUnreachable"], true);
        assert_eq!(detail["installUnreachable"], true);
        let message = detail["message"].as_str().unwrap();
        assert!(message.starts_with("the weft daemon is not answering. Start it with `weft daemon start`"), "{message}");

        let cause = weft_core::net::http_client().build().unwrap().get(format!("http://{closed}/")).send().await.unwrap_err();
        let remote = crate::client::InstallUnreachable {
            url: "https://weft-prod.example.run.app".into(),
            target: Some("prod".into()),
            cause,
        };
        let e = anyhow::Error::new(remote).context("POST https://weft-prod.example.run.app/projects/p/builds").context("ask the install to build");
        let detail = error_detail(&e);
        assert_eq!(detail["daemonUnreachable"], false, "nothing on this machine would start it");
        assert_eq!(detail["installUnreachable"], true);
        let message = detail["message"].as_str().unwrap();
        assert!(
            message.contains("the install `prod` (https://weft-prod.example.run.app) did not answer: "),
            "{message}"
        );
        assert!(message.ends_with("; check the network and run the command again"), "{message}");
        assert!(!message.contains("weft daemon"), "{message}");
    }
}
