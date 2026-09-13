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
//!   { "ts_unix": <u64>,
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
/// (Reactivate / resume-active reuse the plain `Activate` verb, so
/// they have no variant here.)
// SYNC: ActionVerb <-> packages/weft-graph/src/protocol.ts ActionVerb, crates/weft-dispatcher/src/api/project.rs compute_available_actions
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionVerb {
    Run,
    Activate,
    Bake,
    /// Cancel an in-flight `activate` (status=Activating). Wipes
    /// partial trigger registrations and flips the project back to
    /// Inactive.
    CancelActivate,
    /// Cancel an in-flight build (transition=building).
    /// Flips the transition to cancelling_build; the pod driving the
    /// build interrupts the builder job.
    CancelBuild,
    Deactivate,
    /// Force-cancel running executions while a deactivate-with-wait
    /// is draining. The lifecycle target the original deactivate
    /// wrote stays in place; this just unblocks the drain.
    CancelRunning,
    Resync,
    Build,
    InfraStart,
    InfraStop,
    InfraTerminate,
    InfraUpgrade,
    InfraNodeStop,
    InfraNodeTerminate,
    /// Cancel in-flight infra work: halt claimed lifecycle commands,
    /// cancel unclaimed ones, interrupt the InfraSetup provisioning
    /// execution. HALT, not rollback: per-node partial state stays.
    InfraCancel,
    /// Multi-level project cleanup (deactivate + unregister + optional
    /// infra/journal/image/local wipes).
    Rm,
}

/// All phases any verb can emit. Closed enum so the extension's
/// reducer covers every variant via match-exhaustiveness.
// SYNC: Phase <-> packages/weft-graph/src/protocol.ts CliPhase
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    /// Worker / infra image build started.
    BuildStart,
    /// Build skipped because the hash matched an existing image.
    BuildSkip,
    /// Build finished (image is local).
    BuildDone,
    /// Loading image into kind cluster (or pushing it to a registry).
    ImagePushStart,
    ImagePushDone,
    /// The stale-image sweep after an ensure dropped something. Detail
    /// carries `{ "images": [<ref>, ...] }`, the host refs untagged.
    /// Never emitted when the sweep dropped nothing.
    ImagesReclaimed,
    /// HTTP request to the dispatcher started.
    DispatcherCallStart,
    /// HTTP request to the dispatcher finished. Body in `detail`
    /// (e.g. `{ "color": "..." }` for run, `{ "signal_count": N }`
    /// for activate).
    DispatcherCallDone,
    /// Infra provision started (one event covers every infra node
    /// in the verb; node ids in detail).
    InfraProvisionStart,
    InfraProvisionDone,
    /// Trigger registration started (signals about to be wired up).
    TriggerRegisterStart,
    TriggerRegisterDone,
    /// Waiting on a long-running infra supervisor command (stop /
    /// terminate / start), which can depend on draining in-flight
    /// executions and so is unbounded. Detail:
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

/// The one report for a failure that reached `main` unreported: a verb
/// with no progress channel, or a failure before a channel existed.
/// Same shape as a channel's own error, so every verb fails the same
/// way: under `--json` one `phase: "error"` event on stdout, otherwise
/// one lowercase `error:` line on stderr. The cause chain rides along
/// in the message (`{e:#}`), since readers build their errors from
/// context and the chain is where the "why" lives.
pub fn report_plain_error(json: bool, e: &anyhow::Error) {
    let message = format!("{e:#}");
    if json {
        let detail = serde_json::json!({ "message": message });
        let ev = Event { ts_unix: now_unix(), verb: None, phase: Phase::Error, detail: Some(&detail) };
        println!("{}", serde_json::to_string(&ev).expect("Event serializes"));
    } else {
        eprintln!("error: {message}");
    }
}

/// One emitter per CLI invocation. Cheap to clone; commands thread
/// it through their helper functions so any layer can emit.
#[derive(Clone)]
pub struct Progress {
    pub json: bool,
    pub verb: ActionVerb,
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
    pub fn new(verb: ActionVerb, json: bool) -> Self {
        Self {
            json,
            verb,
            error_emitted: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// True if any error event has already been emitted on this
    /// emitter. Used by `with_progress` to avoid double-emitting
    /// when the body already produced a structured error.
    pub fn has_emitted_error(&self) -> bool {
        self.error_emitted.load(std::sync::atomic::Ordering::SeqCst)
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
            if let Some(line) = human_line(&ev) {
                match ev.phase {
                    Phase::Warning | Phase::Error => eprintln!("{line}"),
                    _ => println!("{line}"),
                }
            }
        }
    }

    pub fn build_start(&self, image: &str) {
        self.emit(
            Phase::BuildStart,
            Some(serde_json::json!({ "image": image })),
        );
    }

    pub fn build_skip(&self, image: &str, reason: &str) {
        self.emit(
            Phase::BuildSkip,
            Some(serde_json::json!({ "image": image, "reason": reason })),
        );
    }

    pub fn build_done(&self, image: &str) {
        self.emit(
            Phase::BuildDone,
            Some(serde_json::json!({ "image": image })),
        );
    }

    pub fn image_push_start(&self, image: &str) {
        self.emit(
            Phase::ImagePushStart,
            Some(serde_json::json!({ "image": image })),
        );
    }

    pub fn image_push_done(&self, image: &str) {
        self.emit(
            Phase::ImagePushDone,
            Some(serde_json::json!({ "image": image })),
        );
    }

    /// Report the stale images an ensure's sweep untagged; silent when
    /// there were none, so a no-change build prints nothing about it.
    pub fn images_reclaimed(&self, images: &[String]) {
        if images.is_empty() {
            return;
        }
        self.emit(
            Phase::ImagesReclaimed,
            Some(serde_json::json!({ "images": images })),
        );
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
            Some(serde_json::json!({ "node_ids": node_ids })),
        );
    }

    pub fn infra_provision_done(&self) {
        self.emit(Phase::InfraProvisionDone, None);
    }

    pub fn trigger_register_start(&self) {
        self.emit(Phase::TriggerRegisterStart, None);
    }

    pub fn infra_wait(&self, verb: &str, elapsed_seconds: u64) {
        self.emit(
            Phase::InfraWait,
            Some(serde_json::json!({
                "verb": verb,
                "elapsedSeconds": elapsed_seconds,
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

    pub fn error(&self, message: &str) {
        self.error_emitted.store(true, std::sync::atomic::Ordering::SeqCst);
        self.emit(
            Phase::Error,
            Some(serde_json::json!({ "message": message })),
        );
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

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock past UNIX_EPOCH")
        .as_secs()
}

/// Render a human-mode status line. Returns None for phases that
/// shouldn't surface in plain text mode (paired _done events are
/// implicit; complete is summary-only).
fn human_line(ev: &Event<'_>) -> Option<String> {
    Some(match ev.phase {
        Phase::BuildStart => format!(
            "building {}",
            ev.detail
                .and_then(|d| d.get("image"))
                .and_then(|v| v.as_str())
                .unwrap_or("?")
        ),
        // The image is what is cached: node code and the engine. The
        // definition (config, `@file` contents) always registers fresh,
        // so a SQL or prompt edit runs without a rebuild and this line
        // must not read as "your change was skipped".
        Phase::BuildSkip => format!(
            "{} unchanged since the last build, reusing the image (config and @file contents still update)",
            ev.detail
                .and_then(|d| d.get("image"))
                .and_then(|v| v.as_str())
                .unwrap_or("?")
        ),
        Phase::ImagePushStart => "loading image".to_string(),
        Phase::ImagesReclaimed => {
            let n = ev
                .detail
                .and_then(|d| d.get("images"))
                .and_then(|v| v.as_array())
                .map_or(0, |a| a.len());
            format!("dropped {n} stale image tag{}", if n == 1 { "" } else { "s" })
        }
        Phase::InfraProvisionStart => "provisioning infra".to_string(),
        Phase::TriggerRegisterStart => "registering triggers".to_string(),
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
            format!("still waiting on infra {verb} (elapsed {elapsed}s; Ctrl+C to back out)")
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
