//! `weft wait <run>`: block until a run ends, then say how it ended in
//! one line and in the exit code, so a script (or an assistant) waits on
//! a run with one command instead of a loop around `weft runs`.
//!
//! It reads the same stream `weft run` follows a run with
//! (`follow::watch_run`): the run's history first, then its live events,
//! so a run that ended before the wait began is answered at once from its
//! history. With `--waiting-for-input` it also returns as soon as the run
//! parks: every node still open waits on something (a form, a timer, an
//! outside job), which is what `weft runs` reads as `waiting_for_input`.
//! The dispatcher says so with `run_parked`, at the end of the history of
//! a run parked already and live when it parks. A timeout leaves the run
//! alone: it only stops waiting.

use std::ops::ControlFlow;
use std::time::Duration;

use weft_core::live_event::{DispatcherEvent, LiveEvent};

use super::Ctx;

/// The longest one `weft wait` blocks. A longer wait is several calls,
/// so whoever waits hears back at least this often.
pub const MAX_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// How a wait on a run came out. Each outcome has its exit code
/// ([`Outcome::code`]); `weft run` exits with the same code for the run
/// it followed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Completed,
    Failed,
    Cancelled,
    /// The run parked: every node still open waits on something. Only
    /// `weft wait --waiting-for-input` stops here.
    WaitingForInput,
    /// The wait ran out; the run is still going.
    TimedOut,
}

impl Outcome {
    /// Every outcome, in the order `--help` lists them.
    const ALL: [Outcome; 5] = [Self::Completed, Self::Failed, Self::Cancelled, Self::WaitingForInput, Self::TimedOut];

    /// The run's ending, when `event` is its terminal event. A terminal
    /// kind this list does not name is refused rather than read as some
    /// other ending.
    pub fn ended<V>(event: &DispatcherEvent<V>) -> anyhow::Result<Option<Self>> {
        Ok(match event {
            DispatcherEvent::RunCompleted { .. } => Some(Self::Completed),
            DispatcherEvent::RunFailed { .. } => Some(Self::Failed),
            DispatcherEvent::RunCancelled { .. } => Some(Self::Cancelled),
            other if other.is_run_terminal() => {
                anyhow::bail!("the run ended in a way this CLI does not know; upgrade the CLI to match the install")
            }
            _ => None,
        })
    }

    /// The process exit code for this outcome. Clap exits 2 on a wrong
    /// command line and any error exits 1, so the outcomes that are not a
    /// completion sit apart from both.
    pub const fn code(self) -> u8 {
        match self {
            Self::Completed => 0,
            Self::Failed | Self::Cancelled => 10,
            Self::WaitingForInput => 11,
            Self::TimedOut => 12,
        }
    }

    pub fn exit_code(self) -> std::process::ExitCode {
        std::process::ExitCode::from(self.code())
    }

    fn meaning(self) -> &'static str {
        match self {
            Self::Completed => "the run completed",
            Self::Failed => "the run failed",
            Self::Cancelled => "the run was cancelled",
            Self::WaitingForInput => "the run is waiting for input (only with --waiting-for-input)",
            Self::TimedOut => "the timeout ran out and the run is still going",
        }
    }
}

/// The exit codes, as `weft wait --help` lists them.
pub fn exit_codes_help() -> String {
    let mut lines = vec!["Exit codes:".to_string()];
    lines.extend(Outcome::ALL.iter().map(|outcome| format!("  {:<3} {}", outcome.code(), outcome.meaning())));
    lines.push("  1   the wait itself failed (no such run, or the install unreachable)".to_string());
    lines.push("  2   the command line is wrong".to_string());
    lines.join("\n")
}

/// `--timeout`: a duration (`30s`, `10m`), at least a second and at most
/// [`MAX_TIMEOUT`].
pub fn parse_timeout(text: &str) -> Result<Duration, String> {
    let secs = crate::parse_duration_secs(text)?;
    if secs == 0 {
        return Err("the timeout must be at least 1s".to_string());
    }
    let timeout = Duration::from_secs(secs);
    if timeout > MAX_TIMEOUT {
        return Err(format!(
            "a wait lasts at most {}; to wait longer, call `weft wait` again when it times out",
            spell_duration(MAX_TIMEOUT)
        ));
    }
    Ok(timeout)
}

/// A duration the way `--timeout` takes it: whole minutes as `10m`,
/// anything else in seconds.
fn spell_duration(duration: Duration) -> String {
    match duration.as_secs() {
        secs if secs % 60 == 0 => format!("{}m", secs / 60),
        secs => format!("{secs}s"),
    }
}

/// How the wait ended, with what the run's ending said about itself.
#[derive(Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct Ended {
    outcome: Outcome,
    /// For a failed run: what failed it.
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    /// For a cancelled run: who or what stopped it.
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

impl Ended {
    fn bare(outcome: Outcome) -> Self {
        Self { outcome, error: None, reason: None }
    }
}

/// What `weft wait` watches for in one run's events: its ending always,
/// and with `waiting_for_input` also its parking.
struct Watch {
    waiting_for_input: bool,
}

impl Watch {
    fn new(waiting_for_input: bool) -> Self {
        Self { waiting_for_input }
    }

    fn step(&mut self, event: &LiveEvent) -> anyhow::Result<ControlFlow<Ended>> {
        if let Some(outcome) = Outcome::ended(&event.event)? {
            return Ok(ControlFlow::Break(Ended {
                outcome,
                error: match &event.event {
                    DispatcherEvent::RunFailed { error, .. } => Some(error.clone()),
                    _ => None,
                },
                reason: match &event.event {
                    DispatcherEvent::RunCancelled { reason, .. } => Some(reason.clone()),
                    _ => None,
                },
            }));
        }
        Ok(match &event.event {
            DispatcherEvent::RunParked { .. } if self.waiting_for_input => ControlFlow::Break(Ended::bare(Outcome::WaitingForInput)),
            _ => ControlFlow::Continue(()),
        })
    }
}

pub async fn run(ctx: Ctx, run_id: String, timeout: Duration, waiting_for_input: bool) -> anyhow::Result<Outcome> {
    let run_id = super::resolve_run_id(&ctx, &run_id).await?;
    let client = ctx.client()?;
    let mut watch = Watch::new(waiting_for_input);
    let watching = super::follow::watch_run(&client, ctx.on(), &run_id, |event| watch.step(event));
    let ended = match tokio::time::timeout(timeout, watching).await {
        Ok(ended) => ended?,
        Err(_) => Ended::bare(Outcome::TimedOut),
    };
    let mut answer = serde_json::to_value(&ended)?;
    answer["runId"] = serde_json::Value::String(run_id.clone());
    if ctx.json_out(&answer)? {
        return Ok(ended.outcome);
    }
    println!("{}", ended_line(&ended, &run_id, timeout, waiting_for_input, ctx.on()));
    Ok(ended.outcome)
}

/// The line a person reads when the wait on `run_id` stops: how the run
/// ended, or what to call to keep waiting (aimed at the install `on`
/// names).
fn ended_line(ended: &Ended, run_id: &str, timeout: Duration, waiting_for_input: bool, on: Option<&str>) -> String {
    let short = super::versions::short(run_id);
    match ended.outcome {
        Outcome::Completed => format!("run {short} completed"),
        Outcome::Failed => format!("run {short} failed: {}", ended.error.as_deref().unwrap_or_default()),
        Outcome::Cancelled => format!("run {short} was cancelled: {}", ended.reason.as_deref().unwrap_or_default()),
        Outcome::WaitingForInput => format!(
            "run {short} is waiting for input; see what it waits on with `{}`",
            super::weft_on(on, &format!("events {run_id} --kind node_suspended"))
        ),
        Outcome::TimedOut => {
            let again = if waiting_for_input { format!("wait {run_id} --waiting-for-input") } else { format!("wait {run_id}") };
            format!(
                "run {short} is still going after {}; to keep waiting, call `{}` again",
                spell_duration(timeout),
                super::weft_on(on, &again)
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// One event as the dispatcher sends it: `fields` for its kind, and
    /// the ones every run event carries.
    fn event(kind: &str, fields: serde_json::Value) -> LiveEvent {
        let mut fields = fields;
        let object = fields.as_object_mut().unwrap();
        object.insert("eventId".into(), json!(format!("journal:{}:0:0", object.len())));
        object.insert("kind".into(), json!(kind));
        object.insert("runId".into(), json!(uuid::Uuid::from_u128(1)));
        object.insert("projectId".into(), json!(uuid::Uuid::nil()));
        object.insert("atUnix".into(), json!(1));
        serde_json::from_value(fields).unwrap()
    }

    fn suspended(token: &str) -> LiveEvent {
        event("node_suspended", json!({ "node": "review", "frames": [], "token": token }))
    }

    /// The run parked: what the dispatcher says at the end of a parked
    /// run's history, and live when it parks.
    fn parked() -> LiveEvent {
        let mut parked = event("run_parked", json!({}));
        assert!(matches!(parked.event, DispatcherEvent::RunParked { .. }), "{parked:?}");
        parked.event_id = weft_core::live_event::EventId::Transient(uuid::Uuid::nil());
        parked
    }

    /// Feed `history`, then `live`, the way the stream does; answers
    /// where the watch stopped, if it did.
    fn watch(waiting_for_input: bool, history: &[LiveEvent], live: &[LiveEvent]) -> Option<Ended> {
        let mut watch = Watch::new(waiting_for_input);
        for event in history.iter().chain(live) {
            if let ControlFlow::Break(ended) = watch.step(event).unwrap() {
                return Some(ended);
            }
        }
        None
    }

    #[test]
    fn every_outcome_has_its_own_code_and_failing_ones_share_one() {
        assert_eq!(Outcome::Completed.code(), 0);
        assert_eq!(Outcome::Failed.code(), 10);
        assert_eq!(Outcome::Cancelled.code(), Outcome::Failed.code());
        assert_eq!(Outcome::WaitingForInput.code(), 11);
        assert_eq!(Outcome::TimedOut.code(), 12);
        // Clap's usage error and an error's exit code stay unambiguous.
        assert!(Outcome::ALL.iter().all(|o| o.code() != 1 && o.code() != 2));
        let help = exit_codes_help();
        for outcome in Outcome::ALL {
            assert!(help.contains(&format!("{:<3} {}", outcome.code(), outcome.meaning())), "{help}");
        }
    }

    #[test]
    fn the_timeout_takes_a_duration_up_to_ten_minutes() {
        assert_eq!(parse_timeout("30s"), Ok(Duration::from_secs(30)));
        assert_eq!(parse_timeout("10m"), Ok(MAX_TIMEOUT));
        let longer = parse_timeout("11m").unwrap_err();
        assert!(longer.contains("at most 10m") && longer.contains("call `weft wait` again"), "{longer}");
        assert!(parse_timeout("601s").is_err());
        assert!(parse_timeout("1h").is_err());
        assert!(parse_timeout("0s").unwrap_err().contains("at least 1s"));
        assert!(parse_timeout("10").is_err(), "a duration needs its unit");
    }

    /// A wait that timed out names the whole call that keeps waiting on
    /// the same run, with the flag it was given and the install it was
    /// aimed at; a run that ended says how, with the error or the reason.
    #[test]
    fn a_timed_out_wait_names_the_call_that_keeps_waiting() {
        let run = "c2000000-0000-0000-0000-000000000000";
        let ten = Duration::from_secs(600);
        let line = ended_line(&Ended::bare(Outcome::TimedOut), run, ten, false, None);
        assert_eq!(line, format!("run c2000000 is still going after 10m; to keep waiting, call `weft wait {run}` again"));
        let line = ended_line(&Ended::bare(Outcome::TimedOut), run, Duration::from_secs(90), true, Some("prod"));
        assert!(line.contains(&format!("`weft wait {run} --waiting-for-input --on prod`")) && line.contains("after 90s"), "{line}");
        let cancelled = Ended { reason: Some("stopped by a person".into()), ..Ended::bare(Outcome::Cancelled) };
        assert_eq!(ended_line(&cancelled, run, ten, false, None), "run c2000000 was cancelled: stopped by a person");
        let failed = Ended { error: Some("this run fails on purpose".into()), ..Ended::bare(Outcome::Failed) };
        assert_eq!(ended_line(&failed, run, ten, false, None), "run c2000000 failed: this run fails on purpose");
        assert_eq!(ended_line(&Ended::bare(Outcome::Completed), run, ten, false, None), "run c2000000 completed");
    }

    #[test]
    fn the_timeout_is_said_back_the_way_it_was_given() {
        assert_eq!(spell_duration(Duration::from_secs(600)), "10m");
        assert_eq!(spell_duration(Duration::from_secs(90)), "90s");
    }

    #[test]
    fn a_run_that_ended_before_the_wait_is_answered_from_its_history() {
        let failed = event("run_failed", json!({ "error": "boom" }));
        let ended = watch(false, &[failed], &[]).unwrap();
        assert_eq!(ended, Ended { outcome: Outcome::Failed, error: Some("boom".into()), reason: None });
        let cancelled = event("run_cancelled", json!({ "reason": "stopped by a person" }));
        let ended = watch(true, &[suspended("t"), cancelled], &[]).unwrap();
        assert_eq!(ended.outcome, Outcome::Cancelled);
        assert_eq!(ended.reason.as_deref(), Some("stopped by a person"));
    }

    /// Only the run parking stops a wait at a pause: a node pausing while
    /// another still works is not the run waiting for input.
    #[test]
    fn a_parked_run_stops_the_wait_only_when_asked() {
        assert_eq!(watch(true, &[suspended("t"), parked()], &[]).unwrap().outcome, Outcome::WaitingForInput, "parked already");
        assert_eq!(watch(true, &[], &[suspended("t"), parked()]).unwrap().outcome, Outcome::WaitingForInput, "parks while watched");
        assert_eq!(watch(true, &[suspended("t")], &[]), None, "a node paused while the run still works");
        assert_eq!(watch(false, &[suspended("t"), parked()], &[]), None, "without the flag only an ending stops it");
        let completed = event("run_completed", json!({ "outputs": {} }));
        assert_eq!(watch(false, &[parked()], &[completed]).unwrap().outcome, Outcome::Completed);
    }
}
