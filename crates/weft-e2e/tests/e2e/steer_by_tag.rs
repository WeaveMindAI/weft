//! Runs steering each other by tag, end to end, through the two
//! catalog nodes `TagRun` and `StopTagged` and nothing else.
//!
//! The `steer_by_tag` fixture is the debounce on one fake SSE feed: each
//! "go" event starts a run that tags itself with the sender, stops the
//! earlier runs for that sender, and parks on a form. The one test proves
//! the newest run for a sender is the only one left, the ones it stopped
//! read as stopped BY it with the tag named, a stopped run's form is gone
//! (its wake was erased), and the tags show on the run everywhere a
//! person looks for them.
#![cfg(feature = "e2e")]

use std::collections::HashSet;
use std::time::Duration;

use serde_json::{json, Value};
use uuid::Uuid;
use weft_e2e::client::Dispatcher;
use weft_e2e::fakes::SseFake;
use weft_e2e::{ensure, human, project::Project, run, signal, SettledRun};

/// A prepared, activated copy of the fixture with its own feed, ready to
/// push events at.
struct Rig {
    disp: Dispatcher,
    project: Project,
    pid: Uuid,
    feed: SseFake,
    known: HashSet<Uuid>,
}

impl Rig {
    /// One rig on the install under test, its trigger reading the feed.
    async fn up() -> anyhow::Result<Self> {
        let disp = ensure::up().await?;
        let mut project = Project::prepare("steer_by_tag", disp.clone()).await?;
        let feed = SseFake::start().await?;
        project.substitute_in_main("__E2E_FAKE_URL__", &feed.url())?;
        project.activate().await?;
        feed.wait_for_subscriber(Duration::from_secs(60)).await?;
        let pid = project.id();
        let known = run::runs(&disp, &pid).await?;
        Ok(Self {
            disp,
            project,
            pid,
            feed,
            known,
        })
    }

    /// One SSE event's data line. The listener collapses two fires whose
    /// token and payload are identical while the first is still in
    /// flight (a transport retry of one event must land once), and the
    /// fake feed carries no `id:` line the way a real source would, so
    /// each push gets a nonce: the rig's events are distinct events.
    fn event_data(value: &str) -> String {
        json!({ "value": value, "nonce": Uuid::new_v4() }).to_string()
    }

    /// Push one event and return the run it started.
    async fn fire(&mut self, event: &str, value: &str) -> anyhow::Result<Uuid> {
        self.feed.push_event(event, &Self::event_data(value))?;
        let run_id =
            run::wait_for_triggered_run(&self.disp, &self.pid, &self.known, Duration::from_secs(60))
                .await?;
        self.known.insert(run_id);
        Ok(run_id)
    }

    async fn summary(&self, run_id: Uuid) -> anyhow::Result<Value> {
        self.disp.get_json(&format!("/runs/{run_id}")).await
    }

    async fn parked(&self, run_id: Uuid) -> anyhow::Result<()> {
        run::wait_for_status(&self.disp, run_id, "waiting_for_input").await
    }

    async fn finish(self) -> anyhow::Result<()> {
        self.project.finish().await
    }
}

/// The debounce, three deep: each new message for a sender stops the
/// run answering the previous one, the survivor answers, and the tags
/// show on the run in the API, the listing, and the CLI.
#[tokio::test]
async fn later_run_stops_earlier_runs_for_the_same_sender() -> anyhow::Result<()> {
    let mut rig = Rig::up().await?;

    // Run 1: parks on its form. Capture the form's token while it exists.
    let first = rig.fire("go", "user_7").await?;
    let first_form = human::wait_for_form_by_node(&rig.disp, &rig.pid, "review").await?;
    let first_token = first_form.token().expect("the parked run's form has a token").to_string();

    // Both tags are on the run while it is alive, in the order they were
    // written (the created port first, then the `tags` list), and the
    // tagging is on the record as its own event.
    let first_summary = rig.summary(first).await?;
    assert_eq!(first_summary["status"], "waiting_for_input", "{first_summary}");
    assert_eq!(first_summary["tags"], json!(["user_7", "everyone"]), "{first_summary}");

    // Run 2: tags itself, stops run 1, parks. Run 1 ends cancelled BY run 2.
    let second = rig.fire("go", "user_7").await?;
    let first_settled = SettledRun::observe(&rig.disp, first).await?;
    assert_eq!(first_settled.status, "cancelled", "run 1 was stopped by run 2");
    assert_stopped_by(&first_settled, second, "user_7")?;
    assert_tagged_event(&first_settled, &["user_7", "everyone"])?;

    // Run 1's form is gone: firing its old token finds no signal, and
    // the run stays exactly as it was (no resume, no new events).
    let events_before = first_settled.replay().events.len();
    let fire = signal::fire_token(&rig.disp, &first_token, &json!({ "decision": "approve" })).await;
    assert!(fire.is_err(), "a stopped run's form must be gone, not answerable");
    tokio::time::sleep(Duration::from_secs(3)).await;
    let first_again = SettledRun::observe(&rig.disp, first).await?;
    assert_eq!(first_again.status, "cancelled");
    assert_eq!(
        first_again.replay().events.len(),
        events_before,
        "answering a stopped run's form must wake nothing: {:?}",
        first_again.replay().events
    );

    // Run 3: stops run 2 the same way. Only run 3 is left, still parked.
    let third = rig.fire("go", "user_7").await?;
    let second_settled = SettledRun::observe(&rig.disp, second).await?;
    assert_eq!(second_settled.status, "cancelled", "run 2 was stopped by run 3");
    assert_stopped_by(&second_settled, third, "user_7")?;

    // The tags are where a person looks for them: the run listing
    // (what the sidebar and `weft runs` read) and `weft events`
    // (which prints the cancel's reason and the tagging).
    rig.parked(third).await?;
    let listing: Value = rig
        .disp
        .get_json(&format!("/runs?project_id={}&limit=50", rig.pid))
        .await?;
    let row = listing["runs"]
        .as_array()
        .and_then(|rows| rows.iter().find(|r| r["runId"] == json!(third)))
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("run 3 missing from the listing: {listing}"))?;
    assert_eq!(row["tags"], json!(["user_7", "everyone"]), "{row}");
    let events_cli = rig.project.weft(&["events", &second.to_string()]).await?;
    anyhow::ensure!(
        events_cli.contains(&format!("reason=Stopped by run {third} (tag user_7)")),
        "weft events must print who stopped run 2:\n{events_cli}"
    );
    anyhow::ensure!(
        events_cli.contains("run_tagged") && events_cli.contains("tags=user_7,everyone"),
        "weft events must print the tagging:\n{events_cli}"
    );
    let runs_cli = rig.project.weft(&["runs"]).await?;
    let third_line = runs_cli
        .lines()
        .find(|l| l.starts_with(&third.to_string()))
        .ok_or_else(|| anyhow::anyhow!("run 3 missing from weft runs:\n{runs_cli}"))?;
    anyhow::ensure!(
        third_line.ends_with("user_7,everyone"),
        "weft runs must end the row with its tags: {third_line}"
    );

    // Answering run 3's form completes it with the approval.
    let third_form = human::wait_for_form_by_node(&rig.disp, &rig.pid, "review").await?;
    human::answer_form(&rig.disp, &third_form, &json!({ "decision": "approve" })).await?;
    let third_settled = SettledRun::observe(&rig.disp, third).await?;
    third_settled.completed()?;
    third_settled.assert_input("out", "data", &json!(true))?;

    rig.finish().await
}

/// The stopped run's terminal names the run that asked and the tag
/// that matched, on the run row AND on the parked node's own
/// cancel row, so the inspector never shows it as a plain failure.
fn assert_stopped_by(settled: &SettledRun, by: Uuid, tag: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        settled.status == "cancelled",
        "run {} must be cancelled, is {}",
        settled.run_id,
        settled.status
    );
    let terminal = settled
        .replay()
        .first_kind("run_cancelled")
        .ok_or_else(|| anyhow::anyhow!("no run_cancelled for {}", settled.run_id))?;
    let cause = terminal.0["cause"].clone();
    anyhow::ensure!(
        cause == json!({ "kind": "run", "by": by, "tag": tag }),
        "cause on {}: {cause}",
        settled.run_id
    );
    let reason = settled.cancel_reason().unwrap_or_default();
    anyhow::ensure!(
        reason.contains(&by.to_string()) && reason.contains(tag),
        "reason on {}: {reason}",
        settled.run_id
    );
    // A node is cancelled only if one was live when the stop landed: a
    // run stopped between two nodes (one finished, the next not yet
    // started) has nothing to cancel, and that is the stop working.
    if !live_before_cancel(settled).is_empty() {
        let node_cancel = settled
            .replay()
            .first_kind("node_cancelled")
            .ok_or_else(|| anyhow::anyhow!("no node_cancelled for the live node on {}", settled.run_id))?;
        anyhow::ensure!(
            node_cancel.str_field("reason").unwrap_or_default() == reason,
            "the live node's cancel row names the same cause: {:?}",
            node_cancel.0
        );
    }
    Ok(())
}

/// The nodes that had started (or resumed, or parked) and not ended
/// when the run's cancel was written: the ones the stop must cancel.
fn live_before_cancel(settled: &SettledRun) -> Vec<String> {
    let mut live: Vec<String> = Vec::new();
    for event in &settled.replay().events {
        let Some(node) = event.node() else { continue };
        match event.kind() {
            "node_started" | "node_resumed" | "node_suspended" => {
                if !live.iter().any(|n| n == node) {
                    live.push(node.to_string());
                }
            }
            "node_completed" | "node_failed" | "node_skipped" => live.retain(|n| n != node),
            "node_cancelled" => return live,
            _ => {}
        }
    }
    live
}

/// The tagging is on the record as one `run_tagged` carrying the
/// tags in the order the node put them on.
fn assert_tagged_event(settled: &SettledRun, tags: &[&str]) -> anyhow::Result<()> {
    let tagged = settled
        .replay()
        .first_kind("run_tagged")
        .ok_or_else(|| anyhow::anyhow!("no run_tagged on {}", settled.run_id))?;
    anyhow::ensure!(
        tagged.0["tags"] == json!(tags),
        "tags on {}: {:?}",
        settled.run_id,
        tagged.0
    );
    Ok(())
}
