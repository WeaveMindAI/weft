//! Executions steering each other by tag, end to end, through the two
//! catalog nodes `TagRun` and `StopTagged` and nothing else.
//!
//! The `steer_by_tag` fixture has three programs on one fake SSE feed:
//! "go" (the debounce: tag with the sender, stop the earlier runs for
//! that sender, park on a form), "purge" (an untagged operator run stops
//! everything carrying a tag and survives) and "abort" (a run tags
//! itself and takes the whole batch down, itself included). Every test
//! is one contract from the steering page of the book:
//!
//!   - the newest run for a sender is the only one left, and the ones it
//!     stopped read as stopped BY it with the tag named;
//!   - a stopped run's form is gone (its wake was erased);
//!   - senders never touch each other;
//!   - two runs a moment apart do not kill each other: one survivor;
//!   - a stop reaches a run whose node is in flight, not only a parked one;
//!   - `includeSelf` reaches everything, the asker included when it
//!     carries the tag and not when it does not;
//!   - a bad tag fails the run loudly at the node;
//!   - a stop never crosses a project;
//!   - the tags show on the run everywhere a person looks for them.
#![cfg(feature = "e2e")]

use std::collections::HashSet;
use std::time::Duration;

use serde_json::{json, Value};
use uuid::Uuid;
use weft_e2e::client::Dispatcher;
use weft_e2e::fakes::{PollFake, SseFake};
use weft_e2e::{ensure, human, project::Project, run, signal, SettledRun};

/// The fixture's three trigger nodes each hold one SSE connection to the
/// feed, so a push is only safe once all three are reading.
const FEED_SUBSCRIBERS: usize = 3;

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
    /// One rig on a freshly swept cluster. A test that needs two rigs
    /// calls [`Self::up_on`] with one `ensure::up` dispatcher: the sweep
    /// in `ensure::up` deletes every project, the first rig's included.
    async fn up() -> anyhow::Result<Self> {
        Self::up_on(ensure::up().await?).await
    }

    async fn up_on(disp: Dispatcher) -> anyhow::Result<Self> {
        let project = Project::prepare("steer_by_tag", disp.clone()).await?;
        let feed = SseFake::start().await?;
        project.substitute_in_main("__E2E_FAKE_URL__", &feed.url())?;
        Self::activate(disp, project, feed, FEED_SUBSCRIBERS).await
    }

    async fn activate(
        disp: Dispatcher,
        mut project: Project,
        feed: SseFake,
        subscribers: usize,
    ) -> anyhow::Result<Self> {
        project.activate().await?;
        feed.wait_for_subscribers(subscribers, Duration::from_secs(60)).await?;
        let pid = project.id();
        let known = run::execution_colors(&disp, &pid).await?;
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
        self.feed.push_event(event, &Self::event_data(value));
        let color =
            run::wait_for_triggered_execution(&self.disp, &self.pid, &self.known, Duration::from_secs(60))
                .await?;
        self.known.insert(color);
        Ok(color)
    }

    /// Push `values` as a burst of "go" events with no wait between them
    /// and return the runs they started.
    async fn fire_burst(&mut self, values: &[&str]) -> anyhow::Result<Vec<Uuid>> {
        for value in values {
            self.feed.push_event("go", &Self::event_data(value));
        }
        let colors = run::wait_for_triggered_executions(
            &self.disp,
            &self.pid,
            &self.known,
            values.len(),
            Duration::from_secs(60),
        )
        .await?;
        self.known.extend(colors.iter().copied());
        Ok(colors)
    }

    async fn summary(&self, color: Uuid) -> anyhow::Result<Value> {
        self.disp.get_json(&format!("/executions/{color}")).await
    }

    async fn parked(&self, color: Uuid) -> anyhow::Result<()> {
        run::wait_for_status(&self.disp, color, "waiting_for_input").await
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

    // The tags are where a person looks for them: the execution listing
    // (what the sidebar and `weft executions` read) and `weft events`
    // (which prints the cancel's reason and the tagging).
    rig.parked(third).await?;
    let listing: Value = rig
        .disp
        .get_json(&format!("/executions?project_id={}&limit=50", rig.pid))
        .await?;
    let row = listing["executions"]
        .as_array()
        .and_then(|rows| rows.iter().find(|r| r["color"] == json!(third)))
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("run 3 missing from the listing: {listing}"))?;
    assert_eq!(row["tags"], json!(["user_7", "everyone"]), "{row}");
    let events_cli = rig.project.weft(&["events", &second.to_string()]).await?;
    anyhow::ensure!(
        events_cli.contains(&format!("reason=Stopped by execution {third} (tag user_7)")),
        "weft events must print who stopped run 2:\n{events_cli}"
    );
    anyhow::ensure!(
        events_cli.contains("execution_tagged") && events_cli.contains("tags=user_7,everyone"),
        "weft events must print the tagging:\n{events_cli}"
    );
    let executions_cli = rig.project.weft(&["executions"]).await?;
    let third_line = executions_cli
        .lines()
        .find(|l| l.starts_with(&third.to_string()))
        .ok_or_else(|| anyhow::anyhow!("run 3 missing from weft executions:\n{executions_cli}"))?;
    anyhow::ensure!(
        third_line.ends_with("user_7,everyone"),
        "weft executions must end the row with its tags: {third_line}"
    );

    // Answering run 3's form completes it with the approval.
    let third_form = human::wait_for_form_by_node(&rig.disp, &rig.pid, "review").await?;
    human::answer_form(&rig.disp, &third_form, &json!({ "decision": "approve" })).await?;
    let third_settled = SettledRun::observe(&rig.disp, third).await?;
    third_settled.completed()?;
    third_settled.assert_input("out", "data", &json!(true))?;

    rig.finish().await
}

/// A stop is by tag: a message from one sender leaves every other
/// sender's run exactly where it was.
#[tokio::test]
async fn senders_do_not_touch_each_other() -> anyhow::Result<()> {
    let mut rig = Rig::up().await?;

    let alice = rig.fire("go", "alice").await?;
    let bob = rig.fire("go", "bob").await?;
    rig.parked(alice).await?;
    rig.parked(bob).await?;

    // Alice again: her first run dies, Bob's is untouched.
    let alice_again = rig.fire("go", "alice").await?;
    let alice_settled = SettledRun::observe(&rig.disp, alice).await?;
    assert_stopped_by(&alice_settled, alice_again, "alice")?;
    rig.parked(alice_again).await?;
    assert_eq!(run::status_of(&rig.disp, bob).await?, "waiting_for_input");

    rig.finish().await
}

/// Two messages from one sender land a moment apart: both runs say
/// "stop the others, keep me". The tag order decides, so exactly one
/// survives and the other reads as stopped by it. Never zero, never two.
#[tokio::test]
async fn two_messages_at_once_leave_exactly_one_survivor() -> anyhow::Result<()> {
    let mut rig = Rig::up().await?;

    let burst = rig.fire_burst(&["user_7", "user_7"]).await?;
    let [a, b] = burst.as_slice() else { unreachable!() };
    let (a, b) = (*a, *b);

    // One of the two settles as cancelled; wait for whichever it is. The
    // timeout names what both runs were last doing.
    let last = std::sync::Mutex::new(String::new());
    let (dead, alive) = weft_e2e::poll_until_describing(
        "one of the two burst runs to be cancelled",
        run::RUN_SETTLE_DEADLINE,
        Duration::from_millis(300),
        || {
            let disp = rig.disp.clone();
            let last = &last;
            async move {
                let mut seen = String::new();
                for (x, y) in [(a, b), (b, a)] {
                    let now = run::status_of(&disp, x).await?;
                    seen.push_str(&format!("{x}: {now}; "));
                    if now == "cancelled" {
                        return Ok(Some((x, y)));
                    }
                }
                *last.lock().unwrap() = seen;
                Ok(None)
            }
        },
        || format!("last statuses observed: {}", last.lock().unwrap()),
    )
    .await?;
    let dead_settled = SettledRun::observe(&rig.disp, dead).await?;
    assert_stopped_by(&dead_settled, alive, "user_7")?;
    rig.parked(alive).await?;

    // The survivor stays alive: it was tagged after the run it stopped,
    // so that run's own stop never reached it.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(run::status_of(&rig.disp, alive).await?, "waiting_for_input");

    rig.finish().await
}

/// A stop reaches a run whose node is in flight, not only a parked one:
/// the held node is cancelled with the same cause, and the run that did
/// the stopping goes on to finish once released.
#[tokio::test]
async fn a_stop_reaches_a_run_in_flight() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("steer_by_tag", disp.clone()).await?;
    let feed = SseFake::start().await?;
    let gate = PollFake::start("hold").await?;
    project.add_node_from_fixture("lifecycle", "hold_gate")?;
    project.set_main(&graph_in_flight(&feed.url(), &gate.url()))?;
    let mut rig = Rig::activate(disp, project, feed, 1).await?;

    let first = rig.fire("go", "user_7").await?;
    run::wait_for_status(&rig.disp, first, "running").await?;

    let second = rig.fire("go", "user_7").await?;
    let first_settled = SettledRun::observe(&rig.disp, first).await?;
    assert_stopped_by(&first_settled, second, "user_7")?;
    let held = first_settled
        .replay()
        .by_kind("node_cancelled")
        .find(|e| e.is_node("hold"))
        .ok_or_else(|| anyhow::anyhow!("the in-flight node must be cancelled: {:?}", first_settled.replay().events))?;
    anyhow::ensure!(
        held.str_field("reason").unwrap_or_default().contains("user_7"),
        "the held node's cancel names the tag: {:?}",
        held.0
    );

    // The survivor is still held; release it and it completes.
    assert_eq!(run::status_of(&rig.disp, second).await?, "running");
    gate.set_body("release").await;
    SettledRun::observe(&rig.disp, second)
        .await?
        .completed()?
        .assert_input("out", "data", &json!("released"))?;

    rig.finish().await
}

/// `includeSelf` from a run that never tagged itself: every run carrying
/// the tag goes, whoever the sender was, and the asker survives because
/// it does not carry the tag. Having no tag of its own, it reaches
/// everything tagged so far.
#[tokio::test]
async fn purge_stops_every_run_carrying_the_tag_and_spares_the_untagged_asker() -> anyhow::Result<()> {
    let mut rig = Rig::up().await?;

    let alice = rig.fire("go", "alice").await?;
    let bob = rig.fire("go", "bob").await?;
    rig.parked(alice).await?;
    rig.parked(bob).await?;

    let purge = rig.fire("purge", "everyone").await?;
    for victim in [alice, bob] {
        let settled = SettledRun::observe(&rig.disp, victim).await?;
        assert_stopped_by(&settled, purge, "everyone")?;
    }

    // The purge run carries no tag, is still alive, and answers.
    rig.parked(purge).await?;
    let purge_summary = rig.summary(purge).await?;
    assert_eq!(purge_summary["tags"], json!([]), "{purge_summary}");
    let form = human::wait_for_form_by_node(&rig.disp, &rig.pid, "purge_confirm").await?;
    human::answer_form(&rig.disp, &form, &json!({ "ack": "approve" })).await?;
    SettledRun::observe(&rig.disp, purge)
        .await?
        .completed()?
        .assert_input("purge_out", "data", &json!(true))?;

    rig.finish().await
}

/// `includeSelf` from a run that carries the tag: the whole batch goes,
/// the asker included, and the asker's own terminal names itself.
#[tokio::test]
async fn abort_takes_the_asker_down_too() -> anyhow::Result<()> {
    let mut rig = Rig::up().await?;

    let alice = rig.fire("go", "alice").await?;
    rig.parked(alice).await?;

    let abort = rig.fire("abort", "everyone").await?;
    let alice_settled = SettledRun::observe(&rig.disp, alice).await?;
    assert_stopped_by(&alice_settled, abort, "everyone")?;

    let abort_settled = SettledRun::observe(&rig.disp, abort).await?;
    assert_eq!(abort_settled.status, "cancelled", "the aborting run stops itself");
    assert_stopped_by(&abort_settled, abort, "everyone")?;
    assert_tagged_event(&abort_settled, &["everyone"])?;
    anyhow::ensure!(
        abort_settled.replay().for_node("abort_out").next().is_none(),
        "nothing past the self-stop may complete: {:?}",
        abort_settled.replay().events
    );

    rig.finish().await
}

/// A tag outside `[A-Za-z0-9_-]` fails at the node, naming the character,
/// before anything is written: the run fails, nothing is tagged.
#[tokio::test]
async fn a_bad_tag_fails_the_run_loudly() -> anyhow::Result<()> {
    let mut rig = Rig::up().await?;

    let color = rig.fire("go", "+33 6 12").await?;
    let settled = SettledRun::observe(&rig.disp, color).await?;
    settled.failed_with("invalid character '+'")?;
    anyhow::ensure!(
        settled.replay().first_kind("execution_tagged").is_none(),
        "a rejected tag must write nothing: {:?}",
        settled.replay().events
    );
    let summary = rig.summary(color).await?;
    assert_eq!(summary["tags"], json!([]), "{summary}");

    rig.finish().await
}

/// A stop is scoped to its project: the same tag in another project is
/// a different tag.
#[tokio::test]
async fn a_stop_never_crosses_a_project() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut left = Rig::up_on(disp.clone()).await?;
    let mut right = Rig::up_on(disp).await?;

    let left_run = left.fire("go", "user_7").await?;
    left.parked(left_run).await?;

    // Two messages on the right: the first right run dies, the left one
    // does not notice.
    let right_first = right.fire("go", "user_7").await?;
    right.parked(right_first).await?;
    let right_second = right.fire("go", "user_7").await?;
    let right_settled = SettledRun::observe(&right.disp, right_first).await?;
    assert_stopped_by(&right_settled, right_second, "user_7")?;
    right.parked(right_second).await?;
    assert_eq!(run::status_of(&left.disp, left_run).await?, "waiting_for_input");

    // A purge on the right, same story.
    let purge = right.fire("purge", "everyone").await?;
    SettledRun::observe(&right.disp, right_second).await?;
    right.parked(purge).await?;
    assert_eq!(run::status_of(&left.disp, left_run).await?, "waiting_for_input");

    right.finish().await?;
    left.finish().await
}

/// The "go" program with the form swapped for a HoldGate, so the run
/// sits in `running` on a node in flight instead of parked on a person.
fn graph_in_flight(sse_url: &str, release_url: &str) -> String {
    format!(
        "start = TestSseTrigger {{\n\
         \x20 url: \"{sse_url}\"\n\
         \x20 event_name: \"go\"\n\
         }}\n\
         claim = TagRun(sender: String)\n\
         claim.sender = start.value\n\
         stop = StopTagged(sender: String) {{\n\
         \x20 _should_flow: claim.done\n\
         }}\n\
         stop.sender = start.value\n\
         hold = HoldGate {{\n\
         \x20 _should_flow: stop.done\n\
         \x20 url: \"{release_url}\"\n\
         }}\n\
         hold.arm = start.value\n\
         out = Debug\n\
         out.data = hold.done\n"
    )
}

/// The stopped run's terminal names the run that asked and the tag
/// that matched, on the execution row AND on the parked node's own
/// cancel row, so the inspector never shows it as a plain failure.
fn assert_stopped_by(settled: &SettledRun, by: Uuid, tag: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        settled.status == "cancelled",
        "run {} must be cancelled, is {}",
        settled.color,
        settled.status
    );
    let terminal = settled
        .replay()
        .first_kind("execution_cancelled")
        .ok_or_else(|| anyhow::anyhow!("no execution_cancelled for {}", settled.color))?;
    let cause = terminal.0["cause"].clone();
    anyhow::ensure!(
        cause == json!({ "kind": "execution", "by": by, "tag": tag }),
        "cause on {}: {cause}",
        settled.color
    );
    let reason = settled.cancel_reason().unwrap_or_default();
    anyhow::ensure!(
        reason.contains(&by.to_string()) && reason.contains(tag),
        "reason on {}: {reason}",
        settled.color
    );
    let node_cancel = settled
        .replay()
        .first_kind("node_cancelled")
        .ok_or_else(|| anyhow::anyhow!("no node_cancelled for the live node on {}", settled.color))?;
    anyhow::ensure!(
        node_cancel.str_field("reason").unwrap_or_default() == reason,
        "the live node's cancel row names the same cause: {:?}",
        node_cancel.0
    );
    Ok(())
}

/// The tagging is on the record as one `execution_tagged` carrying the
/// tags in the order the node put them on.
fn assert_tagged_event(settled: &SettledRun, tags: &[&str]) -> anyhow::Result<()> {
    let tagged = settled
        .replay()
        .first_kind("execution_tagged")
        .ok_or_else(|| anyhow::anyhow!("no execution_tagged on {}", settled.color))?;
    anyhow::ensure!(
        tagged.0["tags"] == json!(tags),
        "tags on {}: {:?}",
        settled.color,
        tagged.0
    );
    Ok(())
}
