//! Two nodes converse over a bus; assert the conversation from the event log.
#![cfg(feature = "e2e")]

use weft_e2e::{ensure, project::Project, run, SettledRun};

#[tokio::test]
async fn host_and_guest_converse_over_a_bus() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("bus_chat", disp).await?;

    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;

    // A bus with both participants (host + guest) and at least one message.
    let bus_id = settled.assert_bus_conversation(2, 1)?;
    // The host closes the bus after its last send.
    assert!(
        settled.bus_closed(&bus_id),
        "expected bus {bus_id} to be closed; messages: {:?}",
        settled.bus_messages(&bus_id)
    );

    project.finish().await
}

/// The host waits on a timer after its first exchange: every node waits
/// and one of them on a timer, so the run pauses with its bus open; the
/// timer resumes it on a fresh worker, where the bus comes back with its
/// members and both nodes carry on where they stopped. The conversation
/// finishes on the one bus, every line said once.
#[tokio::test]
async fn a_bus_conversation_carries_on_across_a_pause() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("bus_chat", disp.clone()).await?;
    // Long enough that the status poll sees the run parked.
    project.set_node_config("host", "pauseSeconds", "10")?;

    // Paused at once, not after the quiet window a run first stays on its
    // worker for (`pauseAfterSecs`), which would outlast the timer.
    let run_id = run::start_with(&mut project, &["--pause-after-secs", "0"]).await?;
    run::wait_for_status(&disp, run_id, "waiting_for_input").await?;
    let settled = SettledRun::observe(&disp, run_id).await?;
    settled.completed()?;

    assert_eq!(settled.bus_ids().len(), 1, "one bus, opened again after the pause");
    let bus_id = settled.assert_bus_conversation(2, 6)?;
    let said: Vec<(String, Option<serde_json::Value>)> =
        settled.bus_messages(&bus_id).into_iter().map(|m| (m.from, m.value)).collect();
    let line = |from: &str, text: &str| (from.to_string(), Some(serde_json::json!(text)));
    assert_eq!(
        said,
        vec![
            line("host", "hello"),
            line("guest", "hey there"),
            line("host", "what's up"),
            line("guest", "not much, you?"),
            line("host", "ok bye"),
            line("guest", "later"),
        ],
        "every line said once, in order, across the pause"
    );
    assert!(settled.bus_closed(&bus_id), "the host closed the bus after its last line");

    project.finish().await
}
