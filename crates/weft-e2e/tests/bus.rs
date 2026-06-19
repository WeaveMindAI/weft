//! Two nodes converse over a bus; assert the conversation from the event log.
#![cfg(feature = "e2e")]

use weft_e2e::{ensure, project::Project, run};

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
