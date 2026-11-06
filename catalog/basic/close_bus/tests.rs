//! CloseBus self-tests: an open bus ends finished, its readers' reads
//! ending, and a bus that already ended keeps the end it had. A close of
//! an open bus nobody is on is tested beside `BusHandle::close`
//! (`weft_core::bus`): a rig's bus runs in no engine, so it ends the
//! moment its last name leaves and is never open with nobody on it.

use serde_json::json;

use weft::bus::{BusOptions, StreamEnd};
use weft::{FakeRig, NodeTest, WeftResult};

use super::CloseBusNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("an_open_bus_ends_finished", ends_finished),
        NodeTest::fake("a_bus_that_already_failed_keeps_its_failure", keeps_failure),
    ]
}

/// A bus with a member on it and a message sent ends finished: the
/// member's handle reads it closed.
async fn ends_finished(rig: FakeRig) -> WeftResult<()> {
    let (mut bus, marker) = rig.seed_bus(BusOptions::default())?;
    bus.register("member").expect("a fresh bus accepts its first name");
    bus.send("event", json!("hello")).expect("an open bus accepts a send");
    let outcome = rig.run(&CloseBusNode, json!({ "bus": marker })).await.ok()?;
    assert!(outcome.outputs.is_empty(), "CloseBus emits nothing");
    assert!(bus.is_closed(), "the bus is closed");
    assert_eq!(bus.end(), Some(StreamEnd::Finished));
    Ok(())
}

/// A bus that already ended failed is not turned into a finished one.
async fn keeps_failure(rig: FakeRig) -> WeftResult<()> {
    let (bus, marker) = rig.seed_bus(BusOptions::default())?;
    let failed = StreamEnd::Failed { error: "the session went away".into() };
    bus.inner_arc().close(failed.clone());
    rig.run(&CloseBusNode, json!({ "bus": marker })).await.ok()?;
    assert_eq!(bus.end(), Some(failed), "the first end stands");
    Ok(())
}
