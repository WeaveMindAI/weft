//! Shared by the instance nodes that take something down: how the triggers they
//! reach go down (`door`, `doorForMinutes`, `held`, `running`, the same
//! three questions `weft deactivate` and `weft infra stop` answer) and
//! whether the run doing it goes too. One reader, so every such node
//! offers the same choices with the same defaults.

use weft::node::NodeOutput;
use weft::program::InfraDownAnswer;
use weft::{Door, Held, RunContext, Running, StopSelf, TakeDownChoice, WeftResult};

/// How a take-down brings the triggers it reaches down: what happens to
/// work arriving (`door`, refusing after `doorForMinutes` when set), to
/// the work they already hold (`held`), and to the runs going (`running`).
/// A contradiction (`doorForMinutes` beside a refusing door) is the
/// node's rule, refused before it runs.
pub fn take_down_choice(ctx: &RunContext) -> WeftResult<TakeDownChoice> {
    // Read typed: a word outside the enum fails naming the input, the
    // word, and the words it takes.
    Ok(TakeDownChoice {
        door: Some(ctx.inputs.get::<Door>("door")?),
        door_for_minutes: ctx.inputs.opt::<u32>("doorForMinutes")?,
        held: Some(ctx.inputs.get::<Held>("held")?),
        running: Some(ctx.inputs.get::<Running>("running")?),
    })
}

/// Whether the run doing the take-down goes too, when it is among what
/// the take-down reaches.
pub fn stop_self(ctx: &RunContext) -> WeftResult<StopSelf> {
    Ok(if ctx.inputs.get::<bool>("includeSelf")? { StopSelf::Include } else { StopSelf::Keep })
}

/// What a node that takes a copy down sends on: `done` when the copy is
/// down (taken down now, or already down), `noCopy` when there is no such
/// copy (never started, terminated before, or a mistyped instance id), so
/// a program answering a person can tell them which.
pub fn taken_down(answer: InfraDownAnswer) -> NodeOutput {
    match answer {
        InfraDownAnswer::TakenDown | InfraDownAnswer::AlreadyDown => NodeOutput::new().set("done", true),
        InfraDownAnswer::NoCopy => NodeOutput::new().set("noCopy", true),
    }
}
