//! Shared by the instance nodes: how they read an instance id (one they
//! always need, or one that, left out, means the program's own copies),
//! and, for the ones that take something down, what happens to the
//! triggers and runs they reach and whether the run doing it goes too.
//! One reader, so every such node offers the same choices with the same
//! defaults.

use weft::instance::InstanceId;
use weft::node::NodeOutput;
use weft::program::InfraDownAnswer;
use weft::{node_bail, DeactivateSpec, DeactivationMode, ExecutionContext, RunningPolicy, StopSelf, WeftResult};

/// The instance id a node was handed, checked the way every instance id
/// is (`InstanceId::new`: never blank, a plain id), so a bad one fails
/// here naming the input.
pub fn instance(ctx: &ExecutionContext) -> WeftResult<InstanceId> {
    let instance: String = ctx.inputs.get("instance")?;
    match InstanceId::new(instance) {
        Ok(id) => Ok(id),
        Err(why) => node_bail!("instance: {why}"),
    }
}

/// The instance id a node acting on either kind of copy was handed, or
/// `None` when `instance` is unwired, which means the program's own
/// copies. A given id is checked like [`instance`], so an empty one (a
/// value that never arrived) fails rather than reaching the program's
/// own copies by accident.
pub fn instance_if_given(ctx: &ExecutionContext) -> WeftResult<Option<InstanceId>> {
    match ctx.inputs.opt::<String>("instance")? {
        None => Ok(None),
        Some(given) => match InstanceId::new(given) {
            Ok(id) => Ok(Some(id)),
            Err(why) => node_bail!("instance: {why}"),
        },
    }
}

/// What a take-down does to the triggers it reaches (`triggers`: park,
/// hibernate or wipe) and to the runs they fired (`running`: wait or
/// cancel). Waiting before a wipe is refused, as everywhere else.
pub fn take_down_spec(ctx: &ExecutionContext) -> WeftResult<DeactivateSpec> {
    // Read typed: a word outside the enum fails naming the input, the
    // word, and the words it takes.
    let mode: DeactivationMode = ctx.inputs.get("triggers")?;
    let running_policy: RunningPolicy = ctx.inputs.get("running")?;
    let grace: f64 = ctx.inputs.get("graceMinutes")?;
    if grace < 0.0 || grace.fract() != 0.0 || grace > f64::from(u32::MAX) {
        node_bail!("graceMinutes must be a whole number of minutes from 0 to {}, got {grace}", u32::MAX);
    }
    let spec = DeactivateSpec {
        mode,
        grace_minutes: grace as u32,
        running_policy,
        drain_timeout_secs: None,
    };
    if let Err(why) = spec.validate() {
        node_bail!("{why}");
    }
    Ok(spec)
}

/// Whether the run doing the take-down goes too, when it is among what
/// the take-down reaches.
pub fn stop_self(ctx: &ExecutionContext) -> WeftResult<StopSelf> {
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
