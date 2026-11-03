//! Shared by `ListRuns` and `CountRuns`: how a node reads which of the
//! project's runs it asks about. One reader, so the two nodes always
//! reach the same runs for the same inputs.

use weft::context::RunQuery;
use weft::{RunContext, WeftResult};

/// `ctx.runs()` narrowed by every filter input the node was given. The
/// tag is read the way `TagRun` writes it, so the same value finds the
/// runs `TagRun` tagged with it.
pub fn runs_from_inputs(ctx: &RunContext) -> WeftResult<RunQuery<'_>> {
    let mut query = ctx.runs();
    if let Some(instance) = ctx.inputs.opt::<String>("instance")? {
        query = query.instance(instance);
    }
    if let Some(status) = ctx.inputs.opt::<String>("status")? {
        query = query.status(&status)?;
    }
    if let Some(node) = ctx.inputs.opt::<String>("node")? {
        query = query.node(node);
    }
    if let Some(tag) = ctx.inputs.opt::<String>("tag")? {
        query = query.tag(&tag)?;
    }
    if let Some(secs) = ctx.inputs.opt::<u64>("olderThanSecs")? {
        query = query.older_than(std::time::Duration::from_secs(secs));
    }
    Ok(query)
}
