//! Shared by `ListRuns` and `CountRuns`: how a node reads which of the
//! project's runs it asks about. One reader, so the two nodes always
//! reach the same runs for the same inputs.

use weft::context::RunQuery;
use weft::{ExecutionContext, WeftError, WeftResult};

/// `ctx.runs()` narrowed by every filter input the node was given. An
/// empty text input narrows nothing. The tag is read the way `TagRun`
/// writes it, so the same value finds the runs `TagRun` tagged with it.
pub fn runs_from_inputs(ctx: &ExecutionContext) -> WeftResult<RunQuery<'_>> {
    let text = |name: &str| -> WeftResult<Option<String>> {
        Ok(ctx.inputs.opt::<String>(name)?.filter(|s| !s.trim().is_empty()))
    };
    let mut query = ctx.runs();
    if let Some(instance) = text("instance")? {
        query = query.instance(instance);
    }
    if let Some(status) = text("status")? {
        query = query.status(&status)?;
    }
    if let Some(node) = text("node")? {
        query = query.node(node);
    }
    if let Some(tag) = text("tag")? {
        query = query.tag(&tag)?;
    }
    if let Some(secs) = ctx.inputs.opt::<f64>("olderThanSecs")? {
        if !(secs >= 0.0) || secs.fract() != 0.0 {
            return Err(WeftError::Input(format!(
                "olderThanSecs is an age in whole seconds, so it cannot be {secs}"
            )));
        }
        query = query.older_than(std::time::Duration::from_secs(secs as u64));
    }
    Ok(query)
}
