//! Shared by `TagRun` and `StopTagged`: how a steering node reads the
//! tags it was handed. One reader, so the two nodes can never disagree
//! on what counts as a tag. Any string works: `ctx.tag_run` and
//! `ctx.stop_tagged` normalise each value the same way
//! (`weft::tag::normalize_tag`), so the same value always lands on the
//! same tag from either side, and refuse an empty one.

use weft::{RunContext, WeftResult};

/// The tags a steering node was handed: every created input (one tag
/// each, typed `String` by both manifests) plus the optional `tags`
/// list, created ports first. A node with neither is refused by its
/// rule, before it runs.
pub fn tags_from_inputs(ctx: &RunContext) -> WeftResult<Vec<String>> {
    // `custom()` is the created ports only; the declared `tags` list
    // is read below.
    let mut tags = ctx.inputs.custom().map(|(name, _)| ctx.inputs.get::<String>(name)).collect::<WeftResult<Vec<_>>>()?;
    tags.extend(ctx.inputs.opt::<Vec<String>>("tags")?.unwrap_or_default());
    Ok(tags)
}
