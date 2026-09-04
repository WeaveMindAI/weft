//! Shared by `TagRun` and `StopTagged`: how a steering node reads the
//! tags it was handed. One reader, so the two nodes can never disagree
//! on what counts as a tag.

use weft::{node_bail, ExecutionContext, WeftResult};

/// The tags a steering node was handed: every created input (one tag
/// each, typed `String` by both manifests) plus the optional `tags`
/// list, created ports first, duplicates dropped. Empty is a failure:
/// a node with nothing to tag or stop is a wiring mistake, never a
/// silent no-op.
pub fn tags_from_inputs(ctx: &ExecutionContext) -> WeftResult<Vec<String>> {
    let mut tags: Vec<String> = Vec::new();
    for (name, value) in ctx.inputs.custom() {
        // `tags` is the declared list, read below; everything else in
        // the instance data is a created port.
        if name == "tags" {
            continue;
        }
        match value.as_str() {
            Some(tag) => tags.push(tag.to_string()),
            None => node_bail!("input '{name}' must carry a string tag, got {value}"),
        }
    }
    if let Some(list) = ctx.inputs.opt::<Vec<String>>("tags")? {
        tags.extend(list);
    }
    let mut seen = std::collections::HashSet::new();
    tags.retain(|t| seen.insert(t.clone()));
    if tags.is_empty() {
        node_bail!(
            "no tags: wire at least one input onto the node (`TagRun(sender: String)`) or a \
             `tags` list"
        );
    }
    Ok(tags)
}
