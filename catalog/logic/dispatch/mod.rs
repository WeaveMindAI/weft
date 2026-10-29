//! Dispatch: send a value to whichever branch claims it.
//!
//! Each branch is an input the program adds and the output of the same
//! name, which the compiler makes (`features.pairsAddedPorts`). A branch
//! input carries a record, usually written there by a metadata wire
//! (`route.search = search.tool`), and the branch whose `by` field
//! equals the value's own takes the value on its output. Every other
//! output is left unset, so it closes and its branch never runs. A
//! value no branch claims leaves on `otherwise`.
//!
//! Two branches with the same `by` field would make the choice depend
//! on written order, which nobody reading the graph would guess, so
//! that fails the node instead.

use async_trait::async_trait;
use serde_json::Value;

use weft::node::NodeOutput;
use weft::{node_bail, Node, NodeManifest, RunContext, WeftResult};

use super::compare::same;

#[derive(NodeManifest)]
pub struct DispatchNode;

#[cfg(feature = "node-tests")]
mod tests;

/// The branch whose `by` field equals `value`'s, or `None` when no
/// branch claims it. `branches` is every branch in written order, each
/// with the value its input carries. Fails on a value or a branch with
/// no `by` field, and on two branches with equal ones, each naming what
/// to fix.
fn claimant<'a>(value: &Value, by: &str, branches: &[(&'a str, &Value)]) -> WeftResult<Option<&'a str>> {
    let mut keyed: Vec<(&'a str, &Value)> = Vec::with_capacity(branches.len());
    for &(name, branch) in branches {
        let Some(key) = branch.get(by) else {
            node_bail!("branch `{name}` has no `{by}` field to compare with the value's");
        };
        if let Some((first, _)) = keyed.iter().find(|(_, earlier)| same(earlier, key)) {
            node_bail!("branches `{first}` and `{name}` have the same `{by}` ({key}), so a value could not tell them apart");
        }
        keyed.push((name, key));
    }
    let Value::Object(fields) = value else {
        node_bail!("the value to dispatch is not an object, so it has no `{by}` field to compare: {value}");
    };
    let Some(key) = fields.get(by) else {
        node_bail!("the value to dispatch has no `{by}` field to compare with the branches'");
    };
    Ok(keyed.into_iter().find(|(_, branch)| same(branch, key)).map(|(name, _)| name))
}

#[async_trait]
impl Node for DispatchNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let value: Value = ctx.inputs.get("value")?;
        let by: String = ctx.inputs.get("by")?;

        // `created` is every branch the program added, in written order,
        // a silent one included, so a branch whose input carried nothing
        // is named here instead of quietly never matching.
        let mut branches: Vec<(&str, &Value)> = Vec::new();
        for name in ctx.inputs.created() {
            let Some(branch) = ctx.inputs.raw(name) else {
                node_bail!("branch `{name}` received nothing; its input carries the record the value is compared with");
            };
            branches.push((name.as_str(), branch));
        }

        let port = claimant(&value, &by, &branches)?.unwrap_or("otherwise");
        ctx.pulse_downstream(NodeOutput::new().set(port, value)).await
    }
}
