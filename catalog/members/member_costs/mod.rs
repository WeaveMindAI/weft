//! MemberCosts: what one member's runs cost, narrowed by the optional
//! filters, with the measured total.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::program::PaidBy;
use weft::{node_bail, ExecutionContext, Node, NodeManifest, WeftResult};

use super::lifecycle::member;

#[derive(NodeManifest)]
pub struct MemberCostsNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for MemberCostsNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let mut query = ctx.costs().member(member(&ctx)?);
        if let Some(service) = ctx.inputs.opt::<String>("service")?.filter(|s| !s.trim().is_empty()) {
            query = query.service(service);
        }
        if let Some(node) = ctx.inputs.opt::<String>("node")?.filter(|s| !s.trim().is_empty()) {
            query = query.node(node);
        }
        if let Some(paid_by) = ctx.inputs.opt::<String>("paidBy")?.filter(|s| !s.trim().is_empty()) {
            query = query.paid_by(match paid_by.as_str() {
                "platform" => PaidBy::Platform,
                "author" => PaidBy::Author,
                "member" => PaidBy::Member,
                other => node_bail!("paidBy must be platform, author or member, got '{other}'"),
            });
        }
        if let Some(since) = ctx.inputs.opt::<f64>("since")? {
            if !(since >= 0.0) {
                node_bail!("since is a moment in unix seconds, so it cannot be {since}");
            }
            query = query.since(since as u64);
        }
        let records = query.list().await?;
        // Folded from 0.0: `Sum` for f64 starts at -0.0, so a member with
        // no records would read a total of -0.
        let total: f64 = records.iter().filter_map(|r| r.amount_usd).fold(0.0, |sum, amount| sum + amount);
        let records = serde_json::to_value(&records).map_err(|e| weft::WeftError::NodeExecution(format!("costs: {e}")))?;
        ctx.pulse_downstream(NodeOutput::new().set("records", records).set("totalUsd", total)).await
    }
}
