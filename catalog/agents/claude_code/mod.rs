//! ClaudeCode: one conversation with Claude Code. Everything it does is
//! the agent nodes' shared shape (`agent.rs`); the agent's own facts are
//! `server/agents.json` under `claude`, and its adapter
//! `server/adapter_claude.py`.

use async_trait::async_trait;

use weft::{InfraProvisionContext, InfraSpec, Node, NodeManifest, RunContext, ValueBag, WeftResult};

use super::agent;

#[derive(NodeManifest)]
pub struct ClaudeCodeNode;

#[cfg(feature = "node-tests")]
mod tests;

// SYNC: the kind <-> catalog/agents/server/agents.json (claude), catalog/agents/server/adapter_claude.py (ClaudeCode.kind)
pub(crate) const KIND: &str = "claude";

#[async_trait]
impl Node for ClaudeCodeNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn provision_infra(&self, ctx: InfraProvisionContext, input: ValueBag) -> WeftResult<InfraSpec> {
        agent::provision(KIND, &ctx, &input)
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        agent::run(KIND, ctx).await
    }
}
