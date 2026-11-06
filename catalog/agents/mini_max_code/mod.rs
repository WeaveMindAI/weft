//! MiniMaxCode: one conversation with MiniMax Code. Everything it does is the agent
//! nodes' shared shape (`agent.rs`); the agent's own facts are
//! `server/agents.json` under `minimax`, and its adapter
//! `server/acp_minimax.py`.

use async_trait::async_trait;

use weft::{InfraProvisionContext, InfraSpec, Node, NodeManifest, RunContext, ValueBag, WeftResult};

use super::agent;

#[derive(NodeManifest)]
pub struct MiniMaxCodeNode;

// SYNC: the kind <-> catalog/agents/server/agents.json (minimax), catalog/agents/server/acp_minimax.py (AcpSpec "minimax")
pub(crate) const KIND: &str = "minimax";

#[async_trait]
impl Node for MiniMaxCodeNode {
    async fn provision_infra(&self, ctx: InfraProvisionContext, input: ValueBag) -> WeftResult<InfraSpec> {
        agent::provision(KIND, &ctx, &input)
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        agent::run(KIND, ctx).await
    }
}
