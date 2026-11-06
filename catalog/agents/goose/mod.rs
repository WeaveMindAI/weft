//! Goose: one conversation with goose. Everything it does is the agent
//! nodes' shared shape (`agent.rs`); the agent's own facts are
//! `server/agents.json` under `goose`, and its adapter
//! `server/acp_goose.py`.

use async_trait::async_trait;

use weft::{InfraProvisionContext, InfraSpec, Node, NodeManifest, RunContext, ValueBag, WeftResult};

use super::agent;

#[derive(NodeManifest)]
pub struct GooseNode;

// SYNC: the kind <-> catalog/agents/server/agents.json (goose), catalog/agents/server/acp_goose.py (AcpSpec "goose")
pub(crate) const KIND: &str = "goose";

#[async_trait]
impl Node for GooseNode {
    async fn provision_infra(&self, ctx: InfraProvisionContext, input: ValueBag) -> WeftResult<InfraSpec> {
        agent::provision(KIND, &ctx, &input)
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        agent::run(KIND, ctx).await
    }
}
