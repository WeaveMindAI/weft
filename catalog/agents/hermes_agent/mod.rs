//! HermesAgent: one conversation with Hermes Agent. Everything it does is the agent
//! nodes' shared shape (`agent.rs`); the agent's own facts are
//! `server/agents.json` under `hermes`, and its adapter
//! `server/acp_hermes.py`.

use async_trait::async_trait;

use weft::{InfraProvisionContext, InfraSpec, Node, NodeManifest, RunContext, ValueBag, WeftResult};

use super::agent;

#[derive(NodeManifest)]
pub struct HermesAgentNode;

// SYNC: the kind <-> catalog/agents/server/agents.json (hermes), catalog/agents/server/acp_hermes.py (AcpSpec "hermes")
pub(crate) const KIND: &str = "hermes";

#[async_trait]
impl Node for HermesAgentNode {
    async fn provision_infra(&self, ctx: InfraProvisionContext, input: ValueBag) -> WeftResult<InfraSpec> {
        agent::provision(KIND, &ctx, &input)
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        agent::run(KIND, ctx).await
    }
}
