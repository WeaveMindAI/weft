//! GrokBuild: one conversation with Grok Build. Everything it does is the agent
//! nodes' shared shape (`agent.rs`); the agent's own facts are
//! `server/agents.json` under `grok`, and its adapter
//! `server/acp_grok.py`.

use async_trait::async_trait;

use weft::{InfraProvisionContext, InfraSpec, Node, NodeManifest, RunContext, ValueBag, WeftResult};

use super::agent;

#[derive(NodeManifest)]
pub struct GrokBuildNode;

// SYNC: the kind <-> catalog/agents/server/agents.json (grok), catalog/agents/server/acp_grok.py (AcpSpec "grok")
pub(crate) const KIND: &str = "grok";

#[async_trait]
impl Node for GrokBuildNode {
    async fn provision_infra(&self, ctx: InfraProvisionContext, input: ValueBag) -> WeftResult<InfraSpec> {
        agent::provision(KIND, &ctx, &input)
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        agent::run(KIND, ctx).await
    }
}
