//! OpenCode: one conversation with OpenCode. Everything it does is the agent
//! nodes' shared shape (`agent.rs`); the agent's own facts are
//! `server/agents.json` under `opencode`, and its adapter
//! `server/acp_opencode.py`.

use async_trait::async_trait;

use weft::{InfraProvisionContext, InfraSpec, Node, NodeManifest, RunContext, ValueBag, WeftResult};

use super::agent;

#[derive(NodeManifest)]
pub struct OpenCodeNode;

// SYNC: the kind <-> catalog/agents/server/agents.json (opencode), catalog/agents/server/acp_opencode.py (AcpSpec "opencode")
pub(crate) const KIND: &str = "opencode";

#[async_trait]
impl Node for OpenCodeNode {
    async fn provision_infra(&self, ctx: InfraProvisionContext, input: ValueBag) -> WeftResult<InfraSpec> {
        agent::provision(KIND, &ctx, &input)
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        agent::run(KIND, ctx).await
    }
}
