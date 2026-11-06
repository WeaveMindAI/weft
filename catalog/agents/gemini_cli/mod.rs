//! GeminiCli: one conversation with Gemini CLI. Everything it does is the agent
//! nodes' shared shape (`agent.rs`); the agent's own facts are
//! `server/agents.json` under `gemini`, and its adapter
//! `server/acp_gemini.py`.

use async_trait::async_trait;

use weft::{InfraProvisionContext, InfraSpec, Node, NodeManifest, RunContext, ValueBag, WeftResult};

use super::agent;

#[derive(NodeManifest)]
pub struct GeminiCliNode;

// SYNC: the kind <-> catalog/agents/server/agents.json (gemini), catalog/agents/server/acp_gemini.py (AcpSpec "gemini")
pub(crate) const KIND: &str = "gemini";

#[async_trait]
impl Node for GeminiCliNode {
    async fn provision_infra(&self, ctx: InfraProvisionContext, input: ValueBag) -> WeftResult<InfraSpec> {
        agent::provision(KIND, &ctx, &input)
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        agent::run(KIND, ctx).await
    }
}
