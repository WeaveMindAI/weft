//! CopilotCli: one conversation with GitHub Copilot CLI. Everything it does is the agent
//! nodes' shared shape (`agent.rs`); the agent's own facts are
//! `server/agents.json` under `copilot`, and its adapter
//! `server/adapter_copilot.py`.

use async_trait::async_trait;

use weft::{InfraProvisionContext, InfraSpec, Node, NodeManifest, RunContext, ValueBag, WeftResult};

use super::agent;

#[derive(NodeManifest)]
pub struct CopilotCliNode;

// SYNC: the kind <-> catalog/agents/server/agents.json (copilot), catalog/agents/server/adapter_copilot.py (Copilot.kind)
pub(crate) const KIND: &str = "copilot";

#[async_trait]
impl Node for CopilotCliNode {
    async fn provision_infra(&self, ctx: InfraProvisionContext, input: ValueBag) -> WeftResult<InfraSpec> {
        agent::provision(KIND, &ctx, &input)
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        agent::run(KIND, ctx).await
    }
}
