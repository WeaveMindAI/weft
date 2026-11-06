//! DeepSeekHarness: one conversation with DeepSeek Harness. Everything it does is the agent
//! nodes' shared shape (`agent.rs`); the agent's own facts are
//! `server/agents.json` under `dsh`, and its adapter
//! `server/acp_dsh.py`.

use async_trait::async_trait;

use weft::{InfraProvisionContext, InfraSpec, Node, NodeManifest, RunContext, ValueBag, WeftResult};

use super::agent;

#[derive(NodeManifest)]
pub struct DeepSeekHarnessNode;

// SYNC: the kind <-> catalog/agents/server/agents.json (dsh), catalog/agents/server/acp_dsh.py (AcpSpec "dsh")
pub(crate) const KIND: &str = "dsh";

#[async_trait]
impl Node for DeepSeekHarnessNode {
    async fn provision_infra(&self, ctx: InfraProvisionContext, input: ValueBag) -> WeftResult<InfraSpec> {
        agent::provision(KIND, &ctx, &input)
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        agent::run(KIND, ctx).await
    }
}
