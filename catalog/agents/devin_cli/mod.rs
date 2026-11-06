//! DevinCli: one conversation with Devin CLI. Everything it does is the agent
//! nodes' shared shape (`agent.rs`); the agent's own facts are
//! `server/agents.json` under `devin`, and its adapter
//! `server/acp_devin.py`.

use async_trait::async_trait;

use weft::{InfraProvisionContext, InfraSpec, Node, NodeManifest, RunContext, ValueBag, WeftResult};

use super::agent;

#[derive(NodeManifest)]
pub struct DevinCliNode;

// SYNC: the kind <-> catalog/agents/server/agents.json (devin), catalog/agents/server/acp_devin.py (AcpSpec "devin")
pub(crate) const KIND: &str = "devin";

#[async_trait]
impl Node for DevinCliNode {
    async fn provision_infra(&self, ctx: InfraProvisionContext, input: ValueBag) -> WeftResult<InfraSpec> {
        agent::provision(KIND, &ctx, &input)
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        agent::run(KIND, ctx).await
    }
}
