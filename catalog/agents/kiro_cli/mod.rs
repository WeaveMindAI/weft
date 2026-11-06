//! KiroCli: one conversation with Kiro CLI. Everything it does is the agent
//! nodes' shared shape (`agent.rs`); the agent's own facts are
//! `server/agents.json` under `kiro`, and its adapter
//! `server/acp_kiro.py`.

use async_trait::async_trait;

use weft::{InfraProvisionContext, InfraSpec, Node, NodeManifest, RunContext, ValueBag, WeftResult};

use super::agent;

#[derive(NodeManifest)]
pub struct KiroCliNode;

// SYNC: the kind <-> catalog/agents/server/agents.json (kiro), catalog/agents/server/acp_kiro.py (AcpSpec "kiro")
pub(crate) const KIND: &str = "kiro";

#[async_trait]
impl Node for KiroCliNode {
    async fn provision_infra(&self, ctx: InfraProvisionContext, input: ValueBag) -> WeftResult<InfraSpec> {
        agent::provision(KIND, &ctx, &input)
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        agent::run(KIND, ctx).await
    }
}
