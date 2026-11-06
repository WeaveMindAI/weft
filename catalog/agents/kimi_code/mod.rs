//! KimiCode: one conversation with Kimi Code. Everything it does is the agent
//! nodes' shared shape (`agent.rs`); the agent's own facts are
//! `server/agents.json` under `kimi`, and its adapter
//! `server/adapter_kimi.py`.

use async_trait::async_trait;

use weft::{InfraProvisionContext, InfraSpec, Node, NodeManifest, RunContext, ValueBag, WeftResult};

use super::agent;

#[derive(NodeManifest)]
pub struct KimiCodeNode;

// SYNC: the kind <-> catalog/agents/server/agents.json (kimi), catalog/agents/server/adapter_kimi.py (Kimi.kind)
pub(crate) const KIND: &str = "kimi";

#[async_trait]
impl Node for KimiCodeNode {
    async fn provision_infra(&self, ctx: InfraProvisionContext, input: ValueBag) -> WeftResult<InfraSpec> {
        agent::provision(KIND, &ctx, &input)
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        agent::run(KIND, ctx).await
    }
}
