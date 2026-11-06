//! KiloCode: one conversation with Kilo Code. Everything it does is the agent
//! nodes' shared shape (`agent.rs`); the agent's own facts are
//! `server/agents.json` under `kilo`, and its adapter
//! `server/adapter_kilo.py`.

use async_trait::async_trait;

use weft::{InfraProvisionContext, InfraSpec, Node, NodeManifest, RunContext, ValueBag, WeftResult};

use super::agent;

#[derive(NodeManifest)]
pub struct KiloCodeNode;

// SYNC: the kind <-> catalog/agents/server/agents.json (kilo), catalog/agents/server/adapter_kilo.py (Kilo.kind)
pub(crate) const KIND: &str = "kilo";

#[async_trait]
impl Node for KiloCodeNode {
    async fn provision_infra(&self, ctx: InfraProvisionContext, input: ValueBag) -> WeftResult<InfraSpec> {
        agent::provision(KIND, &ctx, &input)
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        agent::run(KIND, ctx).await
    }
}
