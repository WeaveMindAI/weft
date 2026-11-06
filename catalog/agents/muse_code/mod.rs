//! MuseCode: one conversation with Muse Code. Everything it does is the agent
//! nodes' shared shape (`agent.rs`); the agent's own facts are
//! `server/agents.json` under `muse`, and its adapter
//! `server/adapter_muse.py`.

use async_trait::async_trait;

use weft::{InfraProvisionContext, InfraSpec, Node, NodeManifest, RunContext, ValueBag, WeftResult};

use super::agent;

#[derive(NodeManifest)]
pub struct MuseCodeNode;

// SYNC: the kind <-> catalog/agents/server/agents.json (muse), catalog/agents/server/adapter_muse.py (Muse.kind)
pub(crate) const KIND: &str = "muse";

#[async_trait]
impl Node for MuseCodeNode {
    async fn provision_infra(&self, ctx: InfraProvisionContext, input: ValueBag) -> WeftResult<InfraSpec> {
        agent::provision(KIND, &ctx, &input)
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        agent::run(KIND, ctx).await
    }
}
