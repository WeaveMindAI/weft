//! Nanobot: one conversation with nanobot. Everything it does is the agent
//! nodes' shared shape (`agent.rs`); the agent's own facts are
//! `server/agents.json` under `nanobot`, and its adapter
//! `server/adapter_nanobot.py`.

use async_trait::async_trait;

use weft::{InfraProvisionContext, InfraSpec, Node, NodeManifest, RunContext, ValueBag, WeftResult};

use super::agent;

#[derive(NodeManifest)]
pub struct NanobotNode;

// SYNC: the kind <-> catalog/agents/server/agents.json (nanobot), catalog/agents/server/adapter_nanobot.py (Nanobot.kind)
pub(crate) const KIND: &str = "nanobot";

#[async_trait]
impl Node for NanobotNode {
    async fn provision_infra(&self, ctx: InfraProvisionContext, input: ValueBag) -> WeftResult<InfraSpec> {
        agent::provision(KIND, &ctx, &input)
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        agent::run(KIND, ctx).await
    }
}
