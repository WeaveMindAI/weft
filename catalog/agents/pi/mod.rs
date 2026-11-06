//! Pi: one conversation with Pi. Everything it does is the agent
//! nodes' shared shape (`agent.rs`); the agent's own facts are
//! `server/agents.json` under `pi`, and its adapter
//! `server/adapter_pi.py`.

use async_trait::async_trait;

use weft::{InfraProvisionContext, InfraSpec, Node, NodeManifest, RunContext, ValueBag, WeftResult};

use super::agent;

#[derive(NodeManifest)]
pub struct PiNode;

// SYNC: the kind <-> catalog/agents/server/agents.json (pi), catalog/agents/server/adapter_pi.py (Pi.kind)
pub(crate) const KIND: &str = "pi";

#[async_trait]
impl Node for PiNode {
    async fn provision_infra(&self, ctx: InfraProvisionContext, input: ValueBag) -> WeftResult<InfraSpec> {
        agent::provision(KIND, &ctx, &input)
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        agent::run(KIND, ctx).await
    }
}
