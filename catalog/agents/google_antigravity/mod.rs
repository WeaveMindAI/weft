//! GoogleAntigravity: one conversation with Google Antigravity. Everything it does is the agent
//! nodes' shared shape (`agent.rs`); the agent's own facts are
//! `server/agents.json` under `antigravity`, and its adapter
//! `server/adapter_antigravity.py`.

use async_trait::async_trait;

use weft::{InfraProvisionContext, InfraSpec, Node, NodeManifest, RunContext, ValueBag, WeftResult};

use super::agent;

#[derive(NodeManifest)]
pub struct GoogleAntigravityNode;

// SYNC: the kind <-> catalog/agents/server/agents.json (antigravity), catalog/agents/server/adapter_antigravity.py (Antigravity.kind)
pub(crate) const KIND: &str = "antigravity";

#[async_trait]
impl Node for GoogleAntigravityNode {
    async fn provision_infra(&self, ctx: InfraProvisionContext, input: ValueBag) -> WeftResult<InfraSpec> {
        agent::provision(KIND, &ctx, &input)
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        agent::run(KIND, ctx).await
    }
}
