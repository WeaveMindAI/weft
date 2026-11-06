//! MistralVibe: one conversation with Mistral Vibe. Everything it does is the agent
//! nodes' shared shape (`agent.rs`); the agent's own facts are
//! `server/agents.json` under `vibe`, and its adapter
//! `server/adapter_vibe.py`.

use async_trait::async_trait;

use weft::{InfraProvisionContext, InfraSpec, Node, NodeManifest, RunContext, ValueBag, WeftResult};

use super::agent;

#[derive(NodeManifest)]
pub struct MistralVibeNode;

// SYNC: the kind <-> catalog/agents/server/agents.json (vibe), catalog/agents/server/adapter_vibe.py (Vibe.kind)
pub(crate) const KIND: &str = "vibe";

#[async_trait]
impl Node for MistralVibeNode {
    async fn provision_infra(&self, ctx: InfraProvisionContext, input: ValueBag) -> WeftResult<InfraSpec> {
        agent::provision(KIND, &ctx, &input)
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        agent::run(KIND, ctx).await
    }
}
