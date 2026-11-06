//! OpenClaw: one conversation with OpenClaw. Everything it does is the agent
//! nodes' shared shape (`agent.rs`); the agent's own facts are
//! `server/agents.json` under `openclaw`, and its adapter
//! `server/adapter_openclaw.py`, which drives OpenClaw's own Gateway protocol.

use async_trait::async_trait;

use weft::{InfraProvisionContext, InfraSpec, Node, NodeManifest, RunContext, ValueBag, WeftResult};

use super::agent;

#[derive(NodeManifest)]
pub struct OpenClawNode;

// SYNC: the kind <-> catalog/agents/server/agents.json (openclaw), catalog/agents/server/adapter_openclaw.py (OpenClaw.kind)
pub(crate) const KIND: &str = "openclaw";

#[async_trait]
impl Node for OpenClawNode {
    async fn provision_infra(&self, ctx: InfraProvisionContext, input: ValueBag) -> WeftResult<InfraSpec> {
        agent::provision(KIND, &ctx, &input)
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        agent::run(KIND, ctx).await
    }
}
