//! QwenCode: one conversation with Qwen Code. Everything it does is the agent
//! nodes' shared shape (`agent.rs`); the agent's own facts are
//! `server/agents.json` under `qwen`, and its adapter
//! `server/acp_qwen.py`.

use async_trait::async_trait;

use weft::{InfraProvisionContext, InfraSpec, Node, NodeManifest, RunContext, ValueBag, WeftResult};

use super::agent;

#[derive(NodeManifest)]
pub struct QwenCodeNode;

// SYNC: the kind <-> catalog/agents/server/agents.json (qwen), catalog/agents/server/acp_qwen.py (AcpSpec "qwen")
pub(crate) const KIND: &str = "qwen";

#[async_trait]
impl Node for QwenCodeNode {
    async fn provision_infra(&self, ctx: InfraProvisionContext, input: ValueBag) -> WeftResult<InfraSpec> {
        agent::provision(KIND, &ctx, &input)
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        agent::run(KIND, ctx).await
    }
}
