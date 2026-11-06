//! Codex: one conversation with Codex. Everything it does is
//! the agent nodes' shared shape (`agent.rs`); the agent's own facts are
//! `server/agents.json` under `codex`, and its adapter
//! `server/adapter_codex.py`.

use async_trait::async_trait;

use weft::{InfraProvisionContext, InfraSpec, Node, NodeManifest, RunContext, ValueBag, WeftResult};

use super::agent;

#[derive(NodeManifest)]
pub struct CodexNode;

// SYNC: the kind <-> catalog/agents/server/agents.json (codex), catalog/agents/server/adapter_codex.py (Codex.kind)
pub(crate) const KIND: &str = "codex";

#[async_trait]
impl Node for CodexNode {
    async fn provision_infra(&self, ctx: InfraProvisionContext, input: ValueBag) -> WeftResult<InfraSpec> {
        agent::provision(KIND, &ctx, &input)
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        agent::run(KIND, ctx).await
    }
}
