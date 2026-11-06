//! ClineCli: one conversation with Cline CLI. Everything it does is the agent
//! nodes' shared shape (`agent.rs`); the agent's own facts are
//! `server/agents.json` under `cline`, and its adapter
//! `server/adapter_cline.py` (Cline's SDK, through `server/cline_bridge.mjs`).

use async_trait::async_trait;

use weft::{InfraProvisionContext, InfraSpec, Node, NodeManifest, RunContext, ValueBag, WeftResult};

use super::agent;

#[derive(NodeManifest)]
pub struct ClineCliNode;

// SYNC: the kind <-> catalog/agents/server/agents.json (cline), catalog/agents/server/adapter_cline.py (Cline.kind)
pub(crate) const KIND: &str = "cline";

#[async_trait]
impl Node for ClineCliNode {
    async fn provision_infra(&self, ctx: InfraProvisionContext, input: ValueBag) -> WeftResult<InfraSpec> {
        agent::provision(KIND, &ctx, &input)
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        agent::run(KIND, ctx).await
    }
}
