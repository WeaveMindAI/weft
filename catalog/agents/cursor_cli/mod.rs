//! CursorCli: one conversation with Cursor CLI. Everything it does is the agent
//! nodes' shared shape (`agent.rs`); the agent's own facts are
//! `server/agents.json` under `cursor`, and its adapter
//! `server/acp_cursor.py`.

use async_trait::async_trait;

use weft::{InfraProvisionContext, InfraSpec, Node, NodeManifest, RunContext, ValueBag, WeftResult};

use super::agent;

#[derive(NodeManifest)]
pub struct CursorCliNode;

// SYNC: the kind <-> catalog/agents/server/agents.json (cursor), catalog/agents/server/acp_cursor.py (AcpSpec "cursor")
pub(crate) const KIND: &str = "cursor";

#[async_trait]
impl Node for CursorCliNode {
    async fn provision_infra(&self, ctx: InfraProvisionContext, input: ValueBag) -> WeftResult<InfraSpec> {
        agent::provision(KIND, &ctx, &input)
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        agent::run(KIND, ctx).await
    }
}
