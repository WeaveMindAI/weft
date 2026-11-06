//! JunieCli: one conversation with Junie CLI. Everything it does is the agent
//! nodes' shared shape (`agent.rs`); the agent's own facts are
//! `server/agents.json` under `junie`, and its adapter
//! `server/acp_junie.py`.

use async_trait::async_trait;

use weft::{InfraProvisionContext, InfraSpec, Node, NodeManifest, RunContext, ValueBag, WeftResult};

use super::agent;

#[derive(NodeManifest)]
pub struct JunieCliNode;

// SYNC: the kind <-> catalog/agents/server/agents.json (junie), catalog/agents/server/acp_junie.py (AcpSpec "junie")
pub(crate) const KIND: &str = "junie";

#[async_trait]
impl Node for JunieCliNode {
    async fn provision_infra(&self, ctx: InfraProvisionContext, input: ValueBag) -> WeftResult<InfraSpec> {
        agent::provision(KIND, &ctx, &input)
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        agent::run(KIND, ctx).await
    }
}
