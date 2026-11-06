//! CodeBuddyCode: one conversation with CodeBuddy Code. Everything it does is the agent
//! nodes' shared shape (`agent.rs`); the agent's own facts are
//! `server/agents.json` under `codebuddy`, and its adapter
//! `server/acp_codebuddy.py`.

use async_trait::async_trait;

use weft::{InfraProvisionContext, InfraSpec, Node, NodeManifest, RunContext, ValueBag, WeftResult};

use super::agent;

#[derive(NodeManifest)]
pub struct CodeBuddyCodeNode;

// SYNC: the kind <-> catalog/agents/server/agents.json (codebuddy), catalog/agents/server/acp_codebuddy.py (AcpSpec "codebuddy")
pub(crate) const KIND: &str = "codebuddy";

#[async_trait]
impl Node for CodeBuddyCodeNode {
    async fn provision_infra(&self, ctx: InfraProvisionContext, input: ValueBag) -> WeftResult<InfraSpec> {
        agent::provision(KIND, &ctx, &input)
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        agent::run(KIND, ctx).await
    }
}
