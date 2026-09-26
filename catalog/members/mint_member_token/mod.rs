//! MintMemberToken: a token acting as one member of this program, for
//! that member's browser or extension. It always expires; the value
//! comes out once.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use weft::node_bail;

use super::lifecycle::member;

#[derive(NodeManifest)]
pub struct MintMemberTokenNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for MintMemberTokenNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let hours: f64 = ctx.inputs.get("expiresInHours")?;
        // A year at most (a token that outlives that is not one that
        // expires), and a second at least (a shorter one is dead before
        // anybody can use it). The bounds also keep the duration finite.
        const MAX_HOURS: f64 = 365.0 * 24.0;
        const MIN_HOURS: f64 = 1.0 / 3600.0;
        if !(MIN_HOURS..=MAX_HOURS).contains(&hours) {
            node_bail!(
                "expiresInHours must be from {MIN_HOURS} (one second) to {MAX_HOURS} (a year), got {hours}: \
                 a member token always expires"
            );
        }
        let expires_in = std::time::Duration::from_secs_f64(hours * 3600.0);
        let minted = ctx.tokens().mint_for_member(member(&ctx)?, expires_in).await?;
        ctx.pulse_downstream(
            NodeOutput::new().set("token", minted.token).set("expiresAt", minted.expires_at_unix),
        )
        .await
    }
}
