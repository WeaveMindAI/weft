//! MintInstanceToken: a token that acts inside one instance of this
//! program, for a browser or extension. It always expires; the value
//! comes out once.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{node_bail, ExecutionContext, Node, NodeManifest, WeftResult};

use super::lifecycle::instance;

#[derive(NodeManifest)]
pub struct MintInstanceTokenNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for MintInstanceTokenNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let hours: f64 = ctx.inputs.get("expiresInHours")?;
        // The token's life is counted in whole seconds, so the hours
        // round to the nearest one (1.1 hours is 3960.0000000000005
        // seconds in floating point, and means 3960). The bounds (at
        // least a second, at most a year) are the core's, checked at the
        // mint, so anything rounding to 0 is refused there.
        let secs = (hours * 3600.0).round();
        if !(secs.is_finite() && secs >= 0.0) {
            node_bail!("expiresInHours must be a finite, non-negative number of hours, got {hours}");
        }
        let expires_in = std::time::Duration::from_secs(secs as u64);
        let minted = ctx.tokens().mint_for_instance(instance(&ctx)?, expires_in).await?;
        ctx.pulse_downstream(
            NodeOutput::new().set("token", minted.token).set("expiresAt", minted.expires_at_unix),
        )
        .await
    }
}
