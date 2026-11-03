//! MintInstanceToken: a token that acts inside one instance of this
//! program, for a browser or extension. It always expires; the value
//! comes out once, as a secret, so the run's record holds its marker and
//! a Reply or a Stream sends it to the browser revealed.

use async_trait::async_trait;

use weft::instance::InstanceId;
use weft::node::NodeOutput;
use weft::{RunContext, Node, NodeManifest, WeftResult};


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

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let hours: f64 = ctx.inputs.get("expiresInHours")?;
        // The token's life is counted in whole seconds, so the hours
        // round to the nearest one (1.1 hours is 3960.0000000000005
        // seconds in floating point, and means 3960). The widget keeps it
        // from 0 up; the bounds (at least a second, at most a year) are the
        // core's, checked at the mint, so anything rounding to 0 is refused
        // there.
        let expires_in = std::time::Duration::from_secs_f64((hours * 3600.0).round());
        let minted = ctx.tokens().mint_for_instance(ctx.inputs.get::<InstanceId>("instance")?, expires_in).await?;
        let token = ctx.secret(&minted.token)?;
        ctx.pulse_downstream(NodeOutput::new().set("token", token).set("expiresAt", minted.expires_at_unix)).await
    }
}
