//! BaileyBridge: infra node.
//!
//! - `provision_infra` returns an `InfraSpec` declaring one unit (the
//!   Baileys bridge container) with a small disk for its WhatsApp
//!   session, and the `api` endpoint the node calls. The supervisor
//!   runs it and writes the `infra_node` row, then `run` forwards the
//!   bridge's `/outputs` to the node's pulse output ports.
//! - On later invocations (trigger setup, a normal firing) provisioning
//!   is skipped (infra is already up) and `run` queries
//!   `endpoint_url("api")` and forwards `/outputs` as before.

use async_trait::async_trait;

use weft::infra::{
    Container, ContainerPort, Endpoint, EndpointTarget, EnvEntry, Expose, Image, InfraSpec, Limits, Mount, Probe,
    Protocol, Unit, Volume, VolumeKind,
};
use weft::{ExecutionContext, InfraProvisionContext, Node, NodeManifest, ValueBag, WeftResult};

#[derive(NodeManifest)]
pub struct BaileyBridgeNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for BaileyBridgeNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn provision_infra(
        &self,
        _ctx: InfraProvisionContext,
        _input: ValueBag,
    ) -> WeftResult<InfraSpec> {
        // No programmatic inputs; the bridge is parameterless.
        // One unit, one copy: WhatsApp's session cannot tolerate two
        // bridges at once, and a unit never runs beside its own next
        // version.
        const BRIDGE_PORT: u16 = 8090;
        Ok(InfraSpec {
            units: vec![Unit {
                name: "bridge".into(),
                containers: vec![Container::new("whatsapp", Image::Local { name: "bridge".into() })
                    .with_env(vec![
                        EnvEntry::new("PORT", BRIDGE_PORT.to_string()),
                        EnvEntry::new("AUTH_DIR", "/data/auth"),
                    ])
                    .with_ports(vec![ContainerPort { name: "http".into(), port: BRIDGE_PORT, protocol: Protocol::Tcp }])
                    .with_limits(Limits { cpu: Some("0.5".into()), memory: Some("512Mi".into()) })
                    .with_mounts(vec![Mount::new("auth", "/data/auth")])
                    .with_readiness(Probe::http("/health", BRIDGE_PORT).with_initial_delay(5))],
                ..Default::default()
            }],
            volumes: vec![Volume { name: "auth".into(), kind: VolumeKind::Disk { size: "100Mi".into(), class: None } }],
            endpoints: vec![Endpoint {
                name: "api".into(),
                target: EndpointTarget::Unit { unit: "bridge".into(), container: "whatsapp".into(), port: "http".into() },
                expose: Expose::Project,
            }],
            keep_on_terminate: Vec::new(),
        })
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // Bridge behaves the same on every invocation: resolve the
        // endpoint and emit it. Downstream nodes (trigger setup,
        // fire-time data reads) all need the URL.
        //
        // One broker round-trip resolves the endpoint; the handle
        // caches the URL so `.url()` and `.call(...)` don't repeat
        // the lookup. Output ports: `endpointUrl` (the bare URL, so
        // downstream nodes like BaileySend can target the bridge
        // from outside the declared-endpoint graph) plus the bridge's
        // `/outputs` keys (status, phoneNumber, jid, pushName). The
        // fan takes the declared ports and nothing else, so a key the
        // container grows does not have to be added here first, and it
        // skips the nulls an unpaired bridge reports (no phone number
        // yet), which leaves those ports closed rather than mismatched.
        let api = ctx.endpoint("api").await?;
        let bridge_outputs = api
            .call(weft::EndpointMethod::Get, "/outputs", None)
            .await?;
        // `endpointUrl` is our locally-known truth (the resolved
        // EndpointHandle URL). Set AFTER the fan (set-after-fan wins) so a
        // misbehaving container can't shadow it with its own value.
        let out = ctx.fan_declared(&bridge_outputs).set("endpointUrl", api.url());
        ctx.pulse_downstream(out).await
    }
}
