//! BaileyBridge: infra node.
//!
//! - `provision_infra` returns an `InfraSpec` declaring one unit (the
//!   Baileys bridge container) with a small disk for its WhatsApp
//!   session, and the `api` endpoint the node calls. The supervisor
//!   runs it and writes the `infra_node` row, then `run` forwards the
//!   bridge's `/outputs` to the node's pulse output ports.
//! - On later invocations (trigger setup, a normal firing) provisioning
//!   is skipped (infra is already up) and `run` resolves
//!   `ctx.endpoint("api")` and forwards `/outputs` as before.

use async_trait::async_trait;

use weft::infra::{
    Container, ContainerPort, Endpoint, EndpointTarget, EnvEntry, Expose, Image, InfraSpec, MachineShape, Mount, Probe,
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
        input: ValueBag,
    ) -> WeftResult<InfraSpec> {
        // The machine, as the node's settings size it; the cloud picks
        // its cheapest that holds it.
        let machine = MachineShape { cpu: input.opt("cpu")?, memory: input.opt("memory")?, kind: input.opt::<String>("machineType")?.filter(|t| !t.trim().is_empty()), gpu: None };
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
                    .with_mounts(vec![Mount::new("auth", "/data/auth")])
                    .with_readiness(Probe::http("/health", BRIDGE_PORT).with_initial_delay(5))],
                machine,
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
        // caches the URL so `.call(...)` doesn't repeat the lookup.
        // Output ports: `bridge` (the `Infra` handle on the `api`
        // endpoint, which every other WhatsApp node resolves to reach
        // the bridge) plus the bridge's `/outputs` keys (status,
        // phoneNumber, jid, pushName). The
        // fan takes the declared ports and nothing else, so a key the
        // container grows does not have to be added here first, and it
        // skips the nulls an unpaired bridge reports (no phone number
        // yet), which leaves those ports closed rather than mismatched.
        let api = ctx.endpoint("api").await?;
        let bridge_outputs = api
            .call(weft::EndpointMethod::Get, "/outputs", None)
            .await?;
        // `bridge` is our own truth (the endpoint's handle). Set AFTER
        // the fan (set-after-fan wins) so a misbehaving container can't
        // shadow it with its own value.
        let out = ctx.fan_declared(&bridge_outputs).set("bridge", api.infra_handle());
        ctx.pulse_downstream(out).await
    }
}
