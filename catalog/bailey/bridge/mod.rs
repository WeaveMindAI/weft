//! BaileyBridge: infra node.
//!
//! - `provision_infra` returns an `InfraSpec` declaring a Deployment +
//!   Service + PVC for the Baileys bridge. The dispatcher's apply task
//!   compiles + applies the manifests and writes the `infra_node` row,
//!   then `run` forwards the bridge's `/outputs` to the node's pulse
//!   output ports.
//! - On later invocations (trigger setup, a normal firing) provisioning
//!   is skipped (infra is already up) and `run` queries
//!   `endpoint_url("api")` and forwards `/outputs` as before.

use async_trait::async_trait;

use weft::infra::{
    AccessMode, Container, ContainerPort, Endpoint, EnvEntry, Expose, Image, InfraSpec, Lifecycle,
    Mount, Probe, Protocol, Resources, TerminateBehavior, Unit, UnitKind, UpgradeBehavior, Volume,
    VolumeKind,
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
        Ok(InfraSpec {
            units: vec![Unit {
                name: "bridge".into(),
                kind: UnitKind::Deployment,
                // WhatsApp's session can't tolerate two pod replicas
                // simultaneously, so use Recreate for upgrades. The
                // strategy is per-Unit; this node has only one Unit,
                // so all upgrades use it.
                on_upgrade: UpgradeBehavior::Recreate,
                containers: vec![{
                    // Bridge port. One source of truth : the env
                    // var, the ContainerPort, and the readiness
                    // probe all derive from this constant.
                    const BRIDGE_PORT: u16 = 8090;
                    Container::new("whatsapp", Image::Local { name: "bridge".into() })
                        .with_env(vec![
                            EnvEntry::Literal {
                                name: "PORT".into(),
                                value: BRIDGE_PORT.to_string(),
                            },
                            EnvEntry::Literal {
                                name: "AUTH_DIR".into(),
                                value: "/data/auth".into(),
                            },
                        ])
                        .with_ports(vec![ContainerPort {
                            name: "http".into(),
                            port: BRIDGE_PORT,
                            protocol: Protocol::Tcp,
                        }])
                        .with_resources(Resources {
                            cpu_request: Some("100m".into()),
                            memory_request: Some("128Mi".into()),
                            cpu_limit: Some("500m".into()),
                            memory_limit: Some("512Mi".into()),
                            ..Default::default()
                        })
                        .with_mounts(vec![Mount {
                            volume: "auth".into(),
                            path: "/data/auth".into(),
                            ..Default::default()
                        }])
                        .with_readiness(
                            Probe::http("/health", BRIDGE_PORT).with_initial_delay(5),
                        )
                }],
                ..Default::default()
            }],
            volumes: vec![Volume {
                name: "auth".into(),
                kind: VolumeKind::Persistent {
                    size: "100Mi".into(),
                    storage_class: None,
                    access_modes: vec![AccessMode::ReadWriteOnce],
                },
            }],
            endpoints: vec![Endpoint {
                name: "api".into(),
                unit: "bridge".into(),
                container: "whatsapp".into(),
                port: "http".into(),
                expose: Expose::ClusterInternal,
            }],
            lifecycle: Lifecycle {
                on_terminate: TerminateBehavior {
                    preserve_pvcs: Vec::new(),
                },
                ..Default::default()
            },
            ..Default::default()
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
