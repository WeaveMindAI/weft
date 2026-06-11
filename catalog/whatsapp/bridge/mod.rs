//! WhatsAppBridge: infra node.
//!
//! - `Phase::InfraSetup`: `Node::provision` returns an `InfraSpec`
//!   declaring a Deployment + Service + PVC for the Baileys bridge.
//!   The dispatcher's apply task compiles + applies the manifests
//!   and writes the `infra_node` row. Then `execute` runs in
//!   InfraSetup phase to forward the bridge's `/outputs` to the
//!   node's pulse output ports.
//! - `Phase::TriggerSetup` / `Phase::Fire`: `execute` queries
//!   `endpoint_url("api")` and forwards `/outputs` as before. Provision
//!   is skipped (infra is already up).

use async_trait::async_trait;
use serde_json::Value;

use weft_core::infra::{
    AccessMode, Container, ContainerPort, Endpoint, EnvEntry, Expose, Image, InfraSpec, Lifecycle,
    Mount, Probe, Protocol, Resources, TerminateBehavior, Unit, UnitKind, UpgradeBehavior, Volume,
    VolumeKind,
};
use weft_core::node::{NodeMetadata, NodeOutput};
use weft_core::{ExecutionContext, InfraProvisionContext, InputBag, Node, WeftResult};

pub struct WhatsAppBridgeNode;

const METADATA_JSON: &str = include_str!("metadata.json");

#[async_trait]
impl Node for WhatsAppBridgeNode {
    fn node_type(&self) -> &'static str {
        "WhatsAppBridge"
    }

    fn metadata(&self) -> NodeMetadata {
        serde_json::from_str(METADATA_JSON).expect("WhatsAppBridge metadata.json must be valid")
    }

    async fn provision(
        &self,
        _ctx: InfraProvisionContext,
        _input: InputBag,
    ) -> WeftResult<InfraSpec> {
        // No programmatic inputs today; the bridge is parameterless.
        // Future: a `device_label` input could be threaded into env.
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

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // Bridge behaves the same in every phase: resolve the endpoint
        // and emit it. Downstream nodes (trigger setup, fire-time data
        // reads) all need the URL.
        let out = query_outputs(&ctx).await?;
        ctx.pulse_downstream(out).await
    }
}

async fn query_outputs(ctx: &ExecutionContext) -> WeftResult<NodeOutput> {
    // One broker round-trip resolves the endpoint; the handle
    // caches the URL so `.url()` and `.call(...)` don't repeat
    // the lookup. Output ports: `endpointUrl` (the bare URL, so
    // downstream nodes like WhatsAppSend can target the bridge
    // from outside the declared-endpoint graph) plus the bridge's
    // `/outputs` keys (status, phoneNumber, jid, pushName), each
    // a declared port in metadata.json. The `/outputs` key set
    // and the declared output ports must stay in sync.
    let api = ctx.endpoint("api").await?;
    let bridge_outputs = api
        .call(weft_core::EndpointMethod::Get, "/outputs", None)
        .await?;
    // `endpointUrl` is our locally-known truth (the resolved
    // EndpointHandle URL). Exclude it from the bridge response merge
    // so a misbehaving container can't shadow it with its own value.
    Ok(NodeOutput::empty()
        .extend_from_object(&bridge_outputs, &["endpointUrl"])
        .set("endpointUrl", Value::String(api.url().to_string())))
}
