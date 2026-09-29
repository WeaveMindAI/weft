//! MiniService: a minimal infra node. Provisions a single-container HTTP
//! sidecar (the smallest possible backing service) and, at fire time, resolves
//! its endpoint and emits the sidecar's `/outputs` plus the resolved URL.
//!
//! Exists so the e2e rig can drive the full infra lifecycle (provision ->
//! running -> read outputs -> terminate) end to end against a real container, with no
//! domain weight (no PVC, no external service, just a tiny HTTP server).

use async_trait::async_trait;

use weft::infra::{
    Container, ContainerPort, Endpoint, EndpointTarget, Expose, Image, InfraSpec, Limits, Probe, Protocol, Unit,
};
use weft::node::NodeOutput;
use weft::{ExecutionContext, InfraProvisionContext, Node, NodeManifest, ValueBag, WeftResult};

#[derive(NodeManifest)]
pub struct MiniServiceNode;

const PORT: u16 = 8080;

#[async_trait]
impl Node for MiniServiceNode {
    async fn provision_infra(
        &self,
        _ctx: InfraProvisionContext,
        input: ValueBag,
    ) -> WeftResult<InfraSpec> {
        let reachable: bool = input.get("reachable")?;
        let public: bool = input.get("public")?;
        Ok(InfraSpec {
            units: vec![Unit {
                name: "svc".into(),
                containers: vec![Container::new("app", Image::Local {
                    name: "mini_service".into(),
                })
                .with_ports(vec![ContainerPort {
                    name: "http".into(),
                    port: PORT,
                    protocol: Protocol::Tcp,
                }])
                .with_limits(Limits { cpu: Some("0.25".into()), memory: Some("128Mi".into()) })
                .with_readiness(Probe::http("/health", PORT).with_initial_delay(2))],
                ..Default::default()
            }],
            // A door is part of what this node IS, and its author left
            // the choice to whoever writes it into a program. Off, the
            // endpoint answers only the project's own workers; on, it
            // gets an address on the install's network, and `weft infra
            // list-doors` prints it.
            endpoints: vec![Endpoint {
                name: "api".into(),
                target: EndpointTarget::Unit { unit: "svc".into(), container: "app".into(), port: "http".into() },
                expose: if public {
                    Expose::Public { path: "/svc".into() }
                } else if reachable {
                    Expose::SameNetwork
                } else {
                    Expose::Project
                },
            }],
            ..Default::default()
        })
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // Resolve the endpoint, read its /outputs, and emit them plus the bare
        // URL. The `/outputs` key set (status) matches the declared output port.
        let api = ctx.endpoint("api").await?;
        let outputs = api.call(weft::EndpointMethod::Get, "/outputs", None).await?;
        let out = NodeOutput::new()
            .extend_from_object(&outputs)
            .set("endpointUrl", api.url());
        ctx.pulse_downstream(out).await
    }
}
