//! DisplayService: an infra node with a display. Provisions a single-container
//! sidecar whose `/live` panel counts presses of its one button, so the e2e rig
//! can read and press a display through the signal-token doors, the way an app
//! built on a weft program does.

use async_trait::async_trait;

use weft::infra::{
    Container, ContainerPort, Endpoint, EndpointTarget, Expose, Image, InfraSpec, Limits, Probe, Protocol, Unit,
};
use weft::node::NodeOutput;
use weft::{ExecutionContext, InfraProvisionContext, Node, NodeManifest, ValueBag, WeftResult};

#[derive(NodeManifest)]
pub struct DisplayServiceNode;

const PORT: u16 = 8080;

#[async_trait]
impl Node for DisplayServiceNode {
    async fn provision_infra(
        &self,
        _ctx: InfraProvisionContext,
        _input: ValueBag,
    ) -> WeftResult<InfraSpec> {
        Ok(InfraSpec {
            units: vec![Unit {
                name: "svc".into(),
                containers: vec![Container::new("app", Image::Local {
                    name: "display_service".into(),
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
            // `api` is the endpoint metadata.json names as the one serving
            // `/live`; the display doors reach it at its install address.
            endpoints: vec![Endpoint {
                name: "api".into(),
                target: EndpointTarget::Unit { unit: "svc".into(), container: "app".into(), port: "http".into() },
                expose: Expose::Project,
            }],
            ..Default::default()
        })
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        ctx.pulse_downstream(NodeOutput::new().set("status", "ready")).await
    }
}
