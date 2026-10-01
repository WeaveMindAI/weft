//! TickService: an infra unit streaming a `tick` Server-Sent Event every
//! second. Its run hands out the stream's address as the workers reach it,
//! so a trigger wired from it proves the listener finds the same unit
//! from where it runs.

use async_trait::async_trait;

use weft::infra::{Container, ContainerPort, Endpoint, EndpointTarget, Expose, Image, InfraSpec, Probe, Protocol, Unit};
use weft::node::NodeOutput;
use weft::{ExecutionContext, InfraProvisionContext, Node, NodeManifest, ValueBag, WeftResult};

#[derive(NodeManifest)]
pub struct TickServiceNode;

const PORT: u16 = 8080;

#[async_trait]
impl Node for TickServiceNode {
    async fn provision_infra(&self, _ctx: InfraProvisionContext, _input: ValueBag) -> WeftResult<InfraSpec> {
        Ok(InfraSpec {
            units: vec![Unit {
                name: "ticks".into(),
                containers: vec![Container::new("app", Image::Local { name: "tick_service".into() })
                    .with_ports(vec![ContainerPort { name: "http".into(), port: PORT, protocol: Protocol::Tcp }])
                    .with_readiness(Probe::http("/health", PORT).with_initial_delay(1))],
                ..Default::default()
            }],
            endpoints: vec![Endpoint {
                name: "api".into(),
                target: EndpointTarget::Unit { unit: "ticks".into(), container: "app".into(), port: "http".into() },
                expose: Expose::Project,
            }],
            ..Default::default()
        })
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let api = ctx.endpoint("api").await?;
        ctx.pulse_downstream(NodeOutput::new().set("eventsUrl", format!("{}/events", api.url()))).await
    }
}
