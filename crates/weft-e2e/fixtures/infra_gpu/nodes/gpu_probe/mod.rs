//! GpuProbe: an infra node whose one unit asks for a GPU. Its container
//! answers `/outputs` with what `nvidia-smi -L` lists inside it, so a run
//! shows whether the platform really handed the GPU over.

use async_trait::async_trait;

use weft::infra::{
    Container, ContainerPort, Endpoint, EndpointTarget, Expose, Gpu, Image, InfraSpec, MachineShape, Probe, Protocol, Unit,
};
use weft::node::NodeOutput;
use weft::{ExecutionContext, InfraProvisionContext, Node, NodeManifest, ValueBag, WeftResult};

#[derive(NodeManifest)]
pub struct GpuProbeNode;

const PORT: u16 = 8080;

#[async_trait]
impl Node for GpuProbeNode {
    async fn provision_infra(&self, _ctx: InfraProvisionContext, _input: ValueBag) -> WeftResult<InfraSpec> {
        Ok(InfraSpec {
            units: vec![Unit {
                name: "probe".into(),
                containers: vec![Container::new("app", Image::Local { name: "gpu_probe".into() })
                    .with_ports(vec![ContainerPort { name: "http".into(), port: PORT, protocol: Protocol::Tcp }])
                    .with_readiness(Probe::http("/health", PORT).with_initial_delay(1))],
                // A local install hands every GPU it has and ignores the
                // kind; a cloud install makes a machine with one L4.
                machine: MachineShape { gpu: Some(Gpu { kind: "nvidia-l4".into(), count: 1 }), ..Default::default() },
                ..Default::default()
            }],
            endpoints: vec![Endpoint {
                name: "api".into(),
                target: EndpointTarget::Unit { unit: "probe".into(), container: "app".into(), port: "http".into() },
                expose: Expose::Project,
            }],
            ..Default::default()
        })
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let api = ctx.endpoint("api").await?;
        let outputs = api.call(weft::EndpointMethod::Get, "/outputs", None).await?;
        ctx.pulse_downstream(NodeOutput::new().extend_from_object(&outputs)).await
    }
}
