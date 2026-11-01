//! ContainerFetch: an infra node whose container downloads a stored file
//! through the link the node hands it, so the e2e rig can prove a
//! project's own container opens a file's container link
//! (`StorageHandle::container_link`). On a local install a container's
//! `127.0.0.1` is the container itself, which is what an internal link
//! would point it at.

use async_trait::async_trait;

use weft::infra::{
    Container, ContainerPort, Endpoint, EndpointTarget, Expose, Image, InfraSpec, Limits, Probe, Protocol, Unit,
};
use weft::node::NodeOutput;
use weft::storage::{FileHandle, StorageScope};
use weft::{InfraProvisionContext, Node, NodeManifest, RunContext, ValueBag, WeftResult};

#[derive(NodeManifest)]
pub struct ContainerFetchNode;

const PORT: u16 = 8080;

#[async_trait]
impl Node for ContainerFetchNode {
    async fn provision_infra(&self, _ctx: InfraProvisionContext, _input: ValueBag) -> WeftResult<InfraSpec> {
        Ok(InfraSpec {
            units: vec![Unit {
                name: "fetcher".into(),
                containers: vec![Container::new("app", Image::Local { name: "container_fetch".into() })
                    .with_ports(vec![ContainerPort { name: "http".into(), port: PORT, protocol: Protocol::Tcp }])
                    .with_limits(Limits { cpu: Some("0.25".into()), memory: Some("128Mi".into()) })
                    .with_readiness(Probe::http("/health", PORT).with_initial_delay(2))],
                ..Default::default()
            }],
            endpoints: vec![Endpoint {
                name: "api".into(),
                target: EndpointTarget::Unit { unit: "fetcher".into(), container: "app".into(), port: "http".into() },
                expose: Expose::Project,
            }],
            ..Default::default()
        })
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let file: FileHandle = ctx.inputs.get("file")?;
        let link = ctx.storage(StorageScope::Project).container_link(&file).await?;
        let api = ctx.endpoint("api").await?;
        let answer = api.call(weft::EndpointMethod::Post, "/fetch", Some(serde_json::json!({ "url": link.url }))).await?;
        ctx.pulse_downstream(NodeOutput::new().set("size", answer["size"].clone())).await
    }
}
