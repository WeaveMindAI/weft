//! MiniService: a minimal infra node. Provisions a single-container HTTP
//! sidecar (the smallest possible backing service) and, at fire time, resolves
//! its endpoint and emits the sidecar's `/outputs` plus the resolved URL.
//!
//! Exists so the e2e rig can drive the full infra lifecycle (provision ->
//! running -> read outputs -> terminate) end to end against a real pod, with no
//! domain weight (no PVC, no external service, just a tiny HTTP server).

use async_trait::async_trait;
use serde_json::Value;

use weft_core::infra::{
    Container, ContainerPort, Endpoint, Expose, Image, InfraSpec, Probe, Protocol, Resources, Unit,
    UnitKind,
};
use weft_core::node::{NodeMetadata, NodeOutput};
use weft_core::{ExecutionContext, InfraProvisionContext, InputBag, Node, WeftResult};

pub struct MiniServiceNode;

const METADATA_JSON: &str = include_str!("metadata.json");
const PORT: u16 = 8080;

#[async_trait]
impl Node for MiniServiceNode {
    fn node_type(&self) -> &'static str {
        "MiniService"
    }

    fn metadata(&self) -> NodeMetadata {
        serde_json::from_str(METADATA_JSON).expect("MiniService metadata.json must be valid")
    }

    async fn provision(
        &self,
        _ctx: InfraProvisionContext,
        _input: InputBag,
    ) -> WeftResult<InfraSpec> {
        Ok(InfraSpec {
            units: vec![Unit {
                name: "svc".into(),
                kind: UnitKind::Deployment,
                containers: vec![Container::new("app", Image::Local {
                    name: "mini_service".into(),
                })
                .with_ports(vec![ContainerPort {
                    name: "http".into(),
                    port: PORT,
                    protocol: Protocol::Tcp,
                }])
                .with_resources(Resources {
                    cpu_request: Some("50m".into()),
                    memory_request: Some("32Mi".into()),
                    cpu_limit: Some("250m".into()),
                    memory_limit: Some("128Mi".into()),
                    ..Default::default()
                })
                .with_readiness(Probe::http("/health", PORT).with_initial_delay(2))],
                ..Default::default()
            }],
            endpoints: vec![Endpoint {
                name: "api".into(),
                unit: "svc".into(),
                container: "app".into(),
                port: "http".into(),
                expose: Expose::ClusterInternal,
            }],
            ..Default::default()
        })
    }

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // Resolve the endpoint, read its /outputs, and emit them plus the bare
        // URL. The `/outputs` key set (status) matches the declared output port.
        let api = ctx.endpoint("api").await?;
        let outputs = api
            .call(weft_core::EndpointMethod::Get, "/outputs", None)
            .await?;
        let out = NodeOutput::empty()
            .extend_from_object(&outputs, &["endpointUrl"])
            .set("endpointUrl", Value::String(api.url().to_string()));
        ctx.pulse_downstream(out).await
    }
}
