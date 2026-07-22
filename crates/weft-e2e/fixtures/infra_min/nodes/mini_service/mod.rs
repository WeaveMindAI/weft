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
use weft_core::node::NodeOutput;
use weft_core::{ExecutionContext, InfraProvisionContext, Node, NodeManifest, ValueBag, WeftResult};

#[derive(NodeManifest)]
pub struct MiniServiceNode;

const PORT: u16 = 8080;

#[async_trait]
impl Node for MiniServiceNode {
    async fn provision_infra(
        &self,
        _ctx: InfraProvisionContext,
        _input: ValueBag,
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

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // Resolve the endpoint, read its /outputs, and emit them plus the bare
        // URL. The `/outputs` key set (status) matches the declared output port.
        let api = ctx.endpoint("api").await?;
        // The service's readiness probe gates the pod Ready, and the
        // supervisor waits for readiness before reporting Running, but the
        // first cross-pod call can still race the Service's network path
        // becoming warm (especially on a loaded cluster: kube-proxy /
        // endpoint propagation lags pod-Ready by a beat). A single call
        // would then fail spuriously. Retry briefly until it answers or a
        // short deadline: a genuinely-up service responds within seconds,
        // so a timeout here is a real failure, not flakiness.
        let outputs = call_with_warmup_retry(&api).await?;
        let out = NodeOutput::new()
            .extend_from_object(&outputs)
            .set("endpointUrl", api.url());
        ctx.pulse_downstream(out).await
    }
}

/// GET `/outputs`, retrying on error until it succeeds or the warmup
/// deadline elapses. The last error is returned on timeout so a service
/// that is genuinely down still fails loudly (the retry only smooths the
/// brief post-Ready network-warmup window, it never hides a real outage).
async fn call_with_warmup_retry(
    api: &weft_core::EndpointHandle,
) -> WeftResult<Value> {
    use std::time::{Duration, Instant};
    const WARMUP_DEADLINE: Duration = Duration::from_secs(30);
    const RETRY_INTERVAL: Duration = Duration::from_millis(500);
    let start = Instant::now();
    loop {
        match api
            .call(weft_core::EndpointMethod::Get, "/outputs", None)
            .await
        {
            Ok(v) => return Ok(v),
            Err(e) => {
                if start.elapsed() >= WARMUP_DEADLINE {
                    return Err(e);
                }
                tokio::time::sleep(RETRY_INTERVAL).await;
            }
        }
    }
}
