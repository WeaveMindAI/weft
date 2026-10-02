//! JobService: an infra node whose run waits on a long job in its own
//! container. The job starts inside `ctx.run` (a replay must not start a
//! second one), then the run parks on a `PollEndpoint` over the job's status
//! route at the endpoint's worker address. The listener polls it, and the
//! first answer that is no longer `running` resumes the run.

use async_trait::async_trait;
use serde_json::json;

use weft::infra::{Container, ContainerPort, Endpoint, EndpointTarget, Expose, Image, InfraSpec, Limits, Probe, Protocol, Unit};
use weft::node::NodeOutput;
use weft::signal::{PollEndpoint, Predicate};
use weft::{EndpointMethod, ExecutionContext, InfraProvisionContext, Node, NodeManifest, ValueBag, WeftResult};

#[derive(NodeManifest)]
pub struct JobServiceNode;

const PORT: u16 = 8080;

#[async_trait]
impl Node for JobServiceNode {
    async fn provision_infra(&self, _ctx: InfraProvisionContext, _input: ValueBag) -> WeftResult<InfraSpec> {
        Ok(InfraSpec {
            units: vec![Unit {
                name: "svc".into(),
                containers: vec![Container::new("app", Image::Local { name: "job_service".into() })
                    .with_ports(vec![ContainerPort { name: "http".into(), port: PORT, protocol: Protocol::Tcp }])
                    .with_limits(Limits { cpu: Some("0.25".into()), memory: Some("128Mi".into()) })
                    .with_readiness(Probe::http("/health", PORT).with_initial_delay(2))],
                ..Default::default()
            }],
            endpoints: vec![Endpoint {
                name: "api".into(),
                target: EndpointTarget::Unit { unit: "svc".into(), container: "app".into(), port: "http".into() },
                expose: Expose::Project,
            }],
            ..Default::default()
        })
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let api = ctx.endpoint("api").await?;
        let job = ctx
            .run("start the job", || async { api.call(EndpointMethod::Post, "/jobs", Some(json!({}))).await })
            .await?;
        let id = job["id"].as_str().ok_or_else(|| weft::node_error(format!("the service answered no job id: {job}")))?;
        let state = ctx
            .await_signal(PollEndpoint {
                url: format!("{}/jobs/{id}", api.url()),
                interval_secs: 5,
                filters: vec![Predicate::neq("status", "running")],
                ..Default::default()
            })
            .await?;
        if state["status"] != "done" {
            return Err(weft::node_error(format!("job {id} ended {state}")));
        }
        let result = state["result"]
            .as_str()
            .ok_or_else(|| weft::node_error(format!("the finished job carries no result: {state}")))?;
        ctx.pulse_downstream(NodeOutput::new().set("result", result)).await
    }
}
