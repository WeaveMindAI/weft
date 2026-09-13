//! A paid call with a QUEUE shape, and the meter that prices it, both
//! defined in this project.
//!
//! The provider commits the money when a job is SUBMITTED and states the
//! amount only on a later read. Between those two points the spend is
//! real and its figure does not exist yet, which is the window the
//! worker's open-charge machinery exists for. Everything here is
//! declarative, the way a meter is meant to be: the meter says which
//! route spends, which response reports on an earlier spend, and how to
//! read the figure off it. It never polls; the worker watches the
//! node's own read.
//!
//! `readBack` is the lever the rig pulls. Left off, the job is submitted
//! and never read, so nothing will ever state what it cost and the only
//! honest record is a spend with no figure on it.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};
use weft_providers::providers::JsonBodyObservation;
use weft_providers::{
    CallObservation, FollowUp, MeasuredCost, ObservedCall, Pricing, ProviderMeter, RouteClass,
};

/// The provider's declared API. The call is aimed here and the
/// connection's stored base re-aims it at whatever host is really
/// serving; the meter classifies the declared form, which is what keeps
/// a rig-owned fake and a real provider the same shape.
const API: &str = "https://queue.e2e.invalid";

/// What one billed unit costs. A queue provider states units, not
/// dollars, so the meter turns one into the other.
const USD_PER_UNIT: f64 = 0.01;

/// The header a read states the billed count in.
// SYNC: UNITS_HEADER <-> crates/weft-e2e/src/fakes.rs QUEUE_UNITS_HEADER
// (the fake queue answers with this header; the two must agree or the
// meter observes nothing)
const UNITS_HEADER: &str = "x-queue-units";

#[derive(NodeManifest)]
pub struct QueueJobNode;

#[async_trait]
impl Node for QueueJobNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let read_back: bool = ctx.inputs.get_or("readBack", false)?;
        let account = ctx.inputs.get("connection")?;
        let conn = ctx.open(&account).await?;

        // Submit: this is where the money goes out.
        let submitted: serde_json::Value = conn
            .client()
            .post(format!("{API}/submit"))
            .json(&serde_json::json!({ "work": "something" }))
            .send()
            .await
            .node_err("queue_fake")?
            .json()
            .await
            .node_err("queue_fake")?;
        let request_id = submitted
            .get("request_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string();

        // Read the answer back, or deliberately do not. Walking away is
        // an ordinary thing for a node to do (a cancel, a branch that
        // stops caring), and the spend is just as real either way.
        if read_back {
            let _: serde_json::Value = conn
                .client()
                .get(format!("{API}/result/{request_id}"))
                .send()
                .await
                .node_err("queue_fake")?
                .json()
                .await
                .node_err("queue_fake")?;
        }

        ctx.pulse_downstream(NodeOutput::new().set("requestId", request_id)).await
    }
}

/// This project's own meter for the queue provider.
struct QueueMeter;

static QUEUE_FAKE: QueueMeter = QueueMeter;

#[async_trait]
impl ProviderMeter for QueueMeter {
    fn service(&self) -> &'static str {
        "queue_fake"
    }

    fn base_url(&self) -> &'static str {
        API
    }

    fn classify(&self, method: &str, path: &str) -> RouteClass {
        match (method, path) {
            // The submit commits the money. What it comes to is not
            // knowable here, so it prices as metered and the figure
            // arrives later.
            ("POST", "submit") => RouteClass::Billable(Pricing::Metered),
            // A read costs nothing and reports on the submit's spend.
            ("GET", p) if p.starts_with("result/") => RouteClass::Reports,
            _ => RouteClass::Unknown,
        }
    }

    fn observe(&self, _path: &str, _query: &str, _request_body: &[u8]) -> Box<dyn CallObservation> {
        // Declared rather than hand-rolled: the shared observation
        // buffers under a cap, which a hand-written tap keeps forgetting.
        Box::new(
            JsonBodyObservation::new()
                .header_f64(UNITS_HEADER, "units")
                .field("requestId", "/request_id")
                .field("jobStatus", "/status")
                .note_outcome(),
        )
    }

    /// The submit's own answer never states a cost, so it opens a charge
    /// instead: the id the later read will report under.
    fn opens_charge(&self, path: &str, observed: &ObservedCall) -> Option<String> {
        if path != "submit" || !observed.data["accepted"].as_bool().unwrap_or(false) {
            return None;
        }
        observed.data["requestId"].as_str().map(str::to_string)
    }

    /// Which charge a read speaks for: the id in its own path.
    fn charge_reported_on(&self, path: &str, _observed: &ObservedCall) -> Option<String> {
        path.strip_prefix("result/").filter(|id| !id.is_empty()).map(str::to_string)
    }

    /// A read that states the count closes the charge; one that says the
    /// job is still running leaves it open for the read that does.
    async fn fold_report(
        &self,
        _path: &str,
        observed: ObservedCall,
        scratch: &mut serde_json::Value,
        _follow_up: FollowUp<'_>,
    ) -> Option<MeasuredCost> {
        let units = observed.data["units"].as_f64()?;
        let mut metadata = scratch.clone();
        metadata["resolution"] = serde_json::json!("the queue stated the billed units");
        metadata["units"] = serde_json::json!(units);
        Some(MeasuredCost {
            amount_usd: Some(units * USD_PER_UNIT),
            model: None,
            metadata,
        })
    }

    async fn resolve(
        &self,
        path: &str,
        observed: ObservedCall,
        _follow_up: FollowUp<'_>,
    ) -> MeasuredCost {
        // Reached only when the submit opened no charge: the provider
        // refused it, or accepted it without an id. A refusal spends
        // nothing and that is a known zero; an acceptance nobody can
        // look up is a genuine unknown.
        if let Some(cost) = weft_providers::providers::cost_from_status(observed.status, path) {
            return cost;
        }
        MeasuredCost {
            amount_usd: None,
            model: None,
            metadata: serde_json::json!({
                "resolution": format!("'{path}' was accepted without a job id, so nothing can report what it spent"),
                "status": observed.status,
            }),
        }
    }
}

weft_providers::register_meter!(QUEUE_FAKE);
