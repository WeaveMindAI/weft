//! A meter defined OUTSIDE the weft-providers crate (as a project package
//! does) is discovered by `meter_for` all the same. An integration test is a
//! separate crate linking `weft-providers`, so registering a meter here is
//! exactly the shape a user's project package produces: a `.rs` file that
//! calls `weft_providers::register_meter!`. If this passes, a project can
//! define its own provider and the worker will find its meter with no change
//! to weft.

use async_trait::async_trait;
use serde_json::json;
use weft_providers::{
    CallObservation, FollowUp, MeasuredCost, ObservedCall, Pricing, ProviderMeter, RouteClass,
};

struct ProjectMeter;

static PROJECT_METER: ProjectMeter = ProjectMeter;

#[async_trait]
impl ProviderMeter for ProjectMeter {
    fn provider(&self) -> &'static str {
        "acme_project_only"
    }

    fn base_url(&self) -> &'static str {
        "https://api.acme.example/v1"
    }

    fn classify(&self, method: &str, path: &str) -> RouteClass {
        match (method, path) {
            ("POST", "do") => RouteClass::Billable(Pricing::Fixed { usd: 0.01 }),
            _ => RouteClass::Unknown,
        }
    }

    fn prepare(&self, _path: &str, _body: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(None)
    }

    async fn ceiling_usd(
        &self,
        _path: &str,
        _body: &[u8],
        _http: &reqwest::Client,
    ) -> anyhow::Result<f64> {
        Ok(0.01)
    }

    fn observe(&self) -> Box<dyn CallObservation> {
        Box::new(NoopObservation)
    }

    async fn resolve(&self, _observed: ObservedCall, _follow_up: FollowUp<'_>) -> MeasuredCost {
        MeasuredCost {
            amount_usd: Some(0.01),
            model: None,
            metadata: json!({}),
        }
    }
}

struct NoopObservation;

impl CallObservation for NoopObservation {
    fn on_status(&mut self, _status: u16) {}
    fn on_chunk(&mut self, _bytes: &[u8]) {}
    fn end(self: Box<Self>, interrupted: bool) -> ObservedCall {
        ObservedCall {
            interrupted,
            data: json!({}),
        }
    }
}

weft_providers::register_meter!(PROJECT_METER);

#[test]
fn a_meter_registered_outside_the_crate_is_discovered() {
    let found =
        weft_providers::meter_for("acme_project_only").expect("project-defined meter must resolve");
    assert_eq!(found.provider(), "acme_project_only");
    assert_eq!(found.base_url(), "https://api.acme.example/v1");
    // And a weft-shipped meter still resolves alongside it: the two registries
    // are one, so a project meter adds to the set, it does not replace it.
    assert!(weft_providers::meter_for("openrouter").is_some());
}
