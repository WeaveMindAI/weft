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
    fn service(&self) -> &'static str {
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

    async fn ceiling_usd(
        &self,
        _path: &str,
        _body: &[u8],
        _follow_up: FollowUp<'_>,
    ) -> anyhow::Result<f64> {
        Ok(0.01)
    }

    fn observe(&self, _path: &str, _query: &str, _request_body: &[u8]) -> Box<dyn CallObservation> {
        Box::new(NoopObservation)
    }

    async fn resolve(
        &self,
        _path: &str,
        _observed: ObservedCall,
        _follow_up: FollowUp<'_>,
    ) -> MeasuredCost {
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
            status: 200,
            data: json!({}),
        }
    }
}

weft_providers::register_meter!(PROJECT_METER);

#[test]
fn a_meter_registered_outside_the_crate_is_discovered() {
    let found =
        weft_providers::meter_for("acme_project_only").expect("project-defined meter must resolve");
    assert_eq!(found.service(), "acme_project_only");
    assert_eq!(found.base_url(), "https://api.acme.example/v1");
    // And a weft-shipped meter still resolves alongside it: the two registries
    // are one, so a project meter adds to the set, it does not replace it.
    assert!(weft_providers::meter_for("openrouter").is_some());
}

/// The SMALLEST meter that compiles, written out in full.
///
/// A meter author outside this crate writes what the trait requires and
/// nothing else, and the only place that is checked today is inside the
/// worker's docker build: a method that gains a `required` on it fails
/// minutes into a deployed end-to-end run, reading like a bug in the
/// feature rather than a change to the trait. This is that check, in an
/// ordinary `cargo test`.
///
/// Adding a method WITHOUT a default breaks this on purpose. That is the
/// question to answer when it does: does every meter author really have
/// to write this one, or does it want a default?
mod minimal {
    use async_trait::async_trait;
    use weft_providers::{
        CallObservation, FollowUp, MeasuredCost, ObservedCall, Pricing, ProviderMeter, RouteClass,
    };

    struct MinimalMeter;

    #[async_trait]
    impl ProviderMeter for MinimalMeter {
        fn service(&self) -> &'static str {
            "minimal_example"
        }
        fn base_url(&self) -> &'static str {
            "https://api.minimal.example"
        }
        fn classify(&self, method: &str, path: &str) -> RouteClass {
            match (method, path) {
                ("POST", "do") => RouteClass::Billable(Pricing::Fixed { usd: 0.01 }),
                _ => RouteClass::Unknown,
            }
        }
        fn observe(&self, _path: &str, _query: &str, _body: &[u8]) -> Box<dyn CallObservation> {
            Box::new(weft_providers::providers::JsonBodyObservation::default())
        }
        async fn resolve(
            &self,
            _path: &str,
            observed: ObservedCall,
            _follow_up: FollowUp<'_>,
        ) -> MeasuredCost {
            MeasuredCost {
                amount_usd: (200..300).contains(&observed.status).then_some(0.01),
                model: None,
                metadata: serde_json::json!({ "resolution": "flat route price" }),
            }
        }
    }

    #[test]
    fn the_minimal_meter_is_a_whole_meter() {
        let m = MinimalMeter;
        assert_eq!(m.service(), "minimal_example");
        assert!(matches!(m.classify("POST", "do"), RouteClass::Billable(_)));
        // The defaults answer for everything this meter did not write.
        assert!(m.prepare("do", b"{}").expect("the default sends the body as-is").is_none());
        assert!(m.opens_charge("do", &ObservedCall { interrupted: false, status: 200, data: serde_json::json!({}) }).is_none());
    }
}
