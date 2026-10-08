//! The Exa meter (web search).
//!
//! Routes (relative to `https://api.exa.ai`):
//! - `POST search`, `POST contents`, `POST answer` are BILLABLE,
//!   metered: every response reports its own `costDollars.total`
//!   (exa.ai/docs, checked 2026-08).
//!
//! Everything else is Unknown.

use serde_json::{json, Value};

use crate::{
    CallObservation, FollowUp, MeasuredCost, ObservedCall, Pricing, ProviderMeter, RouteClass,
};

pub struct ExaMeter;

pub static EXA: ExaMeter = ExaMeter;

crate::register_meter!(EXA);

#[async_trait::async_trait]
impl ProviderMeter for ExaMeter {
    fn service(&self) -> &'static str {
        "exa"
    }

    fn base_url(&self) -> &'static str {
        "https://api.exa.ai"
    }

    fn classify(&self, method: &str, path: &str) -> RouteClass {
        match (method, path) {
            ("POST", "search") | ("POST", "contents") | ("POST", "answer") => {
                RouteClass::Billable(Pricing::Metered)
            }
            _ => RouteClass::Unknown,
        }
    }

    fn observe(&self, _path: &str, _query: &str, _request_body: &[u8]) -> Box<dyn CallObservation> {
        Box::new(super::JsonBodyObservation::default())
    }

    // Every billable route reports the same `costDollars` figure, so
    // one resolve serves search, contents, and answer alike.
    async fn resolve(
        &self,
        _path: &str,
        observed: ObservedCall,
        _follow_up: FollowUp<'_>,
    ) -> MeasuredCost {
        let cost = observed.data.pointer("/costDollars/total").and_then(Value::as_f64);
        MeasuredCost {
            amount_usd: cost,
            model: None,
            metadata: json!({
                "costDollars": observed.data.get("costDollars"),
                "interrupted": observed.interrupted,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Route classification is a billing boundary: the three search
    /// verbs bill metered, everything else is Unknown.
    #[test]
    fn route_classification_is_exact() {
        let m = &EXA;
        for route in ["search", "contents", "answer"] {
            assert!(
                matches!(m.classify("POST", route), RouteClass::Billable(Pricing::Metered)),
                "POST {route} bills metered"
            );
        }
        for trick in [
            ("GET", "search"),
            ("POST", "search/x"),
            ("POST", "findSimilar"),
            ("DELETE", "contents"),
        ] {
            assert!(
                matches!(m.classify(trick.0, trick.1), RouteClass::Unknown),
                "{trick:?} must classify Unknown"
            );
        }
    }
}
