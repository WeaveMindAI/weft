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

/// Per-result overage above 10 results ($1 per 1k results), and the
/// per-page-per-content-type contents price (also $1 per 1k).
const PER_RESULT_USD: f64 = 0.001;
/// The answer route's flat request price ($5 per 1k).
const ANSWER_REQUEST_USD: f64 = 0.005;

/// How many content types (text, highlights, summary) a container of
/// content flags enables; a container with none set still bills text,
/// so it counts one.
fn enabled_content_types(container: &Value) -> f64 {
    let n = ["text", "highlights", "summary"]
        .iter()
        .filter(|k| container.get(**k).is_some_and(|v| !v.is_null() && v != false))
        .count();
    (n.max(1)) as f64
}

/// A SEARCH request's content billing: zero without a `contents`
/// block, else its enabled types.
fn requested_content_types(parsed: &Value) -> f64 {
    match parsed.get("contents") {
        None => 0.0,
        Some(c) => enabled_content_types(c),
    }
}

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

    fn prepare(&self, _path: &str, _body: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        // `costDollars` always rides the response; nothing to opt into.
        Ok(None)
    }

    async fn ceiling_usd(
        &self,
        path: &str,
        body: &[u8],
        _http: &reqwest::Client,
    ) -> anyhow::Result<f64> {
        // Estimate from what the request actually asks for: the
        // request's own search tier, the requested result count (only
        // beyond 10 bills overage), and only the content types the
        // request enables. The measured `costDollars` settles the
        // truth.
        let parsed: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
        if path == "answer" {
            return Ok(ANSWER_REQUEST_USD);
        }
        let results = parsed
            .get("numResults")
            .and_then(Value::as_f64)
            .unwrap_or(10.0)
            .clamp(1.0, 100.0);
        if path == "contents" {
            // A contents request lists urls (its flags sit top-level)
            // and bills per page per enabled type.
            let pages = parsed
                .get("urls")
                .and_then(Value::as_array)
                .map(|u| u.len() as f64)
                .unwrap_or(1.0)
                .max(1.0);
            return Ok(pages * PER_RESULT_USD * enabled_content_types(&parsed));
        }
        let request_usd = match parsed.get("type").and_then(Value::as_str) {
            Some("deep-lite") | Some("deep") => 0.012,
            Some("deep-reasoning") => 0.015,
            // instant / fast / auto / keyword: the standard rate.
            _ => 0.007,
        };
        let overage = (results - 10.0).max(0.0) * PER_RESULT_USD;
        let contents = results * PER_RESULT_USD * requested_content_types(&parsed);
        Ok(request_usd + overage + contents)
    }

    fn observe(&self, _path: &str) -> Box<dyn CallObservation> {
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
