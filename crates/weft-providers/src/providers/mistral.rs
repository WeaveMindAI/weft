//! The Mistral meter (document OCR).
//!
//! Routes (relative to `https://api.mistral.ai/v1`):
//! - `POST ocr` is BILLABLE, metered: the response reports
//!   `usage_info.pages_processed` and OCR prices per page.
//! - `POST files`, `GET files/{id}/url`, `DELETE files/{id}`,
//!   `GET models` are FREE: the upload, its signed-URL mint, and the
//!   post-OCR cleanup delete carry no charge on Mistral's price card,
//!   and models is the public catalog.
//!
//! Everything else is Unknown.

use serde_json::{json, Value};

use crate::{
    CallObservation, FollowUp, MeasuredCost, ObservedCall, Pricing, ProviderMeter, RouteClass,
};

/// The dearest OCR page rate across the model family: what the ceiling
/// estimate assumes, since the model only becomes known in the answer.
const MAX_USD_PER_PAGE: f64 = 0.004;

/// USD per OCR page for the model the RESPONSE reports (mistral.ai
/// pricing page, checked 2026-08): the ocr-4 family at $4 per 1k
/// pages, ocr-3 at $2, the original ocr-2 releases (2503/2505) at $1.
/// An unrecognized model answers None so the resolve books no money
/// on a rate that may be wrong.
fn usd_per_page(model: &str) -> Option<f64> {
    if model.starts_with("mistral-ocr-4") {
        Some(0.004)
    } else if model.starts_with("mistral-ocr-3") {
        Some(0.002)
    } else if model.starts_with("mistral-ocr-2") {
        Some(0.001)
    } else {
        None
    }
}

/// Mistral caps one OCR document at 1,000 pages, which bounds the
/// worst case of a single call.
const MAX_PAGES_PER_DOC: f64 = 1000.0;

pub struct MistralMeter;

pub static MISTRAL: MistralMeter = MistralMeter;

crate::register_meter!(MISTRAL);

#[async_trait::async_trait]
impl ProviderMeter for MistralMeter {
    fn service(&self) -> &'static str {
        "mistral"
    }

    fn base_url(&self) -> &'static str {
        "https://api.mistral.ai/v1"
    }

    fn classify(&self, method: &str, path: &str) -> RouteClass {
        match (method, path) {
            ("POST", "ocr") => RouteClass::Billable(Pricing::Metered),
            ("POST", "files") | ("GET", "models") => RouteClass::Free,
            ("GET", p) if p.starts_with("files/") && p.ends_with("/url") => RouteClass::Free,
            // The post-OCR cleanup: deleting an uploaded file costs
            // nothing, and refusing it would strand the upload.
            ("DELETE", p) if p.starts_with("files/") => RouteClass::Free,
            _ => RouteClass::Unknown,
        }
    }

    fn prepare(&self, _path: &str, _body: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        // The page count always rides the response; nothing to opt into.
        Ok(None)
    }

    async fn ceiling_usd(
        &self,
        _path: &str,
        body: &[u8],
        http: &reqwest::Client,
    ) -> anyhow::Result<f64> {
        // Estimate, never a blanket cap: (1) an explicit `pages`
        // selection bounds the call exactly; (2) else the document's
        // byte size (a HEAD on its URL, which for the weft node is
        // Mistral's own signed upload URL) at a dense-PDF worst case
        // of one page per 15 KB; (3) only when neither is knowable,
        // the provider's own per-document cap.
        let parsed: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
        if let Some(pages) = parsed.get("pages").and_then(Value::as_array) {
            let n = (pages.len() as f64).clamp(1.0, MAX_PAGES_PER_DOC);
            return Ok(n * MAX_USD_PER_PAGE);
        }
        // The HEAD only ever goes to the provider's own hosts (the weft
        // node passes Mistral's signed upload URL); any other document
        // URL falls through to the per-document cap instead of being
        // fetched from inside the metering client.
        let own_host = |url: &str| {
            reqwest::Url::parse(url)
                .ok()
                .and_then(|u| u.host_str().map(str::to_string))
                .is_some_and(|h| h == "api.mistral.ai" || h.ends_with(".mistral.ai"))
        };
        if let Some(url) = parsed
            .pointer("/document/document_url")
            .and_then(Value::as_str)
            .filter(|u| own_host(u))
        {
            if let Ok(resp) = http.head(url).send().await {
                if let Some(bytes) = resp.content_length().filter(|b| *b > 0) {
                    const WORST_BYTES_PER_PAGE: f64 = 15.0 * 1024.0;
                    let pages =
                        (bytes as f64 / WORST_BYTES_PER_PAGE).ceil().clamp(1.0, MAX_PAGES_PER_DOC);
                    return Ok(pages * MAX_USD_PER_PAGE);
                }
            }
        }
        Ok(MAX_PAGES_PER_DOC * MAX_USD_PER_PAGE)
    }

    fn observe(&self, _path: &str) -> Box<dyn CallObservation> {
        Box::new(super::JsonBodyObservation::default())
    }

    // `ocr` is the only billable route, so the match is the assertion.
    async fn resolve(
        &self,
        path: &str,
        observed: ObservedCall,
        _follow_up: FollowUp<'_>,
    ) -> MeasuredCost {
        // Loud in release too: pricing another route at the OCR page
        // rate would book wrong money silently.
        if path != "ocr" {
            return MeasuredCost {
                amount_usd: None,
                model: None,
                metadata: serde_json::json!({
                    "reason": format!("no metered pricing on route '{path}'"),
                }),
            };
        }
        let body = &observed.data;
        let pages = body
            .pointer("/usage_info/pages_processed")
            .and_then(Value::as_f64);
        // The page rate follows the model the RESPONSE reports (the
        // request usually names a moving alias like mistral-ocr-latest,
        // which re-points across price tiers); a model outside the
        // known family books nothing rather than a rate that may be
        // wrong.
        let model = body.get("model").and_then(Value::as_str).map(str::to_string);
        let rate = model.as_deref().and_then(usd_per_page);
        let amount_usd = match (pages, rate) {
            (Some(p), Some(r)) => Some(p * r),
            _ => None,
        };
        let mut metadata = json!({
            "pages_processed": pages,
            "usd_per_page": rate,
            "interrupted": observed.interrupted,
        });
        if rate.is_none() {
            metadata["reason"] = json!(format!(
                "no known page rate for model '{}'",
                model.as_deref().unwrap_or("<unreported>")
            ));
        }
        MeasuredCost { amount_usd, model, metadata }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Route classification is a billing boundary: only the OCR submit
    /// bills, the file plumbing is free, everything else is Unknown.
    /// The signed-url match is prefix+suffix, so the trick shapes that
    /// could smuggle a path past it are pinned here.
    #[test]
    fn route_classification_is_exact() {
        let m = &MISTRAL;
        assert!(matches!(m.classify("POST", "ocr"), RouteClass::Billable(Pricing::Metered)));
        assert!(matches!(m.classify("POST", "files"), RouteClass::Free));
        assert!(matches!(m.classify("GET", "models"), RouteClass::Free));
        assert!(matches!(m.classify("GET", "files/abc123/url"), RouteClass::Free));
        // The cleanup DELETE is explicitly Free, never Unknown: an
        // Unknown class refuses the call on runtime-supplied
        // credentials and would strand the uploaded file.
        assert!(matches!(m.classify("DELETE", "files/abc123"), RouteClass::Free));
        assert!(matches!(m.classify("DELETE", "files"), RouteClass::Unknown));
        assert!(matches!(m.classify("DELETE", "ocr"), RouteClass::Unknown));
        for trick in [
            ("GET", "files/url"),
            ("GET", "files//url"),
            ("GET", "files/x/y/url"),
            ("GET", "files/../ocr/url"),
            ("GET", "ocr"),
            ("POST", "files/abc123/url"),
            ("DELETE", "files"),
        ] {
            // `files/url` has no id segment yet still matches the
            // prefix+suffix rule; the route relay never normalizes
            // dot segments, so `files/../ocr/url` stays a literal
            // files/ path on the wire. Both are Free by construction,
            // never Billable: the only money route is POST ocr.
            let class = m.classify(trick.0, trick.1);
            assert!(
                !matches!(class, RouteClass::Billable(_)),
                "{trick:?} must never classify billable"
            );
        }
    }

    /// Resolve prices only the OCR route; any other path answers
    /// unknown instead of booking page-rate money for it.
    #[tokio::test]
    async fn resolve_refuses_to_price_a_non_ocr_route() {
        let http = reqwest_middleware::ClientBuilder::new(reqwest::Client::new()).build();
        let observed = ObservedCall {
            interrupted: false,
            status: 200,
            data: json!({"usage_info": {"pages_processed": 3}}),
        };
        let cost = MISTRAL
            .resolve("files", observed, FollowUp { http: &http, base_url: "http://unused.test" })
            .await;
        assert_eq!(cost.amount_usd, None);

        let observed = ObservedCall {
            interrupted: false,
            status: 200,
            data: json!({"model": "mistral-ocr-2505", "usage_info": {"pages_processed": 3}}),
        };
        let cost = MISTRAL
            .resolve("ocr", observed, FollowUp { http: &http, base_url: "http://unused.test" })
            .await;
        assert_eq!(cost.amount_usd, Some(3.0 * 0.001));
    }

    /// The page rate follows the model the RESPONSE reports: each
    /// family prices at its own tier, and a model outside the known
    /// family (or a missing one) books nothing, with the reason in the
    /// metadata.
    #[tokio::test]
    async fn resolve_prices_by_the_reported_model() {
        let http = reqwest_middleware::ClientBuilder::new(reqwest::Client::new()).build();
        let resolve = |model: Value| {
            let observed = ObservedCall {
                interrupted: false,
                status: 200,
                data: json!({"model": model, "usage_info": {"pages_processed": 10}}),
            };
            MISTRAL.resolve("ocr", observed, FollowUp { http: &http, base_url: "http://unused.test" })
        };
        assert_eq!(resolve(json!("mistral-ocr-4106")).await.amount_usd, Some(10.0 * 0.004));
        assert_eq!(resolve(json!("mistral-ocr-3210")).await.amount_usd, Some(10.0 * 0.002));
        assert_eq!(resolve(json!("mistral-ocr-2503")).await.amount_usd, Some(10.0 * 0.001));

        let cost = resolve(json!("mistral-ocr-next-gen")).await;
        assert_eq!(cost.amount_usd, None);
        assert_eq!(cost.model.as_deref(), Some("mistral-ocr-next-gen"));
        let reason = cost.metadata["reason"].as_str().expect("a reason rides the metadata");
        assert!(reason.contains("mistral-ocr-next-gen"), "{reason}");

        let cost = resolve(Value::Null).await;
        assert_eq!(cost.amount_usd, None);
        assert!(cost.metadata["reason"].as_str().expect("reason").contains("<unreported>"));
    }
}
