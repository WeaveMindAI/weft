//! The Firecrawl meter (page fetching + crawling).
//!
//! Routes (relative to `https://api.firecrawl.dev`):
//! - `POST v2/scrape` is BILLABLE, fixed: one page = one credit (the
//!   weft node requests plain markdown; the credit-multiplying
//!   options, stealth proxies and JSON extraction, are not sent).
//!   Resolve books the credit only when the response confirms the
//!   scrape succeeded (2xx + `success: true`); a failed scrape is
//!   not billed by Firecrawl and books zero.
//! - `POST v2/crawl` is BILLABLE, metered: the job's status report
//!   carries `creditsUsed` once it completes; a still-running job at
//!   resolve time records an unknown cost naming the job id.
//! - `GET v2/crawl/{id}` is FREE: the status/result poll.
//!
//! Credits price by subscription tier; the constant below prices one
//! credit at the Standard tier (~$0.00083, firecrawl.dev/pricing,
//! checked 2026-08), rounded up to stay a ceiling-side figure.
//!
//! Everything else is Unknown.

use serde_json::{json, Value};

use crate::{
    CallObservation, FollowUp, MeasuredCost, ObservedCall, Pricing, ProviderMeter, RouteClass,
};

/// USD per credit, rounded UP from the Standard tier rate so the booked
/// figure never understates the spend.
const USD_PER_CREDIT: f64 = 0.001;

/// How long resolve waits for a crawl job's final credit figure. A
/// crawl can run for many minutes; past this window the cost records
/// as unknown (with the job id in the trail) rather than holding the
/// pipeline.
const CRAWL_POLLS: u32 = 25;

pub struct FirecrawlMeter;

pub static FIRECRAWL: FirecrawlMeter = FirecrawlMeter;

crate::register_meter!(FIRECRAWL);

#[async_trait::async_trait]
impl ProviderMeter for FirecrawlMeter {
    fn service(&self) -> &'static str {
        "firecrawl"
    }

    fn base_url(&self) -> &'static str {
        "https://api.firecrawl.dev"
    }

    fn classify(&self, method: &str, path: &str) -> RouteClass {
        match (method, path) {
            ("POST", "v2/scrape") => {
                RouteClass::Billable(Pricing::Fixed { usd: USD_PER_CREDIT })
            }
            ("POST", "v2/crawl") => RouteClass::Billable(Pricing::Metered),
            ("GET", p) if p.starts_with("v2/crawl/") => RouteClass::Free,
            _ => RouteClass::Unknown,
        }
    }

    fn prepare(&self, _path: &str, _body: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(None)
    }

    async fn ceiling_usd(
        &self,
        path: &str,
        body: &[u8],
        _follow_up: FollowUp<'_>,
    ) -> anyhow::Result<f64> {
        if path == "v2/scrape" {
            return Ok(USD_PER_CREDIT);
        }
        // A crawl's worst case is its own page limit, one credit each;
        // a request without a limit cannot be bounded and is refused.
        let parsed: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
        match parsed.get("limit").and_then(Value::as_f64) {
            Some(limit) if limit >= 1.0 => Ok(limit * USD_PER_CREDIT),
            _ => anyhow::bail!(
                "a crawl without a page `limit` cannot be price-bounded; set one"
            ),
        }
    }

    fn observe(&self, _path: &str, _query: &str, _request_body: &[u8]) -> Box<dyn CallObservation> {
        Box::new(super::JsonBodyObservation::default())
    }

    async fn resolve(
        &self,
        path: &str,
        observed: ObservedCall,
        follow_up: FollowUp<'_>,
    ) -> MeasuredCost {
        // The scrape prices fixed, but whether the credit was SPENT is
        // in the response: Firecrawl answers a failed scrape with a
        // non-2xx or a 2xx whose body says `success: false`, and bills
        // only successful scrapes. An unreadable outcome (cut stream,
        // unparseable body) records unknown, never a guess.
        if path == "v2/scrape" {
            let refused = !(200..300).contains(&observed.status);
            let success = observed.data.get("success").and_then(Value::as_bool);
            let (amount, resolution) = if refused || success == Some(false) {
                (Some(0.0), "the scrape failed; Firecrawl bills only successful scrapes")
            } else if success == Some(true) {
                (Some(USD_PER_CREDIT), "fixed route price; the scrape succeeded")
            } else {
                (None, "the scrape's outcome was unreadable; cost unknown")
            };
            return MeasuredCost {
                amount_usd: amount,
                model: None,
                metadata: json!({
                    "resolution": resolution,
                    "status": observed.status,
                    "interrupted": observed.interrupted,
                    "usd_per_credit": USD_PER_CREDIT,
                }),
            };
        }
        // The route is a fact, not an inference: only the crawl submit
        // is Metered.
        if path != "v2/crawl" {
            return MeasuredCost {
                amount_usd: None,
                model: None,
                metadata: json!({
                    "reason": format!("no metered pricing on route '{path}'"),
                }),
            };
        }
        let Some(job) = observed.data.get("id").and_then(Value::as_str) else {
            return MeasuredCost {
                amount_usd: None,
                model: None,
                metadata: json!({ "reason": "the crawl submit answered without a job id" }),
            };
        };
        let url = format!("{}/v2/crawl/{job}", follow_up.base_url.trim_end_matches('/'));
        for _ in 0..CRAWL_POLLS {
            let answer: Option<Value> = match follow_up.http.get(&url).send().await {
                Ok(resp) => resp.json().await.ok(),
                Err(_) => None,
            };
            if let Some(answer) = answer {
                let done = matches!(
                    answer.get("status").and_then(Value::as_str),
                    Some("completed") | Some("failed") | Some("cancelled")
                );
                if done {
                    let credits = answer.get("creditsUsed").and_then(Value::as_f64);
                    return MeasuredCost {
                        amount_usd: credits.map(|c| c * USD_PER_CREDIT),
                        model: None,
                        metadata: json!({
                            "job": job,
                            "creditsUsed": credits,
                            "usd_per_credit": USD_PER_CREDIT,
                        }),
                    };
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        // Still running: the figure exists at the provider but not yet;
        // record unknown WITH the job id so the trail can be settled.
        MeasuredCost {
            amount_usd: None,
            model: None,
            metadata: json!({
                "job": job,
                "reason": "the crawl was still running when the cost window closed",
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn follow_up_stub() -> reqwest_middleware::ClientWithMiddleware {
        reqwest_middleware::ClientBuilder::new(reqwest::Client::new()).build()
    }

    /// Route classification is a billing boundary: scrape fixed, crawl
    /// submit metered, the crawl poll free, everything else Unknown.
    #[test]
    fn route_classification_is_exact() {
        let m = &FIRECRAWL;
        assert!(matches!(
            m.classify("POST", "v2/scrape"),
            RouteClass::Billable(Pricing::Fixed { .. })
        ));
        assert!(matches!(m.classify("POST", "v2/crawl"), RouteClass::Billable(Pricing::Metered)));
        assert!(matches!(m.classify("GET", "v2/crawl/job-1"), RouteClass::Free));
        for trick in [
            ("GET", "v2/scrape"),
            ("POST", "v2/crawl/job-1"),
            ("POST", "v2/batch/scrape"),
            ("DELETE", "v2/crawl/job-1"),
        ] {
            assert!(
                matches!(m.classify(trick.0, trick.1), RouteClass::Unknown),
                "{trick:?} must classify Unknown"
            );
        }
    }

    /// The fixed scrape price books only when the response confirms
    /// the scrape succeeded; a failed scrape (Firecrawl bills none)
    /// books zero, an unreadable outcome records unknown.
    #[tokio::test]
    async fn scrape_resolve_books_by_confirmed_outcome() {
        let http = follow_up_stub();
        let fu = || FollowUp { http: &http, base_url: "http://unused.test" };

        let ok = ObservedCall {
            interrupted: false,
            status: 200,
            data: json!({"success": true, "data": {"markdown": "# hi"}}),
        };
        assert_eq!(
            FIRECRAWL.resolve("v2/scrape", ok, fu()).await.amount_usd,
            Some(USD_PER_CREDIT)
        );

        let failed_body = ObservedCall {
            interrupted: false,
            status: 200,
            data: json!({"success": false, "error": "blocked"}),
        };
        assert_eq!(FIRECRAWL.resolve("v2/scrape", failed_body, fu()).await.amount_usd, Some(0.0));

        let refused = ObservedCall { interrupted: false, status: 402, data: json!({}) };
        assert_eq!(FIRECRAWL.resolve("v2/scrape", refused, fu()).await.amount_usd, Some(0.0));

        let cut = ObservedCall { interrupted: true, status: 200, data: serde_json::Value::Null };
        assert_eq!(
            FIRECRAWL.resolve("v2/scrape", cut, fu()).await.amount_usd,
            None,
            "an unreadable outcome is unknown, never a guess"
        );
    }
}
