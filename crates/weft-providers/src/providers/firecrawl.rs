//! The Firecrawl meter (page fetching + crawling).
//!
//! Routes (relative to `https://api.firecrawl.dev`):
//! - `POST v2/scrape` is BILLABLE: one page is one credit for a plain
//!   fetch, and the REQUEST is read to see whether it asked for
//!   something dearer (`multiplying_options`). Resolve books the credit
//!   only when the response confirms the scrape succeeded (2xx +
//!   `success: true`); a failed scrape is not billed by Firecrawl and
//!   books zero.
//! - `POST v2/crawl` is BILLABLE, metered: the job's status report
//!   carries `creditsUsed` once it completes. The submit opens a charge
//!   and the node's own status reads close it.
//! - `GET v2/crawl/{id}` and `GET v2/crawl/{id}/status` REPORT on that
//!   charge: free at Firecrawl, and the only place `creditsUsed` is
//!   stated.
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

/// Which of a scrape request's options Firecrawl bills above the
/// one-credit page rate, named as the request spelled them.
///
/// Empty means a plain page fetch, which is the one credit. Anything
/// here means the flat rate is known to be wrong, and since no rate for
/// these is recorded in this file the call books as unknown rather than
/// as a guess.
fn multiplying_options(asked: &Value) -> Vec<String> {
    // A crawl states its per-page options under `scrapeOptions`, a
    // scrape states the same options at the top level. BOTH are read,
    // not one or the other: a request may carry either spelling, and
    // reading only the nested one whenever it exists meant a top-level
    // `proxy: "stealth"` beside an empty `scrapeOptions` was never seen
    // and the call priced as a plain page.
    let mut levels = vec![asked];
    if let Some(nested) = asked.get("scrapeOptions").filter(|v| v.is_object()) {
        levels.push(nested);
    }
    let mut found = Vec::new();
    if levels.iter().any(|l| l.get("proxy").and_then(Value::as_str) == Some("stealth")) {
        found.push("a stealth proxy".to_string());
    }
    // `formats` is a list, and each entry is either the format's name or
    // an object naming it under `type`. A lone format may also be given
    // as a bare string, which is still the format being asked for.
    let asks_json = |level: &Value| -> bool {
        let names: Vec<&str> = match level.get("formats") {
            Some(Value::Array(items)) => items
                .iter()
                .filter_map(|v| v.as_str().or_else(|| v.get("type").and_then(Value::as_str)))
                .collect(),
            Some(Value::String(one)) => vec![one.as_str()],
            _ => Vec::new(),
        };
        // Structured extraction and the schema that configures it are one
        // option asked for two ways, so they name one entry. Saying both
        // read as two separate surcharges on the spend trail.
        names.contains(&"json")
            || level.get("jsonOptions").is_some()
            || level.get("extract").is_some()
    };
    if levels.iter().copied().any(asks_json) {
        found.push("structured extraction".to_string());
    }
    found.sort();
    found.dedup();
    found
}

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
                RouteClass::Billable(Pricing::Metered)
            }
            ("POST", "v2/crawl") => RouteClass::Billable(Pricing::Metered),
            // The crawl's own status reads. Free at Firecrawl, and the
            // only place `creditsUsed` is stated, so they are what closes
            // the charge the submit opened. The node polls these anyway
            // to wait on its own job; the meter reads them in passing
            // rather than running a second poll of its own.
            ("GET", p) if p.starts_with("v2/crawl/") => RouteClass::Reports,
            _ => RouteClass::Unknown,
        }
    }

    async fn ceiling_usd(
        &self,
        path: &str,
        body: &[u8],
        _follow_up: FollowUp<'_>,
    ) -> anyhow::Result<f64> {
        let parsed: Value = serde_json::from_slice(body)?;
        let options = multiplying_options(&parsed);
        anyhow::ensure!(options.is_empty(), "Firecrawl cannot price-bound these requested options: {}", options.join(", "));
        if path == "v2/scrape" {
            return Ok(USD_PER_CREDIT);
        }
        // A crawl's worst case is its own page limit, one credit each;
        // a request without a limit cannot be bounded and is refused.
        match parsed.get("limit").and_then(Value::as_f64) {
            Some(limit) if limit >= 1.0 => Ok(limit * USD_PER_CREDIT),
            _ => anyhow::bail!(
                "a crawl without a page `limit` cannot be price-bounded; set one"
            ),
        }
    }

    fn observe(&self, _path: &str, _query: &str, request_body: &[u8]) -> Box<dyn CallObservation> {
        // The REQUEST decides the price as much as the answer does, so
        // what it asked for is carried into `resolve` rather than
        // assumed. A scrape is one credit for a plain fetch, and
        // Firecrawl charges several for a stealth proxy or for structured
        // extraction. This meter sits on the CONNECTION, not on one node,
        // so "our node does not send those" was never a fact about the
        // call: any program with a firecrawl connection can send them,
        // and every such call was booked at one credit.
        let asked: Value = serde_json::from_slice(request_body).unwrap_or(Value::Null);
        let mut seed = serde_json::Map::new();
        seed.insert("requestedOptions".to_string(), json!(multiplying_options(&asked)));
        // Seeding SHAPES the observation, which means the parsed body no
        // longer arrives whole: from here on every value this meter reads
        // out of a response has to be declared, or it observes null. It is
        // the whole body's worth for this meter, which is four fields.
        Box::new(
            super::JsonBodyObservation::new()
                .seed(seed)
                // The scrape's own verdict: Firecrawl answers 2xx with
                // `success: false` on a page it did not bill.
                .field("success", "/success")
                // The accepted crawl's job id, which is what the charge is
                // opened under, and what its status reads report on.
                .field("id", "/id")
                // The crawl status report: whether it is done, and the
                // credits it came to.
                .field("status", "/status")
                .field("creditsUsed", "/creditsUsed"),
        )
    }

    async fn resolve(
        &self,
        path: &str,
        observed: ObservedCall,
        _follow_up: FollowUp<'_>,
    ) -> MeasuredCost {
        // The scrape prices fixed, but whether the credit was SPENT is
        // in the response: Firecrawl answers a failed scrape with a
        // non-2xx or a 2xx whose body says `success: false`, and bills
        // only successful scrapes. An unreadable outcome (cut stream,
        // unparseable body) records unknown, never a guess.
        if path == "v2/scrape" {
            let success = observed.data.get("success").and_then(Value::as_bool);
            // The one shared status rule: a refusal is a known zero, a
            // 5xx is not a refusal and books unknown.
            if let Some(cost) = super::cost_from_status(observed.status, "the scrape") {
                return cost;
            }
            let asked: Vec<String> = observed.data["requestedOptions"]
                .as_array()
                .map(|a| a.iter().filter_map(Value::as_str).map(str::to_string).collect())
                .unwrap_or_default();
            let (amount, resolution) = if success == Some(false) {
                (Some(0.0), "the scrape failed; Firecrawl bills only successful scrapes".to_string())
            } else if success == Some(true) && !asked.is_empty() {
                // The request asked for something Firecrawl charges more
                // than one credit for. The multiplier is not written down
                // here, and one credit is KNOWN to be wrong, so the spend
                // records as unknown naming what made it so. Guessing a
                // multiplier would put a wrong number on the trail; zero
                // or one credit would understate a real spend.
                (
                    None,
                    format!(
                        "the scrape asked for {}, which Firecrawl bills above the one-credit page \
                         rate, and this meter holds no rate for it: the spend is real and its \
                         figure is not known here",
                        asked.join(" and ")
                    ),
                )
            } else if success == Some(true) {
                (Some(USD_PER_CREDIT), "fixed route price; the scrape succeeded".to_string())
            } else {
                (None, "the scrape's outcome was unreadable; cost unknown".to_string())
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
                    "resolution": format!("no metered pricing on route '{path}'"),
                }),
            };
        }
        // An accepted crawl opens a charge instead of resolving here, so
        // reaching this point means no charge was opened. Either
        // Firecrawl REFUSED the submit, which spends nothing and is a
        // known zero, or it accepted it without a job id, which nothing
        // can ever report on and is a genuine unknown.
        // The shared status rule again: a refused submit spends nothing,
        // and a 5xx is NOT a refusal (a gateway timing out over a crawl
        // that was in fact queued leaves the job running and spending,
        // with no id, so nothing can ever report on it).
        if let Some(cost) = super::cost_from_status(observed.status, "the crawl submit") {
            return cost;
        }
        MeasuredCost {
            amount_usd: None,
            model: None,
            metadata: json!({
                "resolution": "the crawl submit answered without a job id, so nothing can report what it spent",
                "status": observed.status,
            }),
        }
    }

    fn opens_charge(&self, path: &str, observed: &ObservedCall) -> Option<String> {
        // Only the crawl submits a job; a scrape is charged where it is
        // made. A crawl Firecrawl refused spends nothing and has no id.
        // The shared status rule: only an ACCEPTED submit has a job, and
        // it is spelled once, here as everywhere.
        if path != "v2/crawl"
            || !matches!(super::verdict_for_status(observed.status), super::StatusVerdict::Accepted)
        {
            return None;
        }
        observed.data.get("id").and_then(Value::as_str).map(str::to_string)
    }

    fn charge_reported_on(&self, path: &str, _observed: &ObservedCall) -> Option<String> {
        // The job id is the FIRST segment after the prefix and nothing
        // more: `v2/crawl/{id}`, `v2/crawl/{id}/status` and
        // `v2/crawl/{id}/errors` all report on the same job, and taking
        // the rest of the path with it produced an id (`{id}/status`)
        // that matched no open charge, so the crawl never closed and
        // booked as unknown while its own answer carried `creditsUsed`.
        path.strip_prefix("v2/crawl/")?
            .split('/')
            .next()
            .filter(|j| !j.is_empty())
            .map(str::to_string)
    }

    async fn fold_report(
        &self,
        _path: &str,
        observed: ObservedCall,
        scratch: &mut Value,
        _follow_up: FollowUp<'_>,
    ) -> Option<MeasuredCost> {
        // A crawl accrues credits as it runs and only settles when it
        // stops, so a read of a running job prices nothing and the
        // charge waits for the read that finds it terminal.
        let state = observed.data.get("status").and_then(Value::as_str)?;
        if !matches!(state, "completed" | "failed" | "cancelled") {
            return None;
        }
        let credits = observed.data.get("creditsUsed").and_then(Value::as_f64);
        Some(MeasuredCost {
            amount_usd: credits.map(|c| c * USD_PER_CREDIT),
            model: None,
            metadata: json!({
                "job": scratch.get("id").and_then(Value::as_str),
                "state": state,
                "creditsUsed": credits,
                "usd_per_credit": USD_PER_CREDIT,
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    /// Every read of a crawl reports on the SAME job, whichever of the
    /// job's routes it went to. An id that carried the rest of the path
    /// matched no open charge, so a finished crawl booked as unknown
    /// while its own answer stated `creditsUsed`.
    #[test]
    fn every_route_of_one_crawl_reports_on_one_job() {
        use crate::ProviderMeter;
        let m = super::FirecrawlMeter;
        let observed = crate::ObservedCall {
            status: 200,
            interrupted: false,
            data: serde_json::json!({}),
        };
        for path in ["v2/crawl/job-1", "v2/crawl/job-1/status", "v2/crawl/job-1/errors"] {
            assert_eq!(
                m.charge_reported_on(path, &observed).as_deref(),
                Some("job-1"),
                "{path} reported on the wrong job"
            );
            assert!(matches!(m.classify("GET", path), crate::RouteClass::Reports));
        }
        assert_eq!(m.charge_reported_on("v2/crawl/", &observed), None);
    }

    /// The dearer options are the same options wherever the request
    /// spells them, and structured extraction asked for twice is one
    /// option, not two surcharges.
    #[test]
    fn the_dearer_options_are_read_however_the_request_spells_them() {
        let top = serde_json::json!({"formats": "json", "jsonOptions": {"schema": {}}});
        assert_eq!(super::multiplying_options(&top), vec!["structured extraction".to_string()]);
        let crawl = serde_json::json!({
            "limit": 10,
            "scrapeOptions": {"proxy": "stealth", "formats": [{"type": "json"}]},
        });
        assert_eq!(
            super::multiplying_options(&crawl),
            vec!["a stealth proxy".to_string(), "structured extraction".to_string()]
        );
        assert!(super::multiplying_options(&serde_json::json!({"url": "https://x.example"})).is_empty());
        // BOTH levels, not one or the other: a top-level option beside a
        // nested block used to go unseen, and the scrape then priced as a
        // plain page.
        let both = serde_json::json!({"proxy": "stealth", "scrapeOptions": {"formats": ["json"]}});
        assert_eq!(
            super::multiplying_options(&both),
            vec!["a stealth proxy".to_string(), "structured extraction".to_string()]
        );
        // A `scrapeOptions` that is not an object is not a place to read
        // options from, and does not stop the top level being read.
        let odd = serde_json::json!({"proxy": "stealth", "scrapeOptions": "all"});
        assert_eq!(super::multiplying_options(&odd), vec!["a stealth proxy".to_string()]);
        // A format entry naming nothing is not a format.
        let anon = serde_json::json!({"formats": [{"quality": 1}]});
        assert!(super::multiplying_options(&anon).is_empty());
    }

    /// Every value this meter reads out of a response comes through
    /// `observe`, and `observe` SHAPES the observation: a field it does
    /// not declare observes null. The rest of this file's tests hand
    /// `resolve` a body they built themselves, so none of them can see
    /// that. This one drives the real observer end to end.
    #[test]
    fn the_observer_carries_every_field_the_meter_reads() {
        use crate::ProviderMeter;
        let m = super::FirecrawlMeter;
        let body = serde_json::json!({
            "success": true,
            "id": "job-1",
            "status": "completed",
            "creditsUsed": 12,
        });
        let mut obs = m.observe("v2/scrape", "", br#"{"url":"https://x.example"}"#);
        obs.on_status(200);
        obs.on_chunk(body.to_string().as_bytes());
        let observed = obs.end(false);
        assert_eq!(observed.data["success"], serde_json::json!(true));
        assert_eq!(observed.data["id"], serde_json::json!("job-1"));
        assert_eq!(observed.data["status"], serde_json::json!("completed"));
        assert_eq!(observed.data["creditsUsed"], serde_json::json!(12));
        assert_eq!(observed.data["requestedOptions"], serde_json::json!([]));

        // And the request's dearer options are read off the request body.
        let mut obs = m.observe(
            "v2/scrape",
            "",
            br#"{"url":"https://x.example","proxy":"stealth","formats":["json"]}"#,
        );
        obs.on_status(200);
        obs.on_chunk(body.to_string().as_bytes());
        let observed = obs.end(false);
        let asked = observed.data["requestedOptions"].as_array().expect("an array").len();
        assert_eq!(asked, 2, "{}", observed.data["requestedOptions"]);
    }

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
            RouteClass::Billable(Pricing::Metered)
        ));
        assert!(matches!(m.classify("POST", "v2/crawl"), RouteClass::Billable(Pricing::Metered)));
        // The crawl reads are watched, not ignored: they carry
        // `creditsUsed`, which is what settles the submit's charge.
        assert!(matches!(m.classify("GET", "v2/crawl/job-1"), RouteClass::Reports));
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

    #[tokio::test]
    async fn a_ceiling_refuses_options_without_a_known_upper_price() {
        let http = follow_up_stub();
        let follow = || FollowUp { http: &http, base_url: "http://unused.test" };
        assert_eq!(FIRECRAWL.ceiling_usd("v2/scrape", br#"{"url":"https://example.test"}"#, follow()).await.unwrap(), USD_PER_CREDIT);
        assert!(FIRECRAWL.ceiling_usd("v2/scrape", br#"{"proxy":"stealth"}"#, follow()).await.is_err());
        assert!(FIRECRAWL.ceiling_usd("v2/crawl", br#"{"limit":10,"scrapeOptions":{"formats":["json"]}}"#, follow()).await.is_err());
    }
}

#[cfg(test)]
mod crawl_charge_tests {
    use super::*;

    fn observed(status: u16, data: Value) -> ObservedCall {
        ObservedCall { interrupted: false, status, data }
    }

    fn http() -> reqwest_middleware::ClientWithMiddleware {
        reqwest_middleware::ClientBuilder::new(reqwest::Client::new()).build()
    }

    /// The crawl submit spends, and what it spent is only knowable once
    /// the job stops, so the submit opens a charge under the job id.
    #[test]
    fn an_accepted_crawl_opens_a_charge_under_its_job_id() {
        let m = &FIRECRAWL;
        assert_eq!(
            m.opens_charge("v2/crawl", &observed(200, json!({ "id": "job-1" }))),
            Some("job-1".to_string())
        );
        // A scrape is charged where it is made, so it opens nothing even
        // if its body happens to carry an id.
        assert_eq!(m.opens_charge("v2/scrape", &observed(200, json!({ "id": "job-1" }))), None);
        // A refused crawl spends nothing.
        assert_eq!(m.opens_charge("v2/crawl", &observed(402, json!({ "id": "job-1" }))), None);
    }

    /// Every read of a crawl job names the charge it settles.
    #[test]
    fn a_crawl_read_names_the_charge_it_reports_on() {
        let m = &FIRECRAWL;
        let o = observed(200, json!({}));
        assert_eq!(m.charge_reported_on("v2/crawl/job-1", &o), Some("job-1".to_string()));
        assert_eq!(m.charge_reported_on("v2/crawl", &o), None);
        assert_eq!(m.charge_reported_on("v2/scrape", &o), None);
    }

    /// A crawl accrues credits while it runs, so only the read that
    /// finds it stopped prices the charge. Pricing a running job would
    /// book a part-finished crawl as if it were the whole thing.
    #[tokio::test]
    async fn only_a_stopped_crawl_prices_the_charge() {
        let m = &FIRECRAWL;
        let mut scratch = json!({ "id": "job-1" });
        let http = http();

        let still_going = m
            .fold_report(
                "v2/crawl/job-1",
                observed(200, json!({ "status": "scraping", "creditsUsed": 4.0 })),
                &mut scratch,
                FollowUp { http: &http, base_url: m.base_url() },
            )
            .await;
        assert!(still_going.is_none(), "a running crawl must leave the charge open");

        let done = m
            .fold_report(
                "v2/crawl/job-1",
                observed(200, json!({ "status": "completed", "creditsUsed": 12.0 })),
                &mut scratch,
                FollowUp { http: &http, base_url: m.base_url() },
            )
            .await
            .expect("a stopped crawl must close the charge");
        assert_eq!(done.amount_usd, Some(12.0 * USD_PER_CREDIT));
        assert_eq!(done.metadata["job"].as_str(), Some("job-1"));
    }

    /// A crawl that stopped without stating its credits has no honest
    /// figure. It still closes the charge, as an unknown, so it cannot
    /// sit open forever waiting for a number that will never come.
    #[tokio::test]
    async fn a_stopped_crawl_with_no_credits_closes_as_unknown() {
        let m = &FIRECRAWL;
        let mut scratch = json!({ "id": "job-1" });
        let http = http();
        let done = m
            .fold_report(
                "v2/crawl/job-1",
                observed(200, json!({ "status": "failed" })),
                &mut scratch,
                FollowUp { http: &http, base_url: m.base_url() },
            )
            .await
            .expect("a stopped crawl must close the charge");
        assert_eq!(done.amount_usd, None, "recorded AS unknown, never as zero");
    }
}
