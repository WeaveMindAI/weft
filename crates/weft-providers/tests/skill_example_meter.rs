//! The meter the `weft-metering` Tangle skill tells people to copy,
//! compiled and exercised here so the snippet in that skill cannot rot.
//! If this file needs editing, edit the skill to match, in every
//! `tangle/*/skills/weft-metering/SKILL.md`.

use async_trait::async_trait;
use weft_providers::providers::JsonBodyObservation;
use weft_providers::{
    CallObservation, FollowUp, MeasuredCost, ObservedCall, Pricing, ProviderMeter, RouteClass,
};

struct AcmeMeter;
static ACME: AcmeMeter = AcmeMeter;

#[async_trait]
impl ProviderMeter for AcmeMeter {
    fn service(&self) -> &'static str {
        "acme"
    }

    fn base_url(&self) -> &'static str {
        "https://api.acme.example/v1"
    }

    fn classify(&self, method: &str, path: &str) -> RouteClass {
        match (method, path) {
            ("POST", "generate") => RouteClass::Billable(Pricing::Fixed { usd: 0.01 }),
            ("GET", "usage") => RouteClass::Free,
            _ => RouteClass::Unknown,
        }
    }

    fn observe(&self, _path: &str, _query: &str, _body: &[u8]) -> Box<dyn CallObservation> {
        Box::new(JsonBodyObservation::default())
    }

    async fn resolve(
        &self,
        _path: &str,
        observed: ObservedCall,
        _follow_up: FollowUp<'_>,
    ) -> MeasuredCost {
        // Three outcomes, never two. A 5xx is NOT a refusal: the work
        // may have run and been billed, with the error coming from in
        // front of it, so zero would claim a real spend was free. The
        // shared rule answers for every non-2xx.
        if let Some(cost) = weft_providers::providers::cost_from_status(observed.status, "the call") {
            return cost;
        }
        MeasuredCost {
            amount_usd: Some(0.01),
            model: None,
            metadata: serde_json::json!({ "status": observed.status }),
        }
    }
}

weft_providers::register_meter!(ACME);

fn observed(status: u16) -> ObservedCall {
    ObservedCall { interrupted: false, status, data: serde_json::json!({}) }
}

fn follow_up() -> reqwest_middleware::ClientWithMiddleware {
    reqwest_middleware::ClientBuilder::new(reqwest::Client::new()).build()
}

#[tokio::test]
async fn the_skills_example_meter_compiles_and_prices() {
    let m = &ACME;
    assert_eq!(m.service(), "acme");
    assert!(matches!(
        m.classify("POST", "generate"),
        RouteClass::Billable(Pricing::Fixed { .. })
    ));
    assert_eq!(m.classify("GET", "usage"), RouteClass::Free);
    assert_eq!(m.classify("POST", "anything-else"), RouteClass::Unknown);

    let http = follow_up();
    let up = || FollowUp { http: &http, base_url: m.base_url() };
    assert_eq!(m.resolve("generate", observed(200), up()).await.amount_usd, Some(0.01));
    // A refusal bills nothing, so zero is a fact.
    assert_eq!(m.resolve("generate", observed(400), up()).await.amount_usd, Some(0.0));
    // A 5xx is NOT a refusal. The work may have run and been billed with
    // the error coming from in front of it, so the honest record is that
    // nobody knows, never zero. This is the rule the example itself used
    // to teach the wrong way round, and node authors copy this file.
    assert_eq!(m.resolve("generate", observed(500), up()).await.amount_usd, None);
}

#[test]
fn the_skills_example_meter_is_discovered_by_its_service_name() {
    let found = weft_providers::meter_for("acme").expect("register_meter! must publish it");
    assert_eq!(found.base_url(), "https://api.acme.example/v1");
}

// ---- The queued-provider snippet from the same skill ----

struct QueuedMeter;
static QUEUED: QueuedMeter = QueuedMeter;

#[async_trait]
impl ProviderMeter for QueuedMeter {
    fn service(&self) -> &'static str {
        "acme_queue"
    }

    fn base_url(&self) -> &'static str {
        "https://api.acme.example/v2"
    }

    fn classify(&self, method: &str, path: &str) -> RouteClass {
        match (method, path) {
            ("POST", "jobs") => RouteClass::Billable(Pricing::Metered),
            ("GET", p) if p.starts_with("jobs/") => RouteClass::Reports,
            _ => RouteClass::Unknown,
        }
    }

    fn observe(&self, _path: &str, _query: &str, _body: &[u8]) -> Box<dyn CallObservation> {
        Box::new(JsonBodyObservation::default())
    }

    fn opens_charge(&self, path: &str, observed: &ObservedCall) -> Option<String> {
        if path != "jobs" || !(200..300).contains(&observed.status) {
            return None;
        }
        observed.data["id"].as_str().map(str::to_string)
    }

    fn charge_reported_on(&self, path: &str, _observed: &ObservedCall) -> Option<String> {
        path.strip_prefix("jobs/")?.split('/').next().filter(|s| !s.is_empty()).map(str::to_string)
    }

    async fn fold_report(
        &self,
        _path: &str,
        observed: ObservedCall,
        scratch: &mut serde_json::Value,
        _follow_up: FollowUp<'_>,
    ) -> Option<MeasuredCost> {
        if observed.data["status"].as_str()? != "done" {
            return None;
        }
        Some(MeasuredCost {
            amount_usd: observed.data["cost_usd"].as_f64(),
            model: None,
            metadata: scratch.clone(),
        })
    }

    async fn resolve(
        &self,
        _path: &str,
        observed: ObservedCall,
        _follow_up: FollowUp<'_>,
    ) -> MeasuredCost {
        // Reaching here means no charge was opened, so the submit did not
        // come back with a job id. A refusal spends nothing; a 5xx may
        // have queued the job anyway, which is then running and spending
        // with no id, so nothing can ever report on it and the honest
        // record is unknown rather than zero.
        weft_providers::providers::cost_from_status(observed.status, "the submit").unwrap_or(MeasuredCost {
            amount_usd: None,
            model: None,
            metadata: serde_json::json!({
                "resolution": "the submit answered without a job id, so nothing can report what it spent",
            }),
        })
    }
}

weft_providers::register_meter!(QUEUED);

#[tokio::test]
async fn the_skills_queued_snippet_pairs_a_submit_with_its_report() {
    let m = &QUEUED;
    let submit = ObservedCall {
        interrupted: false,
        status: 200,
        data: serde_json::json!({ "id": "job-1" }),
    };
    assert_eq!(m.opens_charge("jobs", &submit), Some("job-1".to_string()));
    assert_eq!(m.charge_reported_on("jobs/job-1", &submit), Some("job-1".to_string()));
    // Every route of one job answers the SAME id. Carrying the rest of
    // the path into the id named a charge nothing was held under, so the
    // job never closed and its spend booked as unknown.
    assert_eq!(m.charge_reported_on("jobs/job-1/status", &submit), Some("job-1".to_string()));
    assert_eq!(m.charge_reported_on("jobs/", &submit), None);

    let http = follow_up();
    let up = || FollowUp { http: &http, base_url: m.base_url() };
    let mut scratch = serde_json::json!({ "id": "job-1" });

    let running = ObservedCall {
        interrupted: false,
        status: 200,
        data: serde_json::json!({ "status": "running" }),
    };
    assert!(
        m.fold_report("jobs/job-1", running, &mut scratch, up()).await.is_none(),
        "a running job leaves the charge open"
    );

    let done = ObservedCall {
        interrupted: false,
        status: 200,
        data: serde_json::json!({ "status": "done", "cost_usd": 0.42 }),
    };
    let cost = m
        .fold_report("jobs/job-1", done, &mut scratch, up())
        .await
        .expect("a finished job closes the charge");
    assert_eq!(cost.amount_usd, Some(0.42));
}

// ---- The header declaration from the same skill ----

/// The skill tells a meter author to DECLARE a header rather than write
/// an observation, so what it tells them has to work. It also has to
/// keep saying null for a header that is missing or nonsense: a zero
/// there would claim the call was free.
#[test]
fn the_skills_header_declaration_reads_the_providers_own_figure() {
    let mut headers = http::HeaderMap::new();
    headers.insert("x-acme-cost-usd", http::HeaderValue::from_static("0.037"));

    let mut obs: Box<dyn CallObservation> = Box::new(
        weft_providers::providers::JsonBodyObservation::new().header_f64("x-acme-cost-usd", "costUsd"),
    );
    obs.on_status(200);
    obs.on_headers(&headers);
    assert_eq!(obs.end(false).data["costUsd"].as_f64(), Some(0.037));

    for bad in ["", "not-a-number", "-1"] {
        let mut headers = http::HeaderMap::new();
        if !bad.is_empty() {
            headers.insert("x-acme-cost-usd", http::HeaderValue::from_str(bad).unwrap());
        }
        let mut obs: Box<dyn CallObservation> = Box::new(
            weft_providers::providers::JsonBodyObservation::new()
                .header_f64("x-acme-cost-usd", "costUsd"),
        );
        obs.on_status(200);
        obs.on_headers(&headers);
        assert!(obs.end(false).data["costUsd"].is_null(), "header={bad:?}");
    }
}

// ---- The metered-resolve and priceable snippets from the same skill ----

struct CatalogMeter;

#[async_trait]
impl ProviderMeter for CatalogMeter {
    fn service(&self) -> &'static str {
        "acme_catalog"
    }

    fn base_url(&self) -> &'static str {
        "https://api.acme.example/v3"
    }

    fn classify(&self, method: &str, path: &str) -> RouteClass {
        match (method, path) {
            ("POST", "generate") => RouteClass::Billable(Pricing::Metered),
            _ => RouteClass::Unknown,
        }
    }

    fn observe(&self, _path: &str, _query: &str, _body: &[u8]) -> Box<dyn CallObservation> {
        Box::new(JsonBodyObservation::default())
    }

    /// Compiled here so the skill's snippet cannot rot. Not exercised:
    /// it reaches the provider's own catalog over the network.
    async fn priceable(&self, path: &str, follow_up: FollowUp<'_>) -> anyhow::Result<()> {
        let model = path.trim_start_matches("models/");
        let catalog: serde_json::Value = follow_up
            .http
            .get(format!("{}/pricing", follow_up.base_url))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if catalog[model]["unit_price"].as_f64().is_none() {
            anyhow::bail!(
                "acme lists no price for '{model}', so a call on it could never be \
                 priced; pick a model the catalog prices"
            );
        }
        Ok(())
    }

    async fn resolve(
        &self,
        _path: &str,
        observed: ObservedCall,
        _follow_up: FollowUp<'_>,
    ) -> MeasuredCost {
        let amount_usd = observed.data["usage"]["cost_usd"].as_f64();
        MeasuredCost {
            amount_usd,
            model: observed.data["model"].as_str().map(str::to_string),
            metadata: observed.data,
        }
    }
}

#[tokio::test]
async fn the_skills_metered_resolve_reads_the_providers_own_figure() {
    let m = CatalogMeter;
    let http = follow_up();
    let observed = ObservedCall {
        interrupted: false,
        status: 200,
        data: serde_json::json!({
            "model": "acme-large",
            "usage": { "cost_usd": 0.031 }
        }),
    };
    let cost = m
        .resolve("generate", observed, FollowUp { http: &http, base_url: m.base_url() })
        .await;
    assert_eq!(cost.amount_usd, Some(0.031));
    assert_eq!(cost.model.as_deref(), Some("acme-large"));

    // A provider that says nothing about the charge resolves as unknown,
    // never as a zero.
    let silent = ObservedCall {
        interrupted: false,
        status: 200,
        data: serde_json::json!({ "model": "acme-large" }),
    };
    let cost = m
        .resolve("generate", silent, FollowUp { http: &http, base_url: m.base_url() })
        .await;
    assert_eq!(cost.amount_usd, None);
}
