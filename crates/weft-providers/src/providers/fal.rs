//! The fal meter.
//!
//! fal serves every model through one queue transport
//! (`https://queue.fal.run/<model>/...`). A call is three requests:
//! `POST <model>` submits and answers a ticket, then
//! `GET <app>/requests/<id>/status` polls it and
//! `GET <app>/requests/<id>` fetches the result (the app is the model's
//! first two segments; the full model id cannot be recovered from it).
//!
//! The money is committed by the submit, and the amount does not exist
//! yet when the submit answers: nothing has been generated. So the
//! submit opens a charge under fal's request id and the two reads report
//! on it, which is what [`RouteClass::Reports`] is for. The waiting and
//! the pairing are the worker's job; this meter only declares.
//!
//! Prices come from fal's OWN catalog
//! (`GET https://api.fal.ai/v1/models/pricing?endpoint_id=<model>`,
//! authenticated with the same credential the call rides, free to
//! query), fetched at call time and cached, so EVERY fal model is
//! covered without a hand-kept rate table. It answers a `unit_price` and
//! a `unit`.
//!
//! What the job came to is read one of two ways:
//!
//! - A model billed by something countable in its output has fal state
//!   that count in `X-Fal-Billable-Units` on the RESULT fetch. Units
//!   seen on fal's live catalog: images, megapixels, requests, videos,
//!   seconds, credits.
//! - A model billed by GPU time has no output count to state, so fal
//!   sends no such header and reports `metrics.inference_time` on the
//!   STATUS route instead. Its catalog unit is `compute seconds`, and
//!   that measured run time is the quantity it charges.
//!
//! Every model measured so far is one or the other. The header is
//! optional by fal's design (whoever publishes a model chooses to set
//! it), so a finished job that states no count on a model priced by
//! anything but `compute seconds` has no honest figure and books as
//! unknown.
//!
//! Neither is derived from the request, and that matters: measured
//! against fal on 2026-09-10, one 512x512 image asked of
//! `fal-ai/flux/dev` is a quarter of a megapixel by the request and one
//! whole megapixel by fal's own count, so pricing it off the request
//! under-reports the call four times over.

use serde_json::{json, Value};

use crate::{
    CallObservation, FollowUp, MeasuredCost, ObservedCall, Pricing, ProviderMeter, RouteClass,
};

/// fal's platform API, where the pricing catalog lives. A provider-owned
/// origin distinct from the queue transport; the same `Key` credential
/// authenticates both.
const FAL_PRICING_URL: &str = "https://api.fal.ai/v1/models/pricing";

pub struct FalMeter;

pub static FAL: FalMeter = FalMeter;

crate::register_meter!(FAL);

/// Parse the pricing catalog's answer for `model`: its unit price and
/// billing unit. Pure, so the wire shape is pinned without a server.
fn parse_price(model: &str, body: &Value) -> anyhow::Result<(f64, String)> {
    let entry = body["prices"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|p| p["endpoint_id"].as_str() == Some(model))
        .ok_or_else(|| anyhow::anyhow!("fal's pricing catalog lists no price for '{model}'"))?;
    let unit_price = entry["unit_price"]
        .as_f64()
        .filter(|p| p.is_finite() && *p >= 0.0)
        .ok_or_else(|| anyhow::anyhow!("fal's pricing catalog answers no usable unit_price for '{model}'"))?;
    let unit = entry["unit"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("fal's pricing catalog answers no unit for '{model}'"))?;
    Ok((unit_price, unit.to_string()))
}

impl FalMeter {
    /// The model's (unit_price, unit) from fal's pricing catalog, asked
    /// once per worker (`follow_up.shared`): prices move on the timescale
    /// of product launches, and a worker that goes idle lets its copy go.
    /// The query rides the meter's signed side-query lane (the same
    /// credential the call rides authenticates the catalog; the query
    /// itself is free).
    async fn price_for(&self, model: &str, follow_up: &FollowUp<'_>) -> anyhow::Result<(f64, String)> {
        let price = follow_up
            .shared
            .named(&format!("fal-price:{model}"), || async {
                Self::fetch_price(model, follow_up.http)
                    .await
                    .map_err(|e| weft_core::error::WeftError::NodeExecution(format!("{e:#}")))
            })
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok((*price).clone())
    }

    /// The model's price, asked of fal's pricing catalog now.
    async fn fetch_price(model: &str, http: &reqwest_middleware::ClientWithMiddleware) -> anyhow::Result<(f64, String)> {
        let resp = http
            .get(FAL_PRICING_URL)
            .query(&[("endpoint_id", model)])
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("fal's pricing catalog could not be reached: {e}"))?;
        let status = resp.status();
        let body: Value = resp
            .json()
            .await
            .map_err(|e| anyhow::anyhow!("fal's pricing catalog answered non-JSON: {e}"))?;
        if !status.is_success() {
            anyhow::bail!("fal's pricing catalog refused the price lookup ({status}): {body}");
        }
        parse_price(model, &body)
    }

}

/// Split a queue path into (model, tail). The model is the longest
/// prefix before a `/requests/...` tail (or the whole path for a
/// submit); segments are checked clean so traversal never matches.
fn model_and_tail(path: &str) -> Option<(&str, &str)> {
    let clean = |seg: &str| {
        !seg.is_empty()
            && seg
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
            && !seg.starts_with('.')
    };
    let (model, tail) = match path.find("/requests/") {
        Some(i) => (&path[..i], &path[i + 1..]),
        None => (path, ""),
    };
    if model.split('/').all(clean) && tail.split('/').filter(|s| !s.is_empty()).all(clean) {
        Some((model, tail))
    } else {
        None
    }
}

/// The header fal states the billed quantity in, already denominated in
/// the unit its rate card prices for the model.
/// <https://fal.ai/docs/documentation/model-apis/common-parameters>
const BILLABLE_UNITS_HEADER: &str = "x-fal-billable-units";

/// The request id inside a `requests/<id>[/status]` tail.
fn request_id_in(tail: &str) -> Option<&str> {
    tail.strip_prefix("requests/")?.split('/').next().filter(|s| !s.is_empty())
}

#[async_trait::async_trait]
impl ProviderMeter for FalMeter {
    fn service(&self) -> &'static str {
        "fal"
    }

    fn base_url(&self) -> &'static str {
        "https://queue.fal.run"
    }

    fn classify(&self, method: &str, path: &str) -> RouteClass {
        let Some((_, tail)) = model_and_tail(path) else { return RouteClass::Unknown };
        match (method, tail.is_empty()) {
            // The submit is the spend. EVERY model is billable: the price
            // comes from fal's own catalog at call time, so no model list
            // gates the door.
            ("POST", true) => RouteClass::Billable(Pricing::Metered),
            // Status + result reads of an already-submitted request.
            // Free at fal, and the only responses that say what the
            // submit came to: the status route measures the run, the
            // result route states the billed count. These routes address
            // the app (`owner/name`), which cannot name the variant that
            // was submitted, so the model is read off the open charge
            // rather than off this path.
            ("GET", false) => RouteClass::Reports,
            _ => RouteClass::Unknown,
        }
    }


    fn observe(&self, path: &str, _query: &str, _request_body: &[u8]) -> Box<dyn CallObservation> {
        // The model is all the request contributes. What the call cost
        // comes off what fal reports when the job finishes, never off
        // these bytes.
        let (model, tail) = model_and_tail(path).unwrap_or_default();
        if tail.is_empty() {
            // The submit: the queue ticket's id, plus whether fal took
            // the job at all.
            Box::new(
                super::JsonBodyObservation::new()
                    .seed(serde_json::Map::from_iter([("model".to_string(), json!(model))]))
                    .field("requestId", "/request_id")
                    .note_outcome(),
            )
        } else {
            // A read of an already-submitted request. Both reads are
            // free and both carry a piece of what the submit came to:
            // the status route measures the run and says when it
            // finished, the result route states the billed count in a
            // header. Which one this is does not need deciding here;
            // `fold_report` reads what it needs.
            Box::new(
                super::JsonBodyObservation::new()
                    .header_f64(BILLABLE_UNITS_HEADER, "billableUnits")
                    // `jobStatus` is what the live poll test reads to
                    // know the job finished; `fold_report` branches on
                    // the route, not on it.
                    .field("jobStatus", "/status")
                    .field("computeSeconds", "/metrics/inference_time"),
            )
        }
    }

    async fn priceable(&self, path: &str, follow_up: FollowUp<'_>) -> anyhow::Result<()> {
        // The catalog's unit price is the only place a fal price
        // exists, so a model it does not list can never be priced,
        // whatever fal answers afterwards. Refuse before spending.
        let model = model_and_tail(path).map(|(m, _)| m).unwrap_or_default();
        self.price_for(model, &follow_up).await.map(|_| ()).map_err(|e| {
            anyhow::anyhow!(
                "fal's pricing catalog does not price '{model}', so what a call on it \
                 costs could never be measured: {e:#}"
            )
        })
    }

    fn opens_charge(&self, _path: &str, observed: &ObservedCall) -> Option<String> {
        // A submit fal refused spent nothing, so it has no charge to
        // hold open; `resolve` books it as the zero it is.
        if observed.data["accepted"] != json!(true) {
            return None;
        }
        observed.data["requestId"].as_str().map(str::to_string)
    }

    fn charge_reported_on(&self, path: &str, _observed: &ObservedCall) -> Option<String> {
        let (_, tail) = model_and_tail(path)?;
        request_id_in(tail).map(str::to_string)
    }

    async fn fold_report(
        &self,
        path: &str,
        observed: ObservedCall,
        scratch: &mut Value,
        follow_up: FollowUp<'_>,
    ) -> Option<MeasuredCost> {
        // The status route measures the run; keep whatever it last
        // reported, because the result route does not repeat it.
        if let Some(seconds) = observed.data["computeSeconds"].as_f64() {
            scratch["computeSeconds"] = json!(seconds);
        }
        // The status route NEVER prices the charge, even once it says
        // COMPLETED: a model billed by an output unit states that count
        // on the result fetch and nowhere else, so closing here would
        // read "no count" off the one response that never carries one.
        // The charge stays open for the fetch that answers.
        if path.ends_with("/status") {
            return None;
        }
        // A result fetch fal did not answer successfully (a 429, a
        // gateway 5xx) is not evidence about the job: closing here
        // would record the spend as unknown for ever on a failure the
        // charge can survive, blaming fal for stating nothing when it
        // was never asked. The charge stays open for the read that does
        // answer; if none ever comes, the execution's flush books it as
        // the unknown it then genuinely is.
        if !(200..300).contains(&observed.status) {
            return None;
        }
        let units = observed.data["billableUnits"].as_f64();
        // A read that answered 200 and was then CUT mid-body still
        // carries what did not come from the body, and both of this
        // meter's quantities are in that class: `billableUnits` is
        // declared off the `X-Fal-Billable-Units` header, read before
        // the first body byte, and the compute seconds were folded from
        // an EARLIER status read into the scratch. So a truncated read is
        // not the end of the charge; it is only the end of it when
        // nothing priceable turns up below, which is what `cut` decides
        // at each of those points. Closing as unknown on a transport
        // failure the charge can survive throws away a figure fal
        // already stated, and nothing re-reads that request.
        let cut = observed.interrupted;

        let mut metadata = scratch.clone();
        let model = scratch["model"].as_str().unwrap_or_default().to_string();
        let (unit_price, unit) = match self.price_for(&model, &follow_up).await {
            Ok(price) => price,
            Err(e) => {
                // The rate card is the only place a unit price exists, so
                // a finished job we cannot price is the one case with no
                // honest figure. Recorded AS unknown, never a zero.
                if cut {
                    return None;
                }
                metadata["resolution"] = json!(format!("price lookup failed: {e:#}"));
                return Some(MeasuredCost { amount_usd: None, model: Some(model), metadata });
            }
        };

        // fal's own count, already denominated in the unit its rate card
        // prices. Authoritative for every model billed by an output unit.
        if let Some(units) = units {
            metadata["billedUnits"] = json!(units);
            metadata["resolution"] =
                json!(format!("{units} x ${unit_price} per {unit} (fal's billed units)"));
            return Some(MeasuredCost {
                amount_usd: Some(unit_price * units),
                model: Some(model),
                metadata,
            });
        }

        // No count because there is nothing countable to bill: the model
        // is priced per compute second and fal measured the run. Any
        // other unit here would mean pairing a price with a quantity it
        // does not charge for, which is not something to guess at.
        if unit != "compute seconds" {
            if cut {
                return None;
            }
            metadata["resolution"] = json!(format!(
                "fal stated no billed units on the result of a job it prices per '{unit}', \
                 so nothing it reported is the quantity that unit charges"
            ));
            return Some(MeasuredCost { amount_usd: None, model: Some(model), metadata });
        }
        let Some(seconds) = scratch["computeSeconds"].as_f64() else {
            if cut {
                return None;
            }
            metadata["resolution"] = json!(
                "fal stated no billed units for a finished job and measured no run time \
                 for it either, so it reported nothing this call can be priced from"
            );
            return Some(MeasuredCost { amount_usd: None, model: Some(model), metadata });
        };
        metadata["computeSeconds"] = json!(seconds);
        metadata["resolution"] =
            json!(format!("{seconds}s x ${unit_price} per {unit} (fal's measured run time)"));
        Some(MeasuredCost {
            amount_usd: Some(unit_price * seconds),
            model: Some(model),
            metadata,
        })
    }

    /// Only a submit fal REFUSED reaches here: an accepted one opens a
    /// charge instead (see [`Self::opens_charge`]) and is priced by
    /// [`Self::fold_report`] when fal says what the job came to.
    async fn resolve(
        &self,
        _path: &str,
        observed: ObservedCall,
        _follow_up: FollowUp<'_>,
    ) -> MeasuredCost {
        let mut metadata = observed.data.clone();
        let model = observed.data["model"].as_str().unwrap_or_default().to_string();
        if observed.data["accepted"] == json!(true) {
            // An accepted submit that never opened a charge means it
            // answered no request id, so nothing can ever report on it.
            metadata["resolution"] = json!(
                "fal accepted the submit but answered no request_id, so the job it started \
                 cannot be looked up"
            );
            return MeasuredCost { amount_usd: None, model: Some(model), metadata };
        }
        if let Some(mut cost) = super::cost_from_status(observed.status, "the fal submit") {
            cost.model = Some(model);
            return cost;
        }
        metadata["resolution"] = json!("fal returned no usable acceptance or job id; the spend is unknown");
        MeasuredCost { amount_usd: None, model: Some(model), metadata }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_classify_by_model_and_tail() {
        let m = &FAL;
        // ANY clean model submit is billable; the catalog prices it later.
        assert!(matches!(
            m.classify("POST", "fal-ai/flux/dev"),
            RouteClass::Billable(Pricing::Metered)
        ));
        assert!(matches!(
            m.classify("POST", "fal-ai/nano-banana-2"),
            RouteClass::Billable(Pricing::Metered)
        ));
        assert!(matches!(
            m.classify("POST", "fal-ai/some-brand-new-model"),
            RouteClass::Billable(Pricing::Metered)
        ));
        // Both reads cost nothing and both report on the submit: the
        // status route measures the run, the result route states the
        // billed count.
        assert_eq!(
            m.classify("GET", "fal-ai/flux/requests/req-1/status"),
            RouteClass::Reports
        );
        assert_eq!(m.classify("GET", "fal-ai/kling-video/requests/req-1"), RouteClass::Reports);
        // Request reads report for EVERY app: the app prefix cannot say
        // which variant was submitted (a `fal-ai/flux/requests/..` poll
        // is a dev poll and a schnell poll alike), which is why the
        // model is read off the open charge and not off this path.
        assert_eq!(
            m.classify("GET", "fal-ai/other-app/requests/req-1/status"),
            RouteClass::Reports
        );
        // Wrong verbs and traversal stay Unknown.
        assert_eq!(m.classify("GET", "fal-ai/flux/dev"), RouteClass::Unknown);
        assert_eq!(m.classify("POST", "fal-ai/../flux/dev"), RouteClass::Unknown);
        assert_eq!(m.classify("POST", "fal-ai/flux/dev/requests/%2e%2e"), RouteClass::Unknown);
    }

    #[test]
    fn the_pricing_catalog_answer_parses() {
        let body = json!({
            "has_more": false,
            "next_cursor": null,
            "prices": [
                { "endpoint_id": "fal-ai/nano-banana-2", "unit_price": 0.08,
                  "unit": "images", "currency": "USD" }
            ]
        });
        let (price, unit) = parse_price("fal-ai/nano-banana-2", &body).unwrap();
        assert_eq!((price, unit.as_str()), (0.08, "images"));
        assert!(parse_price("fal-ai/other", &body).is_err());
        assert!(parse_price(
            "fal-ai/nano-banana-2",
            &json!({ "prices": [{ "endpoint_id": "fal-ai/nano-banana-2", "unit": "image" }] })
        )
        .is_err());
    }

    #[test]
    fn a_refused_submit_bills_nothing() {
        let mut obs = FAL.observe("fal-ai/nano-banana-2", "", b"{}");
        obs.on_status(422);
        let observed = obs.end(false);
        assert_eq!(observed.data["accepted"], json!(false));
        let mut obs = FAL.observe("fal-ai/nano-banana-2", "", b"{}");
        obs.on_status(200);
        let observed = obs.end(false);
        assert_eq!(observed.data["accepted"], json!(true));
    }

    fn headers(pairs: &[(&str, &str)]) -> http::HeaderMap {
        let mut h = http::HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                http::HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    /// A submit answers a queue ticket, never a cost: what it keeps is
    /// the request id, which is what ties the later reads back to it.
    #[test]
    fn a_submit_keeps_the_request_id_and_opens_a_charge_under_it() {
        let mut obs = FAL.observe("fal-ai/some-model", "", b"{}");
        obs.on_status(200);
        obs.on_chunk(br#"{"request_id":"req-77","status":"IN_QUEUE"}"#);
        let observed = obs.end(false);
        assert_eq!(observed.data["requestId"].as_str(), Some("req-77"));
        assert_eq!(FAL.opens_charge("fal-ai/some-model", &observed), Some("req-77".to_string()));
    }

    /// A submit fal REFUSED spent nothing, so it opens no charge: there
    /// is nothing for a later read to report on, and `resolve` books the
    /// zero directly.
    #[test]
    fn a_refused_submit_opens_no_charge() {
        let mut obs = FAL.observe("fal-ai/some-model", "", b"{}");
        obs.on_status(422);
        obs.on_chunk(br#"{"detail":"bad input"}"#);
        let observed = obs.end(false);
        assert_eq!(FAL.opens_charge("fal-ai/some-model", &observed), None);
    }

    /// Every read of a request names the charge it reports on, whether
    /// it is the status poll or the result fetch.
    #[test]
    fn a_read_names_the_charge_it_reports_on() {
        let observed = ObservedCall { interrupted: false, status: 200, data: json!({}) };
        assert_eq!(
            FAL.charge_reported_on("fal-ai/flux/requests/req-1/status", &observed),
            Some("req-1".to_string())
        );
        assert_eq!(
            FAL.charge_reported_on("fal-ai/flux/requests/req-1", &observed),
            Some("req-1".to_string())
        );
        // A submit reports on nothing; it IS the call being reported on.
        assert_eq!(FAL.charge_reported_on("fal-ai/flux/dev", &observed), None);
    }

    /// The result fetch is the one response that states fal's own count,
    /// and the status poll is the one that measures the run. Measured
    /// against fal on 2026-09-10: `fal-ai/flux/dev` returns
    /// `x-fal-billable-units: 1`, `fal-ai/fast-sdxl` returns no such
    /// header and reports `metrics.inference_time` instead.
    #[test]
    fn a_read_carries_whichever_figure_fal_reported() {
        let mut obs = FAL.observe("fal-ai/flux/requests/req-1", "", b"");
        obs.on_status(200);
        obs.on_headers(&headers(&[(BILLABLE_UNITS_HEADER, "3.5")]));
        obs.on_chunk(br#"{"images":[]}"#);
        assert_eq!(obs.end(false).data["billableUnits"].as_f64(), Some(3.5));

        let mut obs = FAL.observe("fal-ai/fast-sdxl/requests/req-1/status", "", b"");
        obs.on_status(200);
        obs.on_chunk(br#"{"status":"COMPLETED","metrics":{"inference_time":0.826}}"#);
        let observed = obs.end(false);
        assert_eq!(observed.data["computeSeconds"].as_f64(), Some(0.826));
        assert_eq!(observed.data["jobStatus"].as_str(), Some("COMPLETED"));
    }

    /// The status route never prices the charge, even once it says
    /// COMPLETED. A model billed by an output unit states its count on
    /// the RESULT fetch and nowhere else, so closing on the status read
    /// would read "no count" off the one response that never carries
    /// one and book a real spend as unknown. Caught against live fal:
    /// `fal-ai/flux/dev` finished, the status said COMPLETED, and the
    /// header stating 1 megapixel was still one request away.
    #[tokio::test]
    async fn a_completed_status_read_leaves_the_charge_open_for_the_result() {
        let mut obs = FAL.observe("fal-ai/flux/requests/req-1/status", "", b"");
        obs.on_status(200);
        obs.on_chunk(br#"{"status":"COMPLETED","metrics":{"inference_time":0.47}}"#);
        let observed = obs.end(false);

        let mut scratch = json!({ "model": "fal-ai/flux/dev" });
        let http = reqwest_middleware::ClientBuilder::new(reqwest::Client::new()).build();
        let shared = weft_core::shared::Shared::new(std::time::Duration::MAX);
        let cost = FAL
            .fold_report(
                "fal-ai/flux/requests/req-1/status",
                observed,
                &mut scratch,
                FollowUp { http: &http, base_url: FAL.base_url(), shared: &shared },
            )
            .await;

        assert!(cost.is_none(), "the status read must never close the charge");
        // The run time it measured is kept, because the result fetch
        // does not repeat it and a compute-billed model needs it.
        assert_eq!(scratch["computeSeconds"].as_f64(), Some(0.47));
    }

    /// A header that is absent, unparseable, or nonsense leaves the
    /// billed units unset, so the charge falls through to the run time
    /// rather than inventing a number from the request.
    #[test]
    fn a_missing_or_bad_billable_units_header_sets_nothing() {
        for header in [None, Some("not-a-number"), Some("-2"), Some("NaN")] {
            let mut obs = FAL.observe("fal-ai/some-model/requests/req-1", "", b"");
            obs.on_status(200);
            let sent: Vec<(&str, &str)> =
                header.map(|v| vec![(BILLABLE_UNITS_HEADER, v)]).unwrap_or_default();
            obs.on_headers(&headers(&sent));
            let observed = obs.end(false);
            assert_eq!(observed.data["billableUnits"], Value::Null, "header={header:?}");
        }
    }
}

/// Live checks against fal itself, driving the meter exactly as the
/// harness does. Ignored by default (they spend real money, a few
/// tenths of a cent each); run with a key:
///
/// ```text
/// WEFT_NODE_TEST_FAL_KEY=... cargo test -p weft-providers -- --ignored --nocapture live_fal
/// ```
#[cfg(test)]
mod live_fal {
    use super::*;

    fn signed_client() -> reqwest_middleware::ClientWithMiddleware {
        let key = std::env::var("WEFT_NODE_TEST_FAL_KEY")
            .expect("WEFT_NODE_TEST_FAL_KEY must be set for a live fal check");
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::AUTHORIZATION,
            http::HeaderValue::from_str(&format!("Key {key}")).unwrap(),
        );
        reqwest_middleware::ClientBuilder::new(
            reqwest::Client::builder().default_headers(headers).build().unwrap(),
        )
        .build()
    }

    /// Run one real job and price it the way the harness would: submit,
    /// open the charge, feed every read through `fold_report`, and
    /// report the figure that closes it.
    async fn priced(model: &str, payload: Value) -> MeasuredCost {
        let http = signed_client();
        let shared = weft_core::shared::Shared::new(std::time::Duration::MAX);
        let base = FAL.base_url();

        // The submit, observed as a billable call.
        let resp = http.post(format!("{base}/{model}")).json(&payload).send().await.unwrap();
        let mut obs = FAL.observe(model, "", b"");
        obs.on_status(resp.status().as_u16());
        let body = resp.bytes().await.unwrap();
        obs.on_chunk(&body);
        let submitted = obs.end(false);
        let id = FAL
            .opens_charge(model, &submitted)
            .unwrap_or_else(|| panic!("fal refused the submit: {}", String::from_utf8_lossy(&body)));
        let mut scratch = submitted.data;

        let app: String = model.split('/').take(2).collect::<Vec<_>>().join("/");
        let follow = FollowUp { http: &http, base_url: base, shared: &shared };

        // Poll the status route, folding each read in, exactly as the
        // node's own polling would drive it.
        for _ in 0..120 {
            let path = format!("{app}/requests/{id}/status");
            let resp = http.get(format!("{base}/{path}")).send().await.unwrap();
            let mut obs = FAL.observe(&path, "", b"");
            obs.on_status(resp.status().as_u16());
            obs.on_headers(&convert(resp.headers()));
            let bytes = resp.bytes().await.unwrap();
            obs.on_chunk(&bytes);
            let observed = obs.end(false);
            let done = observed.data["jobStatus"].as_str() == Some("COMPLETED");
            let follow = FollowUp { http: &http, base_url: base, shared: &shared };
            if let Some(cost) = FAL.fold_report(&path, observed, &mut scratch, follow).await {
                return cost;
            }
            if done {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }

        // The result fetch: the read that states fal's own count.
        let path = format!("{app}/requests/{id}");
        let resp = http.get(format!("{base}/{path}")).send().await.unwrap();
        let mut obs = FAL.observe(&path, "", b"");
        obs.on_status(resp.status().as_u16());
        obs.on_headers(&convert(resp.headers()));
        let bytes = resp.bytes().await.unwrap();
        obs.on_chunk(&bytes);
        let observed = obs.end(false);
        FAL.fold_report(&path, observed, &mut scratch, follow)
            .await
            .expect("the result fetch must close the charge")
    }

    fn convert(h: &reqwest::header::HeaderMap) -> http::HeaderMap {
        let mut out = http::HeaderMap::new();
        for (k, v) in h {
            if let (Ok(name), Ok(value)) = (
                http::HeaderName::from_bytes(k.as_str().as_bytes()),
                http::HeaderValue::from_bytes(v.as_bytes()),
            ) {
                out.insert(name, value);
            }
        }
        out
    }

    /// The gate, against the live catalog: a model fal prices is
    /// admitted, and a model it does not is refused before any call
    /// goes out. Costs nothing to run; the catalog query is free.
    #[tokio::test]
    #[ignore = "queries fal's live catalog"]
    async fn the_gate_admits_a_priced_model_and_refuses_an_unpriced_one() {
        let http = signed_client();
        let shared = weft_core::shared::Shared::new(std::time::Duration::MAX);
        let follow = || FollowUp { http: &http, base_url: FAL.base_url(), shared: &shared };

        FAL.priceable("fal-ai/flux/dev", follow())
            .await
            .expect("a model fal prices must be admitted");

        let err = FAL
            .priceable("fal-ai/not-a-real-model-xyz", follow())
            .await
            .expect_err("a model fal does not price must be refused");
        println!("refusal: {err:#}");
        assert!(err.to_string().contains("not-a-real-model-xyz"));
    }

    /// A model fal states a billed count for: the figure comes off the
    /// header on the result fetch.
    #[tokio::test]
    #[ignore = "spends real money at fal"]
    async fn a_unit_billed_model_prices_from_fals_own_count() {
        let cost = priced(
            "fal-ai/flux/dev",
            json!({ "prompt": "a red cube", "image_size": "square", "num_inference_steps": 4 }),
        )
        .await;
        println!("flux/dev -> {cost:#?}");
        assert!(cost.amount_usd.is_some(), "a finished job must have a figure");
        assert!(cost.metadata["billedUnits"].as_f64().unwrap() > 0.0);
    }

    /// A model fal states no count for: the figure comes off the run
    /// time the status route measured, times the per-second price.
    #[tokio::test]
    #[ignore = "spends real money at fal"]
    async fn a_compute_billed_model_prices_from_the_measured_run() {
        let cost = priced(
            "fal-ai/fast-sdxl",
            json!({ "prompt": "a red cube", "image_size": "square", "num_inference_steps": 4 }),
        )
        .await;
        println!("fast-sdxl -> {cost:#?}");
        assert!(cost.amount_usd.is_some(), "a finished job must have a figure");
        assert!(cost.metadata["computeSeconds"].as_f64().unwrap() > 0.0);
    }
}
