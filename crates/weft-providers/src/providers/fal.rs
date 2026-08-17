//! The fal meter.
//!
//! fal serves every model through one queue transport
//! (`https://queue.fal.run/<model>/...`) and reports no cost in its
//! responses. Prices come from fal's OWN pricing catalog
//! (`GET https://api.fal.ai/v1/models/pricing?endpoint_id=<model>`,
//! authenticated with the same credential the call rides, free to
//! query), fetched at call time and cached, so EVERY fal model is
//! covered without a hand-kept rate table.
//!
//! Routes per model: `POST <model>` submits (the actual spend).
//! `GET <app>/requests/<id>[/status]` reads an already-submitted
//! request (where the app is the model's first two segments; the full
//! model id cannot be recovered from it) and is free at fal.
//!
//! The catalog answers a `unit_price` and a `unit` per endpoint; the
//! call's billed quantity is read from the request body per unit kind
//! (images, megapixels, seconds, flat). A unit this meter cannot turn
//! into a quantity refuses loudly (ceiling) or resolves as an honest
//! unknown (settlement); it never guesses. Some models bill high
//! resolutions above the unit price (fal's platform convention:
//! 2K at 1.5x, 4K at 2x); the multiplier is read from the request's
//! `resolution` field so the figure tracks what fal actually charges.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::{
    CallObservation, FollowUp, MeasuredCost, ObservedCall, Pricing, ProviderMeter, RouteClass,
};

/// fal's platform API, where the pricing catalog lives. A provider-owned
/// origin distinct from the queue transport; the same `Key` credential
/// authenticates both.
const FAL_PRICING_URL: &str = "https://api.fal.ai/v1/models/pricing";

/// How long one fetched price serves before it is re-asked. Prices move
/// on the timescale of product launches, not requests; an hour keeps the
/// catalog off the hot path without letting a price change linger.
const PRICE_TTL: Duration = Duration::from_secs(3600);

/// fal caps `num_images` at 16 per request across its image models.
const MAX_IMAGES_PER_REQUEST: f64 = 16.0;

/// The longest single video fal's generation models produce; bounds a
/// `duration` a caller could inflate.
const MAX_VIDEO_SECONDS: f64 = 60.0;

struct CachedPrice {
    unit_price: f64,
    unit: String,
    fetched: Instant,
}

pub struct FalMeter {
    /// Fetched catalog prices per endpoint id, TTL-refreshed. Shared
    /// process-wide (the meter is a `static`), so one fetch serves every
    /// call on the model until the TTL lapses.
    prices: Mutex<BTreeMap<String, CachedPrice>>,
}

pub static FAL: FalMeter = FalMeter { prices: Mutex::new(BTreeMap::new()) };

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

/// Megapixels of one output image for a fal `image_size` value (fal's
/// documented preset dimensions, or an explicit {width,height}), leaning
/// high for anything unrecognized.
fn image_size_megapixels(size: &Value) -> f64 {
    if let (Some(w), Some(h)) = (size["width"].as_f64(), size["height"].as_f64()) {
        return (w * h / 1_000_000.0).max(0.01);
    }
    match size.as_str().unwrap_or("landscape_4_3") {
        "square" => 0.27,               // 512 x 512
        "square_hd" => 1.05,            // 1024 x 1024
        "portrait_4_3" | "landscape_4_3" => 0.79,   // 768 x 1024
        "portrait_16_9" | "landscape_16_9" => 0.59, // 576 x 1024
        _ => 1.05,
    }
}

/// A request's seconds of video: the `duration` field as a number or a
/// `"<n>s"` string. `None` when the request names none (fal applies the
/// model's own default, and this meter does not keep per-model
/// defaults; the caller is asked to pass one).
fn requested_seconds(parsed: &Value) -> Option<f64> {
    match &parsed["duration"] {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim_end_matches('s').parse().ok(),
        _ => None,
    }
}

/// The rate multiplier for the request's `resolution`, per fal's platform
/// convention (0.5K at 0.75x, 1K at 1x, 2K at 1.5x, 4K at 2x; absent means
/// the default 1K). An unrecognized value is refused, never priced at 1x.
fn resolution_multiplier(parsed: &Value) -> anyhow::Result<f64> {
    let Some(res) = parsed["resolution"].as_str() else { return Ok(1.0) };
    match res.to_ascii_uppercase().as_str() {
        "0.5K" | "512P" => Ok(0.75),
        "1K" => Ok(1.0),
        "2K" => Ok(1.5),
        "4K" => Ok(2.0),
        other => anyhow::bail!(
            "resolution '{other}' is not one this meter can price (0.5K/1K/2K/4K)"
        ),
    }
}

/// The number of images a submit asks for (fal's default is 1).
fn requested_images(parsed: &Value) -> f64 {
    parsed["num_images"].as_f64().unwrap_or(1.0).clamp(1.0, MAX_IMAGES_PER_REQUEST)
}

/// The billed quantity of one submit, per the catalog's `unit` (the
/// vocabulary observed on fal's live catalog: "images", "megapixels",
/// "seconds", "videos"; "compute seconds" and "units" also exist but
/// cannot be read from a request). Unit kinds are interpreted
/// generically from the request body; a unit this match does not know
/// is a loud error naming it, so covering a new fal billing unit is one
/// arm here, never a per-model table.
fn quantity_for_unit(unit: &str, parsed: &Value) -> anyhow::Result<f64> {
    match unit {
        "images" => Ok(requested_images(parsed) * resolution_multiplier(parsed)?),
        "megapixels" => Ok(requested_images(parsed) * image_size_megapixels(&parsed["image_size"])),
        "seconds" => requested_seconds(parsed)
            .map(|s| s.clamp(1.0, MAX_VIDEO_SECONDS))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "this model bills per second and the request names no `duration`; \
                     pass an explicit duration so the cost is known"
                )
            }),
        "videos" => Ok(1.0),
        // "compute seconds" (GPU time) and "units" (an opaque fraction)
        // are only knowable after the run; a request cannot bound them.
        other => anyhow::bail!(
            "fal bills this model per '{other}', a quantity that cannot be read from \
             the request"
        ),
    }
}

impl FalMeter {
    /// The model's (unit_price, unit) from fal's pricing catalog, cached
    /// with a TTL. `http` is the meter's signed side-query lane (the same
    /// credential the call rides authenticates the catalog; the query
    /// itself is free).
    async fn price_for(
        &self,
        model: &str,
        http: &reqwest_middleware::ClientWithMiddleware,
    ) -> anyhow::Result<(f64, String)> {
        if let Some(hit) = self.prices.lock().expect("fal price cache lock").get(model) {
            if hit.fetched.elapsed() < PRICE_TTL {
                return Ok((hit.unit_price, hit.unit.clone()));
            }
        }
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
        let (unit_price, unit) = parse_price(model, &body)?;
        self.prices.lock().expect("fal price cache lock").insert(
            model.to_string(),
            CachedPrice { unit_price, unit: unit.clone(), fetched: Instant::now() },
        );
        Ok((unit_price, unit))
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

/// One request's per-call tap. fal answers no figure, so the tap only
/// records the outcome; the model and the request's candidate quantities
/// (one per unit kind, computed from the same bytes fal read) ride the
/// observation so `resolve` can price once the catalog answers the unit.
struct SubmitObservation {
    data: Value,
    status: u16,
}

impl CallObservation for SubmitObservation {
    fn on_status(&mut self, status: u16) {
        self.status = status;
    }

    fn on_chunk(&mut self, _bytes: &[u8]) {}

    fn end(self: Box<Self>, interrupted: bool) -> ObservedCall {
        let mut data = self.data;
        data["accepted"] = json!((200..300).contains(&self.status));
        data["interrupted"] = json!(interrupted);
        ObservedCall { interrupted, status: self.status, data }
    }
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
            // These routes address the app (`owner/name`), which
            // cannot name the variant that was submitted; every such
            // read is free at fal.
            ("GET", false) => RouteClass::Free,
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
        follow_up: FollowUp<'_>,
    ) -> anyhow::Result<f64> {
        let model = model_and_tail(path).map(|(m, _)| m).unwrap_or_default();
        let (unit_price, unit) = self.price_for(model, follow_up.http).await?;
        let parsed: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
        let quantity = quantity_for_unit(&unit, &parsed)
            .map_err(|e| anyhow::anyhow!("cannot bound '{model}': {e}"))?;
        Ok(unit_price * quantity)
    }

    fn observe(&self, path: &str, _query: &str, request_body: &[u8]) -> Box<dyn CallObservation> {
        let model = model_and_tail(path).map(|(m, _)| m).unwrap_or_default();
        let parsed: Value = serde_json::from_slice(request_body).unwrap_or(Value::Null);
        // Every unit kind's quantity, computed NOW from the request bytes
        // (the only input a billing figure may trust); resolve picks the
        // one the catalog's unit names. A quantity that cannot be read
        // rides as null and resolves as an honest unknown.
        let quantities: BTreeMap<&str, Option<f64>> = ["images", "megapixels", "seconds", "videos"]
            .into_iter()
            .map(|u| (u, quantity_for_unit(u, &parsed).ok()))
            .collect();
        Box::new(SubmitObservation {
            data: json!({ "model": model, "quantities": quantities }),
            status: 0,
        })
    }

    async fn resolve(
        &self,
        _path: &str,
        observed: ObservedCall,
        follow_up: FollowUp<'_>,
    ) -> MeasuredCost {
        let mut metadata = observed.data.clone();
        let model = observed.data["model"].as_str().unwrap_or_default().to_string();
        // A refused or cut submit spends nothing at fal (it bills the
        // request when it ACCEPTS it).
        if observed.data["accepted"] != json!(true) {
            metadata["resolution"] = json!("submit not accepted; nothing billed");
            return MeasuredCost { amount_usd: Some(0.0), model: Some(model), metadata };
        }
        let (unit_price, unit) = match self.price_for(&model, follow_up.http).await {
            Ok(price) => price,
            Err(e) => {
                metadata["resolution"] = json!(format!("price lookup failed: {e:#}"));
                return MeasuredCost { amount_usd: None, model: Some(model), metadata };
            }
        };
        let Some(quantity) = observed.data["quantities"][unit.as_str()].as_f64() else {
            metadata["resolution"] =
                json!(format!("the request carries no quantity for billing unit '{unit}'"));
            return MeasuredCost { amount_usd: None, model: Some(model), metadata };
        };
        metadata["resolution"] = json!(format!("{quantity} x ${unit_price} per {unit}"));
        MeasuredCost { amount_usd: Some(unit_price * quantity), model: Some(model), metadata }
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
        assert_eq!(
            m.classify("GET", "fal-ai/flux/requests/req-1/status"),
            RouteClass::Free
        );
        assert_eq!(m.classify("GET", "fal-ai/kling-video/requests/req-1"), RouteClass::Free);
        // Request reads are free for EVERY app: the app prefix cannot
        // say which variant was submitted (a `fal-ai/flux/requests/..`
        // poll is a dev poll and a schnell poll alike), and a status
        // read costs nothing at fal either way.
        assert_eq!(
            m.classify("GET", "fal-ai/other-app/requests/req-1/status"),
            RouteClass::Free
        );
        // Wrong verbs and traversal stay Unknown.
        assert_eq!(m.classify("GET", "fal-ai/flux/dev"), RouteClass::Unknown);
        assert_eq!(m.classify("POST", "fal-ai/../flux/dev"), RouteClass::Unknown);
        assert_eq!(m.classify("POST", "fal-ai/flux/dev/requests/%2e%2e"), RouteClass::Unknown);
    }

    #[test]
    fn quantities_read_per_unit_from_the_request() {
        let q = |unit: &str, body: serde_json::Value| quantity_for_unit(unit, &body);
        // Per image: count times the resolution multiplier.
        assert_eq!(q("images", json!({})).unwrap(), 1.0);
        assert_eq!(q("images", json!({ "num_images": 3 })).unwrap(), 3.0);
        assert_eq!(q("images", json!({ "num_images": 2, "resolution": "4K" })).unwrap(), 4.0);
        assert_eq!(q("images", json!({ "resolution": "2K" })).unwrap(), 1.5);
        assert!(q("images", json!({ "resolution": "8K" })).is_err());
        // Per megapixel: count times the size's megapixels.
        let mp = q("megapixels", json!({ "num_images": 2, "image_size": "square_hd" })).unwrap();
        assert!((mp - 2.0 * 1.05).abs() < 1e-9, "{mp}");
        let mp = q(
            "megapixels",
            json!({ "image_size": { "width": 1000, "height": 500 } }),
        )
        .unwrap();
        assert!((mp - 0.5).abs() < 1e-9, "{mp}");
        // Per second: the explicit duration, either wire spelling; a
        // request naming none refuses rather than guessing a default.
        assert_eq!(q("seconds", json!({ "duration": "6s" })).unwrap(), 6.0);
        assert_eq!(q("seconds", json!({ "duration": 10 })).unwrap(), 10.0);
        assert!(q("seconds", json!({})).is_err());
        // Flat per video.
        assert_eq!(q("videos", json!({})).unwrap(), 1.0);
        // Units a request cannot bound refuse loudly.
        assert!(q("compute seconds", json!({})).is_err());
        assert!(q("units", json!({})).is_err());
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
        assert_eq!(observed.data["quantities"]["images"].as_f64(), Some(1.0));
        assert_eq!(observed.data["quantities"]["seconds"], Value::Null);
    }
}
