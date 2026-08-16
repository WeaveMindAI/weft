//! The fal meter (image + video generation through fal's queue API).
//!
//! fal serves every model through one queue transport
//! (`https://queue.fal.run/<model>/...`) and reports no cost in its
//! responses, so pricing is a per-model table. Only the models with a
//! price VERIFIED against fal's own model pages (checked 2026-08) are
//! classified; every other model stays Unknown and runs on the
//! caller's own key, unmeasured, until its price is added here. A
//! guessed price would misbill, so absence is deliberate.
//!
//! Routes per model: `POST <model>` submits (the actual spend).
//! `GET <app>/requests/<id>/status` and `GET <app>/requests/<id>`
//! (where the app is the model's first two segments; the full model
//! path answers 405) read an already-submitted request. They are
//! Free for every app: a status read costs nothing at fal, and the
//! app prefix cannot say which variant was submitted anyway.
//!
//! Priced models:
//! - `fal-ai/flux/dev`: $0.075 per megapixel of output.
//! - `fal-ai/flux-pro/kontext`: $0.04 per image.
//! - `fal-ai/veo3/fast`: $0.25 per second of video, $0.40 with audio.
//! - `fal-ai/kling-video/v2.1/standard/image-to-video`: $0.28 for the
//!   first 5 seconds, $0.056 per second beyond.

use serde_json::{json, Value};

use crate::{
    CallObservation, FollowUp, MeasuredCost, ObservedCall, Pricing, ProviderMeter, RouteClass,
};

pub struct FalMeter;

pub static FAL: FalMeter = FalMeter;

crate::register_meter!(FAL);

const FLUX_DEV: &str = "fal-ai/flux/dev";
const KONTEXT: &str = "fal-ai/flux-pro/kontext";
const VEO3_FAST: &str = "fal-ai/veo3/fast";
const KLING_STD_I2V: &str = "fal-ai/kling-video/v2.1/standard/image-to-video";

const FLUX_DEV_USD_PER_MEGAPIXEL: f64 = 0.075;
const KONTEXT_USD_PER_IMAGE: f64 = 0.04;
const VEO3_FAST_USD_PER_SECOND: f64 = 0.25;
const VEO3_FAST_AUDIO_USD_PER_SECOND: f64 = 0.40;
const KLING_STD_FIRST_5S_USD: f64 = 0.28;
const KLING_STD_EXTRA_USD_PER_SECOND: f64 = 0.056;

const PRICED_MODELS: [&str; 4] = [FLUX_DEV, KONTEXT, VEO3_FAST, KLING_STD_I2V];

/// Megapixels of one output image for a flux `image_size` preset
/// (fal's documented preset dimensions), leaning high for anything
/// unrecognized (a custom {width,height} object prices off its own
/// numbers when present).
fn flux_megapixels(size: &Value) -> f64 {
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

/// A request's seconds of video: the `duration` field as a number or
/// a `"<n>s"` string, else the model's default.
fn requested_seconds(parsed: &Value, default: f64) -> f64 {
    match &parsed["duration"] {
        Value::Number(n) => n.as_f64().unwrap_or(default),
        Value::String(s) => s.trim_end_matches('s').parse().unwrap_or(default),
        _ => default,
    }
}

/// A priced submit's cost, from the request alone (fal bills the
/// request when it accepts it; the queue answer carries no figure).
fn submit_usd(model: &str, body: &[u8]) -> Option<f64> {
    let parsed: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    match model {
        FLUX_DEV => {
            let images = parsed["num_images"].as_f64().unwrap_or(1.0).clamp(1.0, 16.0);
            Some(images * flux_megapixels(&parsed["image_size"]) * FLUX_DEV_USD_PER_MEGAPIXEL)
        }
        KONTEXT => {
            let images = parsed["num_images"].as_f64().unwrap_or(1.0).clamp(1.0, 16.0);
            Some(images * KONTEXT_USD_PER_IMAGE)
        }
        VEO3_FAST => {
            let secs = requested_seconds(&parsed, 8.0).clamp(1.0, 60.0);
            let rate = if parsed["generate_audio"].as_bool() == Some(false) {
                VEO3_FAST_USD_PER_SECOND
            } else {
                // Audio defaults on; absence prices at the dearer rate.
                VEO3_FAST_AUDIO_USD_PER_SECOND
            };
            Some(secs * rate)
        }
        KLING_STD_I2V => {
            let secs = requested_seconds(&parsed, 5.0).clamp(1.0, 60.0);
            Some(KLING_STD_FIRST_5S_USD + (secs - 5.0).max(0.0) * KLING_STD_EXTRA_USD_PER_SECOND)
        }
        _ => None,
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

/// One request's per-call tap: the price was settled at submit; a
/// refusal bills nothing.
struct SubmitObservation {
    usd: f64,
    status: u16,
}

impl CallObservation for SubmitObservation {
    fn on_status(&mut self, status: u16) {
        self.status = status;
    }

    fn on_chunk(&mut self, _bytes: &[u8]) {}

    fn end(self: Box<Self>, interrupted: bool) -> ObservedCall {
        let usd = if (200..300).contains(&self.status) { Some(self.usd) } else { Some(0.0) };
        ObservedCall {
            interrupted,
            status: self.status,
            data: json!({ "usd": usd, "interrupted": interrupted }),
        }
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
        let Some((model, tail)) = model_and_tail(path) else { return RouteClass::Unknown };
        match (method, tail.is_empty()) {
            // The submit is the spend.
            ("POST", true) if PRICED_MODELS.contains(&model) => {
                RouteClass::Billable(Pricing::Metered)
            }
            // Status + result reads of an already-submitted request.
            // These routes address the app (`owner/name`), which
            // cannot name the variant that was submitted; every such
            // read is free at fal, priced model or not, so the price
            // table plays no part here.
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
        _http: &reqwest::Client,
    ) -> anyhow::Result<f64> {
        let model = model_and_tail(path).map(|(m, _)| m).unwrap_or_default();
        submit_usd(model, body)
            .ok_or_else(|| anyhow::anyhow!("model '{model}' has no price on this meter"))
    }

    fn observe(&self, path: &str, _query: &str, request_body: &[u8]) -> Box<dyn CallObservation> {
        let model = model_and_tail(path).map(|(m, _)| m).unwrap_or_default();
        let usd = submit_usd(model, request_body)
            .expect("observe is only minted for priced submits");
        Box::new(SubmitObservation { usd, status: 0 })
    }

    async fn resolve(
        &self,
        _path: &str,
        observed: ObservedCall,
        _follow_up: FollowUp<'_>,
    ) -> MeasuredCost {
        MeasuredCost {
            amount_usd: observed.data["usd"].as_f64(),
            model: None,
            metadata: observed.data.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_classify_by_model_and_tail() {
        let m = &FAL;
        assert!(matches!(
            m.classify("POST", FLUX_DEV),
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
        // Unpriced models, wrong verbs, and traversal stay Unknown.
        assert_eq!(m.classify("POST", "fal-ai/some-new-model"), RouteClass::Unknown);
        assert_eq!(m.classify("GET", FLUX_DEV), RouteClass::Unknown);
        assert_eq!(m.classify("POST", "fal-ai/../flux/dev"), RouteClass::Unknown);
        assert_eq!(m.classify("POST", "fal-ai/flux/dev/requests/%2e%2e"), RouteClass::Unknown);
    }

    #[test]
    fn submits_price_from_the_request() {
        let usd = |model: &str, body: serde_json::Value| {
            submit_usd(model, &serde_json::to_vec(&body).unwrap()).unwrap()
        };
        // Two square_hd flux images: 2 * 1.05 MP * $0.075.
        let flux = usd(FLUX_DEV, json!({ "num_images": 2, "image_size": "square_hd" }));
        assert!((flux - 2.0 * 1.05 * 0.075).abs() < 1e-9, "{flux}");
        // Kontext is flat per image.
        assert!((usd(KONTEXT, json!({})) - 0.04).abs() < 1e-9);
        // Veo3 fast: audio defaults dearer; 8s default duration.
        assert!((usd(VEO3_FAST, json!({})) - 8.0 * 0.40).abs() < 1e-9);
        assert!(
            (usd(VEO3_FAST, json!({ "duration": "6s", "generate_audio": false }))
                - 6.0 * 0.25)
                .abs()
                < 1e-9
        );
        // Kling: $0.28 covers 5s, extra seconds at $0.056.
        assert!((usd(KLING_STD_I2V, json!({ "duration": "10" })) - (0.28 + 5.0 * 0.056)).abs()
            < 1e-9);
    }

    #[test]
    fn a_refused_submit_bills_nothing() {
        let mut obs = FAL.observe(KONTEXT, "", b"{}");
        obs.on_status(422);
        assert_eq!(obs.end(false).data["usd"].as_f64(), Some(0.0));
        let mut obs = FAL.observe(KONTEXT, "", b"{}");
        obs.on_status(200);
        assert_eq!(obs.end(false).data["usd"].as_f64(), Some(0.04));
    }
}
