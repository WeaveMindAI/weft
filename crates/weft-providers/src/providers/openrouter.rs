//! The OpenRouter meter.
//!
//! Routes (relative to `https://openrouter.ai/api/v1`):
//! - `POST chat/completions` is BILLABLE: OpenRouter reports the charge
//!   natively in `usage.cost` (plus `usage.cost_details.upstream_inference_cost`
//!   when the route is served on the account's own upstream key), delivered
//!   in the response body (non-streaming) or in the final SSE chunk
//!   (streaming). `prepare` forces the `usage: {include: true}` accounting
//!   opt-in so the charge is always reported.
//! - `GET generation` is FREE: it is the cost LOOKUP (`?id=gen-...`). Free
//!   is what makes the double-charge trap structurally impossible: this
//!   meter's own follow-up query and a node re-querying its cost by hand
//!   both ride a route that bills zero.
//! - `GET models` is FREE: the public price catalog.
//!
//! Everything else is Unknown.
//!
//! The price ceiling is minillmlib's own estimator, fed EXCLUSIVELY from
//! the request bytes about to be forwarded: the body IS the JSON minillmlib
//! serialized, so the conversation and the output bounds deserialize
//! straight back out of it. Nothing besides those bytes ever influences the
//! figure, so a caller cannot understate what it is about to spend.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use minillmlib::{
    estimate_cost_usd, CompletionParameters, GeneratorInfo, Message, ProviderSettings,
    ReasoningConfig,
};
use serde_json::{json, Value};

use crate::sse::DataLineScanner;
use crate::{
    CallObservation, FollowUp, MeasuredCost, ObservedCall, Pricing, ProviderMeter, RouteClass,
};

pub struct OpenRouterMeter {
    /// Pooled keyless generators per pricing key: minillmlib caches the
    /// model's published rates on the generator (clones share the cache),
    /// so the price catalog is fetched once per TTL rather than per call.
    generators: Mutex<BTreeMap<String, GeneratorInfo>>,
}

/// A generation's ledger record becomes queryable ~9s after the generation
/// ends, and a CANCELLED generation only after the upstream run finishes on
/// its own (client aborts do not stop it). 25 polls x 1s covers both with
/// margin. The polled route is free, so the retry spends nothing.
const LEDGER_POLLS: u32 = 25;

/// The process-wide OpenRouter meter (its rates pool is shared state, so it
/// lives in a `static`, not a `const`).
pub static OPENROUTER: OpenRouterMeter =
    OpenRouterMeter { generators: Mutex::new(BTreeMap::new()) };

// Self-register into the crate's meter registry: this line is the ONLY thing
// besides the `mod` declaration that adding a provider needs.
crate::register_meter!(OPENROUTER);

impl OpenRouterMeter {
    /// The pooled generator whose price cache serves `model`.
    fn generator_for(&self, model: &str) -> GeneratorInfo {
        let fresh = GeneratorInfo::openrouter(model);
        let mut pool = self.generators.lock().expect("generator pool lock");
        pool.entry(fresh.pricing_key()).or_insert(fresh).clone()
    }
}

/// Everything the ceiling estimate reads, extracted from the request body.
/// Pure, so the extraction is testable without a price catalog: the
/// conversation, the output bounds, and the provider pin, all straight off
/// the wire bytes (the only input a billing figure may trust).
fn ceiling_inputs(
    body: &[u8],
) -> anyhow::Result<(String, Vec<Message>, CompletionParameters, Option<String>)> {
    let parsed: Value = serde_json::from_slice(body)
        .map_err(|e| anyhow::anyhow!("chat/completions body is not JSON: {e}"))?;
    let model = parsed["model"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("chat/completions body names no model"))?
        .to_string();
    // The body is the JSON minillmlib itself serializes, so the
    // conversation deserializes straight back out of it.
    let messages: Vec<Message> = serde_json::from_value(parsed["messages"].clone())
        .map_err(|e| anyhow::anyhow!("chat/completions messages do not parse: {e}"))?;

    // The output bounds the estimator reads: the completion cap (either
    // wire spelling) and the reasoning budget.
    let mut params = CompletionParameters::new();
    if let Some(cap) =
        parsed["max_tokens"].as_u64().or_else(|| parsed["max_completion_tokens"].as_u64())
    {
        params = params.with_max_tokens(u32::try_from(cap).unwrap_or(u32::MAX));
    }
    if !parsed["reasoning"].is_null() {
        let reasoning: ReasoningConfig = serde_json::from_value(parsed["reasoning"].clone())
            .map_err(|e| anyhow::anyhow!("chat/completions reasoning does not parse: {e}"))?;
        params = params.with_reasoning(reasoning);
    }

    // A routing pin with fallbacks off pins the serving provider, and
    // therefore the price; anything looser prices at the dearest endpoint.
    let billing_provider = if parsed["provider"].is_null() {
        None
    } else {
        let routing: ProviderSettings = serde_json::from_value(parsed["provider"].clone())
            .map_err(|e| anyhow::anyhow!("chat/completions provider routing does not parse: {e}"))?;
        routing.billing_provider()
    };
    Ok((model, messages, params, billing_provider))
}

#[async_trait::async_trait]
impl ProviderMeter for OpenRouterMeter {
    fn provider(&self) -> &'static str {
        "openrouter"
    }

    fn base_url(&self) -> &'static str {
        "https://openrouter.ai/api/v1"
    }

    fn classify(&self, method: &str, path: &str) -> RouteClass {
        match (method, path) {
            ("POST", "chat/completions") => RouteClass::Billable(Pricing::Metered),
            ("GET", "generation") => RouteClass::Free,
            ("GET", "models") => RouteClass::Free,
            _ => RouteClass::Unknown,
        }
    }

    fn prepare(&self, path: &str, body: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        // The rewrite below is chat/completions-shaped; running it on any
        // other route would silently mangle a body, so refuse loud (in
        // release too) rather than assume the caller classified first.
        if path != "chat/completions" {
            anyhow::bail!("prepare called on unexpected route '{path}'");
        }
        let mut parsed: Value = serde_json::from_slice(body)
            .map_err(|e| anyhow::anyhow!("chat/completions body is not JSON: {e}"))?;
        // Force the accounting opt-in, overriding whatever the caller set:
        // without it a streaming response carries no cost at all, and the
        // whole point of this meter is that the cost is always reportable.
        parsed["usage"] = json!({ "include": true });
        // Shed the media estimation metadata the caller's wire carried for
        // the ceiling (`ceiling_inputs` has read it by now): OpenRouter
        // tolerates the keys, but internal breadcrumbs have no business
        // riding upstream.
        if let Some(messages) = parsed["messages"].as_array_mut() {
            for message in messages {
                let Some(parts) = message["content"].as_array_mut() else { continue };
                for part in parts {
                    for media_key in ["input_audio", "video_url", "image_url"] {
                        if let Some(media) =
                            part.get_mut(media_key).and_then(|v| v.as_object_mut())
                        {
                            media.remove("duration_secs");
                            media.remove("width");
                            media.remove("height");
                        }
                    }
                }
            }
        }
        Ok(Some(serde_json::to_vec(&parsed)?))
    }

    async fn ceiling_usd(
        &self,
        _path: &str,
        body: &[u8],
        _http: &reqwest::Client,
    ) -> anyhow::Result<f64> {
        let (model, messages, params, billing_provider) = ceiling_inputs(body)?;
        let rates = self
            .generator_for(&model)
            .model_rates_served_by(billing_provider.as_deref())
            .await
            .map_err(|e| anyhow::anyhow!("cannot price model '{model}': {e}"))?;
        Ok(estimate_cost_usd(&messages, &params, &rates))
    }

    fn observe(&self) -> Box<dyn CallObservation> {
        Box::new(OpenRouterObservation::new())
    }

    async fn resolve(&self, observed: ObservedCall, follow_up: FollowUp<'_>) -> MeasuredCost {
        let data = &observed.data;
        let status = data["status"].as_u64().unwrap_or(0);
        let model = data["model"].as_str().map(str::to_string);
        let generation_id = data["id"].as_str().unwrap_or("").to_string();

        // 1. The charge arrived inline (usage accounting): the answer.
        if let Some(amount) = inline_cost(&data["usage"]) {
            return MeasuredCost {
                amount_usd: Some(amount),
                model,
                metadata: json!({
                    "resolution": "inline usage accounting",
                    "generationId": generation_id,
                    "promptTokens": data["usage"]["prompt_tokens"],
                    "completionTokens": data["usage"]["completion_tokens"],
                    "interrupted": observed.interrupted,
                }),
            };
        }

        // 2. A generation exists but its charge did not reach us (the stream
        // was cut before the usage chunk, or the body was too large to
        // parse): ask the ledger. The generation bills whether we hung up or
        // not, so the ledger is the truth; the polled route is free.
        if !generation_id.is_empty() {
            for _ in 0..LEDGER_POLLS {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if let Some((amount, meta)) =
                    query_ledger(&follow_up, &generation_id).await
                {
                    return MeasuredCost {
                        amount_usd: Some(amount),
                        model,
                        metadata: json!({
                            "resolution": "generation ledger",
                            "generationId": generation_id,
                            "interrupted": observed.interrupted,
                            "ledger": meta,
                        }),
                    };
                }
            }
            // The ledger never answered: the cost is genuinely unknown.
            // Record it AS unknown; a fake $0 would silently leak money.
            return MeasuredCost {
                amount_usd: None,
                model,
                metadata: json!({
                    "resolution": "unknown: generation ledger never answered",
                    "generationId": generation_id,
                    "interrupted": observed.interrupted,
                }),
            };
        }

        // 3. No generation was created. A refused call (non-2xx before any
        // generation id) bills nothing; that is a known zero, not an unknown.
        if status != 0 && !(200..300).contains(&(status as u16)) {
            return MeasuredCost {
                amount_usd: Some(0.0),
                model,
                metadata: json!({
                    "resolution": "provider refused the call; nothing billed",
                    "status": status,
                }),
            };
        }

        // 4. Nothing to anchor a lookup on: unknown, said honestly.
        MeasuredCost {
            amount_usd: None,
            model,
            metadata: json!({
                "resolution": "unknown: no usage and no generation id observed",
                "status": status,
                "interrupted": observed.interrupted,
            }),
        }
    }
}

/// OpenRouter's inline charge: `usage.cost` is what OpenRouter charged in
/// credits, and on a BYOK-routed account the real upstream charge sits in
/// `usage.cost_details.upstream_inference_cost` with `cost` at 0. The all-in
/// figure is their sum. `None` when the usage object carries no cost field
/// at all (accounting was not applied).
fn inline_cost(usage: &Value) -> Option<f64> {
    let cost = usage["cost"].as_f64()?;
    Some(cost + usage["cost_details"]["upstream_inference_cost"].as_f64().unwrap_or(0.0))
}

/// One `GET generation?id=` ledger query. `Some((usd, record-extract))` when
/// the record exists and carries a numeric charge; `None` (poll again) on
/// any miss.
async fn query_ledger(follow_up: &FollowUp<'_>, generation_id: &str) -> Option<(f64, Value)> {
    let url = format!(
        "{}/generation?id={}",
        follow_up.base_url.trim_end_matches('/'),
        // The id is provider-issued (`gen-<alnum>`), but it crossed a
        // response body to get here: percent-encode rather than trust it
        // into a URL raw.
        percent_encode(generation_id),
    );
    let response = follow_up
        .http
        .get(&url)
        .header("Authorization", format!("Bearer {}", follow_up.credential))
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    let record: Value = response.json().await.ok()?;
    let data = &record["data"];
    let amount =
        data["total_cost"].as_f64()? + data["upstream_inference_cost"].as_f64().unwrap_or(0.0);
    Some((
        amount,
        json!({
            "tokensPrompt": data["tokens_prompt"],
            "tokensCompletion": data["tokens_completion"],
        }),
    ))
}

fn percent_encode(raw: &str) -> String {
    raw.bytes()
        .flat_map(|b| {
            if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
                vec![b as char]
            } else {
                format!("%{b:02X}").chars().collect()
            }
        })
        .collect()
}

/// Cap on a buffered non-streaming response body. Chat completions are far
/// under this; past it we keep what we have (the id and model live at the
/// front) and resolve through the ledger instead.
const MAX_BUFFERED_BODY: usize = 16 * 1024 * 1024;

/// Per-call response tap. Sniffs the body's first non-whitespace byte to
/// pick a mode: `{` = one JSON document (non-streaming; buffered up to
/// [`MAX_BUFFERED_BODY`]), anything else = SSE (scanned incrementally,
/// tracking the generation id, the model, and the LATEST usage frame; memory
/// stays O(one line) no matter how long the stream runs).
struct OpenRouterObservation {
    status: Option<u16>,
    mode: Mode,
    id: Option<String>,
    model: Option<String>,
    usage: Option<Value>,
    truncated: bool,
}

enum Mode {
    Sniffing,
    Json(Vec<u8>),
    Sse(DataLineScanner),
}

impl OpenRouterObservation {
    fn new() -> Self {
        Self {
            status: None,
            mode: Mode::Sniffing,
            id: None,
            model: None,
            usage: None,
            truncated: false,
        }
    }

    fn ingest_frame(id: &mut Option<String>, model: &mut Option<String>, usage: &mut Option<Value>, frame: &Value) {
        if id.is_none() {
            if let Some(gen_id) = frame["id"].as_str().filter(|s| !s.is_empty()) {
                *id = Some(gen_id.to_string());
            }
        }
        if model.is_none() {
            if let Some(m) = frame["model"].as_str().filter(|s| !s.is_empty()) {
                *model = Some(m.to_string());
            }
        }
        if !frame["usage"].is_null() {
            *usage = Some(frame["usage"].clone());
        }
    }
}

impl CallObservation for OpenRouterObservation {
    fn on_status(&mut self, status: u16) {
        self.status = Some(status);
    }

    fn on_chunk(&mut self, mut bytes: &[u8]) {
        if let Mode::Sniffing = self.mode {
            // Skip leading whitespace while deciding the mode.
            while let Some((first, rest)) = bytes.split_first() {
                if first.is_ascii_whitespace() {
                    bytes = rest;
                } else {
                    break;
                }
            }
            let Some(first) = bytes.first() else { return };
            self.mode = if *first == b'{' {
                Mode::Json(Vec::new())
            } else {
                Mode::Sse(DataLineScanner::new())
            };
        }
        match &mut self.mode {
            Mode::Sniffing => unreachable!("mode decided above when bytes are non-empty"),
            Mode::Json(buffer) => {
                let room = MAX_BUFFERED_BODY.saturating_sub(buffer.len());
                if bytes.len() > room {
                    self.truncated = true;
                }
                buffer.extend_from_slice(&bytes[..bytes.len().min(room)]);
            }
            Mode::Sse(scanner) => {
                let (id, model, usage) = (&mut self.id, &mut self.model, &mut self.usage);
                scanner.feed(bytes, |payload| {
                    if payload == "[DONE]" {
                        return;
                    }
                    if let Ok(frame) = serde_json::from_str::<Value>(payload) {
                        Self::ingest_frame(id, model, usage, &frame);
                    }
                });
            }
        }
    }

    fn end(mut self: Box<Self>, interrupted: bool) -> ObservedCall {
        if let Mode::Json(buffer) = &self.mode {
            match serde_json::from_slice::<Value>(buffer) {
                Ok(body) => {
                    let (id, model, usage) = (&mut self.id, &mut self.model, &mut self.usage);
                    Self::ingest_frame(id, model, usage, &body);
                }
                Err(_) if !self.truncated => {
                    // A complete but unparseable body: nothing to extract;
                    // resolve falls through to status / unknown handling.
                }
                Err(_) => {
                    // Truncated at the cap: the front of the body (id,
                    // model) is still recoverable from the valid prefix.
                    if let Some(front) = std::str::from_utf8(&buffer[..buffer.len().min(4096)]).ok()
                    {
                        let (id, model) = (&mut self.id, &mut self.model);
                        if id.is_none() {
                            if let Some(found) = extract_string_field(front, "id") {
                                *id = Some(found);
                            }
                        }
                        if model.is_none() {
                            if let Some(found) = extract_string_field(front, "model") {
                                *model = Some(found);
                            }
                        }
                    }
                }
            }
        }
        let sse_overflow = matches!(&self.mode, Mode::Sse(s) if s.overflowed());
        ObservedCall {
            interrupted,
            data: serde_json::json!({
                "status": self.status,
                "id": self.id,
                "model": self.model,
                "usage": self.usage,
                "truncated": self.truncated || sse_overflow,
            }),
        }
    }
}

/// Pull `"field": "value"` out of a JSON text prefix that failed to parse as
/// a whole (a truncated buffered body). Textual on purpose: the prefix is
/// not a complete document.
fn extract_string_field(text: &str, field: &str) -> Option<String> {
    let needle = format!("\"{field}\"");
    let at = text.find(&needle)? + needle.len();
    let rest = text[at..].trim_start().strip_prefix(':')?.trim_start();
    let rest = rest.strip_prefix('"')?;
    let end = rest.find('"')?;
    let value = &rest[..end];
    (!value.is_empty() && !value.contains('\\')).then(|| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meter() -> &'static OpenRouterMeter {
        &OPENROUTER
    }

    // ---- L1: route classification -------------------------------------

    #[test]
    fn route_classification() {
        let m = meter();
        assert_eq!(
            m.classify("POST", "chat/completions"),
            RouteClass::Billable(Pricing::Metered)
        );
        assert_eq!(m.classify("GET", "generation"), RouteClass::Free);
        assert_eq!(m.classify("GET", "models"), RouteClass::Free);
        assert_eq!(m.classify("GET", "chat/completions"), RouteClass::Unknown);
        assert_eq!(m.classify("POST", "generation"), RouteClass::Unknown);
        assert_eq!(m.classify("POST", "embeddings"), RouteClass::Unknown);
        assert_eq!(m.classify("POST", ""), RouteClass::Unknown);
    }

    /// The double-charge trap, pinned: the cost LOOKUP is free, so a node
    /// re-querying its own cost is billed zero and the meter's own
    /// follow-up query never bills recursively.
    #[test]
    fn the_cost_lookup_route_is_free_not_billable() {
        assert_eq!(meter().classify("GET", "generation"), RouteClass::Free);
    }

    /// Route matching is a security boundary: traversal, encodings,
    /// userinfo, and backslashes never match a known route; they classify
    /// Unknown (which a managed-key gate refuses).
    #[test]
    fn trick_paths_classify_unknown() {
        let m = meter();
        for path in [
            "../generation",
            "chat/../chat/completions",
            "chat%2fcompletions",
            "%63hat/completions",
            "chat\\completions",
            "generation@evil.example",
            "chat/completions/",
            "/chat/completions",
            "chat/completions%00",
            "GENERATION",
        ] {
            assert_eq!(m.classify("POST", path), RouteClass::Unknown, "path {path:?}");
            assert_eq!(m.classify("GET", path), RouteClass::Unknown, "path {path:?}");
        }
    }

    // ---- L1: prepare forces the accounting opt-in ----------------------

    #[test]
    fn prepare_forces_usage_accounting_even_when_the_caller_opted_out() {
        let body = br#"{"model":"m","messages":[],"usage":{"include":false}}"#;
        let prepared = meter().prepare("chat/completions", body).unwrap().unwrap();
        let parsed: Value = serde_json::from_slice(&prepared).unwrap();
        assert_eq!(parsed["usage"]["include"], true);
        assert_eq!(parsed["model"], "m", "the rest of the body is untouched");

        let garbage = meter().prepare("chat/completions", b"not json");
        assert!(garbage.is_err(), "an unpreparable body is a loud error, never sent unmeasured");

        let wrong_route = meter().prepare("generation", br#"{"model":"m"}"#);
        assert!(wrong_route.is_err(), "prepare on a route it was not written for refuses loud");
    }

    #[test]
    fn percent_encode_covers_the_reserved_characters() {
        assert_eq!(percent_encode("gen-abc_1.2~x"), "gen-abc_1.2~x");
        assert_eq!(percent_encode("gen-a b/c?&#%"), "gen-a%20b%2Fc%3F%26%23%25");
    }

    /// The media estimation metadata the caller's wire carried (for the
    /// price ceiling) is shed before the bytes go upstream; the media
    /// itself is untouched.
    #[test]
    fn prepare_sheds_the_estimation_metadata_before_forwarding() {
        let body = serde_json::json!({
            "model": "m",
            "messages": [{"role": "user", "content": [
                {"type": "input_audio",
                 "input_audio": {"data": "AAAA", "format": "mp3", "duration_secs": 90.0}},
                {"type": "image_url",
                 "image_url": {"url": "https://x/y.png", "width": 800, "height": 600}}
            ]}]
        });
        let prepared = meter()
            .prepare("chat/completions", &serde_json::to_vec(&body).unwrap())
            .unwrap()
            .unwrap();
        let parsed: Value = serde_json::from_slice(&prepared).unwrap();
        let parts = parsed["messages"][0]["content"].as_array().unwrap();
        assert!(parts[0]["input_audio"].get("duration_secs").is_none(), "{parsed}");
        assert_eq!(parts[0]["input_audio"]["format"], "mp3");
        assert!(parts[1]["image_url"].get("width").is_none(), "{parsed}");
        assert_eq!(parts[1]["image_url"]["url"], "https://x/y.png");
    }

    /// Declared media metadata sharpens the ceiling: the same request with
    /// a long declared audio clip must price higher than with a short one.
    #[test]
    fn declared_media_metadata_sharpens_the_ceiling() {
        let body_with = |secs: f64| {
            serde_json::to_vec(&serde_json::json!({
                "model": "m", "max_tokens": 10,
                "messages": [{"role": "user", "content": [
                    {"type": "input_audio",
                     "input_audio": {"data": "AAAA", "format": "mp3", "duration_secs": secs}}
                ]}]
            }))
            .unwrap()
        };
        let rates = minillmlib::ModelRates {
            price: minillmlib::TokenPrice::new(3.0, 15.0),
            max_completion_tokens: Some(1000),
            context_length: 200_000,
        };
        let estimate = |body: &[u8]| {
            let (_, messages, params, _) = ceiling_inputs(body).unwrap();
            estimate_cost_usd(&messages, &params, &rates)
        };
        let short = estimate(&body_with(5.0));
        let long = estimate(&body_with(3600.0));
        assert!(long > short, "long clip {long} must out-price short clip {short}");
    }

    // ---- L2: observation + resolve against recorded real responses -----

    /// A recorded non-streaming chat/completions response (usage accounting
    /// on). The charge is inline: resolve is pure, no follow-up.
    const RECORDED_JSON_RESPONSE: &str = r#"{
        "id": "gen-01JQXK5X3T8Z9Y",
        "provider": "Anthropic",
        "model": "anthropic/claude-sonnet-4.6",
        "object": "chat.completion",
        "created": 1752480000,
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": "Hello there."},
            "finish_reason": "stop"
        }],
        "usage": {
            "prompt_tokens": 12,
            "completion_tokens": 4,
            "total_tokens": 16,
            "cost": 0.000096,
            "is_byok": false,
            "cost_details": {"upstream_inference_cost": null}
        }
    }"#;

    #[tokio::test]
    async fn a_recorded_json_response_resolves_to_its_inline_cost() {
        let mut obs = meter().observe();
        obs.on_status(200);
        // Feed in awkward pieces to prove reassembly.
        let bytes = RECORDED_JSON_RESPONSE.as_bytes();
        obs.on_chunk(&bytes[..7]);
        obs.on_chunk(&bytes[7..200]);
        obs.on_chunk(&bytes[200..]);
        let observed = obs.end(false);

        let http = reqwest::Client::new();
        let follow_up = FollowUp { http: &http, base_url: "http://unused.test", credential: "k" };
        let cost = meter().resolve(observed, follow_up).await;
        assert_eq!(cost.amount_usd, Some(0.000096));
        assert_eq!(cost.model.as_deref(), Some("anthropic/claude-sonnet-4.6"));
        assert_eq!(cost.metadata["resolution"], "inline usage accounting");
    }

    /// A recorded BYOK-routed response: OpenRouter's own charge is 0 and the
    /// real upstream charge sits in cost_details. The all-in figure is the sum.
    #[tokio::test]
    async fn a_byok_routed_response_sums_the_upstream_charge() {
        let body = r#"{
            "id": "gen-2", "model": "openai/gpt-5",
            "choices": [{"message": {"content": "x"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 1, "total_tokens": 6,
                      "cost": 0.0, "cost_details": {"upstream_inference_cost": 0.00042}}
        }"#;
        let mut obs = meter().observe();
        obs.on_status(200);
        obs.on_chunk(body.as_bytes());
        let http = reqwest::Client::new();
        let cost = meter()
            .resolve(obs.end(false), FollowUp { http: &http, base_url: "http://u.test", credential: "k" })
            .await;
        assert_eq!(cost.amount_usd, Some(0.00042));
    }

    /// A recorded streaming response: content chunks fly through, the final
    /// chunk (before [DONE]) carries the usage. The tap reads it in passing.
    #[tokio::test]
    async fn a_recorded_sse_stream_resolves_from_its_final_usage_chunk() {
        let stream = concat!(
            "data: {\"id\":\"gen-3\",\"model\":\"anthropic/claude-sonnet-4.6\",\"choices\":[{\"delta\":{\"content\":\"Hel\"}}]}\n\n",
            "data: {\"id\":\"gen-3\",\"choices\":[{\"delta\":{\"content\":\"lo\"}}]}\n\n",
            "data: {\"id\":\"gen-3\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":9,\"completion_tokens\":2,\"total_tokens\":11,\"cost\":0.000031,\"cost_details\":{\"upstream_inference_cost\":null}}}\n\n",
            "data: [DONE]\n\n",
        );
        let mut obs = meter().observe();
        obs.on_status(200);
        // Split mid-line to prove the scanner reassembles across chunks.
        let bytes = stream.as_bytes();
        obs.on_chunk(&bytes[..40]);
        obs.on_chunk(&bytes[40..90]);
        obs.on_chunk(&bytes[90..]);
        let observed = obs.end(false);

        let http = reqwest::Client::new();
        let cost = meter()
            .resolve(observed, FollowUp { http: &http, base_url: "http://u.test", credential: "k" })
            .await;
        assert_eq!(cost.amount_usd, Some(0.000031));
        assert_eq!(cost.model.as_deref(), Some("anthropic/claude-sonnet-4.6"));
    }

    /// A refused call (non-2xx, no generation minted) is a KNOWN zero.
    #[tokio::test]
    async fn a_refused_call_is_a_known_zero() {
        let mut obs = meter().observe();
        obs.on_status(401);
        obs.on_chunk(br#"{"error":{"message":"invalid key","code":401}}"#);
        let http = reqwest::Client::new();
        let cost = meter()
            .resolve(obs.end(false), FollowUp { http: &http, base_url: "http://u.test", credential: "k" })
            .await;
        assert_eq!(cost.amount_usd, Some(0.0));
    }

    /// An interrupted stream with no id and no usage is an honest UNKNOWN,
    /// never a fake $0.
    #[tokio::test]
    async fn an_unanchored_interrupt_is_unknown_not_zero() {
        let obs = meter().observe();
        let http = reqwest::Client::new();
        let cost = meter()
            .resolve(obs.end(true), FollowUp { http: &http, base_url: "http://u.test", credential: "k" })
            .await;
        assert_eq!(cost.amount_usd, None);
        assert!(cost.metadata["resolution"].as_str().unwrap().starts_with("unknown"));
    }

    // ---- L1: the ceiling inputs come ONLY from the wire bytes ----------

    /// The estimator's inputs are extracted from the request body itself
    /// (the conversation round-trips through minillmlib's own wire shape),
    /// and the figure they produce moves with the declared output cap.
    /// Rates are hand-built, so this is pure.
    #[test]
    fn the_ceiling_reads_the_wire_bytes_and_scales_with_the_declared_cap() {
        let body = serde_json::json!({
            "model": "anthropic/claude-sonnet-4.6",
            "messages": [
                {"role": "system", "content": "You are helpful."},
                {"role": "user", "content": "Hello there, meter."}
            ],
            "max_tokens": 1000,
            "stream": true,
            "usage": {"include": true}
        });
        let (model, messages, params, pin) =
            ceiling_inputs(&serde_json::to_vec(&body).unwrap()).unwrap();
        assert_eq!(model, "anthropic/claude-sonnet-4.6");
        assert_eq!(messages.len(), 2, "the conversation round-trips off the wire");
        assert_eq!(pin, None, "no routing pin on the body");

        let rates = minillmlib::ModelRates {
            price: minillmlib::TokenPrice::new(3.0, 15.0),
            max_completion_tokens: Some(64000),
            context_length: 200000,
        };
        let capped = estimate_cost_usd(&messages, &params, &rates);
        // Without the declared cap, the model's published cap bounds the
        // output side, so the ceiling must be strictly larger.
        let (_, _, uncapped_params, _) = ceiling_inputs(
            &serde_json::to_vec(&serde_json::json!({
                "model": "anthropic/claude-sonnet-4.6",
                "messages": body["messages"],
            }))
            .unwrap(),
        )
        .unwrap();
        let uncapped = estimate_cost_usd(&messages, &uncapped_params, &rates);
        assert!(capped > 0.0, "{capped}");
        assert!(uncapped > capped, "uncapped {uncapped} must exceed capped {capped}");
    }

    /// A routing pin with fallbacks OFF pins the billing provider; anything
    /// looser prices at the dearest endpoint (no pin).
    #[test]
    fn the_billing_pin_requires_fallbacks_off() {
        let pinned = serde_json::json!({
            "model": "m", "messages": [],
            "provider": {"order": ["anthropic"], "allow_fallbacks": false}
        });
        let (_, _, _, pin) = ceiling_inputs(&serde_json::to_vec(&pinned).unwrap()).unwrap();
        assert_eq!(pin.as_deref(), Some("anthropic"));

        let loose = serde_json::json!({
            "model": "m", "messages": [],
            "provider": {"order": ["anthropic"]}
        });
        let (_, _, _, pin) = ceiling_inputs(&serde_json::to_vec(&loose).unwrap()).unwrap();
        assert_eq!(pin, None, "fallbacks allowed = anyone may serve = no price pin");
    }

    /// An unpriceable request is a loud error, never a guess: no model
    /// named, or a conversation that does not parse.
    #[test]
    fn an_unpriceable_call_is_a_loud_error_never_a_guess() {
        assert!(ceiling_inputs(br#"{"messages": []}"#).is_err(), "no model");
        assert!(
            ceiling_inputs(br#"{"model": "m", "messages": "not an array"}"#).is_err(),
            "unparseable conversation"
        );
        assert!(ceiling_inputs(b"not json").is_err());
    }
}
