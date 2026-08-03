//! ElevenLabsTranscribe Node - Transcribe audio using ElevenLabs Scribe v2.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};

/// ElevenLabs STT pricing: USD per hour of audio (Creator plan overage rate: $0.48/hr).
/// Uses overage rate as the worst-case marginal cost per hour.
/// Raw cost; margin is applied downstream by get_user_margin().
const STT_RATE_PER_HOUR: f64 = 0.48;

/// Valid ISO 639-3 language codes accepted by ElevenLabs Scribe v2.
const VALID_LANGUAGE_CODES: &[&str] = &[
    "afr", "amh", "ara", "asm", "ast", "aze", "bak", "bas", "bel", "ben", "bhr", "bod",
    "bos", "bre", "bul", "cat", "ceb", "ces", "chv", "ckb", "cnh", "cre", "cym", "dan",
    "dav", "deu", "div", "dyu", "ell", "eng", "epo", "est", "eus", "fao", "fas", "fil",
    "fin", "fra", "fry", "ful", "gla", "gle", "glg", "guj", "hat", "hau", "heb", "hin",
    "hrv", "hsb", "hun", "hye", "ibo", "ina", "ind", "isl", "ita", "jav", "jpn", "kab",
    "kan", "kas", "kat", "kaz", "kea", "khm", "kin", "kir", "kln", "kmr", "kor", "kur",
    "lao", "lat", "lav", "lij", "lin", "lit", "ltg", "ltz", "lug", "luo", "mal", "mar",
    "mdf", "mhr", "mkd", "mlg", "mlt", "mon", "mri", "mrj", "msa", "mya", "myv", "nan",
    "nep", "nhi", "nld", "nor", "nso", "nya", "oci", "ori", "orm", "oss", "pan", "pol",
    "por", "pus", "quy", "roh", "ron", "rus", "sah", "san", "sat", "sin", "skr", "slk",
    "slv", "smo", "sna", "snd", "som", "sot", "spa", "sqi", "srd", "srp", "sun", "swa",
    "swe", "tam", "tat", "tel", "tgk", "tha", "tig", "tir", "tok", "ton", "tsn", "tuk",
    "tur", "twi", "uig", "ukr", "umb", "urd", "uzb", "vie", "vot", "vro", "wol", "xho",
    "yid", "yor", "yue", "zgh", "zho", "zul", "zza",
];

#[derive(Default)]
pub struct ElevenLabsTranscribeNode;

#[async_trait]
impl Node for ElevenLabsTranscribeNode {
    fn node_type(&self) -> &'static str {
        "ElevenLabsTranscribe"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "ElevenLabs Transcribe",
            inputs: vec![
                PortDef::wired_only("config", "Dict[String, String]", false),
                PortDef::new("audio", "Audio", true),
            ],
            outputs: vec![
                PortDef::new("transcription", "String", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![
                FieldDef::checkbox("diarize"),
                FieldDef::text("language"),
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let audio_url = ctx.input.get("audio")
            .and_then(|v| v.as_object())
            .and_then(|obj| obj.get("url"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if audio_url.is_empty() {
            return NodeResult::failed("Audio input is required (expected a media object with a 'url' field)");
        }

        let config_input = ctx.input.get("config").and_then(|v| v.as_object());
        let api_key_value = config_input
            .and_then(|c| c.get("apiKey"))
            .and_then(|v| v.as_str());
        let resolved = match ctx.resolve_api_key(api_key_value, "elevenlabs") {
            Some(r) => r,
            None => return NodeResult::failed(
                "No ElevenLabs API key available. Connect an ElevenLabsConfig node or set the platform key."
            ),
        };

        let diarize = ctx.config.get("diarize")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let is_public_url = audio_url.starts_with("https://") && !audio_url.contains("localhost");
        let client = reqwest::Client::new();

        let mut form = reqwest::multipart::Form::new()
            .text("model_id", "scribe_v2")
            .text("diarize", diarize.to_string())
            .text("timestamps_granularity", "word");

        if is_public_url {
            form = form.text("cloud_storage_url", audio_url.to_string());
        } else {
            let resp = client.get(audio_url)
                .timeout(std::time::Duration::from_secs(120))
                .send()
                .await
                .map_err(|e| format!("Failed to download audio: {}", e));
            let resp = match resp {
                Ok(r) => r,
                Err(e) => return NodeResult::failed(&e),
            };
            if !resp.status().is_success() {
                return NodeResult::failed(&format!("Audio download failed: {}", resp.status()));
            }
            let bytes = match resp.bytes().await {
                Ok(b) => b,
                Err(e) => return NodeResult::failed(&format!("Failed to read audio bytes: {}", e)),
            };
            let filename = ctx.input.get("audio")
                .and_then(|v| v.get("filename"))
                .and_then(|v| v.as_str())
                .unwrap_or("audio.wav")
                .to_string();
            let file_part = reqwest::multipart::Part::bytes(bytes.to_vec())
                .file_name(filename)
                .mime_str("application/octet-stream")
                .unwrap();
            form = form.part("file", file_part);
        }

        if let Some(lang) = ctx.config.get("language").and_then(|v| v.as_str()) {
            if !lang.is_empty() {
                if !VALID_LANGUAGE_CODES.contains(&lang) {
                    return NodeResult::failed(&format!(
                        "Invalid language code '{}'. Use a valid ISO 639-3 code (e.g. 'eng', 'fra', 'deu') or leave empty for auto-detect.",
                        lang
                    ));
                }
                form = form.text("language_code", lang.to_string());
            }
        }

        let response = client
            .post("https://api.elevenlabs.io/v1/speech-to-text")
            .header("xi-api-key", &resolved.key)
            .multipart(form)
            .send()
            .await;

        let response = match response {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("ElevenLabs STT request failed: {}", e);
                return NodeResult::failed(&format!("ElevenLabs request failed: {}", e));
            }
        };

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            tracing::error!("ElevenLabs STT error ({}): {}", status, body);
            return NodeResult::failed(&format!("ElevenLabs STT error ({}): {}", status, body));
        }

        let result: serde_json::Value = match response.json().await {
            Ok(v) => v,
            Err(e) => return NodeResult::failed(&format!("Failed to parse ElevenLabs response: {}", e)),
        };

        let transcription = result.get("text")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let duration_secs = result.get("words")
            .and_then(|w| w.as_array())
            .and_then(|words| {
                words.iter().rev().find_map(|w| w.get("end").and_then(|e| e.as_f64()))
            })
            .unwrap_or(0.0);

        if duration_secs > 0.0 {
            let duration_hours = duration_secs / 3600.0;
            let cost_usd = duration_hours * STT_RATE_PER_HOUR;
            ctx.report_usage_cost("scribe_v2", "speech_to_text", cost_usd, resolved.is_byok, Some(serde_json::json!({
                "durationSecs": duration_secs,
            }))).await;
        }

        NodeResult::completed(serde_json::json!({
            "transcription": transcription,
        }))
    }
}

register_node!(ElevenLabsTranscribeNode);
