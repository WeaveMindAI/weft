//! The ElevenLabs meter.
//!
//! Routes (relative to `https://api.elevenlabs.io/v1`):
//! - `speech-to-text/realtime` is a BILLABLE SESSION: the realtime
//!   transcription WebSocket (Scribe v2 realtime). ElevenLabs prices it by
//!   the DURATION OF INPUT AUDIO ($0.39 per hour, plus $0.07/h with entity
//!   detection and $0.05/h with keyterm prompting), so the session observer
//!   accrues cost from the audio chunks the CALLER sends: each
//!   `input_audio_chunk` frame carries base64 PCM whose byte count, at the
//!   session's negotiated audio format, IS a duration. Frames from the
//!   provider (transcripts) cost nothing.
//!
//! Everything else is Unknown: the platform key refuses it, and a caller's
//! own key passes it through unmeasured, exactly like any unknown route.
//!
//! The audio format (and with it bytes-per-second) is read from the
//! session URL's query string, the same parameters the provider itself
//! reads to configure the session, so the observer and the provider price
//! the same bytes the same way. A frame that does not parse as an audio
//! chunk accrues nothing (the provider ignores or refuses it too).

use serde_json::{json, Value};

use crate::{
    CallObservation, FollowUp, MeasuredCost, ProviderMeter, RouteClass, SessionObservation,
};

pub struct ElevenLabsMeter;

pub static ELEVENLABS: ElevenLabsMeter = ElevenLabsMeter;

crate::register_meter!(ELEVENLABS);

/// The realtime transcription route, relative to the base.
const REALTIME_STT: &str = "speech-to-text/realtime";

/// Published rates, USD per hour of input audio.
const REALTIME_STT_USD_PER_HOUR: f64 = 0.39;
const ENTITY_DETECTION_USD_PER_HOUR: f64 = 0.07;
const KEYTERMS_USD_PER_HOUR: f64 = 0.05;

/// One admission slice: one minute of audio worth at the session's rate.
const SECONDS_OF_AUDIO_PER_SLICE: f64 = 60.0;

/// The dearest bytes-per-second any format this meter knows can carry
/// (pcm_48000, 16-bit mono), for sizing worst-case per-frame bounds.
const DEAREST_BYTES_PER_SECOND: f64 = 96_000.0;

/// Bytes per second of audio for a declared format, or `None` for a format
/// this meter does not know (refused at session start rather than guessed).
/// `pcm_<rate>` is 16-bit mono (2 bytes per sample); `ulaw_8000` is 1 byte
/// per sample. A rate that is not a finite positive number is refused the
/// same way: it could never price bytes honestly.
fn bytes_per_second(audio_format: &str) -> Option<f64> {
    match audio_format {
        "ulaw_8000" => Some(8_000.0),
        _ => {
            let rate: f64 = audio_format.strip_prefix("pcm_")?.parse().ok()?;
            if !rate.is_finite() || rate <= 0.0 {
                return None;
            }
            Some(rate * 2.0)
        }
    }
}

/// The session's effective rate in USD per hour of input audio, from the
/// session URL's query parameters (the add-ons are opt-in query flags).
fn usd_per_hour(query: &str) -> f64 {
    let mut rate = REALTIME_STT_USD_PER_HOUR;
    for (k, v) in url::form_urlencoded::parse(query.as_bytes()) {
        match (&*k, &*v) {
            ("entity_detection", "true") => rate += ENTITY_DETECTION_USD_PER_HOUR,
            ("keyterms", v) if !v.is_empty() => rate += KEYTERMS_USD_PER_HOUR,
            _ => {}
        }
    }
    rate
}

fn query_param(query: &str, name: &str) -> Option<String> {
    url::form_urlencoded::parse(query.as_bytes())
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.into_owned())
}

/// The number of bytes a base64 string decodes to, from its length and
/// padding alone (no allocation; the audio itself is never needed).
/// Handles padded and unpadded forms; `None` for a length no base64
/// value can have.
fn base64_decoded_len(b64: &str) -> Option<u64> {
    let padding = b64.bytes().rev().take_while(|b| *b == b'=').count() as u64;
    let significant = (b64.len() as u64).saturating_sub(padding);
    let groups = significant / 4;
    let rem = significant % 4;
    if rem == 1 {
        return None;
    }
    Some(groups * 3 + rem.saturating_sub(1))
}

// ── Batch (one-shot) routes ─────────────────────────────────────────
//
// Published API rates (elevenlabs.io/pricing/api, checked 2026-08):
// TTS $0.10 per 1k characters (flash/turbo models $0.05), sound
// effects / voice changer / voice isolator $0.12 per minute of audio,
// music $0.15 per minute, dubbing $0.50 per minute (no watermark),
// batch transcription (Scribe, `speech-to-text`) $0.22 per hour of
// input audio, forced alignment billed the same way.
const TTS_USD_PER_1K_CHARS: f64 = 0.10;
const TTS_FLASH_USD_PER_1K_CHARS: f64 = 0.05;
const SOUND_EFFECT_USD_PER_MINUTE: f64 = 0.12;
const VOICE_CHANGER_USD_PER_MINUTE: f64 = 0.12;
const ISOLATOR_USD_PER_MINUTE: f64 = 0.12;
const MUSIC_USD_PER_MINUTE: f64 = 0.15;
const DUBBING_USD_PER_MINUTE: f64 = 0.50;
const ALIGN_USD_PER_HOUR: f64 = 0.22;
const STT_USD_PER_HOUR: f64 = 0.22;

/// The worst (most minutes per byte) plausible input audio: 32 kbps
/// mono mp3 = 240 KB per minute. Input-priced routes only see the
/// upload's byte count, so this converts it to a LEAN-HIGH duration
/// bound (a wav overshoots, which is the safe direction for a bound).
const INPUT_BYTES_PER_MINUTE: f64 = 240_000.0;

/// The streaming TTS websocket (`text-to-speech/{voice}/stream-input`):
/// a billable session priced per character of TEXT the caller sends,
/// at the model's batch TTS rate. Returns the voice segment when the
/// path is exactly that shape.
fn streaming_tts_voice(path: &str) -> Option<&str> {
    let rest = path.strip_prefix("text-to-speech/")?;
    let (voice, tail) = rest.split_once('/')?;
    (tail == "stream-input" && one_segment(voice)).then_some(voice)
}

/// The per-character rate for a TTS model id (flash/turbo at the
/// cheaper rate, everything else at the standard one).
fn tts_rate_of(model: Option<&str>) -> f64 {
    let model = model.unwrap_or("");
    if model.contains("flash") || model.contains("turbo") {
        TTS_FLASH_USD_PER_1K_CHARS
    } else {
        TTS_USD_PER_1K_CHARS
    }
}

/// One admission slice / max frame for the streaming TTS session: a
/// 4 KB text frame carries at most ~4k characters, well under the
/// slice's worth at the dearest rate.
const STREAMING_TTS_SLICE_USD: f64 = 0.50;
const STREAMING_TTS_MAX_FRAME_BYTES: usize = 4096;

/// The streaming TTS session tap: characters of text the CALLER
/// sends accrue at the model's rate; audio frames back cost nothing.
struct StreamingTtsSession {
    usd_per_1k_chars: f64,
    model: Option<String>,
    chars: u64,
}

impl SessionObservation for StreamingTtsSession {
    fn on_frame_to_provider(&mut self, payload: &[u8]) {
        // Only the `text` field carries billable characters; the
        // opener's lone space and the empty closer round to nothing
        // anyway, so no special-casing.
        let Ok(msg) = serde_json::from_slice::<Value>(payload) else { return };
        if let Some(text) = msg["text"].as_str() {
            self.chars += text.chars().count() as u64;
        }
    }

    fn on_frame_to_caller(&mut self, _payload: &[u8]) {}

    fn accrued_usd(&self) -> f64 {
        self.chars as f64 / 1000.0 * self.usd_per_1k_chars
    }

    fn end(self: Box<Self>, interrupted: bool) -> MeasuredCost {
        MeasuredCost {
            amount_usd: Some(self.accrued_usd()),
            model: self.model.clone(),
            metadata: json!({
                "characters": self.chars,
                "usdPer1kChars": self.usd_per_1k_chars,
                "interrupted": interrupted,
            }),
        }
    }
}

/// One clean URL segment (a voice id): no traversal, no separators.
fn one_segment(s: &str) -> bool {
    !s.is_empty()
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// The batch route families this meter prices. Voice management
/// (`voices/add`, listing) is free; the voice-DESIGN routes
/// (`text-to-voice/...`) stay Unknown deliberately: their credit
/// price is not published in USD, and a guessed figure would misbill,
/// so they run on the caller's own key only until priced.
enum BatchRoute {
    Tts,
    Sts,
    Isolation,
    SoundEffect,
    Music,
    Dub,
    Align,
    /// Batch transcription (`speech-to-text`): the upload's duration
    /// at the Scribe hourly rate, priced like alignment.
    Stt,
    Free,
}

fn batch_route(method: &str, path: &str) -> Option<BatchRoute> {
    if method == "GET" {
        // Cost-free reads: the pickers' lists, the account probe, and
        // a dub's status + already-paid audio download.
        if path == "voices" || path == "models" || path == "user" {
            return Some(BatchRoute::Free);
        }
        if let Some(rest) = path.strip_prefix("dubbing/") {
            let mut parts = rest.split('/');
            let id_ok = parts.next().is_some_and(one_segment);
            let tail_ok = match (parts.next(), parts.next(), parts.next()) {
                (None, ..) => true,
                (Some("audio"), Some(lang), None) => one_segment(lang),
                _ => false,
            };
            if id_ok && tail_ok {
                return Some(BatchRoute::Free);
            }
        }
        return None;
    }
    if method != "POST" {
        return None;
    }
    if let Some(voice) = path.strip_prefix("text-to-speech/") {
        return one_segment(voice).then_some(BatchRoute::Tts);
    }
    if let Some(voice) = path.strip_prefix("speech-to-speech/") {
        return one_segment(voice).then_some(BatchRoute::Sts);
    }
    match path {
        "audio-isolation" => Some(BatchRoute::Isolation),
        "sound-generation" => Some(BatchRoute::SoundEffect),
        "music" => Some(BatchRoute::Music),
        "dubbing" => Some(BatchRoute::Dub),
        "forced-alignment" => Some(BatchRoute::Align),
        "speech-to-text" => Some(BatchRoute::Stt),
        // `voices/add` (and the text-to-voice mint) create ACCOUNT
        // ASSETS: a voice minted through a credential lands in that
        // credential's account. Unknown keeps the runtime credential
        // off these routes; a user's own key passes as always.
        _ => None,
    }
}

/// A TTS request's price: its text's characters at the model's rate.
/// An unparseable body prices as zero characters (the provider would
/// refuse it too).
fn tts_usd(body: &[u8]) -> f64 {
    let parsed: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let chars = parsed["text"].as_str().map(|t| t.chars().count()).unwrap_or(0) as f64;
    let model = parsed["model_id"].as_str().unwrap_or("");
    let rate = if model.contains("flash") || model.contains("turbo") {
        TTS_FLASH_USD_PER_1K_CHARS
    } else {
        TTS_USD_PER_1K_CHARS
    };
    chars / 1000.0 * rate
}

/// A lean-high minutes bound for an input-priced route, from the
/// request's byte count (a multipart upload is dominated by the audio
/// bytes). Floored at one minute: these routes bill a minimum.
fn input_minutes(body: &[u8]) -> f64 {
    (body.len() as f64 / INPUT_BYTES_PER_MINUTE).max(1.0)
}

/// Bytes per second of OUTPUT audio for the request's declared
/// `output_format` (absent = the API default, mp3_44100_128).
/// `mp3_<rate>_<kbps>` is kbps/8*1000; pcm/ulaw as in the realtime
/// math. An unknown format prices at the default's rate.
fn output_bytes_per_second(query: &str) -> f64 {
    let format =
        query_param(query, "output_format").unwrap_or_else(|| "mp3_44100_128".to_string());
    if let Some(rest) = format.strip_prefix("mp3_") {
        if let Some(kbps) = rest.split('_').nth(1).and_then(|k| k.parse::<f64>().ok()) {
            if kbps > 0.0 && kbps.is_finite() {
                return kbps * 1000.0 / 8.0;
            }
        }
        return 128_000.0 / 8.0;
    }
    bytes_per_second(&format).unwrap_or(16_000.0)
}

/// How one batch call's cost resolves; minted per route by `observe`.
enum BatchPricing {
    /// Settled from the request the moment the provider accepts.
    FromRequest { usd: f64 },
    /// The answered audio's bytes, at the format's byte rate, are the
    /// billed duration.
    OutputMinutes { bytes_per_second: f64, usd_per_minute: f64 },
    /// A dub bills by the RESPONSE's own `expected_duration_sec`.
    DubbedMinutes,
}

/// The per-call tap for the batch routes: counts (or, for a dub,
/// collects) the response and closes into the priced figure. The
/// small JSON envelope in `end` is what `resolve` reads.
struct BatchObservation {
    pricing: BatchPricing,
    status: u16,
    /// Collected body, only for `DubbedMinutes` (a small JSON answer).
    body: Vec<u8>,
    body_bytes: u64,
}

impl CallObservation for BatchObservation {
    fn on_status(&mut self, status: u16) {
        self.status = status;
    }

    fn on_chunk(&mut self, bytes: &[u8]) {
        self.body_bytes += bytes.len() as u64;
        if matches!(self.pricing, BatchPricing::DubbedMinutes) {
            self.body.extend_from_slice(bytes);
        }
    }

    fn end(self: Box<Self>, interrupted: bool) -> crate::ObservedCall {
        let success = (200..300).contains(&self.status);
        let usd: Option<f64> = if !success {
            // A refused call bills nothing.
            Some(0.0)
        } else {
            match &self.pricing {
                BatchPricing::FromRequest { usd } => Some(*usd),
                // A cut output stream under-measures the audio the
                // provider generated and billed: honestly unknown.
                BatchPricing::OutputMinutes { .. } if interrupted => None,
                BatchPricing::OutputMinutes { bytes_per_second, usd_per_minute } => {
                    let minutes = self.body_bytes as f64 / bytes_per_second / 60.0;
                    Some(minutes * usd_per_minute)
                }
                BatchPricing::DubbedMinutes => {
                    let parsed: Value =
                        serde_json::from_slice(&self.body).unwrap_or(Value::Null);
                    parsed["expected_duration_sec"]
                        .as_f64()
                        .map(|secs| secs / 60.0 * DUBBING_USD_PER_MINUTE)
                }
            }
        };
        crate::ObservedCall {
            interrupted,
            status: self.status,
            data: json!({
                "usd": usd,
                "outputBytes": self.body_bytes,
                "interrupted": interrupted,
            }),
        }
    }
}

struct RealtimeSttSession {
    bytes_per_second: f64,
    usd_per_hour: f64,
    model: Option<String>,
    audio_bytes: u64,
}

impl SessionObservation for RealtimeSttSession {
    fn on_frame_to_provider(&mut self, payload: &[u8]) {
        // Only audio chunks carry billable duration; anything else (a
        // commit control, malformed bytes) accrues nothing.
        let Ok(msg) = serde_json::from_slice::<Value>(payload) else { return };
        if msg["message_type"] != "input_audio_chunk" {
            return;
        }
        if let Some(b64) = msg["audio_base_64"].as_str() {
            if let Some(n) = base64_decoded_len(b64) {
                self.audio_bytes += n;
            }
        }
    }

    fn on_frame_to_caller(&mut self, _payload: &[u8]) {}

    fn accrued_usd(&self) -> f64 {
        let audio_seconds = self.audio_bytes as f64 / self.bytes_per_second;
        audio_seconds / 3600.0 * self.usd_per_hour
    }

    fn end(self: Box<Self>, interrupted: bool) -> MeasuredCost {
        let audio_seconds = self.audio_bytes as f64 / self.bytes_per_second;
        MeasuredCost {
            amount_usd: Some(self.accrued_usd()),
            model: self.model.clone(),
            metadata: json!({
                "audioSeconds": audio_seconds,
                "usdPerHour": self.usd_per_hour,
                "interrupted": interrupted,
            }),
        }
    }
}

#[async_trait::async_trait]
impl ProviderMeter for ElevenLabsMeter {
    fn service(&self) -> &'static str {
        "elevenlabs"
    }

    fn base_url(&self) -> &'static str {
        "https://api.elevenlabs.io/v1"
    }

    fn classify(&self, method: &str, path: &str) -> RouteClass {
        if path == REALTIME_STT || streaming_tts_voice(path).is_some() {
            return RouteClass::BillableSession;
        }
        match batch_route(method, path) {
            Some(BatchRoute::Free) => RouteClass::Free,
            Some(_) => RouteClass::Billable(crate::Pricing::Metered),
            None => RouteClass::Unknown,
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
        // Every billable batch route prices off its REQUEST; the same
        // math the observer runs is the ceiling (plus the output-priced
        // routes' generous size-based bound).
        match batch_route("POST", path) {
            Some(BatchRoute::Tts) => Ok(tts_usd(body)),
            Some(BatchRoute::SoundEffect) => {
                let parsed: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
                let secs = parsed["duration_seconds"].as_f64().unwrap_or(30.0).clamp(0.5, 30.0);
                Ok(secs / 60.0 * SOUND_EFFECT_USD_PER_MINUTE)
            }
            Some(BatchRoute::Music) => {
                let parsed: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
                let secs = parsed["music_length_ms"]
                    .as_f64()
                    .map(|ms| ms / 1000.0)
                    .unwrap_or(600.0)
                    .clamp(3.0, 600.0);
                Ok(secs / 60.0 * MUSIC_USD_PER_MINUTE)
            }
            Some(BatchRoute::Sts) => Ok(input_minutes(body) * VOICE_CHANGER_USD_PER_MINUTE),
            Some(BatchRoute::Isolation) => Ok(input_minutes(body) * ISOLATOR_USD_PER_MINUTE),
            Some(BatchRoute::Dub) => Ok(input_minutes(body) * DUBBING_USD_PER_MINUTE),
            Some(BatchRoute::Align) => Ok(input_minutes(body) / 60.0 * ALIGN_USD_PER_HOUR),
            Some(BatchRoute::Stt) => Ok(input_minutes(body) / 60.0 * STT_USD_PER_HOUR),
            _ => anyhow::bail!("'{path}' has no pre-call price on this meter"),
        }
    }

    fn observe(&self, path: &str, query: &str, request_body: &[u8]) -> Box<dyn CallObservation> {
        let pricing = match batch_route("POST", path) {
            // The whole cost is a function of the request: settled the
            // moment the provider accepts the call.
            Some(BatchRoute::Tts) => BatchPricing::FromRequest { usd: tts_usd(request_body) },
            Some(BatchRoute::Dub) => BatchPricing::DubbedMinutes,
            Some(BatchRoute::Align) => BatchPricing::FromRequest {
                usd: input_minutes(request_body) / 60.0 * ALIGN_USD_PER_HOUR,
            },
            Some(BatchRoute::Stt) => BatchPricing::FromRequest {
                usd: input_minutes(request_body) / 60.0 * STT_USD_PER_HOUR,
            },
            // Output-priced audio: the answered bytes, at the output
            // format's byte rate, are the duration.
            Some(BatchRoute::SoundEffect) => BatchPricing::OutputMinutes {
                bytes_per_second: output_bytes_per_second(query),
                usd_per_minute: SOUND_EFFECT_USD_PER_MINUTE,
            },
            Some(BatchRoute::Music) => BatchPricing::OutputMinutes {
                bytes_per_second: output_bytes_per_second(query),
                usd_per_minute: MUSIC_USD_PER_MINUTE,
            },
            Some(BatchRoute::Sts) => BatchPricing::OutputMinutes {
                bytes_per_second: output_bytes_per_second(query),
                usd_per_minute: VOICE_CHANGER_USD_PER_MINUTE,
            },
            Some(BatchRoute::Isolation) => BatchPricing::OutputMinutes {
                bytes_per_second: output_bytes_per_second(query),
                usd_per_minute: ISOLATOR_USD_PER_MINUTE,
            },
            _ => unreachable!("observe is only minted for Billable routes"),
        };
        Box::new(BatchObservation { pricing, status: 0, body: Vec::new(), body_bytes: 0 })
    }

    fn observe_session(
        &self,
        path: &str,
        query: &str,
    ) -> anyhow::Result<Box<dyn SessionObservation>> {
        if streaming_tts_voice(path).is_some() {
            return Ok(Box::new(StreamingTtsSession {
                usd_per_1k_chars: tts_rate_of(query_param(query, "model_id").as_deref()),
                model: query_param(query, "model_id"),
                chars: 0,
            }));
        }
        anyhow::ensure!(path == REALTIME_STT, "'{path}' is not a session route");
        let format =
            query_param(query, "audio_format").unwrap_or_else(|| "pcm_16000".to_string());
        let bytes_per_second = bytes_per_second(&format).ok_or_else(|| {
            anyhow::anyhow!("unknown audio_format '{format}'; this meter cannot price it")
        })?;
        Ok(Box::new(RealtimeSttSession {
            bytes_per_second,
            usd_per_hour: usd_per_hour(query),
            model: query_param(query, "model_id"),
            audio_bytes: 0,
        }))
    }

    fn session_slice_usd(&self, path: &str) -> anyhow::Result<f64> {
        if streaming_tts_voice(path).is_some() {
            return Ok(STREAMING_TTS_SLICE_USD);
        }
        anyhow::ensure!(path == REALTIME_STT, "'{path}' is not a session route");
        // One minute of audio worth, at the dearest rate the session could
        // negotiate (both add-ons on): a slice must never under-carve.
        let dearest =
            REALTIME_STT_USD_PER_HOUR + ENTITY_DETECTION_USD_PER_HOUR + KEYTERMS_USD_PER_HOUR;
        Ok(SECONDS_OF_AUDIO_PER_SLICE / 3600.0 * dearest)
    }

    fn session_max_frame_bytes(&self, path: &str) -> anyhow::Result<usize> {
        if streaming_tts_voice(path).is_some() {
            // A text frame's characters are at most its bytes, so this
            // bound keeps one frame's accrual under the slice at the
            // dearest per-character rate.
            return Ok(STREAMING_TTS_MAX_FRAME_BYTES);
        }
        anyhow::ensure!(path == REALTIME_STT, "'{path}' is not a session route");
        // One slice of audio at the dearest format's byte rate, expanded
        // to its base64 wire form, plus a small JSON envelope allowance:
        // a single admitted frame can never accrue more than one slice.
        let raw = SECONDS_OF_AUDIO_PER_SLICE * DEAREST_BYTES_PER_SECOND;
        Ok((raw * 4.0 / 3.0).ceil() as usize + 1024)
    }

    async fn resolve(
        &self,
        _path: &str,
        observed: crate::ObservedCall,
        _follow_up: FollowUp<'_>,
    ) -> MeasuredCost {
        // The observation already computed the figure (see
        // `BatchObservation::end`); a refused call bills nothing, an
        // interrupted output-priced call is honestly unknown.
        let amount = observed.data["usd"].as_f64();
        MeasuredCost {
            amount_usd: amount,
            model: observed.data["model"].as_str().map(str::to_string),
            metadata: observed.data.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;

    fn chunk(bytes: usize) -> Vec<u8> {
        let b64 = base64::engine::general_purpose::STANDARD.encode(vec![0u8; bytes]);
        serde_json::to_vec(&json!({
            "message_type": "input_audio_chunk",
            "audio_base_64": b64,
        }))
        .unwrap()
    }

    #[test]
    fn audio_duration_prices_the_session() {
        // 60 seconds of pcm_16000 (32000 B/s) at $0.39/h.
        let mut obs = ELEVENLABS
            .observe_session(REALTIME_STT, "model_id=scribe_v2_realtime&audio_format=pcm_16000")
            .unwrap();
        obs.on_frame_to_provider(&chunk(32_000 * 60));
        let expected = 60.0 / 3600.0 * 0.39;
        assert!((obs.accrued_usd() - expected).abs() < 1e-9, "{}", obs.accrued_usd());
        // Transcripts back cost nothing; junk frames cost nothing.
        obs.on_frame_to_caller(br#"{"message_type":"partial_transcript","text":"hi"}"#);
        obs.on_frame_to_provider(b"not json");
        assert!((obs.accrued_usd() - expected).abs() < 1e-9);
        let cost = obs.end(false);
        assert_eq!(cost.amount_usd, Some(expected));
        assert_eq!(cost.model.as_deref(), Some("scribe_v2_realtime"));
        assert_eq!(cost.metadata["audioSeconds"], serde_json::json!(60.0));
    }

    #[test]
    fn base64_length_math_matches_real_decoding() {
        for n in [0usize, 1, 2, 3, 4, 5, 31_999, 32_000] {
            let b64 = base64::engine::general_purpose::STANDARD.encode(vec![7u8; n]);
            assert_eq!(base64_decoded_len(&b64), Some(n as u64), "padded n={n}");
            let b64 = base64::engine::general_purpose::STANDARD_NO_PAD.encode(vec![7u8; n]);
            assert_eq!(base64_decoded_len(&b64), Some(n as u64), "unpadded n={n}");
        }
        // Degenerate inputs measure as invalid or empty, never panic.
        assert_eq!(base64_decoded_len(""), Some(0));
        assert_eq!(base64_decoded_len("="), Some(0));
        assert_eq!(base64_decoded_len("=="), Some(0));
        assert_eq!(base64_decoded_len("===="), Some(0));
        assert_eq!(base64_decoded_len("A"), None, "one significant char is not base64");
    }

    #[test]
    fn a_degenerate_audio_rate_is_refused_at_session_start() {
        for format in ["pcm_0", "pcm_-16000", "pcm_inf", "pcm_NaN"] {
            let err = ELEVENLABS
                .observe_session(REALTIME_STT, &format!("audio_format={format}"))
                .err()
                .unwrap_or_else(|| panic!("'{format}' must be refused"));
            assert!(err.to_string().contains("audio_format"), "{err:#}");
        }
    }

    #[test]
    fn the_max_frame_bound_covers_one_slice_of_the_dearest_format() {
        // 60s * 96000 B/s, base64-expanded, plus the envelope allowance:
        // just under 8 MB. Pin the order of magnitude, not the digits.
        let bound = ELEVENLABS.session_max_frame_bytes(REALTIME_STT).unwrap();
        assert!((7_000_000..9_000_000).contains(&bound), "{bound}");
        assert!(ELEVENLABS.session_max_frame_bytes("user").is_err());
    }

    #[test]
    fn addons_raise_the_rate_and_the_slice_covers_the_dearest() {
        let obs = |q: &str| ELEVENLABS.observe_session(REALTIME_STT, q).unwrap();
        let base = {
            let mut o = obs("");
            o.on_frame_to_provider(&chunk(32_000 * 3600));
            o.accrued_usd()
        };
        assert!((base - 0.39).abs() < 1e-9);
        let with_addons = {
            let mut o = obs("entity_detection=true&keyterms=alpha");
            o.on_frame_to_provider(&chunk(32_000 * 3600));
            o.accrued_usd()
        };
        assert!((with_addons - 0.51).abs() < 1e-9);
        // The slice pre-carves one minute at the dearest possible rate.
        let slice = ELEVENLABS.session_slice_usd(REALTIME_STT).unwrap();
        assert!((slice - 0.51 / 60.0).abs() < 1e-9);
    }

    #[test]
    fn only_the_realtime_route_is_a_session() {
        assert_eq!(
            ELEVENLABS.classify("GET", REALTIME_STT),
            RouteClass::BillableSession
        );
        assert_eq!(
            ELEVENLABS.classify("POST", "speech-to-text"),
            RouteClass::Billable(crate::Pricing::Metered)
        );
        assert!(ELEVENLABS.observe_session("speech-to-text", "").is_err());
        assert_eq!(ELEVENLABS.classify("GET", "user"), RouteClass::Free);
        assert!(ELEVENLABS.observe_session("user", "").is_err());
    }

    #[test]
    fn batch_routes_classify_and_trick_paths_stay_unknown() {
        let billable = |m: &str, p: &str| {
            matches!(ELEVENLABS.classify(m, p), RouteClass::Billable(crate::Pricing::Metered))
        };
        for p in [
            "text-to-speech/v1",
            "speech-to-speech/v1",
            "audio-isolation",
            "sound-generation",
            "music",
            "dubbing",
            "forced-alignment",
            "speech-to-text",
        ] {
            assert!(billable("POST", p), "POST {p} bills metered");
            assert_eq!(ELEVENLABS.classify("GET", p), RouteClass::Unknown, "GET {p}");
        }
        for p in ["voices", "models", "user", "dubbing/d1", "dubbing/d1/audio/fr"] {
            assert_eq!(ELEVENLABS.classify("GET", p), RouteClass::Free, "GET {p}");
        }
        // Voice creation mints an ACCOUNT asset, so the runtime
        // credential never travels there (Unknown = own key only);
        // voice DESIGN is also unpriced until a USD rate is published.
        assert_eq!(ELEVENLABS.classify("POST", "voices/add"), RouteClass::Unknown);
        assert_eq!(ELEVENLABS.classify("POST", "text-to-voice/design"), RouteClass::Unknown);
        for trick in [
            "text-to-speech/../user",
            "text-to-speech/a/b",
            "text-to-speech/%2e%2e",
            "dubbing/d1/audio/fr/extra",
            "dubbing/../voices",
        ] {
            assert_eq!(
                ELEVENLABS.classify("POST", trick),
                RouteClass::Unknown,
                "POST {trick}"
            );
            assert_eq!(ELEVENLABS.classify("GET", trick), RouteClass::Unknown, "GET {trick}");
        }
    }

    #[test]
    fn tts_prices_the_request_characters_at_the_model_rate() {
        let body = |model: &str, text: &str| {
            serde_json::to_vec(&json!({ "text": text, "model_id": model })).unwrap()
        };
        let run = |body: &[u8]| {
            let mut obs = ELEVENLABS.observe("text-to-speech/v1", "output_format=mp3_44100_128", body);
            obs.on_status(200);
            obs.on_chunk(b"audio-bytes");
            obs.end(false)
        };
        let thousand = "x".repeat(1000);
        let observed = run(&body("eleven_multilingual_v2", &thousand));
        assert!((observed.data["usd"].as_f64().unwrap() - 0.10).abs() < 1e-9);
        let observed = run(&body("eleven_flash_v2_5", &thousand));
        assert!((observed.data["usd"].as_f64().unwrap() - 0.05).abs() < 1e-9);
        // A refused call bills nothing, whatever the request said.
        let mut obs =
            ELEVENLABS.observe("text-to-speech/v1", "", &body("eleven_v3", &thousand));
        obs.on_status(401);
        assert_eq!(obs.end(false).data["usd"].as_f64(), Some(0.0));
    }

    #[test]
    fn output_priced_routes_measure_the_answered_bytes() {
        // 60s of mp3_44100_128 (16000 B/s) of music at $0.15/min.
        let mut obs = ELEVENLABS.observe("music", "", b"{}");
        obs.on_status(200);
        obs.on_chunk(&vec![0u8; 16_000 * 60]);
        let observed = obs.end(false);
        assert!((observed.data["usd"].as_f64().unwrap() - 0.15).abs() < 1e-9);
        // An interrupted output stream is honestly unknown, never a
        // low number.
        let mut obs = ELEVENLABS.observe("sound-generation", "", b"{}");
        obs.on_status(200);
        obs.on_chunk(&vec![0u8; 16_000]);
        assert_eq!(obs.end(true).data["usd"].as_f64(), None);
    }

    #[test]
    fn streaming_tts_prices_the_sent_characters() {
        assert_eq!(
            ELEVENLABS.classify("GET", "text-to-speech/v1/stream-input"),
            RouteClass::BillableSession
        );
        assert_eq!(
            ELEVENLABS.classify("GET", "text-to-speech/../stream-input"),
            RouteClass::Unknown
        );
        let mut obs = ELEVENLABS
            .observe_session("text-to-speech/v1/stream-input", "model_id=eleven_flash_v2_5")
            .unwrap();
        obs.on_frame_to_provider(br#"{"text":" "}"#);
        let thousand = serde_json::to_vec(&json!({ "text": "x".repeat(999) })).unwrap();
        obs.on_frame_to_provider(&thousand);
        // Audio back costs nothing; junk frames cost nothing.
        obs.on_frame_to_caller(br#"{"audio":"aGk=","isFinal":false}"#);
        obs.on_frame_to_provider(b"not json");
        assert!((obs.accrued_usd() - 0.05).abs() < 1e-9, "{}", obs.accrued_usd());
        let cost = obs.end(false);
        assert_eq!(cost.model.as_deref(), Some("eleven_flash_v2_5"));
        // The slice covers the biggest admissible frame at the
        // dearest rate: max_frame_bytes chars at $0.10/1k.
        let slice =
            ELEVENLABS.session_slice_usd("text-to-speech/v1/stream-input").unwrap();
        let max = ELEVENLABS
            .session_max_frame_bytes("text-to-speech/v1/stream-input")
            .unwrap();
        assert!(max as f64 / 1000.0 * 0.10 <= slice, "one frame never outruns a slice");
    }

    #[test]
    fn a_dub_prices_its_answered_expected_duration() {
        let mut obs = ELEVENLABS.observe("dubbing", "", b"multipart-ignored");
        obs.on_status(200);
        obs.on_chunk(br#"{"dubbing_id":"d1","expected_duration_sec":120.0}"#);
        let observed = obs.end(false);
        assert!((observed.data["usd"].as_f64().unwrap() - 1.0).abs() < 1e-9, "{observed:?}");
    }

    #[test]
    fn ulaw_prices_at_one_byte_per_sample() {
        let mut o = ELEVENLABS
            .observe_session(REALTIME_STT, "audio_format=ulaw_8000")
            .unwrap();
        o.on_frame_to_provider(&chunk(8_000 * 60));
        assert!((o.accrued_usd() - 0.39 / 60.0).abs() < 1e-9);
        assert!(ELEVENLABS.observe_session(REALTIME_STT, "audio_format=opus").is_err());
    }
}
