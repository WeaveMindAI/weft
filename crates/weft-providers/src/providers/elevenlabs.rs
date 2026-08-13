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

fn query_param<'a>(query: &'a str, name: &str) -> Option<String> {
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

    fn classify(&self, _method: &str, path: &str) -> RouteClass {
        match path {
            REALTIME_STT => RouteClass::BillableSession,
            _ => RouteClass::Unknown,
        }
    }

    fn prepare(&self, _path: &str, _body: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(None)
    }

    fn observe(&self, _path: &str) -> Box<dyn CallObservation> {
        // No route classifies Billable, so no per-call observer is ever
        // minted; a call here is a meter bug.
        unreachable!("the elevenlabs meter has no Billable (one-shot) routes")
    }

    fn observe_session(
        &self,
        path: &str,
        query: &str,
    ) -> anyhow::Result<Box<dyn SessionObservation>> {
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
        anyhow::ensure!(path == REALTIME_STT, "'{path}' is not a session route");
        // One minute of audio worth, at the dearest rate the session could
        // negotiate (both add-ons on): a slice must never under-carve.
        let dearest =
            REALTIME_STT_USD_PER_HOUR + ENTITY_DETECTION_USD_PER_HOUR + KEYTERMS_USD_PER_HOUR;
        Ok(SECONDS_OF_AUDIO_PER_SLICE / 3600.0 * dearest)
    }

    fn session_max_frame_bytes(&self, path: &str) -> anyhow::Result<usize> {
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
        _observed: crate::ObservedCall,
        _follow_up: FollowUp<'_>,
    ) -> MeasuredCost {
        unreachable!("the elevenlabs meter has no Billable (one-shot) routes")
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
        assert_eq!(ELEVENLABS.classify("POST", "speech-to-text"), RouteClass::Unknown);
        assert_eq!(ELEVENLABS.classify("GET", "user"), RouteClass::Unknown);
        assert!(ELEVENLABS.observe_session("user", "").is_err());
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
