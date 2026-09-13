//! The concrete provider meters, one module per provider. Adding a provider
//! is a file here (ending in its own `register_meter!` line) plus its
//! `pub mod` line below; the registry collects the registrations at link
//! time, so there is nothing central to edit.
//! The shared toolkit these build on (the [`ProviderMeter`](crate::ProviderMeter)
//! trait, the route classification, the SSE tap helper) lives at the crate
//! root, not here: this folder is only the concrete meters.

pub mod elevenlabs;
pub mod exa;
pub mod fal;
pub mod firecrawl;
pub mod mistral;
pub mod openrouter;

use crate::{CallObservation, MeasuredCost, ObservedCall};

/// What an HTTP status alone says about money, for a call whose amount
/// the provider does not state.
///
/// Every meter needs this and each one was writing it out again, which
/// is how two of them ended up on the wrong side of the 5xx rule. Three
/// outcomes, and only one of them is zero:
///
/// - 2xx: the call went through, so the caller prices it.
/// - 5xx: NOT a refusal. The provider may well have done the work and
///   billed for it, and the error came from a gateway in front of a
///   generation that already existed. Zero would claim the call was
///   free, so the honest record is that nobody knows.
/// - any other non-2xx: the provider refused before doing anything, so
///   nothing was billed and zero is a fact rather than a guess.
pub enum StatusVerdict {
    /// The call went through; the caller works out the amount.
    Accepted,
    /// Refused before any work: a known zero.
    KnownZero,
    /// Neither: the work may have run and spent.
    Unknown,
}

/// [`StatusVerdict`] for one status code.
pub fn verdict_for_status(status: u16) -> StatusVerdict {
    match status {
        200..=299 => StatusVerdict::Accepted,
        500..=599 => StatusVerdict::Unknown,
        _ => StatusVerdict::KnownZero,
    }
}

/// The cost a non-2xx status books on its own, or `None` when the status
/// says the call went through and the caller must price it.
///
/// `what` names the act in the provider's own words ("the scrape", "the
/// crawl submit"), and lands in the trail's resolution line.
pub fn cost_from_status(status: u16, what: &str) -> Option<MeasuredCost> {
    match verdict_for_status(status) {
        StatusVerdict::Accepted => None,
        StatusVerdict::KnownZero => Some(MeasuredCost {
            amount_usd: Some(0.0),
            model: None,
            metadata: serde_json::json!({
                "resolution": format!("the provider refused {what} ({status}), which spends nothing"),
                "status": status,
            }),
        }),
        StatusVerdict::Unknown => Some(MeasuredCost {
            amount_usd: None,
            model: None,
            metadata: serde_json::json!({
                "resolution": format!(
                    "the provider answered {status}, so whether {what} ran is unknown, and so is \
                     what it spent"
                ),
                "status": status,
            }),
        }),
    }
}

/// The plain-JSON response tap every meter shares: buffer the body
/// (bounded) and hand the parsed value to `resolve` as the
/// observation data. An overflowing body LATCHES as truncated and
/// observes `Null`: once any chunk is dropped the body is unreadable
/// forever, and letting later chunks re-append could reassemble a
/// fragment that happens to parse, booking a figure from a body that
/// was never fully seen.
///
/// A meter that needs more than the whole parsed body DECLARES what it
/// needs rather than writing its own tap: [`Self::seed`] carries facts
/// known before the answer, [`Self::field`] lifts one value out of the
/// body by JSON pointer, [`Self::header_f64`] reads a number off a
/// header, and [`Self::note_outcome`] adds the status and interruption
/// as plain fields.
///
/// The cap applies to the BODY. A header is the provider's own count
/// and is read before any of the body arrives, so a declared header
/// still carries its figure on a response whose body overflowed; the
/// declared body fields observe null, as they do for any unreadable
/// body. Hand-rolled taps kept forgetting the cap: fal had
/// two of them, both unbounded, so a large answer on a metered route
/// grew the worker's memory without limit.
#[derive(Default)]
pub struct JsonBodyObservation {
    status: u16,
    buffer: Vec<u8>,
    truncated: bool,
    seed: serde_json::Map<String, serde_json::Value>,
    fields: Vec<(&'static str, &'static str)>,
    headers_f64: Vec<(&'static str, &'static str)>,
    header_values: serde_json::Map<String, serde_json::Value>,
    note_outcome: bool,
    /// Observe the declared fields instead of the whole parsed body.
    /// Set by any declaration, so a meter that declares nothing keeps
    /// the plain "the body IS the data" behaviour.
    shaped: bool,
}

impl JsonBodyObservation {
    pub fn new() -> Self {
        Self::default()
    }

    /// Facts the meter already knows (the model it asked for, the route
    /// it called), carried through to the observation.
    ///
    /// Takes a map rather than a `Value` so a meter cannot hand this a
    /// string or an array and have it quietly drop, surfacing later as
    /// an empty field inside `fold_report`.
    pub fn seed(mut self, seed: serde_json::Map<String, serde_json::Value>) -> Self {
        self.seed.extend(seed);
        self.shaped = true;
        self
    }

    /// Observe `pointer` out of the parsed body under `key`. Pointer is
    /// JSON-pointer syntax: `/request_id`, `/metrics/inference_time`.
    pub fn field(mut self, key: &'static str, pointer: &'static str) -> Self {
        self.fields.push((key, pointer));
        self.shaped = true;
        self
    }

    /// Observe a header's value as a number under `key`. A header that
    /// is missing, unparseable, negative or not finite observes null,
    /// never a guess.
    pub fn header_f64(mut self, header: &'static str, key: &'static str) -> Self {
        self.headers_f64.push((header, key));
        self.shaped = true;
        self
    }

    /// Add `accepted` (a 2xx answer) and `interrupted` (the stream was
    /// cut) as fields of the observation.
    pub fn note_outcome(mut self) -> Self {
        self.note_outcome = true;
        self.shaped = true;
        self
    }
}

/// Far above any billing envelope these providers answer with; a body
/// past it observes `Null` and the cost resolves as unknown, never as
/// a guess.
const JSON_BODY_CAP: usize = 4 * 1024 * 1024;

impl CallObservation for JsonBodyObservation {
    fn on_status(&mut self, status: u16) {
        self.status = status;
    }
    fn on_headers(&mut self, headers: &http::HeaderMap) {
        for (header, key) in &self.headers_f64 {
            let value = headers
                .get(*header)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.trim().parse::<f64>().ok())
                .filter(|n| n.is_finite() && *n >= 0.0);
            self.header_values.insert((*key).to_string(), serde_json::json!(value));
        }
    }
    fn on_chunk(&mut self, bytes: &[u8]) {
        if self.truncated {
            return;
        }
        if self.buffer.len() + bytes.len() > JSON_BODY_CAP {
            self.truncated = true;
            return;
        }
        self.buffer.extend_from_slice(bytes);
    }
    fn end(self: Box<Self>, interrupted: bool) -> ObservedCall {
        let parsed: serde_json::Value = if self.truncated {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&self.buffer).unwrap_or(serde_json::Value::Null)
        };
        if !self.shaped {
            return ObservedCall { interrupted, status: self.status, data: parsed };
        }
        let mut data = serde_json::Value::Object(self.seed);
        for (key, value) in self.header_values {
            data[key] = value;
        }
        for (key, pointer) in &self.fields {
            data[*key] = parsed.pointer(pointer).cloned().unwrap_or(serde_json::Value::Null);
        }
        if self.note_outcome {
            data["accepted"] = serde_json::json!((200..300).contains(&self.status));
            data["interrupted"] = serde_json::json!(interrupted);
        }
        ObservedCall { interrupted, status: self.status, data }
    }
}

#[cfg(test)]
mod declared_observation_tests {
    use super::*;

    fn run(obs: Box<dyn CallObservation>, headers: &[(&str, &str)], body: &str, status: u16) -> ObservedCall {
        let mut obs = obs;
        obs.on_status(status);
        let mut map = http::HeaderMap::new();
        for (k, v) in headers {
            map.insert(
                http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                http::HeaderValue::from_str(v).unwrap(),
            );
        }
        obs.on_headers(&map);
        obs.on_chunk(body.as_bytes());
        obs.end(false)
    }

    /// A meter declares what it reads; the tap does the reading, so the
    /// cap can never be forgotten.
    #[test]
    fn a_declared_observation_lifts_the_header_the_fields_and_the_outcome() {
        let obs = Box::new(
            JsonBodyObservation::new()
                .seed(serde_json::Map::from_iter([("model".to_string(), serde_json::json!("m1"))]))
                .header_f64("x-fal-billable-units", "billableUnits")
                .field("requestId", "/request_id")
                .field("computeSeconds", "/metrics/inference_time")
                .note_outcome(),
        );
        let got = run(obs, &[("x-fal-billable-units", "2.5")], r#"{"request_id":"r1","metrics":{"inference_time":1.5}}"#, 200);
        assert_eq!(got.data["model"], serde_json::json!("m1"));
        assert_eq!(got.data["billableUnits"], serde_json::json!(2.5));
        assert_eq!(got.data["requestId"], serde_json::json!("r1"));
        assert_eq!(got.data["computeSeconds"], serde_json::json!(1.5));
        assert_eq!(got.data["accepted"], serde_json::json!(true));
    }

    /// Nothing is guessed: an absent header, an unreadable one and a
    /// missing body field all observe null.
    #[test]
    fn what_the_answer_did_not_say_observes_null() {
        let obs = Box::new(
            JsonBodyObservation::new()
                .header_f64("x-fal-billable-units", "billableUnits")
                .field("requestId", "/request_id"),
        );
        let got = run(obs, &[("x-fal-billable-units", "not a number")], r#"{"other":1}"#, 500);
        assert_eq!(got.data["billableUnits"], serde_json::Value::Null);
        assert_eq!(got.data["requestId"], serde_json::Value::Null);
    }

    /// The cap holds for a declared observation too: an over-cap body
    /// reads as nothing rather than as a fragment that happens to
    /// parse.
    #[test]
    fn an_over_cap_body_observes_nothing_for_its_declared_fields() {
        let mut obs: Box<dyn CallObservation> =
            Box::new(JsonBodyObservation::new().field("requestId", "/request_id"));
        obs.on_status(200);
        obs.on_chunk(&vec![b'x'; JSON_BODY_CAP + 1]);
        obs.on_chunk(br#"{"request_id":"r1"}"#);
        let got = obs.end(false);
        assert_eq!(got.data["requestId"], serde_json::Value::Null);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An over-cap body latches: later small chunks can never
    /// reassemble a parseable fragment, so the observation is `Null`
    /// (cost unknown), never a figure read from a hole-punched body.
    #[test]
    fn an_overflowing_body_latches_to_null() {
        let mut obs = JsonBodyObservation::default();
        obs.on_status(200);
        obs.on_chunk(&vec![b'x'; JSON_BODY_CAP + 1]);
        obs.on_chunk(br#"{"success": false}"#);
        let observed = Box::new(obs).end(false);
        assert_eq!(observed.data, serde_json::Value::Null);
        assert_eq!(observed.status, 200);
    }
}
