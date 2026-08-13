//! The concrete provider meters, one module per provider. Adding a provider
//! is a file here (ending in its own `register_meter!` line) plus its
//! `pub mod` line below; the registry collects the registrations at link
//! time, so there is nothing central to edit.
//! The shared toolkit these build on (the [`ProviderMeter`](crate::ProviderMeter)
//! trait, the route classification, the SSE tap helper) lives at the crate
//! root, not here: this folder is only the concrete meters.

pub mod elevenlabs;
pub mod exa;
pub mod firecrawl;
pub mod mistral;
pub mod openrouter;

use crate::{CallObservation, ObservedCall};

/// The plain-JSON response tap several meters share: buffer the body
/// (bounded) and hand the parsed value to `resolve` as the
/// observation data. An overflowing body LATCHES as truncated and
/// observes `Null`: once any chunk is dropped the body is unreadable
/// forever, and letting later chunks re-append could reassemble a
/// fragment that happens to parse, booking a figure from a body that
/// was never fully seen.
#[derive(Default)]
pub struct JsonBodyObservation {
    status: u16,
    buffer: Vec<u8>,
    truncated: bool,
}

/// Far above any billing envelope these providers answer with; a body
/// past it observes `Null` and the cost resolves as unknown, never as
/// a guess.
const JSON_BODY_CAP: usize = 4 * 1024 * 1024;

impl CallObservation for JsonBodyObservation {
    fn on_status(&mut self, status: u16) {
        self.status = status;
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
        let data = if self.truncated {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&self.buffer).unwrap_or(serde_json::Value::Null)
        };
        ObservedCall { interrupted, status: self.status, data }
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
