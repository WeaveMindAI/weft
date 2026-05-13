//! Long-lived outbound Server-Sent Events subscription. The listener
//! opens a GET with `Accept: text/event-stream`, parses `data:` lines,
//! and relays events whose JSON `event` field matches `event_name`
//! (or every event if `event_name` is empty).

use serde::{Deserialize, Serialize};

use super::Signal;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sse {
    pub url: String,
    #[serde(default)]
    pub event_name: String,
}

impl Signal for Sse {
    const TAG: &'static str = "sse";

    fn validate(&self) -> Result<(), String> {
        if self.url.trim().is_empty() {
            return Err("sse.url must not be empty".into());
        }
        if !(self.url.starts_with("http://") || self.url.starts_with("https://")) {
            return Err(format!("sse.url must be http(s): got '{}'", self.url));
        }
        Ok(())
    }
}

crate::register_signal_kind!(Sse);
