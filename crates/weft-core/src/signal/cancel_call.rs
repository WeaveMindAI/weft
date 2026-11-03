//! The last call a run's wait may carry: one request the listener sends
//! if the run is cancelled while it waits.
//!
//! A node that started a job on an outside service (a video render, a
//! crawl) waits for it with `state.wait(.., kind.cancel_with(call))`, naming the
//! service's own cancel address. Cancelling the run takes the wait down,
//! and without this the service would keep working and charging for a
//! result nobody reads any more. The call goes once, as the connection
//! the wait polls with (`SignalSpec::access`), and only when the wait is
//! taken down BECAUSE its run was cancelled: never when it is answered,
//! and never when its run ends any other way.
//!
//! Wire-pure like `predicate`: [`crate::primitive::SignalSpec`] carries
//! it, and it is stored with the wait's row, so whichever copy of the
//! listener sends it reads it from there.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One request to send when the run waiting on a signal is cancelled.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancelCall {
    /// The service's cancel address for the job the wait is about.
    pub url: String,
    pub method: CancelMethod,
    /// The JSON body sent with it, for a service whose cancel takes one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<Value>,
}

/// The HTTP verb a [`CancelCall`] is sent with: whichever the service's
/// cancel endpoint answers to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancelMethod {
    Post,
    Put,
    Patch,
    Delete,
}

impl CancelMethod {
    /// The verb as HTTP spells it.
    pub fn as_http(self) -> &'static str {
        match self {
            CancelMethod::Post => "POST",
            CancelMethod::Put => "PUT",
            CancelMethod::Patch => "PATCH",
            CancelMethod::Delete => "DELETE",
        }
    }
}

impl CancelCall {
    pub fn post(url: impl Into<String>) -> Self {
        Self { url: url.into(), method: CancelMethod::Post, body: None }
    }

    pub fn put(url: impl Into<String>) -> Self {
        Self { url: url.into(), method: CancelMethod::Put, body: None }
    }

    pub fn patch(url: impl Into<String>) -> Self {
        Self { url: url.into(), method: CancelMethod::Patch, body: None }
    }

    pub fn delete(url: impl Into<String>) -> Self {
        Self { url: url.into(), method: CancelMethod::Delete, body: None }
    }

    /// The same call, sending `body` as JSON.
    pub fn with_body(mut self, body: Value) -> Self {
        self.body = Some(body);
        self
    }

    /// Refuse a call that could never be sent: an address that is not
    /// http(s).
    pub fn validate(&self) -> Result<(), String> {
        let url = self.url.trim();
        if url.is_empty() {
            return Err("the cancel call's url must not be empty".into());
        }
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Err(format!("the cancel call's url must be http(s): got '{}'", self.url));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The call rides the signal row as JSON; its verb keeps its wire word
    /// and a body-less call writes no body.
    #[test]
    fn a_cancel_call_round_trips() {
        let call = CancelCall::put("https://queue.example/requests/7/cancel");
        let wire = serde_json::to_value(&call).unwrap();
        assert_eq!(wire, serde_json::json!({ "url": "https://queue.example/requests/7/cancel", "method": "put" }));
        assert_eq!(serde_json::from_value::<CancelCall>(wire).unwrap(), call);
        let with_body = CancelCall::post("https://x/cancel").with_body(serde_json::json!({ "id": 7 }));
        let back: CancelCall = serde_json::from_value(serde_json::to_value(&with_body).unwrap()).unwrap();
        assert_eq!(back, with_body);
    }

    /// Each verb has its constructor, and each writes its own wire word.
    #[test]
    fn every_verb_has_a_constructor() {
        let url = "https://x/jobs/1";
        for (call, method, word) in [
            (CancelCall::post(url), CancelMethod::Post, "post"),
            (CancelCall::put(url), CancelMethod::Put, "put"),
            (CancelCall::patch(url), CancelMethod::Patch, "patch"),
            (CancelCall::delete(url), CancelMethod::Delete, "delete"),
        ] {
            assert_eq!(call, CancelCall { url: url.into(), method, body: None });
            assert_eq!(serde_json::to_value(&call).unwrap()["method"], word);
        }
    }

    #[test]
    fn only_an_http_address_is_a_cancel_call() {
        assert!(CancelCall::delete("https://x/jobs/1").validate().is_ok());
        assert!(CancelCall::delete("").validate().unwrap_err().contains("empty"));
        assert!(CancelCall::delete("ftp://x/jobs/1").validate().unwrap_err().contains("http(s)"));
    }
}
