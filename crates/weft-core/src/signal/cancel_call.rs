//! A call weft sends once for a node, at the end of what it is about,
//! carrying when to send it ([`SendWhen`]): only if the run is cancelled
//! first, or however it ends.
//!
//! Two places record one, and the call itself is the same in both:
//!
//! - A node's own: `ctx.after_node(call, &access)`, right after the node
//!   made something at a provider that must not outlive it (an upload a
//!   job reads, a project it dubs in). It goes on the run's record
//!   (`RunEvent::EndCallOwed`), so a worker that picks the run up after a
//!   crash still owes it, and it is sent once when the node ends: as its
//!   [`SendWhen`] says, through the node's own connection. A `404` counts
//!   as done; a refusal shows on the node's log, quoting the provider.
//! - A wait's: `state.wait(.., kind.cancel_with(call))`, naming the
//!   service's own cancel address for the job the wait is about. It goes
//!   with the wait's row (`SignalSpec::on_cancel`), only ever
//!   [`SendWhen::OnCancel`], and only when the wait is taken down BECAUSE
//!   its run was cancelled: never when it is answered, and never when its
//!   run ends any other way. It is sent as the connection the wait polls
//!   with (`SignalSpec::access`).
//!
//! Without either, a cancelled run would leave the service working and
//! charging for a result nobody reads any more, or leave what the node
//! made in the user's account.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One request weft sends once when what it is about ends (see the
/// module doc for the two places one is recorded).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancelCall {
    /// The service's address for the thing the call ends or removes.
    pub url: String,
    pub method: CancelMethod,
    /// The JSON body sent with it, for a service whose call takes one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<Value>,
    /// When it is sent. Left out on the wire for the default,
    /// [`SendWhen::OnCancel`].
    #[serde(default, skip_serializing_if = "SendWhen::is_on_cancel")]
    pub when: SendWhen,
}

/// When a [`CancelCall`] is sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SendWhen {
    /// Only if the run is cancelled before the node (or the wait) ended:
    /// a cancel for a job that would otherwise keep running.
    #[default]
    OnCancel,
    /// However the node ends: it returned, failed, was cancelled, gave up
    /// or skipped a wait, or its worker died and the run ended without
    /// it. A clean-up for something that must not outlive the node.
    Always,
}

impl SendWhen {
    fn is_on_cancel(&self) -> bool {
        *self == SendWhen::OnCancel
    }

    /// Whether a call sent this way goes out for an end that was a
    /// cancel (`cancelled`) or any other.
    pub fn sends(self, cancelled: bool) -> bool {
        cancelled || self == SendWhen::Always
    }
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
    fn of(method: CancelMethod, url: impl Into<String>) -> Self {
        Self { url: url.into(), method, body: None, when: SendWhen::OnCancel }
    }

    pub fn post(url: impl Into<String>) -> Self {
        Self::of(CancelMethod::Post, url)
    }

    pub fn put(url: impl Into<String>) -> Self {
        Self::of(CancelMethod::Put, url)
    }

    pub fn patch(url: impl Into<String>) -> Self {
        Self::of(CancelMethod::Patch, url)
    }

    pub fn delete(url: impl Into<String>) -> Self {
        Self::of(CancelMethod::Delete, url)
    }

    /// The same call, sending `body` as JSON.
    pub fn with_body(mut self, body: Value) -> Self {
        self.body = Some(body);
        self
    }

    /// The same call, sent however the node ends ([`SendWhen::Always`]).
    pub fn always(mut self) -> Self {
        self.when = SendWhen::Always;
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

    /// Refuse what a wait cannot carry: a call that is not
    /// [`SendWhen::OnCancel`]. A wait's call goes with the wait's row,
    /// which is gone once the wait is answered, so only a cancel while it
    /// waits can send it.
    pub fn validate_for_wait(&self) -> Result<(), String> {
        self.validate()?;
        if self.when != SendWhen::OnCancel {
            return Err(format!(
                "a wait's call is sent only if its run is cancelled while it waits, and {} {} says `always`; \
                 for a call sent however the node ends, register it with `ctx.after_node` instead",
                self.method.as_http(),
                self.url
            ));
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
            assert_eq!(call, CancelCall { url: url.into(), method, body: None, when: SendWhen::OnCancel });
            assert_eq!(serde_json::to_value(&call).unwrap()["method"], word);
        }
    }

    /// A call sent however the node ends writes its `when`; a cancel-only
    /// one writes none, and both read back as written.
    #[test]
    fn when_rides_the_wire_only_when_it_is_always() {
        let always = CancelCall::delete("https://api.example/uploads/123").always();
        let wire = serde_json::to_value(&always).unwrap();
        assert_eq!(wire, serde_json::json!({ "url": "https://api.example/uploads/123", "method": "delete", "when": "always" }));
        assert_eq!(serde_json::from_value::<CancelCall>(wire).unwrap(), always);
        assert_eq!(serde_json::from_value::<CancelCall>(serde_json::json!({ "url": "https://x", "method": "put" })).unwrap().when, SendWhen::OnCancel);
        assert!(serde_json::from_value::<CancelCall>(serde_json::json!({ "url": "https://x", "method": "put", "when": "sometimes" })).is_err());
    }

    /// A cancel-only call goes out on a cancel alone; an always call on
    /// every end. A wait carries only a cancel-only one.
    #[test]
    fn when_decides_which_ends_send_it() {
        assert!(SendWhen::OnCancel.sends(true) && !SendWhen::OnCancel.sends(false));
        assert!(SendWhen::Always.sends(true) && SendWhen::Always.sends(false));
        assert!(CancelCall::put("https://x/cancel").validate_for_wait().is_ok());
        assert!(CancelCall::delete("https://x/1").always().validate_for_wait().unwrap_err().contains("after_node"));
    }

    #[test]
    fn only_an_http_address_is_a_cancel_call() {
        assert!(CancelCall::delete("https://x/jobs/1").validate().is_ok());
        assert!(CancelCall::delete("").validate().unwrap_err().contains("empty"));
        assert!(CancelCall::delete("ftp://x/jobs/1").validate().unwrap_err().contains("http(s)"));
    }
}
