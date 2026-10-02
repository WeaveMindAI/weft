//! The action envelope an infra endpoint speaks: the one door through
//! which both a node (`EndpointHandle::action`) and the editor (a button
//! pressed on an infra node's display) ask a container to do something.
//!
//! The request is `POST /action` with `{"action": <name>, "payload":
//! <object>}`. The answer is `{"result": <object>}`, and a `result`
//! carrying `error` is a refusal the container chose to answer with a
//! 200 (a WhatsApp bridge whose phone is not paired yet): it is read as
//! a failure just as loud as a non-2xx.

use serde_json::Value;

/// The path every infra action is posted to.
pub const ACTION_PATH: &str = "/action";

/// The body of one action request.
pub fn action_request(action: &str, payload: Value) -> Value {
    serde_json::json!({ "action": action, "payload": payload })
}

/// Why an action's answer is not a result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActionFailure {
    /// The container answered with something other than the envelope.
    /// Never read as an action that succeeded with nothing to say.
    NotEnvelope { action: String, answer: String },
    /// The container refused the action (`result.error`).
    Refused { action: String, reason: String },
}

impl std::fmt::Display for ActionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotEnvelope { action, answer } => {
                write!(f, "{action}: the container answered without a `result` envelope: {answer}")
            }
            Self::Refused { action, reason } => write!(f, "{action}: {reason}"),
        }
    }
}

impl std::error::Error for ActionFailure {}

/// The `result` of an action's answer, or why there is none.
pub fn action_result(action: &str, answer: Value) -> Result<Value, ActionFailure> {
    let Some(result) = answer.get("result").cloned() else {
        return Err(ActionFailure::NotEnvelope { action: action.to_string(), answer: answer.to_string() });
    };
    if let Some(reason) = result.get("error").and_then(Value::as_str) {
        return Err(ActionFailure::Refused { action: action.to_string(), reason: reason.to_string() });
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_request_names_the_action_and_carries_the_payload() {
        assert_eq!(
            action_request("sendMessage", json!({ "to": "49" })),
            json!({ "action": "sendMessage", "payload": { "to": "49" } })
        );
    }

    #[test]
    fn a_result_is_handed_back_whole() {
        assert_eq!(action_result("send", json!({ "result": { "messageId": "m" } })), Ok(json!({ "messageId": "m" })));
    }

    #[test]
    fn a_soft_refusal_is_a_failure_naming_the_action() {
        let failure = action_result("send", json!({ "result": { "error": "WhatsApp not connected" } })).unwrap_err();
        assert!(matches!(failure, ActionFailure::Refused { .. }));
        assert_eq!(failure.to_string(), "send: WhatsApp not connected");
    }

    #[test]
    fn an_answer_without_the_envelope_is_not_a_success() {
        let failure = action_result("send", json!({ "ok": true })).unwrap_err();
        assert!(matches!(failure, ActionFailure::NotEnvelope { .. }));
        assert!(failure.to_string().contains("without a `result` envelope"), "{failure}");
    }
}
