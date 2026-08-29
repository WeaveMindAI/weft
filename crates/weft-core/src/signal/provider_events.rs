//! Transport-neutral event subscription: "fire when this connection's
//! service reports an event matching these filters, however that
//! event reaches us."
//!
//! This is the ONE kind that names no address, no protocol and no
//! frame. A trigger node registers a connection plus a filter; which
//! transport actually serves it is decided by the environment, not by
//! the registration:
//!
//!   - the service declares a DIAL-OUT recipe (`events.socket`) and
//!     the connection can run it: the listener holds one socket per
//!     connection and fans every inbound event to that connection's
//!     subscriptions.
//!   - the service declares a DIAL-IN recipe (`events.webhook`) and
//!     an app is configured to receive: the provider posts each event
//!     to the public events surface, which finds the connection it
//!     concerns and fans it to the same subscriptions.
//!
//! Both ends evaluate the same [`super::Predicate`] list against the
//! same NAMED fields (the service's `events.fields` table), so one
//! subscription means exactly one thing everywhere and a node body
//! never learns which door its event came through.

use serde::{Deserialize, Serialize};

use super::{Predicate, Signal};
use crate::access::Access;
use crate::primitive::AccessRef;

/// What one subscription covers. The two are DIFFERENT user
/// expectations and belong to different nodes, never auto-detected
/// behind one:
///
///   - `account`: the events of THIS connection's provider account
///     (your workspace's messages, your mailbox). Served by either
///     transport; on a shared multi-account socket the fan-out drops
///     other accounts' events.
///   - `app`: the events of EVERY account that installed the
///     connection's app (you own the app as a product; each install's
///     events are yours). Only the dial-out socket delivers app-wide,
///     so a connection that cannot run it refuses loudly.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventScope {
    #[default]
    Account,
    App,
}

/// A subscription to a connection's events. Both of its parts live on
/// the shared [`crate::primitive::SignalSpec`] (the connection under
/// `access`, the filter under `match`), because both are general
/// facilities every kind has; this struct is what a node CONSTRUCTS
/// and the tag is what tells the serving side to treat the signal as
/// an event subscription.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderEvents {
    /// Which of the service's event topics to subscribe on (the
    /// service recipe declares them: "events", "drive_changes",
    /// "mailbox", ...).
    pub topic: String,
    /// What the subscription covers; see [`EventScope`]. The nodes
    /// embodying the two expectations are DIFFERENT nodes; this field
    /// is how each states which one it is.
    #[serde(default)]
    pub scope: EventScope,
    /// Values the topic's subscribe call interpolates beyond the
    /// connection's stored values (the file to watch, a label
    /// filter). Empty for topics that need no subscribing.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub params: std::collections::BTreeMap<String, String>,
    /// The connection whose events to subscribe to. Lifted onto the
    /// spec by [`super::to_spec`]; not part of the kind's own config
    /// blob (hence `skip`), so there is exactly one home for it.
    #[serde(skip)]
    pub access: Option<AccessRef>,
    /// The filter, over the service's NAMED event fields
    /// (`text`, `channel`, ...) rather than raw provider paths.
    /// Lifted onto the spec like `access`.
    #[serde(skip)]
    pub filters: Vec<Predicate>,
}

impl ProviderEvents {
    /// Subscribe to `access`'s events on `topic`, firing on those
    /// matching every predicate in `filters` (an empty list = every
    /// event of this connection's topic).
    pub fn new(access: &Access, topic: impl Into<String>, filters: Vec<Predicate>) -> Self {
        Self {
            topic: topic.into(),
            scope: EventScope::Account,
            params: std::collections::BTreeMap::new(),
            access: Some(AccessRef::from(access)),
            filters,
        }
    }

    /// Subscribe APP-WIDE: every account that installed the
    /// connection's app. For the nodes embodying the own-the-app
    /// product expectation; needs the dial-out transport.
    pub fn app_wide(mut self) -> Self {
        self.scope = EventScope::App;
        self
    }

    /// Add the values the topic's subscribe call needs (a watched
    /// file's id). One call because a node has them in hand together.
    pub fn with_params(mut self, params: std::collections::BTreeMap<String, String>) -> Self {
        self.params = params;
        self
    }
}

impl Signal for ProviderEvents {
    const TAG: &'static str = "provider_events";
    /// The connection is what the whole kind resolves through: no
    /// connection means no service recipe, no transport, and nothing
    /// to route an inbound event by.
    const REQUIRES_ACCESS: bool = true;

    /// Only the config-blob fields are checkable here; the
    /// connection and the filter are spec-level and enforced by
    /// [`crate::signal::validate_spec`] (a rule written here would
    /// read the round-tripped config, where the skipped fields are
    /// always absent).
    fn validate(&self) -> Result<(), String> {
        if self.topic.trim().is_empty() {
            return Err(
                "provider_events needs the event topic to subscribe on (the service's \
                 recipe names its topics)"
                    .into(),
            );
        }
        Ok(())
    }

    fn access(&self) -> Option<AccessRef> {
        self.access.clone()
    }

    fn match_predicates(&self) -> &[Predicate] {
        &self.filters
    }
}

crate::register_signal_kind!(ProviderEvents);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signal::{to_spec, validate_spec, PredicateOp};

    fn access() -> Access {
        Access::new("11111111-2222-3333-4444-555555555555", "slack", None)
    }

    /// The connection and the filter land on the SPEC, not in the
    /// kind's config blob: one home each, and the shared listener
    /// plumbing reads them without knowing the kind.
    #[test]
    fn the_connection_and_filter_ride_the_spec() {
        let kind = ProviderEvents::new(
            &access(),
            "events",
            vec![Predicate {
                field: "type".into(),
                op: PredicateOp::Eq,
                value: Some("message".into()),
            }],
        );
        let spec = to_spec(kind);
        assert_eq!(spec.kind, "provider_events");
        let access_ref = spec.access.as_ref().expect("the connection rides the spec");
        assert_eq!(access_ref.service, "slack");
        assert_eq!(access_ref.id, "11111111-2222-3333-4444-555555555555");
        assert_eq!(spec.match_predicates.len(), 1);
        assert_eq!(
            spec.config,
            serde_json::json!({ "topic": "events", "scope": "account" }),
            "only the topic, scope and any params ride the kind's own config"
        );
        validate_spec(&spec).expect("a well-formed subscription validates");
    }

    /// No filters is legal and means "every event of this connection".
    #[test]
    fn an_empty_filter_subscribes_to_everything() {
        let spec = to_spec(ProviderEvents::new(&access(), "events", Vec::new()));
        assert!(spec.match_predicates.is_empty());
        validate_spec(&spec).expect("no filter is a valid subscription");
    }

    /// A bad regex is refused at REGISTRATION (through the shared
    /// spec-level validation), never left to silently match nothing.
    #[test]
    fn a_bad_filter_is_refused_at_registration() {
        let spec = to_spec(ProviderEvents::new(
            &access(),
            "events",
            vec![Predicate {
                field: "text".into(),
                op: PredicateOp::Regex,
                value: Some("([unclosed".into()),
            }],
        ));
        let err = validate_spec(&spec).unwrap_err();
        assert!(err.contains("regex"), "{err}");
    }

    /// A subscription with no connection has nothing to resolve, no
    /// recipe, and no way to be routed; refused loudly at
    /// registration, where the connection actually lives (on the
    /// spec, not in the kind's config blob).
    #[test]
    fn a_connectionless_subscription_is_refused() {
        let kind = ProviderEvents { topic: "events".into(), ..ProviderEvents::default() };
        let spec = to_spec(kind);
        assert!(spec.access.is_none());
        let err = validate_spec(&spec).unwrap_err();
        assert!(err.contains("connection"), "{err}");
    }

    /// A topicless subscription cannot pick a recipe; refused.
    #[test]
    fn a_topicless_subscription_is_refused() {
        let spec = to_spec(ProviderEvents::new(&access(), " ", Vec::new()));
        let err = validate_spec(&spec).unwrap_err();
        assert!(err.contains("topic"), "{err}");
    }
}
