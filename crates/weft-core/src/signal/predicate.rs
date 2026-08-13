//! Pre-fire predicates: the declarative filter every wake signal may
//! carry, evaluated between "the kind produced a payload" and "a fire
//! is enqueued".
//!
//! One evaluator, shared by every kind and by both delivery
//! directions (the listener holding a stream, the public receiver
//! taking a push), so a filter means exactly the same thing wherever
//! the event came in. The grammar is deliberately tiny: field paths
//! plus six operators. Anything richer belongs in ordinary node code
//! after the fire, never in the listener (which holds many tenants'
//! signals and must only ever run weft's own data-driven code).
//!
//! This is NOT [`crate::node::Condition`], which is a compile-time
//! query over a node's wiring and config. A predicate reads a runtime
//! JSON payload; the two share no input and no use site.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::access::spec::lookup_path;

/// One filter over an event payload. `field` is a dotted path
/// ([`lookup_path`] vocabulary); the `provider_events` kind maps its
/// service's NAMED fields to paths before evaluating, so a
/// subscription's predicates stay provider-agnostic.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Predicate {
    pub field: String,
    pub op: PredicateOp,
    /// The comparison operand. Required by every op except `exists`
    /// and `not_exists`, which read presence alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

impl Predicate {
    fn with_value(field: &str, op: PredicateOp, value: impl Into<String>) -> Self {
        Self { field: field.into(), op, value: Some(value.into()) }
    }

    /// `field == value`.
    pub fn eq(field: &str, value: impl Into<String>) -> Self {
        Self::with_value(field, PredicateOp::Eq, value)
    }

    /// `field != value`.
    pub fn neq(field: &str, value: impl Into<String>) -> Self {
        Self::with_value(field, PredicateOp::Neq, value)
    }

    /// `field` contains `value` as a substring.
    pub fn contains(field: &str, value: impl Into<String>) -> Self {
        Self::with_value(field, PredicateOp::Contains, value)
    }

    /// `field` matches the regex `value`.
    pub fn regex(field: &str, value: impl Into<String>) -> Self {
        Self::with_value(field, PredicateOp::Regex, value)
    }

    /// The field is present, whatever its value.
    pub fn exists(field: &str) -> Self {
        Self { field: field.into(), op: PredicateOp::Exists, value: None }
    }

    /// The field is absent.
    pub fn not_exists(field: &str) -> Self {
        Self { field: field.into(), op: PredicateOp::NotExists, value: None }
    }
}

/// The predicate operators. Comparisons are over the field's DISPLAY
/// string (a JSON string compares as itself, a number/bool as its JSON
/// text), so one operand shape serves every payload type and a
/// filter never has to know whether a provider sends `"42"` or `42`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PredicateOp {
    Eq,
    Neq,
    Contains,
    Regex,
    Exists,
    NotExists,
}

impl PredicateOp {
    /// Does this operator compare against an operand? `exists` /
    /// `not_exists` read presence alone, so an operand on them is a
    /// config mistake worth refusing.
    fn takes_value(self) -> bool {
        !matches!(self, Self::Exists | Self::NotExists)
    }
}

#[cfg(test)]
mod constructor_tests {
    use super::*;

    #[test]
    fn constructors_build_the_exact_struct_shapes() {
        assert_eq!(
            Predicate::eq("type", "message"),
            Predicate { field: "type".into(), op: PredicateOp::Eq, value: Some("message".into()) }
        );
        assert_eq!(
            Predicate::not_exists("bot"),
            Predicate { field: "bot".into(), op: PredicateOp::NotExists, value: None }
        );
        assert_eq!(
            Predicate::exists("thread"),
            Predicate { field: "thread".into(), op: PredicateOp::Exists, value: None }
        );
        assert_eq!(Predicate::contains("text", "hi").op, PredicateOp::Contains);
        assert_eq!(Predicate::regex("type", "^a$").op, PredicateOp::Regex);
        assert_eq!(Predicate::neq("state", "sync").op, PredicateOp::Neq);
    }
}

/// The string a payload value compares as: a JSON string is itself,
/// anything else is its JSON text. Shared by comparison and by the
/// field mapping so both read a value the same way.
pub fn display_of(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Validate a predicate list at REGISTRATION time: operand presence
/// per operator, and every regex compiles. A bad regex must fail when
/// the trigger is set up, never silently match nothing on every event
/// for the life of the signal.
pub fn validate(predicates: &[Predicate]) -> Result<(), String> {
    for p in predicates {
        if p.field.trim().is_empty() {
            return Err("a predicate needs a non-empty field".into());
        }
        match (p.op.takes_value(), &p.value) {
            (true, None) => {
                return Err(format!(
                    "predicate on '{}' uses an operator that compares against a value, but \
                     declares none",
                    p.field
                ))
            }
            (false, Some(_)) => {
                return Err(format!(
                    "predicate on '{}' checks presence only, so it must declare no value",
                    p.field
                ))
            }
            _ => {}
        }
        if p.op == PredicateOp::Regex {
            let pattern = p.value.as_deref().unwrap_or_default();
            regex::Regex::new(pattern).map_err(|e| {
                format!("predicate on '{}' has an invalid regex '{pattern}': {e}", p.field)
            })?;
        }
    }
    Ok(())
}

/// Does `payload` satisfy every predicate? An empty list matches
/// everything (a subscription with no filters wants every event).
/// Predicates AND together; an OR is expressed by registering two
/// subscriptions, which is also what the provider bills as two.
///
/// A regex that does not compile evaluates to NO MATCH rather than
/// panicking; [`validate`] refuses it at registration, so reaching
/// this state means a row was written before that check existed and
/// dropping the event is the safe read.
pub fn matches(predicates: &[Predicate], payload: &Value) -> bool {
    predicates.iter().all(|p| matches_one(p, payload))
}

fn matches_one(p: &Predicate, payload: &Value) -> bool {
    // A JSON null at the path reads as absent: a provider that sends
    // `{"thread_ts": null}` means the same thing as one that omits
    // the key, and a filter must not tell them apart.
    let found = lookup_path(payload, &p.field).filter(|v| !v.is_null());
    let operand = p.value.as_deref().unwrap_or_default();
    match p.op {
        PredicateOp::Exists => found.is_some(),
        PredicateOp::NotExists => found.is_none(),
        PredicateOp::Eq => found.map(display_of).as_deref() == Some(operand),
        // A missing field is not equal to the operand, so it SATISFIES
        // a `neq`. That is the honest reading of "this field is not X"
        // and is what makes `neq` usable on optional fields.
        PredicateOp::Neq => found.map(display_of).as_deref() != Some(operand),
        PredicateOp::Contains => {
            found.map(display_of).is_some_and(|s| s.contains(operand))
        }
        PredicateOp::Regex => match regex::Regex::new(operand) {
            Ok(re) => found.map(display_of).is_some_and(|s| re.is_match(&s)),
            Err(_) => false,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn p(field: &str, op: PredicateOp, value: Option<&str>) -> Predicate {
        Predicate {
            field: field.into(),
            op,
            value: value.map(str::to_string),
        }
    }

    fn event() -> Value {
        json!({
            "type": "message",
            "channel": "C42",
            "text": "hello team",
            "count": 3,
            "nested": { "user": "U1" },
            "thread": null
        })
    }

    #[test]
    fn no_predicates_matches_every_event() {
        assert!(matches(&[], &event()));
        assert!(matches(&[], &Value::Null));
    }

    #[test]
    fn every_operator_reads_the_payload() {
        let e = event();
        assert!(matches(&[p("type", PredicateOp::Eq, Some("message"))], &e));
        assert!(!matches(&[p("type", PredicateOp::Eq, Some("reaction"))], &e));
        assert!(matches(&[p("type", PredicateOp::Neq, Some("reaction"))], &e));
        assert!(matches(&[p("text", PredicateOp::Contains, Some("team"))], &e));
        assert!(!matches(&[p("text", PredicateOp::Contains, Some("nope"))], &e));
        assert!(matches(&[p("text", PredicateOp::Regex, Some("^hello"))], &e));
        assert!(!matches(&[p("text", PredicateOp::Regex, Some("^bye"))], &e));
        assert!(matches(&[p("channel", PredicateOp::Exists, None)], &e));
        assert!(matches(&[p("missing", PredicateOp::NotExists, None)], &e));
        assert!(matches(&[p("nested.user", PredicateOp::Eq, Some("U1"))], &e));
    }

    /// A non-string field compares as its JSON text, so a filter never
    /// has to know whether the provider sends `3` or `"3"`.
    #[test]
    fn numbers_compare_as_their_json_text() {
        assert!(matches(&[p("count", PredicateOp::Eq, Some("3"))], &event()));
    }

    /// An explicit JSON null reads as ABSENT: a provider sending
    /// `{"thread": null}` means the same as one omitting the key, and
    /// the "top-level messages only" filter depends on it.
    #[test]
    fn an_explicit_null_reads_as_absent() {
        let e = event();
        assert!(matches(&[p("thread", PredicateOp::NotExists, None)], &e));
        assert!(!matches(&[p("thread", PredicateOp::Exists, None)], &e));
    }

    /// A missing field satisfies `neq` (nothing there is not X) and
    /// fails every positive comparison.
    #[test]
    fn a_missing_field_fails_positives_and_satisfies_neq() {
        let e = event();
        assert!(matches(&[p("missing", PredicateOp::Neq, Some("x"))], &e));
        assert!(!matches(&[p("missing", PredicateOp::Eq, Some("x"))], &e));
        assert!(!matches(&[p("missing", PredicateOp::Contains, Some("x"))], &e));
        assert!(!matches(&[p("missing", PredicateOp::Regex, Some(".*"))], &e));
    }

    #[test]
    fn predicates_and_together() {
        let e = event();
        let both = vec![
            p("type", PredicateOp::Eq, Some("message")),
            p("channel", PredicateOp::Eq, Some("C42")),
        ];
        assert!(matches(&both, &e));
        let one_wrong = vec![
            p("type", PredicateOp::Eq, Some("message")),
            p("channel", PredicateOp::Eq, Some("C99")),
        ];
        assert!(!matches(&one_wrong, &e));
    }

    /// Registration-time validation: a bad regex, a missing operand,
    /// and an operand on a presence check all fail loudly.
    #[test]
    fn validation_refuses_bad_shapes_at_registration() {
        assert!(validate(&[p("text", PredicateOp::Regex, Some("valid.*"))]).is_ok());

        let err = validate(&[p("text", PredicateOp::Regex, Some("([unclosed"))]).unwrap_err();
        assert!(err.contains("regex"), "{err}");

        let err = validate(&[p("text", PredicateOp::Eq, None)]).unwrap_err();
        assert!(err.contains("declares none"), "{err}");

        let err = validate(&[p("text", PredicateOp::Exists, Some("x"))]).unwrap_err();
        assert!(err.contains("presence only"), "{err}");

        let err = validate(&[p("  ", PredicateOp::Exists, None)]).unwrap_err();
        assert!(err.contains("field"), "{err}");
    }

    #[test]
    fn wire_round_trip() {
        let list = vec![
            p("type", PredicateOp::Eq, Some("message")),
            p("bot", PredicateOp::NotExists, None),
        ];
        let v = serde_json::to_value(&list).unwrap();
        let back: Vec<Predicate> = serde_json::from_value(v).unwrap();
        assert_eq!(back, list);
    }
}
