//! Waiting on the first of several signals ([`FirstOf`]).
//!
//! Some waits have to end for more than one reason: a person answers a
//! form, or what the form asks about stops needing an answer (a coding
//! agent dropped the question it asked). `state.wait(field,
//! FirstOf::either(a, b))` waits on both and ends with whichever answers
//! first, as an [`Either`] saying which. The other is taken down at that
//! moment: a form that lost is listed to nobody any more, a poll that
//! lost polls no more, and an answer that still comes for it is refused
//! like an answer to any wait that ended.
//!
//! To the run it is ONE wait: one number, one token, one answer on its
//! record, so pausing, resuming and replaying it are a plain wait's. The
//! dispatcher splits it as it registers it: each branch becomes a signal
//! of its own, registered and served exactly as if the node had waited on
//! it alone, carrying [`BranchOf`], which names the wait it answers. The
//! first answer to reach a branch answers the wait, as
//! `{ "branch": <its position>, "value": <its answer> }` ([`branch_answer`]),
//! and takes every branch down in the same transaction. A person
//! skipping a branch (a form's skip) skips the wait, and a run cancelled
//! sends each branch's own cancel call.

use serde::{Deserialize, Serialize};

/// The `SignalSpec::kind` of a wait on the first of several signals.
pub const TAG: &str = "first_of";

/// What a branch's signal carries ([`crate::primitive::SignalSpec::branch_of`]):
/// the token of the wait it is a branch of, and its position among the
/// wait's branches, which its answer is handed over under.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BranchOf {
    pub wait: String,
    pub branch: u32,
}

/// The answer the wait takes when branch `branch` answered with `value`.
// SYNC: the first_of answer `{branch, value}` <-> `Either`'s serde below
pub fn branch_answer(branch: u32, value: serde_json::Value) -> serde_json::Value {
    serde_json::json!({ "branch": branch, "value": value })
}

#[cfg(feature = "runtime")]
pub use runtime::{validate, Either, FirstOf};

#[cfg(feature = "runtime")]
mod runtime {
    use serde::de::DeserializeOwned;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use serde_json::Value;

    use super::TAG;
    use crate::primitive::SignalSpec;
    use crate::signal::WaitOn;

    /// A wait on the first of `branches` to answer (see the module doc).
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct FirstOf {
        pub branches: Vec<SignalSpec>,
    }

    impl FirstOf {
        /// Wait on `first` and `second`, whichever answers first: the
        /// answer is an [`Either`]. Each may be any signal a wait takes,
        /// one that cancels its job included (`kind.cancel_with(call)`).
        pub fn either(first: impl WaitOn, second: impl WaitOn) -> Self {
            Self { branches: vec![first.into_spec(), second.into_spec()] }
        }

        /// The wait `spec` holds, when it is one ([`TAG`]).
        pub fn read(spec: &SignalSpec) -> Result<Self, String> {
            if spec.kind != TAG {
                return Err(format!("a '{}' signal is no wait on the first of several", spec.kind));
            }
            serde_json::from_value(spec.config.clone()).map_err(|e| format!("a '{TAG}' wait does not read: {e}"))
        }
    }

    impl WaitOn for FirstOf {
        fn into_spec(self) -> SignalSpec {
            SignalSpec::of_kind(TAG, serde_json::to_value(&self).expect("a FirstOf always serializes"))
        }
    }

    /// The rules of a wait on the first of several: at least two branches,
    /// each a signal that validates on its own, none of them a wait on the
    /// first of several in turn. The wait itself carries nothing but its
    /// branches: a connection, a filter, a cancel call or a consumer label
    /// belongs to the branch it is about.
    pub fn validate(spec: &SignalSpec) -> Result<(), String> {
        let first = FirstOf::read(spec)?;
        let bare = spec.access.is_none()
            && spec.match_predicates.is_empty()
            && spec.on_cancel.is_none()
            && spec.consumer_kind.is_none()
            && spec.branch_of.is_none()
            && spec.limits.is_default()
            && spec.settings == crate::run_settings::RunSettings::default();
        if !bare {
            return Err(format!(
                "a '{TAG}' wait carries only its branches: set a connection, a filter, a cancel call or a consumer \
                 label on the branch it is about"
            ));
        }
        if first.branches.len() < 2 {
            return Err(format!("a '{TAG}' wait needs at least two branches, and this one has {}", first.branches.len()));
        }
        for (at, branch) in first.branches.iter().enumerate() {
            if branch.kind == TAG {
                return Err(format!("branch {at} of a '{TAG}' wait is a '{TAG}' wait too: list its signals as branches of this one"));
            }
            if branch.branch_of.is_some() {
                return Err(format!("branch {at} of a '{TAG}' wait already names a wait it is a branch of; only the dispatcher sets that"));
            }
            crate::signal::validate_spec(branch).map_err(|e| format!("branch {at} of a '{TAG}' wait ('{}'): {e}", branch.kind))?;
        }
        Ok(())
    }

    /// The answer to a wait on [`FirstOf::either`]: the first branch's
    /// answer, or the second's. Kept in a node's state as the wait's
    /// answer reads (`{ "branch": 0 | 1, "value": .. }`), so it reads back
    /// the same after a pause.
    #[derive(Debug, Clone, PartialEq)]
    pub enum Either<A, B> {
        First(A),
        Second(B),
    }

    // SYNC: the first_of answer `{branch, value}` <-> super::branch_answer
    #[derive(Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Answered<V> {
        branch: u32,
        value: V,
    }

    impl<A: Serialize, B: Serialize> Serialize for Either<A, B> {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            match self {
                Either::First(value) => Answered { branch: 0, value }.serialize(serializer),
                Either::Second(value) => Answered { branch: 1, value }.serialize(serializer),
            }
        }
    }

    impl<'de, A: DeserializeOwned, B: DeserializeOwned> Deserialize<'de> for Either<A, B> {
        fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            use serde::de::Error;
            let answered = Answered::<Value>::deserialize(deserializer)?;
            match answered.branch {
                0 => serde_json::from_value(answered.value).map(Either::First).map_err(D::Error::custom),
                1 => serde_json::from_value(answered.value).map(Either::Second).map_err(D::Error::custom),
                n => Err(D::Error::custom(format!("branch {n} answered a wait on two signals, whose branches are 0 and 1"))),
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use serde_json::json;

        use super::*;
        use crate::signal::{to_spec, PollEndpoint, Timer, TimerSpec};

        fn poll() -> PollEndpoint {
            PollEndpoint { url: "https://agent.test/question?id=q1".into(), ..Default::default() }
        }

        fn timer() -> Timer {
            Timer { spec: TimerSpec::After { duration_ms: 1000 } }
        }

        #[test]
        fn a_wait_on_either_of_two_signals_validates_each_branch() {
            let wait = FirstOf::either(poll(), timer()).into_spec();
            assert_eq!(wait.kind, TAG);
            crate::signal::validate_spec(&wait).unwrap();
            let broken = FirstOf::either(PollEndpoint { url: "ftp://nowhere".into(), ..Default::default() }, timer()).into_spec();
            let err = crate::signal::validate_spec(&broken).unwrap_err();
            assert!(err.contains("branch 0") && err.contains("poll_endpoint"), "{err}");
        }

        #[test]
        fn a_wait_on_the_first_of_needs_two_flat_branches() {
            let one = FirstOf { branches: vec![to_spec(poll())] }.into_spec();
            assert!(validate(&one).unwrap_err().contains("at least two"));
            let nested = FirstOf { branches: vec![to_spec(poll()), FirstOf::either(poll(), timer()).into_spec()] }.into_spec();
            assert!(validate(&nested).unwrap_err().contains("branch 1"));
            let mut claimed = to_spec(poll());
            claimed.branch_of = Some(super::super::BranchOf { wait: "w".into(), branch: 0 });
            let claimed = FirstOf { branches: vec![claimed, to_spec(timer())] }.into_spec();
            assert!(validate(&claimed).unwrap_err().contains("only the dispatcher"));
        }

        #[test]
        fn the_wait_itself_carries_only_its_branches() {
            let mut wait = FirstOf::either(poll(), timer()).into_spec();
            wait.consumer_kind = Some("browser".into());
            assert!(validate(&wait).unwrap_err().contains("only its branches"));
        }

        /// The answer the dispatcher hands over reads as the branch that
        /// answered, and an `Either` kept in a node's state reads back as
        /// it was.
        #[test]
        fn an_answer_reads_as_the_branch_that_gave_it() {
            let first: Either<String, Value> = serde_json::from_value(super::super::branch_answer(0, json!("yes"))).unwrap();
            assert_eq!(first, Either::First("yes".to_string()));
            let second: Either<String, Value> = serde_json::from_value(super::super::branch_answer(1, json!({ "gone": true }))).unwrap();
            assert_eq!(second, Either::Second(json!({ "gone": true })));
            assert_eq!(serde_json::to_value(&second).unwrap(), json!({ "branch": 1, "value": { "gone": true } }));
            let third = serde_json::from_value::<Either<Value, Value>>(super::super::branch_answer(2, json!(1))).unwrap_err();
            assert!(third.to_string().contains("branch 2"), "{third}");
        }
    }
}
