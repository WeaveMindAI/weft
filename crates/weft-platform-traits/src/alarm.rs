//! Wake a role at a time.
//!
//! Nothing in weft sleeps for long in memory any more: a role may be a
//! service scaled to zero, and a sleeping task there is a task that never
//! wakes. Whatever must happen later (a timer's next fire, a poll's next
//! cycle, a role's periodic sweep) is handed to an [`Alarm`], which calls
//! the role back at that time.
//!
//! The contract is deliberately small, because the receiver is the one
//! that knows the truth:
//!
//! - **One shot.** A wake is delivered once; whatever wants the next one
//!   sets it when it handles this one (so a cron is "each wake sets the
//!   next").
//! - **At least once, maybe late, maybe stale.** A platform may deliver a
//!   wake twice, a little late, or after the thing it was set for has
//!   changed. The receiver recomputes from its own durable state whether
//!   anything is due, and does nothing when not. That is also why there
//!   is no cancel: a wake nobody wants any more is simply a no-op.
//! - **Same key and time, same wake.** Setting a wake whose key and time
//!   are already set does not add a second one where the platform can
//!   tell (every platform here can).
//! - **The answer's status says whether to try again.** A receiver that
//!   fails for now (5xx, an auth or routing 4xx such as 401, 403 or 404,
//!   or no answer at all) gets the wake again later. Only an answer that
//!   says the request body itself is wrong (400, 413, 415, 422: a body
//!   from an older build the receiver no longer reads) means the same call
//!   can never succeed: an alarm that can tell drops it, loudly
//!   ([`WakeRefusal::of_status`]).

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::roles::CoreRole;

/// One wake: call `role` at `path` with `body`, at `at_unix_ms` (posting
/// [`WakeCall`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Wake {
    /// What this wake is for, unique per thing that wakes (a signal's
    /// token, a role's tick). With `at_unix_ms` it names the wake.
    pub key: String,
    pub at_unix_ms: i64,
    pub role: CoreRole,
    /// The role's internal path to call (`/wake`, or
    /// [`crate::roles::TICK_PATH`] for a role's own tick).
    pub path: String,
    pub body: serde_json::Value,
}

/// What an alarm posts to the role's `path` when the wake comes due: the
/// moment it was set for and the body it was set with. Every platform's
/// alarm delivers exactly this, so a role reads one shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WakeCall<B> {
    pub at_unix_ms: i64,
    pub body: B,
}

/// Whether a wake the receiver did not take can ever be taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeRefusal {
    /// The receiver failed for now: try again later.
    Temporary,
    /// The request itself is wrong: the same call will be refused forever.
    Permanent,
}

impl WakeRefusal {
    /// Read the status of an answer that was not a success. Only the
    /// statuses that judge the request body (400 bad request, 413 too
    /// large, 415 unsupported type, 422 unprocessable) are permanent.
    /// Everything else is temporary: a 5xx, a 408 or 429, and also a 401,
    /// 403 or 404, which say the caller's credentials or the receiver's
    /// routes are not right YET (a rotating key, a role mid-deploy), and
    /// would delete a good wake if read as final.
    pub fn of_status(status: u16) -> Self {
        match status {
            400 | 413 | 415 | 422 => WakeRefusal::Permanent,
            _ => WakeRefusal::Temporary,
        }
    }
}

impl Wake {
    /// The request body an alarm posts for this wake.
    pub fn call(&self) -> WakeCall<serde_json::Value> {
        WakeCall { at_unix_ms: self.at_unix_ms, body: self.body.clone() }
    }

    /// A name for this wake that is the same for the same key and time,
    /// and safe as an identifier on every platform (letters, digits,
    /// `-`, `_`; at most 100 characters).
    pub fn name(&self) -> String {
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(format!("{}|{}", self.key, self.at_unix_ms).as_bytes());
        let hex: String = digest.iter().take(16).map(|b| format!("{b:02x}")).collect();
        format!("w-{}-{hex}", self.at_unix_ms)
    }
}

#[async_trait]
pub trait Alarm: Send + Sync {
    async fn set(&self, wake: Wake) -> anyhow::Result<()>;
}

#[cfg(any(test, feature = "test-helpers"))]
pub mod fake {
    use super::*;
    use parking_lot::Mutex;

    /// Records every wake set; delivers nothing (a test calls the
    /// receiver itself with what it finds here).
    #[derive(Default)]
    pub struct FakeAlarm {
        set: Mutex<Vec<Wake>>,
    }

    impl FakeAlarm {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn wakes(&self) -> Vec<Wake> {
            self.set.lock().clone()
        }

        /// Every wake set for `key`, oldest first.
        pub fn wakes_for(&self, key: &str) -> Vec<Wake> {
            self.set.lock().iter().filter(|w| w.key == key).cloned().collect()
        }
    }

    #[async_trait]
    impl Alarm for FakeAlarm {
        async fn set(&self, wake: Wake) -> anyhow::Result<()> {
            self.set.lock().push(wake);
            Ok(())
        }
    }
}

#[cfg(test)]
mod refusal_tests {
    use super::WakeRefusal;

    #[test]
    fn a_server_error_throttle_or_auth_failure_is_tried_again() {
        for status in [500, 502, 503, 599, 408, 429, 401, 403, 404, 405, 409, 499, 302] {
            assert_eq!(WakeRefusal::of_status(status), WakeRefusal::Temporary, "{status}");
        }
    }

    #[test]
    fn only_a_rejected_body_is_permanent() {
        for status in [400, 413, 415, 422] {
            assert_eq!(WakeRefusal::of_status(status), WakeRefusal::Permanent, "{status}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wake(key: &str, at: i64) -> Wake {
        Wake { key: key.into(), at_unix_ms: at, role: CoreRole::Listener, path: "/wake".into(), body: serde_json::json!({}) }
    }

    #[test]
    fn a_wake_name_is_stable_per_key_and_time_and_safe_everywhere() {
        assert_eq!(wake("a", 1).name(), wake("a", 1).name());
        assert_ne!(wake("a", 1).name(), wake("a", 2).name());
        assert_ne!(wake("a", 1).name(), wake("b", 1).name());
        let n = wake("sig:@src:x/y z", 1_700_000_000_000).name();
        assert!(n.len() <= 100);
        assert!(n.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'), "{n}");
    }

    #[test]
    fn a_wake_round_trips() {
        let w = wake("k", 5);
        let v = serde_json::to_value(&w).unwrap();
        assert_eq!(serde_json::from_value::<Wake>(v).unwrap(), w);
    }
}
