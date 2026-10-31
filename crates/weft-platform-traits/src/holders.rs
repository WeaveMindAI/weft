//! What holders hear and how a dead one is noticed.
//!
//! A holder (`weft_listener::hold`) keeps outside connections open for
//! the signals it claims, and a claim has no expiry: it lasts until the
//! holder lets it go. Two things replace a lease it would renew on a
//! timer:
//!
//! - **News** ([`HolderNews`]): whoever changes which signals need a
//!   holder tells every holder, which then looks once (one broker call
//!   that says what it holds and takes what is free). A holder also looks
//!   when it starts and whenever it may have missed some.
//! - **A dead man's switch** ([`HolderLiveness`]): a living holder keeps
//!   one check scheduled a little ahead of now on the platform's alarm,
//!   and pushes it further ahead before it comes due. A holder that dies
//!   stops pushing, its check goes off, and the broker frees what it
//!   claimed (`HELD_CHECK_PATH`), which is news to the other holders.
//!
//! A local install's one process holds everything itself under a name
//! that survives its restart, so it needs no switch ([`OneProcessHolds`]),
//! and its news is its own database's notifications.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::news::{Listening, MayMiss};
use crate::roles::CoreRole;
use crate::{Alarm, HolderPool, Wake, WakeQueue};

/// What holders are told. A platform that carries a note as a word
/// writes and reads it through serde (`held_set_changed`,
/// `access_changed`), so every note reads back as itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HolderNote {
    /// The signals that need a holder changed: one came, went, was
    /// registered again, gave up, or its holder let it go.
    HeldSetChanged,
    /// A connection was made again (a reconnect, a rotate) or removed:
    /// what a holder keeps of connections' credentials is stale. A token
    /// refresh is not told: what a holder keeps carries the moment the
    /// broker said it stops being good, and is fetched again before it.
    AccessChanged,
}

/// What a holder hears.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HolderHeard {
    Note(HolderNote),
    /// The listening started, or came back after a gap: a note may have
    /// been missed, so the holder looks again and keeps no credential.
    MissedSome,
}

impl MayMiss for HolderHeard {
    fn missed_some() -> Self {
        HolderHeard::MissedSome
    }
}

#[async_trait]
pub trait HolderNews: Send + Sync {
    /// Tell every holder `note`. Called once the writes it announces
    /// committed; an error means it did not go out for sure.
    async fn tell(&self, note: HolderNote) -> anyhow::Result<()>;

    /// Everything heard from now on. The first call starts the process's
    /// listening in the background, and [`HolderHeard::MissedSome`] says
    /// when it is up.
    fn listen(&self) -> Listening<HolderHeard>;

    /// The process is stopping: what its listening holds goes.
    async fn close(&self) -> anyhow::Result<()>;
}

/// The path the broker frees a dead holder's claims on: where a holder's
/// check goes when it comes due.
// SYNC: HELD_CHECK_PATH <-> crates/weft-broker (the `/held/check` route)
pub const HELD_CHECK_PATH: &str = "/held/check";

/// One holder's dead man's switch: if it goes off, holder `replica` is
/// taken for dead and everything it claims is freed. The body the
/// broker receives at [`HELD_CHECK_PATH`] (as a `WakeCall`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeldCheck {
    pub replica: String,
    /// Which of the holder's pushes this is: a number it never uses twice,
    /// so each push is a wake of its own, and the logs say which went off.
    pub generation: i64,
    /// When it goes off, unix ms. Whenever it arrives, the broker frees
    /// the holder's claims and never sets it again, so withdrawing the
    /// check the holder pushed always stops it.
    pub due_at_ms: i64,
}

impl HeldCheck {
    /// The wake that calls the broker when this check goes off, on the
    /// queue that never drops a wake ([`WakeQueue::Triggers`]): a dropped
    /// check would leave a dead holder's claims taken for good.
    pub fn wake(&self) -> anyhow::Result<Wake> {
        Ok(Wake {
            key: format!("holder:{}", self.replica),
            at_unix_ms: self.due_at_ms,
            generation: self.generation,
            queue: WakeQueue::Triggers,
            role: CoreRole::Broker,
            path: HELD_CHECK_PATH.to_string(),
            body: serde_json::to_value(self)?,
        })
    }
}

#[async_trait]
pub trait HolderLiveness: Send + Sync {
    /// Schedule `check`. An error means it may not be scheduled.
    async fn push(&self, check: &HeldCheck) -> anyhow::Result<()>;

    /// Drop a check pushed earlier, so it does not go off. One already
    /// gone off, or never made, is no failure.
    async fn withdraw(&self, check: &HeldCheck) -> anyhow::Result<()>;
}

/// The check on a platform's [`Alarm`]: one wake for each push, calling
/// the broker ([`HeldCheck::wake`]).
pub struct AlarmLiveness {
    alarm: std::sync::Arc<dyn Alarm>,
}

impl AlarmLiveness {
    pub fn new(alarm: std::sync::Arc<dyn Alarm>) -> Self {
        Self { alarm }
    }
}

#[async_trait]
impl HolderLiveness for AlarmLiveness {
    async fn push(&self, check: &HeldCheck) -> anyhow::Result<()> {
        self.alarm.set(check.wake()?).await
    }

    async fn withdraw(&self, check: &HeldCheck) -> anyhow::Result<()> {
        self.alarm.cancel(&check.wake()?).await
    }
}

/// An install whose one process holds every held connection itself: no
/// pool to size, and no check, since the process holds under a name that
/// survives its restart and takes its claims back under it as it starts
/// again.
pub struct OneProcessHolds;

#[async_trait]
impl HolderPool for OneProcessHolds {
    async fn resize(&self, _copies: u32) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl HolderLiveness for OneProcessHolds {
    async fn push(&self, _check: &HeldCheck) -> anyhow::Result<()> {
        Ok(())
    }

    async fn withdraw(&self, _check: &HeldCheck) -> anyhow::Result<()> {
        Ok(())
    }
}

/// Records every note told, and hands every listener what a test sends.
#[cfg(any(test, feature = "test-helpers"))]
pub mod fake {
    use super::*;
    use tokio::sync::broadcast;

    pub struct FakeHolderNews {
        pub told: parking_lot::Mutex<Vec<HolderNote>>,
        pub heard: broadcast::Sender<HolderHeard>,
    }

    impl Default for FakeHolderNews {
        fn default() -> Self {
            Self { told: parking_lot::Mutex::default(), heard: broadcast::channel(crate::news::LISTEN_CAPACITY).0 }
        }
    }

    #[async_trait]
    impl HolderNews for FakeHolderNews {
        async fn tell(&self, note: HolderNote) -> anyhow::Result<()> {
            self.told.lock().push(note);
            Ok(())
        }

        fn listen(&self) -> Listening<HolderHeard> {
            self.heard.subscribe().into()
        }

        async fn close(&self) -> anyhow::Result<()> {
            Ok(())
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FakeAlarm;

    /// A check is a wake on the queue that never drops one, to the
    /// broker's check path, carrying the check; each push is a wake of its
    /// own, and withdrawing one cancels exactly it.
    #[tokio::test]
    async fn a_check_is_a_never_dropped_wake_to_the_broker_and_each_push_its_own() {
        let alarm = std::sync::Arc::new(FakeAlarm::new());
        let liveness = AlarmLiveness::new(alarm.clone());
        let first = HeldCheck { replica: "h1".into(), generation: 1, due_at_ms: 180_000 };
        let second = HeldCheck { generation: 2, due_at_ms: 240_000, ..first.clone() };
        liveness.push(&first).await.unwrap();
        liveness.push(&second).await.unwrap();
        liveness.withdraw(&first).await.unwrap();
        let wakes = alarm.wakes();
        assert_eq!(wakes.len(), 2);
        assert_eq!((wakes[0].queue, wakes[0].role, wakes[0].path.as_str()), (WakeQueue::Triggers, CoreRole::Broker, HELD_CHECK_PATH));
        assert_eq!(serde_json::from_value::<HeldCheck>(wakes[0].body.clone()).unwrap(), first);
        assert_ne!(wakes[0].name(), wakes[1].name());
        assert_eq!(alarm.pending(), vec![wakes[1].clone()], "only the withdrawn one is cancelled");
    }
}
