//! What happens to work arriving for a trigger, by how its activation
//! stands: the one rule every way work arrives goes through.
//!
//! Work arrives four ways: a call from outside at weft's door (a webhook,
//! a form, a provider's push), an answer to a run waiting on a person, an
//! event a listener picks up itself (a timer's tick, a message on a
//! connection it holds), and a caller on the line of a live route. While
//! the trigger is on, each becomes a run (or the answer reaches its run)
//! at once. While it is off, the way it went down decides, the same for
//! all four:
//!
//! - **park**: everything happens as usual except that nothing becomes a
//!   run: the work waits, and runs once the trigger is back on, on the
//!   version it comes back with. Listeners keep listening, so a trigger
//!   that fires by itself keeps firing and each fire waits too.
//! - **hibernate**: the same until its grace window ends; after it, new
//!   work is refused and the listeners stop.
//! - **wipe**: work is refused at once.
//!
//! An activation still being set up takes work the way a parked one does:
//! its listeners may not all be ready, and the setup's end runs what
//! waited. A caller on the line is the exception: nothing holds a caller
//! (the worker it reached goes away on an update), so a caller arriving
//! while the trigger is not on is answered `503` with a `Retry-After` at
//! once, and retries (the worker's door).
//!
//! What a person SEES while a trigger is off is a separate rule, kept on
//! the activation as `fires_visible_to_consumers`: a parked trigger's
//! questions stay listed for the people who answer them, a hibernated
//! one's are hidden.

use serde::{Deserialize, Serialize};

use crate::projects::ProjectStatus;

/// What becomes of work arriving now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arrival {
    /// The trigger is on: the work becomes its run now.
    Live,
    /// The trigger is parked (or hibernating within its grace window, or
    /// being set up): the work waits for it to be back on.
    Wait,
    /// The trigger takes no work: wiped, never switched on, or past its
    /// hibernation's grace window.
    Refused,
}

/// How an activation stands, as far as arriving work is concerned: its
/// status, whether it takes fires at all, and until when.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Standing {
    pub status: ProjectStatus,
    pub accepting_fires: bool,
    pub fires_deadline_unix: Option<i64>,
}

impl Standing {
    /// What becomes of work arriving at `now_unix`.
    // SYNC: arrival <-> weft_broker_client::protocol::ACTIVATION_LISTENS, IN_GRACE_WINDOW_SQL (crates/weft-dispatcher/src/activation_store.rs)
    pub fn arrival(&self, now_unix: i64) -> Arrival {
        match self.status {
            // A signal no activation governs (a wait of a run started by
            // hand) reads as active: it has no moment it comes back at,
            // so work for it never waits.
            ProjectStatus::Active => Arrival::Live,
            ProjectStatus::Registered => Arrival::Refused,
            ProjectStatus::Activating | ProjectStatus::Deactivating | ProjectStatus::Inactive => {
                let past_grace = self.fires_deadline_unix.is_some_and(|deadline| now_unix > deadline);
                if self.accepting_fires && !past_grace {
                    Arrival::Wait
                } else {
                    Arrival::Refused
                }
            }
        }
    }
}

/// What a caller is told when what they called cannot take them right now
/// (its trigger is not on, what it needs is not running, a new version is
/// taking over), the way any server that is down or not ready answers: a
/// `503` with this and a `Retry-After` of [`NOT_NOW_RETRY_SECS`]. Nothing
/// about how the program is run, which the caller cannot change; the
/// operator's side goes to the log and `weft status`.
pub const NOT_NOW_FOR_CALLER: &str = "This service is unavailable right now; try again in a moment.";

/// How long a caller turned away with [`NOT_NOW_FOR_CALLER`] is told to wait.
pub const NOT_NOW_RETRY_SECS: u64 = 5;

#[cfg(test)]
mod tests {
    use super::*;

    fn standing(status: ProjectStatus, accepting_fires: bool, fires_deadline_unix: Option<i64>) -> Standing {
        Standing { status, accepting_fires, fires_deadline_unix }
    }

    #[test]
    fn work_waits_while_parked_or_hibernating_and_is_refused_once_wiped_or_past_grace() {
        assert_eq!(standing(ProjectStatus::Active, true, None).arrival(10), Arrival::Live);
        assert_eq!(standing(ProjectStatus::Inactive, true, None).arrival(10), Arrival::Wait, "parked");
        assert_eq!(standing(ProjectStatus::Inactive, true, Some(20)).arrival(10), Arrival::Wait, "hibernating, in grace");
        assert_eq!(standing(ProjectStatus::Inactive, true, Some(20)).arrival(21), Arrival::Refused, "hibernating, past grace");
        assert_eq!(standing(ProjectStatus::Inactive, false, Some(20)).arrival(21), Arrival::Refused, "a hibernation whose end was marked");
        assert_eq!(standing(ProjectStatus::Inactive, false, None).arrival(10), Arrival::Refused, "wiped");
        assert_eq!(standing(ProjectStatus::Activating, true, None).arrival(10), Arrival::Wait, "being set up");
        assert_eq!(standing(ProjectStatus::Deactivating, true, None).arrival(10), Arrival::Wait, "going down to park");
        assert_eq!(standing(ProjectStatus::Deactivating, false, None).arrival(10), Arrival::Refused, "going down to wipe");
        assert_eq!(standing(ProjectStatus::Registered, true, None).arrival(10), Arrival::Refused, "never switched on");
    }
}
