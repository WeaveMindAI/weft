//! An entry's limits, held at the worker's door: how often one caller may
//! call, how often the entry may start runs, how many of its runs may go
//! at once, and how many refused tokens one address may present.
//!
//! Every count is kept in this worker's memory, so no call waits on a
//! database. Once a second the worker states its counts of the minute and
//! hears the project's other copies' (`/v1/door/tick`), so with several
//! copies a count is the copies' together, as of the last second: several
//! copies can let through at most about a second's worth of calls more than
//! the limit. One copy is exact. "At once" is held per copy: each takes its
//! share of the limit, the limit divided by the copies alive.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use weft_core::signal::ResolvedLimits;

pub use weft_core::signal::limits::{Limited, Refused};
use weft_core::signal::limits::{failed_key, ran_key, window};

/// The counts one copy keeps (see the module doc).
pub struct Limits {
    state: Mutex<Counts>,
}

#[derive(Default)]
struct Counts {
    /// The minute the counts are of.
    window_start: i64,
    /// What this copy counted this minute.
    mine: HashMap<String, i64>,
    /// What the other copies counted this minute, as of the last tick.
    others: HashMap<String, i64>,
    /// Copies alive at the last tick, this one included.
    copies: u32,
    /// Runs of each entry going on this copy.
    going: HashMap<String, u32>,
}

/// Held for as long as an admitted run goes: it counts toward its entry's
/// "at once" until it is dropped.
pub struct Going {
    limits: Arc<Limits>,
    token: String,
    /// The minute it was counted in, and the keys it was counted under.
    counted: (i64, Vec<String>),
}

impl std::fmt::Debug for Going {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Going").field("token", &self.token).finish()
    }
}

impl Drop for Going {
    fn drop(&mut self) {
        let mut counts = self.limits.state.lock().expect("door limits");
        if let Some(going) = counts.going.get_mut(&self.token) {
            *going = going.saturating_sub(1);
            if *going == 0 {
                counts.going.remove(&self.token);
            }
        }
    }
}

impl Going {
    /// The run failed: counted toward its entry's failed runs this minute
    /// (`weft status`), however it is kept.
    pub fn failed(&self, now: i64) {
        let mut counts = self.limits.state.lock().expect("door limits");
        counts.roll(window(now).0);
        *counts.mine.entry(failed_key(&self.token)).or_default() += 1;
    }

    /// The admitted work did not become a run (it waits in its trigger's
    /// queue, or was born already): it is not counted this minute, and its
    /// room is free.
    pub fn uncount(self) {
        let mut counts = self.limits.state.lock().expect("door limits");
        let (minute, keys) = &self.counted;
        if counts.window_start == *minute {
            for key in keys {
                if let Some(hits) = counts.mine.get_mut(key) {
                    *hits = hits.saturating_sub(1);
                }
            }
        }
    }
}

impl Limits {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { state: Mutex::new(Counts { copies: 1, ..Counts::default() }) })
    }

    /// Admit one call to the entry `token` by `caller` (the identity its
    /// gate established, else the instance, else the address, already
    /// spelled as a key) at `now`: the caller's count, then the entry's,
    /// then its room. Counted only when admitted; a refusal is counted as
    /// one for `weft status`.
    pub fn admit(self: &Arc<Self>, token: &str, caller: &str, limits: &ResolvedLimits, now: i64) -> Result<Going, Refused> {
        let mut counts = self.state.lock().expect("door limits");
        let (start, left) = window(now);
        counts.roll(start);
        let checks = [
            (limits.per_caller_per_minute, format!("c:{token}:{caller}"), Limited::PerCaller),
            (limits.per_minute, format!("e:{token}"), Limited::PerEntry),
        ];
        for (limit, key, reason) in &checks {
            if let Some(limit) = limit {
                if counts.total(key) + 1 > *limit as i64 {
                    counts.refused(token, *reason);
                    return Err(Refused { reason: *reason, retry_after_secs: left });
                }
            }
        }
        if let Some(at_once) = limits.at_once {
            let share = (at_once / counts.copies.max(1)).max(1);
            if counts.going.get(token).copied().unwrap_or(0) >= share {
                counts.refused(token, Limited::AtOnce);
                return Err(Refused { reason: Limited::AtOnce, retry_after_secs: 5 });
            }
        }
        let mut counted = vec![ran_key(token)];
        for (limit, key, _) in checks {
            if limit.is_some() {
                counted.push(key);
            }
        }
        for key in &counted {
            *counts.mine.entry(key.clone()).or_default() += 1;
        }
        *counts.going.entry(token.to_string()).or_default() += 1;
        Ok(Going { limits: self.clone(), token: token.to_string(), counted: (start, counted) })
    }

    /// Whether `address` may present a token at all this minute: it has
    /// not presented more than `bound` refused ones.
    pub fn address_allowed(&self, address: &str, bound: Option<u32>, now: i64) -> Result<(), Refused> {
        let Some(bound) = bound else { return Ok(()) };
        let mut counts = self.state.lock().expect("door limits");
        let (start, left) = window(now);
        counts.roll(start);
        if counts.total(&invalid_tokens_key(address)) >= bound as i64 {
            return Err(Refused { reason: Limited::InvalidTokens, retry_after_secs: left });
        }
        Ok(())
    }

    /// Count one refused token from `address`.
    pub fn token_refused(&self, address: &str, now: i64) {
        let mut counts = self.state.lock().expect("door limits");
        counts.roll(window(now).0);
        *counts.mine.entry(invalid_tokens_key(address)).or_default() += 1;
    }

    /// How many runs of each entry go on this copy right now: what a drain
    /// of those entries waits for, stated at a tick.
    pub fn going(&self) -> std::collections::BTreeMap<String, u32> {
        self.state.lock().expect("door limits").going.iter().map(|(token, going)| (token.clone(), *going)).collect()
    }

    /// This copy's counts of the minute, to state at a tick.
    pub fn mine(&self, now: i64) -> (i64, Vec<(String, i64)>) {
        let mut counts = self.state.lock().expect("door limits");
        let start = window(now).0;
        counts.roll(start);
        (start, counts.mine.iter().map(|(key, hits)| (key.clone(), *hits)).collect())
    }

    /// What a tick heard of the minute `window_start`: the other copies'
    /// counts and how many copies are alive. A tick of a minute already
    /// over changes nothing.
    pub fn heard(&self, window_start: i64, others: impl IntoIterator<Item = (String, i64)>, copies: u32) {
        let mut counts = self.state.lock().expect("door limits");
        counts.copies = copies.max(1);
        if counts.window_start == window_start {
            counts.others = others.into_iter().collect();
        }
    }
}

impl Counts {
    /// Start counting the minute beginning at `start`, when it is a new one.
    fn roll(&mut self, start: i64) {
        if self.window_start != start {
            self.window_start = start;
            self.mine.clear();
            self.others.clear();
        }
    }

    fn total(&self, key: &str) -> i64 {
        self.mine.get(key).copied().unwrap_or(0) + self.others.get(key).copied().unwrap_or(0)
    }

    fn refused(&mut self, token: &str, reason: Limited) {
        *self.mine.entry(reason.refusal_key(token)).or_default() += 1;
    }
}

/// The key a refused token from `address` is counted under. Its second
/// part is no route's token, so no route reads it back at a tick; each copy
/// bounds what it saw itself.
fn invalid_tokens_key(address: &str) -> String {
    format!("t:-:{address}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits(per_caller: Option<u32>, per_minute: Option<u32>, at_once: Option<u32>) -> ResolvedLimits {
        ResolvedLimits { per_caller_per_minute: per_caller, per_minute, at_once }
    }

    #[test]
    fn a_caller_is_held_to_its_count_and_the_entry_to_its_own() {
        let door = Limits::new();
        let l = limits(Some(2), Some(3), None);
        let _a = door.admit("tok", "ip:a", &l, 0).unwrap();
        let _b = door.admit("tok", "ip:a", &l, 1).unwrap();
        assert_eq!(door.admit("tok", "ip:a", &l, 2).unwrap_err().reason, Limited::PerCaller);
        let _c = door.admit("tok", "ip:b", &l, 3).unwrap();
        assert_eq!(door.admit("tok", "ip:c", &l, 4).unwrap_err().reason, Limited::PerEntry);
        // A new minute counts from nothing.
        assert!(door.admit("tok", "ip:a", &l, 60).is_ok());
    }

    #[test]
    fn a_refused_call_is_not_counted_against_the_limit_but_is_counted_as_refused() {
        let door = Limits::new();
        let l = limits(Some(1), None, None);
        let _a = door.admit("tok", "ip:a", &l, 0).unwrap();
        for _ in 0..3 {
            assert!(door.admit("tok", "ip:a", &l, 0).is_err());
        }
        let (_, mine) = door.mine(0);
        let mine: HashMap<String, i64> = mine.into_iter().collect();
        assert_eq!(mine["c:tok:ip:a"], 1);
        assert_eq!(mine["refused:tok:per_caller"], 3);
    }

    #[test]
    fn the_other_copies_counts_join_this_ones() {
        let door = Limits::new();
        let l = limits(Some(5), None, None);
        door.mine(10);
        door.heard(0, [("c:tok:ip:a".to_string(), 4)], 2);
        let _a = door.admit("tok", "ip:a", &l, 10).unwrap();
        assert!(door.admit("tok", "ip:a", &l, 10).is_err(), "4 elsewhere and 1 here is the limit");
        // A tick of a minute already over is not read into this one.
        door.heard(-60, [("c:tok:ip:b".to_string(), 99)], 2);
        assert!(door.admit("tok", "ip:b", &l, 11).is_ok());
    }

    #[test]
    fn runs_at_once_are_held_per_copy_and_free_their_room_when_they_end() {
        let door = Limits::new();
        let l = limits(None, None, Some(4));
        door.heard(0, [], 2);
        let a = door.admit("tok", "ip:a", &l, 0).unwrap();
        let _b = door.admit("tok", "ip:b", &l, 0).unwrap();
        assert_eq!(door.admit("tok", "ip:c", &l, 0).unwrap_err().reason, Limited::AtOnce, "this copy's share is 2");
        drop(a);
        assert!(door.admit("tok", "ip:c", &l, 0).is_ok());
    }

    /// Work that was admitted and then did not start is neither counted
    /// this minute nor holding a room.
    #[test]
    fn work_that_did_not_start_is_uncounted() {
        let door = Limits::new();
        let l = limits(None, Some(1), Some(1));
        door.admit("tok", "ip:a", &l, 0).unwrap().uncount();
        let _a = door.admit("tok", "ip:a", &l, 1).expect("the uncounted one left the minute's count and its room");
        assert_eq!(door.admit("tok", "ip:a", &l, 2).unwrap_err().reason, Limited::PerEntry);
    }

    #[test]
    fn an_address_guessing_tokens_is_refused_for_the_rest_of_the_minute() {
        let door = Limits::new();
        for _ in 0..3 {
            door.token_refused("1.2.3.4", 0);
        }
        assert!(door.address_allowed("1.2.3.4", Some(3), 1).is_err());
        assert!(door.address_allowed("1.2.3.4", None, 1).is_ok());
        assert!(door.address_allowed("5.6.7.8", Some(3), 1).is_ok());
        assert!(door.address_allowed("1.2.3.4", Some(3), 60).is_ok());
    }
}
