//! How many holders run: the copies of the listener that keep the
//! outside connections some signals need open between fires.
//!
//! A held connection must live somewhere that stays up, and that costs
//! money for as long as it runs. So holders run only while there is
//! something to hold, and as many as the held signals need: none when no
//! signal holds a connection. The dispatcher, which registers and drops
//! signals, sets the count; each holder claims what it holds through a
//! lease, so they share the work and take over from one that went.
//!
//! The count of held signals is a floor, and the holders' load moves the
//! count above it: each holder says the CPU it uses whenever it moved a
//! band (`weft_listener::load`), and the dispatcher runs as many as that
//! load, summed, needs at the target share each ([`copies_wanted`]).

use std::num::{NonZeroU32, NonZeroU8};

use async_trait::async_trait;

#[async_trait]
pub trait HolderPool: Send + Sync {
    /// Run `copies` holders; 0 stops every one. Setting the count it
    /// already has changes nothing.
    async fn resize(&self, copies: u32) -> anyhow::Result<()>;
}

/// The holders a count of held signals needs: none for none, then one
/// per `per_copy` signals, rounded up.
pub fn copies_for(held: u64, per_copy: NonZeroU32) -> u32 {
    u32::try_from(held.div_ceil(u64::from(per_copy.get()))).unwrap_or(u32::MAX)
}

/// What the holders said of their load, as the sizing reads it (the
/// `holder_load` rows).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Loads {
    /// The CPU the holders that claim signals say they use, summed, each
    /// in percent of the CPUs it is given. A holder that claims nothing
    /// holds no work its number could stand for, so it is left out.
    pub cpu_percent: u64,
    /// The room the holders gave up, summed: one that handed signals over
    /// takes only what it kept, so the copies have that much less room
    /// than one `per_copy` each.
    pub short: u64,
    /// How many held signals no holder claims now: one that went has let
    /// them go and no other holder has taken them yet (or they are new).
    pub unclaimed: u64,
    /// How many holders claim signals without having said the load of what
    /// they hold now: they took some since they said it, handed some over
    /// in saying it, or have said nothing yet.
    pub unsettled: u32,
}

/// The holders to run for `held` signals, `per_copy` to a holder, each
/// meant to run at `target_percent` of its CPUs, when the pool was last
/// set to `last_set` (`None`: never, as far as the database knows) and the
/// holders said `loads`. As many as the load summed needs at that share
/// each, and never under what the signals need with the room the holders
/// gave up counted in, nor over one holder per signal (a signal is never
/// split, so a holder more holds nothing). The count does not go down
/// while signals are on their way to a holder or a holder's load is not
/// measured on what it holds: the load said was measured before they
/// moved, and the sum would come out short of what the copies left carry.
pub fn copies_wanted(held: u64, per_copy: NonZeroU32, target_percent: NonZeroU8, last_set: Option<u32>, loads: Loads) -> u32 {
    let floor = copies_for(held.saturating_add(loads.short), per_copy);
    let by_load = u32::try_from(loads.cpu_percent.div_ceil(u64::from(target_percent.get()))).unwrap_or(u32::MAX);
    let wanted = floor.max(by_load);
    let wanted = match last_set {
        Some(n) if wanted < n && (loads.unclaimed > 0 || loads.unsettled > 0) => n,
        _ => wanted,
    };
    wanted.min(u32::try_from(held).unwrap_or(u32::MAX))
}

/// Records every resize.
#[cfg(any(test, feature = "test-helpers"))]
pub mod fake {
    use super::*;

    #[derive(Default)]
    pub struct FakeHolderPool {
        pub sizes: parking_lot::Mutex<Vec<u32>>,
    }

    #[async_trait]
    impl HolderPool for FakeHolderPool {
        async fn resize(&self, copies: u32) -> anyhow::Result<()> {
            self.sizes.lock().push(copies);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_held_signal_runs_no_holder_and_more_run_more() {
        assert_eq!(copies_for(0, PER_COPY), 0);
        assert_eq!(copies_for(1, PER_COPY), 1);
        assert_eq!(copies_for(200, PER_COPY), 1);
        assert_eq!(copies_for(201, PER_COPY), 2);
    }

    const FIFTY: NonZeroU8 = NonZeroU8::new(50).unwrap();
    const PER_COPY: NonZeroU32 = NonZeroU32::new(200).unwrap();

    fn said(cpu_percent: u64) -> Loads {
        Loads { cpu_percent, ..Loads::default() }
    }

    /// With nothing said, the count of held signals decides.
    #[test]
    fn with_nothing_said_the_held_count_decides() {
        assert_eq!(copies_wanted(0, PER_COPY, FIFTY, Some(3), said(300)), 0, "nothing held runs nothing, whatever ran");
        assert_eq!(copies_wanted(450, PER_COPY, FIFTY, None, Loads::default()), 3);
        assert_eq!(copies_wanted(450, PER_COPY, FIFTY, Some(5), Loads::default()), 3, "copies a load no longer needs go");
    }

    /// The floor wins when the signals need more copies than their load.
    #[test]
    fn the_floor_wins_over_a_light_load() {
        assert_eq!(copies_wanted(450, PER_COPY, FIFTY, Some(3), said(60)), 3);
        assert_eq!(copies_wanted(300, PER_COPY, FIFTY, Some(2), Loads { cpu_percent: 40, short: 150, ..Loads::default() }), 3, "two copies hold only 250 now");
    }

    /// The load wins when the copies the signals need would run over the
    /// target: the sum over the target share, rounded up.
    #[test]
    fn the_load_wins_over_a_light_count() {
        assert_eq!(copies_wanted(300, PER_COPY, FIFTY, Some(2), said(160)), 4);
        assert_eq!(copies_wanted(300, PER_COPY, FIFTY, Some(2), said(150)), 3, "exactly three at the target");
        assert_eq!(copies_wanted(300, PER_COPY, FIFTY, Some(2), said(151)), 4);
        assert_eq!(copies_wanted(300, PER_COPY, FIFTY, Some(4), said(90)), 2, "a load that went down takes copies away");
    }

    #[test]
    fn never_more_holders_than_signals() {
        assert_eq!(copies_wanted(2, PER_COPY, FIFTY, Some(2), said(400)), 2, "a signal is never split, so a third holder holds nothing");
        assert_eq!(copies_wanted(1, PER_COPY, FIFTY, Some(1), Loads { short: 199, ..said(90) }), 1);
    }

    /// Dead copies are left out by what the sizing reads: their claims
    /// freed (the check went off), they claim nothing, so no number of
    /// theirs is in the sum; while signals are on their way to a holder,
    /// or a holder took some and has not said its load since, the count
    /// stays where it is.
    #[test]
    fn no_copy_goes_while_the_load_said_is_behind_what_moved() {
        let moving = Loads { unclaimed: 5, ..said(90) };
        assert_eq!(copies_wanted(300, PER_COPY, FIFTY, Some(4), moving), 4, "the one that went let go, and nobody took its signals yet");
        let behind = Loads { unsettled: 1, ..said(90) };
        assert_eq!(copies_wanted(300, PER_COPY, FIFTY, Some(4), behind), 4, "a holder took them and measures afresh");
        assert_eq!(copies_wanted(300, PER_COPY, FIFTY, Some(4), said(90)), 2, "once each said its load, the sum decides");
        assert_eq!(copies_wanted(300, PER_COPY, FIFTY, Some(2), Loads { unsettled: 1, ..said(200) }), 4, "adding stays open");
        assert_eq!(copies_wanted(1, PER_COPY, FIFTY, Some(4), Loads { unsettled: 1, ..said(90) }), 1, "the cap holds whatever moves");
        assert_eq!(copies_wanted(300, PER_COPY, FIFTY, None, Loads { unsettled: 1, ..said(90) }), 2, "never set: nothing to hold on to");
    }
}
