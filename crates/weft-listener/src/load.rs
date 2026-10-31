//! A holder's load, as the pool's size follows it.
//!
//! The count of held signals sizes the pool of holders, but one busy
//! connection (a chatty socket, a fast stream) costs a holder far more
//! than a quiet subscription, and both count as one. So each holder in a
//! pool watches its own CPU and says the share it uses, and weft runs as
//! many holders as those shares, summed, need at the target share each
//! (`HolderSettings::target_cpu_percent`). A holder says its load when
//! it first measured it, when it moved a band ([`LOAD_BAND_PERCENT`])
//! away from what it said last, and once more after its share changed
//! (it took signals or handed some over): a pool whose load holds still
//! writes nothing to the database, since the watching runs on this
//! process's own clock and reads only this process. At the high mark
//! (`HolderSettings::high_cpu_percent`) it also hands part of its
//! connections over, keeping what brings it back to the target. So does
//! a holder over the target while a spare copy is up: one the sizing
//! added for the load, which holds nothing until work is handed to it.
//! With no spare up, a holder between the target and the high mark keeps
//! its work, so a reconnect happens only when there is somewhere for the
//! work to go.
//!
//! The load is judged on a minute of samples ([`SAMPLES`] taken
//! [`sample_every`] apart), so a burst (every connection coming back at
//! once) is not taken for load. The samples start over whenever what the
//! holder holds changes: what it measured before was the load of a share
//! it no longer has.

use std::collections::VecDeque;
use std::time::Duration;

use weft_platform_traits::config::{HolderSettings, LOAD_BAND_PERCENT};

/// How many samples a level is judged on.
pub const SAMPLES: usize = 6;

/// How far apart a holder samples its CPU.
pub fn sample_every() -> Duration {
    weft_core::time_scale::scaled(Duration::from_secs(10))
}

/// This process's CPU.
pub trait CpuUse: Send {
    /// The share of the CPUs this process may use that it used since the
    /// last call (since it was made, for the first): 1.0 is all of them.
    fn busy(&mut self) -> anyhow::Result<f64>;
}

/// The CPU this process used, from the operating system
/// (`getrusage`), over the CPUs it is given ([`cpus_given`]).
pub struct ProcessCpu {
    cpus: f64,
    last: (std::time::Instant, Duration),
}

impl ProcessCpu {
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self { cpus: cpus_given()?, last: (std::time::Instant::now(), cpu_used()?) })
    }
}

/// Where a container's CPU allowance is: its cgroup's (cgroup v2), which
/// a container sees as the root of its own tree.
const CPU_MAX: &str = "/sys/fs/cgroup/cpu.max";

/// The CPUs this process is given: its container's allowance
/// ([`CPU_MAX`], [`allowance_of`]), which can be a fraction of one (a
/// holder given half a CPU), or, when there is none (the file is missing,
/// or says `max`, as on a machine of one's own), the CPUs it may run on.
fn cpus_given() -> anyhow::Result<f64> {
    let cpu_max = match std::fs::read_to_string(CPU_MAX) {
        Ok(text) => Some(text),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(anyhow::anyhow!("read {CPU_MAX}: {e}")),
    };
    if let Some(cpus) = allowance_of(cpu_max.as_deref()).map_err(|e| anyhow::anyhow!("{CPU_MAX}: {e}"))? {
        return Ok(cpus);
    }
    let cpus = std::thread::available_parallelism().map_err(|e| anyhow::anyhow!("how many CPUs this process may use: {e}"))?;
    Ok(cpus.get() as f64)
}

/// The CPUs a cgroup v2 `cpu.max` allows (`<quota> <period>`, in
/// microseconds of CPU per microseconds of time), `None` when there is
/// no file or no limit (`max`). Anything else is refused: a holder that
/// misreads its allowance would size the pool on a wrong load.
pub fn allowance_of(cpu_max: Option<&str>) -> Result<Option<f64>, String> {
    let Some(text) = cpu_max else { return Ok(None) };
    let mut fields = text.split_whitespace();
    let (Some(quota), Some(period), None) = (fields.next(), fields.next(), fields.next()) else {
        return Err(format!("'{}' is not '<quota> <period>'", text.trim()));
    };
    if quota == "max" {
        return Ok(None);
    }
    let number = |field: &str| field.parse::<u64>().ok().filter(|n| *n > 0).ok_or_else(|| format!("'{field}' in '{}' is not a positive number", text.trim()));
    Ok(Some(number(quota)? as f64 / number(period)? as f64))
}

impl CpuUse for ProcessCpu {
    fn busy(&mut self) -> anyhow::Result<f64> {
        let now = (std::time::Instant::now(), cpu_used()?);
        let share = share_of(now.1.saturating_sub(self.last.1), now.0 - self.last.0, self.cpus);
        self.last = now;
        Ok(share)
    }
}

/// The CPU time this process used since it started, every thread's, in
/// the kernel and out.
fn cpu_used() -> anyhow::Result<Duration> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: `getrusage` writes the whole struct when it returns 0, and
    // the struct is read only then.
    let usage = unsafe {
        if libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) != 0 {
            return Err(anyhow::anyhow!("read this process's CPU time: {}", std::io::Error::last_os_error()));
        }
        usage.assume_init()
    };
    let time = |t: libc::timeval| {
        Duration::from_secs(u64::try_from(t.tv_sec).unwrap_or(0)) + Duration::from_micros(u64::try_from(t.tv_usec).unwrap_or(0))
    };
    Ok(time(usage.ru_utime) + time(usage.ru_stime))
}

/// The share of `cpus` CPUs (a fraction of one when the container is
/// given less) that `used` of CPU time is over `over` of wall time.
pub fn share_of(used: Duration, over: Duration, cpus: f64) -> f64 {
    if over.is_zero() || cpus <= 0.0 {
        return 0.0;
    }
    used.as_secs_f64() / (over.as_secs_f64() * cpus)
}

/// The shares a holder's load is judged against (1.0 is all its CPUs).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Shares {
    /// Where it hands connections over.
    pub high: f64,
    /// What each holder is meant to run at: what one that hands
    /// connections over keeps, and under which one that did gets its full
    /// room back.
    pub target: f64,
}

impl Shares {
    pub fn of(settings: &HolderSettings) -> Self {
        Self { high: f64::from(settings.high_cpu_percent) / 100.0, target: f64::from(settings.target_cpu_percent) / 100.0 }
    }
}

/// What a holder says of its load.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Say {
    /// The CPU it uses, in percent of the CPUs it is given: of what it
    /// keeps, when it hands connections over, so the work moved is counted
    /// once, by whoever takes it.
    pub cpu_percent: u16,
    /// What it hands over, if anything ([`hand_over`]).
    pub hand: Option<Hand>,
    /// It handed connections over since its room was last full, and its
    /// load is under the target now: it takes its full room again.
    pub room_back: bool,
}

/// A hand-over: keep `keep` of the connections up and hand the rest over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Hand {
    pub keep: usize,
    /// Only to a spare, or not at all (the holder is only over the target);
    /// `false` over the high mark, which sheds its work either way.
    pub spare_only: bool,
}

/// The band detector: samples in, what to say out.
#[derive(Debug, Clone)]
pub struct Watch {
    shares: Shares,
    samples: VecDeque<f64>,
    /// The load weft keeps for this holder, measured on what it holds, as
    /// far as it knows: what it last said, or `None` (it said nothing, what
    /// it said is behind what it holds, or the report's answer was lost).
    said: Option<u16>,
    /// The most a spare up could take, as the last look said: one this
    /// holder has not handed work to since. `None`: no spare.
    spare: Option<u32>,
    /// Whether it handed connections over and has not had its full room
    /// back since: it takes only what it kept until its load is under the
    /// target, and going under is said even within the band, or the room
    /// would stay lowered for good on a load that settles just under it.
    lowered: bool,
}

impl Watch {
    pub fn new(shares: Shares) -> Self {
        Self { shares, samples: VecDeque::with_capacity(SAMPLES), said: None, spare: None, lowered: false }
    }

    /// Start the samples over: what the holder measured so far was the
    /// load of a share it no longer has.
    pub fn restart(&mut self) {
        self.samples.clear();
    }

    /// What a look found: whether it `moved` what this holder holds, the
    /// load weft keeps for it now, and whether a spare copy is up. A change
    /// of what it holds or of the load kept starts the samples over, and a
    /// load weft no longer keeps is said again once measured afresh.
    pub fn looked(&mut self, moved: bool, kept: Option<u16>, spare: Option<u32>) {
        if moved || kept != self.said {
            self.restart();
        }
        self.said = kept;
        self.spare = spare;
    }

    /// `say` was said, and weft keeps it. The samples start over, so the
    /// next load said is a full window later and measured on samples of
    /// its own: a load that jumps is said at most twice (the window that
    /// caught the jump, then one of the new load alone), however many
    /// bands it crossed. A load said handing connections over is behind
    /// what the holder holds then, so it is said again once measured, and
    /// the spare it handed them to is taken to have work until a look says
    /// otherwise: another holder may hand it work too.
    pub fn said(&mut self, say: Say) {
        self.restart();
        self.said = if say.hand.is_some() { None } else { Some(say.cpu_percent) };
        if say.hand.is_some() {
            self.spare = None;
            self.lowered = true;
        }
        if say.room_back {
            self.lowered = false;
        }
    }

    /// Take one sample, `share` of the CPUs used since the last, with `up`
    /// connections up here: what to say, once a full window has something
    /// to hand over (over the high mark, or over the target with a spare
    /// up, [`hand_over`]), or its load is a band or more from what weft
    /// keeps (or weft keeps none). Otherwise nothing, and what was said
    /// stands.
    pub fn sample(&mut self, share: f64, up: usize) -> Option<Say> {
        if self.samples.len() == SAMPLES {
            self.samples.pop_front();
        }
        self.samples.push_back(share);
        if self.samples.len() < SAMPLES {
            return None;
        }
        let load = self.samples.iter().sum::<f64>() / SAMPLES as f64;
        if let Some(hand) = hand_over(up, load, self.shares, self.spare) {
            // `up` is at least 2 here, and `keep` under it.
            return Some(Say { cpu_percent: percent_of(load * hand.keep as f64 / up as f64), hand: Some(hand), room_back: false });
        }
        let cpu_percent = percent_of(load);
        let room_back = self.lowered && load < self.shares.target;
        let moved = match self.said {
            None => true,
            Some(said) => cpu_percent.abs_diff(said) >= u16::from(LOAD_BAND_PERCENT),
        };
        (moved || room_back).then_some(Say { cpu_percent, hand: None, room_back })
    }
}

/// A share of the CPUs (1.0 is all of them) in whole percent.
fn percent_of(share: f64) -> u16 {
    // Clamped first, so the cast holds it.
    (share * 100.0).round().clamp(0.0, f64::from(u16::MAX)) as u16
}

/// Of `up` connections carrying `load`, what to hand over, if anything,
/// as if each cost the same. Over the high mark, it keeps what carries the
/// target, rounded up, and hands the rest to a spare or to any holder,
/// always at least one, as long as one stays (one connection is never
/// split). Over the target with a spare up that could take `spare` more,
/// it keeps what brings it nearest the target and hands the rest to the
/// spare only, at least one and no more than the spare could take: a copy
/// the sizing added for the load gets work even when a holder's share is
/// over the target by less than one connection, or it would stay empty for
/// good. Otherwise nothing: with no spare up, a reconnect is worth it only
/// over the high mark.
pub fn hand_over(up: usize, load: f64, shares: Shares, spare: Option<u32>) -> Option<Hand> {
    if up < 2 || load <= 0.0 {
        return None;
    }
    let carry = up as f64 * shares.target / load;
    // A count of connections, clamped to them, so the casts hold it.
    let keep = |carry: f64| (carry as usize).clamp(1, up - 1);
    if load >= shares.high {
        return Some(Hand { keep: keep(carry.ceil()), spare_only: false });
    }
    let room = spare.filter(|room| *room > 0 && load > shares.target)?;
    let room = usize::try_from(room).unwrap_or(usize::MAX);
    Some(Hand { keep: keep(carry.round()).max(up.saturating_sub(room)), spare_only: true })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHARES: Shares = Shares { high: 0.7, target: 0.5 };

    /// Feed `share` until the window is full, returning what the last
    /// sample said.
    fn fill(watch: &mut Watch, share: f64, up: usize) -> Option<Say> {
        let mut said = None;
        for _ in 0..SAMPLES {
            said = watch.sample(share, up);
        }
        said
    }

    fn load(cpu_percent: u16) -> Say {
        Say { cpu_percent, hand: None, room_back: false }
    }

    fn room_back(cpu_percent: u16) -> Say {
        Say { cpu_percent, hand: None, room_back: true }
    }

    fn shed(cpu_percent: u16, keep: usize) -> Say {
        Say { cpu_percent, hand: Some(Hand { keep, spare_only: false }), room_back: false }
    }

    fn to_spare(cpu_percent: u16, keep: usize) -> Say {
        Say { cpu_percent, hand: Some(Hand { keep, spare_only: true }), room_back: false }
    }

    /// Every sample of `shares` taken with `up` connections, each load
    /// said kept as weft would keep it: what was said, in order.
    fn said_over(watch: &mut Watch, shares: impl Iterator<Item = f64>, up: usize) -> Vec<Say> {
        let mut said = Vec::new();
        for share in shares {
            if let Some(say) = watch.sample(share, up) {
                watch.said(say);
                said.push(say);
            }
        }
        said
    }

    #[test]
    fn the_first_full_window_is_said_and_nothing_before_it() {
        let mut watch = Watch::new(SHARES);
        for _ in 0..SAMPLES - 1 {
            assert_eq!(watch.sample(0.95, 4), None, "a window not full yet judges nothing");
        }
        let mut idle = Watch::new(SHARES);
        assert_eq!(fill(&mut idle, 0.02, 4), Some(load(2)), "even a load near none is said once, so the sizing reads it");
    }

    /// A load that stays within a band of what was said writes nothing,
    /// however long it runs; one that moves a band is said, up or down.
    #[test]
    fn a_load_is_said_again_only_once_it_moved_a_band() {
        let mut watch = Watch::new(SHARES);
        assert_eq!(said_over(&mut watch, std::iter::repeat_n(0.40, SAMPLES), 4), vec![load(40)]);
        assert!(said_over(&mut watch, std::iter::repeat_n(0.49, 100), 4).is_empty(), "nine points up is within the band");
        assert!(said_over(&mut watch, std::iter::repeat_n(0.31, 100), 4).is_empty(), "nine points down too");
        assert_eq!(said_over(&mut watch, std::iter::repeat_n(0.50, SAMPLES), 4), vec![load(50)], "ten up crosses");
        assert_eq!(said_over(&mut watch, std::iter::repeat_n(0.38, SAMPLES * 3), 4), vec![load(38)], "twelve down crosses, said once");
    }

    /// Noise at the edge of a band never says twice: the band moves with
    /// what was said, so coming back takes a whole band again.
    #[test]
    fn noise_around_the_edge_of_a_band_says_once() {
        let mut watch = Watch::new(SHARES);
        assert_eq!(said_over(&mut watch, std::iter::repeat_n(0.30, SAMPLES), 4), vec![load(30)]);
        // Windows of this swing between 39 and 41: around the edge at 40.
        let edge = (0..300).map(|i| if i % 4 < 2 { 0.36 } else { 0.44 });
        let said = said_over(&mut watch, edge, 4);
        assert_eq!(said.len(), 1, "a load drifting onto the edge is said once, then the noise around it never again: {said:?}");
        assert!((40..=41).contains(&said[0].cpu_percent), "said as it reached the edge: {said:?}");
    }

    /// Over the high mark with more than one connection up, every window
    /// measured afresh hands some over, saying the load of what it keeps;
    /// one connection is never split, so a holder with one only says its
    /// load.
    #[test]
    fn over_the_high_mark_a_holder_hands_over_until_one_is_left() {
        let mut watch = Watch::new(SHARES);
        let said = fill(&mut watch, 0.8, 4).expect("over the high mark");
        assert_eq!(said, shed(60, 3), "three of four at 80% carry 60%");
        watch.said(said);
        for _ in 0..SAMPLES - 1 {
            assert_eq!(watch.sample(0.8, 3), None, "the window starts over after a hand-over");
        }
        assert_eq!(watch.sample(0.8, 3), Some(shed(53, 2)), "still over: it hands over again");
        let mut one = Watch::new(SHARES);
        assert_eq!(said_over(&mut one, std::iter::repeat_n(0.95, SAMPLES * 3), 1), vec![load(95)]);
        let mut none = Watch::new(SHARES);
        assert_eq!(fill(&mut none, 0.95, 0), Some(load(95)));
    }

    /// A load weft no longer keeps (the holder took signals, or the answer
    /// to its report was lost) is said again, but only once measured
    /// afresh; so is one after the holder's share moved.
    #[test]
    fn a_load_behind_or_a_moved_share_is_measured_afresh() {
        let mut watch = Watch::new(SHARES);
        let said = fill(&mut watch, 0.2, 2).unwrap();
        watch.said(said);
        assert_eq!(fill(&mut watch, 0.2, 2), None, "within its band");
        watch.looked(false, Some(20), None);
        assert_eq!(watch.sample(0.2, 2), None, "kept as said: nothing to say");
        watch.looked(false, None, None);
        for _ in 0..SAMPLES - 1 {
            assert_eq!(watch.sample(0.2, 2), None, "behind: a new window first");
        }
        assert_eq!(watch.sample(0.2, 2), Some(load(20)), "said again though it stayed in its band");
        watch.said(load(20));
        fill(&mut watch, 0.2, 2);
        watch.looked(true, Some(20), None);
        for _ in 0..SAMPLES - 1 {
            assert_eq!(watch.sample(0.5, 2), None, "moved: the samples from before are gone, so they cannot tip a window early");
        }
        assert_eq!(watch.sample(0.5, 2), Some(load(50)));
    }

    /// A load that jumps across bands is said for the window that caught
    /// the jump, then once more for the new load alone, never once per band.
    #[test]
    fn a_jump_across_bands_is_said_twice_at_most() {
        let mut watch = Watch::new(SHARES);
        assert_eq!(said_over(&mut watch, std::iter::repeat_n(0.65, SAMPLES * 2), 4), vec![load(65)]);
        let said = said_over(&mut watch, std::iter::repeat_n(0.05, SAMPLES * 4), 4);
        assert_eq!(said, vec![load(55), load(5)], "the first window with the jump in it, then the new load");
    }

    /// After a hand-over, the room comes back once the load is under the
    /// target, said even when the load moved less than a band; over the
    /// target it stays what was kept.
    #[test]
    fn the_room_comes_back_once_the_load_is_under_the_target() {
        let mut watch = Watch::new(SHARES);
        let handed = fill(&mut watch, 0.8, 4).unwrap();
        watch.said(handed);
        let said = fill(&mut watch, 0.55, 3).unwrap();
        assert_eq!(said, load(55), "measured on what it kept, still over the target: the room stays");
        watch.said(said);
        assert_eq!(said_over(&mut watch, std::iter::repeat_n(0.47, SAMPLES * 3), 3), vec![room_back(47)], "under the target by eight points: said once, room back");
        assert!(said_over(&mut watch, std::iter::repeat_n(0.40, SAMPLES * 3), 3).is_empty(), "room full: a move under a band says nothing");
        let mut never = Watch::new(SHARES);
        let said = fill(&mut never, 0.55, 3).unwrap();
        never.said(said);
        assert!(said_over(&mut never, std::iter::repeat_n(0.47, SAMPLES * 3), 3).is_empty(), "a room never lowered has nothing to come back");
    }

    #[test]
    fn a_holder_over_the_high_mark_keeps_enough_to_land_on_the_target() {
        let over = |up, load| hand_over(up, load, SHARES, None).map(|hand| (hand.keep, hand.spare_only));
        assert_eq!(over(10, 1.0), Some((5, false)), "at full load, half brings it to 50%");
        assert_eq!(over(10, 0.7), Some((8, false)));
        assert_eq!(over(2, 1.0), Some((1, false)));
        assert_eq!(hand_over(3, 0.71, Shares { high: 0.7, target: 0.7 }, None).map(|h| h.keep), Some(2), "never all of them");
        assert_eq!(over(4, 50.0), Some((1, false)), "at least one");
        assert_eq!(over(1, 1.0), None, "one connection is never split");
        assert_eq!(hand_over(10, 0.9, SHARES, Some(200)), Some(Hand { keep: 6, spare_only: false }), "over the high mark, it sheds whether a spare is up or not");
    }

    /// Between the target and the high mark a holder hands over only while
    /// a spare is up, keeping what brings it nearest the target, and at
    /// least one connection goes: a spare added for a load just over the
    /// target gets work, even from a holder whose connections each carry
    /// more than the excess.
    #[test]
    fn between_the_target_and_the_high_mark_only_a_spare_takes_work() {
        let spare = |up, load| hand_over(up, load, SHARES, Some(200)).map(|hand| (hand.keep, hand.spare_only));
        assert_eq!(hand_over(10, 0.65, SHARES, None), None, "no spare: nowhere for the work to go");
        assert_eq!(spare(10, 0.65), Some((8, true)), "eight carry 52%, the nearest to 50%");
        assert_eq!(spare(6, 0.65), Some((5, true)));
        assert_eq!(spare(4, 0.65), Some((3, true)), "three carry 49%");
        assert_eq!(spare(2, 0.52), Some((1, true)), "over by less than one connection: one goes all the same");
        assert_eq!(spare(20, 0.52), Some((19, true)));
        assert_eq!(spare(10, 0.5), None, "at the target: nothing to hand over");
        assert_eq!(spare(10, 0.3), None);
        assert_eq!(spare(1, 0.65), None, "one connection is never split");
        assert_eq!(hand_over(10, 0.65, SHARES, Some(1)).map(|hand| hand.keep), Some(9), "never more than the spare could take");
        assert_eq!(hand_over(10, 0.65, SHARES, Some(0)), None, "a spare with no room takes nothing");
    }

    /// A spare a look reported makes a full window over the target hand
    /// over to it; having handed over, the holder hands over again only
    /// once a later look says a spare is up.
    #[test]
    fn a_spare_is_handed_work_once_per_look_that_reports_it() {
        let mut watch = Watch::new(SHARES);
        let said = fill(&mut watch, 0.6, 6).unwrap();
        assert_eq!(said, load(60), "no spare: only its load");
        watch.said(said);
        assert_eq!(fill(&mut watch, 0.6, 6), None);
        watch.looked(false, Some(60), Some(200));
        let handed = watch.sample(0.6, 6).unwrap();
        assert_eq!(handed, to_spare(50, 5), "the window is full: it hands one of six over at once, and says what it keeps");
        watch.said(handed);
        assert_eq!(fill(&mut watch, 0.6, 5), Some(load(60)), "measured afresh, with no look since: it says its load and hands nothing over");
        watch.said(load(60));
        assert_eq!(fill(&mut watch, 0.6, 5), None, "no look has said a spare is up again");
        watch.looked(false, Some(60), Some(200));
        assert_eq!(watch.sample(0.6, 5), Some(to_spare(48, 4)), "a look says one is: it hands over again");
    }

    #[test]
    fn the_share_is_cpu_time_over_wall_time_and_cpus() {
        assert_eq!(share_of(Duration::from_secs(1), Duration::from_secs(2), 1.0), 0.5);
        assert_eq!(share_of(Duration::from_secs(2), Duration::from_secs(2), 4.0), 0.25);
        assert_eq!(share_of(Duration::from_secs(1), Duration::from_secs(4), 0.5), 0.5, "a quarter of a CPU's time is half of half a CPU");
        assert_eq!(share_of(Duration::from_secs(1), Duration::ZERO, 1.0), 0.0, "no time passed, nothing measured");
    }

    #[test]
    fn a_containers_allowance_is_its_quota_over_its_period() {
        assert_eq!(allowance_of(Some("50000 100000\n")), Ok(Some(0.5)), "half a CPU");
        assert_eq!(allowance_of(Some("200000 100000")), Ok(Some(2.0)));
        assert_eq!(allowance_of(Some("max 100000\n")), Ok(None), "no limit: the CPUs it may run on decide");
        assert_eq!(allowance_of(None), Ok(None), "no file: the same");
        assert!(allowance_of(Some("50000")).unwrap_err().contains("is not '<quota> <period>'"));
        assert!(allowance_of(Some("50000 0")).unwrap_err().contains("'0' in '50000 0' is not a positive number"));
        assert!(allowance_of(Some("lots 100000")).is_err());
    }

    #[test]
    fn this_process_reads_its_own_cpu() {
        let mut cpu = ProcessCpu::new().unwrap();
        let spin = std::time::Instant::now();
        while spin.elapsed() < Duration::from_millis(50) {
            std::hint::black_box(spin.elapsed());
        }
        let share = cpu.busy().unwrap();
        assert!(share > 0.0, "a spin shows: {share}");
    }
}
