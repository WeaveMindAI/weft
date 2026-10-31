//! Holding the signals that keep a connection to the outside open between
//! fires (`BetweenFires::Holds`: an SSE subscription, a socket, a stream).
//!
//! Such a connection lives in a process that stays up: a local install's
//! one process, or a holder, one of the copies of this code weft runs in
//! a pool as many as the held signals need (none when there are none). A
//! holder claims each signal it holds through the broker, in its own
//! name, and the claim lasts until it lets go. Holders share the signals
//! by taking only the ones no holder claims. A signal whose row is
//! rewritten (a registration again) loses its claim, so its holder stops
//! the old connection and the new one comes up as the row now reads.
//!
//! A holder's look runs on no timer. It looks (one broker call: say what
//! each connection is doing when that changed, learn which are still its
//! own, take what is free) when it starts, when it is told the held
//! signals changed or may have missed it
//! (`weft_platform_traits::HolderNews`), and when one of its connections
//! changes what it says. A holder that dies is noticed by its dead man's
//! switch (`weft_platform_traits::HolderLiveness`): it keeps one check
//! scheduled ahead of now ([`check_ahead`]) and, on the one timer here,
//! pushes it further every [`push_every`] ([`keep_checked`]); a check
//! that goes off frees what the holder claimed. That timer runs only in a
//! holder in a pool, and the pool runs none while no signal is held; a
//! local install's one process keeps no check.
//!
//! A signal that ended (its row is gone, or its activation parked) stops
//! along with what it arranged outside (a provider subscription). One
//! that only changed hands stops here and nothing more: what it arranged
//! outside is keyed by its token, and is the next connection's now.
//!
//! A holder in a pool also watches its own CPU ([`crate::load`]) and says
//! the share it uses whenever it moved a band, so the pool grows and
//! shrinks with the load and not only with the count. One over the high
//! mark hands part of its connections over in the same call, each stopped
//! here first, and takes only as many as it kept from then on, so its
//! next look does not take them back before another holder can; once it
//! says a load under the target, it takes its full room again. While a
//! spare is up (a copy the sizing added that holds nothing yet), a holder
//! over the target hands over to it the same way, and the broker gives
//! what it hands over to that spare alone.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use weft_broker_client::protocol::{
    HeldNow, HeldServing, HeldTransport, SignalHoldRequest, SignalHolderLoadRequest, SignalLetGoRequest, SignalRowWire,
};
use weft_core::signal::listener_protocol::StartMode;
use weft_platform_traits::config::HolderSettings;
use weft_platform_traits::{HeldCheck, HolderHeard, HolderLiveness, HolderNote};

use crate::load::{CpuUse, Say, Shares, Watch};
use crate::registry::{ServingState, Transport};
use crate::ListenerState;

/// How far ahead of now a holder's check is set.
fn check_ahead() -> Duration {
    weft_core::time_scale::scaled(Duration::from_secs(180))
}

/// How often a holder pushes its check further ahead.
fn push_every() -> Duration {
    weft_core::time_scale::scaled(Duration::from_secs(60))
}

/// How long before its check goes off a holder that could not push it
/// further lets go of everything instead.
fn fence_margin() -> Duration {
    weft_core::time_scale::scaled(Duration::from_secs(30))
}

/// The pauses before a look that failed is tried again. After the last, a
/// holder in a pool ends (the platform starts another, which looks as it
/// starts); an install's one process looks at its next news, which its
/// own database sends as it comes back.
const LOOK_RETRIES: [Duration; 3] = [Duration::from_secs(1), Duration::from_secs(5), Duration::from_secs(20)];

/// One process's holding: one look at a time, the most signals it holds,
/// and what it last said about each connection, so a look only carries
/// what changed.
#[derive(Default)]
pub struct Holding {
    said: tokio::sync::Mutex<HashMap<String, HeldServing>>,
    /// The most signals this process holds, set once by [`run`]; unset, for
    /// every one there is. A local install's loop holds every one; a holder
    /// in a pool serves no routes, so nothing takes on its behalf before
    /// its loop has set its room.
    room: std::sync::OnceLock<Room>,
    /// Told when a connection changes what it says
    /// (`crate::kinds::serving_sink`), so the holder says it.
    pub(crate) changed: Arc<tokio::sync::Notify>,
}

/// The most signals one process holds.
enum Room {
    /// Every one there is: a local install's one process.
    Every,
    /// At most `now`: `full` (`signalsPerCopy`) for a holder in a pool, and
    /// what it kept from the moment it hands connections over until it
    /// says a load under the target ([`say_load`], `Say::room_back`): a
    /// holder that was over takes no more until it has room to spare. The
    /// sizing counts the room it gave up, so the pool holds every signal
    /// meanwhile.
    Most { full: u32, now: AtomicU32 },
}

impl Room {
    fn now(&self) -> Option<u32> {
        match self {
            Room::Every => None,
            Room::Most { now, .. } => Some(now.load(Ordering::Acquire)),
        }
    }
}

/// What one process holds of the held signals.
pub enum Share {
    /// Every one there is: a local install's one process, which keeps no
    /// check and says no load.
    Every,
    /// A share, as one copy of a pool the platform keeps running: at most
    /// `settings.signals_per_copy`, with a check kept, since a copy that
    /// dies leaves its claims behind (a copy that ends is replaced by a
    /// new one, which looks as it starts), and its load measured on `cpu`
    /// every `sample_every` (`crate::load::sample_every`) and said against
    /// `settings`' shares ([`crate::load`]).
    Copy { settings: HolderSettings, cpu: Box<dyn CpuUse>, sample_every: Duration },
}

/// A holder's load being watched: what it measures, the detector, and the
/// clock it samples on.
struct Watching {
    cpu: Box<dyn CpuUse>,
    watch: Watch,
    every: tokio::time::Interval,
}

/// Hold `share` until `stop` resolves, then give every claim up
/// ([`let_go`]). A holder in a pool sets its first check before anything
/// is taken, and gives everything up and ends when it cannot keep its
/// check ahead ([`keep_checked`]) or its looks keep failing
/// ([`LOOK_RETRIES`]): the platform starts another. `stop` is heard only
/// between looks, so a look the broker is answering is never cut off with
/// claims it took and nothing here knowing of them.
pub async fn run(state: ListenerState, share: Share, stop: impl std::future::Future<Output = ()>) {
    let (room, mut watching) = match share {
        Share::Every => (Room::Every, None),
        Share::Copy { settings, cpu, sample_every } => {
            let mut every = tokio::time::interval_at(tokio::time::Instant::now() + sample_every, sample_every);
            // A sample owed while a look or a report ran is taken late,
            // never twice in a row: the samples stay apart.
            every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let room = Room::Most { full: settings.signals_per_copy, now: AtomicU32::new(settings.signals_per_copy) };
            (room, Some(Watching { cpu, watch: Watch::new(Shares::of(&settings)), every }))
        }
    };
    if state.holding.room.set(room).is_err() {
        tracing::error!(target: "weft_listener::hold", "a second hold loop started in this process; it ends at once");
        return;
    }
    tokio::pin!(stop);
    let mut heard = state.holders.news.listen();
    let liveness = state.holders.liveness.clone();
    let pooled = watching.is_some();
    let mut check = None;
    if pooled {
        tokio::select! {
            (first, pushed_at) = first_check(liveness.as_ref(), &state.config.replica) => check = Some((parking_lot::Mutex::new(first), pushed_at)),
            () = &mut stop => return,
        }
    }
    let fenced = async {
        match &check {
            Some((check, pushed_at)) => keep_checked(liveness.as_ref(), check, *pushed_at).await,
            None => std::future::pending().await,
        }
    };
    tokio::pin!(fenced);
    let mut owed = Some(0);
    loop {
        if let Some(tries) = owed {
            owed = match look(&state, &[], StartMode::Restore).await {
                Ok(looked) => {
                    if let Some(w) = &mut watching {
                        w.watch.looked(looked.moved, looked.load, looked.spare);
                    }
                    None
                }
                Err(e) if tries < LOOK_RETRIES.len() => {
                    tracing::warn!(target: "weft_listener::hold", error = %format!("{e:#}"), "a holder's look failed; trying again");
                    Some(tries + 1)
                }
                Err(e) if pooled => {
                    tracing::error!(target: "weft_listener::hold", error = %format!("{e:#}"), "a holder's looks keep failing; it lets go of everything and ends, and the copy started in its place looks again");
                    break;
                }
                Err(e) => {
                    tracing::error!(target: "weft_listener::hold", error = %format!("{e:#}"), "a holder's looks keep failing; it looks again at the next news");
                    None
                }
            };
        }
        let retry = async {
            match owed {
                Some(tries) => tokio::time::sleep(LOOK_RETRIES[tries - 1]).await,
                None => std::future::pending().await,
            }
        };
        let sample = async {
            match watching.as_mut() {
                Some(w) => {
                    w.every.tick().await;
                }
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            () = retry => {}
            () = sample => {
                if let Some(w) = &mut watching {
                    owed = owed.or(sample_load(&state, w).await);
                }
            }
            news = heard.next() => match news {
                Ok(HolderHeard::Note(HolderNote::HeldSetChanged)) => owed = owed.or(Some(0)),
                Ok(HolderHeard::Note(HolderNote::AccessChanged)) => state.reach.access_changed(),
                Ok(HolderHeard::MissedSome) => {
                    state.reach.access_changed();
                    owed = owed.or(Some(0));
                }
                Err(e) => {
                    tracing::error!(target: "weft_listener::hold", error = %format!("{e:#}"), "a holder hears no news any more; it lets go of everything");
                    break;
                }
            },
            () = state.holding.changed.notified() => match say(&state).await {
                Ok(Some(looked)) => {
                    if let Some(w) = &mut watching {
                        w.watch.looked(looked.moved, looked.load, looked.spare);
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(target: "weft_listener::hold", error = %format!("{e:#}"), "a holder could not say what its connections are doing; it looks again to say it");
                    owed = owed.or(Some(0));
                }
            },
            () = &mut fenced => {
                tracing::error!(target: "weft_listener::hold", "a holder could not keep its check ahead; it lets go of everything and ends");
                break;
            }
            () = &mut stop => break,
        }
    }
    let_go(&state, check.as_ref().map(|(c, _)| c.lock().clone())).await;
}

/// This holder's first check, pushed until it is set, each try
/// [`check_ahead`] of when it starts, and when the try that set it
/// started.
async fn first_check(liveness: &dyn HolderLiveness, replica: &str) -> (HeldCheck, tokio::time::Instant) {
    loop {
        let pushing_at = tokio::time::Instant::now();
        let check = HeldCheck { replica: replica.to_string(), generation: 1, due_at_ms: in_ms(check_ahead()) };
        match liveness.push(&check).await {
            Ok(()) => return (check, pushing_at),
            Err(e) => {
                tracing::error!(target: "weft_listener::hold", error = %format!("{e:#}"), "a holder could not set its first check; it holds nothing until it can");
                tokio::time::sleep(weft_core::time_scale::scaled(Duration::from_secs(5))).await;
            }
        }
    }
}

/// Keep the holder's check ahead of now: every [`push_every`], set a new
/// one [`check_ahead`] of now, then withdraw the old. Each is tried again
/// until [`fence_margin`] before the old one goes off, counted from when
/// it was pushed (`pushed_at`, the first one's included). Answers only
/// when no new check could be set by then: the old one goes off and frees
/// what the holder claims, so it must let go first. A new check set and
/// an old one left is no harm: it goes off and frees the claims of a
/// holder that is alive, which takes them back on the news that follows.
pub async fn keep_checked(liveness: &dyn HolderLiveness, current: &parking_lot::Mutex<HeldCheck>, mut pushed_at: tokio::time::Instant) {
    loop {
        tokio::time::sleep(push_every()).await;
        let old = current.lock().clone();
        let until = pushed_at + check_ahead().saturating_sub(fence_margin());
        let pushing_at = tokio::time::Instant::now();
        let new = HeldCheck { generation: old.generation + 1, due_at_ms: in_ms(check_ahead()), ..old.clone() };
        if !until_done(until, || liveness.push(&new)).await {
            return;
        }
        *current.lock() = new;
        pushed_at = pushing_at;
        if !until_done(until, || liveness.withdraw(&old)).await {
            tracing::warn!(target: "weft_listener::hold", generation = old.generation, "a holder's old check could not be withdrawn; it goes off, and the holder takes its signals back");
        }
    }
}

/// Try `call` until it works, a few seconds apart, or `until` passes.
async fn until_done<F: std::future::Future<Output = anyhow::Result<()>>>(until: tokio::time::Instant, mut call: impl FnMut() -> F) -> bool {
    loop {
        match call().await {
            Ok(()) => return true,
            Err(e) if tokio::time::Instant::now() < until => {
                tracing::warn!(target: "weft_listener::hold", error = %format!("{e:#}"), "a holder's check could not be moved; trying again");
                tokio::time::sleep_until((tokio::time::Instant::now() + weft_core::time_scale::scaled(Duration::from_secs(5))).min(until)).await;
            }
            Err(e) => {
                tracing::error!(target: "weft_listener::hold", error = %format!("{e:#}"), "a holder's check could not be moved in time");
                return false;
            }
        }
    }
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn in_ms(ahead: Duration) -> i64 {
    now_ms() + i64::try_from(ahead.as_millis()).unwrap_or(i64::MAX / 2)
}

/// Take `tokens` now and bring each up in `mode`, within this process's
/// room: a registration or an activation does not wait for the next
/// look, a fresh one fails with its kind's refusal, and a down row comes
/// back under a claim. A token this process holds already is taken again
/// (its row was rewritten, so it comes up as the row now reads). A token
/// another holder holds is left to it.
pub async fn take_now(state: &ListenerState, tokens: &[String], mode: StartMode) -> anyhow::Result<()> {
    look(state, tokens, mode).await.map(|_| ())
}

/// What the connections held here say now that they did not say last,
/// as a look carries it.
fn holding(state: &ListenerState, want: &[String], said: &mut HashMap<String, HeldServing>) -> Vec<HeldNow> {
    let held = renewed(held_here(state), want);
    said.retain(|token, _| held.contains(token));
    held.into_iter()
        .map(|token| {
            let now = serving_of(state, &token);
            let serving = (said.get(&token) != Some(&now)).then_some(now);
            HeldNow { token, serving }
        })
        .collect()
}

/// Say what changed, when something did: a look that takes nothing.
async fn say(state: &ListenerState) -> anyhow::Result<Option<Looked>> {
    let changed = holding(state, &[], &mut state.holding.said.lock().await.clone());
    if changed.iter().all(|h| h.serving.is_none()) {
        return Ok(None);
    }
    look(state, &[], StartMode::Restore).await.map(Some)
}

/// What a look found, for the load a holder measures ([`Watch::looked`]).
struct Looked {
    /// Whether what this process holds changed: it took something, or
    /// something it held is no longer its own.
    moved: bool,
    /// The load weft keeps for this holder, measured on what it holds.
    load: Option<u16>,
    /// The most a spare up could take (`SignalHoldResponse::spare`).
    spare: Option<u32>,
}

/// Take one sample of a holder's load and say it, when it is to be said
/// ([`Watch::sample`], [`say_load`]). `Some` when a look is owed: the report failed,
/// so this holder does not know whether it was written (the broker
/// refused, or its answer was lost after it committed). The look takes
/// back whatever it stopped to hand over and is still free, and writes
/// the room this holder runs with onto its load row, so the room the
/// sizing counts is right whichever way the report went. A report that
/// fails starts the samples over, so the next try comes a full window
/// later.
async fn sample_load(state: &ListenerState, w: &mut Watching) -> Option<usize> {
    let share = match w.cpu.busy() {
        Ok(share) => share,
        Err(e) => {
            tracing::warn!(target: "weft_listener::hold", error = %format!("{e:#}"), "a holder could not read its CPU; its load is not measured this time");
            return None;
        }
    };
    let say = w.watch.sample(share, up_here(state).len())?;
    match say_load(state, say).await {
        Ok(Some(said)) => {
            w.watch.said(said);
            None
        }
        Ok(None) => None,
        Err(e) => {
            tracing::warn!(target: "weft_listener::hold", error = %format!("{e:#}"), cpu_percent = say.cpu_percent, "a holder could not say its load; it measures again first");
            w.watch.restart();
            Some(0)
        }
    }
}

/// Say `say` to weft, and take the room it sets. A holder handing
/// connections over (over the high mark, or over the target with a spare
/// up) stops the ones it hands over here first (the last ones up, by
/// token: each costs one reconnect to move, so none is cheaper), so no
/// signal is ever connected from two holders at once, then hands them
/// over in the same call that says its load, and takes only what it kept
/// until it says a load under the target, which gives it its full room
/// again. One the broker refuses (no spare free, or nothing it hands over
/// is its own any more) is an error: its next look takes back what it
/// stopped and still claims. Answers what was said, `None` when there turned out to be
/// nothing to hand over (a connection went since the sample).
async fn say_load(state: &ListenerState, say: Say) -> anyhow::Result<Option<Say>> {
    let Some(Room::Most { full, now }) = state.holding.room.get() else {
        anyhow::bail!("only a holder in a pool says its load");
    };
    // No look runs while this does, so what it keeps is what the next one
    // says it holds.
    let mut said = state.holding.said.lock().await;
    let (room, let_go) = match say.hand {
        Some(hand) => {
            let mut up = up_here(state);
            let let_go = up.split_off(hand.keep.min(up.len()));
            if let_go.is_empty() {
                return Ok(None);
            }
            for token in &let_go {
                crate::kinds::stop_here(state, token);
                said.remove(token);
            }
            (u32::try_from(renewed(held_here(state), &[]).len()).unwrap_or(u32::MAX), let_go)
        }
        None if say.room_back => (*full, Vec::new()),
        None => (now.load(Ordering::Acquire), Vec::new()),
    };
    let request = SignalHolderLoadRequest {
        replica: state.config.replica.clone(),
        cpu_percent: say.cpu_percent,
        room,
        let_go: let_go.clone(),
        spare_only: say.hand.is_some_and(|hand| hand.spare_only),
    };
    state.signals.holder_load(&request).await?;
    now.store(room, Ordering::Release);
    tracing::info!(target: "weft_listener::hold", cpu_percent = say.cpu_percent, room, handed_over = let_go.len(), "a holder said its load");
    Ok(Some(say))
}

/// One look: say what changed, stop what this process no longer holds,
/// bring up what it took (the `want` ones in `mode`, the others as a
/// restore).
async fn look(state: &ListenerState, want: &[String], mode: StartMode) -> anyhow::Result<Looked> {
    let mut said = state.holding.said.lock().await;
    // A give-up written now lets go of its claim, so the look below no
    // longer holds it; one that still fails stays held, and says so.
    for (token, gave_up) in state.registry.unrecorded() {
        if let Err(e) = state.fire_sink.give_up(&token, gave_up.generation, &gave_up.reason).await {
            tracing::warn!(target: "weft_listener::hold", %token, error = %format!("{e:#}"), "a give-up still could not be recorded; the next look tries again");
        }
    }
    let room = state.holding.room.get().and_then(Room::now);
    let holding = holding(state, want, &mut said);
    let answer = state
        .signals
        .hold(&SignalHoldRequest { replica: state.config.replica.clone(), holding: holding.clone(), room, want: want.to_vec() })
        .await?;
    let kept: HashSet<&String> = answer.kept.iter().collect();
    let ended: HashSet<&String> = answer.ended.iter().collect();
    let moved = !answer.taken.is_empty() || holding.iter().any(|now| !kept.contains(&now.token));
    for now in &holding {
        if kept.contains(&now.token) {
            if let Some(serving) = &now.serving {
                said.insert(now.token.clone(), serving.clone());
            }
            continue;
        }
        said.remove(&now.token);
        if ended.contains(&now.token) {
            tracing::info!(target: "weft_listener::hold", token = %now.token, "a held signal ended; stopping it and what it arranged outside");
            // The claim that ended is whatever this holder held under the
            // token: looks run one at a time, and only a look brings a
            // held signal up here, so nothing newer came up since.
            crate::kinds::forget(state, &now.token, crate::registry::Registrations::All);
        } else {
            tracing::info!(target: "weft_listener::hold", token = %now.token, "a held signal is no longer this holder's; stopping it here");
            crate::kinds::stop_here(state, &now.token);
        }
    }
    let mut refused = Vec::new();
    for row in answer.taken {
        let token = row.token.clone();
        // A take clears what the row says it is doing, so the new
        // connection's first word is carried even when it repeats the old.
        said.remove(&token);
        let wanted = want.contains(&token);
        if let Err(e) = bring_up(state, row, if wanted { mode } else { StartMode::Restore }).await {
            if wanted {
                refused.push(format!("signal {token}: {e:#}"));
            }
        }
    }
    anyhow::ensure!(refused.is_empty(), "{}", refused.join("; "));
    Ok(Looked { moved, load: answer.load, spare: answer.spare })
}

/// Bring a taken row up through the one path every held bring-up takes
/// (`crate::registry::hold`): a restore that fails is marked down, shown
/// and retried there.
async fn bring_up(state: &ListenerState, row: SignalRowWire, mode: StartMode) -> anyhow::Result<()> {
    crate::registry::hold(state, row, mode).await
}

/// Give up every claim this process holds (it is stopping), so another
/// holder takes them at once. Each connection stops here first, so no
/// signal is ever connected from two holders at once; what it arranged
/// outside stays, for the next holder. The broker lets go of every claim
/// under this process's name, including any a look took that never came
/// up here. Once it has, the holder's `check` is withdrawn; a claim the
/// broker could not let go of is left to the check.
async fn let_go(state: &ListenerState, check: Option<HeldCheck>) {
    for token in held_here(state) {
        crate::kinds::stop_here(state, &token);
    }
    if let Err(e) = state.signals.let_go(&SignalLetGoRequest { replica: state.config.replica.clone() }).await {
        tracing::warn!(target: "weft_listener::hold", error = %format!("{e:#}"), "could not give up this holder's claims; its check frees them when it goes off");
        return;
    }
    if let Some(check) = check {
        if let Err(e) = state.holders.liveness.withdraw(&check).await {
            tracing::warn!(target: "weft_listener::hold", error = %format!("{e:#}"), "could not withdraw this holder's check; it goes off and finds nothing to free");
        }
    }
}

/// The connections up here, in token order: what carries a holder's load,
/// and what one over the high mark hands over.
fn up_here(state: &ListenerState) -> Vec<String> {
    // A token can be counted down (or given up on) while an entry for it
    // is still registered: that connection carries no load.
    let resting: HashSet<String> =
        state.registry.down_connections().into_iter().chain(state.registry.unrecorded().into_iter().map(|(token, _)| token)).collect();
    let mut tokens: Vec<String> =
        state.registry.list().into_iter().map(|(token, _)| token).filter(|token| !resting.contains(token)).collect();
    tokens.sort_unstable();
    tokens
}

/// Every connection this process holds now: the ones up, the ones down
/// and retrying, and the ones given up on whose give-up is still to be
/// written. A signal down here that holds no connection is no holder's
/// claim: its own retry brings it back.
fn held_here(state: &ListenerState) -> Vec<String> {
    let mut tokens: Vec<String> = state.registry.list().into_iter().map(|(token, _)| token).collect();
    tokens.extend(state.registry.down_connections());
    tokens.extend(state.registry.unrecorded().into_iter().map(|(token, _)| token));
    tokens
}

/// Of the signals `held` here, the ones a look renews, once each and in
/// order: every one but those in `want`, which the look takes again so
/// each comes up as its row now reads (a take skips what a look renews).
fn renewed(mut held: Vec<String>, want: &[String]) -> Vec<String> {
    held.retain(|token| !want.contains(token));
    held.sort_unstable();
    held.dedup();
    held
}

/// What one held signal says it is doing now.
fn serving_of(state: &ListenerState, token: &str) -> HeldServing {
    if let Some(gave_up) = state.registry.unrecorded_of(token) {
        return HeldServing { status: format!("given up on: {}", gave_up.reason), transport: None };
    }
    if let Some(reason) = state.registry.down_reason(token) {
        return HeldServing { status: format!("down, retrying: {reason}"), transport: None };
    }
    match state.registry.get(token) {
        Some(sig) => wire(&sig.serving.lock()),
        None => HeldServing { status: String::new(), transport: None },
    }
}

/// A serving state as a holder says it.
pub fn wire(serving: &ServingState) -> HeldServing {
    HeldServing {
        status: serving.status.clone(),
        transport: serving.transport.as_ref().map(|t| match t {
            Transport::Socket => HeldTransport::Socket,
            Transport::Webhook => HeldTransport::Webhook,
            Transport::Unservable(reason) => HeldTransport::Unservable { reason: reason.clone() },
        }),
    }
}

/// A serving state as the holder said it, for the process rendering the
/// display. A signal no holder holds right now says so.
pub fn from_wire(said: Option<&HeldServing>) -> ServingState {
    match said {
        None => ServingState { status: "waiting for a holder to take it".into(), transport: None },
        Some(said) => ServingState {
            status: said.status.clone(),
            transport: said.transport.as_ref().map(|t| match t {
                HeldTransport::Socket => Transport::Socket,
                HeldTransport::Webhook => Transport::Webhook,
                HeldTransport::Unservable { reason } => Transport::Unservable(reason.clone()),
            }),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_platform_traits::{AlarmLiveness, FakeAlarm};

    /// A holder whose pushes work keeps one check ahead and withdraws
    /// each old one, and never lets go. Once its alarm refuses, it lets go
    /// within the last few seconds before the fence margin of the check
    /// last set, counted on the one clock the holder keeps.
    #[tokio::test(start_paused = true)]
    async fn a_holder_that_cannot_push_its_check_lets_go_before_the_old_one_goes_off() {
        let alarm = Arc::new(FakeAlarm::new());
        let liveness = AlarmLiveness::new(alarm.clone());
        let started = tokio::time::Instant::now();
        let current = parking_lot::Mutex::new(HeldCheck { replica: "h".into(), generation: 1, due_at_ms: in_ms(check_ahead()) });
        let pushing = tokio::time::timeout(push_every() * 3 + Duration::from_secs(1), keep_checked(&liveness, &current, started)).await;
        assert!(pushing.is_err(), "a holder whose pushes work never lets go");
        assert_eq!(current.lock().generation, 4);
        assert_eq!(alarm.wakes().len(), 3, "each new check set");
        assert_eq!(alarm.cancelled().len(), 3, "each old check withdrawn");
        assert_eq!(alarm.pending().len(), 1, "one check ahead at a time");

        // Pick up from the third push, as the loop left it.
        let last_pushed_at = started + push_every() * 3;
        alarm.fail_sets(true);
        let set = alarm.wakes().len();
        keep_checked(&liveness, &current, last_pushed_at).await;
        let fence = last_pushed_at + check_ahead() - fence_margin();
        let let_go_at = tokio::time::Instant::now();
        assert!(let_go_at <= fence, "let go {:?} past the fence", let_go_at - fence);
        assert!(fence - let_go_at <= Duration::from_secs(5), "let go {:?} before the fence, while pushing still had time", fence - let_go_at);
        assert_eq!(alarm.wakes().len(), set, "nothing more was set");
        assert_eq!(current.lock().generation, 4, "the check that goes off is the last one set");
    }

    #[test]
    fn a_serving_state_says_the_same_on_both_sides() {
        for transport in [None, Some(Transport::Socket), Some(Transport::Webhook), Some(Transport::Unservable("no token".into()))] {
            let state = ServingState { status: "listening".into(), transport };
            let back = from_wire(Some(&wire(&state)));
            assert_eq!((back.status, back.transport), (state.status, state.transport));
        }
        assert_eq!(from_wire(None).status, "waiting for a holder to take it");
    }

    fn strings(tokens: &[&str]) -> Vec<String> {
        tokens.iter().map(|t| t.to_string()).collect()
    }

    /// A look renews each signal held here once, in order, and leaves the
    /// wanted ones out so the broker takes them again.
    #[test]
    fn a_look_renews_what_it_holds_but_what_it_wants_taken_again() {
        assert_eq!(renewed(strings(&["b", "a", "b"]), &[]), strings(&["a", "b"]), "up and down at once counts once");
        assert_eq!(renewed(strings(&["a", "b", "c"]), &strings(&["b"])), strings(&["a", "c"]));
        assert_eq!(renewed(strings(&["a"]), &strings(&["a", "z"])), Vec::<String>::new(), "a wanted token not held here adds nothing");
    }
}
