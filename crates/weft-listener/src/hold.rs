//! Holding the signals that keep a connection to the outside open between
//! fires (`BetweenFires::Holds`: an SSE subscription, a socket, a stream).
//!
//! Such a connection lives in a process that stays up: a local install's
//! one process, or a holder, one of the copies of this code weft runs in
//! a pool as many as the held signals need (none when there are none). A
//! holder claims each signal it holds through the broker, in its own
//! name, and renews the claim at every look. Holders share the signals by
//! taking only the ones no live holder claims, and when one dies its
//! claims lapse and another takes them over. A signal whose row is
//! rewritten (a registration again) loses its claim, so its holder stops
//! the old connection and the new one comes up as the row now reads.
//!
//! A signal that ended (its row is gone, or its activation parked) stops
//! along with what it arranged outside (a provider subscription). One
//! that only changed hands stops here and nothing more: what it arranged
//! outside is keyed by its token, and is the next connection's now.
//!
//! Each look also carries what every connection says it is doing, so the
//! node's display, which the listener renders in another process, shows
//! it.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use weft_broker_client::protocol::{HeldNow, HeldServing, HeldTransport, SignalHoldRequest, SignalLetGoRequest, SignalRowWire};
use weft_core::signal::listener_protocol::StartMode;

use crate::registry::{ServingState, Transport};
use crate::ListenerState;

/// How often a holder looks: renews its claims, says what its connections
/// are doing, and takes the signals waiting for a holder. A third of a
/// claim's life (`weft_broker_client::protocol::hold_lease_secs`), at this
/// install's pace.
fn look_every() -> Duration {
    weft_core::time_scale::scaled(Duration::from_secs(10))
}

/// One process's holding: one look at a time, the most signals it holds,
/// and what it last said about each connection, so a look only carries
/// what changed.
#[derive(Default)]
pub struct Holding {
    said: tokio::sync::Mutex<HashMap<String, HeldServing>>,
    /// The most signals this process holds, set once by [`run`]; `None`
    /// (and unset) for every one there is. A local install's loop runs with
    /// `None`; a holder in a pool serves no routes, so nothing takes on its
    /// behalf before its loop has set its room.
    room: std::sync::OnceLock<Option<u32>>,
}

/// Hold until `stop` resolves: a look now, then one every
/// [`look_every`], then every claim given up ([`let_go`]). `room` is the
/// most signals this process holds (`None` for every one there is). A
/// look that fails is logged; the next one tries again, and the claims it
/// could not renew lapse to another holder meanwhile. `stop` is heard
/// only between looks, so a look the broker is answering is never cut
/// off with claims it took and nothing here knowing of them.
pub async fn run(state: ListenerState, room: Option<u32>, stop: impl std::future::Future<Output = ()>) {
    if state.holding.room.set(room).is_err() {
        tracing::error!(target: "weft_listener::hold", "a second hold loop started in this process; it ends at once");
        return;
    }
    tokio::pin!(stop);
    loop {
        if let Err(e) = look(&state, &[], StartMode::Restore).await {
            tracing::warn!(target: "weft_listener::hold", error = %format!("{e:#}"), "a holder's look failed; looking again shortly");
        }
        tokio::select! {
            () = tokio::time::sleep(look_every()) => {}
            () = &mut stop => break,
        }
    }
    let_go(&state).await;
}

/// Take `tokens` now and bring each up in `mode`, within this process's
/// room: a registration or an activation does not wait for the next
/// look, a fresh one fails with its kind's refusal, and a down row comes
/// back under a claim. A token this process holds already is taken again
/// (its row was rewritten, so it comes up as the row now reads). A token
/// another holder holds is left to it.
pub async fn take_now(state: &ListenerState, tokens: &[String], mode: StartMode) -> anyhow::Result<()> {
    look(state, tokens, mode).await
}

/// One look: renew what this process holds, stop what it no longer holds,
/// bring up what it took (the `want` ones in `mode`, the others as a
/// restore).
async fn look(state: &ListenerState, want: &[String], mode: StartMode) -> anyhow::Result<()> {
    let mut said = state.holding.said.lock().await;
    let room = state.holding.room.get().copied().flatten();
    let holding: Vec<HeldNow> = renewed(held_here(state), want)
        .into_iter()
        .map(|token| {
            let now = serving_of(state, &token);
            let serving = (said.get(&token) != Some(&now)).then_some(now);
            HeldNow { token, serving }
        })
        .collect();
    let answer = state
        .signals
        .hold(&SignalHoldRequest { replica: state.config.replica.clone(), holding: holding.clone(), room, want: want.to_vec() })
        .await?;
    let kept: HashSet<&String> = answer.kept.iter().collect();
    let ended: HashSet<&String> = answer.ended.iter().collect();
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
            crate::kinds::forget(state, &now.token);
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
    Ok(())
}

/// Bring a taken row up through the one path every held bring-up takes
/// (`crate::registry::hold`): a restore that fails is marked down, shown
/// and retried there.
async fn bring_up(state: &ListenerState, row: SignalRowWire, mode: StartMode) -> anyhow::Result<()> {
    crate::registry::hold(state, row, mode).await
}

/// Give up every claim this process holds (it is stopping), so another
/// holder takes them at its next look rather than after they lapse. Each
/// connection stops here first, so no signal is ever connected from two
/// holders at once; what it arranged outside stays, for the next holder.
/// The broker lets go of every claim under this process's name, including
/// any a look took that never came up here.
async fn let_go(state: &ListenerState) {
    for token in held_here(state) {
        crate::kinds::stop_here(state, &token);
    }
    if let Err(e) = state.signals.let_go(&SignalLetGoRequest { replica: state.config.replica.clone() }).await {
        tracing::warn!(target: "weft_listener::hold", error = %format!("{e:#}"), "could not give up this holder's claims; they lapse to another holder instead");
    }
}

/// Every signal this process holds now: the connections up, and the ones
/// down and retrying.
fn held_here(state: &ListenerState) -> Vec<String> {
    let mut tokens: Vec<String> = state.registry.list().into_iter().map(|(token, _)| token).collect();
    tokens.extend(state.registry.down_tokens());
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
