//! Waking the roles that scale to zero when work is written for them.
//!
//! A role in a local install's one process hears the database announce
//! every write it waits on (`weft_task_store::pg_signal`) and drains the
//! loops it concerns. A role placed serverless hears nothing while it is
//! at zero: it runs only when its tick is called
//! (`weft_platform_traits::roles::TICK_PATH`). Every write is made by a
//! dispatcher or a broker (every other role writes through the broker),
//! and such a process is up while it writes, listening on the database's
//! announcements for its own waiters anyway. So each one also hears the
//! writes, its own and its siblings', and rings the tick of every role
//! that sleeps, naming the loops a write concerns, by the same rules the
//! role's loops wake by in one process. The announcement comes from a
//! trigger on the table, so no writer has to remember to wake anyone; and
//! no process stays up to listen when nothing is being written, so an
//! idle install and its database sleep.
//!
//! It rings every such role, naming every loop a write wakes, whenever
//! notifications may have been lost (its listening connection dropped
//! while it had work). When its watch only went quiet and listens again
//! ([`Heard::Resumed`]), a process that writes listened while it wrote,
//! so nothing was missed, and ringing every role then would wake the whole
//! install each time the database woke up: it rings only its own roles,
//! whose pass books the looks its loops ask for as they catch up. A ring that
//! fails is rung again, with a growing pause, until the role answers; each
//! failure is kept in the database (`weft_task_store::unanswered`), so
//! `weft status` can say why the work waiting on that role does not move.
//! A process that is stopping waits a few seconds for the rings still out,
//! and logs the ones it gives up on; the next process to start rings those
//! roles again.
//!
//! A ring is coalesced per role: one tick runs at a time, and rings
//! arriving during it are sent as one more once it answers, naming every
//! loop they woke. Two writers that hear the same write each ring once;
//! the tick a write causes twice drains what is there, then nothing.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use weft_broker_client::lifecycle_command::{ISSUED_WAKE, LOOK_WAKE};
use weft_core::net::EmptyBody;
use weft_platform_traits::config::InstallConfig;
use weft_platform_traits::roles::TICK_PATH;
use weft_platform_traits::{CoreRole, IdentityTokens, Placement, RoleAddresses};
use weft_task_store::drain::WakeOn;
use weft_task_store::pg_signal::{Heard, Subscription};
use weft_task_store::unanswered::{self, Callee};

use crate::server::TICK_LOOP_PARAM;

/// The supervisor's one pass, woken by a lifecycle command being issued, or
/// by a machine running a project's infra saying how it stands changed.
const SUPERVISOR_WAKES: &[(&str, &[WakeOn])] = &[("supervisor", &[ISSUED_WAKE, LOOK_WAKE])];

/// What wakes each of `role`'s loops, by loop name. No write wakes the
/// broker's loops (sweeps, run by its own process while it has work, and
/// by its passes); the listener and the holder have no loops at all.
fn loop_wakes(role: CoreRole) -> Vec<(&'static str, &'static [WakeOn])> {
    match role {
        CoreRole::Dispatcher => weft_dispatcher::app::loop_wakes(),
        CoreRole::Supervisor => SUPERVISOR_WAKES.to_vec(),
        CoreRole::Broker | CoreRole::Listener | CoreRole::Holder => Vec::new(),
    }
}

/// The loops of `wakes` a notification on `channel` with `payload` is
/// work for.
fn woken_by(wakes: &[(&'static str, &[WakeOn])], channel: &str, payload: &str) -> BTreeSet<String> {
    wakes
        .iter()
        .filter(|(_, wake_on)| wake_on.iter().any(|w| w.hears(channel, payload)))
        .map(|(name, _)| name.to_string())
        .collect()
}

/// Every loop of `role` a write wakes, by name.
fn every_loop(role: CoreRole) -> BTreeSet<String> {
    loop_wakes(role).iter().map(|(name, _)| name.to_string()).collect()
}

/// The longest pause between two tries of a ring that failed.
const LONGEST_RETRY: Duration = Duration::from_secs(60);

/// How long a stopping process waits for its rings still out. Cloud Run
/// gives a stopping instance 10 seconds after SIGTERM; this leaves room
/// to log what was not delivered.
pub const RING_GRACE: Duration = Duration::from_secs(8);

#[derive(Default)]
struct Ring {
    ringing: bool,
    /// Loops woken while a ring was out, sent as one more once it answers.
    pending: Option<BTreeSet<String>>,
}

struct Bell {
    role: CoreRole,
    url: String,
    audience: String,
    ring: Mutex<Ring>,
}

pub struct RoleWaker {
    bells: BTreeMap<CoreRole, Arc<Bell>>,
    /// The roles this process runs itself.
    own: Vec<CoreRole>,
    tokens: Arc<dyn IdentityTokens>,
    /// Where a ring that keeps failing is written down.
    pool: sqlx::PgPool,
    http: reqwest::Client,
    /// Told whenever a ring is answered and none is left for its role, so
    /// a stopping process can wait for the rings still out ([`Self::settle`]).
    answered: Arc<tokio::sync::Notify>,
}

impl RoleWaker {
    /// A bell for every serverless role with loops to wake, at its
    /// address as this process reaches it. `None` when none sleeps.
    pub fn new(
        config: &InstallConfig,
        own: &[CoreRole],
        addresses: &RoleAddresses,
        tokens: Arc<dyn IdentityTokens>,
        pool: sqlx::PgPool,
    ) -> anyhow::Result<Option<Self>> {
        let mut bells = BTreeMap::new();
        // The listener has no loops to drain, so no tick to ring; the
        // holder is never serverless.
        for role in CoreRole::ALL
            .into_iter()
            .filter(|r| *r != CoreRole::Listener && config.roles.of(*r) == Placement::Serverless)
        {
            let base = addresses.of(role)?.to_string();
            let url = format!("{}{TICK_PATH}", base.trim_end_matches('/'));
            bells.insert(role, Arc::new(Bell { role, url, audience: base, ring: Mutex::new(Ring::default()) }));
        }
        if bells.is_empty() {
            return Ok(None);
        }
        Ok(Some(Self { bells, own: own.to_vec(), tokens, pool, http: reqwest::Client::new(), answered: Arc::default() }))
    }

    /// Every channel a sleeping role's loops wait on: what this process's
    /// watch has to listen on for the waker.
    pub fn channels(&self) -> Vec<&'static str> {
        self.bells.keys().flat_map(|r| loop_wakes(*r).into_iter().flat_map(|(_, w)| w.iter().map(|w| w.channel))).collect()
    }

    /// Ring on what the watch hears, for the process's whole life.
    pub async fn run(self: Arc<Self>, mut heard: Subscription) -> anyhow::Result<()> {
        // A ring a stopped process gave up on is still owed.
        match unanswered::unanswered_roles(&self.pool).await {
            Ok(roles) => {
                for role in roles.iter().filter_map(|r| self.bells.keys().find(|b| b.as_str() == r.as_str()).copied()) {
                    self.ring(role, every_loop(role));
                }
            }
            Err(e) => tracing::warn!(target: "weft_runtime::role_waker", error = %format!("{e:#}"), "could not read the rings left unanswered; their roles run at their own next wake"),
        }
        loop {
            match heard.next().await? {
                Heard::Signal { channel, payload } => {
                    for role in self.bells.keys() {
                        let woken = woken_by(&loop_wakes(*role), channel, &payload);
                        if !woken.is_empty() {
                            self.ring(*role, woken);
                        }
                    }
                }
                // Notifications may have been lost.
                Heard::Recheck => self.ring_every_loop(),
                // Quiet, then listening again: nothing was missed, but its
                // own loops catch up, and their next looks get booked.
                Heard::Resumed => {
                    for role in &self.own {
                        self.ring(*role, every_loop(*role));
                    }
                }
                // The recheck after the reconnect rings every loop.
                Heard::Lost => {}
            }
        }
    }

    /// Ring every sleeping role, naming each of its loops that a write
    /// wakes, so each looks whether or not its next look is due: whenever
    /// notifications may have been lost.
    fn ring_every_loop(&self) {
        for role in self.bells.keys() {
            self.ring(*role, every_loop(*role));
        }
    }

    /// Wait, `grace` at most, for the rings still out to be answered, for
    /// a process that is stopping; log the roles whose ring it gives up on
    /// (each runs at its own next wake instead).
    pub async fn settle(&self, grace: Duration) {
        let deadline = tokio::time::Instant::now() + grace;
        loop {
            // Listening before looking, so an answer between the two is
            // not missed.
            let answered = self.answered.notified();
            tokio::pin!(answered);
            answered.as_mut().enable();
            let out: Vec<CoreRole> = self.bells.values().filter(|b| b.ring.lock().ringing).map(|b| b.role).collect();
            if out.is_empty() {
                return;
            }
            if tokio::time::timeout_at(deadline, answered).await.is_err() {
                tracing::warn!(
                    target: "weft_runtime::role_waker", dropped = out.len(), roles = ?out,
                    "stopping with rings not answered; those roles run at their own next wake"
                );
                return;
            }
        }
    }

    fn ring(&self, role: CoreRole, woken: BTreeSet<String>) {
        let Some(bell) = self.bells.get(&role).cloned() else { return };
        {
            let mut ring = bell.ring.lock();
            if ring.ringing {
                ring.pending.get_or_insert_with(BTreeSet::new).extend(woken);
                return;
            }
            ring.ringing = true;
        }
        let (tokens, http, answered, pool) = (self.tokens.clone(), self.http.clone(), self.answered.clone(), self.pool.clone());
        tokio::spawn(async move {
            let mut woken = woken;
            let mut pause = Duration::from_secs(1);
            let callee = Callee::Role(bell.role.as_str());
            // Whether this ring's last try failed, so its first answer
            // clears what it wrote down.
            let mut failing = false;
            loop {
                let sent = async {
                    let token = tokens.token_for(&bell.audience).await?;
                    let query: Vec<(&str, &str)> = woken.iter().map(|name| (TICK_LOOP_PARAM, name.as_str())).collect();
                    http.post(&bell.url).query(&query).bearer_auth(token).empty_body().send().await?.error_for_status()?;
                    anyhow::Ok(())
                };
                if let Err(e) = sent.await {
                    let error = format!("{e:#}");
                    tracing::warn!(
                        target: "weft_runtime::role_waker", role = %bell.role, %error,
                        retry_in_secs = pause.as_secs(), "could not wake a role; ringing again"
                    );
                    failing = true;
                    if let Err(e) = unanswered::failed(&pool, callee, &error).await {
                        tracing::warn!(target: "weft_runtime::role_waker", role = %bell.role, error = %format!("{e:#}"), "could not write down a ring that failed");
                    }
                    tokio::time::sleep(pause).await;
                    pause = (pause * 2).min(LONGEST_RETRY);
                    if let Some(more) = bell.ring.lock().pending.take() {
                        woken.extend(more);
                    }
                    continue;
                }
                pause = Duration::from_secs(1);
                if std::mem::take(&mut failing) {
                    if let Err(e) = unanswered::answered(&pool, callee).await {
                        tracing::warn!(target: "weft_runtime::role_waker", role = %bell.role, error = %format!("{e:#}"), "could not clear a ring that failed before");
                    }
                }
                let mut ring = bell.ring.lock();
                match ring.pending.take() {
                    Some(more) => woken = more,
                    None => {
                        ring.ringing = false;
                        answered.notify_waiters();
                        break;
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_broker_client::lifecycle_command::INFRA_COMMAND_CHANNEL;

    #[test]
    fn a_write_wakes_only_the_loops_it_concerns() {
        let dispatcher = loop_wakes(CoreRole::Dispatcher);
        assert_eq!(
            woken_by(&dispatcher, weft_task_store::tasks::TASK_READY_CHANNEL, "dispatcher"),
            BTreeSet::from(["dispatcher_picker".to_string()])
        );
        assert_eq!(
            woken_by(&dispatcher, weft_task_store::runs::RUN_QUEUED_CHANNEL, &uuid::Uuid::nil().to_string()),
            BTreeSet::from(["delivery".to_string()])
        );

        let issued = format!("issued:{}", uuid::Uuid::nil());
        assert_eq!(woken_by(&loop_wakes(CoreRole::Supervisor), INFRA_COMMAND_CHANNEL, &issued), BTreeSet::from(["supervisor".to_string()]));
        assert!(woken_by(&loop_wakes(CoreRole::Supervisor), INFRA_COMMAND_CHANNEL, "done:7").is_empty(), "a command's end wakes nobody");
        assert!(woken_by(&loop_wakes(CoreRole::Broker), weft_task_store::tasks::TASK_READY_CHANNEL, "dispatcher").is_empty());
    }
}
