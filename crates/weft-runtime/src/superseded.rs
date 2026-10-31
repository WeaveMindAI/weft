//! Telling a copy of a role that a newer copy replaced it.
//!
//! On a cloud each role runs as copies of one revision of its service, and
//! an update (or a rollback) starts copies of another revision, which the
//! platform then routes every new request to. Nothing tells the old copies
//! they were replaced: one holding something open (a follower's event
//! stream, a worker's line) lives on for as long as that stays open, which
//! a request may do for an hour, claiming work from the shared queue and
//! holding its share of the database's connections beside the new copies'
//! own.
//!
//! Which revision is current is the platform's answer, never weft's: the
//! one it routes the requests to (`weft_platform_traits::ServiceRevisions`,
//! on Cloud Run the service's traffic status). A copy asks once as it
//! starts serving, and steps aside (`weft_task_store::aside`) when the
//! platform routes to another revision; while its own revision is the one
//! the platform is moving the requests to (a rollout under way), it asks
//! again every [`INCOMING_LOOK`] until it is routed to or not. Once it
//! knows it serves, it says so on its role's row of [`GROUP`] and
//! announces it, and every copy of the role hears that ([`run`]): a copy
//! of another revision asks the platform again, once per announcement.
//! The announcement only says when to look.
//!
//! Copies of the same revision never step aside for each other: N copies
//! is the normal case. A rollback is the platform routing back to an older
//! revision, so its copies stay and the newer ones step aside. A process
//! with no revision (a local install, one process) asks and announces
//! nothing. A copy that cannot ask the platform, or read or write the row,
//! keeps serving, says why, and tries again ([`RETRY_FIRST`], doubling up
//! to [`RETRY_LONGEST`]) until it can: it announces only what the platform
//! confirmed.
//!
//! It costs one call to the platform and one row written per copy that
//! starts, and one call per announcement to every copy of another
//! revision: nothing on any request, and nothing on a timer outside a
//! rollout of the copy's own revision or a spell the platform or the
//! database does not answer.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use weft_platform_traits::{CoreRole, Routing, RoutingRefused, ServiceRevisions};
use weft_task_store::aside::StepAside;
use weft_task_store::db::{Client as _, Pool};
use weft_task_store::pg_signal::{Heard, Subscription};

/// The channel a copy announces its revision on as it starts serving, its
/// role as payload.
// SYNC: ROLE_REVISION_CHANNEL <-> 'weft_role_revision' in `boot_as`
pub const ROLE_REVISION_CHANNEL: &str = "weft_role_revision";

/// The `role_revision` table: the revision of each role announced last,
/// by a copy the platform routes requests to as it started serving.
pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "role_revision",
    tables: &["role_revision"],
    ddl: &[r#"CREATE TABLE IF NOT EXISTS role_revision (
            -- The role (`CoreRole::as_str`): one row each.
            role TEXT PRIMARY KEY,
            -- The platform's name for the revision of the copy that
            -- started serving last (Cloud Run's `K_REVISION`).
            revision TEXT NOT NULL,
            -- When it did, in unix milliseconds on the database's clock.
            started_ms BIGINT NOT NULL
        )"#],
    seed: &[],
    data_steps: &[],
};

/// This process's revision, as the platform names it (`K_REVISION`, read
/// once at boot); `None` where the platform says nothing of revisions.
pub fn revision() -> Option<String> {
    revision_of(std::env::var("K_REVISION").ok())
}

/// [`revision`] from what the environment holds: an empty name is none.
/// Pure.
fn revision_of(found: Option<String>) -> Option<String> {
    found.map(|name| name.trim().to_string()).filter(|name| !name.is_empty())
}

/// How often a copy whose revision the platform is moving the requests
/// to asks again whether it was routed to yet.
pub const INCOMING_LOOK: Duration = Duration::from_secs(2);

/// How often a copy still waiting to be routed to says so: a rollout that
/// does not end is seen in its log.
const INCOMING_SAID_EVERY: u32 = 30;

/// The first wait before trying again what the platform or the database
/// did not answer; each further failure doubles it, up to
/// [`RETRY_LONGEST`].
pub const RETRY_FIRST: Duration = Duration::from_secs(2);
pub const RETRY_LONGEST: Duration = Duration::from_secs(60);

/// Where a copy of a revision stands, by the platform's answer.
#[derive(Debug, PartialEq, Eq)]
pub enum Standing {
    /// Its revision receives requests.
    Current,
    /// Its revision is the one the platform is moving the requests to.
    Incoming,
    /// The platform routes every request to other revisions.
    Replaced { serving: Vec<String> },
    /// The platform could not be asked: the copy keeps serving, for why.
    /// `refused` when the platform refused, which waiting does not fix
    /// (`weft_platform_traits::RoutingRefused`).
    Unknown { why: String, refused: bool },
}

/// Where a copy of revision `mine` stands, by what the platform answered
/// when asked where its service sends its requests. Pure.
pub fn standing(mine: &str, routing: &anyhow::Result<Routing>) -> Standing {
    match routing {
        Err(e) => Standing::Unknown { why: format!("{e:#}"), refused: e.chain().any(|c| c.is::<RoutingRefused>()) },
        Ok(routing) if routing.serving.iter().any(|r| r == mine) => Standing::Current,
        Ok(routing) if routing.incoming.as_deref() == Some(mine) => Standing::Incoming,
        Ok(routing) => Standing::Replaced { serving: routing.serving.clone() },
    }
}

/// One announcement read off a role's row: the revision, and when its
/// copy wrote it, which tells one announcement from the next of the same
/// revision (the traffic may have moved between them).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Announced {
    pub revision: String,
    pub started_ms: i64,
}

/// How a copy ended its start ([`Copy::settle`]).
#[derive(Debug, PartialEq, Eq)]
pub enum Settled {
    /// The platform routes requests to this copy's revision.
    Serving,
    SteppedAside,
}

/// One copy of a role with a revision, and the platform it asks.
pub struct Copy {
    pub role: CoreRole,
    pub mine: Arc<str>,
    pub revisions: Arc<dyn ServiceRevisions>,
    pub aside: StepAside,
    /// The announcement this copy last asked the platform about, so it
    /// asks once per announcement.
    looked_at: Option<Announced>,
    /// Whether a refusal of the platform's (`Standing::Unknown`'s
    /// `refused`) went out as an alert already: the first one does.
    refusal_alerted: std::sync::atomic::AtomicBool,
}

impl Copy {
    pub fn new(role: CoreRole, mine: Arc<str>, revisions: Arc<dyn ServiceRevisions>, aside: StepAside) -> Self {
        Self { role, mine, revisions, aside, looked_at: None, refusal_alerted: Default::default() }
    }

    /// Ask the platform where this copy stands, again after each answer it
    /// could not give, and step aside if it was replaced. Never
    /// [`Standing::Unknown`].
    async fn ask(&self) -> Standing {
        let mut wait = RETRY_FIRST;
        loop {
            match standing(&self.mine, &self.revisions.routing().await) {
                Standing::Replaced { serving } => {
                    tracing::warn!(
                        target: "weft_runtime::superseded",
                        role = %self.role, revision = %self.mine, ?serving,
                        "the platform routes this role's requests to another revision; stepping aside"
                    );
                    self.aside.step_aside();
                    return Standing::Replaced { serving };
                }
                Standing::Unknown { why, refused } => {
                    // A refusal that waiting does not lift (a permission
                    // the copy lacks) keeps every older copy serving beside
                    // this one: the first reaches the install's alerts.
                    if refused && !self.refusal_alerted.swap(true, std::sync::atomic::Ordering::Relaxed) {
                        tracing::error!(
                            target: weft_platform_traits::ALERT_TARGET,
                            role = %self.role, revision = %self.mine, %why,
                            "the platform refuses to say which revision of this role it routes requests to, so copies of older revisions are not stepped aside; give the role's service account read access to its own Cloud Run service"
                        );
                    }
                    tracing::warn!(
                        target: "weft_runtime::superseded",
                        role = %self.role, revision = %self.mine, %why, retry_in = ?wait,
                        "could not ask the platform which revision of this role it routes requests to; this copy keeps serving and asks again"
                    );
                    tokio::time::sleep(wait).await;
                    wait = (wait * 2).min(RETRY_LONGEST);
                }
                known => return known,
            }
        }
    }

    /// As this copy starts serving: ask the platform where it stands, and
    /// again every [`INCOMING_LOOK`] while its revision is the one being
    /// rolled out, until it is routed to or replaced.
    pub async fn settle(&self) -> Settled {
        let mut looks = 0u32;
        loop {
            match self.ask().await {
                Standing::Current => return Settled::Serving,
                Standing::Replaced { .. } => return Settled::SteppedAside,
                Standing::Unknown { .. } => unreachable!("`ask` answers only what the platform said"),
                Standing::Incoming => {
                    if looks.is_multiple_of(INCOMING_SAID_EVERY) {
                        tracing::info!(
                            target: "weft_runtime::superseded",
                            role = %self.role, revision = %self.mine, waited = ?INCOMING_LOOK * looks,
                            "the platform is moving this role's requests to this revision; waiting for it to before announcing it"
                        );
                    }
                    looks += 1;
                    tokio::time::sleep(INCOMING_LOOK).await;
                }
            }
        }
    }

    /// Hearing that `announced` is the announcement made last: a copy of
    /// another revision asks the platform, once per announcement. Whether
    /// this copy stepped aside.
    pub async fn heard(&mut self, announced: Option<Announced>) -> bool {
        let Some(announced) = announced.filter(|a| *a.revision != *self.mine && Some(a) != self.looked_at.as_ref()) else {
            return false;
        };
        let stepped = matches!(self.ask().await, Standing::Replaced { .. });
        self.looked_at = Some(announced);
        stepped
    }
}

/// Say that a copy of `role` of `revision` serves: the role's row names
/// it, and every copy of the role is told, in one statement.
pub async fn boot_as(pool: &Pool, role: CoreRole, revision: &str) -> anyhow::Result<()> {
    // SYNC: 'weft_role_revision' <-> ROLE_REVISION_CHANNEL
    pool.execute(
        &format!(
            "WITH written AS ( \
                 INSERT INTO role_revision (role, revision, started_ms) VALUES ($1, $2, {now}) \
                 ON CONFLICT (role) DO UPDATE SET revision = EXCLUDED.revision, started_ms = EXCLUDED.started_ms \
                 RETURNING role) \
             SELECT pg_notify('weft_role_revision', role) FROM written",
            now = weft_task_store::drain::DB_NOW_MS
        ),
        &[&role.as_str(), &revision],
    )
    .await
    .with_context(|| format!("say that a copy of the {role} of revision {revision} serves"))?;
    Ok(())
}

/// The announcement of `role` made last.
pub async fn announced(pool: &Pool, role: CoreRole) -> anyhow::Result<Option<Announced>> {
    let row: Option<(String, i64)> = pool
        .opt("SELECT revision, started_ms FROM role_revision WHERE role = $1", &[&role.as_str()])
        .await
        .with_context(|| format!("read the {role}'s revision announced last"))?;
    Ok(row.map(|(revision, started_ms)| Announced { revision, started_ms }))
}

/// `attempt`'s answer, trying again after each failure (said, with what
/// `doing` names) on the waits [`RETRY_FIRST`] to [`RETRY_LONGEST`].
async fn until_done<T, Fut>(role: CoreRole, doing: &str, mut attempt: impl FnMut() -> Fut) -> T
where
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    let mut wait = RETRY_FIRST;
    loop {
        match attempt().await {
            Ok(done) => return done,
            Err(e) => {
                tracing::warn!(target: "weft_runtime::superseded", %role, error = %format!("{e:#}"), retry_in = ?wait, "could not {doing}; trying again");
                tokio::time::sleep(wait).await;
                wait = (wait * 2).min(RETRY_LONGEST);
            }
        }
    }
}

/// Settle `copy` as it starts serving, announce it unless it stepped
/// aside, then follow what is announced on `heard` (subscribed on
/// [`ROLE_REVISION_CHANNEL`] before this copy announced its own): read
/// when a copy announces, and whenever an announcement may have been
/// missed (the listening resumed, or rechecks). Never returns once this
/// copy stepped aside, which ends the process its own way; returns only
/// when its listening is gone for good, and the process's supervisor
/// ends the process then.
pub async fn run(pool: Pool, mut heard: Subscription, mut copy: Copy) {
    if copy.settle().await == Settled::SteppedAside {
        return std::future::pending().await;
    }
    let (role, mine) = (copy.role, copy.mine.clone());
    until_done(role, "announce this copy", || boot_as(&pool, role, &mine)).await;
    tracing::info!(target: "weft_runtime::superseded", %role, revision = %mine, "serving as the current revision of its role");
    loop {
        match heard.next().await {
            Ok(Heard::Signal { channel, payload }) if channel == ROLE_REVISION_CHANNEL && &*payload == role.as_str() => {}
            Ok(Heard::Recheck | Heard::Resumed) => {}
            Ok(_) => continue,
            Err(e) => {
                tracing::error!(target: "weft_runtime::superseded", %role, error = %format!("{e:#}"), "stopped hearing which revision of this role is announced");
                return;
            }
        }
        let last = until_done(role, "read which revision was announced", || announced(&pool, role)).await;
        if copy.heard(last).await {
            return std::future::pending().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use weft_platform_traits::FakeServiceRevisions;

    use super::*;

    fn routing(serving: &[&str], incoming: Option<&str>) -> Routing {
        Routing { serving: serving.iter().map(|s| s.to_string()).collect(), incoming: incoming.map(str::to_string) }
    }

    /// A copy is current while the platform routes to its revision (its
    /// siblings' too), replaced once it routes only elsewhere, including
    /// back to an older revision (a rollback), and never replaced by a
    /// platform it could not ask.
    #[test]
    fn the_platform_routing_decides_where_a_copy_stands() {
        let mine = "dispatcher-00005";
        assert_eq!(standing(mine, &Ok(routing(&["dispatcher-00005"], None))), Standing::Current, "its own revision serving");
        assert_eq!(standing(mine, &Ok(routing(&["dispatcher-00004", "dispatcher-00005"], None))), Standing::Current, "a share of a split");
        assert_eq!(
            standing("dispatcher-00004", &Ok(routing(&["dispatcher-00005"], None))),
            Standing::Replaced { serving: vec!["dispatcher-00005".into()] },
            "another revision serving"
        );
        assert_eq!(
            standing(mine, &Ok(routing(&["dispatcher-00004"], None))),
            Standing::Replaced { serving: vec!["dispatcher-00004".into()] },
            "a rollback routes to the older revision"
        );
        assert_eq!(standing(mine, &Ok(routing(&["dispatcher-00004"], Some(mine)))), Standing::Incoming, "its own rollout under way");
        assert_eq!(
            standing(mine, &Err(anyhow::anyhow!("403 Forbidden"))),
            Standing::Unknown { why: "403 Forbidden".into(), refused: false },
            "the platform unreachable: it keeps serving, and says why"
        );
        assert_eq!(
            standing(mine, &Err(anyhow::Error::new(RoutingRefused("403 Forbidden".into())).context("read the service"))),
            Standing::Unknown { why: "read the service: 403 Forbidden".into(), refused: true },
            "the platform refusing for good: said as such, for the alert"
        );
    }

    /// A process whose platform names no revision has none.
    #[test]
    fn no_revision_where_the_platform_names_none() {
        assert_eq!(revision_of(None), None);
        assert_eq!(revision_of(Some("  ".into())), None);
        assert_eq!(revision_of(Some("weft-role-dispatcher-00004-289".into())).as_deref(), Some("weft-role-dispatcher-00004-289"));
    }

    fn copy(mine: &str, revisions: &Arc<FakeServiceRevisions>) -> Copy {
        Copy::new(CoreRole::Dispatcher, Arc::from(mine), revisions.clone(), StepAside::new())
    }

    fn announced(revision: &str, started_ms: i64) -> Option<Announced> {
        Some(Announced { revision: revision.into(), started_ms })
    }

    /// At its start a copy stays when the platform routes to it, and
    /// steps aside when it routes elsewhere: a copy of an old revision
    /// that starts late, or of a newer one after a rollback.
    #[tokio::test]
    async fn a_starting_copy_settles_by_the_platform() {
        let revisions = Arc::new(FakeServiceRevisions::new(Ok(routing(&["d-00005"], None))));
        let current = copy("d-00005", &revisions);
        assert_eq!(current.settle().await, Settled::Serving);
        assert!(!current.aside.is_set());

        let late = copy("d-00004", &revisions);
        assert_eq!(late.settle().await, Settled::SteppedAside);
        assert!(late.aside.is_set(), "an old revision's copy starting late steps aside");

        revisions.answer(Ok(routing(&["d-00004"], None)));
        let rolled_back = copy("d-00005", &revisions);
        assert_eq!(rolled_back.settle().await, Settled::SteppedAside);
    }

    /// A copy that cannot ask the platform keeps serving and asks again,
    /// further apart each time, until it gets an answer: it never steps
    /// aside on no answer, nor announces before it has one.
    #[tokio::test(start_paused = true)]
    async fn a_copy_that_cannot_ask_keeps_serving_and_asks_again() {
        let revisions = Arc::new(FakeServiceRevisions::new(Err("the Cloud Run API is unreachable".into())));
        let blind = Arc::new(copy("d-00005", &revisions));
        let settling = tokio::spawn({
            let blind = blind.clone();
            async move { blind.settle().await }
        });
        tokio::time::sleep(RETRY_FIRST * 6).await;
        assert!(!settling.is_finished(), "no answer, nothing to announce");
        assert!(!blind.aside.is_set(), "a platform that cannot be asked replaces nobody");
        assert_eq!(revisions.asked(), 3, "asked at once, then after 2s and 4s more");
        revisions.answer(Ok(routing(&["d-00005"], None)));
        assert_eq!(settling.await.unwrap(), Settled::Serving);
    }

    // A copy of the revision being rolled out asks again until the
    // platform routes to it, and serves then.
    weft_core::stress_test! {
        name: a_copy_being_rolled_out_asks_until_it_is_routed_to,
        runs: 10,
        worker_threads: 2,
        async fn body() {
            let revisions = Arc::new(FakeServiceRevisions::new(Ok(routing(&["d-00004"], Some("d-00005")))));
            let rolling = copy("d-00005", &revisions);
            let settling = tokio::spawn(async move { (rolling.settle().await, rolling) });
            while revisions.asked() < 2 {
                tokio::task::yield_now().await;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert!(!settling.is_finished(), "still incoming");
            revisions.answer(Ok(routing(&["d-00005"], None)));
            let (settled, rolling) = tokio::time::timeout(INCOMING_LOOK * 3, settling).await.expect("settles once routed to").unwrap();
            assert_eq!(settled, Settled::Serving);
            assert!(!rolling.aside.is_set());
        }
    }

    /// A copy asks the platform once per announcement, never for its own
    /// revision's, again for a later announcement of the same revision
    /// (the traffic may have moved since, as after a split), and steps
    /// aside once the platform routes elsewhere.
    #[tokio::test]
    async fn a_copy_asks_once_per_announcement() {
        let revisions = Arc::new(FakeServiceRevisions::new(Ok(routing(&["d-00004", "d-00005"], None))));
        let mut old = copy("d-00004", &revisions);
        assert!(!old.heard(None).await);
        assert!(!old.heard(announced("d-00004", 1)).await, "a sibling announcing");
        assert_eq!(revisions.asked(), 0);

        assert!(!old.heard(announced("d-00005", 2)).await, "a split: it still has a share");
        assert_eq!(revisions.asked(), 1);
        assert!(!old.heard(announced("d-00005", 2)).await);
        assert_eq!(revisions.asked(), 1, "the same announcement, heard again, is not asked about again");

        revisions.answer(Ok(routing(&["d-00005"], None)));
        assert!(old.heard(announced("d-00005", 3)).await, "the same revision announced again, the traffic moved");
        assert!(old.aside.is_set());
    }
}
