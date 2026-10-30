//! The SQL behind the supervisor's lifecycle calls: its ownership tick
//! (`sync_ownership`), which command it runs next (`next_command`), the
//! inserts that issue a command or record an infra event
//! (`issue_command`, `record_event`), and the fenced writes behind its
//! `set_status` and `command_complete`, plus the one answer every
//! fenced write gives when it matched no row. Pool-level (no
//! `BrokerState`, no HTTP) so the layer-3 db suite drives the exact
//! statements the handlers serve; the handlers in `handlers.rs` are
//! the thin scope-checked HTTP wrappers around these.

use anyhow::Context as _;
use weft_task_store::db::{Client, JsonText, Param, Pool, Row, Transaction};

use weft_broker_client::lifecycle_command::{
    command_reaches_copy, live_lease_exists, ownable_project, owns_project_predicate, supervisor_work,
    pending_supervisor_command,
};
use weft_broker_client::protocol::{
    HealthCommandEnding, HealthRuleState, InfraLifecycleVerb, LifecycleOutcome, ProjectStatus, QueuedHealthCommand,
    SupervisorCommandCompleteRequest, SupervisorCommandRow, SupervisorHealthStateResponse, SupervisorProject,
    SupervisorSetHealthStateRequest, SupervisorSetStatusRequest, SupervisorSetWaitingRequest,
    SupervisorSyncOwnershipRequest, SupervisorSyncOwnershipResponse, SUPERVISOR_GIVE_UP_AFTER,
};
use weft_task_store::unanswered::{infra_key_sql, Alerts};

/// One supervisor ownership tick, atomically, so two supervisors never
/// end up owning one project. Steps in one transaction:
///   1. Take the failed passes' retries over the projects the supervisor
///      looked at and still holds (`req.looked`: the look was the
///      retry), and end the failures in a row of the ones whose look
///      failed in nothing; clear what it gave up on for the projects a
///      pass went through with nothing given up (`req.settled`); write
///      down what its passes could not get through (`req.failed`,
///      `req.gave_up`, `req.all_failed`: [`record_troubles`]), which sets
///      the next retry of a pass that failed again.
///   2. A pass ending (`req.ending`): give back every lease, and claim
///      nothing. Otherwise renew this supervisor's
///      existing leases (it is alive and working) over the projects it
///      may own, then claim a BATCH of MORE projects it may own with no
///      live owner, the ones with something due first ([`supervisor_due`]).
///      `FOR UPDATE SKIP LOCKED` + `ON CONFLICT` make
///      concurrent supervisors partition the free projects without
///      double-claiming. A sync that listed the host then clears the
///      retries and failures of what nothing can own any more
///      ([`clear_what_nothing_owns`]).
///   3. With a `booking` (a supervisor that scales to zero): when a
///      supervisor next has something to look at that nothing announces
///      ([`next_due`]), which is booked as its next pass once the
///      transaction commits. Every sync books, a pass's ending one
///      included: the last one of a pass sees what the pass wrote (a
///      window started, a retry set). A booking that fails is logged and
///      the sync still answers (`Booking` writes down a wake it could not
///      set); the next sync books again.
///   4. Return the full owned set (the work loops act only on these), and
///      which of them this tick took on: freshly claimed, or its own lease
///      revived after it lapsed. A command issued on such a project while
///      nobody held it woke no claim of this supervisor's, so it asks
///      again at once.
/// Claiming and renewing both go through ONE condition,
/// `ownable_project` (infra to manage, copies on this host, or a
/// supervisor command waiting), and the owned set is every live lease
/// this supervisor holds. So a lease over a project with nothing left
/// to own is not renewed and lapses, and the supervisor reports a
/// project lost exactly when its lease is gone, never while a command
/// it runs still keeps the project ownable. All time comes from the DB
/// clock, never the app clock, so a skewed host can't mis-judge lease
/// expiry.
pub async fn sync_ownership(
    pool: &Pool,
    req: &SupervisorSyncOwnershipRequest,
    booking: Option<&weft_task_store::drain::Booking>,
) -> anyhow::Result<SupervisorSyncOwnershipResponse> {
    let replica = req.replica.as_str();
    let held_projects: &[uuid::Uuid] = req.held_projects.as_deref().unwrap_or_default();
    let listed = req.held_projects.is_some();
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    if !req.looked.is_empty() {
        // A look that failed in nothing (no `failed`, no `all_failed`)
        // ends the project's failures in a row.
        let clean: Vec<uuid::Uuid> = match req.all_failed {
            Some(_) => Vec::new(),
            None => req.looked.iter().copied().filter(|p| !req.failed.iter().any(|t| t.project_id == *p)).collect(),
        };
        tx.execute(
            &format!(
                "UPDATE infra_health_look l \
                    SET retry_at_unix = NULL, \
                        failures = CASE WHEN l.project_id = ANY($3) THEN 0 ELSE l.failures END \
                  WHERE l.project_id = ANY($1) \
                    AND (l.retry_at_unix IS NOT NULL OR (l.failures <> 0 AND l.project_id = ANY($3))) \
                    AND {held}",
                held = owns_project_predicate("$2", "l.project_id"),
            ),
            &[&req.looked, &replica, &clean],
        )
        .await
        .context("take the retries of the projects the supervisor looked at")?;
    }
    if !req.settled.is_empty() {
        tx.execute(
            &format!("DELETE FROM unanswered_call WHERE callee IN (SELECT {key} FROM unnest($1::uuid[]) AS s)", key = infra_key_sql("s")),
            &[&req.settled],
        )
        .await
        .context("clear what the supervisor gave up on for settled projects")?;
    }
    let alerts = record_troubles(&tx, req, booking.is_some()).await?;
    let claimed = match req.ending {
        true => {
            tx.execute("DELETE FROM infra_owner WHERE supervisor_replica = $1", &[&replica])
                .await
                .context("give back the leases of a pass that ended")?;
            Vec::new()
        }
        false => renew_and_claim(&tx, replica, held_projects, listed).await?,
    };
    if let Some(listing) = &req.held_projects {
        let failed: Vec<uuid::Uuid> = req.failed.iter().map(|t| t.project_id).collect();
        clear_what_nothing_owns(&tx, listing, &failed).await?;
    }
    let next_due_unix = match booking {
        Some(_) => next_due(&tx, replica, held_projects, listed).await?,
        None => None,
    };
    let owned = owned_projects(&tx, replica).await?;
    tx.commit().await.context("commit sync_ownership tx")?;
    // Back to the pool before the booking takes one of its own.
    drop(conn);
    alerts.log();
    if let Some(booking) = booking {
        let at_ms = next_due_unix.map(|s| s.saturating_mul(1000));
        if let Err(e) = booking.book(weft_broker_client::lifecycle_command::SUPERVISOR_PASS_LOOP, at_ms).await {
            tracing::error!(
                target: "weft_broker::supervisor", error = %format!("{e:#}"), ?at_ms,
                "could not book the supervisor's next pass; its next sync books it again"
            );
        }
    }
    let mut claimed = claimed;
    claimed.retain(|id| owned.iter().any(|p| p.project_id == *id));
    claimed.sort_unstable();
    claimed.dedup();
    Ok(SupervisorSyncOwnershipResponse { owned, claimed, next_due_unix })
}

/// Renew `replica`'s leases and claim a batch of free projects; the
/// projects this took on (claimed, or a lapsed lease of its own revived).
/// `held_projects` is its host's latest listing, `listed` whether one
/// went through.
async fn renew_and_claim(
    tx: &Transaction<'_>,
    replica: &str,
    held_projects: &[uuid::Uuid],
    listed: bool,
) -> anyhow::Result<Vec<uuid::Uuid>> {
    let lease_secs = weft_broker_client::lifecycle_command::infra_owner_lease_secs();
    // Renew owned leases. A lease that had lapsed comes back as taken on
    // this tick (the self-join reads the row as it was before this
    // statement).
    let mut claimed: Vec<uuid::Uuid> = tx
        .scalars::<Option<uuid::Uuid>>(
            &format!(
                "UPDATE infra_owner io \
                 SET leased_until_unix = EXTRACT(EPOCH FROM NOW())::BIGINT + $1 \
                 FROM infra_owner prior \
                 WHERE io.supervisor_replica = $2 AND prior.project_id = io.project_id \
                   AND EXISTS (SELECT 1 FROM project p WHERE p.id = io.project_id AND {ownable}) \
                 RETURNING CASE WHEN prior.leased_until_unix < EXTRACT(EPOCH FROM NOW())::BIGINT \
                                THEN io.project_id END",
                ownable = ownable_project("p", "$3"),
            ),
            &[&lease_secs, &replica, &held_projects],
        )
        .await
        .context("renew infra_owner leases")?
    .into_iter()
    .flatten()
    .collect();

    // Claim a batch. `free` selects projects with no live owner and LOCKS
    // them `FOR UPDATE OF p SKIP LOCKED`, so a sibling supervisor's
    // concurrent claim takes a DISJOINT set. ON CONFLICT overwrites a
    // stale (expired-lease) row still present ONLY if its lease is
    // actually expired, so a live owner is never stolen.
    //
    // The projects with something due now come first: the next pass is
    // booked for the earliest of them (`next_due`), so a batch that took
    // others ahead of one would leave it due at every pass, each booked
    // at once.
    let sql = format!(
        "WITH due AS ({due}), free AS ( \
             SELECT p.id AS project_id, p.tenant_id \
             FROM project p \
             WHERE {ownable} \
               AND NOT {leased} \
             ORDER BY p.id IN (SELECT d.project_id FROM due d \
                                WHERE d.at <= EXTRACT(EPOCH FROM NOW())::BIGINT) DESC, \
                      p.id \
             LIMIT $2 \
             FOR UPDATE OF p SKIP LOCKED \
         ), taken AS ( \
             INSERT INTO infra_owner \
                 (project_id, supervisor_replica, tenant_id, leased_until_unix) \
             SELECT project_id, $1, tenant_id, EXTRACT(EPOCH FROM NOW())::BIGINT + $3 \
             FROM free \
             ON CONFLICT (project_id) DO UPDATE \
               SET supervisor_replica = EXCLUDED.supervisor_replica, \
                   tenant_id = EXCLUDED.tenant_id, \
                   leased_until_unix = EXCLUDED.leased_until_unix \
               WHERE infra_owner.leased_until_unix < EXTRACT(EPOCH FROM NOW())::BIGINT \
             RETURNING project_id \
         ) \
         SELECT project_id FROM taken",
        due = supervisor_due("$1", "$4", "$5"),
        ownable = ownable_project("p", "$4"),
        leased = live_lease_exists(None, "p.id"),
    );
    let taken: Vec<uuid::Uuid> = tx
        .scalars(
            &sql,
            &[&replica, &weft_broker_client::lifecycle_command::SUPERVISOR_CLAIM_BATCH, &lease_secs, &held_projects, &listed],
        )
        .await
        .context("claim infra_owner rows")?;
    claimed.extend(taken);
    Ok(claimed)
}

/// Write down what `req.replica`'s passes could not get through, over the
/// projects whose lease it still holds (a project that moved is the new
/// owner's to judge, the same live lease the `looked` update reads). A
/// failed one (`req.failed`, and with `req.all_failed` every project the
/// pass covered: [`SupervisorSyncOwnershipRequest::all_failed`]) counts
/// one more failure in a row (`infra_health_look`), once whatever failed
/// in it, and gets its retry
/// ([`weft_broker_client::protocol::supervisor_retry_after`]), which
/// [`next_due`] books, until it gives up; one a protocol `gave_up` on is
/// only written down. Both show in `weft status` with what happens next.
/// A supervisor that does not scale to zero (`scales_to_zero` false)
/// looks at every project it owns on its own clock and never gives a
/// project up, so its failures are written down as tried again at its
/// next look, and nothing is counted or booked.
///
/// A removed project (no row) has no lease and no page of its own: what
/// a pass could not get through of one is what the host still holds of
/// it, which any pass that lists the host sweeps. Its failures are
/// counted the same way, and all of them are written down as one
/// install-wide failure (`Callee::GoneCopies`), which
/// [`clear_what_nothing_owns`] clears.
async fn record_troubles(
    tx: &Transaction<'_>,
    req: &SupervisorSyncOwnershipRequest,
    scales_to_zero: bool,
) -> anyhow::Result<Alerts> {
    use weft_task_store::unanswered::{failed as write_down, gave_up, Callee};
    let mut alerts = Alerts::default();
    if req.failed.is_empty() && req.gave_up.is_empty() && req.all_failed.is_none() {
        return Ok(alerts);
    }
    let replica = req.replica.as_str();
    let named: Vec<uuid::Uuid> = req.failed.iter().chain(&req.gave_up).map(|t| t.project_id).collect();
    // (project, removed) of every named project still held or removed.
    let judged: Vec<(uuid::Uuid, bool)> = tx
        .all(
            &format!(
                "SELECT n.project_id, NOT EXISTS (SELECT 1 FROM project p WHERE p.id = n.project_id) AS removed \
                   FROM unnest($2::uuid[]) AS n(project_id) \
                  WHERE {owns} OR NOT EXISTS (SELECT 1 FROM project p WHERE p.id = n.project_id)",
                owns = owns_project_predicate("$1", "n.project_id"),
            ),
            &[&replica, &named],
        )
        .await
        .context("read which troubled projects the supervisor still holds, or were removed")?;
    let mut removed: std::collections::BTreeSet<uuid::Uuid> = judged.iter().filter(|(_, removed)| *removed).map(|(p, _)| *p).collect();
    let held: Vec<uuid::Uuid> = judged.iter().filter(|(_, removed)| !*removed).map(|(p, _)| *p).collect();
    let mut failed: std::collections::BTreeMap<uuid::Uuid, String> = req
        .failed
        .iter()
        .filter(|t| held.contains(&t.project_id) || removed.contains(&t.project_id))
        .map(|t| (t.project_id, t.error.clone()))
        .collect();
    if let Some(error) = &req.all_failed {
        // Only a supervisor that scales to zero keeps retries a failed
        // listing can leave unclaimed.
        let listed = req.held_projects.is_some() || !scales_to_zero;
        for (project, was_removed) in all_failed_over(tx, replica, listed).await? {
            if was_removed {
                removed.insert(project);
            }
            let own = failed
                .remove(&project)
                .or_else(|| req.gave_up.iter().find(|t| t.project_id == project).map(|t| t.error.clone()));
            let error = match own {
                Some(own) => format!("{own}; {error}"),
                None => error.clone(),
            };
            failed.insert(project, error);
        }
    }
    let mut removed_failures = Vec::new();
    // Whether the supervisor stopped retrying one of the removed projects.
    let mut removed_given_up = false;
    for (project, error) in &failed {
        if removed.contains(project) {
            let next = match count_failure(tx, *project, scales_to_zero).await? {
                _ if !scales_to_zero => "tried again at the supervisor's next look".to_string(),
                (failures, Some(after)) => format!("failure {failures} in a row; tried again in {} s", after.as_secs()),
                (failures, None) => {
                    removed_given_up = true;
                    format!("failed {failures} times in a row, so the supervisor stopped trying; its next pass tries again")
                }
            };
            removed_failures.push(format!("removed project {project}: {error} ({next})"));
            continue;
        }
        if !scales_to_zero {
            let error = format!("{error} (tried again at the supervisor's next look)");
            alerts = alerts.and(
                write_down(tx, Callee::Infra(*project), None, &error)
                    .await
                    .context("write down the project's failed pass")?,
            );
            continue;
        }
        let written = match count_failure(tx, *project, true).await? {
            (failures, Some(after)) => {
                let error = format!("{error} (failure {failures} in a row; tried again in {} s)", after.as_secs());
                write_down(tx, Callee::Infra(*project), None, &error).await
            }
            (failures, None) => {
                let error = format!("{error} (failed {failures} times in a row, so the supervisor stopped trying; `weft infra look` tries again)");
                gave_up(tx, Callee::Infra(*project), None, &error).await
            }
        };
        alerts = alerts.and(written.context("write down the project's failed pass")?);
    }
    if !removed_failures.is_empty() {
        let error = removed_failures.join("; ");
        let written = match removed_given_up {
            true => gave_up(tx, Callee::GoneCopies, None, &error).await,
            false => write_down(tx, Callee::GoneCopies, None, &error).await,
        };
        alerts = alerts.and(written.context("write down what the host still holds of removed projects")?);
    }
    for trouble in req.gave_up.iter().filter(|t| held.contains(&t.project_id) && !failed.contains_key(&t.project_id)) {
        alerts = alerts.and(
            gave_up(tx, Callee::Infra(trouble.project_id), None, &trouble.error)
                .await
                .context("write down what a health rule gave up on")?,
        );
    }
    Ok(alerts)
}

/// Count one more failed pass over `project` in a row
/// (`infra_health_look.failures`), and, when `retried` (a supervisor that
/// scales to zero), set when a pass tries it again
/// ([`weft_broker_client::protocol::supervisor_retry_after`]): the
/// failures in a row, and how long until the retry, `None` once it gave up.
async fn count_failure(
    tx: &Transaction<'_>,
    project: uuid::Uuid,
    retried: bool,
) -> anyhow::Result<(u32, Option<std::time::Duration>)> {
    let failures: i32 = tx
        .scalar(
            "INSERT INTO infra_health_look (project_id, failures) VALUES ($1, 1) \
             ON CONFLICT (project_id) DO UPDATE SET failures = infra_health_look.failures + 1 \
             RETURNING failures",
            &[&project],
        )
        .await
        .context("count the project's failed pass")?;
    let failures = u32::try_from(failures).with_context(|| format!("infra_health_look.failures is {failures}"))?;
    if !retried {
        return Ok((failures, None));
    }
    let retry_after = weft_broker_client::protocol::supervisor_retry_after(failures);
    tx.execute(
        "UPDATE infra_health_look SET retry_at_unix = EXTRACT(EPOCH FROM NOW())::BIGINT + $2 WHERE project_id = $1",
        &[&project, &retry_after.map(|d| d.as_secs() as i64)],
    )
    .await
    .context("set when the project is tried again")?;
    Ok((failures, retry_after))
}

/// The projects a pass of `replica` covered when a step over all of them
/// failed ([`SupervisorSyncOwnershipRequest::all_failed`]), each with
/// whether it was removed: every one its live lease holds, and, when its
/// host could not be listed (`listed` false), every one whose failed
/// pass's retry is due, that no lease covers, and that only the listing
/// could make ownable (a removed project's leftovers on the host
/// included).
async fn all_failed_over(tx: &Transaction<'_>, replica: &str, listed: bool) -> anyhow::Result<Vec<(uuid::Uuid, bool)>> {
    tx.all(
        &format!(
            "SELECT held.project_id, FALSE FROM infra_owner held WHERE {owns} \
             UNION \
             SELECT l.project_id, p.id IS NULL FROM infra_health_look l LEFT JOIN project p ON p.id = l.project_id \
              WHERE NOT $2 AND l.retry_at_unix <= EXTRACT(EPOCH FROM NOW())::BIGINT \
                AND NOT {leased} AND (p.id IS NULL OR NOT {ownable})",
            owns = owns_project_predicate("$1", "held.project_id"),
            leased = live_lease_exists(None, "l.project_id"),
            ownable = ownable_project("p", "'{}'::uuid[]"),
        ),
        &[&replica, &listed],
    )
    .await
    .context("read which projects a pass that failed as a whole covered")
}

/// A sync that listed the host (`held_projects`, its listing) clears what
/// nothing can own any more, so no later pass that cannot list the host
/// finds it due and counts a failure against it:
/// - a project with a failed pass's retry or failures in a row, that no
///   live lease covers and that is not ownable by this listing: its retry
///   and failures are cleared, and what was written down of its infra;
/// - a removed project that is neither in the listing nor failed in this
///   sync (`failed`): the host holds nothing of it any more, so its row
///   goes, and once no removed project's row is left, the install-wide
///   failure of the sweep of removed projects (`Callee::GoneCopies`).
async fn clear_what_nothing_owns(tx: &Transaction<'_>, held_projects: &[uuid::Uuid], failed: &[uuid::Uuid]) -> anyhow::Result<()> {
    let sql = format!(
        "WITH stale AS ( \
             SELECT l.project_id, p.id IS NULL AS removed \
               FROM infra_health_look l LEFT JOIN project p ON p.id = l.project_id \
              WHERE (l.retry_at_unix IS NOT NULL OR l.failures <> 0) \
                AND NOT (l.project_id = ANY($1)) AND NOT (l.project_id = ANY($2)) \
                AND NOT {leased} AND (p.id IS NULL OR NOT {ownable}) \
         ), dropped AS ( \
             DELETE FROM infra_health_look l USING stale s WHERE s.removed AND l.project_id = s.project_id \
         ), reset AS ( \
             UPDATE infra_health_look l SET retry_at_unix = NULL, failures = 0 \
               FROM stale s WHERE NOT s.removed AND l.project_id = s.project_id \
         ) \
         DELETE FROM unanswered_call u \
          WHERE u.callee IN (SELECT {infra_key} FROM stale s WHERE NOT s.removed) \
             OR (u.callee = {gone_key} AND NOT EXISTS ( \
                     SELECT 1 FROM infra_health_look l \
                      WHERE NOT EXISTS (SELECT 1 FROM project p WHERE p.id = l.project_id) \
                        AND l.project_id NOT IN (SELECT s.project_id FROM stale s)))",
        leased = live_lease_exists(None, "l.project_id"),
        ownable = ownable_project("p", "$1"),
        infra_key = infra_key_sql("s.project_id"),
        gone_key = weft_task_store::unanswered::gone_copies_key_sql(),
    );
    tx.execute(&sql, &[&held_projects, &failed])
        .await
        .context("clear the retries and failures of what nothing can own any more")?;
    Ok(())
}

/// When a supervisor next has something to look at that nothing
/// announces (unix seconds, database clock), or `None`: the earliest of
/// [`supervisor_due`], `held_projects` being the projects this replica's
/// host held at its latest listing, `listed` whether one went through.
async fn next_due(db: &impl Client, replica: &str, held_projects: &[uuid::Uuid], listed: bool) -> anyhow::Result<Option<i64>> {
    db.scalar(&format!("SELECT MIN(d.at) FROM ({due}) d", due = supervisor_due("$1", "$2", "$3")), &[&replica, &held_projects, &listed])
        .await
        .context("look for when the supervisors next have something to do")
}

/// The SQL rows `(project_id, at)` (unix seconds, database clock) of what
/// the supervisors have to look at that nothing announces, for the
/// supervisor bound at `replica_param` whose host holds the projects
/// bound at `held_param`: a command waiting on a project no lease covers
/// (now), a unit's health window ending, a health rule's retry, the
/// deadline of a command a health rule queued (its ending asks for a look
/// itself; the deadline is for one that never ends), a
/// failed pass's retry (once no lease covers the project: its holder
/// looks anyway; and only while the project is ownable: nothing claims
/// one that is not, so its retry stays set until the project is ownable
/// again; when the host could not be listed, `listed_param` false, a
/// project only the listing could make ownable keeps its retry booked, and
/// the pass counts the failed listing against it; a removed project's,
/// whose leftovers on the host any pass sweeps, is always booked), or the
/// lease another
/// replica holds lapsing over a project with
/// work or running infra (that replica may be gone, and a lease is taken
/// over only once it lapsed). A time at or before which the project's
/// infra was written down as failing is left out: the look then already
/// failed, and the pass's retry, a person's look or the next real event
/// tries again. A retry is always set after its failure was written, so
/// it is never left out. [`next_due`] books the earliest; the claim takes
/// the projects with one due now first.
fn supervisor_due(replica_param: &str, held_param: &str, listed_param: &str) -> String {
    // SYNC: the unit stamps <-> UnitRuntime (not_ready_since_unix, ready_since_unix, flaky_after_seconds, recovery_after_seconds)
    format!(
        "SELECT d.project_id, d.at FROM ( \
             SELECT c.project_id, EXTRACT(EPOCH FROM NOW())::BIGINT \
               FROM infra_lifecycle_command c \
              WHERE {pending_c} AND NOT c.drains \
                AND NOT {leased} \
                AND NOT EXISTS (SELECT 1 FROM infra_lifecycle_command o \
                                 WHERE {pending_o} AND o.project_id = c.project_id \
                                   AND o.id < c.id AND {overlap}) \
           UNION ALL \
             SELECT u.project_id, \
                    CASE u.status \
                      WHEN 'running' THEN u.not_ready_since_unix + u.flaky_after_seconds \
                      WHEN 'flaky' THEN u.ready_since_unix + u.recovery_after_seconds \
                    END \
               FROM infra_unit u \
              WHERE u.status IN ('running', 'flaky') \
           UNION ALL \
             SELECT h.project_id, h.next_retry_at_unix FROM infra_health_rule h \
              WHERE h.gave_up_at_unix IS NULL AND h.next_retry_at_unix IS NOT NULL \
           UNION ALL \
             SELECT h.project_id, h.queued_deadline_unix FROM infra_health_rule h \
              WHERE h.queued_deadline_unix IS NOT NULL \
           UNION ALL \
             SELECT l.project_id, GREATEST(l.retry_at_unix, ( \
                        SELECT MAX(io.leased_until_unix) + 1 FROM infra_owner io \
                         WHERE io.project_id = l.project_id \
                           AND io.leased_until_unix >= EXTRACT(EPOCH FROM NOW())::BIGINT)) \
               FROM infra_health_look l LEFT JOIN project p ON p.id = l.project_id \
              WHERE l.retry_at_unix IS NOT NULL AND (p.id IS NULL OR {ownable} OR NOT {listed_param}) \
           UNION ALL \
             SELECT io.project_id, io.leased_until_unix + 1 \
               FROM infra_owner io JOIN project p ON p.id = io.project_id \
              WHERE io.supervisor_replica <> {replica_param} AND ({work} OR {runs}) \
         ) AS d(project_id, at) \
          WHERE d.at IS NOT NULL \
            AND NOT EXISTS (SELECT 1 FROM unanswered_call u \
                             WHERE u.callee = {key} AND u.last_ms >= d.at * 1000)",
        pending_c = pending_supervisor_command("c"),
        pending_o = pending_supervisor_command("o"),
        leased = live_lease_exists(None, "c.project_id"),
        overlap = weft_broker_client::lifecycle_command::commands_overlap("o", "c"),
        work = supervisor_work("p"),
        runs = weft_broker_client::lifecycle_command::runs_infra("p"),
        key = infra_key_sql("d.project_id"),
        ownable = ownable_project("p", held_param),
    )
}

/// The projects a supervisor owns (every live `infra_owner` lease it
/// holds, whatever made the project ownable), joined to live project state,
/// on any `Client` (a pool or the ownership tick's transaction).
pub async fn owned_projects(db: &impl Client, replica: &str) -> anyhow::Result<Vec<SupervisorProject>> {
    // Every activation of the owned projects rides along (a project with
    // none has one row of NULLs from the LEFT JOIN): the health loop reads
    // the project's listening as ONE lifecycle, the aggregate over all its
    // activations, every owner's, and whether any activation it took down
    // itself is still down.
    let sql = format!(
        "SELECT p.id AS project_id, p.tenant_id, \
                a.status, a.accepting_fires, a.fires_visible_to_consumers, a.fires_deadline_unix, \
                a.went_down_with, a.went_down_by_command, a.activating_run_id \
         FROM infra_owner io \
         JOIN project p ON p.id = io.project_id \
         LEFT JOIN trigger_activation a ON a.project_id = p.id \
         WHERE {owns} \
         ORDER BY p.id",
        owns = owns_project_predicate("$1", "p.id"),
    );
    let rows = db.query(&sql, &[&replica]).await?;
    let mut projects: Vec<(uuid::Uuid, String, Vec<weft_broker_client::activation::ActivationLifecycle>)> = Vec::new();
    for r in &rows {
        let project_id: uuid::Uuid = r.try_get("project_id").context("decode project_id")?;
        if projects.last().map(|p| p.0) != Some(project_id) {
            projects.push((
                project_id,
                r.try_get("tenant_id").context("decode tenant_id")?,
                Vec::new(),
            ));
        }
        let status: Option<String> = r.try_get("status").context("decode status")?;
        let Some(status) = status else { continue };
        let lifecycle = weft_broker_client::activation::ActivationLifecycle {
            status: ProjectStatus::parse(&status)
                .with_context(|| format!("trigger_activation.status='{status}' is not a known status"))?,
            accepting_fires: r.try_get("accepting_fires").context("decode accepting_fires")?,
            fires_visible_to_consumers: r.try_get("fires_visible_to_consumers").context("decode visibility")?,
            fires_deadline_unix: r.try_get("fires_deadline_unix").context("decode deadline")?,
            went_down: weft_broker_client::activation::WentDown::of_columns(
                r.try_get::<_, Option<String>>("went_down_with").context("decode went_down_with")?.as_deref(),
                r.try_get::<_, Option<i64>>("went_down_by_command").context("decode went_down_by_command")?,
            )
            .map_err(anyhow::Error::msg)?,
            activating_run_id: r.try_get("activating_run_id").context("decode activating_run_id")?,
        };
        projects.last_mut().expect("pushed above").2.push(lifecycle);
    }
    Ok(projects
        .into_iter()
        .map(|(project_id, tenant_id, lifecycles)| {
            let aggregate = weft_broker_client::activation::aggregate(&lifecycles);
            SupervisorProject {
                project_id,
                tenant_id,
                status: aggregate.status,
                health_down: lifecycles
                    .iter()
                    .any(weft_broker_client::activation::ActivationLifecycle::down_by_health),
            }
        })
        .collect())
}

/// Claim the command `claimer_replica` runs next: the oldest uncompleted
/// one of a project it owns (the `infra_owner` exclusive lease) that it
/// may take (`supervisor_claimable`: not already running it, `busy`, and
/// nobody else's claim still held) and that no older uncompleted command
/// of the project reaches a copy of (`commands_overlap`), or `None`.
///
/// Ownership is the supervisor's one single-actor authority: exclusive
/// (one process per project) and renewed on every ownership tick, so two
/// supervisors never change one project's infrastructure. Inside the owner,
/// the commands that touch a copy run in the order they were issued, since
/// a younger one waits until every older one touching one of its copies has
/// completed, and the ones touching different copies run side by side:
/// three instances started together come up together.
///
/// The claim marks the row (`claimed_by_replica`) with no expiry of its
/// own: it holds while the claimer holds the project's lease. So a cancel
/// flags a command under way instead of completing it (the dispatcher's
/// `request_cancel_owner` completes only unclaimed rows), and a second
/// pass or a sibling never takes it while it runs. If ownership moves
/// mid-command, every write from the old owner is refused
/// (`owns_project_predicate` on the fenced writes below) and the new owner
/// claims it and runs it again, which is safe because the supervisor's
/// infrastructure work is declarative.
///
/// Only the supervisor's verbs: `deactivate` and `reactivate` are the
/// dispatcher's, claimed by dispatchers under their own
/// `claimed_by_replica` lease. A stop or terminate still waiting for the
/// running work it reaches (`drains`) is the dispatcher's until
/// that wait is over; it still holds back the younger commands that
/// overlap it.
///
/// Every claim counts a run of the command (`runs`). A command claimed
/// [`SUPERVISOR_GIVE_UP_AFTER`] times never ended: its run or the write
/// of its ending keeps failing, or its supervisor keeps dying on it. The
/// same statement gives it up instead of handing it out again, and
/// answers the ones it gave up: its outcome says so to whoever issued it,
/// the same statement writes it down as given up for the project's
/// `weft status` (`Callee::Commands`, which an install that alerts alerts
/// on, cleared by [`complete_command`]), and the younger commands it held
/// back are free to go.
pub async fn claim_next(
    pool: &Pool,
    claimer_replica: &str,
    busy_commands: &[i64],
) -> anyhow::Result<Option<SupervisorCommandRow>> {
    let sql = format!(
        "WITH spent AS ( \
             UPDATE infra_lifecycle_command c \
             SET completed_at_unix = EXTRACT(EPOCH FROM NOW())::BIGINT, outcome = $4, outcome_message = $5 \
             WHERE {pending} \
               AND NOT c.drains \
               AND {owns} \
               AND {claimable} \
               AND c.runs >= $3 \
             RETURNING c.id, c.project_id \
         ), claimed AS ( \
             UPDATE infra_lifecycle_command \
             SET claimed_by_replica = $1, claimed_at_unix = EXTRACT(EPOCH FROM NOW())::BIGINT, runs = runs + 1 \
             WHERE id = ( \
                 SELECT c.id FROM infra_lifecycle_command c \
                 WHERE {pending} \
                   AND NOT c.drains \
                   AND {owns} \
                   AND {claimable} \
                   AND c.runs < $3 \
                   AND NOT EXISTS ( \
                       SELECT 1 FROM infra_lifecycle_command o \
                       WHERE {older_pending} AND o.project_id = c.project_id AND o.id < c.id AND {overlap} \
                         AND o.id NOT IN (SELECT id FROM spent) \
                   ) \
                 ORDER BY c.id ASC \
                 LIMIT 1 \
                 FOR UPDATE SKIP LOCKED \
             ) \
             RETURNING id, project_id, node_id, verb, spec_json, force, instance_id, every_copy, cancel_requested \
         ), gave_up AS ({gave_up}) \
         SELECT claimed.*, {alerted} AS alerted \
         FROM (SELECT) one LEFT JOIN claimed ON TRUE",
        pending = pending_supervisor_command("c"),
        older_pending = pending_supervisor_command("o"),
        owns = owns_project_predicate("$1", "c.project_id"),
        claimable = weft_broker_client::lifecycle_command::supervisor_claimable("c", "$1", "$2"),
        overlap = weft_broker_client::lifecycle_command::commands_overlap("o", "c"),
        // The commands given up, one failure per project naming them
        // oldest first.
        gave_up = weft_task_store::unanswered::write_sql(
            &format!(
                "SELECT {callee} AS callee, NULL::text AS label, \
                     'command ' || string_agg(s.id::text, ', ' ORDER BY s.id) || ': ' || $5::text AS error, 0::bigint AS lasted_ms \
                 FROM spent s GROUP BY s.project_id",
                callee = weft_task_store::unanswered::commands_key_sql("s.project_id"),
            ),
            "TRUE",
        ),
        alerted = weft_task_store::unanswered::alerted_sql("gave_up"),
    );
    let row: Row = pool
        .one(
            &sql,
            &[&claimer_replica, &busy_commands, &(SUPERVISOR_GIVE_UP_AFTER as i32), &LifecycleOutcome::Failed.as_str(), &gave_up_message()],
        )
        .await?;
    // One statement on the pool: committed once it answered.
    Alerts::of(row.try_get("alerted")?).log();
    match row.try_get::<_, Option<i64>>("id")? {
        Some(_) => Ok(Some(decode_command(&row)?)),
        None => Ok(None),
    }
}

/// The outcome of a command [`claim_next`] gave up.
fn gave_up_message() -> String {
    format!(
        "the supervisor took this command up {SUPERVISOR_GIVE_UP_AFTER} times and it never ended (its run, or the \
         write of how it ended, kept failing), so it gave the command up; issue it again"
    )
}

/// Where each health rule of `project_id`'s copies stands
/// (`infra_health_rule`), and how the commands they queued ended. A rule
/// that gave up comes back armed once a person's infra command touched
/// the project since, or a person asked for a look since
/// (`infra_health_look.asked_at_unix`): its failures and its give-up are
/// cleared (whether it fired, the command it queued and what it took
/// down are kept), and the supervisor's next write of the state stores
/// that.
pub async fn health_state(pool: &Pool, project_id: uuid::Uuid) -> anyhow::Result<SupervisorHealthStateResponse> {
    let rows = pool
        .query(
            "SELECT h.node_id, h.instance_id, h.rule, h.fired, h.consecutive_failures, h.next_retry_at_unix, \
                    h.gave_up_at_unix, h.gave_up_seen, h.last_error, h.queued_command, h.queued_deadline_unix, h.took_down, \
                    (EXISTS (SELECT 1 FROM infra_lifecycle_command c \
                              WHERE c.project_id = h.project_id AND c.issued_at_unix >= h.gave_up_at_unix \
                                AND c.verb IN ('apply', 'stop', 'terminate', 'upgrade')) \
                     OR EXISTS (SELECT 1 FROM infra_health_look l \
                                 WHERE l.project_id = h.project_id AND l.asked_at_unix >= h.gave_up_at_unix)) AS touched \
             FROM infra_health_rule h WHERE h.project_id = $1 ORDER BY h.node_id, h.instance_id NULLS FIRST, h.rule",
            &[&project_id],
        )
        .await
        .context("read the health rules' state")?;
    let rules = rows
        .iter()
        .map(|r| {
            let rule: String = r.try_get("rule")?;
            let node_id: String = r.try_get("node_id")?;
            let instance = r
                .try_get::<_, Option<String>>("instance_id")?
                .map(weft_core::instance::InstanceId::new)
                .transpose()
                .map_err(|e| anyhow::anyhow!("infra_health_rule.instance_id of '{rule}' on '{node_id}': {e}"))?;
            let copy = weft_broker_client::protocol::InfraCopy { node_id, instance };
            let queued = match (r.try_get::<_, Option<i64>>("queued_command")?, r.try_get::<_, Option<i64>>("queued_deadline_unix")?) {
                (Some(command), Some(deadline_unix)) => Some(QueuedHealthCommand { command, deadline_unix }),
                (None, None) => None,
                (command, deadline) => anyhow::bail!(
                    "infra_health_rule '{rule}' on '{}' has queued_command {command:?} and queued_deadline_unix {deadline:?}: both or neither",
                    copy.node_id
                ),
            };
            let fired: bool = r.try_get("fired")?;
            let took_down: bool = r.try_get("took_down")?;
            if r.try_get::<_, bool>("touched")? {
                return Ok(HealthRuleState {
                    copy,
                    rule,
                    fired,
                    queued,
                    took_down,
                    consecutive_failures: 0,
                    next_retry_at_unix: None,
                    gave_up_at_unix: None,
                    gave_up_seen: None,
                    last_error: None,
                });
            }
            let failures: i32 = r.try_get("consecutive_failures")?;
            Ok(HealthRuleState {
                consecutive_failures: u32::try_from(failures)
                    .with_context(|| format!("infra_health_rule.consecutive_failures of '{rule}' is {failures}"))?,
                copy,
                rule,
                fired,
                queued,
                took_down,
                next_retry_at_unix: r.try_get("next_retry_at_unix")?,
                gave_up_at_unix: r.try_get("gave_up_at_unix")?,
                gave_up_seen: r.try_get("gave_up_seen")?,
                last_error: r.try_get("last_error")?,
            })
        })
        .collect::<anyhow::Result<Vec<HealthRuleState>>>()?;
    let queued: Vec<i64> = rules.iter().filter_map(|r| r.queued.map(|q| q.command)).collect();
    let ended = match queued.is_empty() {
        true => Vec::new(),
        false => pool
            .query(
                "SELECT id, outcome, outcome_message FROM infra_lifecycle_command \
                 WHERE id = ANY($1) AND completed_at_unix IS NOT NULL ORDER BY id",
                &[&queued],
            )
            .await
        .context("read how the health rules' commands ended")?
        .iter()
        .map(|r| {
            let command: i64 = r.try_get("id")?;
            let outcome: Option<String> = r.try_get("outcome")?;
            let outcome = outcome
                .as_deref()
                .and_then(LifecycleOutcome::parse)
                .ok_or_else(|| anyhow::anyhow!("infra_lifecycle_command {command} completed with outcome {outcome:?}"))?;
            Ok(HealthCommandEnding { command, outcome, message: r.try_get("outcome_message")? })
        })
        .collect::<anyhow::Result<Vec<HealthCommandEnding>>>()?,
    };
    Ok(SupervisorHealthStateResponse { rules, ended })
}

/// Replace a project's health rules' state with `req.rules` (a rule on a
/// copy not listed has none left), for the supervisor holding its lease.
/// The lease row is read `FOR SHARE` first in the same transaction, so a
/// takeover waits for the write and a displaced supervisor writes
/// nothing.
pub async fn set_health_state(pool: &Pool, req: &SupervisorSetHealthStateRequest) -> anyhow::Result<FencedWrite> {
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    let owns: bool = tx
        .scalar(
            "SELECT EXISTS (SELECT 1 FROM infra_owner WHERE project_id = $1 AND supervisor_replica = $2 \
                              AND leased_until_unix >= EXTRACT(EPOCH FROM NOW())::BIGINT FOR SHARE)",
            &[&req.project_id, &req.replica],
        )
        .await
        .context("read the project's lease")?;
    if !owns {
        return Ok(FencedWrite::Displaced);
    }
    tx.execute("DELETE FROM infra_health_rule WHERE project_id = $1", &[&req.project_id])
        .await
        .context("drop the project's health rules' state before writing it again")?;
    for r in &req.rules {
        let failures = i32::try_from(r.consecutive_failures).context("a rule's failures fit an INTEGER")?;
        tx.execute(
            "INSERT INTO infra_health_rule \
                 (project_id, node_id, instance_id, rule, fired, consecutive_failures, next_retry_at_unix, \
                  gave_up_at_unix, gave_up_seen, last_error, queued_command, queued_deadline_unix, took_down) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
            &[
                &req.project_id,
                &r.copy.node_id,
                &r.copy.instance.as_ref().map(|i| i.as_str()),
                &r.rule,
                &r.fired,
                &failures,
                &r.next_retry_at_unix,
                &r.gave_up_at_unix,
                &r.gave_up_seen,
                &r.last_error,
                &r.queued.map(|q| q.command),
                &r.queued.map(|q| q.deadline_unix),
                &r.took_down,
            ],
        )
        .await
        .with_context(|| format!("write health rule '{}' on '{}'", r.rule, r.copy.node_id))?;
    }
    tx.commit().await.context("commit the health rules' state")?;
    Ok(FencedWrite::Applied)
}

/// Which of the `busy` commands a person asked to cancel, but the ones
/// whose cancel the supervisor already heard (`cancelling`).
pub async fn cancels_for(pool: &Pool, busy: &[i64], cancelling: &[i64]) -> anyhow::Result<Vec<i64>> {
    pool.scalars(
        "SELECT id FROM infra_lifecycle_command \
         WHERE id = ANY($1) AND cancel_requested AND NOT (id = ANY($2)) ORDER BY id",
        &[&busy, &cancelling],
    )
    .await
    .context("read the cancels of the supervisor's busy commands")
}

/// Whether a supervisor command waits on a project no supervisor holds
/// a live lease over.
pub async fn unowned_work_waiting(pool: &Pool) -> anyhow::Result<bool> {
    let sql = format!(
        "SELECT EXISTS ( \
             SELECT 1 FROM infra_lifecycle_command c \
             WHERE {pending} AND NOT {leased} \
         )",
        pending = pending_supervisor_command("c"),
        leased = live_lease_exists(None, "c.project_id"),
    );
    Ok(pool.scalar(&sql, &[]).await?)
}

/// A lifecycle command to issue. The tenant is the project's, never
/// the caller's.
pub struct IssuedCommand<'a> {
    pub tenant_id: &'a str,
    pub project_id: uuid::Uuid,
    pub node_id: Option<&'a str>,
    /// Which copies of the node it acts on.
    pub copies: &'a weft_core::instance::Copies,
    pub verb: InfraLifecycleVerb,
    pub spec_json: Option<&'a serde_json::Value>,
    /// An apply's place hash in the build its setup ran, stamped on the
    /// copy as the apply succeeds (`infra_lifecycle_command.source_hash`).
    pub source_hash: Option<&'a str>,
    pub issued_by_replica: &'a str,
}

/// Issue a lifecycle command; its id, or `None` when the project row is
/// gone. The caller's authorization comes from a project-to-tenant cache
/// that can outlive a removal, so the insert reads the live row itself:
/// a command for a deleted project would sit forever with nobody to run
/// it. It reads it `FOR KEY SHARE`: a removal that has deleted the row
/// but not committed makes the insert wait and then find nothing, and
/// an insert that got the lock first commits before the removal's own
/// transaction deletes the project's commands (`ProjectStore::remove`).
///
/// An apply is deduplicated against an in-flight apply for the same
/// copy, so a worker restart retrying the call never issues it twice:
/// the partial unique index `uq_lifecycle_cmd_pending_apply` allows one
/// pending apply per (project_id, node_id, instance_id), and on a clash
/// the no-op `DO UPDATE` hands back the existing row's id in the same
/// statement (a `DO NOTHING` would return no row and need a second read
/// that races the row's completion). Other verbs never match that
/// index's predicate.
pub async fn issue_command(pool: &Pool, cmd: &IssuedCommand<'_>) -> anyhow::Result<Option<i64>> {
    let (instance_id, every_copy) = cmd.copies.columns();
    pool.scalar_opt(
        "INSERT INTO infra_lifecycle_command \
         (tenant_id, project_id, node_id, verb, \
          spec_json, issued_by_replica, issued_at_unix, instance_id, every_copy, source_hash) \
         SELECT $1, p.id, $3, $4, $5, $6, EXTRACT(EPOCH FROM NOW())::BIGINT, $7, $8, $9 \
         FROM project p WHERE p.id = $2 \
         FOR KEY SHARE OF p \
         ON CONFLICT (project_id, node_id, instance_id) \
           WHERE completed_at_unix IS NULL AND verb = 'apply' \
           DO UPDATE SET issued_at_unix = infra_lifecycle_command.issued_at_unix \
         RETURNING id",
        &[
            &cmd.tenant_id,
            &cmd.project_id,
            &cmd.node_id,
            &cmd.verb.as_str(),
            &cmd.spec_json,
            &cmd.issued_by_replica,
            &instance_id,
            &every_copy,
            &cmd.source_hash,
        ],
    )
    .await
    .context("issue infra_lifecycle_command")
}

/// The hash of the infra at `place` in the build run `run_id` of
/// `project_id` runs (`project_definition.infra_places` of the program its
/// cut was cut from, `run.cut_from`): what an apply that run issues stamps on the
/// copy. `None` when the run runs no registered program (a node test) or
/// its build named no hash for the place: the copy then reads as behind
/// until an apply stamps one.
pub async fn place_hash_of_run(pool: &Pool, project_id: uuid::Uuid, run_id: weft_core::RunId, place: &str) -> anyhow::Result<Option<String>> {
    let found: Option<(uuid::Uuid, Option<String>)> = pool
        .opt(
            "SELECT r.run_id, p.infra_places ->> $3::text FROM run r \
             LEFT JOIN project_definition p ON p.project_id = r.project_id AND p.definition_hash = COALESCE(r.cut_from, r.definition_hash) \
             WHERE r.run_id = $1 AND r.project_id = $2",
            &[&run_id, &project_id, &place],
        )
        .await
        .context("the place hash of an infra setup's build")?;
    let Some((_, hash)) = found else {
        anyhow::bail!("run {run_id} of project {project_id} applies infra, and no such run is on record");
    };
    Ok(hash)
}

/// Record one infra event; its id, or `None` when the project row is
/// gone (read live, for the reason `issue_command` gives).
pub async fn record_event(
    pool: &Pool,
    tenant_id: &str,
    project_id: uuid::Uuid,
    node_id: Option<&str>,
    instance: Option<&weft_core::instance::InstanceId>,
    kind: &str,
    payload: &serde_json::Value,
) -> anyhow::Result<Option<i64>> {
    pool.scalar_opt(
        "INSERT INTO infra_event \
         (tenant_id, project_id, node_id, kind, payload, at_unix, instance_id) \
         SELECT $1, p.id, $3, $4, $5, EXTRACT(EPOCH FROM NOW())::BIGINT, $6 \
         FROM project p WHERE p.id = $2 \
         FOR KEY SHARE OF p \
         RETURNING id",
        &[&tenant_id, &project_id, &node_id, &kind, &JsonText(payload), &instance.map(|i| i.as_str())],
    )
    .await
    .context("record infra_event")
}

/// Set `values` over what copy `instance` of infra node `place` of
/// `project` baked before, the rest kept; `false` when the copy has no
/// row. Merged here, on the locked row: Postgres reads no JSON holding a
/// NUL character, and a baked value may hold one.
pub async fn save_baked(
    pool: &Pool,
    project: uuid::Uuid,
    place: &str,
    instance: Option<&str>,
    values: &std::collections::BTreeMap<String, serde_json::Value>,
) -> anyhow::Result<bool> {
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    let baked: Option<JsonText<std::collections::BTreeMap<String, serde_json::Value>>> = tx
        .scalar_opt(
            "SELECT baked_json FROM infra_node WHERE project_id = $1 AND node_id = $2 AND instance_id IS NOT DISTINCT FROM $3 FOR UPDATE",
            &[&project, &place, &instance],
        )
        .await?;
    let Some(JsonText(mut baked)) = baked else { return Ok(false) };
    baked.extend(values.clone());
    tx.execute(
        "UPDATE infra_node SET baked_json = $4 WHERE project_id = $1 AND node_id = $2 AND instance_id IS NOT DISTINCT FROM $3",
        &[&project, &place, &instance, &JsonText(&baked)],
    )
    .await?;
    tx.commit().await?;
    Ok(true)
}

/// Decode one `infra_lifecycle_command` row into the typed
/// `SupervisorCommandRow` wire shape. EVERY column read propagates
/// errors; an unknown verb becomes a 500 rather than a silent fallback.
fn decode_command(r: &Row) -> anyhow::Result<SupervisorCommandRow> {
    let id: i64 = r.try_get("id")?;
    let project_id: uuid::Uuid = r.try_get("project_id")?;
    let node_id: Option<String> = r.try_get::<_, Option<String>>("node_id")?;
    let verb_str: String = r.try_get("verb")?;
    let verb = InfraLifecycleVerb::parse(&verb_str)
        .ok_or_else(|| anyhow::anyhow!("infra_lifecycle_command.id={id}: unknown verb '{verb_str}'"))?;
    let spec_json: Option<serde_json::Value> =
        r.try_get::<_, Option<serde_json::Value>>("spec_json")?;
    let force: bool = r.try_get("force")?;
    let instance_id: Option<String> = r.try_get("instance_id")?;
    let every_copy: bool = r.try_get("every_copy")?;
    let cancel_requested: bool = r.try_get("cancel_requested")?;
    let copies = weft_core::instance::Copies::from_columns(instance_id, every_copy)
        .map_err(|e| anyhow::anyhow!("infra_lifecycle_command.id={id}: {e}"))?;
    Ok(SupervisorCommandRow {
        id,
        project_id,
        node_id,
        copies,
        verb,
        spec_json,
        force,
        cancel_requested,
    })
}

/// What a fenced lifecycle write did. The two stale answers are
/// deliberately distinct because the supervisor must do different
/// things with them (see `WriteOutcome` in the client crate, the wire
/// twin of this): `Displaced`, the process no longer owns the project, it
/// leaves the command for the new owner; `Gone`, the process still owns
/// the project and the target itself is not there (row removed, unit
/// not in the roster, command already completed), the process's work for
/// that target is moot and the command proceeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FencedWrite {
    Applied,
    Displaced,
    Gone,
}

/// The answer to a fenced write that matched no row: one ownership
/// SELECT settles which predicate failed (the WHERE that just failed
/// cannot say). Displaced when `replica` no longer holds the
/// project's `infra_owner` lease, Gone otherwise.
pub async fn stale_answer(pool: &Pool, replica: &str, project_id: uuid::Uuid) -> anyhow::Result<FencedWrite> {
    let owns: bool = pool
        .scalar(&format!("SELECT {owns}", owns = owns_project_predicate("$1", "$2")), &[&replica, &project_id])
        .await?;
    Ok(if owns { FencedWrite::Gone } else { FencedWrite::Displaced })
}

/// Roll the unit statuses `statuses` (a query of one column) up to one
/// node status (worst-of-units by `InfraNodeStatus::rollup_rank`). Must
/// match `InfraNodeStatus::rollup_rank` in the protocol crate. No units ->
/// 'stopped' (the rank-0 default).
fn rollup_sql(statuses: &str) -> String {
    format!(
        "(SELECT COALESCE( \
            (SELECT s.status FROM ({statuses}) AS s(status) \
             ORDER BY CASE s.status \
               WHEN 'terminating' THEN 7 WHEN 'stopping' THEN 6 \
               WHEN 'provisioning' THEN 5 WHEN 'failed' THEN 4 \
               WHEN 'flaky' THEN 3 WHEN 'running' THEN 2 \
               WHEN 'stopped' THEN 1 ELSE 0 END DESC \
             LIMIT 1), \
            'stopped'))"
    )
}

/// `value` as the next `$n` of `params`: a statement whose parameters
/// depend on the request binds each one only where it reads it, since
/// every parameter a statement takes must appear in it.
fn bind<'a>(params: &mut Vec<Param<'a>>, value: Param<'a>) -> String {
    params.push(value);
    format!("${}", params.len())
}

/// Stamp a status on an `infra_node` row, per unit or node-wide, under
/// the write's fence.
///
/// Per-unit (`unit = Some`): set that unit's status on its `infra_unit`
/// row, then recompute the node-level `status` as the rollup over the
/// units as they now stand. Node-wide (`unit = None`): set every unit's
/// status AND the node status to the same value (a lifecycle-driven
/// uniform transition like Stopping/Terminating).
///
/// Both run as one transaction of two statements. The first reads the
/// copy's `infra_node` row under the fence, locks it (`FOR UPDATE`), and
/// writes the units, acting only on the row it found. The second writes
/// the node status. They are two statements because of the rollup: under
/// READ COMMITTED a statement reads the table as it stood when the
/// statement began, so a rollup in the first statement could miss another
/// unit's stamp that committed while this one waited on the row lock, and
/// two units of one copy stamped at once could each leave the node rolled
/// up from a roster without the other's new status. Every stamp of a copy
/// takes that row lock before it writes a unit (and the apply's roster
/// write, [`write_units`], runs in the transaction that upserted the row),
/// so writes to one copy's units run one after another, and the second
/// statement, which begins once the lock is held, rolls up every unit as
/// committed plus this stamp's own.
///
/// Per-unit, the unit's presence in the roster is part of the fence, so
/// a stamp for a unit the row does not carry matches no row and answers
/// Gone. That is a real race for the health engine (its roster was read
/// at the top of the tick) and never a write.
///
/// The fence itself: `replica` must still hold the project's
/// `infra_owner` lease, on both branches, evaluated in the first
/// statement together with the lock and the unit writes, so nothing is
/// written unless the fence passed on the row the lock holds; the
/// instant ownership moves, the write is rejected (Displaced). With a
/// `command_id`, the row must also still be targeted by that uncompleted
/// command, so the command flows to the new owner. Without one (the
/// autonomous health reconcile), the write is also fenced against ANY
/// uncompleted command for this project/node: the lifecycle handler owns
/// the status while a user action is in flight, and the EXISTS is
/// evaluated atomically with the write so a command that appeared after
/// the supervisor's tick-level gate still blocks here.
pub async fn set_status(pool: &Pool, req: &SupervisorSetStatusRequest) -> anyhow::Result<FencedWrite> {
    // What a change waits on belongs to the change the row's status is
    // in (an apply while it provisions, a stop while it stops, a
    // terminate while it terminates): a stamp that changes the status
    // clears it, so neither the next change nor the resting copy shows
    // an earlier one's. An apply's start time goes once the copy leaves
    // provisioning. `status` on the right of a SET is the row's status
    // before this write.
    let progress = |new_status: &str| {
        format!(
            ", waiting_on = CASE WHEN {new_status} = status THEN waiting_on END, \
             provisioning_since_unix = CASE WHEN {new_status} = 'provisioning' THEN provisioning_since_unix END"
        )
    };
    // The first statement's binds: `$1` the status, `$2` to `$5` the
    // project, the node, the fence's replica and the instance; then, only
    // where the statement reads them, the command, and per unit the unit,
    // the stamps its health windows run from
    // (`UnitRuntime::not_ready_since_unix`, `ready_since_unix`) and a
    // restart the health check saw first (`UnitRuntime::last_restart`:
    // when, why, the exit code), which only a write that carries one
    // changes (its time NULL leaves the unit's as it is). The second
    // statement's: `$1` to `$3` the copy, `$4`, `$5` its failure, and
    // node-wide `$6` the status (per unit, the rollup decides).
    const SAME_COPY: &str = "u.project_id = f.project_id AND u.node_id = f.node_id AND u.instance_id IS NOT DISTINCT FROM f.instance_id";
    let status = req.status.as_str();
    let instance = req.instance.as_ref().map(|m| m.as_str());
    let restart = req.restart.as_ref();
    let restart_at = restart.map(|r| r.at_unix);
    let restart_cause = restart.map(|r| r.cause.as_str());
    let restart_code = restart.and_then(|r| r.cause.exit_code());
    let mut fence_params: Vec<Param<'_>> = vec![&status, &req.project_id, &req.node_id, &req.replica, &instance];
    let (unit_fence, units, node_status) = match &req.unit {
        Some(unit) => {
            let unit = bind(&mut fence_params, unit);
            let not_ready = bind(&mut fence_params, &req.not_ready_since_unix);
            let ready = bind(&mut fence_params, &req.ready_since_unix);
            let at = bind(&mut fence_params, &restart_at);
            let cause = bind(&mut fence_params, &restart_cause);
            let code = bind(&mut fence_params, &restart_code);
            (
                format!(
                    " AND EXISTS (SELECT 1 FROM infra_unit u WHERE u.project_id = n.project_id AND u.node_id = n.node_id \
                      AND u.instance_id IS NOT DISTINCT FROM n.instance_id AND u.unit = {unit})"
                ),
                format!(
                    "UPDATE infra_unit u SET status = $1::text, not_ready_since_unix = {not_ready}::bigint, \
                     ready_since_unix = {ready}::bigint, \
                     last_restart_at_unix = CASE WHEN {at}::bigint IS NULL THEN u.last_restart_at_unix ELSE {at} END, \
                     last_restart_cause = CASE WHEN {at}::bigint IS NULL THEN u.last_restart_cause ELSE {cause}::text END, \
                     last_restart_exit_code = CASE WHEN {at}::bigint IS NULL THEN u.last_restart_exit_code ELSE {code}::bigint END \
                     FROM fenced f WHERE {SAME_COPY} AND u.unit = {unit}"
                ),
                rollup_sql(
                    "SELECT u.status FROM infra_unit u WHERE u.project_id = n.project_id AND u.node_id = n.node_id \
                     AND u.instance_id IS NOT DISTINCT FROM n.instance_id",
                ),
            )
        }
        // Node-wide: every unit's status and the node status become
        // the stamp directly. The rollup over all-equal units would be
        // the stamp too, except for a unit-less roster (a spec with only
        // shared resources), where the rollup's empty default is
        // 'stopped' and would turn a Failed / Terminating stamp into a
        // cleanly stopped node.
        None => (String::new(), format!("UPDATE infra_unit u SET status = $1::text FROM fenced f WHERE {SAME_COPY}"), "$6::text".to_string()),
    };
    let reaches = command_reaches_copy("c", "$3", "$5");
    let commands = match &req.command_id {
        Some(command_id) => format!(
            "EXISTS ( \
               SELECT 1 FROM infra_lifecycle_command c \
               WHERE c.id = {command} \
                 AND c.project_id = $2 \
                 AND {reaches} \
                 AND c.completed_at_unix IS NULL \
             )",
            command = bind(&mut fence_params, command_id),
        ),
        None => format!(
            "NOT EXISTS ( \
               SELECT 1 FROM infra_lifecycle_command c \
               WHERE c.project_id = $2 \
                 AND {reaches} \
                 AND c.completed_at_unix IS NULL \
             )"
        ),
    };
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    let fenced: i64 = tx
        .scalar(
            &format!(
                "WITH fenced AS ( \
                     SELECT n.project_id, n.node_id, n.instance_id FROM infra_node n \
                      WHERE n.project_id = $2 AND n.node_id = $3 \
                        AND n.instance_id IS NOT DISTINCT FROM $5{unit_fence} AND {commands} AND {owns} \
                      FOR UPDATE), \
                      units AS ({units}) \
                 SELECT COUNT(*) FROM fenced",
                owns = owns_project_predicate("$4", "$2"),
            ),
            &fence_params,
        )
        .await?;
    if fenced == 0 {
        drop(tx);
        drop(conn);
        return stale_answer(pool, &req.replica, req.project_id).await;
    }
    // The node row is ours until commit, so this write reaches it.
    let failure_stage = req.failure_stage.map(|s| s.as_str());
    let failure_message = req.failure_message.as_deref();
    let mut node_params: Vec<Param<'_>> = vec![&req.project_id, &req.node_id, &instance, &failure_stage, &failure_message];
    if req.unit.is_none() {
        node_params.push(&status);
    }
    tx.execute(
        &format!(
            "UPDATE infra_node n SET status = {node_status}, failure_stage = $4, failure_message = $5{progress} \
              WHERE n.project_id = $1 AND n.node_id = $2 AND n.instance_id IS NOT DISTINCT FROM $3",
            progress = progress(&node_status),
        ),
        &node_params,
    )
    .await?;
    tx.commit().await.context("commit set_status tx")?;
    Ok(FencedWrite::Applied)
}

/// Stamp a copy's roster whole, in the transaction that wrote its row:
/// its `infra_unit` rows become exactly `units`.
// SYNC: write_units <-> weft_broker_client::protocol::UnitRuntime, weft_broker_client::protocol::units_of_copy_sql, crates/weft-dispatcher/src/infra_node.rs (infra_unit, UNITS_MOVE)
pub async fn write_units(
    db: &impl Client,
    project_id: uuid::Uuid,
    node_id: &str,
    instance: Option<&weft_core::instance::InstanceId>,
    units: &std::collections::BTreeMap<String, weft_broker_client::protocol::UnitRuntime>,
) -> anyhow::Result<()> {
    let instance = instance.map(|m| m.as_str());
    db.execute(
        "DELETE FROM infra_unit WHERE project_id = $1 AND node_id = $2 AND instance_id IS NOT DISTINCT FROM $3",
        &[&project_id, &node_id, &instance],
    )
    .await?;
    if units.is_empty() {
        return Ok(());
    }
    type UnitColumns<'a> =
        (&'a str, &'a str, &'a str, i64, i64, Option<i64>, Option<i64>, Vec<&'a str>, Option<i64>, Option<&'a str>, Option<i64>);
    let rows: Vec<UnitColumns<'_>> = units
        .iter()
        .map(|(unit, runtime)| {
            let restart = runtime.last_restart.as_ref();
            (
                unit.as_str(),
                runtime.status.as_str(),
                runtime.stop_behavior.kind(),
                i64::from(runtime.flaky_after_seconds),
                i64::from(runtime.recovery_after_seconds),
                runtime.not_ready_since_unix,
                runtime.ready_since_unix,
                runtime.image_refs.iter().map(String::as_str).collect(),
                restart.map(|r| r.at_unix),
                restart.map(|r| r.cause.as_str()),
                restart.and_then(|r| r.cause.exit_code()),
            )
        })
        .collect();
    // `$1` to `$3` the copy, then each unit's own columns.
    let mut params: Vec<Param<'_>> = vec![&project_id, &node_id, &instance];
    let mut values = Vec::with_capacity(rows.len());
    for (unit, status, stop, flaky, recovery, not_ready, ready, images, restart_at, restart_cause, restart_code) in &rows {
        let columns = [
            bind(&mut params, unit),
            bind(&mut params, status),
            bind(&mut params, stop),
            bind(&mut params, flaky),
            bind(&mut params, recovery),
            bind(&mut params, not_ready),
            bind(&mut params, ready),
            bind(&mut params, images),
            bind(&mut params, restart_at),
            bind(&mut params, restart_cause),
            bind(&mut params, restart_code),
        ];
        values.push(format!("($1, $2, $3, {})", columns.join(", ")));
    }
    db.execute(
        &format!(
            "INSERT INTO infra_unit (project_id, node_id, instance_id, unit, status, stop_behavior, \
             flaky_after_seconds, recovery_after_seconds, not_ready_since_unix, ready_since_unix, image_refs, \
             last_restart_at_unix, last_restart_cause, last_restart_exit_code) VALUES {}",
            values.join(", "),
        ),
        &params,
    )
    .await?;
    Ok(())
}

/// Record what the supervisor's command `req.command_id` (an apply, a
/// stop or a terminate) waits on for its copy. The command must still be
/// an uncompleted supervisor command that reaches the copy, and `replica`
/// must still own the project, all in the UPDATE's own WHERE. The next
/// stamp that changes the row's status clears it ([`set_status`]).
pub async fn set_waiting(pool: &Pool, req: &SupervisorSetWaitingRequest) -> anyhow::Result<FencedWrite> {
    let sql = format!(
        "UPDATE infra_node SET waiting_on = $1 \
         WHERE project_id = $2 AND node_id = $3 \
           AND instance_id IS NOT DISTINCT FROM $4 AND EXISTS ( \
           SELECT 1 FROM infra_lifecycle_command c \
           WHERE c.id = $5 \
             AND c.project_id = $2 \
             AND {pending} \
             AND {reaches} \
         ) AND {owns}",
        pending = pending_supervisor_command("c"),
        reaches = command_reaches_copy("c", "$3", "$4"),
        owns = owns_project_predicate("$6", "$2"),
    );
    let written = pool
        .execute(
            &sql,
            &[&req.waiting, &req.project_id, &req.node_id, &req.instance.as_ref().map(|m| m.as_str()), &req.command_id, &req.replica],
        )
        .await?;
    if written > 0 {
        return Ok(FencedWrite::Applied);
    }
    stale_answer(pool, &req.replica, req.project_id).await
}

/// Stamp a lifecycle command terminal: success (`error = None`),
/// failure (`error = Some`), or a user-requested cancellation the
/// supervisor honored mid-command (`cancelled`; `error` then carries
/// the halt point as the outcome message, never counted as a failure).
/// Only the process that currently OWNS the project may complete it: a
/// supervisor that lost ownership mid-command must NOT, because leaving
/// the command uncompleted is exactly what lets the new owner re-run
/// and finish it. Combined with `completed_at_unix IS NULL` this is
/// "exactly the current owner, exactly once": a second completion, or
/// one for a command deleted with its project, is Gone. An ending written
/// clears the project's commands [`claim_next`] gave up from `weft status`:
/// endings are being written again. An apply that succeeded stamps its
/// copy with the place hash it was issued with, in the same statement:
/// what tells the copy behind the source later. A NUL byte in the error, which a
/// Postgres text column refuses, is dropped, so the ending is written.
pub async fn complete_command(
    pool: &Pool,
    req: &SupervisorCommandCompleteRequest,
) -> anyhow::Result<FencedWrite> {
    let outcome = if req.cancelled {
        LifecycleOutcome::Cancelled
    } else {
        match req.error {
            Some(_) => LifecycleOutcome::Failed,
            None => LifecycleOutcome::Succeeded,
        }
    };
    let sql = format!(
        "WITH done AS ( \
             UPDATE infra_lifecycle_command \
             SET completed_at_unix = EXTRACT(EPOCH FROM NOW())::BIGINT, \
                 outcome = $1, \
                 outcome_message = $2 \
             WHERE id = $3 \
               AND completed_at_unix IS NULL \
               AND {owns} \
             RETURNING project_id, node_id, instance_id, verb, source_hash \
         ), cleared AS ( \
             DELETE FROM unanswered_call WHERE callee IN (SELECT {key} FROM done) \
         ), stamped AS ( \
             UPDATE infra_node n SET source_hash = d.source_hash FROM done d \
             WHERE $1 = 'succeeded' AND d.verb = 'apply' AND d.source_hash IS NOT NULL \
               AND n.project_id = d.project_id AND n.node_id = d.node_id \
               AND n.instance_id IS NOT DISTINCT FROM d.instance_id \
         ) \
         SELECT COUNT(*) FROM done",
        owns = owns_project_predicate("$4", "project_id"),
        key = weft_task_store::unanswered::commands_key_sql("project_id"),
    );
    let written: i64 = pool.scalar(&sql, &[&outcome.as_str(), &req.error.as_deref(), &req.command_id, &req.replica]).await?;
    if written > 0 {
        return Ok(FencedWrite::Applied);
    }
    // The project id lives on the command row, so the ownership answer
    // comes from there; no command row at all is a gone target.
    let project: Option<uuid::Uuid> =
        pool.scalar_opt("SELECT project_id FROM infra_lifecycle_command WHERE id = $1", &[&req.command_id]).await?;
    match project {
        Some(project_id) => stale_answer(pool, &req.replica, project_id).await,
        None => Ok(FencedWrite::Gone),
    }
}
