//! The `image_build` ledger: one row per image ref, naming the build
//! running for it and how it ended.
//!
//! The build runs on the platform's builder (a `docker build` on a local
//! install, a Cloud Build on GCP). The row is how everybody finds it: a
//! second verb needing the same image joins the running build instead of
//! starting another, and the version build waiting on it reads its end
//! here (`super::waiting`, `image_states`).
//!
//! The row is the truth about a build, and anyone may move it forward:
//! the dispatcher's build loop (`super::follow`) asks the builder how
//! each running build is doing and records how it ended, on whichever
//! dispatcher wakes next. No process has to stay up for a build to be
//! seen through, which a dispatcher that scales to zero could not.

use anyhow::{Context, Result};
use sqlx::PgPool;

/// What a claim writes into `image_build.held_until`, for a dispatcher of
/// an older release still up beside this one (a rollout): it ends a start
/// not made on the builder by then. Nothing here reads the column any
/// more: a start whose starter went away is noticed from its project's
/// build heartbeat instead (`end_lost_starts`), within a minute. The column
/// is dropped in a release after this one, once no older reader is left.
const OLDER_READERS_START_HOLD_SECS: i64 = 1800;

/// How long the builder may keep failing to answer about a build before
/// it is given up on, ending failed with the last answer. An internal wait
/// (the builder's API), never a build's own length.
pub const UNANSWERED_GIVE_UP_SECS: i64 = 600;

/// How long a build request's claim on its images lasts without renewal
/// (`super::prune::ImageHold`).
pub const CLAIM_LEASE_SECS: i64 = 60;

pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "image_build",
    tables: &["image_build", "image_use", "image_claim", "version_build"],
    ddl: &[
        r#"CREATE TABLE IF NOT EXISTS image_build (
            -- The content-addressed ref the build pushes: one build per
            -- content, whichever project asked for it.
            image_ref TEXT PRIMARY KEY,
            project_id UUID NOT NULL,
            tenant_id TEXT NOT NULL,
            -- The build's name, minted before it starts.
            build_name TEXT NOT NULL,
            -- Its id on the image builder, which the builder may choose
            -- (Cloud Build does): NULL until the start recorded it, and
            -- nobody asks the builder about a build without one.
            builder_id TEXT,
            -- The compile lane a worker build holds (see
            -- weft_compiler::worker_image::COMPILE_LANE_ARG); NULL for an
            -- image that compiles nothing.
            lane INTEGER,
            -- SYNC: the statuses <-> weft_core::projects::BuildState
            status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed', 'cancelled')),
            reason TEXT,
            -- Read only by dispatchers of an older release (see
            -- OLDER_READERS_START_HOLD_SECS); dropped in a later release.
            held_until BIGINT NOT NULL,
            started_at BIGINT NOT NULL,
            finished_at BIGINT,
            -- Since when the builder has failed to answer about it, every
            -- look since; NULL while it answers.
            failing_since BIGINT,
            -- Where a person reads the build's log, when the builder
            -- keeps one at an address.
            log_url TEXT
        )"#,
        // One row per image a project's running version names, and when
        // it last became part of that version (`note_running`). What
        // "a project's current and previous build" means for reclaiming
        // (`super::prune::older_builds`): the builder may have built the
        // image for another project, or long ago, and a build that finds
        // it already there builds nothing, so `image_build` cannot say.
        r#"CREATE TABLE IF NOT EXISTS image_use (
            project_id UUID NOT NULL,
            image_ref TEXT NOT NULL,
            running_since BIGINT NOT NULL,
            PRIMARY KEY (project_id, image_ref)
        )"#,
        // One row per image a build in progress relies on before its
        // project's running version names it (`claim_images`): every image
        // it found in the registry or is building. A prune spares every
        // ref with a live claim (`claim_live`). A claim lives as long as
        // the build request holding it renews it (`super::prune::ImageHold`),
        // and, once the request answered while its builds ran, as long as
        // the version build it became waits (`super::waiting`: the claim's
        // id is that version's). One nobody keeps stops protecting anything.
        r#"CREATE TABLE IF NOT EXISTS image_claim (
            image_ref TEXT NOT NULL,
            claim_id UUID NOT NULL,
            -- The project whose build request holds the claim: what its
            -- status lists while the build runs, whoever started it. Every
            -- claim names one (`claim_images`).
            project_id UUID,
            holder_until BIGINT NOT NULL,
            PRIMARY KEY (image_ref, claim_id)
        )"#,
        super::waiting::VERSION_BUILD_TABLE,
        super::waiting::ONE_WAITING_PER_PROJECT,
    ],
    seed: &[],
};

/// What a dispatcher does about one image.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Claim {
    /// Nothing is building it: start `name`, in `lane` (a worker build;
    /// none for an image that compiles nothing). The row is already
    /// written, so a sibling sees the build before it starts.
    Start { name: String, lane: Option<u32> },
    /// A build is running for it, minted as `name`: the project waits on
    /// that one.
    /// `ours` when this same project started it, so a cancel of this
    /// project stops it (a build another project started is left to that
    /// project, and this one only stops waiting).
    Join { name: String, ours: bool },
    /// A build of it already succeeded and the registry still holds the
    /// image, so there is nothing to build. A verb that found the image
    /// missing a moment before, while another verb's build was still
    /// running, lands here instead of building it again.
    Built,
}

impl Claim {
    /// Whether a cancel of this project may stop the build.
    pub fn stoppable(&self) -> bool {
        match self {
            Claim::Start { .. } => true,
            Claim::Join { ours, .. } => *ours,
            Claim::Built => false,
        }
    }
}

/// Decide, for one image, whether to start a build or join the running
/// one, and record the decision, in one transaction serialized per image.
/// Refused ([`super::BuildCancelled`]) once the project's build was
/// cancelled, so an image claimed after a cancel never starts: the claim
/// either commits before the cancel, which then finds the build and
/// stops it, or sees it.
///
/// A row that says the build succeeded is believed only while `images`
/// still holds the image: a prune forgets the row when it deletes one,
/// but an image deleted by hand or by the registry's own cleanup policy
/// leaves the row behind, and trusting it would skip that image's build
/// for ever. Such a row is overwritten by a new build, under the lock.
/// The registry is asked before the lock is taken, so a slow registry
/// never holds up the other verbs and prunes of the image; a build that
/// succeeded only after that (a row that turned succeeded, or another
/// build's name on it) was just built, so its image is there.
pub async fn claim(
    pool: &PgPool,
    images: &dyn weft_platform_traits::ImageBuilder,
    image_ref: &str,
    project_id: uuid::Uuid,
    tenant: &str,
    lanes: Option<u32>,
    now: i64,
) -> Result<Claim> {
    let built_before: Option<(String, String)> = sqlx::query_as("SELECT status, build_name FROM image_build WHERE image_ref = $1")
        .bind(image_ref)
        .fetch_optional(pool)
        .await
        .context("read how the image's build stands")?;
    // The build the registry was asked about, and whether it still holds
    // that build's image.
    let asked = match built_before {
        Some((status, name)) if status == "succeeded" => Some((
            name,
            images
                .image_exists(image_ref)
                .await
                .with_context(|| format!("ask the registry whether the built image {image_ref} is still there"))?,
        )),
        _ => None,
    };
    let mut tx = pool.begin().await.context("begin the build claim")?;
    lock_image(&mut tx, image_ref).await?;
    refuse_once_cancelled(&mut tx, project_id).await?;
    let row: Option<(String, uuid::Uuid, String)> =
        sqlx::query_as("SELECT status, project_id, build_name FROM image_build WHERE image_ref = $1")
            .bind(image_ref)
            .fetch_optional(&mut *tx)
            .await?;
    let claim = match row {
        Some((status, _, name)) if status == "succeeded" => {
            let there = match &asked {
                Some((asked_about, there)) if *asked_about == name => *there,
                _ => true,
            };
            if !there {
                tracing::warn!(
                    target: "weft_dispatcher::build",
                    image = image_ref,
                    "the ledger records this image as built but the registry no longer holds it (deleted by hand or by a \
                     cleanup policy); building it again"
                );
            }
            there.then_some(Claim::Built)
        }
        Some((status, builds_for, name)) if status == "running" => Some(Claim::Join { name, ours: builds_for == project_id }),
        _ => None,
    };
    if let Some(claim) = claim {
        tx.commit().await.context("commit the build claim")?;
        return Ok(claim);
    }
    let lane = match lanes {
        Some(lanes) => Some(pick_lane(&mut tx, lanes).await?),
        None => None,
    };
    let name = build_name();
    // Announced in the same statement (delivered when it commits), so the
    // build loop watches the build from its first moment: one whose
    // starter goes away before it is made on the builder is ended once
    // its project's heartbeat goes stale (`end_lost_starts`), even on an
    // install that was asleep.
    sqlx::query(
        "INSERT INTO image_build (image_ref, project_id, tenant_id, build_name, builder_id, lane, status, reason, \
                                  held_until, started_at, finished_at, log_url, failing_since) \
         VALUES ($1, $2, $3, $4, NULL, $5, 'running', NULL, $6, $7, NULL, NULL, NULL) \
         ON CONFLICT (image_ref) DO UPDATE SET project_id = EXCLUDED.project_id, tenant_id = EXCLUDED.tenant_id, \
             build_name = EXCLUDED.build_name, builder_id = NULL, lane = EXCLUDED.lane, status = 'running', \
             reason = NULL, held_until = EXCLUDED.held_until, started_at = EXCLUDED.started_at, \
             finished_at = NULL, log_url = NULL, failing_since = NULL \
         RETURNING pg_notify($8, image_ref)",
    )
    .bind(image_ref)
    .bind(project_id)
    .bind(tenant)
    .bind(&name)
    .bind(lane.map(|l| l as i32))
    .bind(now + OLDER_READERS_START_HOLD_SECS)
    .bind(now)
    .bind(super::follow::BUILD_CLAIMED_CHANNEL)
    .execute(&mut *tx)
    .await
    .context("record the build")?;
    tx.commit().await.context("commit the build claim")?;
    Ok(Claim::Start { name, lane })
}

/// Refuse, inside `tx`, once `project`'s build was cancelled (its
/// transition is `cancelling_build`). The project's row is read under a
/// share lock, so a cancel landing meanwhile waits for `tx` to commit and
/// then finds what it wrote.
pub(crate) async fn refuse_once_cancelled(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, project: uuid::Uuid) -> Result<()> {
    let transition: Option<String> = sqlx::query_scalar("SELECT transition FROM project WHERE id = $1 FOR SHARE")
        .bind(project)
        .fetch_optional(&mut **tx)
        .await
        .context("read whether the project's build was cancelled")?;
    if transition.as_deref() == Some(weft_core::projects::ProjectTransition::CancellingBuild.as_str()) {
        return Err(super::BuildCancelled.into());
    }
    Ok(())
}

/// The build `name` was made on the builder as `handle` (whose id the
/// builder may have chosen): the row records that id, so anyone reading
/// it asks the builder about the right build. `false` when the row is no
/// longer this running build's (it was cancelled, or given up on, while
/// it was being made): nobody will ask about it, so the caller frees it.
pub async fn started(pool: &PgPool, image_ref: &str, name: &str, handle: &weft_platform_traits::BuildHandle) -> Result<bool> {
    // The build loop already watches it, woken by its claim.
    let recorded = sqlx::query(
        "UPDATE image_build SET builder_id = $3, log_url = $4 \
         WHERE image_ref = $1 AND build_name = $2 AND status = 'running' AND builder_id IS NULL \
         RETURNING 1",
    )
    .bind(image_ref)
    .bind(name)
    .bind(&handle.external_build_id)
    .bind(&handle.log_url)
    .fetch_optional(pool)
    .await
    .context("record the build's id on its builder")?;
    Ok(recorded.is_some())
}

/// The builder failed to answer about the build `name` at `now`: since
/// when it has, every look since.
pub async fn unanswered(pool: &PgPool, image_ref: &str, name: &str, now: i64) -> Result<i64> {
    sqlx::query_scalar(
        "UPDATE image_build SET failing_since = COALESCE(failing_since, $3) \
         WHERE image_ref = $1 AND build_name = $2 AND status = 'running' RETURNING failing_since",
    )
    .bind(image_ref)
    .bind(name)
    .bind(now)
    .fetch_optional(pool)
    .await
    .context("record that the builder did not answer")
    .map(|since| since.unwrap_or(now))
}

/// The builder answered about the build `name`.
pub async fn answered(pool: &PgPool, image_ref: &str, name: &str) -> Result<()> {
    sqlx::query("UPDATE image_build SET failing_since = NULL WHERE image_ref = $1 AND build_name = $2 AND failing_since IS NOT NULL")
        .bind(image_ref)
        .bind(name)
        .execute(pool)
        .await
        .context("record that the builder answered")?;
    Ok(())
}

/// Serialize every decision about `image_ref`'s row, for the transaction.
async fn lock_image(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, image_ref: &str) -> Result<()> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('image_build:' || $1, 0))")
        .bind(image_ref)
        .execute(&mut **tx)
        .await
        .context("lock the image's build row")?;
    Ok(())
}

/// The least busy lane: builds in different lanes compile side by side.
async fn pick_lane(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, lanes: u32) -> Result<u32> {
    let busy: Vec<(i32, i64)> =
        sqlx::query_as("SELECT lane, COUNT(*) FROM image_build WHERE status = 'running' AND lane IS NOT NULL GROUP BY lane")
            .fetch_all(&mut **tx)
            .await
            .context("count the busy lanes")?;
    Ok(least_busy_lane(lanes, &busy))
}

/// The lane with the fewest running builds, the lowest on a tie.
pub fn least_busy_lane(lanes: u32, busy: &[(i32, i64)]) -> u32 {
    (0..lanes.max(1))
        .min_by_key(|lane| busy.iter().find(|(l, _)| *l as u32 == *lane).map(|(_, n)| *n).unwrap_or(0))
        .unwrap_or(0)
}

/// A build's name: DNS-clean, unique, and recognizable.
fn build_name() -> String {
    format!("weft-build-{}", &uuid::Uuid::new_v4().simple().to_string()[..20])
}

/// How a build ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Succeeded,
    Failed(String),
    Cancelled,
}

/// Record how the build `name` ended. Only that build's row moves: a
/// later build of the same ref, already started, is never overwritten.
/// `false` when its end was recorded already (by whoever looked first),
/// so only one of them frees it.
pub async fn finish(pool: &PgPool, image_ref: &str, name: &str, outcome: Outcome, now: i64) -> Result<bool> {
    let (status, reason) = match outcome {
        Outcome::Succeeded => ("succeeded", None),
        Outcome::Failed(reason) => ("failed", Some(reason)),
        Outcome::Cancelled => ("cancelled", None),
    };
    let res = sqlx::query(
        "UPDATE image_build SET status = $3, reason = $4, finished_at = $5 \
         WHERE image_ref = $1 AND build_name = $2 AND status = 'running'",
    )
    .bind(image_ref)
    .bind(name)
    .bind(status)
    .bind(reason)
    .bind(now)
    .execute(pool)
    .await
    .context("record how the build ended")?;
    Ok(res.rows_affected() == 1)
}

/// Record the build `name` of `image_ref` as cancelled, and answer its id
/// on the builder (to free it). `None` when it no longer runs (it ended,
/// and the row may run another build since); `Some(None)` when it was not
/// made on the builder yet, so its starter, finding the row no longer its
/// build's (`started`), frees it.
pub async fn cancel(pool: &PgPool, image_ref: &str, name: &str, now: i64) -> Result<Option<Option<String>>> {
    sqlx::query_scalar(
        "UPDATE image_build SET status = 'cancelled', reason = NULL, finished_at = $3 \
         WHERE image_ref = $1 AND build_name = $2 AND status = 'running' RETURNING builder_id",
    )
    .bind(image_ref)
    .bind(name)
    .bind(now)
    .fetch_optional(pool)
    .await
    .context("record the build as cancelled")
}

/// One build the ledger says runs.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct RunningBuild {
    pub image_ref: String,
    pub project_id: uuid::Uuid,
    /// The name it was minted under: what `finish` names.
    pub build_name: String,
    /// Its id on the builder, once its start recorded it (`started`).
    pub builder_id: Option<String>,
    pub started_at: i64,
    pub log_url: Option<String>,
}

const RUNNING_COLUMNS: &str = "image_ref, project_id, build_name, builder_id, started_at, log_url";

/// Which starts [`end_lost_starts`] ends: those still waiting to be made
/// on the builder whose starter went away.
#[derive(Debug, Clone, Copy)]
pub enum LostStarts {
    /// Every such start whose project no request drives any more
    /// (`crate::project_store::build_driven`, against this cutoff): the
    /// build loop's sweep. The starter's heartbeat beats from before its
    /// first claim until its last start is recorded, on whichever
    /// dispatcher it runs, so a live start is never ended.
    Undriven { stale_before: i64 },
    /// Every such start of this project: a request just took its build
    /// transition over from one whose heartbeat went stale
    /// (`crate::project_store::ProjectStoreOps::try_begin_building`), so
    /// none of them is driven.
    Of(uuid::Uuid),
}

/// End, failed, every build still waiting to be made on the builder whose
/// starter went away between the claim and recording the builder's id:
/// nothing will ever make it. Answers how many it ended.
pub async fn end_lost_starts(conn: &mut sqlx::PgConnection, lost: LostStarts, now: i64) -> Result<u64> {
    let (project, stale_before) = match lost {
        LostStarts::Undriven { stale_before } => (None, stale_before),
        LostStarts::Of(project) => (Some(project), 0),
    };
    let ended = sqlx::query(&format!(
        "UPDATE image_build b SET status = 'failed', finished_at = $1, \
             reason = 'the build ' || b.build_name || ' was never made on the builder: the dispatcher starting it went \
                       away before recording it there. Build again' \
         WHERE b.status = 'running' AND b.builder_id IS NULL \
           AND CASE WHEN $2::UUID IS NULL \
                    THEN NOT EXISTS (SELECT 1 FROM project p WHERE p.id = b.project_id AND {}) \
                    ELSE b.project_id = $2 END",
        crate::project_store::build_driven("p", "$3")
    ))
    .bind(now)
    .bind(project)
    .bind(stale_before)
    .execute(conn)
    .await
    .context("end the starts whose starter went away")?;
    Ok(ended.rows_affected())
}

/// The SQL condition "the claim row `c` still protects its image": its
/// request renews it (`holder_until` not past `now`), or the version build
/// its id names still waits (`super::waiting`), whatever its lease says.
/// One definition for every reader of a claim: a prune deciding about an
/// image, the sweep of lapsed claims, and which builds a project waits on.
pub(crate) fn claim_live(c: &str, now: &str) -> String {
    format!(
        "({c}.holder_until >= {now} OR EXISTS (SELECT 1 FROM version_build v WHERE v.id = {c}.claim_id AND v.state = '{}'))",
        super::waiting::WAITING
    )
}

/// The SQL condition "the project bound at `project` waits on the build
/// row `b`": it started the build, or a live claim of its names the image
/// (a build another project started of the same content). What the
/// project's status lists as its builds.
pub(crate) fn waited_on_by(project: &str, now: &str) -> String {
    format!(
        "(b.project_id = {project} OR EXISTS (SELECT 1 FROM image_claim c WHERE c.image_ref = b.image_ref \
             AND c.project_id = {project} AND {}))",
        claim_live("c", now)
    )
}

/// Where the build of each of `images` stands: the one the ledger holds for
/// its image now, under the name it was minted with, which is a newer
/// build than the one named when the image's build was started again
/// since. An image with no row (forgotten) has no state.
pub async fn image_states(
    pool: &PgPool,
    images: &[weft_core::builds::ImageBuild],
) -> Result<Vec<weft_core::builds::ImageBuildState>> {
    #[derive(sqlx::FromRow)]
    struct Row {
        image_ref: String,
        build_name: String,
        status: String,
        reason: Option<String>,
        builder_id: Option<String>,
        started_at: i64,
        log_url: Option<String>,
    }
    let refs: Vec<&str> = images.iter().map(|image| image.image.as_str()).collect();
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT image_ref, build_name, status, reason, builder_id, started_at, log_url FROM image_build \
         WHERE image_ref = ANY($1)",
    )
    .bind(&refs)
    .fetch_all(pool)
    .await
    .context("read where the version's image builds are")?;
    let mut out = Vec::with_capacity(images.len());
    for image in images {
        let state = match rows.iter().find(|row| row.image_ref == image.image) {
            None => weft_core::builds::ImageBuildState { build: image.clone(), state: Default::default() },
            Some(row) => weft_core::builds::ImageBuildState {
                build: weft_core::builds::ImageBuild { image: image.image.clone(), name: row.build_name.clone() },
                state: weft_core::projects::BuildStateResponse {
                    state: Some(
                        serde_json::from_value(serde_json::Value::String(row.status.clone()))
                            .context("a build state the ledger does not know")?,
                    ),
                    reason: row.reason.clone(),
                    build: row.builder_id.clone(),
                    started_at_unix: Some(row.started_at),
                    log_url: row.log_url.clone(),
                },
            },
        };
        out.push(state);
    }
    Ok(out)
}

/// Every build running, oldest first; when `project` is given, the ones it
/// waits on ([`waited_on_by`]).
pub async fn running(pool: &PgPool, project: Option<uuid::Uuid>) -> Result<Vec<RunningBuild>> {
    sqlx::query_as(&format!(
        "SELECT {RUNNING_COLUMNS} FROM image_build b WHERE status = 'running' AND ($1::UUID IS NULL OR {}) \
         ORDER BY started_at, image_ref",
        waited_on_by("$1", "$2")
    ))
    .bind(project)
    .bind(crate::lease::now_unix())
    .fetch_all(pool)
    .await
    .context("list the running builds")
}

/// Every image whose build ended, however it ended; one project's when
/// `project` is given: the images it built or ran (`image_use`).
/// Everything a prune may consider, before the keep-set and the other
/// projects' recent builds spare their part. A build that failed or was
/// cancelled may still have pushed its image (a step after the push
/// failed, a cancel that came late), so its row counts too; deleting an
/// image that was never pushed deletes nothing, and the row is forgotten.
pub async fn built_images(pool: &PgPool, project: Option<uuid::Uuid>) -> Result<Vec<String>> {
    sqlx::query_scalar(
        "SELECT b.image_ref FROM image_build b \
         WHERE b.status <> 'running' AND ($1::UUID IS NULL OR b.project_id = $1 \
             OR EXISTS (SELECT 1 FROM image_use u WHERE u.image_ref = b.image_ref AND u.project_id = $1)) \
         ORDER BY b.image_ref",
    )
    .bind(project)
    .fetch_all(pool)
    .await
    .context("list the built images")
}

/// Every image whose build ended (as `built_images` counts them) that no
/// project's version names (no `image_use` row): a removed project's, one
/// whose build never registered or failed, the standard worker
/// (`note_shared`). An image a build in progress relies on is claimed
/// instead (`claim_images`).
pub async fn unused_images(pool: &PgPool) -> Result<Vec<String>> {
    sqlx::query_scalar(
        "SELECT b.image_ref FROM image_build b \
         WHERE b.status <> 'running' AND NOT EXISTS (SELECT 1 FROM image_use u WHERE u.image_ref = b.image_ref) \
         ORDER BY b.image_ref",
    )
    .fetch_all(pool)
    .await
    .context("list the images no project uses")
}

/// Record an image the install did not build itself but keeps in its
/// registry: the standard worker the install's image step made ready
/// (`weft_compiler::build::standard_worker_hash`), under the nil project.
/// Recorded, it is reclaimed like any image once a newer weft names
/// another standard worker. Recording it twice changes nothing.
pub async fn note_shared(pool: &PgPool, image_ref: &str, now: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO image_build (image_ref, project_id, tenant_id, build_name, lane, status, held_until, started_at, finished_at) \
         VALUES ($1, $2, '', 'shared', NULL, 'succeeded', 0, $3, $3) ON CONFLICT (image_ref) DO NOTHING",
    )
    .bind(image_ref)
    .bind(uuid::Uuid::nil())
    .bind(now)
    .execute(pool)
    .await
    .context("record a shared image")?;
    Ok(())
}

/// Forget an image deleted from the registry, and every project's use of
/// it, inside the prune's transaction that holds the image's lock.
pub async fn forget(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, image_ref: &str) -> Result<()> {
    sqlx::query("DELETE FROM image_build WHERE image_ref = $1 AND status <> 'running'")
        .bind(image_ref)
        .execute(&mut **tx)
        .await
        .context("forget a deleted image")?;
    sqlx::query("DELETE FROM image_use WHERE image_ref = $1")
        .bind(image_ref)
        .execute(&mut **tx)
        .await
        .context("forget the uses of a deleted image")?;
    Ok(())
}

/// What a prune may do with one image, decided under its lock.
pub enum Reclaim {
    /// Nothing claims or references it: delete it, then `forget` it inside
    /// this transaction, which still holds the lock.
    Free(sqlx::Transaction<'static, sqlx::Postgres>),
    /// A build in progress claims it, or the ledger has a build of it
    /// running (a failed row a new build took over since the prune
    /// listed it).
    Claimed,
    /// Something references it now (`super::prune::keep_set`), though the
    /// prune's keep-set, read before, did not.
    Referenced,
}

/// Decide about one image a prune may delete, holding the image's lock
/// (the one every build claim takes too) in a transaction: whether a build
/// in progress still claims it, and whether anything references it right
/// now. The prune deletes the image and `forget`s it inside this
/// transaction, so no build can claim the image between this check and
/// its deletion: a build that claims it afterwards finds it gone and
/// builds it again.
///
/// Both checks are needed because a build lets go of its claim only once
/// its registration commits, or in the same transaction
/// (`super::waiting::register_version`): a reader
/// holding the lock sees either the live claim or the committed reference,
/// never neither. The prune's own keep-set was read before its loop and
/// can predate that registration, so it cannot stand in for the reference
/// check here.
pub async fn begin_reclaim(pool: &PgPool, image_ref: &str, now: i64) -> Result<Reclaim> {
    let scope = super::prune::ImageScope::of(image_ref)
        .with_context(|| format!("{image_ref} is not an image a prune may reclaim"))?;
    let mut tx = pool.begin().await.context("begin reclaiming an image")?;
    lock_image(&mut tx, image_ref).await?;
    let claimed: bool = sqlx::query_scalar(&format!(
        "SELECT EXISTS (SELECT 1 FROM image_claim c WHERE c.image_ref = $1 AND {}) \
             OR EXISTS (SELECT 1 FROM image_build WHERE image_ref = $1 AND status = 'running')",
        claim_live("c", "$2")
    ))
    .bind(image_ref)
    .bind(now)
    .fetch_one(&mut *tx)
    .await
    .context("read whether a build claims the image")?;
    if claimed {
        return Ok(Reclaim::Claimed);
    }
    if super::prune::keep_set(&mut tx, scope).await?.references(scope) {
        return Ok(Reclaim::Referenced);
    }
    Ok(Reclaim::Free(tx))
}

/// Claim `images` for the build request holding `claim`, before it looks
/// for any of them in the registry. Taken under each image's lock, in one
/// order, so it serializes with a prune deciding about the same image.
pub async fn claim_images(pool: &PgPool, claim: uuid::Uuid, project: uuid::Uuid, images: &[String], now: i64) -> Result<()> {
    let sorted: std::collections::BTreeSet<&String> = images.iter().collect();
    let mut tx = pool.begin().await.context("begin claiming the build's images")?;
    for image_ref in &sorted {
        lock_image(&mut tx, image_ref).await?;
    }
    let refs: Vec<&String> = sorted.into_iter().collect();
    sqlx::query(
        "INSERT INTO image_claim (image_ref, claim_id, project_id, holder_until) \
         SELECT image_ref, $2, $4, $3 FROM UNNEST($1::TEXT[]) AS image_ref \
         ON CONFLICT (image_ref, claim_id) DO UPDATE SET holder_until = EXCLUDED.holder_until",
    )
    .bind(&refs)
    .bind(claim)
    .bind(now + CLAIM_LEASE_SECS)
    .bind(project)
    .execute(&mut *tx)
    .await
    .context("claim the build's images")?;
    tx.commit().await.context("commit claiming the build's images")?;
    Ok(())
}

/// Extend every live row of `claim`, and answer how many it still holds.
/// A row that lapsed stays lapsed: a prune may already have taken its
/// image, so only the holder's own check (`ImageHold::confirm`) decides.
pub async fn renew_claim(pool: &PgPool, claim: uuid::Uuid, now: i64) -> Result<u64> {
    let res = sqlx::query("UPDATE image_claim SET holder_until = $3 WHERE claim_id = $1 AND holder_until >= $2")
        .bind(claim)
        .bind(now)
        .bind(now + CLAIM_LEASE_SECS)
        .execute(pool)
        .await
        .context("renew the build's claim on its images")?;
    Ok(res.rows_affected())
}

/// Let go of `claim`: its build registered its images, or ended.
pub async fn drop_claim(pool: &PgPool, claim: uuid::Uuid) -> Result<()> {
    sqlx::query("DELETE FROM image_claim WHERE claim_id = $1")
        .bind(claim)
        .execute(pool)
        .await
        .context("let go of the build's claim on its images")?;
    Ok(())
}

/// Clear the claims no longer live ([`claim_live`]: their holder stopped
/// renewing them, its dispatcher gone, and no waiting version keeps them):
/// they protect nothing any more.
pub async fn drop_lapsed_claims(pool: &PgPool, now: i64) -> Result<()> {
    sqlx::query(&format!("DELETE FROM image_claim c WHERE NOT {}", claim_live("c", "$1")))
        .bind(now)
        .execute(pool)
        .await
        .context("clear the lapsed image claims")?;
    Ok(())
}

/// Record that `images` are the project's running version as of `now`:
/// each one's `running_since` moves to `now`, whether this build built it,
/// found it built by another project, or found it from an earlier build
/// of its own.
pub async fn note_running(pool: &PgPool, project_id: uuid::Uuid, images: &[String], now: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO image_use (project_id, image_ref, running_since) \
         SELECT $1, image_ref, $3 FROM UNNEST($2::TEXT[]) AS image_ref \
         ON CONFLICT (project_id, image_ref) DO UPDATE SET running_since = EXCLUDED.running_since",
    )
    .bind(project_id)
    .bind(images)
    .bind(now)
    .execute(pool)
    .await
    .context("record the images the project runs")?;
    Ok(())
}

/// Forget a removed project's uses: its images are then only as recent as
/// the other projects that use them say.
pub async fn forget_project(pool: &PgPool, project_id: uuid::Uuid) -> Result<()> {
    sqlx::query("DELETE FROM image_use WHERE project_id = $1")
        .bind(project_id)
        .execute(pool)
        .await
        .context("forget the removed project's images")?;
    Ok(())
}

/// One project's use of one image: the project's version that runs it
/// became current at `running_since` (`note_running` stamps every image
/// of one version with the same instant, so the instant names the build).
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct ImageUse {
    pub project_id: uuid::Uuid,
    pub image_ref: String,
    pub running_since: i64,
}

/// Every project's use of every image whose build ended (as
/// `built_images` counts them).
pub async fn image_uses(pool: &PgPool) -> Result<Vec<ImageUse>> {
    sqlx::query_as(
        "SELECT u.project_id, u.image_ref, u.running_since FROM image_use u \
         JOIN image_build b ON b.image_ref = u.image_ref AND b.status <> 'running'",
    )
    .fetch_all(pool)
    .await
    .context("list the images each project runs")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_least_busy_lane_is_picked_lowest_first() {
        assert_eq!(least_busy_lane(4, &[]), 0);
        assert_eq!(least_busy_lane(4, &[(0, 1)]), 1);
        assert_eq!(least_busy_lane(4, &[(0, 1), (1, 1), (2, 1), (3, 1)]), 0);
        assert_eq!(least_busy_lane(2, &[(0, 3), (1, 1)]), 1);
        assert_eq!(least_busy_lane(0, &[]), 0, "no lanes configured is one lane");
    }

    #[test]
    fn a_build_name_is_a_dns_label() {
        let name = build_name();
        assert!(name.len() <= 63 && name.starts_with("weft-build-"));
        assert!(name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'));
        assert_ne!(name, build_name());
    }
}
