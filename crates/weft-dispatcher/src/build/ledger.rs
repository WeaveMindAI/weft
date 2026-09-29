//! The `image_build` ledger: one row per image ref, naming the build
//! running for it, the dispatcher driving it, and how it ended.
//!
//! The build runs on the platform's builder (a `docker build` on a local
//! install, a Cloud Build on GCP), driven by one dispatcher. The row is
//! how everybody else finds it: a second verb needing the same image joins
//! the running build instead of starting another, and a sibling dispatcher
//! adopts a build whose driver stopped renewing its lease and sees it
//! through.
//!
//! A taken-over build is always built again, under a new name in the same
//! row (`take_over`): the new driver cannot tell how far the old one's
//! build got, or whether it is still running at all. Rebuilding costs
//! little, since the compile cache of its lane outlives any build, and it
//! makes a take-over always end the same way.
//!
//! A waiter follows the row, not one name: the build running for an image
//! may change name under it (a take-over), and whatever ends there is the
//! answer, since every build of one ref pushes the same content.

use anyhow::{Context, Result};
use sqlx::PgPool;

/// How long a driver's hold on a build lasts without renewal. Renewed on
/// every poll, so only a driver that is gone lets it lapse.
pub const DRIVER_LEASE_SECS: i64 = 60;

pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "image_build",
    tables: &["image_build", "image_use", "image_claim"],
    ddl: &[
        r#"CREATE TABLE IF NOT EXISTS image_build (
            -- The content-addressed ref the build pushes: one build per
            -- content, whichever project asked for it.
            image_ref TEXT PRIMARY KEY,
            project_id UUID NOT NULL,
            tenant_id TEXT NOT NULL,
            -- The build's name on the image builder, minted before it starts.
            build_name TEXT NOT NULL,
            -- The compile lane a worker build holds (see
            -- weft_compiler::worker_image::COMPILE_LANE_ARG).
            lane INTEGER NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed', 'cancelled')),
            reason TEXT,
            -- The dispatcher instance driving the build, and until when its
            -- hold lasts without renewal.
            driver_instance TEXT NOT NULL,
            driver_until BIGINT NOT NULL,
            started_at BIGINT NOT NULL,
            finished_at BIGINT
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
        // ref with a live claim. A claim lives as long as the build request
        // holding it renews it (`super::prune::ImageHold`), so one whose
        // dispatcher is gone stops protecting anything.
        r#"CREATE TABLE IF NOT EXISTS image_claim (
            image_ref TEXT NOT NULL,
            claim_id UUID NOT NULL,
            holder_until BIGINT NOT NULL,
            PRIMARY KEY (image_ref, claim_id)
        )"#,
    ],
    seed: &[],
};

/// What a dispatcher does about one image.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Claim {
    /// Nothing is building it: start `name`, in `lane`. The row is
    /// already written, so a sibling sees the build before it starts.
    Start { name: String, lane: u32 },
    /// A build is running for it: poll `name`. `driving` when this process
    /// holds the build (a second verb on this process asking for the same
    /// image); `ours` when this same project started it, so a cancel of
    /// this project stops it (a build another project started is left to
    /// that project, and this one only stops waiting).
    Join { name: String, driving: bool, ours: bool },
    /// A build `gone` was running for it, but its driver stopped renewing
    /// its hold (that dispatcher is gone): this process now drives it, and
    /// builds it again as `name` in `lane` (`take_over`). `ours` as for
    /// `Join`.
    Adopt { gone: String, name: String, lane: u32, ours: bool },
}

impl Claim {
    /// Whether a cancel of this project may stop the build.
    pub fn stoppable(&self) -> bool {
        match self {
            Claim::Start { .. } => true,
            Claim::Join { ours, .. } | Claim::Adopt { ours, .. } => *ours,
        }
    }
}

/// Decide, for one image, whether to start a build, join the running one,
/// or take over the running one whose driver's hold lapsed, and record the
/// decision, in one transaction serialized per image.
pub async fn claim(
    pool: &PgPool,
    image_ref: &str,
    project_id: uuid::Uuid,
    tenant: &str,
    instance: &str,
    lanes: u32,
    now: i64,
) -> Result<Claim> {
    let mut tx = pool.begin().await.context("begin the build claim")?;
    lock_image(&mut tx, image_ref).await?;
    let running: Option<(String, String, i64, uuid::Uuid)> = sqlx::query_as(
        "SELECT build_name, driver_instance, driver_until, project_id FROM image_build \
         WHERE image_ref = $1 AND status = 'running'",
    )
    .bind(image_ref)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some((name, driver, driver_until, builds_for)) = running {
        let ours = builds_for == project_id;
        // A lapsed hold is taken over even when it names this process: a
        // dispatcher restarted under the same name lost track of its
        // build, like any other gone driver.
        let claim = if driver_until < now {
            let (new, lane) = take_over_in(&mut tx, image_ref, &name, instance, lanes, now)
                .await?
                .context("a lapsed build under the image lock could not be taken over")?;
            Claim::Adopt { gone: name, name: new, lane, ours }
        } else {
            Claim::Join { driving: driver == instance, ours, name }
        };
        tx.commit().await.context("commit the build claim")?;
        return Ok(claim);
    }
    let lane = pick_lane(&mut tx, lanes).await?;
    let name = build_name();
    sqlx::query(
        "INSERT INTO image_build (image_ref, project_id, tenant_id, build_name, lane, status, reason, \
                                  driver_instance, driver_until, started_at, finished_at) \
         VALUES ($1, $2, $3, $4, $5, 'running', NULL, $6, $7, $8, NULL) \
         ON CONFLICT (image_ref) DO UPDATE SET project_id = EXCLUDED.project_id, tenant_id = EXCLUDED.tenant_id, \
             build_name = EXCLUDED.build_name, lane = EXCLUDED.lane, status = 'running', reason = NULL, \
             driver_instance = EXCLUDED.driver_instance, driver_until = EXCLUDED.driver_until, \
             started_at = EXCLUDED.started_at, finished_at = NULL",
    )
    .bind(image_ref)
    .bind(project_id)
    .bind(tenant)
    .bind(&name)
    .bind(lane as i32)
    .bind(instance)
    .bind(now + DRIVER_LEASE_SECS)
    .bind(now)
    .execute(&mut *tx)
    .await
    .context("record the build")?;
    tx.commit().await.context("commit the build claim")?;
    Ok(Claim::Start { name, lane })
}

/// Take over the build `gone` of `image_ref`, whose driver's hold lapsed:
/// this process drives it from now on and builds it again under a new name, in
/// the same row (why always again: the module doc). The new name and lane;
/// none when `gone` no longer runs there with a lapsed hold (its end was
/// recorded, or another dispatcher took it over first).
pub async fn take_over(
    pool: &PgPool,
    image_ref: &str,
    gone: &str,
    instance: &str,
    lanes: u32,
    now: i64,
) -> Result<Option<(String, u32)>> {
    let mut tx = pool.begin().await.context("begin the build take-over")?;
    lock_image(&mut tx, image_ref).await?;
    let taken = take_over_in(&mut tx, image_ref, gone, instance, lanes, now).await?;
    tx.commit().await.context("commit the build take-over")?;
    Ok(taken)
}

async fn take_over_in(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    image_ref: &str,
    gone: &str,
    instance: &str,
    lanes: u32,
    now: i64,
) -> Result<Option<(String, u32)>> {
    let lane = pick_lane(tx, lanes).await?;
    let name = build_name();
    let res = sqlx::query(
        "UPDATE image_build SET build_name = $3, lane = $4, driver_instance = $5, driver_until = $6, started_at = $7 \
         WHERE image_ref = $1 AND build_name = $2 AND status = 'running' AND driver_until < $7",
    )
    .bind(image_ref)
    .bind(gone)
    .bind(&name)
    .bind(lane as i32)
    .bind(instance)
    .bind(now + DRIVER_LEASE_SECS)
    .bind(now)
    .execute(&mut **tx)
    .await
    .context("record the taken-over build")?;
    Ok((res.rows_affected() > 0).then_some((name, lane)))
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
        sqlx::query_as("SELECT lane, COUNT(*) FROM image_build WHERE status = 'running' GROUP BY lane")
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

/// Extend this process's hold on the build it drives.
pub async fn renew(pool: &PgPool, image_ref: &str, name: &str, instance: &str, now: i64) -> Result<()> {
    sqlx::query(
        "UPDATE image_build SET driver_until = $4 \
         WHERE image_ref = $1 AND build_name = $2 AND driver_instance = $3 AND status = 'running'",
    )
    .bind(image_ref)
    .bind(name)
    .bind(instance)
    .bind(now + DRIVER_LEASE_SECS)
    .execute(pool)
    .await
    .context("renew the build's driver hold")?;
    Ok(())
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
pub async fn finish(pool: &PgPool, image_ref: &str, name: &str, outcome: Outcome, now: i64) -> Result<()> {
    let (status, reason) = match outcome {
        Outcome::Succeeded => ("succeeded", None),
        Outcome::Failed(reason) => ("failed", Some(reason)),
        Outcome::Cancelled => ("cancelled", None),
    };
    sqlx::query(
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
    Ok(())
}

/// What the row of `image_ref` says now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Seen {
    /// A build runs for it: `name`, driven by `driver` until `driver_until`
    /// unless renewed.
    Running { name: String, driver: String, driver_until: i64 },
    /// Its build ended, recorded by whoever saw it end (`finish`).
    Ended(Outcome),
}

/// How the build a waiter holds as `name` stands. A build ends once however
/// many dispatchers wait on it, and the one that saw it end cleans it up
/// after recording this, so every other waiter reads the end here. The row
/// may name another build by now (a take-over); what it says is the answer
/// all the same (the module doc).
pub async fn look(pool: &PgPool, image_ref: &str, name: &str) -> Result<Seen> {
    let row: Option<(String, String, Option<String>, String, i64)> = sqlx::query_as(
        "SELECT build_name, status, reason, driver_instance, driver_until FROM image_build WHERE image_ref = $1",
    )
    .bind(image_ref)
    .fetch_optional(pool)
    .await
    .context("read how the build stands")?;
    let Some((current, status, reason, driver, driver_until)) = row else {
        return Ok(Seen::Ended(Outcome::Failed(format!("the build {name} is no longer recorded"))));
    };
    Ok(match status.as_str() {
        "running" => Seen::Running { name: current, driver, driver_until },
        "succeeded" => Seen::Ended(Outcome::Succeeded),
        "cancelled" => Seen::Ended(Outcome::Cancelled),
        _ => Seen::Ended(Outcome::Failed(reason.unwrap_or_else(|| "the build failed".into()))),
    })
}

/// Every image the install built and still holds (`status =
/// succeeded`); one project's when `project` is given: the images it
/// built or ran (`image_use`). Everything a prune may consider, before
/// the keep-set and the other projects' recent builds spare their part.
pub async fn built_images(pool: &PgPool, project: Option<uuid::Uuid>) -> Result<Vec<String>> {
    sqlx::query_scalar(
        "SELECT b.image_ref FROM image_build b \
         WHERE b.status = 'succeeded' AND ($1::UUID IS NULL OR b.project_id = $1 \
             OR EXISTS (SELECT 1 FROM image_use u WHERE u.image_ref = b.image_ref AND u.project_id = $1)) \
         ORDER BY b.image_ref",
    )
    .bind(project)
    .fetch_all(pool)
    .await
    .context("list the built images")
}

/// Every image the install built and still holds that no project's
/// version names (no `image_use` row): a removed project's, one whose
/// build never registered, the standard worker (`note_shared`). An image
/// a build in progress relies on is claimed instead (`claim_images`).
pub async fn unused_images(pool: &PgPool) -> Result<Vec<String>> {
    sqlx::query_scalar(
        "SELECT b.image_ref FROM image_build b \
         WHERE b.status = 'succeeded' AND NOT EXISTS (SELECT 1 FROM image_use u WHERE u.image_ref = b.image_ref) \
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
        "INSERT INTO image_build (image_ref, project_id, tenant_id, build_name, lane, status, driver_instance, driver_until, started_at, finished_at) \
         VALUES ($1, $2, '', 'shared', 0, 'succeeded', '', 0, $3, $3) ON CONFLICT (image_ref) DO NOTHING",
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
    /// A build in progress claims it.
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
/// Both checks are needed because a build lets go of its claim only after
/// its registration commits (`crate::api::project::build`): a reader
/// holding the lock sees either the live claim or the committed reference,
/// never neither. The prune's own keep-set was read before its loop and
/// can predate that registration, so it cannot stand in for the reference
/// check here.
pub async fn begin_reclaim(pool: &PgPool, image_ref: &str, now: i64) -> Result<Reclaim> {
    let scope = super::prune::ImageScope::of(image_ref)
        .with_context(|| format!("{image_ref} is not an image a prune may reclaim"))?;
    let mut tx = pool.begin().await.context("begin reclaiming an image")?;
    lock_image(&mut tx, image_ref).await?;
    let claimed: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM image_claim WHERE image_ref = $1 AND holder_until >= $2)")
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
pub async fn claim_images(pool: &PgPool, claim: uuid::Uuid, images: &[String], now: i64) -> Result<()> {
    let sorted: std::collections::BTreeSet<&String> = images.iter().collect();
    let mut tx = pool.begin().await.context("begin claiming the build's images")?;
    for image_ref in &sorted {
        lock_image(&mut tx, image_ref).await?;
    }
    let refs: Vec<&String> = sorted.into_iter().collect();
    sqlx::query(
        "INSERT INTO image_claim (image_ref, claim_id, holder_until) \
         SELECT image_ref, $2, $3 FROM UNNEST($1::TEXT[]) AS image_ref \
         ON CONFLICT (image_ref, claim_id) DO UPDATE SET holder_until = EXCLUDED.holder_until",
    )
    .bind(&refs)
    .bind(claim)
    .bind(now + DRIVER_LEASE_SECS)
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
        .bind(now + DRIVER_LEASE_SECS)
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

/// Clear the claims whose holder stopped renewing them (its dispatcher is
/// gone): they protect nothing any more.
pub async fn drop_lapsed_claims(pool: &PgPool, now: i64) -> Result<()> {
    sqlx::query("DELETE FROM image_claim WHERE holder_until < $1")
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

/// Every project's use of every image the install built and still holds.
pub async fn image_uses(pool: &PgPool) -> Result<Vec<ImageUse>> {
    sqlx::query_as(
        "SELECT u.project_id, u.image_ref, u.running_since FROM image_use u \
         JOIN image_build b ON b.image_ref = u.image_ref AND b.status = 'succeeded'",
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
