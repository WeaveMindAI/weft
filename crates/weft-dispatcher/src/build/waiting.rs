//! A version waiting on its image builds, and the one way any version
//! becomes its project's running one.
//!
//! A build request whose version needs images the registry lacks starts
//! their builds and answers at once (a 202). The version it compiled is
//! written down here, `waiting` ([`ask`]), and the build loop
//! (`super::follow`) registers it once every image it waits on is built,
//! or ends it `failed` with why ([`advance`]). Nobody has to ask again and
//! nothing has to stay up for it: the row is the whole of it, and the
//! caller follows it (`GET /projects/{id}/builds/{build}`, [`state`]).
//!
//! Every build request takes the next number in its project's order of
//! asks as it arrives, before it compiles, from the project's row
//! (`ProjectStoreOps::next_build_ask`), so every replica agrees on which
//! ask is newer whatever its clock says. A project has at most one waiting
//! version: a newer ask supersedes it, and the same version asked again
//! (a rerun after Ctrl+C) joins it. Every registration of a project, a
//! request's that found every image as much as the loop's, lands only
//! over an older ask than the one registered (`project.registered_ask`),
//! read under the project's version lock in the transaction that moves
//! the project's running pointers ([`settle`]), so an older version never
//! lands over a newer one, whichever finishes first.
//!
//! The row's id is the id of the request's claim on its images
//! (`super::prune::ImageHold`): the claim spares them from a prune for as
//! long as the version waits (`super::ledger::claim_live`), and goes in the
//! transaction that ends the row, whichever way it ends.

use anyhow::{anyhow, Context, Result};
use weft_task_store::db::{self, Client, FromRow, Pool};
use weft_core::builds::{BuiltProgram, ImageBuild, VersionBuildState, VersionBuildStatus};

use crate::project_store::{ProjectStoreOps, ProjectTransition, StoredProjectSummary};
use crate::state::DispatcherState;

/// The state a version waits in, as the column spells it.
pub(crate) const WAITING: &str = "waiting";

/// How long an ended version build stays readable: the caller following
/// it reads how it ended well within this. Older ones go on the build
/// loop's next pass ([`advance`]), and every one of a project as it is
/// removed ([`forget_project`]).
pub const ENDED_KEPT_SECS: i64 = 3600;

/// The channel a version build announces its end on (registered, failed,
/// cancelled or superseded), the payload its id: what a wait for it inside
/// the install hears ([`wait_ended`]).
pub const VERSION_BUILD_ENDED_CHANNEL: &str = "weft_version_build_ended";

/// Why a version superseded by a newer registration never registers.
const NEWER_REGISTERED: &str = "a newer build of this project registered first";

/// One row per version a build request answered while its images were
/// still building.
pub const VERSION_BUILD_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS version_build (
            -- Also the claim_id of the image_claim rows sparing the
            -- version's images while it waits.
            id UUID PRIMARY KEY,
            -- Its place in the project's order of asks (project.build_asks).
            ask BIGINT NOT NULL,
            project_id UUID NOT NULL,
            tenant_id TEXT NOT NULL,
            -- The project's name, as the request named it.
            project_name TEXT NOT NULL,
            -- The version's files, registered as the program's source.
            manifest JSONB NOT NULL,
            -- What they were compiled with (weft_core::builds::BuildInputs),
            -- registered with them. NULL for a version an older weft
            -- wrote down, which kept none; a row lives only while its
            -- images build, so no document version goes with it. As its
            -- JSON text (`db::JsonText`): its assets are keyed by
            -- `FileRef::resolution_key`, which holds a NUL.
            inputs TEXT,
            -- The BuiltProgram the request compiled (replacedInfraImages
            -- empty: the registration fills it), as its JSON text
            -- (`db::JsonText`), like `registered`: a text asset is
            -- inlined in it, whatever the file or the page held.
            program TEXT NOT NULL,
            -- Every image the version runs.
            images TEXT[] NOT NULL,
            -- The ImageBuilds it waits on, by image.
            waits_on JSONB NOT NULL,
            -- SYNC: the states <-> weft_core::builds::VersionBuildStatus
            state TEXT NOT NULL CHECK (state IN ('waiting', 'registered', 'failed', 'cancelled', 'superseded')),
            reason TEXT,
            -- The BuiltProgram answered once registered.
            registered TEXT,
            asked_at BIGINT NOT NULL,
            ended_at BIGINT
        )"#;

/// At most one waiting version per project.
pub const ONE_WAITING_PER_PROJECT: &str =
    "CREATE UNIQUE INDEX IF NOT EXISTS version_build_one_waiting ON version_build (project_id) WHERE state = 'waiting'";

/// The SQL condition "the project bound at `project` has a waiting
/// version": what holds its build transition once no request drives it
/// (`crate::project_store::transition_held`).
pub(crate) fn has_waiting_version(project: &str) -> String {
    format!("EXISTS (SELECT 1 FROM version_build v WHERE v.project_id = {project} AND v.state = '{WAITING}')")
}

fn status_name(status: VersionBuildStatus) -> &'static str {
    match status {
        VersionBuildStatus::Waiting => WAITING,
        VersionBuildStatus::Registered => "registered",
        VersionBuildStatus::Failed => "failed",
        VersionBuildStatus::Cancelled => "cancelled",
        VersionBuildStatus::Superseded => "superseded",
    }
}

fn status_of(name: &str) -> Result<VersionBuildStatus> {
    Ok(match name {
        WAITING => VersionBuildStatus::Waiting,
        "registered" => VersionBuildStatus::Registered,
        "failed" => VersionBuildStatus::Failed,
        "cancelled" => VersionBuildStatus::Cancelled,
        "superseded" => VersionBuildStatus::Superseded,
        other => return Err(anyhow!("a version build state the ledger does not know: '{other}'")),
    })
}

/// Whether two compiled programs are the same version: the same worker,
/// runtime shape and infra.
pub fn same_version(a: &BuiltProgram, b: &BuiltProgram) -> bool {
    a.binary_hash == b.binary_hash && a.definition_hash == b.definition_hash && a.infra_hash == b.infra_hash
}

/// Serialize, for the transaction, every decision about `project`'s
/// versions: an ask writing one, a registration, a cancel.
async fn lock_versions(conn: &impl Client, project: uuid::Uuid) -> Result<()> {
    conn.execute("SELECT pg_advisory_xact_lock(hashtextextended('version_build:' || $1::uuid::TEXT, 0))", &[&project])
        .await
        .context("lock the project's versions")?;
    Ok(())
}

/// End the version `id` as `status`, when it still waits, and let go of
/// its claim on its images in the same statement. `false` when it no
/// longer waited.
async fn end(
    conn: &impl Client,
    id: uuid::Uuid,
    status: VersionBuildStatus,
    reason: Option<&str>,
    registered: Option<&BuiltProgram>,
    now: i64,
) -> Result<bool> {
    let ended: i64 = conn
        .scalar(
            "WITH ended AS (UPDATE version_build SET state = $2, reason = $3, registered = $4, ended_at = $5 \
                            WHERE id = $1 AND state = 'waiting' RETURNING id), \
                  dropped AS (DELETE FROM image_claim WHERE claim_id IN (SELECT id FROM ended)) \
             SELECT count(*) FROM (SELECT pg_notify($6, id::TEXT) FROM ended) told",
            &[
                &id,
                &status_name(status),
                &reason,
                &registered.map(db::JsonText),
                &now,
                &VERSION_BUILD_ENDED_CHANNEL,
            ],
        )
        .await
        .context("end the version build")?;
    Ok(ended == 1)
}

/// The project's waiting version (its id, ask and program), read under its
/// version lock.
async fn waiting_of(conn: &impl Client, project: uuid::Uuid) -> Result<Option<(uuid::Uuid, i64, BuiltProgram)>> {
    let row: Option<(uuid::Uuid, i64, String)> = conn
        .opt("SELECT id, ask, program FROM version_build WHERE project_id = $1 AND state = 'waiting'", &[&project])
        .await
        .context("read the project's waiting version")?;
    row.map(|(id, asked_at, program)| {
        Ok((id, asked_at, serde_json::from_str(&program).context("a waiting version's program does not read")?))
    })
    .transpose()
}

/// The ask of what `project` runs; 0 before its first registration, and
/// for a project removed meanwhile, which has nothing registered.
async fn registered_ask(conn: &impl Client, project: uuid::Uuid) -> Result<i64> {
    conn.scalar("SELECT COALESCE((SELECT registered_ask FROM project WHERE id = $1), 0)", &[&project])
        .await
        .context("read the ask the project runs")
}

/// Write `version` down as waiting on `waits_on`, under `hold`'s id, and
/// answer that id. In one transaction: refused once the project's build
/// was cancelled; when the project's waiting version is this same version
/// it is joined instead (its id answered, its ask raised to this one, and
/// `hold`'s claim let go: the joined row's claim covers the same images);
/// otherwise the older of the two asks is superseded. A version older
/// than what the project runs, or than the version waiting, is written
/// down superseded already, for its caller to read why. The build loop is
/// woken as a waiting version commits, so one whose builds already ended
/// registers at once.
pub async fn ask(
    pool: &Pool,
    hold: super::prune::ImageHold,
    project: uuid::Uuid,
    tenant: &str,
    version: &super::Version,
    waits_on: &[ImageBuild],
    now: i64,
) -> Result<uuid::Uuid> {
    let mut conn = pool.get().await.context("begin writing the waiting version")?;
    let tx = conn.transaction().await.context("begin writing the waiting version")?;
    lock_versions(&tx, project).await?;
    super::ledger::refuse_once_cancelled(&tx, project).await?;
    let mut superseded_by = (registered_ask(&tx, project).await? >= version.ask).then_some(NEWER_REGISTERED);
    if let Some((waiting, waiting_ask, program)) = waiting_of(&tx, project).await? {
        if same_version(&program, &version.program) {
            tx.execute("UPDATE version_build SET ask = GREATEST(ask, $2) WHERE id = $1", &[&waiting, &version.ask])
                .await
                .context("join the waiting version")?;
            tx.commit().await.context("commit joining the waiting version")?;
            let_go_of(hold, project, "it joined the waiting version").await;
            return Ok(waiting);
        }
        if waiting_ask > version.ask {
            superseded_by = superseded_by.or(Some("a newer build of this project was asked for"));
        } else if superseded_by.is_none() {
            end(&tx, waiting, VersionBuildStatus::Superseded, Some("a newer build of this project was asked for"), None, now).await?;
        }
    }
    let id = hold.id();
    let (state, ended_at) = match superseded_by {
        Some(_) => (VersionBuildStatus::Superseded, Some(now)),
        None => (VersionBuildStatus::Waiting, None),
    };
    tx.execute(
        "INSERT INTO version_build (id, ask, project_id, tenant_id, project_name, manifest, program, images, waits_on, \
                                    state, reason, registered, asked_at, ended_at, inputs) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NULL, $12, $13, $15) \
         RETURNING pg_notify($14, id::TEXT)",
        &[
            &id,
            &version.ask,
            &project,
            &tenant,
            &version.name,
            &serde_json::to_value(&version.manifest)?,
            &db::JsonText(&version.program),
            &version.images,
            &serde_json::to_value(waits_on)?,
            &status_name(state),
            &superseded_by,
            &now,
            &ended_at,
            &super::follow::BUILD_CLAIMED_CHANNEL,
            &version.inputs.as_ref().map(db::JsonText),
        ],
    )
    .await
    .context("write the waiting version")?;
    tx.commit().await.context("commit the waiting version")?;
    match superseded_by {
        Some(_) => let_go_of(hold, project, "its version was superseded already").await,
        None => hold.pass_to_version(),
    }
    Ok(id)
}

/// Let go of a request's claim the version it asked for does not need
/// (`why`): a failure only leaves the claim to lapse within its lease.
async fn let_go_of(hold: super::prune::ImageHold, project: uuid::Uuid, why: &str) {
    if let Err(e) = hold.release().await {
        tracing::warn!(
            target: "weft_dispatcher::build",
            project_id = %project,
            error = %format!("{e:#}"),
            "letting go of a build request's claim on its images failed after {why}; the claim lapses within 60 seconds"
        );
    }
}

/// A registration refused because a resync holds the project ([`settle`]):
/// what the build's caller is told, as a 409.
#[derive(Debug)]
pub struct ResyncHolds;

impl std::fmt::Display for ResyncHolds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "this build was not registered: {}", ProjectTransition::Resyncing.refusal().unwrap_or_default())
    }
}

impl std::error::Error for ResyncHolds {}

impl ResyncHolds {
    /// Whether `e` is (or wraps) this refusal.
    pub fn is(e: &anyhow::Error) -> bool {
        e.chain().any(|cause| cause.downcast_ref::<ResyncHolds>().is_some())
    }
}

/// What a registration settles, read and written in the registration's
/// own transaction (`ProjectStoreOps::register_with_hashes`). Either way it
/// lands only over an older ask than the one the project runs.
pub enum Settles<'a> {
    /// A request whose ask is `ask` found every image of its version: the
    /// waiting version ends `registered` with `answer` when it is this same
    /// version, `superseded` when it was asked before, and keeps waiting
    /// when it was asked after (it registers over this one later).
    Ask { ask: i64, answer: &'a BuiltProgram },
    /// The build loop registers the waiting `version` itself, as `answer`,
    /// only while it still waits; it ends `superseded` when a newer ask
    /// registered meanwhile.
    Waiting { version: uuid::Uuid, answer: &'a BuiltProgram },
}

/// [`Settles`], inside the registration's transaction and under the
/// project's version lock. `false` when the registration must not land:
/// a newer ask registered already, the version the loop registers no
/// longer waits, or its project was removed, which ends it.
pub(crate) async fn settle(conn: &impl Client, project: uuid::Uuid, settles: &Settles<'_>, now: i64) -> Result<bool> {
    lock_versions(conn, project).await?;
    // Every registration passes here, under the project row's lock, which
    // a resync takes to begin (`ProjectStoreOps::try_begin_resyncing`): no
    // version registers while a resync holds the project, whose triggers
    // it brings back on the build it named.
    let row: Option<(i64, String)> = conn
        .opt("SELECT registered_ask, transition FROM project WHERE id = $1 FOR UPDATE", &[&project])
        .await
        .context("read the ask the project runs")?;
    if row.as_ref().is_some_and(|(_, transition)| transition == ProjectTransition::Resyncing.as_str()) {
        return Err(ResyncHolds.into());
    }
    let running = row.map(|(ask, _)| ask);
    let registers = match settles {
        Settles::Waiting { version, answer } => {
            let row: Option<(String, i64)> = conn
                .opt("SELECT state, ask FROM version_build WHERE id = $1 AND project_id = $2", &[version, &project])
                .await
                .context("read the version to register")?;
            let Some((state, ask)) = row else {
                return Err(anyhow!("no version build {version} of project {project}"));
            };
            if state != WAITING {
                return Ok(false);
            }
            let Some(running) = running else {
                end(conn, *version, VersionBuildStatus::Cancelled, Some("the project was removed"), None, now).await?;
                return Ok(false);
            };
            if ask <= running {
                end(conn, *version, VersionBuildStatus::Superseded, Some(NEWER_REGISTERED), None, now).await?;
                return Ok(false);
            }
            if !end(conn, *version, VersionBuildStatus::Registered, None, Some(answer), now).await? {
                return Ok(false);
            }
            ask
        }
        Settles::Ask { ask, answer } => {
            let running = running.ok_or_else(|| anyhow!("no project {project} to register a build of"))?;
            if *ask <= running {
                return Ok(false);
            }
            let mut registers = *ask;
            if let Some((waiting, waiting_ask, program)) = waiting_of(conn, project).await? {
                if same_version(&program, answer) {
                    end(conn, waiting, VersionBuildStatus::Registered, None, Some(answer), now).await?;
                    registers = registers.max(waiting_ask);
                } else if waiting_ask < *ask {
                    end(conn, waiting, VersionBuildStatus::Superseded, Some(NEWER_REGISTERED), None, now).await?;
                }
            }
            registers
        }
    };
    conn.execute("UPDATE project SET registered_ask = $2 WHERE id = $1", &[&project, &registers])
        .await
        .context("record the ask the project runs")?;
    // A registered version is runnable at once: the project lands at rest
    // in this same transaction when nothing else holds its build, so
    // whoever hears the version registered (its waiter, woken as this
    // commits) never finds the project still building.
    conn.scalars::<uuid::Uuid>(
        &crate::project_store::settle_transition_sql(),
        &[&(now - crate::transition::heartbeat_stale_secs()), &Some(project)],
    )
    .await
    .context("land the project at rest")?;
    Ok(true)
}

/// Who registers a version.
pub enum Registrar {
    /// The build request that found every image there and holds them in
    /// `hold`, let go once the registration committed.
    Request { hold: super::prune::ImageHold },
    /// The build loop, for the waiting version of this id (its claim goes
    /// in the registration's own transaction).
    Loop { version: uuid::Uuid },
}

/// Register `version` as `project`'s running one: the answer's
/// `replaced_infra_images` filled from what ran before, the program, its
/// hashes and its source written with the project's running pointers and
/// the waiting version's end in one transaction ([`Settles`]), then the
/// images recorded as the project's running version, for a prune picking
/// what to reclaim (`ledger::note_running`). With the project's summary,
/// what registered; `None` when it must not land ([`settle`]).
/// The database half of [`register`], which a test drives with a store.
pub async fn register_version(
    projects: &dyn ProjectStoreOps,
    pool: &Pool,
    project: uuid::Uuid,
    tenant: &str,
    version: &super::Version,
    registrar: Registrar,
) -> Result<Option<(StoredProjectSummary, BuiltProgram)>> {
    let (hold, settles_by) = match registrar {
        Registrar::Request { hold } => (Some(hold), None),
        Registrar::Loop { version: waiting } => (None, Some(waiting)),
    };
    // What its worker left out, from the image's own record: a worker
    // reused from an earlier build refuses the same runs it did.
    let mut registered = match super::ledger::left_out(pool, &version.program.worker_image).await {
        Ok(left_out) => registered_in_store(projects, project, tenant, version, settles_by, left_out).await,
        Err(e) => Err(e),
    };
    // Let go only now: a prune deciding about one of these images under
    // its lock sees either this claim or the committed reference
    // (`ledger::decide_reclaim`), never neither.
    if let Some(hold) = hold {
        if let Err(released) = hold.release().await {
            if registered.is_ok() {
                tracing::error!(
                    target: "weft_dispatcher::build",
                    project_id = %project,
                    error = %format!("{released:#}"),
                    "letting go of the build's claim on its images failed after the version registered; the claim \
                     lapses within 60 seconds"
                );
            }
            // A registration that failed keeps its own error (a
            // `ResyncHolds` still answers 409), the release's failure
            // added as context.
            registered = registered.map_err(|e| {
                e.context(format!(
                    "letting go of the build's claim on its images failed too ({released:#}); the claim lapses within 60 seconds"
                ))
            });
        }
    }
    let Some((summary, answer)) = registered? else { return Ok(None) };
    if let Err(e) = super::ledger::note_running(pool, project, &version.images, crate::lease::now_unix()).await {
        tracing::error!(
            target: "weft_dispatcher::build",
            project_id = %project,
            error = %format!("{e:#}"),
            "recording the images the project runs failed after the version registered; a prune keeps one older build \
             of this project longer, until the next build registers"
        );
    }
    Ok(Some((summary, answer)))
}

/// [`register_version`]'s write: `waiting` is the loop's waiting version,
/// `None` for a request.
async fn registered_in_store(
    projects: &dyn ProjectStoreOps,
    project: uuid::Uuid,
    tenant: &str,
    version: &super::Version,
    waiting: Option<uuid::Uuid>,
    left_out: (weft_core::builds::LeftOut, Option<String>),
) -> Result<Option<(StoredProjectSummary, BuiltProgram)>> {
    let before = projects
        .running_infra_image_tags(project)
        .await
        .context("read the registered infra images")?
        .unwrap_or_default();
    let mut answer = version.program.clone();
    answer.replaced_infra_images = super::replaced_infra_images(&before, &answer.infra_images);
    (answer.left_out, answer.worker_log) = (left_out.0.packages, left_out.1);
    let code = crate::project_store::ProgramCode {
        implementations: answer.implementations.clone(),
        left_out: answer.left_out.clone(),
        worker_log: answer.worker_log.clone(),
        node_needs: answer.node_needs.clone(),
    };
    let infra_images: crate::project_store::InfraImageTags = answer
        .infra_images
        .iter()
        .map(|(place, images)| (place.clone(), images.iter().map(|(k, v)| (k.clone(), v.clone())).collect()))
        .collect();
    let settles = match waiting {
        None => Settles::Ask { ask: version.ask, answer: &answer },
        Some(version) => Settles::Waiting { version, answer: &answer },
    };
    let summary = projects
        .register_with_hashes(
            answer.definition.clone(),
            &version.name,
            "",
            tenant,
            Some(&answer.binary_hash),
            Some(&answer.definition_hash),
            Some(&answer.infra_hash),
            Some(&infra_images),
            Some(&code),
            Some(&crate::project_store::RunningSource { manifest: &version.manifest, inputs: version.inputs.as_ref() }),
            Some(&settles),
            Some(&answer.infra_places),
        )
        .await
        .context("register the build")?;
    Ok(summary.map(|summary| (summary, answer)))
}

/// Register `version` ([`register_version`], which lands the project at
/// rest in the same transaction unless something else still holds its
/// build transition: a registered version must be runnable at once), then
/// say so: the images its earlier builds left are reclaimed
/// (`super::prune::AfterBuildPrunes`), and every editor hears of it. THE
/// way a version becomes its project's running one.
pub async fn register(
    state: &DispatcherState,
    project: uuid::Uuid,
    tenant: &str,
    version: &super::Version,
    registrar: Registrar,
) -> Result<Option<BuiltProgram>> {
    let Some((summary, answer)) =
        register_version(state.projects.as_ref(), &state.pg_pool, project, tenant, version, registrar).await?
    else {
        return Ok(None);
    };
    announce(state, &summary).await;
    Ok(Some(answer))
}

/// What follows a registration that committed. Each step runs whatever
/// the others did, and a failure is logged with what it leaves, never
/// answered: the version IS the project's running one.
pub async fn announce(state: &DispatcherState, summary: &StoredProjectSummary) {
    // The registration landed the project at rest when nothing else held
    // it ([`settle`]); editors hear of that here.
    crate::transition::publish_transition_changed(state, summary.id).await;
    state.builder.prunes.request(state, Some(summary.id));
    // The files the build reads are the project's references now, beside
    // every version's and working copy's.
    if let Err((_, message)) = crate::actions::versions::publish_references(state, summary.id, &[]).await {
        tracing::error!(
            target: "weft_dispatcher::build",
            project_id = %summary.id,
            error = %message,
            "publishing the project's asset references failed after the version registered; the next change of its \
             files, stamp or build publishes them again"
        );
    }
    // A queued event a failing hand-over stopped may run on this version.
    crate::parked_drain::retry_stopped(state, summary.id).await;
    state
        .events
        .publish(crate::events::DispatcherEvent::ProjectRegistered {
            project_id: summary.id,
            name: weft_core::truncate_user_string(&summary.name, 4096),
        })
        .await;
}

/// How the image builds a version waits on stand, by image.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// An image still builds (the build named, or a newer one of it).
    Waits,
    /// Every image was built.
    Built,
    /// Some image will not be built: why, each failed image's reason, the
    /// first one the version names first.
    Failed(String),
}

/// One image's row in the ledger, as [`verdict`] reads it.
#[derive(Debug, Clone)]
pub struct ImageRow {
    pub image_ref: String,
    pub status: String,
    pub reason: Option<String>,
}

impl FromRow for ImageRow {
    fn from_row(row: db::Row) -> db::Result<Self> {
        Ok(Self { image_ref: row.try_get("image_ref")?, status: row.try_get("status")?, reason: row.try_get("reason")? })
    }
}

/// Where a version waiting on `waits_on` stands, from the ledger's rows
/// of those images. An image with no row was forgotten (its build no
/// longer on record), which fails it.
pub fn verdict(waits_on: &[ImageBuild], rows: &[ImageRow]) -> Verdict {
    let mut failed = Vec::new();
    for image in waits_on {
        match rows.iter().find(|row| row.image_ref == image.image) {
            Some(row) if row.status == "running" => return Verdict::Waits,
            Some(row) if row.status == "succeeded" => {}
            Some(row) if row.status == "cancelled" => failed.push(format!("{}: its build was cancelled", image.image)),
            Some(row) => failed.push(format!(
                "{}: {}",
                image.image,
                row.reason.as_deref().unwrap_or("its build failed without saying why")
            )),
            None => failed.push(format!("{}: the image's build is no longer on record; build again", image.image)),
        }
    }
    if failed.is_empty() {
        Verdict::Built
    } else {
        Verdict::Failed(failed.join("\n"))
    }
}

/// What one pass over the waiting versions did.
#[derive(Default)]
pub struct Advanced {
    /// The versions it registered, to announce ([`announce`]).
    pub registered: Vec<StoredProjectSummary>,
    /// Whether a version still waits on a build.
    pub waiting: bool,
    /// The versions it could not move forward, and why.
    pub failed: Vec<String>,
}

/// Move every waiting version forward ([`verdict`]): register the ones
/// whose images were all built, end `failed` the ones an image will never
/// come for, and leave the rest waiting. A version that cannot be moved
/// forward for any reason but the database being out of reach (its
/// registration failed, what it stored no longer reads) ends `failed`
/// with why, rather than being tried again on every look for ever, which
/// would keep the install awake and its project building. One version's
/// failure never skips the others. The versions ended over
/// [`ENDED_KEPT_SECS`] ago go first.
pub async fn advance(pool: &Pool, projects: &dyn ProjectStoreOps) -> Result<Advanced> {
    let now = crate::lease::now_unix();
    pool.execute("DELETE FROM version_build WHERE state <> 'waiting' AND ended_at < $1", &[&(now - ENDED_KEPT_SECS)])
        .await
        .context("forget the long-ended version builds")?;
    let rows: Vec<WaitingRow> = pool
        .all(
            "SELECT id, ask, project_id, tenant_id, project_name, manifest, inputs, program, images, waits_on FROM version_build \
             WHERE state = 'waiting' ORDER BY asked_at",
            &[],
        )
        .await
        .context("list the waiting versions")?;
    let mut advanced = Advanced::default();
    for row in rows {
        let id = row.id;
        let moved = match advance_one(pool, projects, row).await {
            Err(e) if !db::database_unreachable(&e) => {
                let reason = format!("{e:#}");
                let ended = end(pool, id, VersionBuildStatus::Failed, Some(&reason), None, crate::lease::now_unix()).await;
                ended.map(|_| Moved::Ended).map_err(|ending| ending.context(reason))
            }
            moved => moved,
        };
        match moved {
            Ok(Moved::Waits) => advanced.waiting = true,
            Ok(Moved::Ended) => {}
            Ok(Moved::Registered(summary)) => advanced.registered.push(summary),
            Err(e) => {
                advanced.waiting = true;
                advanced.failed.push(format!("version build {id}: {e:#}"));
            }
        }
    }
    Ok(advanced)
}

/// A waiting version as [`advance`] reads it.
struct WaitingRow {
    id: uuid::Uuid,
    ask: i64,
    project_id: uuid::Uuid,
    tenant_id: String,
    project_name: String,
    manifest: serde_json::Value,
    inputs: Option<String>,
    program: String,
    images: Vec<String>,
    waits_on: serde_json::Value,
}

impl FromRow for WaitingRow {
    fn from_row(row: db::Row) -> db::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            ask: row.try_get("ask")?,
            project_id: row.try_get("project_id")?,
            tenant_id: row.try_get("tenant_id")?,
            project_name: row.try_get("project_name")?,
            manifest: row.try_get("manifest")?,
            inputs: row.try_get("inputs")?,
            program: row.try_get("program")?,
            images: row.try_get("images")?,
            waits_on: row.try_get("waits_on")?,
        })
    }
}

/// What [`advance_one`] did with one waiting version.
enum Moved {
    Waits,
    /// Ended without registering (failed, or it no longer waited).
    Ended,
    Registered(StoredProjectSummary),
}

/// Move one waiting version forward. Every failure is answered, for
/// [`advance`] to end the version with it.
async fn advance_one(pool: &Pool, projects: &dyn ProjectStoreOps, row: WaitingRow) -> Result<Moved> {
    let waits_on: Vec<ImageBuild> = serde_json::from_value(row.waits_on).context("the images it waits on do not read")?;
    let refs: Vec<&str> = waits_on.iter().map(|image| image.image.as_str()).collect();
    let images: Vec<ImageRow> = pool
        .all("SELECT image_ref, status, reason FROM image_build WHERE image_ref = ANY($1)", &[&refs])
        .await
        .context("read the builds the version waits on")?;
    match verdict(&waits_on, &images) {
        Verdict::Waits => Ok(Moved::Waits),
        Verdict::Failed(reason) => {
            end(pool, row.id, VersionBuildStatus::Failed, Some(&reason), None, crate::lease::now_unix()).await?;
            Ok(Moved::Ended)
        }
        Verdict::Built => {
            let version = super::Version {
                name: row.project_name,
                manifest: serde_json::from_value(row.manifest).context("its files do not read")?,
                inputs: row.inputs.as_deref().map(serde_json::from_str).transpose().context("what its files were compiled with does not read")?,
                program: serde_json::from_str(&row.program).context("its program does not read")?,
                images: row.images,
                ask: row.ask,
            };
            let registrar = Registrar::Loop { version: row.id };
            Ok(match register_version(projects, pool, row.project_id, &row.tenant_id, &version, registrar)
                .await
                .context("every image was built, but registering the version failed")?
            {
                Some((summary, _)) => Moved::Registered(summary),
                None => Moved::Ended,
            })
        }
    }
}

/// Wait until version build `id` of `project` ends, and answer how it
/// stands then. Heard as it ends ([`VERSION_BUILD_ENDED_CHANNEL`]), and
/// read again whenever the watch may have missed something; never on a
/// clock. No deadline: its images build for as long as they take, and
/// `weft cancel-build` ends it.
pub async fn wait_ended(state: &DispatcherState, project: uuid::Uuid, id: uuid::Uuid) -> Result<VersionBuildState> {
    // Subscribed before the first read, so an end between the two is heard.
    let mut heard = state.signals.subscribe();
    let payload = id.to_string();
    loop {
        let now = self::state(&state.pg_pool, project, id)
            .await?
            .ok_or_else(|| anyhow!("version build {id} of project {project} is gone"))?;
        if now.state != VersionBuildStatus::Waiting {
            return Ok(now);
        }
        loop {
            match heard.next().await? {
                weft_task_store::pg_signal::Heard::Signal { channel, payload: p } if channel == VERSION_BUILD_ENDED_CHANNEL && *p == *payload => break,
                weft_task_store::pg_signal::Heard::Recheck | weft_task_store::pg_signal::Heard::Resumed => break,
                _ => {}
            }
        }
    }
}

/// Forget every version build of a removed project, letting go of a
/// waiting one's claim with it.
pub async fn forget_project(pool: &Pool, project: uuid::Uuid) -> Result<()> {
    pool.execute(
        "WITH gone AS (DELETE FROM version_build WHERE project_id = $1 RETURNING id) \
         DELETE FROM image_claim WHERE claim_id IN (SELECT id FROM gone)",
        &[&project],
    )
    .await
    .context("forget the removed project's version builds")?;
    Ok(())
}

/// End `project`'s waiting version `cancelled` (the person cancelled its
/// build), letting go of its claim.
pub async fn cancel(pool: &Pool, project: uuid::Uuid) -> Result<()> {
    let mut conn = pool.get().await.context("begin cancelling the waiting version")?;
    let tx = conn.transaction().await.context("begin cancelling the waiting version")?;
    lock_versions(&tx, project).await?;
    if let Some((waiting, _, _)) = waiting_of(&tx, project).await? {
        end(&tx, waiting, VersionBuildStatus::Cancelled, Some("the build was cancelled"), None, crate::lease::now_unix()).await?;
    }
    tx.commit().await.context("commit cancelling the waiting version")?;
    Ok(())
}

/// Where `project`'s version build `id` stands, with each image build it
/// waits on; `None` when the project has no such version build.
pub async fn state(pool: &Pool, project: uuid::Uuid, id: uuid::Uuid) -> Result<Option<VersionBuildState>> {
    let row: Option<(String, Option<String>, Option<String>, serde_json::Value)> = pool
        .opt("SELECT state, reason, registered, waits_on FROM version_build WHERE id = $1 AND project_id = $2", &[&id, &project])
        .await
        .context("read the version build")?;
    let Some((state, reason, registered, waits_on)) = row else { return Ok(None) };
    let waits_on: Vec<ImageBuild> = serde_json::from_value(waits_on).context("a version build's images do not read")?;
    Ok(Some(VersionBuildState {
        state: status_of(&state)?,
        images: super::ledger::image_states(pool, &waits_on).await?,
        program: registered.as_deref().map(serde_json::from_str).transpose().context("a registered version's program does not read")?,
        reason,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn waits(images: &[&str]) -> Vec<ImageBuild> {
        images.iter().map(|image| ImageBuild { image: image.to_string(), name: format!("{image}-build") }).collect()
    }

    fn row(image: &str, status: &str, reason: Option<&str>) -> ImageRow {
        ImageRow { image_ref: image.into(), status: status.into(), reason: reason.map(str::to_string) }
    }

    #[test]
    fn a_version_waits_while_any_image_builds() {
        let rows = [row("a", "failed", Some("boom")), row("b", "running", None)];
        assert_eq!(verdict(&waits(&["a", "b"]), &rows), Verdict::Waits, "nothing is decided while one still builds");
    }

    #[test]
    fn a_version_is_built_once_every_image_succeeded() {
        let rows = [row("a", "succeeded", None), row("b", "succeeded", None)];
        assert_eq!(verdict(&waits(&["a", "b"]), &rows), Verdict::Built);
        assert_eq!(verdict(&[], &[]), Verdict::Built);
    }

    #[test]
    fn a_failed_version_names_every_failed_image_first_first() {
        let rows = [row("a", "succeeded", None), row("b", "failed", Some("cargo: error[E0425]")), row("c", "cancelled", None)];
        let Verdict::Failed(reason) = verdict(&waits(&["a", "b", "c", "d"]), &rows) else { panic!("failed") };
        assert_eq!(
            reason,
            "b: cargo: error[E0425]\nc: its build was cancelled\nd: the image's build is no longer on record; build again"
        );
    }

    #[test]
    fn every_state_round_trips_through_its_column_name() {
        for status in [
            VersionBuildStatus::Waiting,
            VersionBuildStatus::Registered,
            VersionBuildStatus::Failed,
            VersionBuildStatus::Cancelled,
            VersionBuildStatus::Superseded,
        ] {
            assert_eq!(status_of(status_name(status)).unwrap(), status);
            assert_eq!(serde_json::to_value(status).unwrap(), status_name(status), "the column spells it as the wire does");
        }
    }
}
