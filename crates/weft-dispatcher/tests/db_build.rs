//! A version build's image half against a real database: the ledger that
//! lets any dispatcher find and join a build, the build loop's look that
//! sees it through, the waiting version it registers once its builds
//! ended, and the build transition that version holds until then, driven
//! through the builder with a fake `ImageBuilder`.
//!
//! Same rig as `db_versions.rs`: `#[sqlx::test]` hands each test a fresh
//! database; the dispatcher's whole schema is applied as a boot does.
#![cfg(feature = "db-tests")]

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;

use sqlx::PgPool;
use weft_compiler::build_plan::{ImageKind, PlannedImage};
use weft_core::builds::{BuiltProgram, BuildsUnderway, ImageBuild, VersionBuildStatus};
use weft_core::projects::BuildState;
use weft_dispatcher::build::ledger::{self, Claim};
use weft_dispatcher::build::prune::{ImageHold, ImageScope, KeepSet};
use weft_dispatcher::build::waiting::{self, Registrar};
use weft_dispatcher::build::{follow, BuildGate, Version, VersionBuild, VersionBuilder};
use weft_platform_traits::{BuildStatus, FakeImageBuilder, ImageBuilder, Staging};

const TENANT: &str = "local";

async fn setup(pool: &PgPool) {
    weft_dispatcher::app::apply_core_schema(pool).await.expect("core schema");
}

fn builder(pool: &PgPool, images: Arc<FakeImageBuilder>) -> VersionBuilder {
    builder_on(pool, images)
}

fn builder_on(pool: &PgPool, images: Arc<dyn ImageBuilder>) -> VersionBuilder {
    VersionBuilder {
        bases: weft_compiler::worker_image::BaseImages {
            builder: "reg:5000/base:1".into(),
            runtime: "debian:bookworm-slim".into(),
        },
        images,
        pool: pool.clone(),
        compile_lanes: 2,
        prunes: Default::default(),
        blobs: weft_dispatcher::build::blob_cache::BlobCache::in_temp_dir(),
    }
}

fn image(kind: ImageKind, image_ref: &str) -> PlannedImage {
    PlannedImage {
        kind,
        image_ref: image_ref.into(),
        context_dir: "/tmp/ctx".into(),
        node_id: None,
        image_name: None,
    }
}

/// The asks of the tests that take no number from a project row: later
/// asks get greater numbers, as they would from one.
static ASKS: AtomicI64 = AtomicI64::new(1);

/// `project`'s version whose worker is `binary`, running `images`, asked
/// now.
fn version(project: uuid::Uuid, binary: &str, images: &[PlannedImage]) -> Version {
    version_asked(project, binary, images, ASKS.fetch_add(1, Ordering::SeqCst))
}

/// [`version`], asked as the project's `ask`th build.
fn version_asked(project: uuid::Uuid, binary: &str, images: &[PlannedImage], ask: i64) -> Version {
    let mut refs: Vec<String> = images.iter().map(|image| image.image_ref.clone()).collect();
    refs.sort();
    refs.dedup();
    Version {
        name: "p".into(),
        manifest: Default::default(),
        program: BuiltProgram {
            definition: serde_json::from_value(serde_json::json!({ "id": project, "nodes": [], "edges": [] })).unwrap(),
            binary_hash: binary.into(),
            definition_hash: format!("def-{binary}"),
            infra_hash: "infra".into(),
            implementations: Default::default(),
            infra_images: Default::default(),
            replaced_infra_images: Vec::new(),
        },
        images: refs,
        ask,
    }
}

/// Ask for `project`'s version `binary` of `images`, as a build request does.
async fn ask(
    builder: &VersionBuilder,
    pool: &PgPool,
    project: uuid::Uuid,
    binary: &str,
    images: &[PlannedImage],
    gate: Arc<dyn BuildGate>,
) -> anyhow::Result<VersionBuild> {
    builder
        .start_images(version(project, binary, images), images, &Staging::new(()), project, TENANT, gate, ImageHold::new(pool, project))
        .await
}

/// The version build a request answered with while its builds run.
fn waiting_on(asked: VersionBuild) -> BuildsUnderway {
    match asked {
        VersionBuild::Waiting(underway) => underway,
        VersionBuild::Ready { .. } => panic!("every image was there: nothing to wait on"),
    }
}

/// A gate that counts entries and records being let go.
#[derive(Default)]
struct Gate {
    begun: AtomicUsize,
    let_go: AtomicBool,
}

#[async_trait::async_trait]
impl BuildGate for Gate {
    async fn begin(&self) -> anyhow::Result<()> {
        self.begun.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn let_go(&self) {
        self.let_go.store(true, Ordering::SeqCst);
    }
}

fn gate() -> Arc<Gate> {
    Arc::new(Gate::default())
}

/// How the row of `image_ref` stands.
async fn status_of(pool: &PgPool, image_ref: &str) -> String {
    sqlx::query_scalar("SELECT status FROM image_build WHERE image_ref = $1")
        .bind(image_ref)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// Where the build `name` of `image_ref` stands, as a caller following it
/// reads it.
async fn build_state(pool: &PgPool, image_ref: &str, name: &str) -> weft_core::projects::BuildStateResponse {
    let states = ledger::image_states(pool, &[ImageBuild { image: image_ref.into(), name: name.into() }]).await.unwrap();
    states.into_iter().next().unwrap().state
}

/// How `project`'s version build `id` stands.
async fn version_state(pool: &PgPool, project: uuid::Uuid, id: uuid::Uuid) -> weft_core::builds::VersionBuildState {
    waiting::state(pool, project, id).await.unwrap().expect("the version build is on record")
}

fn crate_now() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64
}

/// Project rows registered under the test tenant, and the store the build
/// transition and registrations go through.
async fn project_store(pool: &PgPool, ids: &[uuid::Uuid]) -> weft_dispatcher::ProjectStore {
    let projects: weft_dispatcher::ProjectStore = Arc::new(weft_dispatcher::PostgresProjectStore::new(pool.clone()));
    for id in ids {
        let empty = serde_json::from_value(serde_json::json!({ "id": id, "nodes": [], "edges": [] })).unwrap();
        projects.register_with_hashes(empty, "p", "", TENANT, None, None, None, None, None, None, None).await.unwrap();
    }
    projects
}

/// Heartbeats older than this are a request that let go.
fn stale_before() -> i64 {
    crate_now() - weft_dispatcher::transition::heartbeat_stale_secs()
}

async fn transition_of(projects: &weft_dispatcher::ProjectStore, id: uuid::Uuid) -> &'static str {
    projects.transition(id).await.unwrap().unwrap().as_str()
}

async fn running_binary(pool: &PgPool, project: uuid::Uuid) -> Option<String> {
    sqlx::query_scalar("SELECT running_binary_hash FROM project WHERE id = $1").bind(project).fetch_one(pool).await.unwrap()
}

async fn claims_of(pool: &PgPool, claim: uuid::Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM image_claim WHERE claim_id = $1").bind(claim).fetch_one(pool).await.unwrap()
}

/// Images already in the registry are not built, and the gate is never
/// entered: a build of an unchanged project never serializes, and the
/// request registers at once.
#[sqlx::test]
async fn present_images_are_skipped_without_the_gate(pool: PgPool) {
    setup(&pool).await;
    let fake = Arc::new(FakeImageBuilder::new());
    fake.set_image_exists("reg:5000/weft-worker:a");
    let gate = gate();
    let asked = ask(&builder(&pool, fake.clone()), &pool, uuid::Uuid::new_v4(), "a", &[image(ImageKind::Worker, "reg:5000/weft-worker:a")], gate.clone())
        .await
        .unwrap();
    assert!(matches!(asked, VersionBuild::Ready { .. }), "nothing to wait on");
    assert!(fake.starts().is_empty());
    assert_eq!(gate.begun.load(Ordering::SeqCst), 0);
}

/// A stale image is started once, in a lane, and the request answers with
/// the version waiting on it while it still runs, never waiting for its
/// end; the build loop's look records the end later.
#[sqlx::test]
async fn a_stale_image_is_started_and_answered_before_it_ends(pool: PgPool) {
    setup(&pool).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let gate = gate();
    let image_ref = "reg:5000/weft-worker:b";
    let project = uuid::Uuid::new_v4();
    let underway = waiting_on(
        ask(&builder(&pool, fake.clone()), &pool, project, "b", &[image(ImageKind::Worker, image_ref), image(ImageKind::Worker, image_ref)], gate.clone())
            .await
            .unwrap(),
    );
    let starts = fake.starts();
    assert_eq!(starts.len(), 1, "one build per ref, however often the plan names it");
    assert_eq!(underway.images, [ImageBuild { image: image_ref.into(), name: starts[0].name.clone() }]);
    assert_eq!(starts[0].build_args, vec![("WEFT_COMPILE_LANE".to_string(), "0".to_string())]);
    assert_eq!(gate.begun.load(Ordering::SeqCst), 1);
    assert!(gate.let_go.load(Ordering::SeqCst), "let go once the version is written down");
    assert_eq!(status_of(&pool, image_ref).await, "running", "answered while it runs");
    assert_eq!(version_state(&pool, project, underway.build).await.state, VersionBuildStatus::Waiting);
    fake.set_poll_result(&starts[0].name, BuildStatus::Succeeded);
    assert!(follow::look(&pool, fake.as_ref()).await.unwrap());
    assert_eq!(status_of(&pool, image_ref).await, "succeeded");
    assert!(!follow::look(&pool, fake.as_ref()).await.unwrap(), "nothing runs any more");
}

/// A version whose builds all succeed registers by itself on the build
/// loop's next pass: the project runs it, the claim on its images goes,
/// and the caller following it reads it registered, with the program.
#[sqlx::test]
async fn a_waiting_version_registers_by_itself_once_its_builds_succeed(pool: PgPool) {
    setup(&pool).await;
    let project = uuid::Uuid::new_v4();
    let projects = project_store(&pool, &[project]).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let images = [image(ImageKind::Worker, "reg:5000/weft-worker:self"), image(ImageKind::Infra, "reg:5000/weft-infra-db:self")];
    fake.set_image_exists("reg:5000/weft-infra-db:self");
    let underway = waiting_on(ask(&builder(&pool, fake.clone()), &pool, project, "self", &images, gate()).await.unwrap());
    assert_eq!(underway.images.len(), 1, "only the missing image is built");
    assert_eq!(claims_of(&pool, underway.build).await, 2, "the version's claim spares every image it runs");

    let early = waiting::advance(&pool, projects.as_ref()).await.unwrap();
    assert!(early.waiting && early.registered.is_empty(), "nothing registers while a build runs");
    fake.set_poll_result(&underway.images[0].name, BuildStatus::Succeeded);
    follow::look(&pool, fake.as_ref()).await.unwrap();
    let advanced = waiting::advance(&pool, projects.as_ref()).await.unwrap();
    assert_eq!(advanced.registered.iter().map(|s| s.id).collect::<Vec<_>>(), [project]);
    assert!(!advanced.waiting && advanced.failed.is_empty());

    let state = version_state(&pool, project, underway.build).await;
    assert_eq!(state.state, VersionBuildStatus::Registered);
    assert_eq!(state.program.expect("the program registered").binary_hash, "self");
    assert_eq!(state.images[0].state.state, Some(BuildState::Succeeded));
    assert_eq!(running_binary(&pool, project).await.as_deref(), Some("self"));
    assert_eq!(claims_of(&pool, underway.build).await, 0, "the claim goes with the registration");
    let uses: i64 = sqlx::query_scalar("SELECT count(*) FROM image_use WHERE project_id = $1").bind(project).fetch_one(&pool).await.unwrap();
    assert_eq!(uses, 2, "its images are the project's running version");
}

/// A version an image build of which failed ends failed, naming every
/// failed image and why, and never registers.
#[sqlx::test]
async fn a_failed_image_build_ends_its_version_failed(pool: PgPool) {
    setup(&pool).await;
    let project = uuid::Uuid::new_v4();
    let projects = project_store(&pool, &[project]).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let images = [image(ImageKind::Worker, "reg:5000/weft-worker:ok"), image(ImageKind::Infra, "reg:5000/weft-infra-x:bad")];
    let underway = waiting_on(ask(&builder(&pool, fake.clone()), &pool, project, "f", &images, gate()).await.unwrap());
    for build in &underway.images {
        let status = if build.image.contains("bad") {
            BuildStatus::Failed { reason: "cargo: error[E0425]".into() }
        } else {
            BuildStatus::Succeeded
        };
        fake.set_poll_result(&build.name, status);
    }
    follow::look(&pool, fake.as_ref()).await.unwrap();
    let advanced = waiting::advance(&pool, projects.as_ref()).await.unwrap();
    assert!(advanced.registered.is_empty() && !advanced.waiting);
    let state = version_state(&pool, project, underway.build).await;
    assert_eq!(state.state, VersionBuildStatus::Failed);
    let reason = state.reason.expect("why it failed");
    assert!(reason.contains("weft-infra-x:bad") && reason.contains("E0425") && !reason.contains("weft-worker:ok"), "{reason}");
    assert_eq!(running_binary(&pool, project).await, None, "nothing registered");
    assert_eq!(claims_of(&pool, underway.build).await, 0, "its claim goes as it ends");
}

/// A newer ask of another version supersedes the one waiting, and the
/// older one never registers over it, even when its builds end last.
#[sqlx::test]
async fn a_newer_ask_supersedes_the_waiting_version(pool: PgPool) {
    setup(&pool).await;
    let project = uuid::Uuid::new_v4();
    let projects = project_store(&pool, &[project]).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let builder = builder(&pool, fake.clone());
    let older = waiting_on(ask(&builder, &pool, project, "old", &[image(ImageKind::Worker, "reg:5000/weft-worker:old")], gate()).await.unwrap());
    let newer = waiting_on(ask(&builder, &pool, project, "new", &[image(ImageKind::Worker, "reg:5000/weft-worker:new")], gate()).await.unwrap());
    assert_ne!(older.build, newer.build);
    let old_state = version_state(&pool, project, older.build).await;
    assert_eq!(old_state.state, VersionBuildStatus::Superseded);
    assert!(old_state.reason.is_some_and(|r| r.contains("newer build")));
    assert_eq!(claims_of(&pool, older.build).await, 0, "the superseded version's claim goes");

    fake.set_poll_result(&newer.images[0].name, BuildStatus::Succeeded);
    follow::look(&pool, fake.as_ref()).await.unwrap();
    waiting::advance(&pool, projects.as_ref()).await.unwrap();
    assert_eq!(running_binary(&pool, project).await.as_deref(), Some("new"));
    fake.set_poll_result(&older.images[0].name, BuildStatus::Succeeded);
    follow::look(&pool, fake.as_ref()).await.unwrap();
    let late = waiting::advance(&pool, projects.as_ref()).await.unwrap();
    assert!(late.registered.is_empty(), "the superseded version never registers");
    assert_eq!(running_binary(&pool, project).await.as_deref(), Some("new"));

    // Its registration, even attempted, does not land.
    let landed = waiting::register_version(
        projects.as_ref(),
        &pool,
        project,
        TENANT,
        &version(project, "old", &[image(ImageKind::Worker, "reg:5000/weft-worker:old")]),
        Registrar::Loop { version: older.build },
    )
    .await
    .unwrap();
    assert!(landed.is_none());
    assert_eq!(running_binary(&pool, project).await.as_deref(), Some("new"));
}

/// Register `project`'s version `binary` as a request whose ask is `ask`
/// and that found every image; `false` when it must not land.
async fn register_asked(pool: &PgPool, projects: &weft_dispatcher::ProjectStore, project: uuid::Uuid, binary: &str, ask: i64) -> bool {
    let hold = ImageHold::new(pool, project);
    let version = version_asked(project, binary, &[image(ImageKind::Worker, &format!("reg:5000/weft-worker:{binary}"))], ask);
    waiting::register_version(projects.as_ref(), pool, project, TENANT, &version, Registrar::Request { hold })
        .await
        .unwrap()
        .is_some()
}

/// Ask for `project`'s version `binary` as its `ask`th build, which waits
/// on the image build it starts.
async fn wait_asked(pool: &PgPool, fake: &Arc<FakeImageBuilder>, project: uuid::Uuid, binary: &str, ask: i64) -> BuildsUnderway {
    let images = [image(ImageKind::Worker, &format!("reg:5000/weft-worker:{binary}"))];
    waiting_on(
        builder(pool, fake.clone())
            .start_images(version_asked(project, binary, &images, ask), &images, &Staging::new(()), project, TENANT, gate(), ImageHold::new(pool, project))
            .await
            .unwrap(),
    )
}

/// A request that found every image registers at once, and settles the
/// project's waiting version by the order of their asks, never by when
/// either finished: the waiting one ends `registered` when it is the same
/// version, `superseded` when it was asked before, and keeps waiting (to
/// register over this one) when it was asked after.
#[sqlx::test]
async fn an_ask_that_registers_settles_the_waiting_version(pool: PgPool) {
    setup(&pool).await;
    let project = uuid::Uuid::new_v4();
    let projects = project_store(&pool, &[project]).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let early = projects.next_build_ask(project).await.unwrap();
    let late = projects.next_build_ask(project).await.unwrap();
    // The later ask compiled faster and waits; the earlier one finds every
    // image and registers after it.
    let waiting = wait_asked(&pool, &fake, project, "late", late).await;
    assert!(register_asked(&pool, &projects, project, "early", early).await, "nothing newer registered yet");
    assert_eq!(version_state(&pool, project, waiting.build).await.state, VersionBuildStatus::Waiting, "asked after it: still waits");
    fake.set_poll_result(&waiting.images[0].name, BuildStatus::Succeeded);
    follow::look(&pool, fake.as_ref()).await.unwrap();
    waiting::advance(&pool, projects.as_ref()).await.unwrap();
    assert_eq!(running_binary(&pool, project).await.as_deref(), Some("late"), "the newer ask lands over the older");

    // The other order: the waiting version was asked first, so the request
    // that registers supersedes it, and it never lands after.
    let first = projects.next_build_ask(project).await.unwrap();
    let older = wait_asked(&pool, &fake, project, "first", first).await;
    let second = projects.next_build_ask(project).await.unwrap();
    assert!(register_asked(&pool, &projects, project, "second", second).await);
    let state = version_state(&pool, project, older.build).await;
    assert_eq!(state.state, VersionBuildStatus::Superseded);
    assert_eq!(state.reason.as_deref(), Some("a newer build of this project registered first"));
    fake.set_poll_result(&older.images[0].name, BuildStatus::Succeeded);
    follow::look(&pool, fake.as_ref()).await.unwrap();
    assert!(waiting::advance(&pool, projects.as_ref()).await.unwrap().registered.is_empty());
    assert_eq!(running_binary(&pool, project).await.as_deref(), Some("second"));

    // The same version waiting is ended registered by the request.
    let third = projects.next_build_ask(project).await.unwrap();
    let same = wait_asked(&pool, &fake, project, "third", third).await;
    let fourth = projects.next_build_ask(project).await.unwrap();
    assert!(register_asked(&pool, &projects, project, "third", fourth).await);
    let state = version_state(&pool, project, same.build).await;
    assert_eq!(state.state, VersionBuildStatus::Registered, "the same version, registered by the request");
    assert_eq!(state.program.unwrap().binary_hash, "third");
}

/// A version registered from an ask lands nothing older after it: a slow
/// request whose ask came before finds the newer one registered and
/// registers nothing, and a version asked before that only now waits is
/// written down superseded.
#[sqlx::test]
async fn an_older_ask_never_lands_over_a_registered_newer_one(pool: PgPool) {
    setup(&pool).await;
    let project = uuid::Uuid::new_v4();
    let projects = project_store(&pool, &[project]).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let slow = projects.next_build_ask(project).await.unwrap();
    let slower = projects.next_build_ask(project).await.unwrap();
    let newest = projects.next_build_ask(project).await.unwrap();
    let waiting = wait_asked(&pool, &fake, project, "newest", newest).await;
    fake.set_poll_result(&waiting.images[0].name, BuildStatus::Succeeded);
    follow::look(&pool, fake.as_ref()).await.unwrap();
    waiting::advance(&pool, projects.as_ref()).await.unwrap();
    assert_eq!(running_binary(&pool, project).await.as_deref(), Some("newest"));

    assert!(!register_asked(&pool, &projects, project, "slow", slow).await, "the older request registers nothing");
    assert_eq!(running_binary(&pool, project).await.as_deref(), Some("newest"));
    let late = wait_asked(&pool, &fake, project, "slower", slower).await;
    let state = version_state(&pool, project, late.build).await;
    assert_eq!(state.state, VersionBuildStatus::Superseded, "written down superseded already");
    assert_eq!(claims_of(&pool, late.build).await, 0, "and its claim let go");
}

/// A version waiting behind a newer one still waiting is written down
/// superseded, and the newer one keeps waiting.
#[sqlx::test]
async fn an_older_ask_never_supersedes_a_newer_waiting_one(pool: PgPool) {
    setup(&pool).await;
    let project = uuid::Uuid::new_v4();
    let projects = project_store(&pool, &[project]).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let older = projects.next_build_ask(project).await.unwrap();
    let newer = projects.next_build_ask(project).await.unwrap();
    let new = wait_asked(&pool, &fake, project, "newer", newer).await;
    let old = wait_asked(&pool, &fake, project, "older", older).await;
    assert_eq!(version_state(&pool, project, old.build).await.state, VersionBuildStatus::Superseded);
    assert_eq!(version_state(&pool, project, new.build).await.state, VersionBuildStatus::Waiting);
}

/// Two requests that both found every image, registering at once, leave
/// the project on the newer ask's version, whichever commits first.
#[sqlx::test]
async fn two_concurrent_registrations_leave_the_newer_ask(pool: PgPool) {
    setup(&pool).await;
    let project = uuid::Uuid::new_v4();
    let projects = project_store(&pool, &[project]).await;
    for _ in 0..10 {
        let older = projects.next_build_ask(project).await.unwrap();
        let newer = projects.next_build_ask(project).await.unwrap();
        let (a, b) = tokio::join!(
            register_asked(&pool, &projects, project, "old", older),
            register_asked(&pool, &projects, project, "new", newer),
        );
        assert!(b, "the newer ask always lands");
        let _ = a;
        assert_eq!(running_binary(&pool, project).await.as_deref(), Some("new"));
    }
}

/// A waiting version whose stored rows no longer read ends failed with
/// why, rather than keeping the build loop awake and its project building
/// for ever.
#[sqlx::test]
async fn a_waiting_version_that_no_longer_reads_ends_failed(pool: PgPool) {
    setup(&pool).await;
    let (garbled_images, garbled_program) = (uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
    let projects = project_store(&pool, &[garbled_images, garbled_program]).await;
    for (project, program, waits_on) in [
        (garbled_images, serde_json::json!({}), serde_json::json!({ "not": "a list" })),
        (garbled_program, serde_json::json!({ "not": "a program" }), serde_json::json!([])),
    ] {
        assert!(projects.try_begin_building(project, stale_before()).await.unwrap());
        projects.release_build_driver(project).await.unwrap();
        sqlx::query(
            "INSERT INTO version_build (id, ask, project_id, tenant_id, project_name, manifest, program, images, waits_on, \
                                        state, asked_at) \
             VALUES ($1, 1, $1, $2, 'p', '{}', $3, '{}', $4, 'waiting', 0)",
        )
        .bind(project)
        .bind(TENANT)
        .bind(program)
        .bind(waits_on)
        .execute(&pool)
        .await
        .unwrap();
    }
    let advanced = waiting::advance(&pool, projects.as_ref()).await.unwrap();
    assert!(!advanced.waiting && advanced.failed.is_empty() && advanced.registered.is_empty());
    for (project, says) in [(garbled_images, "do not read"), (garbled_program, "does not read")] {
        let (state, reason): (String, Option<String>) =
            sqlx::query_as("SELECT state, reason FROM version_build WHERE id = $1").bind(project).fetch_one(&pool).await.unwrap();
        assert_eq!(state, "failed");
        assert!(reason.as_deref().is_some_and(|r| r.contains(says)), "{reason:?}");
    }
    let mut settled = projects.settle_building(None, stale_before()).await.unwrap();
    settled.sort();
    let mut both = vec![garbled_images, garbled_program];
    both.sort();
    assert_eq!(settled, both, "nothing holds either project any more");
}

/// The build loop forgets version builds that ended over an hour ago, and
/// a removed project's go with it, a waiting one's claim included.
#[sqlx::test]
async fn ended_and_removed_version_builds_are_forgotten(pool: PgPool) {
    setup(&pool).await;
    let (old, removed) = (uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
    let projects = project_store(&pool, &[]).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let ended = waiting_on(ask(&builder(&pool, fake.clone()), &pool, old, "ended", &[image(ImageKind::Worker, "reg:5000/weft-worker:ended")], gate()).await.unwrap());
    builder(&pool, fake.clone()).cancel_project(old).await.unwrap();
    sqlx::query("UPDATE version_build SET ended_at = ended_at - $2 WHERE id = $1")
        .bind(ended.build)
        .bind(waiting::ENDED_KEPT_SECS + 1)
        .execute(&pool)
        .await
        .unwrap();
    waiting::advance(&pool, projects.as_ref()).await.unwrap();
    assert!(waiting::state(&pool, old, ended.build).await.unwrap().is_none(), "an hour after it ended");

    let gone = waiting_on(ask(&builder(&pool, fake.clone()), &pool, removed, "removed", &[image(ImageKind::Worker, "reg:5000/weft-worker:removed")], gate()).await.unwrap());
    waiting::forget_project(&pool, removed).await.unwrap();
    assert!(waiting::state(&pool, removed, gone.build).await.unwrap().is_none());
    assert_eq!(claims_of(&pool, gone.build).await, 0);
}

/// The same version asked again while it waits (a rerun after Ctrl+C)
/// joins it: the same version build, the same image build, never a second
/// start, and the rerun's own claim let go.
#[sqlx::test]
async fn a_rerun_of_the_same_version_joins_it(pool: PgPool) {
    setup(&pool).await;
    let project = uuid::Uuid::new_v4();
    let fake = Arc::new(FakeImageBuilder::new());
    let builder = builder(&pool, fake.clone());
    let images = [image(ImageKind::Worker, "reg:5000/weft-worker:again")];
    let first = waiting_on(ask(&builder, &pool, project, "again", &images, gate()).await.unwrap());
    let hold = ImageHold::new(&pool, project);
    let rerun_claim = hold.id();
    let rerun = waiting_on(
        builder
            .start_images(version(project, "again", &images), &images, &Staging::new(()), project, TENANT, gate(), hold)
            .await
            .unwrap(),
    );
    assert_eq!(rerun.build, first.build, "joined");
    assert_eq!(rerun.images, first.images);
    assert_eq!(fake.starts().len(), 1, "never started twice");
    assert_eq!(claims_of(&pool, rerun_claim).await, 0, "the rerun's own claim goes");
    assert_eq!(claims_of(&pool, first.build).await, 1);
    let waiting: i64 = sqlx::query_scalar("SELECT count(*) FROM version_build WHERE project_id = $1").bind(project).fetch_one(&pool).await.unwrap();
    assert_eq!(waiting, 1);
}

/// A failed build's reason is what a caller following it reads, and a
/// later build of the same ref starts afresh.
#[sqlx::test]
async fn a_failed_build_names_its_reason_and_can_be_retried(pool: PgPool) {
    setup(&pool).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let project = uuid::Uuid::new_v4();
    let image_ref = "reg:5000/weft-infra-x:c";
    let underway = waiting_on(ask(&builder(&pool, fake.clone()), &pool, project, "c", &[image(ImageKind::Infra, image_ref)], gate()).await.unwrap());
    assert!(fake.starts()[0].build_args.is_empty(), "an infra image has no compile lane");
    fake.set_poll_result(&underway.images[0].name, BuildStatus::Failed { reason: "cargo: error[E0425]".into() });
    follow::look(&pool, fake.as_ref()).await.unwrap();
    let state = build_state(&pool, image_ref, &underway.images[0].name).await;
    assert_eq!(state.state, Some(BuildState::Failed));
    assert!(state.reason.as_deref().is_some_and(|r| r.contains("E0425")), "{state:?}");
    let claim = ledger::claim(&pool, fake.as_ref(), image_ref, uuid::Uuid::new_v4(), TENANT, Some(2), 0).await.unwrap();
    assert!(matches!(claim, Claim::Start { .. }), "{claim:?}");
}

/// Once the project's build is cancelled, an image claim is refused: a
/// request claiming after the cancel starts nothing, and fails cancelled.
#[sqlx::test]
async fn a_claim_after_a_cancel_is_refused(pool: PgPool) {
    setup(&pool).await;
    let project = uuid::Uuid::new_v4();
    let projects = project_store(&pool, &[project]).await;
    assert!(projects.try_begin_building(project, stale_before()).await.unwrap());
    assert!(projects.request_cancel_build(project).await.unwrap());
    let fake = Arc::new(FakeImageBuilder::new());
    let Err(e) = ask(&builder(&pool, fake.clone()), &pool, project, "d", &[image(ImageKind::Worker, "reg:5000/weft-worker:d")], gate()).await else {
        panic!("a cancelled build starts nothing")
    };
    assert!(weft_dispatcher::build::cancelled(&e), "{e:#}");
    assert!(fake.starts().is_empty());
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM image_build").fetch_one(&pool).await.unwrap();
    assert_eq!(rows, 0, "nothing claimed");
}

/// A second project asking for an image another one is building joins
/// that build (no second start, the same build answered), and its cancel
/// leaves the other's build running while its own waiting version ends.
#[sqlx::test]
async fn a_shared_build_is_joined_and_never_stopped_by_the_joiner(pool: PgPool) {
    setup(&pool).await;
    let first = uuid::Uuid::new_v4();
    let fake = Arc::new(FakeImageBuilder::new());
    let image_ref = "reg:5000/weft-worker:e";
    let Claim::Start { name, .. } = ledger::claim(&pool, fake.as_ref(), image_ref, first, TENANT, Some(2), crate_now()).await.unwrap() else {
        panic!("the first claim starts")
    };
    let joiner = uuid::Uuid::new_v4();
    let joined = waiting_on(ask(&builder(&pool, fake.clone()), &pool, joiner, "e", &[image(ImageKind::Worker, image_ref)], gate()).await.unwrap());
    assert_eq!(joined.images, [ImageBuild { image: image_ref.into(), name: name.clone() }], "the running build is answered");
    builder(&pool, fake.clone()).cancel_project(joiner).await.unwrap();
    assert!(fake.starts().is_empty(), "joined, not started again");
    assert!(fake.releases().is_empty(), "another project's build is left running");
    assert_eq!(status_of(&pool, image_ref).await, "running");
    assert_eq!(version_state(&pool, joiner, joined.build).await.state, VersionBuildStatus::Cancelled);
}

/// Two projects asking at once for one image start one build and each
/// waits on it; once it ends, both versions register.
#[sqlx::test]
async fn every_waiter_on_one_build_registers_at_its_end(pool: PgPool) {
    setup(&pool).await;
    let (a_project, b_project) = (uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
    let projects = project_store(&pool, &[a_project, b_project]).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let wanted = [image(ImageKind::Worker, "reg:5000/weft-worker:g")];
    let (first, second) = (builder(&pool, fake.clone()), builder(&pool, fake.clone()));
    let (a, b) = tokio::join!(
        ask(&first, &pool, a_project, "g", &wanted, gate()),
        ask(&second, &pool, b_project, "g", &wanted, gate()),
    );
    let (a, b) = (waiting_on(a.unwrap()), waiting_on(b.unwrap()));
    assert_eq!(fake.starts().len(), 1, "one build for both");
    assert_eq!(a.images, b.images, "both wait on the same build");
    fake.set_poll_result(&a.images[0].name, BuildStatus::Succeeded);
    follow::look(&pool, fake.as_ref()).await.unwrap();
    let mut registered: Vec<uuid::Uuid> = waiting::advance(&pool, projects.as_ref()).await.unwrap().registered.iter().map(|s| s.id).collect();
    registered.sort();
    let mut both = vec![a_project, b_project];
    both.sort();
    assert_eq!(registered, both);
    assert!(!fake.releases().is_empty(), "the build is freed once the end is recorded");
}

/// A build nobody follows any more is seen through by the next look: the
/// build loop records its end from the builder's answer.
#[sqlx::test]
async fn a_build_nobody_follows_is_seen_through_by_the_next_look(pool: PgPool) {
    setup(&pool).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let image_ref = "reg:5000/weft-worker:f";
    let Claim::Start { name, .. } = ledger::claim(&pool, fake.as_ref(), image_ref, uuid::Uuid::new_v4(), TENANT, Some(2), crate_now()).await.unwrap() else {
        panic!("the first claim starts")
    };
    assert!(ledger::started(&pool, image_ref, &name, &weft_platform_traits::BuildHandle::named(name.clone())).await.unwrap());
    follow::look(&pool, fake.as_ref()).await.unwrap();
    assert_eq!(status_of(&pool, image_ref).await, "running", "still building");
    fake.set_poll_result(&name, BuildStatus::Succeeded);
    follow::look(&pool, fake.as_ref()).await.unwrap();
    assert_eq!(status_of(&pool, image_ref).await, "succeeded");
    assert_eq!(fake.releases(), vec![name], "freed once its end is recorded");
}

/// A builder that names its builds itself (Cloud Build does) is asked
/// about each under its own name, never the one weft minted: asking under
/// the minted one would find nothing for ever.
#[sqlx::test]
async fn a_build_the_builder_named_is_followed_under_that_name(pool: PgPool) {
    setup(&pool).await;
    let fake = Arc::new(FakeImageBuilder::new());
    fake.name_builds_itself();
    let project = uuid::Uuid::new_v4();
    let underway = waiting_on(ask(&builder(&pool, fake.clone()), &pool, project, "n", &[image(ImageKind::Worker, "reg:5000/weft-worker:n")], gate()).await.unwrap());
    let state = version_state(&pool, project, underway.build).await;
    let current = state.images[0].state.build.clone().expect("the builder's id is recorded");
    assert!(current.ends_with("-id"), "the row names the builder's own id: {current}");
    fake.set_poll_result(&current, BuildStatus::Succeeded);
    follow::look(&pool, fake.as_ref()).await.unwrap();
    assert_eq!(status_of(&pool, "reg:5000/weft-worker:n").await, "succeeded");
}

/// A start whose project no request drives any more (its dispatcher went
/// away mid-start) ends failed by name, and a start that lands on a row
/// no longer its build's (ended or cancelled meanwhile) frees what it made
/// instead of leaving it running with nothing tracking it.
#[sqlx::test]
async fn a_start_that_never_lands_ends_and_a_late_one_is_freed(pool: PgPool) {
    setup(&pool).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let image_ref = "reg:5000/weft-worker:s";
    let project = uuid::Uuid::new_v4();
    let projects = project_store(&pool, &[project]).await;
    assert!(projects.try_begin_building(project, stale_before()).await.unwrap());
    let Claim::Start { name, .. } = ledger::claim(&pool, fake.as_ref(), image_ref, project, TENANT, Some(2), crate_now()).await.unwrap() else {
        panic!("the first claim starts")
    };
    projects.release_build_driver(project).await.unwrap();
    follow::look(&pool, fake.as_ref()).await.unwrap();
    let state = build_state(&pool, image_ref, &name).await;
    assert_eq!(state.state, Some(BuildState::Failed), "a start that never landed ends failed");
    assert!(state.reason.as_deref().is_some_and(|r| r.contains("never made on the builder")), "{state:?}");
    assert!(fake.polls().is_empty(), "a build with no id on the builder is never asked about");
    let handle = weft_platform_traits::BuildHandle::named(name.clone());
    assert!(!ledger::started(&pool, image_ref, &name, &handle).await.unwrap(), "the row is no longer that build's");

    let image_ref = "reg:5000/weft-worker:t";
    let Claim::Start { name, .. } = ledger::claim(&pool, fake.as_ref(), image_ref, uuid::Uuid::new_v4(), TENANT, Some(2), crate_now()).await.unwrap() else {
        panic!("the first claim starts")
    };
    assert_eq!(ledger::cancel(&pool, image_ref, &name, crate_now()).await.unwrap(), Some(None), "cancelled before it was made");
    assert!(!ledger::started(&pool, image_ref, &name, &weft_platform_traits::BuildHandle::named(name.clone())).await.unwrap());
}

/// A builder that keeps failing to answer about a build is given up on:
/// the build ends failed with its words, instead of running for ever.
#[sqlx::test]
async fn a_build_its_builder_never_answers_about_is_given_up(pool: PgPool) {
    setup(&pool).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let image_ref = "reg:5000/weft-worker:u";
    let Claim::Start { name, .. } = ledger::claim(&pool, fake.as_ref(), image_ref, uuid::Uuid::new_v4(), TENANT, Some(2), crate_now()).await.unwrap() else {
        panic!("the first claim starts")
    };
    assert!(ledger::started(&pool, image_ref, &name, &weft_platform_traits::BuildHandle::named(name.clone())).await.unwrap());
    fake.fail_polls(&name);
    follow::look(&pool, fake.as_ref()).await.unwrap();
    assert_eq!(status_of(&pool, image_ref).await, "running", "one unanswered look is looked at again");
    sqlx::query("UPDATE image_build SET failing_since = failing_since - $2 WHERE image_ref = $1")
        .bind(image_ref)
        .bind(ledger::UNANSWERED_GIVE_UP_SECS + 1)
        .execute(&pool)
        .await
        .unwrap();
    follow::look(&pool, fake.as_ref()).await.unwrap();
    let state = build_state(&pool, image_ref, &name).await;
    assert_eq!(state.state, Some(BuildState::Failed), "given up on");
    assert!(state.reason.as_deref().is_some_and(|r| r.contains("has not answered")), "{state:?}");
}

/// A build its builder no longer knows ends failed by name instead of
/// being waited on for ever.
#[sqlx::test]
async fn a_build_its_builder_lost_fails_by_name(pool: PgPool) {
    setup(&pool).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let image_ref = "reg:5000/weft-worker:l";
    let Claim::Start { name, .. } = ledger::claim(&pool, fake.as_ref(), image_ref, uuid::Uuid::new_v4(), TENANT, Some(2), crate_now()).await.unwrap() else {
        panic!("the first claim starts")
    };
    assert!(ledger::started(&pool, image_ref, &name, &weft_platform_traits::BuildHandle::named(name.clone())).await.unwrap());
    fake.set_poll_result(&name, BuildStatus::Gone);
    follow::look(&pool, fake.as_ref()).await.unwrap();
    let state = build_state(&pool, image_ref, &name).await;
    assert_eq!(state.state, Some(BuildState::Failed), "a lost build ends failed");
    assert!(state.reason.as_deref().is_some_and(|r| r.contains(&name) && r.contains("gone")), "{state:?}");
}

/// A project joining a build whose starter is still making it is
/// answered with it at once, and the build loop never calls it gone
/// while its starter still drives it, though the builder knows nothing of
/// it yet.
#[sqlx::test]
async fn a_joined_build_still_being_made_is_never_called_gone(pool: PgPool) {
    setup(&pool).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let image_ref = "reg:5000/weft-worker:j";
    let starter = uuid::Uuid::new_v4();
    let projects = project_store(&pool, &[starter]).await;
    assert!(projects.try_begin_building(starter, stale_before()).await.unwrap());
    let Claim::Start { name, .. } = ledger::claim(&pool, fake.as_ref(), image_ref, starter, TENANT, Some(2), crate_now()).await.unwrap() else {
        panic!("the first claim starts")
    };
    // The starter has not created the process yet: it polls as gone.
    fake.set_poll_result(&name, BuildStatus::Gone);
    let joiner = uuid::Uuid::new_v4();
    let underway = waiting_on(ask(&builder(&pool, fake.clone()), &pool, joiner, "j", &[image(ImageKind::Worker, image_ref)], gate()).await.unwrap());
    assert_eq!(underway.images[0].name, name);
    follow::look(&pool, fake.as_ref()).await.unwrap();
    assert_eq!(status_of(&pool, image_ref).await, "running", "not gone while its start may still land");
    let state = version_state(&pool, joiner, underway.build).await;
    assert_eq!((state.images[0].state.state, state.images[0].state.build.clone()), (Some(BuildState::Running), None), "followed before the builder named it");
}

/// A verb that found the image missing while another verb's build of it
/// was running, and claims it only once that build succeeded, builds
/// nothing: the image is there.
#[sqlx::test]
async fn a_claim_after_the_build_succeeded_builds_nothing(pool: PgPool) {
    setup(&pool).await;
    let fake = FakeImageBuilder::new();
    let image_ref = "reg:5000/weft-worker:k";
    let Claim::Start { name, .. } = ledger::claim(&pool, &fake, image_ref, uuid::Uuid::new_v4(), TENANT, Some(2), crate_now()).await.unwrap() else {
        panic!("the first claim starts")
    };
    ledger::finish(&pool, image_ref, &name, ledger::Outcome::Succeeded, crate_now()).await.unwrap();
    fake.set_image_exists(image_ref);
    let late = ledger::claim(&pool, &fake, image_ref, uuid::Uuid::new_v4(), TENANT, Some(2), crate_now()).await.unwrap();
    assert!(matches!(late, Claim::Built), "{late:?}");
}

/// An image the ledger records as built but the registry no longer holds
/// (deleted by hand, or by the registry's cleanup policy) is built again
/// instead of skipped for ever.
#[sqlx::test]
async fn a_built_image_the_registry_lost_is_built_again(pool: PgPool) {
    setup(&pool).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let image_ref = "reg:5000/weft-worker:lost";
    let Claim::Start { name, .. } = ledger::claim(&pool, fake.as_ref(), image_ref, uuid::Uuid::new_v4(), TENANT, Some(2), crate_now()).await.unwrap() else {
        panic!("the first claim starts")
    };
    ledger::finish(&pool, image_ref, &name, ledger::Outcome::Succeeded, crate_now()).await.unwrap();
    let underway = waiting_on(ask(&builder(&pool, fake.clone()), &pool, uuid::Uuid::new_v4(), "lost", &[image(ImageKind::Worker, image_ref)], gate()).await.unwrap());
    let starts = fake.starts();
    assert_eq!(starts.len(), 1, "built again");
    assert_ne!(starts[0].name, name, "under a new build");
    assert_eq!(underway.images, [ImageBuild { image: image_ref.into(), name: starts[0].name.clone() }]);
    assert_eq!(status_of(&pool, image_ref).await, "running");
}

/// Racing claims of one ref start it exactly once.
#[sqlx::test]
async fn racing_claims_start_one_build(pool: PgPool) {
    setup(&pool).await;
    let mut claims = Vec::new();
    for _ in 0..20 {
        let p = pool.clone();
        claims.push(tokio::spawn(async move {
            ledger::claim(&p, &FakeImageBuilder::new(), "reg:5000/weft-worker:g", uuid::Uuid::new_v4(), TENANT, Some(4), 0).await.unwrap()
        }));
    }
    let mut starts = 0;
    for c in claims {
        if matches!(c.await.unwrap(), Claim::Start { .. }) {
            starts += 1;
        }
    }
    assert_eq!(starts, 1);
}

/// A prune spares an image a build in progress claims (it found the
/// image and has not registered it yet), and takes it once the claim is
/// let go, without waiting on any other build.
#[sqlx::test]
async fn a_prune_spares_an_image_a_build_claims(pool: PgPool) {
    setup(&pool).await;
    let fake = FakeImageBuilder::new();
    let runner = weft_platform_traits::FakeRunner::new("http://w");
    let image_ref = "reg:5000/weft-worker:claimed".to_string();
    ledger::note_shared(&pool, &image_ref, crate_now()).await.unwrap();
    fake.set_image_exists(&image_ref);
    let hold = ImageHold::new(&pool, uuid::Uuid::new_v4());
    hold.claim(std::slice::from_ref(&image_ref)).await.unwrap();
    // Another build holds an unrelated claim the whole time.
    let other = ImageHold::new(&pool, uuid::Uuid::new_v4());
    other.claim(&["reg:5000/weft-worker:elsewhere".to_string()]).await.unwrap();
    let none = KeepSet::default();
    let candidates = std::slice::from_ref(&image_ref);
    let spared = weft_dispatcher::build::prune::prune(&pool, &fake, &runner, candidates, &none).await.unwrap();
    assert_eq!(spared.in_use, std::slice::from_ref(&image_ref));
    assert!(fake.deletes().is_empty());
    hold.confirm().await.unwrap();
    hold.release().await.unwrap();
    let taken = weft_dispatcher::build::prune::prune(&pool, &fake, &runner, candidates, &none).await.unwrap();
    assert_eq!(taken.removed, std::slice::from_ref(&image_ref));
    assert!(ledger::built_images(&pool, None).await.unwrap().is_empty(), "forgotten with it");
    other.release().await.unwrap();
}

/// The images a waiting version runs are spared by a prune for as long as
/// it waits, however long ago its request's lease ran out, and are a
/// prune's to judge once it ended.
#[sqlx::test]
async fn a_waiting_versions_images_are_spared_until_it_ends(pool: PgPool) {
    setup(&pool).await;
    let project = uuid::Uuid::new_v4();
    let fake = Arc::new(FakeImageBuilder::new());
    let runner = weft_platform_traits::FakeRunner::new("http://w");
    let found = "reg:5000/weft-worker:found".to_string();
    ledger::note_shared(&pool, &found, crate_now()).await.unwrap();
    fake.set_image_exists(&found);
    let images = [image(ImageKind::Worker, &found), image(ImageKind::Infra, "reg:5000/weft-infra-db:building")];
    let underway = waiting_on(ask(&builder(&pool, fake.clone()), &pool, project, "spared", &images, gate()).await.unwrap());
    // As if the request answered long ago: its lease ran out.
    sqlx::query("UPDATE image_claim SET holder_until = 0").execute(&pool).await.unwrap();
    let candidates = std::slice::from_ref(&found);
    let spared = weft_dispatcher::build::prune::prune(&pool, fake.as_ref(), &runner, candidates, &KeepSet::default()).await.unwrap();
    assert_eq!(spared.in_use, std::slice::from_ref(&found), "spared while the version waits");
    assert_eq!(claims_of(&pool, underway.build).await, 2, "and its claim is not swept as lapsed");
    builder(&pool, fake.clone()).cancel_project(project).await.unwrap();
    let taken = weft_dispatcher::build::prune::prune(&pool, fake.as_ref(), &runner, candidates, &KeepSet::default()).await.unwrap();
    assert_eq!(taken.removed, std::slice::from_ref(&found), "a prune's once the version ended");
}

/// A prune whose keep-set was read before a build registered an image as
/// running must not delete it: the build claimed the image, registered it
/// and let go of the claim, all between the keep-set read and the prune
/// reaching that image, so only the re-check under the image's lock sees
/// that it runs now.
#[sqlx::test]
async fn a_prune_spares_an_image_registered_after_its_keep_set(pool: PgPool) {
    setup(&pool).await;
    let fake = FakeImageBuilder::new();
    let runner = weft_platform_traits::FakeRunner::new("http://w");
    let image_ref = "reg:5000/weft-worker:registered".to_string();
    ledger::note_shared(&pool, &image_ref, crate_now()).await.unwrap();
    let stale = weft_dispatcher::build::prune::keep_set(&mut pool.acquire().await.unwrap(), ImageScope::All)
        .await
        .unwrap();
    assert!(!stale.worker_hashes.contains("registered"), "read before the build registers it");
    let hold = ImageHold::new(&pool, uuid::Uuid::new_v4());
    hold.claim(std::slice::from_ref(&image_ref)).await.unwrap();
    sqlx::query(
        "INSERT INTO project (id, name, status, project_json, updated_at, tenant_id, running_binary_hash) \
         VALUES ($1, 'p', 'inactive', '{}', 0, 't', 'registered')",
    )
    .bind(uuid::Uuid::new_v4())
    .execute(&pool)
    .await
    .unwrap();
    hold.confirm().await.unwrap();
    hold.release().await.unwrap();
    let report =
        weft_dispatcher::build::prune::prune(&pool, &fake, &runner, std::slice::from_ref(&image_ref), &stale).await.unwrap();
    assert!(report.removed.is_empty() && report.failed.is_empty(), "{report:?}");
    assert!(fake.deletes().is_empty(), "the running image stays");
    assert_eq!(ledger::built_images(&pool, None).await.unwrap(), [image_ref]);
}

/// A build that failed or was cancelled after pushing its image leaves
/// that image in the registry, and a prune takes it like any other; a
/// failed build that pushed nothing is only forgotten. A build running
/// for an image is never pruned, whatever its row said before.
#[sqlx::test]
async fn a_prune_takes_what_failed_and_cancelled_builds_left(pool: PgPool) {
    setup(&pool).await;
    let fake = FakeImageBuilder::new();
    let runner = weft_platform_traits::FakeRunner::new("http://w");
    let end = |image_ref: &'static str, outcome: ledger::Outcome| {
        let (pool, fake) = (pool.clone(), &fake);
        async move {
            let Claim::Start { name, .. } = ledger::claim(&pool, fake, image_ref, uuid::Uuid::new_v4(), TENANT, Some(2), crate_now()).await.unwrap() else {
                panic!("the first claim starts")
            };
            ledger::finish(&pool, image_ref, &name, outcome, crate_now()).await.unwrap();
        }
    };
    let (pushed, cancelled, nothing) = ("reg:5000/weft-worker:pushed", "reg:5000/weft-worker:cancelled", "reg:5000/weft-worker:nothing");
    end(pushed, ledger::Outcome::Failed("a step after the push failed".into())).await;
    end(cancelled, ledger::Outcome::Cancelled).await;
    end(nothing, ledger::Outcome::Failed("cargo: error[E0425]".into())).await;
    fake.set_image_exists(pushed);
    fake.set_image_exists(cancelled);
    let running = "reg:5000/weft-worker:running";
    let Claim::Start { .. } = ledger::claim(&pool, &fake, running, uuid::Uuid::new_v4(), TENANT, Some(2), crate_now()).await.unwrap() else {
        panic!("the first claim starts")
    };
    let candidates = ledger::built_images(&pool, None).await.unwrap();
    assert_eq!(candidates, [cancelled, nothing, pushed], "every ended build, none running");
    let mut with_running = candidates.clone();
    with_running.push(running.to_string());
    let report = weft_dispatcher::build::prune::prune(&pool, &fake, &runner, &with_running, &KeepSet::default()).await.unwrap();
    assert_eq!(report.removed, [cancelled, pushed], "only what the registry held is reported removed: {report:?}");
    assert_eq!(report.in_use, [running], "a running build is spared");
    assert_eq!(fake.deletes(), [cancelled, nothing, pushed], "a delete of an image never pushed deletes nothing, and is no error");
    assert!(ledger::built_images(&pool, None).await.unwrap().is_empty(), "every ended row is forgotten");
    assert_eq!(status_of(&pool, running).await, "running");
}

/// The build transition outlives the request that entered it: once the
/// request let go, the version waiting on its builds holds it, a settle
/// leaves it while that version waits (a build ending is not enough), and
/// the first settle after the version registered lands it at rest. A
/// project waiting on a build another project started is held the same
/// way.
#[sqlx::test]
async fn a_waiting_version_holds_the_transition_until_it_registers(pool: PgPool) {
    setup(&pool).await;
    let (own, joiner) = (uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
    let projects = project_store(&pool, &[own, joiner]).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let images = [image(ImageKind::Worker, "reg:5000/weft-worker:held")];

    assert!(projects.try_begin_building(own, stale_before()).await.unwrap());
    let underway = waiting_on(ask(&builder(&pool, fake.clone()), &pool, own, "held", &images, gate()).await.unwrap());
    assert!(projects.settle_building(None, stale_before()).await.unwrap().is_empty(), "a live request holds it");
    projects.release_build_driver(own).await.unwrap();

    assert!(projects.try_begin_building(joiner, stale_before()).await.unwrap());
    let joined = waiting_on(ask(&builder(&pool, fake.clone()), &pool, joiner, "held", &images, gate()).await.unwrap());
    assert_eq!(joined.images, underway.images);
    projects.release_build_driver(joiner).await.unwrap();

    fake.set_poll_result(&underway.images[0].name, BuildStatus::Succeeded);
    follow::look(&pool, fake.as_ref()).await.unwrap();
    assert!(projects.settle_building(None, stale_before()).await.unwrap().is_empty(), "the build ended, but both versions wait");
    assert_eq!(transition_of(&projects, own).await, "building");

    waiting::advance(&pool, projects.as_ref()).await.unwrap();
    let mut settled = projects.settle_building(None, stale_before()).await.unwrap();
    settled.sort();
    let mut both = vec![own, joiner];
    both.sort();
    assert_eq!(settled, both, "both registered: both at rest");
    assert_eq!(transition_of(&projects, own).await, "none");
}

/// A request asking again while its version waits enters the transition
/// and joins it; a request still starting builds keeps every other out.
#[sqlx::test]
async fn a_request_joins_a_waiting_version_but_never_a_live_start(pool: PgPool) {
    setup(&pool).await;
    let id = uuid::Uuid::new_v4();
    let projects = project_store(&pool, &[id]).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let images = [image(ImageKind::Worker, "reg:5000/weft-worker:again")];
    assert!(projects.try_begin_building(id, stale_before()).await.unwrap());
    assert!(!projects.try_begin_building(id, stale_before()).await.unwrap(), "a live start keeps the next out");
    let first = waiting_on(ask(&builder(&pool, fake.clone()), &pool, id, "again", &images, gate()).await.unwrap());
    projects.release_build_driver(id).await.unwrap();
    assert!(projects.try_begin_building(id, stale_before()).await.unwrap(), "only its version holds it: a new request enters");
    let again = waiting_on(ask(&builder(&pool, fake.clone()), &pool, id, "again", &images, gate()).await.unwrap());
    assert_eq!(again.build, first.build, "the waiting version is answered again");
    assert_eq!(fake.starts().len(), 1, "and its build never started twice");
}

/// A cancel stops the builds the project started, leaves the ones another
/// project started running, ends its waiting version cancelled, and the
/// transition lands at rest.
#[sqlx::test]
async fn a_cancel_stops_own_builds_and_ends_the_waiting_version(pool: PgPool) {
    setup(&pool).await;
    let (id, other) = (uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
    let projects = project_store(&pool, &[id]).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let (mine, theirs) = ("reg:5000/weft-worker:mine", "reg:5000/weft-worker:theirs");
    let Claim::Start { .. } = ledger::claim(&pool, fake.as_ref(), theirs, other, TENANT, Some(2), crate_now()).await.unwrap() else {
        panic!("the first claim starts")
    };
    assert!(projects.try_begin_building(id, stale_before()).await.unwrap());
    let underway = waiting_on(
        ask(&builder(&pool, fake.clone()), &pool, id, "mine", &[image(ImageKind::Worker, mine), image(ImageKind::Worker, theirs)], gate())
            .await
            .unwrap(),
    );
    assert_eq!(underway.images.len(), 2);
    projects.release_build_driver(id).await.unwrap();

    assert!(projects.request_cancel_build(id).await.unwrap());
    assert!(projects.request_cancel_build(id).await.unwrap(), "a cancel asked twice is the same cancel");
    builder(&pool, fake.clone()).cancel_project(id).await.unwrap();
    assert_eq!(status_of(&pool, mine).await, "cancelled");
    assert_eq!(fake.releases().len(), 1, "only its own build is freed");
    assert_eq!(status_of(&pool, theirs).await, "running", "the other project's build goes on");
    let state = version_state(&pool, id, underway.build).await;
    assert_eq!(state.state, VersionBuildStatus::Cancelled);
    assert_eq!(claims_of(&pool, underway.build).await, 0, "its claim goes with it");
    assert_eq!(projects.settle_building(Some(id), stale_before()).await.unwrap(), [id]);
    assert_eq!(transition_of(&projects, id).await, "none");
    assert!(!projects.request_cancel_build(id).await.unwrap(), "nothing left to cancel");
}

/// A build announces itself to the build loop the moment it is claimed,
/// before it is made on the builder: an install whose loop was asleep
/// wakes, so a build whose starter went away mid-start is ended once its
/// project's heartbeat stops, and its project leaves the build transition
/// instead of staying building for ever.
#[sqlx::test]
async fn a_claimed_build_wakes_the_loop_and_a_dead_start_lets_its_project_go(pool: PgPool) {
    setup(&pool).await;
    let id = uuid::Uuid::new_v4();
    let projects = project_store(&pool, &[id]).await;
    let fake = FakeImageBuilder::new();
    let image_ref = "reg:5000/weft-worker:dead-start";
    let mut listener = sqlx::postgres::PgListener::connect_with(&pool).await.unwrap();
    listener.listen(follow::BUILD_CLAIMED_CHANNEL).await.unwrap();
    assert!(projects.try_begin_building(id, stale_before()).await.unwrap());
    let Claim::Start { name, .. } = ledger::claim(&pool, &fake, image_ref, id, TENANT, Some(2), crate_now()).await.unwrap() else {
        panic!("the first claim starts")
    };
    let woke = tokio::time::timeout(std::time::Duration::from_secs(10), listener.recv()).await.expect("the claim announces itself").unwrap();
    assert_eq!(woke.payload(), image_ref);
    // Its starter dies here: the request's heartbeat stops, and nothing
    // ever records the build on the builder or writes its version down.
    projects.release_build_driver(id).await.unwrap();
    assert!(!follow::look(&pool, &fake).await.unwrap(), "the woken loop ends the build, and nothing runs after");
    let state = build_state(&pool, image_ref, &name).await;
    assert_eq!(state.state, Some(BuildState::Failed), "ended once nobody drives its start");
    assert_eq!(projects.settle_building(Some(id), stale_before()).await.unwrap(), [id]);
    assert_eq!(transition_of(&projects, id).await, "none");
}

/// A version written down as waiting wakes the build loop as it commits,
/// so one whose builds already ended registers at once.
#[sqlx::test]
async fn a_waiting_version_wakes_the_loop(pool: PgPool) {
    setup(&pool).await;
    let id = uuid::Uuid::new_v4();
    let fake = Arc::new(FakeImageBuilder::new());
    let mut listener = sqlx::postgres::PgListener::connect_with(&pool).await.unwrap();
    listener.listen(follow::BUILD_CLAIMED_CHANNEL).await.unwrap();
    let image_ref = "reg:5000/weft-worker:wakes";
    let underway = waiting_on(ask(&builder(&pool, fake.clone()), &pool, id, "wakes", &[image(ImageKind::Worker, image_ref)], gate()).await.unwrap());
    let mut payloads = Vec::new();
    while payloads.len() < 2 {
        let woke = tokio::time::timeout(std::time::Duration::from_secs(10), listener.recv()).await.expect("announced").unwrap();
        payloads.push(woke.payload().to_string());
    }
    assert_eq!(payloads, [image_ref.to_string(), underway.build.to_string()], "the claim, then the version");
}

/// A build request that fails after joining a build another project
/// started lets go of its claim, writes no version down, and its project
/// lands at rest while that build runs on. A request abandoned midway (its
/// hold dropped) lets go the same way.
#[sqlx::test]
async fn a_failed_request_lets_go_of_its_claim_and_its_project(pool: PgPool) {
    setup(&pool).await;
    let (id, other) = (uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
    let projects = project_store(&pool, &[id]).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let image_ref = "reg:5000/weft-worker:theirs-still";
    let Claim::Start { name, .. } = ledger::claim(&pool, fake.as_ref(), image_ref, other, TENANT, Some(2), crate_now()).await.unwrap() else {
        panic!("the first claim starts")
    };
    assert!(ledger::started(&pool, image_ref, &name, &weft_platform_traits::BuildHandle::named(name.clone())).await.unwrap());

    assert!(projects.try_begin_building(id, stale_before()).await.unwrap());
    let hold = ImageHold::new(&pool, id);
    let claim = hold.id();
    // The person cancels before the request claims its image: the claim is
    // refused.
    let images = [image(ImageKind::Worker, image_ref)];
    let (builder, staging) = (builder(&pool, fake.clone()), Staging::new(()));
    let started = builder.start_images(version(id, "x", &images), &images, &staging, id, TENANT, gate(), hold);
    assert!(projects.request_cancel_build(id).await.unwrap());
    let e = started.await.expect_err("the request fails");
    assert!(weft_dispatcher::build::cancelled(&e), "{e:#}");
    projects.release_build_driver(id).await.unwrap();
    let claims = || async {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM image_claim WHERE project_id = $1").bind(id).fetch_one(&pool).await.unwrap()
    };
    for _ in 0..500 {
        if claims().await == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(claims().await, 0, "the failed request's claim goes with it");
    assert!(waiting::state(&pool, id, claim).await.unwrap().is_none(), "no version written down");
    assert_eq!(projects.settle_building(Some(id), stale_before()).await.unwrap(), [id], "at rest while the build runs on");
    assert_eq!(status_of(&pool, image_ref).await, "running");

    let abandoned = ImageHold::new(&pool, id);
    abandoned.claim(&[image_ref.to_string()]).await.unwrap();
    drop(abandoned);
    for _ in 0..500 {
        if claims().await == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(claims().await, 0, "an abandoned request's claim goes with it");
}

/// A start whose project's heartbeat went stale (its request's dispatcher
/// died between the claim and recording the build on the builder) is
/// ended by the next look, and its project settles at once instead of
/// staying building; a start whose project a live request still drives is
/// left alone, though the builder knows nothing of it yet either.
#[sqlx::test]
async fn a_start_nobody_drives_ends_at_once_and_a_driven_one_is_left(pool: PgPool) {
    setup(&pool).await;
    let (live, dead) = (uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
    let projects = project_store(&pool, &[live, dead]).await;
    let fake = FakeImageBuilder::new();
    let (live_ref, dead_ref) = ("reg:5000/weft-worker:live-start", "reg:5000/weft-worker:dead-start-2");
    let mut names = Vec::new();
    for (project, image_ref) in [(live, live_ref), (dead, dead_ref)] {
        assert!(projects.try_begin_building(project, stale_before()).await.unwrap());
        let Claim::Start { name, .. } = ledger::claim(&pool, &fake, image_ref, project, TENANT, Some(2), crate_now()).await.unwrap() else {
            panic!("the first claim starts")
        };
        names.push(name);
    }
    // The dead project's request stopped bumping a while ago, without ever
    // letting go.
    sqlx::query("UPDATE project SET transition_heartbeat_unix = $2 WHERE id = $1")
        .bind(dead)
        .bind(stale_before() - 1)
        .execute(&pool)
        .await
        .unwrap();
    assert!(follow::look(&pool, &fake).await.unwrap());
    let ended = build_state(&pool, dead_ref, &names[1]).await;
    assert_eq!(ended.state, Some(BuildState::Failed), "{ended:?}");
    assert!(ended.reason.as_deref().is_some_and(|r| r.contains(&names[1]) && r.contains("never made on the builder")), "{ended:?}");
    assert_eq!(status_of(&pool, live_ref).await, "running", "a driven start is left to its starter");
    assert!(fake.polls().is_empty(), "neither has an id on the builder to ask about");
    assert_eq!(projects.settle_building(None, stale_before()).await.unwrap(), [dead], "only the dead start's project settles");
    assert_eq!(transition_of(&projects, live).await, "building");
}

/// A request taking over the build transition from one that died
/// mid-start ends that dead one's starts as it enters, so its own claim
/// starts the image again under a new name rather than joining a start
/// nothing will finish; the dead start, should it ever land, finds the row
/// no longer its own. Another project joins the new start.
#[sqlx::test]
async fn a_takeover_ends_the_lost_starts_it_takes_over(pool: PgPool) {
    setup(&pool).await;
    let (id, other) = (uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
    let projects = project_store(&pool, &[id]).await;
    let fake = FakeImageBuilder::new();
    let image_ref = "reg:5000/weft-worker:retaken";
    assert!(projects.try_begin_building(id, stale_before()).await.unwrap());
    let Claim::Start { name: lost, .. } = ledger::claim(&pool, &fake, image_ref, id, TENANT, Some(2), crate_now()).await.unwrap() else {
        panic!("the first claim starts")
    };
    // The request died: its heartbeat went stale, and the next one enters.
    sqlx::query("UPDATE project SET transition_heartbeat_unix = $2 WHERE id = $1").bind(id).bind(stale_before() - 1).execute(&pool).await.unwrap();
    assert!(projects.try_begin_building(id, stale_before()).await.unwrap());
    let ended = build_state(&pool, image_ref, &lost).await;
    assert_eq!(ended.state, Some(BuildState::Failed), "ended as the new request entered: {ended:?}");
    let again = ledger::claim(&pool, &fake, image_ref, id, TENANT, Some(2), crate_now()).await.unwrap();
    let Claim::Start { name: fresh, .. } = again else { panic!("started again, not joined: {again:?}") };
    assert_ne!(fresh, lost);
    assert!(!ledger::started(&pool, image_ref, &lost, &weft_platform_traits::BuildHandle::named(lost.clone())).await.unwrap());
    let joined = ledger::claim(&pool, &fake, image_ref, other, TENANT, Some(2), crate_now()).await.unwrap();
    assert_eq!(joined, Claim::Join { name: fresh, ours: false });
}

/// A builder whose `start` waits until it is let through, so a test can
/// drop the request in the middle of one.
struct HeldStart {
    inner: Arc<FakeImageBuilder>,
    entered: tokio::sync::Notify,
    go: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl ImageBuilder for HeldStart {
    fn image_ref(&self, tag: &str) -> String {
        self.inner.image_ref(tag)
    }
    async fn start(&self, req: weft_platform_traits::BuildRequest) -> anyhow::Result<weft_platform_traits::BuildHandle> {
        self.entered.notify_one();
        self.go.notified().await;
        self.inner.start(req).await
    }
    async fn poll(&self, handle: &weft_platform_traits::BuildHandle) -> anyhow::Result<BuildStatus> {
        self.inner.poll(handle).await
    }
    async fn image_exists(&self, image_ref: &str) -> anyhow::Result<bool> {
        self.inner.image_exists(image_ref).await
    }
    async fn release(&self, handle: &weft_platform_traits::BuildHandle) {
        self.inner.release(handle).await
    }
    async fn delete_image(&self, image_ref: &str) -> anyhow::Result<weft_platform_traits::ImageDeleted> {
        self.inner.delete_image(image_ref).await
    }
}

/// Wait, a bounded while, for `ready` to hold.
async fn until<F: std::future::Future<Output = bool>>(mut ready: impl FnMut() -> F) {
    for _ in 0..500 {
        if ready().await {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

/// A request dropped in the middle of a start (its caller went away) does
/// not take the start with it: the start goes on, makes the build, records
/// the builder's id and writes the version down as waiting on it, so the
/// install still registers it once it is built. The gate is let go once
/// that is done, with nobody waiting.
#[sqlx::test]
async fn a_start_finishes_when_its_request_is_dropped(pool: PgPool) {
    setup(&pool).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let held = Arc::new(HeldStart { inner: fake.clone(), entered: Default::default(), go: Default::default() });
    let project = uuid::Uuid::new_v4();
    let image_ref = "reg:5000/weft-worker:dropped";
    let gate = gate();
    let request = tokio::spawn({
        let (builder, gate, pool) = (builder_on(&pool, held.clone()), gate.clone(), pool.clone());
        async move { ask(&builder, &pool, project, "dropped", &[image(ImageKind::Worker, image_ref)], gate).await }
    });
    held.entered.notified().await;
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled(), "the request is gone");
    held.go.notify_one();
    until(|| async { gate.let_go.load(Ordering::SeqCst) }).await;
    let starts = fake.starts();
    assert_eq!(starts.len(), 1, "the build was made");
    let builder_id: Option<String> =
        sqlx::query_scalar("SELECT builder_id FROM image_build WHERE image_ref = $1").bind(image_ref).fetch_one(&pool).await.unwrap();
    assert_eq!(builder_id, Some(starts[0].name.clone()), "and its id on the builder recorded");
    assert_eq!(status_of(&pool, image_ref).await, "running");
    assert!(fake.releases().is_empty(), "nothing freed: the build loop follows it");
    let waiting: Vec<String> =
        sqlx::query_scalar("SELECT state FROM version_build WHERE project_id = $1").bind(project).fetch_all(&pool).await.unwrap();
    assert_eq!(waiting, ["waiting"], "the version waits on it with nobody asking");
    assert!(gate.let_go.load(Ordering::SeqCst), "the gate is let go once the start is done, with nobody waiting");
}

/// A cancel that comes while the starts run is honoured even when the
/// request that started them is gone: the version is refused, and what
/// the start made is stopped, as it would be with the request there.
#[sqlx::test]
async fn a_cancel_during_a_start_whose_request_is_dropped_stops_its_build(pool: PgPool) {
    setup(&pool).await;
    let project = uuid::Uuid::new_v4();
    let projects = project_store(&pool, &[project]).await;
    assert!(projects.try_begin_building(project, stale_before()).await.unwrap());
    let fake = Arc::new(FakeImageBuilder::new());
    let held = Arc::new(HeldStart { inner: fake.clone(), entered: Default::default(), go: Default::default() });
    let image_ref = "reg:5000/weft-worker:dropped-cancel";
    let gate = gate();
    let request = tokio::spawn({
        let (builder, gate, pool) = (builder_on(&pool, held.clone()), gate.clone(), pool.clone());
        async move { ask(&builder, &pool, project, "dropped-cancel", &[image(ImageKind::Worker, image_ref)], gate).await }
    });
    held.entered.notified().await;
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled(), "the request is gone");
    assert!(projects.request_cancel_build(project).await.unwrap());
    held.go.notify_one();
    until(|| async { gate.let_go.load(Ordering::SeqCst) }).await;
    assert_eq!(status_of(&pool, image_ref).await, "cancelled", "the cancel reached the build");
    assert_eq!(fake.releases().len(), 1, "and the build on the builder was freed");
    let versions: i64 = sqlx::query_scalar("SELECT count(*) FROM version_build").fetch_one(&pool).await.unwrap();
    assert_eq!(versions, 0, "no version written down after the cancel");
}

/// A request whose claim on its images lapsed while it started its
/// builds (its dispatcher could not renew it in time, so a prune may have
/// taken an image it found) fails loudly, writes no version down, and
/// stops what it started.
#[sqlx::test]
async fn a_lapsed_claim_is_never_passed_to_a_waiting_version(pool: PgPool) {
    setup(&pool).await;
    let project = uuid::Uuid::new_v4();
    let fake = Arc::new(FakeImageBuilder::new());
    let held = Arc::new(HeldStart { inner: fake.clone(), entered: Default::default(), go: Default::default() });
    let image_ref = "reg:5000/weft-worker:lapsed";
    let request = tokio::spawn({
        let (builder, pool) = (builder_on(&pool, held.clone()), pool.clone());
        async move { ask(&builder, &pool, project, "lapsed", &[image(ImageKind::Worker, image_ref)], gate()).await }
    });
    held.entered.notified().await;
    sqlx::query("UPDATE image_claim SET holder_until = 0").execute(&pool).await.unwrap();
    held.go.notify_one();
    let e = request.await.unwrap().expect_err("the request fails");
    assert!(format!("{e:#}").contains("lapsed"), "{e:#}");
    let versions: i64 = sqlx::query_scalar("SELECT count(*) FROM version_build").fetch_one(&pool).await.unwrap();
    assert_eq!(versions, 0, "no version written down");
    assert_eq!(status_of(&pool, image_ref).await, "cancelled", "what it started is stopped");
}
