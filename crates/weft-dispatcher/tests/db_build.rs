//! A version build's image half against a real database: the ledger that
//! lets any dispatcher find, join, or adopt a build, driven through the
//! builder with a fake `ImageBuilder`.
//!
//! Same rig as `db_versions.rs`: `#[sqlx::test]` hands each test a fresh
//! database; the dispatcher's whole schema is applied as a boot does.
#![cfg(feature = "db-tests")]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use sqlx::PgPool;
use weft_compiler::build_plan::{ImageKind, PlannedImage};
use weft_dispatcher::build::ledger::{self, Claim};
use weft_dispatcher::build::prune::{ImageHold, ImageScope, KeepSet};
use weft_dispatcher::build::{BuildGate, VersionBuilder};
use weft_platform_traits::{BuildStatus, FakeImageBuilder};

async fn setup(pool: &PgPool) {
    weft_dispatcher::app::apply_core_schema(pool).await.expect("core schema");
}

fn builder(pool: &PgPool, images: Arc<FakeImageBuilder>, replica: &str) -> VersionBuilder {
    VersionBuilder {
        bases: weft_compiler::worker_image::BaseImages {
            builder: "reg:5000/base:1".into(),
            runtime: "debian:bookworm-slim".into(),
        },
        images,
        pool: pool.clone(),
        replica: replica.into(),
        compile_lanes: 2,
        poll_every: std::time::Duration::from_millis(10),
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

/// A gate that counts entries and answers a cancel when told to.
#[derive(Default)]
struct Gate {
    begun: AtomicUsize,
    cancel: AtomicBool,
}

#[async_trait::async_trait]
impl BuildGate for Gate {
    async fn begin(&self) -> anyhow::Result<()> {
        self.begun.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn cancel_requested(&self) -> anyhow::Result<bool> {
        Ok(self.cancel.load(Ordering::SeqCst))
    }
}

/// Answer the only started build with `status` once it exists.
fn finish_started(fake: Arc<FakeImageBuilder>, status: BuildStatus) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if let Some(started) = fake.starts().first() {
                fake.set_poll_result(&started.name, status.clone());
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
}

/// Images already in the registry are not built, and the gate is never
/// entered: a build of an unchanged project never serializes.
#[sqlx::test]
async fn present_images_are_skipped_without_the_gate(pool: PgPool) {
    setup(&pool).await;
    let fake = Arc::new(FakeImageBuilder::new());
    fake.set_image_exists("reg:5000/weft-worker:a");
    let gate = Gate::default();
    builder(&pool, fake.clone(), "d-0")
        .ensure_images(&[image(ImageKind::Worker, "reg:5000/weft-worker:a")], uuid::Uuid::new_v4(), "local", &gate, &ImageHold::new(&pool))
        .await
        .unwrap();
    assert!(fake.starts().is_empty());
    assert_eq!(gate.begun.load(Ordering::SeqCst), 0);
}

/// A stale image is built once, in a lane, and its row records the end.
#[sqlx::test]
async fn a_stale_image_is_built_and_recorded(pool: PgPool) {
    setup(&pool).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let gate = Gate::default();
    let done = finish_started(fake.clone(), BuildStatus::Succeeded);
    builder(&pool, fake.clone(), "d-0")
        .ensure_images(
            &[image(ImageKind::Worker, "reg:5000/weft-worker:b"), image(ImageKind::Worker, "reg:5000/weft-worker:b")],
            uuid::Uuid::new_v4(),
            "local",
            &gate, &ImageHold::new(&pool))
        .await
        .unwrap();
    done.await.unwrap();
    let starts = fake.starts();
    assert_eq!(starts.len(), 1, "one build per ref, however often the plan names it");
    assert_eq!(starts[0].build_args, vec![("WEFT_COMPILE_LANE".to_string(), "0".to_string())]);
    assert_eq!(gate.begun.load(Ordering::SeqCst), 1);
    let (status,): (String,) = sqlx::query_as("SELECT status FROM image_build WHERE image_ref = $1")
        .bind("reg:5000/weft-worker:b")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "succeeded");
}

/// A failed build fails the verb with the builder's reason, and a later
/// build of the same ref starts afresh.
#[sqlx::test]
async fn a_failed_build_names_its_reason_and_can_be_retried(pool: PgPool) {
    setup(&pool).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let gate = Gate::default();
    let done = finish_started(fake.clone(), BuildStatus::Failed { reason: "cargo: error[E0425]".into() });
    let e = builder(&pool, fake.clone(), "d-0")
        .ensure_images(&[image(ImageKind::Infra, "reg:5000/weft-infra-x:c")], uuid::Uuid::new_v4(), "local", &gate, &ImageHold::new(&pool))
        .await
        .unwrap_err()
        .to_string();
    done.await.unwrap();
    assert!(e.contains("E0425"), "{e}");
    assert!(fake.starts()[0].build_args.is_empty(), "an infra image has no compile lane");
    let claim = ledger::claim(&pool, "reg:5000/weft-infra-x:c", uuid::Uuid::new_v4(), "local", "d-0", 2, 0).await.unwrap();
    assert!(matches!(claim, Claim::Start { .. }), "{claim:?}");
}

/// A cancel of the project stops the build it started.
#[sqlx::test]
async fn a_cancel_stops_the_projects_own_build(pool: PgPool) {
    setup(&pool).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let gate = Gate::default();
    gate.cancel.store(true, Ordering::SeqCst);
    let e = builder(&pool, fake.clone(), "d-0")
        .ensure_images(&[image(ImageKind::Worker, "reg:5000/weft-worker:d")], uuid::Uuid::new_v4(), "local", &gate, &ImageHold::new(&pool))
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("cancelled"), "{e}");
    assert_eq!(fake.releases().len(), 1, "its build is freed");
    let (status,): (String,) = sqlx::query_as("SELECT status FROM image_build WHERE image_ref = $1")
        .bind("reg:5000/weft-worker:d")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "cancelled");
}

/// A second project asking for an image another one is building joins
/// that build (no second start), and its cancel leaves the other's build
/// running.
#[sqlx::test]
async fn a_shared_build_is_joined_and_never_stopped_by_the_joiner(pool: PgPool) {
    setup(&pool).await;
    let first = uuid::Uuid::new_v4();
    let Claim::Start { name, .. } = ledger::claim(&pool, "reg:5000/weft-worker:e", first, "local", "d-0", 2, crate_now()).await.unwrap() else {
        panic!("the first claim starts")
    };
    let fake = Arc::new(FakeImageBuilder::new());
    let gate = Gate::default();
    gate.cancel.store(true, Ordering::SeqCst);
    let e = builder(&pool, fake.clone(), "d-1")
        .ensure_images(&[image(ImageKind::Worker, "reg:5000/weft-worker:e")], uuid::Uuid::new_v4(), "local", &gate, &ImageHold::new(&pool))
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("cancelled"), "{e}");
    assert!(fake.starts().is_empty(), "joined, not started again");
    assert!(fake.releases().is_empty(), "another project's build is left running");
    let (status,): (String,) = sqlx::query_as("SELECT status FROM image_build WHERE build_name = $1")
        .bind(&name)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "running");
}

/// Two projects waiting on one build both get its end. The one that sees
/// it end frees the build's process, so the other would find it gone: the
/// end is read from the ledger, where it is recorded first.
#[sqlx::test]
async fn every_waiter_on_one_build_gets_its_end(pool: PgPool) {
    setup(&pool).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let wanted = [image(ImageKind::Worker, "reg:5000/weft-worker:g")];
    let (first_gate, second_gate) = (Gate::default(), Gate::default());
    let (first, second) = (builder(&pool, fake.clone(), "d-0"), builder(&pool, fake.clone(), "d-1"));
    let done = finish_started(fake.clone(), BuildStatus::Succeeded);
    let (first_hold, second_hold) = (ImageHold::new(&pool), ImageHold::new(&pool));
    let (a, b) = tokio::join!(
        first.ensure_images(&wanted, uuid::Uuid::new_v4(), "local", &first_gate, &first_hold),
        second.ensure_images(&wanted, uuid::Uuid::new_v4(), "local", &second_gate, &second_hold),
    );
    // The verbs first: when both fail before starting anything, `done`
    // never sees a build and would wait forever, hiding why.
    a.unwrap();
    b.unwrap();
    done.await.unwrap();
    assert_eq!(fake.starts().len(), 1, "one build for both");
    assert!(!fake.releases().is_empty(), "the build is freed once the end is recorded");
}

/// A build whose driver died (its hold lapsed) is taken over by the next
/// dispatcher that waits on it, which builds it again in the same row and
/// sees it through (why again: the ledger's module doc).
#[sqlx::test]
async fn an_orphaned_build_is_taken_over_and_built_again(pool: PgPool) {
    setup(&pool).await;
    let project = uuid::Uuid::new_v4();
    let long_ago = crate_now() - 10 * ledger::DRIVER_LEASE_SECS;
    let Claim::Start { name, .. } = ledger::claim(&pool, "reg:5000/weft-worker:f", project, "local", "dead-0", 2, long_ago).await.unwrap() else {
        panic!("the first claim starts")
    };
    let fake = Arc::new(FakeImageBuilder::new());
    let gate = Gate::default();
    let done = finish_started(fake.clone(), BuildStatus::Succeeded);
    builder(&pool, fake.clone(), "d-1")
        .ensure_images(&[image(ImageKind::Worker, "reg:5000/weft-worker:f")], project, "local", &gate, &ImageHold::new(&pool))
        .await
        .unwrap();
    done.await.unwrap();
    let starts = fake.starts();
    assert_eq!(starts.len(), 1, "built again, once");
    assert_ne!(starts[0].name, name, "under a new name");
    let (status, driver, current): (String, String, String) =
        sqlx::query_as("SELECT status, driver_replica, build_name FROM image_build WHERE image_ref = $1")
            .bind("reg:5000/weft-worker:f")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!((status.as_str(), driver.as_str(), current.as_str()), ("succeeded", "d-1", starts[0].name.as_str()));
}

/// A build this process drives but its project may not stop (it took over
/// another project's build) is never left without a driver when the verb
/// is cancelled: the verb stops waiting, and the build is driven to its
/// end anyway.
#[sqlx::test]
async fn a_cancel_never_leaves_a_driven_build_without_a_driver(pool: PgPool) {
    setup(&pool).await;
    let long_ago = crate_now() - 10 * ledger::DRIVER_LEASE_SECS;
    ledger::claim(&pool, "reg:5000/weft-worker:h", uuid::Uuid::new_v4(), "local", "dead-0", 2, long_ago).await.unwrap();
    let fake = Arc::new(FakeImageBuilder::new());
    let gate = Gate::default();
    gate.cancel.store(true, Ordering::SeqCst);
    let e = builder(&pool, fake.clone(), "d-1")
        .ensure_images(&[image(ImageKind::Worker, "reg:5000/weft-worker:h")], uuid::Uuid::new_v4(), "local", &gate, &ImageHold::new(&pool))
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("cancelled"), "{e}");
    finish_started(fake.clone(), BuildStatus::Succeeded).await.unwrap();
    for _ in 0..500 {
        let (status,): (String,) = sqlx::query_as("SELECT status FROM image_build WHERE image_ref = $1")
            .bind("reg:5000/weft-worker:h")
            .fetch_one(&pool)
            .await
            .unwrap();
        if status == "succeeded" {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("the taken-over build was left without a driver");
}

/// A second verb on the same process joining a build whose process is still being
/// created never calls it gone: only the task that started the process may.
#[sqlx::test]
async fn a_joiner_never_calls_a_build_still_being_made_gone(pool: PgPool) {
    setup(&pool).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let project = uuid::Uuid::new_v4();
    let Claim::Start { name, .. } = ledger::claim(&pool, "reg:5000/weft-worker:j", project, "local", "d-0", 2, crate_now()).await.unwrap() else {
        panic!("the first claim starts")
    };
    // The starter has not created the process yet: it polls as gone.
    fake.set_poll_result(&name, BuildStatus::Gone);
    let joiner = tokio::spawn({
        let (pool, fake) = (pool.clone(), fake.clone());
        async move {
            builder(&pool, fake, "d-0")
                .ensure_images(&[image(ImageKind::Worker, "reg:5000/weft-worker:j")], project, "local", &Gate::default(), &ImageHold::new(&pool))
                .await
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(!joiner.is_finished(), "the joiner waits");
    ledger::finish(&pool, "reg:5000/weft-worker:j", &name, ledger::Outcome::Succeeded, crate_now()).await.unwrap();
    joiner.await.unwrap().unwrap();
}

/// A verb that found the image missing while another verb's build of it
/// was running, and claims it only once that build succeeded, builds
/// nothing: the image is there. (Building it again started a build
/// nothing ever finished, which hung the two-waiter test under load.)
#[sqlx::test]
async fn a_claim_after_the_build_succeeded_builds_nothing(pool: PgPool) {
    setup(&pool).await;
    let Claim::Start { name, .. } = ledger::claim(&pool, "reg:5000/weft-worker:k", uuid::Uuid::new_v4(), "local", "d-0", 2, crate_now()).await.unwrap() else {
        panic!("the first claim starts")
    };
    ledger::finish(&pool, "reg:5000/weft-worker:k", &name, ledger::Outcome::Succeeded, crate_now()).await.unwrap();
    let late = ledger::claim(&pool, "reg:5000/weft-worker:k", uuid::Uuid::new_v4(), "local", "d-1", 2, crate_now()).await.unwrap();
    assert!(matches!(late, Claim::Built), "{late:?}");
    let fake = Arc::new(FakeImageBuilder::new());
    builder(&pool, fake.clone(), "d-1")
        .ensure_images(&[image(ImageKind::Worker, "reg:5000/weft-worker:k")], uuid::Uuid::new_v4(), "local", &Gate::default(), &ImageHold::new(&pool))
        .await
        .unwrap();
    assert!(fake.starts().is_empty(), "nothing is built again");
}

/// Racing claims of one ref start it exactly once.
#[sqlx::test]
async fn racing_claims_start_one_build(pool: PgPool) {
    setup(&pool).await;
    let mut claims = Vec::new();
    for i in 0..20 {
        let p = pool.clone();
        claims.push(tokio::spawn(async move {
            ledger::claim(&p, "reg:5000/weft-worker:g", uuid::Uuid::new_v4(), "local", &format!("d-{i}"), 4, 0).await.unwrap()
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

fn crate_now() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64
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
    let hold = ImageHold::new(&pool);
    hold.claim(std::slice::from_ref(&image_ref)).await.unwrap();
    // Another build holds an unrelated claim the whole time.
    let other = ImageHold::new(&pool);
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
    let hold = ImageHold::new(&pool);
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
