//! A version build's image half against a real database: the ledger that
//! lets any dispatcher find and join a build, and see it through whoever
//! looks, driven through the builder with a fake `ImageBuilder`.
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
use weft_dispatcher::build::{follow, BuildGate, VersionBuilder};
use weft_platform_traits::{BuildStatus, FakeImageBuilder};

async fn setup(pool: &PgPool) {
    weft_dispatcher::app::apply_core_schema(pool).await.expect("core schema");
}

fn builder(pool: &PgPool, images: Arc<FakeImageBuilder>) -> VersionBuilder {
    VersionBuilder {
        bases: weft_compiler::worker_image::BaseImages {
            builder: "reg:5000/base:1".into(),
            runtime: "debian:bookworm-slim".into(),
        },
        images,
        pool: pool.clone(),
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
    builder(&pool, fake.clone())
        .ensure_images(&[image(ImageKind::Worker, "reg:5000/weft-worker:a")], uuid::Uuid::new_v4(), "local", &gate, &ImageHold::new(&pool, uuid::Uuid::new_v4()))
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
    builder(&pool, fake.clone())
        .ensure_images(
            &[image(ImageKind::Worker, "reg:5000/weft-worker:b"), image(ImageKind::Worker, "reg:5000/weft-worker:b")],
            uuid::Uuid::new_v4(),
            "local",
            &gate, &ImageHold::new(&pool, uuid::Uuid::new_v4()))
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
    let e = builder(&pool, fake.clone())
        .ensure_images(&[image(ImageKind::Infra, "reg:5000/weft-infra-x:c")], uuid::Uuid::new_v4(), "local", &gate, &ImageHold::new(&pool, uuid::Uuid::new_v4()))
        .await
        .unwrap_err()
        .to_string();
    done.await.unwrap();
    assert!(e.contains("E0425"), "{e}");
    assert!(fake.starts()[0].build_args.is_empty(), "an infra image has no compile lane");
    let claim = ledger::claim(&pool, fake.as_ref(), "reg:5000/weft-infra-x:c", uuid::Uuid::new_v4(), "local", Some(2), 0).await.unwrap();
    assert!(matches!(claim, Claim::Start { .. }), "{claim:?}");
}

/// A cancel of the project stops the build it started.
#[sqlx::test]
async fn a_cancel_stops_the_projects_own_build(pool: PgPool) {
    setup(&pool).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let gate = Gate::default();
    gate.cancel.store(true, Ordering::SeqCst);
    let e = builder(&pool, fake.clone())
        .ensure_images(&[image(ImageKind::Worker, "reg:5000/weft-worker:d")], uuid::Uuid::new_v4(), "local", &gate, &ImageHold::new(&pool, uuid::Uuid::new_v4()))
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
    let fake = Arc::new(FakeImageBuilder::new());
    let Claim::Start { name, .. } = ledger::claim(&pool, fake.as_ref(), "reg:5000/weft-worker:e", first, "local", Some(2), crate_now()).await.unwrap() else {
        panic!("the first claim starts")
    };
    let gate = Gate::default();
    gate.cancel.store(true, Ordering::SeqCst);
    let e = builder(&pool, fake.clone())
        .ensure_images(&[image(ImageKind::Worker, "reg:5000/weft-worker:e")], uuid::Uuid::new_v4(), "local", &gate, &ImageHold::new(&pool, uuid::Uuid::new_v4()))
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
    let (first, second) = (builder(&pool, fake.clone()), builder(&pool, fake.clone()));
    let done = finish_started(fake.clone(), BuildStatus::Succeeded);
    let (first_hold, second_hold) = (ImageHold::new(&pool, uuid::Uuid::new_v4()), ImageHold::new(&pool, uuid::Uuid::new_v4()));
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

/// A build nobody waits on any more (its verb stopped waiting, or its
/// dispatcher went away) is seen through by whoever looks next: the
/// build loop records its end from the builder's answer.
#[sqlx::test]
async fn a_build_nobody_waits_on_is_seen_through_by_the_next_look(pool: PgPool) {
    setup(&pool).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let image_ref = "reg:5000/weft-worker:f";
    let Claim::Start { name, .. } = ledger::claim(&pool, fake.as_ref(), image_ref, uuid::Uuid::new_v4(), "local", Some(2), crate_now()).await.unwrap() else {
        panic!("the first claim starts")
    };
    assert!(ledger::started(&pool, image_ref, &name, &weft_platform_traits::BuildHandle::named(name.clone())).await.unwrap());
    follow::advance(&pool, fake.as_ref(), image_ref).await.unwrap();
    assert_eq!(ledger::look(&pool, image_ref).await.unwrap(), ledger::Seen::Running, "still building");
    fake.set_poll_result(&name, BuildStatus::Succeeded);
    follow::advance(&pool, fake.as_ref(), image_ref).await.unwrap();
    assert_eq!(ledger::look(&pool, image_ref).await.unwrap(), ledger::Seen::Ended(ledger::Outcome::Succeeded));
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
    let gate = Gate::default();
    let done = tokio::spawn({
        let fake = fake.clone();
        async move {
            loop {
                if let Some(started) = fake.starts().first() {
                    fake.set_poll_result(&format!("{}-id", started.name), BuildStatus::Succeeded);
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }
    });
    builder(&pool, fake.clone())
        .ensure_images(&[image(ImageKind::Worker, "reg:5000/weft-worker:n")], uuid::Uuid::new_v4(), "local", &gate, &ImageHold::new(&pool, uuid::Uuid::new_v4()))
        .await
        .unwrap();
    done.await.unwrap();
    let (current,): (String,) = sqlx::query_as("SELECT builder_id FROM image_build WHERE image_ref = $1")
        .bind("reg:5000/weft-worker:n")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(current.ends_with("-id"), "the row names the builder's own id: {current}");
}

/// A start that outlasts its hold (its dispatcher went away mid-start)
/// ends failed by name, and a start that lands on a row no longer its
/// build's (cancelled meanwhile) frees what it made instead of leaving it
/// running with nothing tracking it.
#[sqlx::test]
async fn a_start_that_never_lands_ends_and_a_late_one_is_freed(pool: PgPool) {
    setup(&pool).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let image_ref = "reg:5000/weft-worker:s";
    let long_ago = crate_now() - ledger::START_HOLD_SECS - 1;
    let Claim::Start { name, .. } = ledger::claim(&pool, fake.as_ref(), image_ref, uuid::Uuid::new_v4(), "local", Some(2), long_ago).await.unwrap() else {
        panic!("the first claim starts")
    };
    follow::advance(&pool, fake.as_ref(), image_ref).await.unwrap();
    let ledger::Seen::Ended(ledger::Outcome::Failed(reason)) = ledger::look(&pool, image_ref).await.unwrap() else {
        panic!("a start that never landed ends failed")
    };
    assert!(reason.contains("never made on the builder"), "{reason}");
    assert!(fake.polls().is_empty(), "a build with no id on the builder is never asked about");
    let handle = weft_platform_traits::BuildHandle::named(name.clone());
    assert!(!ledger::started(&pool, image_ref, &name, &handle).await.unwrap(), "the row is no longer that build's");

    let image_ref = "reg:5000/weft-worker:t";
    let Claim::Start { name, .. } = ledger::claim(&pool, fake.as_ref(), image_ref, uuid::Uuid::new_v4(), "local", Some(2), crate_now()).await.unwrap() else {
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
    let Claim::Start { name, .. } = ledger::claim(&pool, fake.as_ref(), image_ref, uuid::Uuid::new_v4(), "local", Some(2), crate_now()).await.unwrap() else {
        panic!("the first claim starts")
    };
    assert!(ledger::started(&pool, image_ref, &name, &weft_platform_traits::BuildHandle::named(name.clone())).await.unwrap());
    fake.fail_polls(&name);
    follow::advance(&pool, fake.as_ref(), image_ref).await.unwrap();
    assert_eq!(ledger::look(&pool, image_ref).await.unwrap(), ledger::Seen::Running, "one unanswered look is looked at again");
    sqlx::query("UPDATE image_build SET failing_since = failing_since - $2 WHERE image_ref = $1")
        .bind(image_ref)
        .bind(ledger::UNANSWERED_GIVE_UP_SECS + 1)
        .execute(&pool)
        .await
        .unwrap();
    follow::advance(&pool, fake.as_ref(), image_ref).await.unwrap();
    let ledger::Seen::Ended(ledger::Outcome::Failed(reason)) = ledger::look(&pool, image_ref).await.unwrap() else {
        panic!("given up on")
    };
    assert!(reason.contains("has not answered"), "{reason}");
}

/// A build its builder no longer knows, once its start hold passed, ends
/// failed by name instead of being waited on for ever.
#[sqlx::test]
async fn a_build_its_builder_lost_fails_by_name(pool: PgPool) {
    setup(&pool).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let image_ref = "reg:5000/weft-worker:l";
    let Claim::Start { name, .. } = ledger::claim(&pool, fake.as_ref(), image_ref, uuid::Uuid::new_v4(), "local", Some(2), crate_now()).await.unwrap() else {
        panic!("the first claim starts")
    };
    assert!(ledger::started(&pool, image_ref, &name, &weft_platform_traits::BuildHandle::named(name.clone())).await.unwrap());
    fake.set_poll_result(&name, BuildStatus::Gone);
    follow::advance(&pool, fake.as_ref(), image_ref).await.unwrap();
    let ledger::Seen::Ended(ledger::Outcome::Failed(reason)) = ledger::look(&pool, image_ref).await.unwrap() else {
        panic!("a lost build ends failed")
    };
    assert!(reason.contains(&name) && reason.contains("gone"), "{reason}");
}

/// A verb cancelled while it waits on another project's build stops
/// waiting, and the build goes on to its end without it.
#[sqlx::test]
async fn a_cancelled_joiner_leaves_the_build_to_finish(pool: PgPool) {
    setup(&pool).await;
    let image_ref = "reg:5000/weft-worker:h";
    let fake = Arc::new(FakeImageBuilder::new());
    let Claim::Start { name, .. } = ledger::claim(&pool, fake.as_ref(), image_ref, uuid::Uuid::new_v4(), "local", Some(2), crate_now()).await.unwrap() else {
        panic!("the first claim starts")
    };
    assert!(ledger::started(&pool, image_ref, &name, &weft_platform_traits::BuildHandle::named(name.clone())).await.unwrap());
    let gate = Gate::default();
    gate.cancel.store(true, Ordering::SeqCst);
    let e = builder(&pool, fake.clone())
        .ensure_images(&[image(ImageKind::Worker, image_ref)], uuid::Uuid::new_v4(), "local", &gate, &ImageHold::new(&pool, uuid::Uuid::new_v4()))
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("cancelled"), "{e}");
    fake.set_poll_result(&name, BuildStatus::Succeeded);
    follow::advance(&pool, fake.as_ref(), image_ref).await.unwrap();
    assert_eq!(ledger::look(&pool, image_ref).await.unwrap(), ledger::Seen::Ended(ledger::Outcome::Succeeded));
}

/// A verb joining a build whose starter is still making it never calls
/// it gone while the start hold lasts, though the builder knows nothing
/// of it yet.
#[sqlx::test]
async fn a_joiner_never_calls_a_build_still_being_made_gone(pool: PgPool) {
    setup(&pool).await;
    let fake = Arc::new(FakeImageBuilder::new());
    let project = uuid::Uuid::new_v4();
    let Claim::Start { name, .. } = ledger::claim(&pool, fake.as_ref(), "reg:5000/weft-worker:j", project, "local", Some(2), crate_now()).await.unwrap() else {
        panic!("the first claim starts")
    };
    // The starter has not created the process yet: it polls as gone.
    fake.set_poll_result(&name, BuildStatus::Gone);
    let joiner = tokio::spawn({
        let (pool, fake) = (pool.clone(), fake.clone());
        async move {
            builder(&pool, fake)
                .ensure_images(&[image(ImageKind::Worker, "reg:5000/weft-worker:j")], project, "local", &Gate::default(), &ImageHold::new(&pool, uuid::Uuid::new_v4()))
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
    let fake = FakeImageBuilder::new();
    let image_ref = "reg:5000/weft-worker:k";
    let Claim::Start { name, .. } = ledger::claim(&pool, &fake, image_ref, uuid::Uuid::new_v4(), "local", Some(2), crate_now()).await.unwrap() else {
        panic!("the first claim starts")
    };
    ledger::finish(&pool, image_ref, &name, ledger::Outcome::Succeeded, crate_now()).await.unwrap();
    fake.set_image_exists(image_ref);
    let late = ledger::claim(&pool, &fake, image_ref, uuid::Uuid::new_v4(), "local", Some(2), crate_now()).await.unwrap();
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
    let Claim::Start { name, .. } = ledger::claim(&pool, fake.as_ref(), image_ref, uuid::Uuid::new_v4(), "local", Some(2), crate_now()).await.unwrap() else {
        panic!("the first claim starts")
    };
    ledger::finish(&pool, image_ref, &name, ledger::Outcome::Succeeded, crate_now()).await.unwrap();
    let done = finish_started(fake.clone(), BuildStatus::Succeeded);
    let built = builder(&pool, fake.clone())
        .ensure_images(&[image(ImageKind::Worker, image_ref)], uuid::Uuid::new_v4(), "local", &Gate::default(), &ImageHold::new(&pool, uuid::Uuid::new_v4()))
        .await
        .unwrap();
    done.await.unwrap();
    assert_eq!(built, [image_ref]);
    let starts = fake.starts();
    assert_eq!(starts.len(), 1, "built again");
    assert_ne!(starts[0].name, name, "under a new build");
    assert_eq!(ledger::look(&pool, image_ref).await.unwrap(), ledger::Seen::Ended(ledger::Outcome::Succeeded));
}

/// Racing claims of one ref start it exactly once.
#[sqlx::test]
async fn racing_claims_start_one_build(pool: PgPool) {
    setup(&pool).await;
    let mut claims = Vec::new();
    for _ in 0..20 {
        let p = pool.clone();
        claims.push(tokio::spawn(async move {
            ledger::claim(&p, &FakeImageBuilder::new(), "reg:5000/weft-worker:g", uuid::Uuid::new_v4(), "local", Some(4), 0).await.unwrap()
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
            let Claim::Start { name, .. } = ledger::claim(&pool, fake, image_ref, uuid::Uuid::new_v4(), "local", Some(2), crate_now()).await.unwrap() else {
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
    let Claim::Start { .. } = ledger::claim(&pool, &fake, running, uuid::Uuid::new_v4(), "local", Some(2), crate_now()).await.unwrap() else {
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
    assert_eq!(ledger::look(&pool, running).await.unwrap(), ledger::Seen::Running);
}
