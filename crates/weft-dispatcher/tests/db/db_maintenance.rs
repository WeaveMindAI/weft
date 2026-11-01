//! Layer-3 tests for the install's maintenance (`weft_dispatcher::maintenance`)
//! against a real Postgres: what `weft maintenance on` writes and how a
//! second one adds to it, what a person's take-down clears, what the lift
//! reads back and in which order, and the claim refusing everyone but the
//! lift while the install is in maintenance.

use serde_json::json;
use uuid::Uuid;

use weft_core::activation::ActivationKey;
use weft_core::instance::{InstanceId, Owner};
use weft_core::take_down::TakeDown;
use weft_dispatcher::activation_store::{
    ActivationLifecycle, ActivationStoreOps, Claimant, ClaimRefused, DownCause, Forget, LifecycleWrite, PostgresActivationStore, ProjectStatus,
    SignalsGoing,
};
use weft_dispatcher::maintenance;
use weft_task_store::db::{Client as _, Pool};

const TENANT: &str = "tenant-1";

async fn setup(pool: &Pool) -> (PostgresActivationStore, Uuid) {
    weft_task_store::apply_groups(pool, weft_dispatcher::app::DISPATCHER_GROUPS).await.expect("core schema");
    let projects: weft_dispatcher::ProjectStore = std::sync::Arc::new(weft_dispatcher::PostgresProjectStore::new(pool.clone()));
    let id = Uuid::new_v4();
    let empty = serde_json::from_value(json!({ "id": id, "nodes": [], "edges": [] })).expect("minimal ProjectDefinition");
    projects
        .register_with_hashes(empty, "db-rig", "", TENANT, Some("bin-A"), Some("def-1"), None, None, None, None, None, None)
        .await
        .expect("register project");
    (PostgresActivationStore::new(pool.clone()), id)
}

fn key(trigger: &str) -> ActivationKey {
    ActivationKey::new(trigger, Owner::Shared)
}

fn ada() -> Option<InstanceId> {
    Some(InstanceId::new("ada").unwrap())
}

/// What is on across the rig's tenant.
async fn going(pool: &Pool) -> weft_core::maintenance::Going {
    let projects = weft_dispatcher::PostgresProjectStore::new(pool.clone());
    maintenance::going(pool, &projects, TENANT).await.unwrap()
}

/// Switch `keys` on, as `claimant`.
async fn turn_on(activations: &PostgresActivationStore, id: Uuid, keys: &[ActivationKey], claimant: Claimant) {
    let setup = Uuid::new_v4();
    activations.try_begin_activating(id, keys, setup, None, claimant, Uuid::new_v4()).await.unwrap().expect("claimed");
    activations.end_activating(id, setup, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");
}

/// Take `keys` down with the default answers, marked by `cause`; the keys
/// it took down.
async fn take_down(activations: &PostgresActivationStore, id: Uuid, keys: &[ActivationKey], cause: DownCause<'_>) -> Vec<ActivationKey> {
    let landing = ActivationLifecycle::down(&TakeDown::DEFAULT, 0);
    match activations.set_lifecycle_guarded(id, keys, &landing, SignalsGoing::Kept, Forget::Nothing, cause, None).await.unwrap() {
        LifecycleWrite::Applied { keys, .. } => keys,
        other => panic!("the take-down went through: {other:?}"),
    }
}

/// `on` marks what it takes down and records the copies it stops; a second
/// `on` adds what is on by then and keeps the first one's time, marks and
/// records.
#[weft_db_macros::test]
async fn maintenance_on_marks_records_and_accumulates(pool: Pool) {
    let (activations, id) = setup(&pool).await;
    turn_on(&activations, id, &[key("feed")], Claimant::Anyone).await;
    assert_eq!(going(&pool).await.triggers_on, 1);

    assert_eq!(maintenance::begin(&pool, 100).await.unwrap(), 100);
    assert_eq!(take_down(&activations, id, &[key("feed")], DownCause::Maintenance).await, vec![key("feed")]);
    maintenance::record_copies(&pool, id, None, &["db".into()], 100).await.unwrap();

    // Something comes on meanwhile (only the lift may, here), and a second
    // `on` takes it down beside what the first one took down.
    turn_on(&activations, id, &[key("door")], Claimant::Lift).await;
    assert_eq!(maintenance::begin(&pool, 200).await.unwrap(), 100, "a second `on` keeps the first one's time");
    let taken = take_down(&activations, id, &[key("feed"), key("door")], DownCause::Maintenance).await;
    assert_eq!(taken, vec![key("door")], "only what is still on is taken down again");
    maintenance::record_copies(&pool, id, None, &["db".into(), "queue".into()], 200).await.unwrap();
    maintenance::record_copies(&pool, id, ada().as_ref(), &["db".into()], 200).await.unwrap();

    let marked: Vec<ActivationKey> = maintenance::marked_triggers(&pool).await.unwrap().into_iter().map(|(_, key)| key).collect();
    assert_eq!(marked, vec![key("door"), key("feed")], "the first `on`'s mark is kept");
    let copies: Vec<(String, Option<InstanceId>)> =
        maintenance::recorded_copies(&pool).await.unwrap().into_iter().map(|copy| (copy.node, copy.instance)).collect();
    assert_eq!(copies, vec![("db".into(), None), ("db".into(), ada()), ("queue".into(), None)], "each copy once, the shared one first");
    for activation in activations.list(id).await.unwrap() {
        assert_eq!(activation.lifecycle.went_down, Some(weft_broker_client::activation::WentDown::Maintenance), "{}", activation.key);
    }
    assert_eq!(going(&pool).await.triggers_on, 0);
    assert!(!maintenance::left(&pool).await.unwrap().is_empty(), "something is left to bring back");
}

/// A copy whose stop is asked for and not yet carried out reads as being
/// stopped, as `weft status` shows it, though its row still says
/// `running`: what `weft maintenance on` waits for.
#[weft_db_macros::test]
async fn a_stop_not_yet_carried_out_counts_as_going_down(pool: Pool) {
    use weft_dispatcher::infra_lifecycle_command::{TakeDownCommands, TakeDownWork, Teardown};
    let (_, id) = setup(&pool).await;
    for node in ["db", "cache"] {
        pool.execute(
            "INSERT INTO infra_node (project_id, node_id, copy_id, status, health_rules_json_version) VALUES ($1, $2, $2, 'running', 1)",
            &[&id, &node],
        )
        .await
        .unwrap();
    }
    assert_eq!((going(&pool).await.copies_up, going(&pool).await.copies_changing), (2, 0));

    let stop = TakeDownCommands {
        tenant_id: TENANT.into(),
        issued_by_replica: "disp-1".into(),
        work: TakeDownWork::Teardown { nodes: vec!["db".into()], copies: weft_core::instance::Copies::Shared, teardown: Teardown::Stop { force: true }, waits: false },
    };
    stop.issue(&pool, id).await.unwrap().expect("the project is there");
    let now = going(&pool).await;
    assert_eq!((now.copies_up, now.copies_changing), (1, 1), "{now:?}");
    assert!(now.still_going_down());
}

/// A person taking a marked trigger down during the maintenance clears its
/// mark: it stays off after the lift, and the maintenance can end without
/// it.
#[weft_db_macros::test]
async fn a_persons_deactivate_clears_the_maintenance_mark(pool: Pool) {
    let (activations, id) = setup(&pool).await;
    turn_on(&activations, id, &[key("feed")], Claimant::Anyone).await;
    maintenance::begin(&pool, 100).await.unwrap();
    take_down(&activations, id, &[key("feed")], DownCause::Maintenance).await;
    assert_eq!(maintenance::marked_triggers(&pool).await.unwrap().len(), 1);

    take_down(&activations, id, &[key("feed")], DownCause::Person).await;
    let feed = activations.list(id).await.unwrap().remove(0);
    assert_eq!((feed.lifecycle.status, feed.lifecycle.went_down), (ProjectStatus::Inactive, None));
    assert!(maintenance::marked_triggers(&pool).await.unwrap().is_empty());
    assert!(maintenance::left(&pool).await.unwrap().is_empty(), "nothing left for the lift to bring back");
}

/// The lift reads back only what the maintenance took down (never a
/// trigger a person or the health check took down), each owner's apart,
/// and the maintenance ends once each record and mark is gone.
#[weft_db_macros::test]
async fn the_lift_brings_back_only_what_was_marked(pool: Pool) {
    let (activations, id) = setup(&pool).await;
    turn_on(&activations, id, &[key("feed"), key("door"), key("cron")], Claimant::Anyone).await;
    take_down(&activations, id, &[key("door")], DownCause::Person).await;
    let broken = [weft_broker_client::protocol::InfraCopy { node_id: "db".into(), instance: None }];
    take_down(&activations, id, &[key("cron")], DownCause::Health(&broken)).await;
    maintenance::begin(&pool, 100).await.unwrap();
    take_down(&activations, id, &[key("feed"), key("door"), key("cron")], DownCause::Maintenance).await;
    maintenance::record_copies(&pool, id, None, &["db".into()], 100).await.unwrap();
    maintenance::record_copies(&pool, id, ada().as_ref(), &["cache".into()], 100).await.unwrap();

    let left = maintenance::left(&pool).await.unwrap();
    assert_eq!(left.len(), 1);
    let ada = Owner::from_instance(ada());
    assert_eq!(left[0].copies, std::collections::BTreeMap::from([(Owner::Shared, vec!["db".into()]), (ada.clone(), vec!["cache".into()])]));
    assert_eq!(left[0].triggers, std::collections::BTreeMap::from([(Owner::Shared, vec!["feed".into()])]));

    // What the lift does as each comes back: a started copy is forgotten,
    // and the lift's claim clears the trigger's mark.
    maintenance::forget_copies(&pool, id, None, &["db".into()]).await.unwrap();
    maintenance::forget_copies(&pool, id, ada.instance(), &["cache".into()]).await.unwrap();
    assert!(!maintenance::left(&pool).await.unwrap().is_empty(), "the trigger is still to come back");
    turn_on(&activations, id, &[key("feed")], Claimant::Lift).await;
    assert!(maintenance::left(&pool).await.unwrap().is_empty());
    let still_down: Vec<(ActivationKey, Option<weft_broker_client::activation::WentDown>)> = activations
        .list(id)
        .await
        .unwrap()
        .into_iter()
        .filter(|a| a.lifecycle.status == ProjectStatus::Inactive)
        .map(|a| (a.key, a.lifecycle.went_down))
        .collect();
    assert_eq!(
        still_down,
        vec![(key("cron"), Some(weft_broker_client::activation::WentDown::Health)), (key("door"), None)],
        "what something else took down stays as it was"
    );
}

/// While the install is in maintenance, every claim but the lift's is
/// refused; once it ends, anyone's goes through again.
#[weft_db_macros::test]
async fn while_in_maintenance_activate_is_refused(pool: Pool) {
    let (activations, id) = setup(&pool).await;
    maintenance::begin(&pool, 100).await.unwrap();
    let refused = activations.try_begin_activating(id, &[key("feed")], Uuid::new_v4(), None, Claimant::Anyone, Uuid::new_v4()).await.unwrap();
    assert!(matches!(refused, Err(ClaimRefused::Maintenance)), "{refused:?}");
    assert!(activations.list(id).await.unwrap().is_empty(), "a refused claim writes nothing");
    turn_on(&activations, id, &[key("feed")], Claimant::Lift).await;

    maintenance::end(&pool).await.unwrap();
    turn_on(&activations, id, &[key("door")], Claimant::Anyone).await;
}

/// A project the lift could not bring back (its code did not build on the
/// new weft) does not hold the install: the maintenance ends, nothing is
/// refused any more, and what it had taken down stays recorded as that
/// project's, for its next resync to bring back, which clears the mark.
#[weft_db_macros::test]
async fn what_a_project_could_not_bring_back_outlives_the_maintenance(pool: Pool) {
    let (activations, id) = setup(&pool).await;
    turn_on(&activations, id, &[key("feed")], Claimant::Anyone).await;
    maintenance::begin(&pool, 100).await.unwrap();
    take_down(&activations, id, &[key("feed")], DownCause::Maintenance).await;
    maintenance::record_copies(&pool, id, None, &["db".into()], 100).await.unwrap();

    maintenance::end(&pool).await.unwrap();
    assert!(!maintenance::is_on(&pool).await.unwrap(), "one project left down never keeps the install in maintenance");
    let left = maintenance::left(&pool).await.unwrap();
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].copies, std::collections::BTreeMap::from([(Owner::Shared, vec!["db".into()])]), "still the project's own, to bring back");
    assert_eq!(left[0].triggers, std::collections::BTreeMap::from([(Owner::Shared, vec!["feed".into()])]));
    // What the resync's bring-back does: the copy started and forgotten,
    // the trigger claimed and landed on, which clears its mark.
    maintenance::forget_copies(&pool, id, None, &["db".into()]).await.unwrap();
    turn_on(&activations, id, &[key("feed")], Claimant::Lift).await;
    assert!(maintenance::left(&pool).await.unwrap().is_empty());
    let feed = activations.list(id).await.unwrap().remove(0);
    assert_eq!((feed.lifecycle.status, feed.lifecycle.went_down), (ProjectStatus::Active, None));
}

/// The lift builds a project again from the request its running build came
/// from: a build the install registers (here through the build loop, its
/// image built by the fake builder, as a version that waited on its images
/// registers) keeps that request's name, files and inputs on the project.
/// A project registered with none (a build an older weft registered) has
/// nothing to build from, which the lift says.
#[weft_db_macros::test]
async fn the_lift_rebuilds_from_the_version_the_running_build_came_from(pool: Pool) {
    use std::sync::Arc;
    use weft_compiler::build_plan::{ImageKind, PlannedImage};
    use weft_core::builds::{BuildInputs, BuiltProgram, NodeSet, VersionSource};
    use weft_dispatcher::build::{follow, prune::ImageHold, waiting, Version, VersionBuild, VersionBuilder};
    use weft_platform_traits::{BuildStatus, FakeImageBuilder, Staging};

    let (_, id) = setup(&pool).await;
    let projects: weft_dispatcher::ProjectStore = Arc::new(weft_dispatcher::PostgresProjectStore::new(pool.clone()));
    assert!(projects.running_build(id).await.unwrap().is_none(), "registered with no build: nothing to build again");

    let request = VersionSource {
        manifest: [("main.weft".to_string(), "sha256:aa".to_string())].into(),
        inputs: BuildInputs { node_set: NodeSet::Referenced, assets: [(weft_core::project::ResolutionKey { path: "readme.txt".into(), ty: "String".into() }, json!("hello"))].into() },
    };
    let worker = "reg:5000/weft-worker:rebuilt";
    let images = [PlannedImage {
        kind: ImageKind::Worker,
        image_ref: worker.into(),
        context_dir: "/tmp/ctx".into(),
        node_id: None,
        image_name: None,
        recipe: weft_core::builds::BuildRecipe::Dockerfile { result_file: None },
    }];
    let version = Version {
        name: "shop".into(),
        manifest: request.manifest.clone(),
        inputs: Some(request.inputs.clone()),
        program: BuiltProgram {
            version: weft_core::project::hash::manifest_version_id(&request.manifest),
            definition: serde_json::from_value(json!({ "id": id, "nodes": [], "edges": [] })).unwrap(),
            binary_hash: "rebuilt".into(),
            definition_hash: "def-rebuilt".into(),
            infra_hash: "infra".into(),
            implementations: Default::default(),
            infra_images: Default::default(),
            replaced_infra_images: Vec::new(),
            infra_places: Default::default(),
            worker_image: worker.into(),
            left_out: Vec::new(),
            worker_log: None,
            node_needs: Default::default(),
        },
        images: vec![worker.into()],
        ask: projects.next_build_ask(id).await.unwrap(),
    };
    let fake = Arc::new(FakeImageBuilder::new());
    let builder = VersionBuilder {
        images: fake.clone(),
        pool: pool.clone(),
        prunes: Default::default(),
        blobs: weft_dispatcher::build::blob_cache::BlobCache::in_temp_dir(),
    };
    let underway = match builder
        .start_images(version, &images, &Staging::new(()), id, TENANT, Arc::new(NoGate), ImageHold::new(&pool, id))
        .await
        .unwrap()
    {
        VersionBuild::Waiting(underway) => underway,
        VersionBuild::Ready { .. } => panic!("the worker image was missing: its build waits"),
    };
    fake.set_poll_result(&underway.images[0].name, BuildStatus::Succeeded { result: None });
    follow::look(&pool, fake.as_ref()).await.unwrap();
    let advanced = waiting::advance(&pool, projects.as_ref()).await.unwrap();
    assert_eq!(advanced.registered.iter().map(|s| s.id).collect::<Vec<_>>(), [id], "{:?}", advanced.failed);

    let stored = projects.running_build(id).await.unwrap().expect("the registered build keeps its version");
    assert_eq!(stored, request);
}

/// A gate that lets every build through.
struct NoGate;

#[async_trait::async_trait]
impl weft_dispatcher::build::BuildGate for NoGate {
    async fn begin(&self) -> anyhow::Result<()> {
        Ok(())
    }
    async fn let_go(&self) {}
}

/// An activation the lift starts that ends without landing (cancelled,
/// failed, its driver dead) leaves the trigger marked: the claim keeps the
/// mark and only landing on clears it, so the next lift brings it back.
#[weft_db_macros::test]
async fn a_failed_lift_activation_keeps_the_mark(pool: Pool) {
    let (activations, id) = setup(&pool).await;
    turn_on(&activations, id, &[key("feed")], Claimant::Anyone).await;
    maintenance::begin(&pool, 100).await.unwrap();
    take_down(&activations, id, &[key("feed")], DownCause::Maintenance).await;

    // Ended the way a failure that turns the trigger off ends it.
    let lift = Uuid::new_v4();
    activations.try_begin_activating(id, &[key("feed")], lift, None, Claimant::Lift, Uuid::new_v4()).await.unwrap().expect("claimed");
    assert!(maintenance::marked_triggers(&pool).await.unwrap().is_empty(), "while it activates, it is not left to bring back");
    activations.end_unfinished(id, lift).await.unwrap().expect("owned");
    let feed = activations.list(id).await.unwrap().remove(0);
    assert_eq!((feed.lifecycle.status, feed.lifecycle.went_down), (ProjectStatus::Inactive, Some(weft_broker_client::activation::WentDown::Maintenance)));
    assert_eq!(maintenance::marked_triggers(&pool).await.unwrap().len(), 1, "the next lift brings it back");

    // Ended the way a failure that puts it back ends it.
    let again = Uuid::new_v4();
    let previous = activations.try_begin_activating(id, &[key("feed")], again, None, Claimant::Lift, Uuid::new_v4()).await.unwrap().expect("claimed");
    assert!(activations.restore(id, again, &previous).await.unwrap());
    assert_eq!(maintenance::marked_triggers(&pool).await.unwrap().len(), 1);

    // Landed on: the mark goes, and nothing is left.
    turn_on(&activations, id, &[key("feed")], Claimant::Lift).await;
    assert!(maintenance::marked_triggers(&pool).await.unwrap().is_empty());
    assert!(maintenance::left(&pool).await.unwrap().is_empty());
}

/// A marked trigger the program no longer has is forgotten by the lift
/// (`maintenance::forget_marks`): its mark goes, so nothing is left for a
/// lift to bring back, while a sibling still marked, and a trigger
/// something else took down, keep theirs.
#[weft_db_macros::test]
async fn a_trigger_the_program_lost_is_forgotten_and_its_siblings_stay_marked(pool: Pool) {
    let (activations, id) = setup(&pool).await;
    turn_on(&activations, id, &[key("old.tick"), key("feed"), key("door")], Claimant::Anyone).await;
    take_down(&activations, id, &[key("door")], DownCause::Person).await;
    maintenance::begin(&pool, 100).await.unwrap();
    take_down(&activations, id, &[key("old.tick"), key("feed")], DownCause::Maintenance).await;
    assert_eq!(maintenance::marked_triggers(&pool).await.unwrap().len(), 2);

    maintenance::forget_marks(&pool, id, None, &["old.tick".into(), "door".into()]).await.unwrap();
    let marked: Vec<ActivationKey> = maintenance::marked_triggers(&pool).await.unwrap().into_iter().map(|(_, key)| key).collect();
    assert_eq!(marked, vec![key("feed")], "only the trigger the program lost is forgotten");
    let left = maintenance::left(&pool).await.unwrap();
    assert_eq!(left[0].triggers, std::collections::BTreeMap::from([(Owner::Shared, vec!["feed".into()])]));
    let old = activations.list(id).await.unwrap().into_iter().find(|a| a.key == key("old.tick")).unwrap();
    assert_eq!((old.lifecycle.status, old.lifecycle.went_down), (ProjectStatus::Inactive, None));
}
