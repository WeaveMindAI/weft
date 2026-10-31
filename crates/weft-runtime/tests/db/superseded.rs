//! Copies of a role stepping aside once the platform routes to another
//! revision, told through a real Postgres and a fake platform.

use std::sync::Arc;
use std::time::Duration;

use weft_platform_traits::{CoreRole, FakeServiceRevisions, Routing};
use weft_runtime::superseded::{self, Copy};
use weft_task_store::aside::StepAside;
use weft_task_store::db::Pool;
use weft_task_store::pg_signal::{Heard, Listening, PgSignalWatch};

fn serving(revision: &str) -> Routing {
    Routing { serving: vec![revision.to_string()], incoming: None }
}

/// Every copy of the old revision announces itself as it starts, asks the
/// platform once at its start and once when another revision is announced
/// (never for a sibling's announcement, nor another role's), and steps
/// aside once the platform routes to the announced revision.
#[weft_db_macros::test]
async fn copies_step_aside_once_the_platform_routes_to_an_announced_revision(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&superseded::GROUP]).await.expect("schema");
    let watch = PgSignalWatch::start(pool.config(), &[superseded::ROLE_REVISION_CHANNEL], Listening::Always).await.expect("listen");
    let revisions = Arc::new(FakeServiceRevisions::new(Ok(serving("dispatcher-00004"))));
    let old: Arc<str> = Arc::from("dispatcher-00004");

    let (old_copy, sibling) = (StepAside::new(), StepAside::new());
    // Heard by the test too, so it goes on once both copies announced.
    let mut told = watch.subscribe();
    for aside in [&old_copy, &sibling] {
        let copy = Copy::new(CoreRole::Dispatcher, old.clone(), revisions.clone(), aside.clone());
        tokio::spawn(superseded::run(pool.clone(), watch.subscribe(), copy));
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut announced = 0;
        while announced < 2 {
            if let Heard::Signal { channel, .. } = told.next().await.expect("hear") {
                announced += usize::from(channel == superseded::ROLE_REVISION_CHANNEL);
            }
        }
    })
    .await
    .expect("both copies settle and announce");
    superseded::boot_as(&pool, CoreRole::Broker, "broker-00009").await.unwrap();
    superseded::boot_as(&pool, CoreRole::Dispatcher, &old).await.unwrap();
    assert_eq!(superseded::announced(&pool, CoreRole::Dispatcher).await.unwrap().map(|a| a.revision).as_deref(), Some("dispatcher-00004"));
    assert!(!old_copy.is_set() && !sibling.is_set(), "a copy of its own revision, or another role's, replaces nothing");

    revisions.answer(Ok(serving("dispatcher-00005")));
    superseded::boot_as(&pool, CoreRole::Dispatcher, "dispatcher-00005").await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), old_copy.stepped()).await.expect("the old copy steps aside");
    tokio::time::timeout(Duration::from_secs(10), sibling.stepped()).await.expect("so does every copy of the old revision");
    assert_eq!(revisions.asked(), 4, "each copy asked once at its start and once for the new revision, never for a sibling");
}
