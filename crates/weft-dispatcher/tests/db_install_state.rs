//! Layer-3 tests against a REAL Postgres for the install's own state: the
//! domains it answers at (`weft_dispatcher::domains`), and a project's
//! frontends (`weft_dispatcher::frontends`).
//!
//! Same rig as `db_lifecycle.rs`: `#[sqlx::test]` hands each test a fresh
//! database, the real boot-time migration path builds the schema, and the
//! tests are gated behind `db-tests` (`scripts/run-db-tests.sh weft-dispatcher`).
#![cfg(feature = "db-tests")]

use sqlx::PgPool;
use uuid::Uuid;
use weft_core::install::{Domain, DomainServes};
use weft_dispatcher::ProjectStoreOps as _;

async fn project(pool: &PgPool) -> Uuid {
    weft_dispatcher::app::apply_core_schema(pool).await.expect("core schema");
    let store = weft_dispatcher::PostgresProjectStore::new(pool.clone());
    let id = Uuid::new_v4();
    let definition = serde_json::from_value(serde_json::json!({ "id": id, "nodes": [], "edges": [] })).expect("a definition");
    store
        .register_with_hashes(definition, "p", "", "tenant-1", Some("bin"), Some("def"), None, None, None, None)
        .await
        .expect("register");
    id
}

#[sqlx::test]
async fn a_domain_is_stored_once_and_goes_with_its_project(pool: PgPool) {
    let id = project(&pool).await;
    let install = Domain { name: "weft.example.com".into(), serves: DomainServes::Install };
    let api = Domain { name: "api.example.com".into(), serves: DomainServes::Api { project: id } };
    use weft_dispatcher::domains::Added;
    assert_eq!(weft_dispatcher::domains::add(&pool, &install, 1).await.unwrap(), Added::New);
    assert_eq!(weft_dispatcher::domains::add(&pool, &api, 1).await.unwrap(), Added::New);
    assert_eq!(weft_dispatcher::domains::add(&pool, &api, 1).await.unwrap(), Added::AlreadyStored, "the same domain again is not refused");

    let again = Domain { name: "api.example.com".into(), serves: DomainServes::Install };
    assert_eq!(weft_dispatcher::domains::add(&pool, &again, 2).await.unwrap(), Added::TakenForSomethingElse, "a stored name never quietly moves");
    assert_eq!(weft_dispatcher::domains::list(&pool).await.unwrap(), vec![api.clone(), install.clone()]);

    sqlx::query("DELETE FROM project WHERE id = $1").bind(id).execute(&pool).await.unwrap();
    assert_eq!(weft_dispatcher::domains::list(&pool).await.unwrap(), vec![install], "a project's domain goes with it");
    assert!(weft_dispatcher::domains::remove(&pool, "weft.example.com").await.unwrap());
    assert!(!weft_dispatcher::domains::remove(&pool, "weft.example.com").await.unwrap());
}

fn frontend(project: Uuid, name: &str, repo: Option<&str>) -> weft_core::frontend::Frontend {
    weft_core::frontend::Frontend {
        name: name.into(),
        project,
        host: if repo.is_some() { weft_core::frontend::FrontendHost::CloudRun } else { weft_core::frontend::FrontendHost::External },
        repo: repo.map(|r| weft_core::frontend::Repository { name: r.into(), id: if r == "me/site" { 1 } else { 2 } }),
        service: repo.map(|_| format!("fe-{name}")),
        url: None,
        token_id: repo.is_none().then(Uuid::new_v4),
        pending_token_ids: Vec::new(),
    }
}

/// A frontend is stored once per name, its repository keeps its right to
/// deploy while another frontend still deploys from it, and a project's
/// frontends go with it.
#[sqlx::test]
async fn a_frontend_is_kept_once_and_its_repository_while_used(pool: PgPool) {
    use weft_dispatcher::frontends::{forget, list, record, repo_still_used};
    let id = project(&pool).await;
    let shop = frontend(id, "shop", Some("me/site"));
    assert!(record(&pool, &shop, 1).await.unwrap());
    assert!(!record(&pool, &shop, 2).await.unwrap(), "a name is taken once per project");
    assert!(record(&pool, &frontend(id, "admin", Some("me/site")), 3).await.unwrap());
    assert!(record(&pool, &frontend(id, "app", None), 4).await.unwrap());
    assert_eq!(list(&pool, id).await.unwrap().iter().map(|f| f.name.as_str()).collect::<Vec<_>>(), ["admin", "app", "shop"]);
    let site = shop.repo.clone().unwrap();
    assert!(repo_still_used(&pool, &site, id, "shop").await.unwrap(), "admin still deploys from it");
    forget(&pool, id, "admin").await.unwrap();
    assert!(!repo_still_used(&pool, &site, id, "shop").await.unwrap());
    let hosted_without_repo = sqlx::query(
        "INSERT INTO project_frontend (project_id, name, host, token_id, added_unix) VALUES ($1, 'x', 'cloud_run', $2, 0)",
    )
    .bind(id)
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await;
    assert!(hosted_without_repo.is_err(), "a hosted frontend always names the repository that deploys it");
    sqlx::query("DELETE FROM project WHERE id = $1").bind(id).execute(&pool).await.unwrap();
    assert!(list(&pool, id).await.unwrap().is_empty(), "a project's frontends go with it");
}

/// A platform door that answers each `serve` from a script (`true`
/// refuses), and records the names it was asked to serve.
struct ScriptedDoor {
    refuse: std::sync::Mutex<std::collections::VecDeque<bool>>,
    asked: std::sync::Mutex<Vec<Vec<String>>>,
}

impl ScriptedDoor {
    fn new(refuse: &[bool]) -> Self {
        Self { refuse: std::sync::Mutex::new(refuse.iter().copied().collect()), asked: std::sync::Mutex::new(Vec::new()) }
    }

    fn asked(&self) -> Vec<Vec<String>> {
        self.asked.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl weft_platform_traits::DomainHosting for ScriptedDoor {
    async fn serve(&self, names: &[String]) -> anyhow::Result<Option<std::net::IpAddr>> {
        self.asked.lock().unwrap().push(names.to_vec());
        let refuse = self.refuse.lock().unwrap().pop_front().expect("the script names every serve");
        anyhow::ensure!(!refuse, "the platform refused");
        Ok((!names.is_empty()).then_some(weft_platform_traits::domains::fake::FAKE_DOOR))
    }

    async fn address(&self) -> anyhow::Result<Option<std::net::IpAddr>> {
        unreachable!("not asked by a change to the domains")
    }

    fn cost(&self) -> Option<&'static str> {
        None
    }
}

fn names(domains: Vec<Domain>) -> Vec<String> {
    domains.into_iter().map(|d| d.name).collect()
}

/// A removal the door refuses leaves the domain stored, so `weft domain
/// rm` can run again; one it accepts forgets it.
#[sqlx::test]
async fn a_domain_is_forgotten_only_once_the_door_let_it_go(pool: PgPool) {
    use weft_dispatcher::domains::{add, list, unserve, ServeError};
    project(&pool).await;
    for name in ["a.example.com", "b.example.com"] {
        add(&pool, &Domain { name: name.into(), serves: DomainServes::Install }, 1).await.unwrap();
    }
    let door = ScriptedDoor::new(&[true, false]);
    let stored = list(&pool).await.unwrap();
    let refused = unserve(&pool, &door, &stored, "a.example.com").await.unwrap_err();
    assert!(matches!(refused, ServeError::Door(_)), "{refused:?}");
    assert_eq!(names(list(&pool).await.unwrap()), vec!["a.example.com", "b.example.com"], "still stored, so the removal can run again");
    unserve(&pool, &door, &stored, "a.example.com").await.unwrap();
    assert_eq!(names(list(&pool).await.unwrap()), vec!["b.example.com"]);
    assert_eq!(door.asked(), vec![vec!["b.example.com".to_string()]; 2], "the door is asked for the names that remain");
}

/// A new domain the door refuses is forgotten again, and its removal
/// wakes the loop that puts the door back on what remains, so no door
/// stands for a domain nobody kept.
#[sqlx::test]
async fn a_refused_new_domain_is_forgotten_and_wakes_the_door(pool: PgPool) {
    use weft_dispatcher::domains::{add, list, serve_added, Added, ServeError, DOMAINS_CHANNEL};
    project(&pool).await;
    static CHANNELS: &[&str] = &[DOMAINS_CHANNEL];
    let watch = weft_task_store::pg_signal::PgSignalWatch::start(&pool.connect_options(), CHANNELS).await.unwrap();
    let mut heard = watch.subscribe();
    let domain = Domain { name: "a.example.com".into(), serves: DomainServes::Install };
    let new = add(&pool, &domain, 1).await.unwrap() == Added::New;
    async fn woken(heard: &mut weft_task_store::pg_signal::Subscription) -> bool {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        heard.woken_before(deadline, |c, _| c == DOMAINS_CHANNEL).await.unwrap()
    }
    assert!(woken(&mut heard).await, "a stored domain wakes the door's loop");
    let door = ScriptedDoor::new(&[true]);
    let refused = serve_added(&pool, &door, &domain.name, new).await.unwrap_err();
    assert!(matches!(refused, ServeError::Door(_)), "{refused:?}");
    assert!(list(&pool).await.unwrap().is_empty());
    assert_eq!(door.asked(), vec![vec!["a.example.com".to_string()]], "the request does not settle the door itself");
    assert!(woken(&mut heard).await, "and so does one forgotten again");
}

/// A domain stored before the add that the door refused stays stored:
/// adding it again retries.
#[sqlx::test]
async fn a_refused_add_of_a_stored_domain_keeps_it(pool: PgPool) {
    use weft_dispatcher::domains::{add, list, serve_added};
    project(&pool).await;
    let domain = Domain { name: "a.example.com".into(), serves: DomainServes::Install };
    add(&pool, &domain, 1).await.unwrap();
    let door = ScriptedDoor::new(&[true]);
    serve_added(&pool, &door, &domain.name, false).await.unwrap_err();
    assert_eq!(list(&pool).await.unwrap(), vec![domain]);
    assert_eq!(door.asked().len(), 1, "nothing is put back for a domain that stays");
}

/// The loop that keeps the door on the stored domains asks the platform
/// nothing while the door serves exactly them, retries a refusal later
/// each time while recording why, and clears the refusal once accepted.
#[sqlx::test]
async fn the_door_follows_the_stored_domains_and_backs_off_a_refusal(pool: PgPool) {
    use weft_dispatcher::domains::{add, door_state, settle};
    use weft_task_store::drain::DrainStep;
    project(&pool).await;
    add(&pool, &Domain { name: "a.example.com".into(), serves: DomainServes::Install }, 1).await.unwrap();
    let door = ScriptedDoor::new(&[false, true, true, false]);
    assert_eq!(settle(&pool, &door).await.unwrap(), DrainStep::Done);
    assert_eq!(settle(&pool, &door).await.unwrap(), DrainStep::Done, "nothing new: the platform is not asked");
    assert_eq!(door.asked().len(), 1);

    add(&pool, &Domain { name: "b.example.com".into(), serves: DomainServes::Install }, 2).await.unwrap();
    let DrainStep::RetryIn(first) = settle(&pool, &door).await.unwrap() else { panic!("a refusal is retried") };
    let DrainStep::RetryIn(second) = settle(&pool, &door).await.unwrap() else { panic!("a refusal is retried") };
    assert_eq!(second, first * 2, "later each time");
    let refused = door_state(&pool).await.unwrap();
    assert_eq!(refused.refusals, 2);
    assert!(refused.refused.is_some_and(|why| why.contains("refused")), "the reason is kept for `weft domain list`");
    assert_eq!(refused.served.map(|(names, _)| names), Some(vec!["a.example.com".to_string()]), "what the door last accepted stays");

    assert_eq!(settle(&pool, &door).await.unwrap(), DrainStep::Done);
    let accepted = door_state(&pool).await.unwrap();
    assert_eq!((accepted.refused, accepted.refusals), (None, 0));
    assert_eq!(accepted.served.map(|(names, _)| names), Some(vec!["a.example.com".to_string(), "b.example.com".to_string()]));
}
