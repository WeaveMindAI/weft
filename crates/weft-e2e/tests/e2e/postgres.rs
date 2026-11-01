//! Postgres end to end: a database the project runs itself, reached
//! through the connection that database published.
//!
//! What only a real run can prove:
//!   - the database node brings a real Postgres up and hands out a
//!     connection that actually opens it, with no credential anywhere
//!     in the project;
//!   - the query nodes run real SQL through it, in order, each waiting
//!     on the rows the previous one answered;
//!   - the value survives the whole round trip (the read matches on
//!     the row the write handed back, not on a constant);
//!   - a SECOND run still works, which it only can because the
//!     connection was stored the first time: by then the database
//!     refuses to hand its password over again. The row count growing
//!     also proves the data is really in a database;
//!   - one firing's statements share one session through the
//!     database's pooler, and the program signs in as its own role,
//!     never the superuser;
//!   - a watching query reached through the pooler keeps its one
//!     connection past the minute after which the database closes an
//!     idle session, and a write still fires it;
//!   - the roles the node lists keep tenants apart under a `FORCE`
//!     policy, one marked to bypass it writes past it, and a login role
//!     signs in through the pooler with its own password;
//!   - a database named with a dash keeps its name, and one the pooler
//!     cannot read as written is reached through a door made from it;
//!   - terminating takes the connection away with the database.
//!
//! Needs an install, no external service and no credentials.
#![cfg(feature = "e2e")]

use anyhow::Result;
use serde_json::json;

use weft_e2e::{ensure, infra, project::Project, run, SettledRun};
use weft_task_store::db::{self, Client as _};

/// The postgres connections belonging to THIS project. The listing is
/// tenant-wide, and every e2e shares one tenant, so a sibling test's
/// database (or a leftover from a crashed run) would otherwise be
/// counted here and fail this test for something it did not do.
async fn postgres_grants_of_this_project(project: &Project) -> Result<Vec<serde_json::Value>> {
    let all: Vec<serde_json::Value> =
        project.dispatcher().get_json("/access/grants?service=postgres").await?;
    let mine = json!(project.id().to_string());
    Ok(all.into_iter().filter(|g| g.get("projectId") == Some(&mine)).collect())
}

/// The one sentence this test writes, and matches on when reading
/// back. Substituted into the fixture so the two cannot drift.
const BODY: &str = "written by the postgres e2e";

/// How a client outside weft signs in to the database `node` of
/// `project`, whose `reachable` input is on: its door from `weft infra
/// list-doors`, and the user, the database and a fresh password from its
/// card, written into the env file `file` by `weft infra env` (starting
/// the database took the password it had). Checks the user is the
/// program's role, never the superuser.
async fn outside_client(project: &Project, node: &str, file: &str) -> Result<db::Config> {
    let listed: weft_core::infra::wire::DoorsResponse = serde_json::from_str(project.weft(&["infra", "list-doors", "--json"]).await?.trim())?;
    let door = listed
        .doors
        .iter()
        .find(|d| d.copy.node == node && d.endpoint == "sql")
        .ok_or_else(|| anyhow::anyhow!("the database's sql endpoint has a door: {listed:?}"))?;
    let (host, port) = door.address.rsplit_once(':').ok_or_else(|| anyhow::anyhow!("the door is host:port: {door:?}"))?;
    let port: u16 = port.parse()?;
    project.weft(&["infra", "press", node, "reset_password"]).await?;
    let env = project.dir().join(file);
    let into = env.to_string_lossy().to_string();
    project
        .weft(&["infra", "env", node, "--into", &into, "--set", "PG_PASSWORD=Password", "--set", "PG_USER=User", "--set", "PG_DB=Database"])
        .await?;
    let text = std::fs::read_to_string(&env)?;
    let value = |name: &str| -> Result<String> {
        text.lines()
            .find_map(|l| l.strip_prefix(&format!("{name}=")))
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .ok_or_else(|| anyhow::anyhow!("the env file does not set {name}"))
    };
    anyhow::ensure!(value("PG_USER")? == "app", "an outside client signs in as the program's role, never the superuser");
    let mut config = db::Config::new();
    config.host(host).port(port).user(value("PG_USER")?).password(value("PG_PASSWORD")?).dbname(value("PG_DB")?);
    Ok(config)
}

/// Run the graph once and hand back the settled run to assert on.
async fn round_trip(project: &mut Project) -> Result<SettledRun> {
    let settled = run::run_and_settle(project).await?;
    settled.completed()?;
    Ok(settled)
}

#[tokio::test]
async fn a_project_runs_its_own_postgres_and_talks_to_it() -> Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("postgres_db", disp).await?;
    project.substitute_in_main("__E2E_BODY__", BODY)?;

    // Builds the credential image, applies the manifests, waits until
    // Postgres itself reports it is serving.
    infra::start_and_wait_running(&mut project, "db").await?;

    // The first run is the one that reads the password off the
    // database and publishes the connection.
    let first = round_trip(&mut project).await?;
    first.assert_input("out", "data", &json!([{ "body": BODY }]))?;

    // An optional port that stayed silent reached the database as SQL
    // NULL instead of refusing the query. The author wrote `photo?`,
    // sent nothing, and the row is written with an empty column: one
    // node, no branch, no split insert.
    first.assert_input("absent", "data", &json!([{ "body": "no picture", "tag": null }]))?;
    first.assert_input("same", "data", &json!([{ "name": "one session", "signed_in": "app", "superuser": false }]))?;

    // The second run: the database no longer hands its password out,
    // so this only works through the stored connection. Two rows now,
    // because the first run's row is still there.
    round_trip(&mut project)
        .await?
        .assert_input("out", "data", &json!([{ "body": BODY }, { "body": BODY }]))?;

    // The connection exists while the database does, and it is the
    // node's own: nobody connected it by hand.
    let live = postgres_grants_of_this_project(&project).await?;
    anyhow::ensure!(live.len() == 1, "expected the database's one connection, got {live:?}");

    infra::terminate_and_wait_gone(&project, "db").await?;

    // And it goes with the database. A credential that outlived the
    // thing it opens would name nothing, and nothing downstream could
    // use it or tell it apart from a working one.
    let after = postgres_grants_of_this_project(&project).await?;
    anyhow::ensure!(
        after.is_empty(),
        "terminating the database left its connection behind: {after:?}"
    );

    project.finish().await
}

/// The roles the next test lists: one per kind of tenant, one that signs
/// in by itself, one that writes past every policy.
const ROLES: &str = r#"[{"name": "tenant_app"}, {"name": "tenant_site", "login": true}, {"name": "tenant_admin", "bypassRowLevelSecurity": true}]"#;

/// Whether `outcome` is the refusal of a row-level security policy.
fn refused_by_a_policy<T: std::fmt::Debug>(outcome: Result<T, impl std::fmt::Display>) -> bool {
    matches!(outcome, Err(e) if e.to_string().contains("row-level security"))
}

/// How a program keeps tenants apart with roles the node lists: `app`
/// (the tables' owner, bound by a `FORCE` policy like everyone) `set
/// role`s to a tenant's role and writes only that tenant's rows, a role
/// marked `bypassRowLevelSecurity` writes past the policy, and a `login`
/// role signs in through the pooler with its own password from the card
/// (`app`'s does not open it), handed out once and minted again by its own
/// Reset button. Taking roles off the list restarts the database, which
/// sets up the one still listed again without failing, and takes back what
/// the list gave the others (`set role`, signing in, passing policies)
/// while keeping them and their rows.
#[tokio::test]
async fn roles_keep_tenants_apart_and_a_login_role_signs_in_by_itself() -> Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("postgres_db", disp).await?;
    project.substitute_in_main("__E2E_BODY__", BODY)?;
    project.set_node_config("db", "reachable", "true")?;
    project.set_node_config("db", "roles", ROLES)?;
    infra::start_and_wait_running(&mut project, "db").await?;

    let app = outside_client(&project, "db", "front.env").await?;
    let owner = db::Session::connect(&app).await?;
    owner
        .batch_execute(
            "CREATE TABLE tenants (tenant text NOT NULL); \
             ALTER TABLE tenants ENABLE ROW LEVEL SECURITY; ALTER TABLE tenants FORCE ROW LEVEL SECURITY; \
             CREATE POLICY own ON tenants USING (tenant = current_user) WITH CHECK (tenant = current_user); \
             GRANT SELECT, INSERT ON tenants TO tenant_app, tenant_site, tenant_admin",
        )
        .await?;
    anyhow::ensure!(
        refused_by_a_policy(owner.execute("INSERT INTO tenants VALUES ('tenant_app')", &[]).await),
        "a FORCE policy binds the tables' owner too"
    );
    owner.batch_execute("SET ROLE tenant_app; INSERT INTO tenants VALUES ('tenant_app')").await?;
    anyhow::ensure!(
        refused_by_a_policy(owner.execute("INSERT INTO tenants VALUES ('tenant_site')", &[]).await),
        "a tenant's role writes only its own rows"
    );
    owner.batch_execute("SET ROLE tenant_admin; INSERT INTO tenants VALUES ('tenant_site'); RESET ROLE").await?;

    // The login role, with its own password from the card.
    let env = project.dir().join("site.env");
    let into = env.to_string_lossy().to_string();
    project.weft(&["infra", "env", "db", "--into", &into, "--set", "SITE_PASSWORD=tenant_site password"]).await?;
    let text = std::fs::read_to_string(&env)?;
    let password = text
        .lines()
        .find_map(|l| l.strip_prefix("SITE_PASSWORD="))
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow::anyhow!("the env file does not set SITE_PASSWORD"))?;
    let mut site = app.clone();
    site.user("tenant_site").password(password.as_str());
    let signed_in = db::Session::connect(&site).await?;
    let seen: Vec<String> = signed_in.scalars("SELECT tenant FROM tenants", &[]).await?;
    anyhow::ensure!(seen == ["tenant_site"], "the login role sees its own rows only, got {seen:?}");
    drop(signed_in);
    let mut wrong = site.clone();
    wrong.password(app.get_password().unwrap_or_default());
    anyhow::ensure!(db::Session::connect(&wrong).await.is_err(), "app's password does not sign the login role in");

    // Handed out once, like app's: taken by the env file, then a reset
    // mints a new one, which the old one no longer signs in after.
    let again: [&str; 7] = ["infra", "env", "db", "--into", &into, "--set", "SITE_PASSWORD=tenant_site password"];
    let refused = project.weft_refused(&again).await?;
    anyhow::ensure!(refused.contains("weft infra press db reset_password:tenant_site"), "{refused}");
    project.weft(&["infra", "press", "db", "reset_password:tenant_site"]).await?;
    project.weft(&again).await?;
    let fresh = std::fs::read_to_string(&env)?
        .lines()
        .find_map(|l| l.strip_prefix("SITE_PASSWORD="))
        .map(|v| v.trim().to_string())
        .ok_or_else(|| anyhow::anyhow!("the env file does not set SITE_PASSWORD after the reset"))?;
    anyhow::ensure!(fresh != password, "the reset minted a new password");
    anyhow::ensure!(db::Session::connect(&site).await.is_err(), "the old password signs in no more");
    let password = fresh;
    site.password(password.as_str());
    db::Session::connect(&site).await?;
    drop(owner);

    // Off the list: the restart sets up the one still listed again, and the
    // two taken off stay in the database with their rows, but lose what the
    // list gave them: `app`'s `set role`, signing in, writing past policies.
    project.set_node_config("db", "roles", r#"[{"name": "tenant_app"}]"#)?;
    project.weft(&["infra", "upgrade"]).await?;
    infra::wait_running(&project, "db").await?;
    let app = outside_client(&project, "db", "front.env").await?;
    let owner = db::Session::connect(&app).await?;
    let gone = "FROM pg_roles WHERE rolname IN ('tenant_site', 'tenant_admin')";
    let kept: i64 = owner.scalar(&format!("SELECT count(*) {gone}"), &[]).await?;
    let login: bool = owner.scalar(&format!("SELECT bool_or(rolcanlogin) {gone}"), &[]).await?;
    let bypass: bool = owner.scalar(&format!("SELECT bool_or(rolbypassrls) {gone}"), &[]).await?;
    anyhow::ensure!(kept == 2, "roles taken off the list stay in the database");
    anyhow::ensure!(!login && !bypass, "they sign in no more and pass the policies no more (login {login}, bypass {bypass})");
    for gone in ["tenant_site", "tenant_admin"] {
        let refused = owner.batch_execute(&format!("SET ROLE {gone}")).await;
        anyhow::ensure!(refused.is_err(), "the program can no longer `set role {gone}`");
    }
    owner.batch_execute("SET ROLE tenant_app").await?;
    owner.batch_execute("RESET ROLE; ALTER TABLE tenants NO FORCE ROW LEVEL SECURITY").await?;
    let rows: i64 = owner.scalar("SELECT count(*) FROM tenants", &[]).await?;
    anyhow::ensure!(rows == 2, "the rows stay, got {rows}");
    let mut site = app.clone();
    site.user("tenant_site").password(password.as_str());
    anyhow::ensure!(db::Session::connect(&site).await.is_err(), "the pooler lets a role taken off the list in no more");
    let card = project.weft(&["infra", "show", "db"]).await?;
    anyhow::ensure!(!card.contains("tenant_site password"), "its card line goes: {card}");
    drop(owner);

    infra::terminate_and_wait_gone(&project, "db").await?;
    project.finish().await
}

/// The next line of the ndjson answer `answer` as JSON, reading what
/// arrives until one is whole (`pending` keeps what came after it), and
/// failing after `within` with what was read.
async fn next_line(answer: &mut reqwest::Response, pending: &mut String, within: std::time::Duration) -> Result<serde_json::Value> {
    let until = tokio::time::Instant::now() + within;
    loop {
        if let Some((line, rest)) = pending.split_once('\n') {
            // A blank line is the stream's keepalive, sent while the
            // program has nothing new to say.
            if line.trim().is_empty() {
                *pending = rest.to_string();
                continue;
            }
            let value = serde_json::from_str(line).map_err(|e| anyhow::anyhow!("a line of the feed is not JSON ({e}): {line}"))?;
            *pending = rest.to_string();
            return Ok(value);
        }
        let chunk = tokio::time::timeout_at(until, answer.chunk())
            .await
            .map_err(|_| anyhow::anyhow!("no whole line within {}s; read so far: {pending:?}", within.as_secs()))??
            .ok_or_else(|| anyhow::anyhow!("the feed ended; read so far: {pending:?}"))?;
        pending.push_str(&String::from_utf8_lossy(&chunk));
    }
}

/// A route's caller keeps a count live through a PostgresWatchQuery
/// reached through the pooler: the first line comes at once, a write
/// with a NOTIFY sends the next, and after the watch sat idle longer than
/// the minute the database gives an idle session, a write still reaches
/// it. The database is named with a dot, so every client, the watch
/// included, reaches it through the door the pooler made from the name.
#[tokio::test]
async fn a_watching_query_keeps_watching_past_the_idle_limit() -> Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("postgres_watch", disp.clone()).await?;
    let base = project.unique_live_path()?;
    infra::start_and_wait_running(&mut project, "db").await?;

    let config = outside_client(&project, "db", "watch.env").await?;
    let door = config.get_dbname().unwrap_or_default().to_string();
    anyhow::ensure!(door.starts_with("db_watch_me_"), "a name the pooler cannot read is reached by a door made from it: {door}");
    let writer = db::Session::connect(&config).await?;
    // The writer sits idle through the wait below too, and the database
    // would close it; it lifts the limit for its own session, as any
    // client that keeps a connection idle on purpose does.
    writer.batch_execute("SET idle_session_timeout = 0; CREATE TABLE notes (id serial PRIMARY KEY)").await?;
    let database: String = writer.scalar("SELECT current_database()", &[]).await?;
    anyhow::ensure!(database == "watch.me", "the door leads to the database under its own name, got {database}");
    let write = || writer.batch_execute("INSERT INTO notes DEFAULT VALUES; SELECT pg_notify('notes_changed', '')");

    project.activate().await?;
    let url = format!("{}/connect/{}/count", disp.base(), base.trim_start_matches('/'));
    let mut answer = weft_core::net::http_client().build()?.get(&url).send().await?;
    anyhow::ensure!(answer.status().is_success(), "GET {url} answered {}", answer.status());
    let mut pending = String::new();
    let soon = std::time::Duration::from_secs(60);
    anyhow::ensure!(next_line(&mut answer, &mut pending, soon).await? == json!({ "n": 0 }));
    write().await?;
    anyhow::ensure!(next_line(&mut answer, &mut pending, soon).await? == json!({ "n": 1 }));

    // Longer than the minute after which the database closes a session
    // that sits idle, and well short of the watch's own cadence, so only
    // the NOTIFY can wake it.
    tokio::time::sleep(std::time::Duration::from_secs(75)).await;
    write().await?;
    let after = next_line(&mut answer, &mut pending, soon).await?;
    anyhow::ensure!(after == json!({ "n": 2 }), "the watch should still be watching after sitting idle, got {after}");

    drop(answer);
    drop(writer);
    infra::terminate_and_wait_gone(&project, "db").await?;
    project.finish().await
}
