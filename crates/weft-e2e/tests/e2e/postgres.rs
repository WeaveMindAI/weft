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
//!   - a client past the connections the pooler holds waits its turn
//!     and is served once one frees up, instead of being refused;
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

/// How many connections the database's pooler holds for queries, as its
/// card says (`queries (<db>): ...; <open> of <pool size> connections open`).
async fn pooler_query_connections(project: &Project) -> Result<usize> {
    let card: serde_json::Value = serde_json::from_str(project.weft(&["infra", "show", "db", "--json"]).await?.trim())?;
    let pooler = card["items"]
        .as_array()
        .and_then(|items| items.iter().find(|i| i["label"] == "Pooler"))
        .and_then(|i| i["value"].as_str())
        .ok_or_else(|| anyhow::anyhow!("the card has no Pooler item: {card}"))?;
    let queries = pooler.lines().find(|l| l.starts_with("queries")).ok_or_else(|| anyhow::anyhow!("no queries line: {pooler}"))?;
    let size = queries
        .rsplit_once(" connections open")
        .and_then(|(before, _)| before.rsplit_once(" of "))
        .map(|(_, size)| size.trim().parse::<usize>())
        .ok_or_else(|| anyhow::anyhow!("no pool size in: {queries}"))??;
    Ok(size)
}

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

/// `weft infra env` writes the database's password into an env file
/// without printing it. Once a run has taken the password it refuses and
/// names `weft infra press`; `weft infra show` lists the card's reset
/// button without printing anything secret; pressing it never asks, and
/// after it `env` writes the fresh password and
/// the user in one go, printing the user, keeping the file's other lines.
#[tokio::test]
async fn the_password_goes_into_an_env_file_and_a_reset_gets_a_new_one() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let disp = ensure::up().await?;
    let mut project = Project::prepare("postgres_db", disp).await?;
    project.substitute_in_main("__E2E_BODY__", BODY)?;
    infra::start_and_wait_running(&mut project, "db").await?;

    // Starting the database runs its setup, which takes the password: by
    // the time anyone looks, the card says it was handed over. So the
    // plain call refuses and names the press that gets a new one.
    let env = project.dir().join("web.env");
    let into = env.to_string_lossy().to_string();
    let only_secret: [&str; 7] = ["infra", "env", "db", "--into", &into, "--as", "PG_PASSWORD"];
    let refused = project.weft_refused(&only_secret).await?;
    anyhow::ensure!(refused.contains("weft infra press"), "the refusal should name `weft infra press`: {refused}");

    // The card lists the reset button, and the action to press it by.
    let shown = project.weft(&["infra", "show", "db"]).await?;
    anyhow::ensure!(shown.contains("weft infra press db reset_password"), "show should list the reset button: {shown}");

    let press: [&str; 4] = ["infra", "press", "db", "reset_password"];
    // What a failure message may show of a text holding the password:
    // the name of each line, never its value.
    let names = |text: &str| -> Vec<String> {
        text.lines().map(|l| l.split_once('=').map_or("<no name>", |(name, _)| name).to_string()).collect()
    };
    let password_in = |text: &str| -> Result<String> {
        let line = text
            .lines()
            .find_map(|l| l.strip_prefix("PG_PASSWORD="))
            .ok_or_else(|| anyhow::anyhow!("the env file does not set PG_PASSWORD, its lines: {:?}", names(text)))?;
        anyhow::ensure!(!line.trim().is_empty(), "an empty password was written");
        Ok(line.trim().to_string())
    };

    let pressed = project.weft(&press).await?;
    // Now the card holds the fresh password, and show still masks it.
    let shown = project.weft(&["infra", "show", "db"]).await?;
    let shown_json = project.weft(&["infra", "show", "db", "--json"]).await?;
    let secret: [&str; 9] =
        ["infra", "env", "db", "--into", &into, "--set", "PG_PASSWORD=Password", "--set", "PG_USER=User"];
    let written = project.weft(&secret).await?;
    anyhow::ensure!(written.contains("PG_USER="), "env should print the plain values it wrote, its lines: {:?}", names(&written));
    // Written into the file is taken: the card shows it handed over now.
    let taken = project.weft_refused(&only_secret).await?;
    anyhow::ensure!(taken.contains("weft infra press"), "the password env wrote should be handed over: {taken}");
    let user_line = std::fs::read_to_string(&env)?;
    anyhow::ensure!(
        user_line.lines().any(|l| l.starts_with("PG_USER=") && l.len() > "PG_USER=".len()),
        "the env file does not set PG_USER, its lines: {:?}",
        names(&user_line)
    );
    let first = password_in(&std::fs::read_to_string(&env)?)?;
    for (what, out) in [("press", &pressed), ("show", &shown), ("show --json", &shown_json), ("env", &written)] {
        anyhow::ensure!(!out.contains(&first), "`{what}` printed the password");
    }
    let mode = std::fs::metadata(&env)?.permissions().mode() & 0o777;
    anyhow::ensure!(mode == 0o600, "a new env file should be the owner's only, got {mode:o}");
    // The press reached the program's connection too, so its next run signs
    // in with the new password.
    round_trip(&mut project).await?;

    // Another reset replaces only its own line.
    let before = std::fs::read_to_string(&env)?;
    std::fs::write(&env, format!("OTHER=kept\n{before}"))?;
    project.weft(&press).await?;
    project.weft(&only_secret).await?;
    let after = std::fs::read_to_string(&env)?;
    anyhow::ensure!(after.starts_with("OTHER=kept\nPG_PASSWORD=") && after.contains("\nPG_USER="), "the other line was not kept, its lines: {:?}", names(&after));
    anyhow::ensure!(password_in(&after)? != first, "the reset wrote the old password back");

    // The program's own next run picks the new password up by itself.
    round_trip(&mut project).await?;
    infra::terminate_and_wait_gone(&project, "db").await?;
    project.finish().await
}

/// The way a frontend reaches the program's database: turn the node's
/// `reachable` input on, find the door with `weft infra list-doors`, write
/// the user, the database and a fresh password into an env file with
/// `weft infra env`, and open Postgres from outside weft with nothing
/// else. More clients than the pooler holds connections for all get in:
/// the one past them waits, the card says so, and it is served once
/// another lets go. The program's own run keeps working beside it.
#[tokio::test]
async fn a_client_outside_weft_opens_the_database_through_its_door() -> Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("postgres_db", disp).await?;
    project.substitute_in_main("__E2E_BODY__", BODY)?;
    project.set_node_config("db", "reachable", "true")?;
    infra::start_and_wait_running(&mut project, "db").await?;

    let config = outside_client(&project, "db", "front.env").await?;
    anyhow::ensure!(config.get_dbname() == Some("notes-db"), "a name with a dash is the database's own: {:?}", config.get_dbname());
    let session = db::Session::connect(&config).await?;
    let one: i32 = session.scalar("SELECT 1", &[]).await?;
    anyhow::ensure!(one == 1, "the database answered through its door");
    drop(session);

    // Every connection the pooler holds for queries, taken: in session
    // pooling a client keeps its connection for as long as it is
    // connected.
    let pooled = pooler_query_connections(&project).await?;
    let mut held = Vec::new();
    for _ in 0..pooled {
        let session = db::Session::connect(&config).await?;
        let _: i32 = tokio::time::timeout(std::time::Duration::from_secs(60), session.scalar("SELECT 1", &[])).await??;
        held.push(session);
    }
    // One more gets in and waits for a connection rather than being
    // refused, and the card shows it waiting.
    let past = db::Session::connect(&config).await?;
    let waiting = tokio::spawn(async move { past.scalar::<i32>("SELECT 2", &[]).await });
    // The card is read when asked, so it is asked until the pooler has
    // queued the client, for as long as a client takes to sign in at most.
    let until = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    let card = loop {
        let card = project.weft(&["infra", "show", "db", "--json"]).await?;
        if card.contains("1 waiting") || tokio::time::Instant::now() >= until {
            break card;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    };
    anyhow::ensure!(card.contains("1 waiting"), "the card should show the client waiting: {card}");
    anyhow::ensure!(!waiting.is_finished(), "a client past the pooler's {pooled} connections was served at once");
    drop(held.pop());
    let two = tokio::time::timeout(std::time::Duration::from_secs(30), waiting).await???;
    anyhow::ensure!(two == 2, "the waiting client was served once a connection came free");
    drop(held);

    round_trip(&mut project).await?;
    infra::terminate_and_wait_gone(&project, "db").await?;
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
