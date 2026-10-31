//! PostgresDatabase self-tests: the declared infrastructure, and the
//! two properties the whole design rests on. The running database
//! itself is exercised end to end.

use serde_json::json;

use weft::{EndpointMethod, FakeRig, NodeTest, WeftResult};

use super::{
    database_name, extension_names, memory_mib, PostgresVersion, pooler_config, pooler_database, pooler_hba, postgres_version, watch_database,
    PostgresDatabaseNode, Tuning,
    CREDENTIAL_PATH, CREDENTIAL_STORED_PATH, HEALTH_PATH, LEAST_MEMORY_MIB,
};

/// The service this node declares it publishes, spelled here as the
/// test's own expectation rather than read from the node, so a change
/// to the declaration shows up as a failing test.
const SERVICE: &str = "postgres";

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("declares_a_database_that_reads_its_password_from_a_file", declares),
        NodeTest::fake("describes_itself_identically_every_time", stable),
        NodeTest::fake("its_machine_is_what_its_settings_say", sized),
        NodeTest::fake("keeps_the_credential_endpoint_to_the_project", internal),
        NodeTest::fake("waits_for_postgres_to_answer_for_itself", ready),
        NodeTest::fake("every_client_comes_in_through_the_pooler", one_door),
        NodeTest::basic("the_pooler_keeps_sessions_and_its_connections_to_what_postgres_serves", pooler_settings),
        NodeTest::basic("only_the_program_s_role_signs_in_through_the_pooler", pooler_sign_ins),
        NodeTest::basic("any_database_name_is_kept_as_postgres_has_it", database_names),
        NodeTest::basic("a_name_the_pooler_cannot_read_gets_a_door_of_its_own", doors),
        NodeTest::fake("a_database_named_with_a_dash_is_reached_by_that_name", dashed),
        NodeTest::fake("a_database_the_pooler_cannot_name_is_published_by_its_door", aliased),
        NodeTest::fake("each_listed_extension_is_handed_to_the_credential_server", extensions),
        NodeTest::basic("a_version_reads_as_its_tag_and_major_and_an_old_one_says_how_to_move", versions),
        NodeTest::fake("an_existing_database_on_an_old_postgres_runs_with_a_warning", old_postgres),
        NodeTest::basic("a_pooled_connection_idle_too_long_is_never_handed_out", idle_close),
        NodeTest::basic("memory_reads_in_every_unit_the_input_takes", memory_units),
        NodeTest::basic("postgres_is_tuned_to_the_memory_it_has", tuned),
        NodeTest::basic("a_machine_too_small_for_one_worker_is_refused", too_small),
        NodeTest::basic("the_least_memory_is_where_one_worker_fits_and_the_input_says_so", least_memory),
        NodeTest::basic("a_full_database_is_told_by_the_runs_it_holds_back", full_waits),
        NodeTest::fake("postgres_runs_with_its_tuning_and_its_share_of_memory", runs_tuned),
        NodeTest::fake("the_first_run_reads_the_password_and_retires_it", first_run),
        NodeTest::fake("a_later_run_never_asks_the_database_again", later_run),
        NodeTest::fake("a_replaced_disk_makes_the_node_ask_again", replaced_disk),
        NodeTest::fake("an_answer_that_is_neither_yes_nor_no_fails", garbled),
        NodeTest::fake("a_refusal_from_the_credential_server_surfaces", refused),
        NodeTest::fake("a_unit_an_older_weft_set_up_is_refused_naming_the_upgrade", older_unit),
    ]
}

fn config() -> serde_json::Value {
    json!({ "database": "app", "storage": "10Gi", "version": "17", "reachable": false, "cpu": "0.25", "memory": "1Gi" })
}

/// The same database, with the author saying something outside the
/// program may connect to it.
fn reachable_config() -> serde_json::Value {
    json!({ "database": "app", "storage": "10Gi", "version": "17", "reachable": true })
}

/// The two addresses the node's own infrastructure answers on. The
/// SQL one is only ever split into host and port, never dialled here.
fn endpoints(rig: &FakeRig) {
    rig.declare_endpoint("sql", "http://wi-db-sql:5432");
    rig.declare_endpoint("credential", "http://wi-db-credential:8099");
    rig.answer_endpoint("credential", EndpointMethod::Get, HEALTH_PATH, json!({ "ok": true, "signsInAs": "app" }));
}

/// The password the node ends up publishing, whichever way it got it.
fn published_password(rig: &FakeRig) -> Option<String> {
    rig.published_values(SERVICE)?.get("password").cloned()
}

fn asked_for_the_password(rig: &FakeRig) -> bool {
    rig.endpoint_calls().iter().any(|c| {
        c.endpoint == "credential" && c.method == EndpointMethod::Get && c.path == CREDENTIAL_PATH
    })
}

/// What the node sent back to be retired, in order.
fn retired(rig: &FakeRig) -> Vec<String> {
    rig.endpoint_calls()
        .iter()
        .filter(|c| c.endpoint == "credential" && c.path == CREDENTIAL_STORED_PATH)
        .filter_map(|c| c.body.as_ref()?.get("password")?.as_str().map(str::to_string))
        .collect()
}

async fn declares(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run_provision_infra(&PostgresDatabaseNode, config()).await.ok()?;
    let spec = outcome.infra_spec()?;
    assert_eq!(spec.units.len(), 1);
    let unit = &spec.units[0];
    assert_eq!(unit.init_containers.len(), 1, "the password is minted before Postgres boots");
    assert_eq!(unit.containers.len(), 3, "Postgres, its pooler, and the server that hands the password over");
    let postgres = &unit.containers[0];
    assert!(
        postgres.env.iter().any(|e| matches!(e,
            weft::infra::EnvEntry { name, .. } if name == "POSTGRES_PASSWORD_FILE")),
        "Postgres reads the password from the shared volume"
    );
    assert!(
        !postgres.env.iter().any(|e| matches!(e,
            weft::infra::EnvEntry { name, .. } if name == "POSTGRES_PASSWORD")),
        "and never from a value carried in the spec"
    );
    assert_eq!(spec.volumes.len(), 3, "one disk for data and password, scratch dirs for the two sockets");
    assert!(
        spec.volumes[1..].iter().all(|v| matches!(v.kind, weft::infra::VolumeKind::Scratch { .. })),
        "a socket never lands on the disk"
    );
    let socket_dir = "/var/run/postgresql";
    let shares_socket = |name: &str| {
        unit.containers.iter().find(|c| c.name == name).expect("declared").mounts.iter().any(|m| m.path == socket_dir && m.volume == "socket")
    };
    assert!(shares_socket("postgres") && shares_socket("credential"), "the socket is how a password nobody holds gets replaced");
    // The socket signs in as the superuser with no password, and the
    // pooler has no business with that.
    assert!(!shares_socket("pooler"), "the pooler never reaches the superuser's door");
    let credential = unit.containers.iter().find(|c| c.name == "credential").expect("declared");
    for name in ["WEFT_SOCKET_DIR", "WEFT_ADMIN_USER", "WEFT_APP_USER", "WEFT_POOLER_USERS_FILE"] {
        assert!(
            credential.env.iter().any(|e| matches!(e,
                weft::infra::EnvEntry { name: n, .. } if n == name)),
            "the credential server is told {name} by the node"
        );
    }
    Ok(())
}

/// The machine follows the node's settings, and an emptied machine
/// type leaves the pick to the cloud.
async fn sized(rig: FakeRig) -> WeftResult<()> {
    let mut settings = config();
    settings["cpu"] = json!("2");
    settings["memory"] = json!("8Gi");
    settings["machineType"] = json!("");
    let outcome = rig.run_provision_infra(&PostgresDatabaseNode, settings).await.ok()?;
    let machine = &outcome.infra_spec()?.units[0].machine;
    assert_eq!((machine.cpu.as_deref(), machine.memory.as_deref(), machine.kind.as_deref()), (Some("2"), Some("8Gi"), None));
    Ok(())
}

/// The rule the whole design rests on: describing the database twice
/// produces the same thing, so starting it again changes nothing. A
/// generated password in the spec would break this and take the
/// database's logins with it.
async fn stable(rig: FakeRig) -> WeftResult<()> {
    let first = rig.run_provision_infra(&PostgresDatabaseNode, config()).await.ok()?;
    let second = rig.run_provision_infra(&PostgresDatabaseNode, config()).await.ok()?;
    assert_eq!(
        serde_json::to_value(first.infra_spec()?).expect("a spec serializes"),
        serde_json::to_value(second.infra_spec()?).expect("a spec serializes"),
        "two descriptions of the same database must be identical"
    );
    Ok(())
}

/// The rule every infra node lives by, on the node that has one of
/// each: an endpoint that HANDS OUT a credential is never open beyond the
/// project's own workers, whatever anybody asks for.
///
/// `sql` may be opened, because reaching Postgres there still costs a
/// password, and that is what lets a frontend on this machine use the
/// program's database instead of standing up a second one.
/// `credential` is the little server that MINTS that password: a door
/// onto it would hand the database to anything that could reach the
/// port, so it stays inside for ever and no input can open it.
async fn internal(rig: FakeRig) -> WeftResult<()> {
    // Nothing is reachable unless the author said so, and `reachable`
    // defaults off: a database on a machine somebody else can type on
    // is not something you get for not thinking about it.
    let closed = rig.run_provision_infra(&PostgresDatabaseNode, config()).await.ok()?;
    for endpoint in &closed.infra_spec()?.endpoints {
        assert!(
            matches!(endpoint.expose, weft::infra::Expose::Project),
            "'{}' is reachable on a database nobody asked to reach",
            endpoint.name
        );
    }

    // With it, exactly one endpoint opens. `credential` HANDS OUT the
    // password, so reaching it would give the database away to
    // anything that could reach the port; no input can open it.
    let open = rig.run_provision_infra(&PostgresDatabaseNode, reachable_config()).await.ok()?;
    for endpoint in &open.infra_spec()?.endpoints {
        let want_reachable = endpoint.name == "sql";
        assert_eq!(
            matches!(endpoint.expose, weft::infra::Expose::SameNetwork),
            want_reachable,
            "'{}' has the wrong exposure: only 'sql' may ever be reachable",
            endpoint.name
        );
    }
    Ok(())
}

/// Postgres accepts TCP while it is still starting up and refuses
/// every connection, so the port being bound is not readiness. The
/// probe has to ask the database the question it answers itself.
async fn ready(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run_provision_infra(&PostgresDatabaseNode, config()).await.ok()?;
    let spec = outcome.infra_spec()?;
    let postgres = &spec.units[0].containers[0];
    let probe = postgres.readiness.as_ref().expect("Postgres declares a readiness probe");
    let weft::infra::ProbeKind::Exec { command } = &probe.kind else {
        panic!("readiness must run the database's own check, not a port test: {:?}", probe.kind);
    };
    assert!(command.iter().any(|a| a == "pg_isready"), "{command:?}");
    // Over TCP, not the unix socket: the entrypoint runs a temporary
    // socket-only server while it initialises, so a socket check
    // reports ready while the port is still closed.
    assert!(command.iter().any(|a| a == "-h"), "the check must go over TCP: {command:?}");
    assert!(command.windows(2).any(|w| w == ["-p", "5433"]), "to Postgres's own port, not the pooler's: {command:?}");
    // A database that always exists: without `-d` the check signs in to
    // one named after the user, and the server logs that it is missing
    // on every check.
    assert!(command.windows(2).any(|w| w == ["-d", "postgres"]), "{command:?}");
    Ok(())
}

fn memory_units() -> WeftResult<()> {
    assert_eq!(memory_mib("1Gi")?, 1024);
    assert_eq!(memory_mib("1.5Gi")?, 1536);
    assert_eq!(memory_mib("512Mi")?, 512);
    // A gigabyte is 10^9 bytes, a little under 954 MiB, rounded down so
    // nothing is planned for memory the machine does not have.
    assert_eq!(memory_mib("1G")?, 953);
    assert_eq!(memory_mib("1500M")?, 1430);
    assert!(memory_mib("1GB").is_err());
    assert!(memory_mib("Gi").is_err());
    Ok(())
}

/// The default machine, worked through by hand from the formula: 1024
/// MiB less 352 for the rest of the machine leaves 672; a quarter of
/// that is the shared cache, a sixteenth each cleaner's, and the 378
/// left over holds 23 connections at 16 MiB each. Three are the
/// superuser's and two the margin; of the 18 left, a quarter (4) are
/// the watchers' and 14 the queries'.
fn tuned() -> WeftResult<()> {
    assert_eq!(
        Tuning::for_machine("1Gi")?,
        Tuning {
            limit_mib: 672,
            max_connections: 23,
            shared_buffers_mib: 168,
            work_mem_kib: 4318,
            maintenance_work_mem_mib: 42,
            effective_cache_size_mib: 504,
            query_pool: 14,
            watch_pool: 4,
        }
    );
    // More memory, more connections, each still given about Postgres's
    // own 4MB to sort in.
    let two = Tuning::for_machine("2Gi")?;
    assert_eq!(two.max_connections, 59);
    assert!((4096..4400).contains(&two.work_mem_kib), "{two:?}");
    // Past the most connections Postgres takes, the memory goes to each
    // connection's sorting instead.
    let big = Tuning::for_machine("64Gi")?;
    assert_eq!(big.max_connections, 200);
    assert_eq!(big.maintenance_work_mem_mib, 1024, "a vacuum uses at most 1GB");
    assert!(big.work_mem_kib > 100 * 1024, "{big:?}");
    // Whatever the machine, everything the settings allow at once fits
    // in Postgres's share.
    for memory in [&format!("{LEAST_MEMORY_MIB}Mi"), "1Gi", "1.5Gi", "2Gi", "4Gi", "8Gi", "16Gi", "64Gi"] {
        let t = Tuning::for_machine(memory)?;
        // The pooler never opens more than Postgres serves to anybody
        // but the superuser, with the margin to spare.
        assert_eq!(t.query_pool + t.watch_pool + 3 + 2, t.max_connections, "{memory}: {t:?}");
        assert!(t.query_pool >= 1 && t.watch_pool >= 1 && t.query_pool >= t.watch_pool, "{memory}: {t:?}");
        let at_once_kib = t.shared_buffers_mib * 1024
            + 3 * t.maintenance_work_mem_mib * 1024
            + t.max_connections * (8 * 1024 + 2 * t.work_mem_kib);
        assert!(at_once_kib <= t.limit_mib * 1024, "{memory}: {t:?}");
    }
    Ok(())
}

fn too_small() -> WeftResult<()> {
    let refused = Tuning::for_machine("512Mi").expect_err("512Mi leaves the pooler no connection to give");
    let floor = format!("raise `memory` to {LEAST_MEMORY_MIB}Mi or more");
    assert!(matches!(&refused, weft::WeftError::Input(m) if m.contains(&floor)), "{refused:?}");
    Ok(())
}

/// The notice about a full database counts runs, each once however
/// many times it is refused, says how many wait now, and comes at most
/// once a minute.
fn full_waits() -> WeftResult<()> {
    let t0 = std::time::Instant::now();
    let mut waits = super::super::postgres::FullWaits::default();
    waits.begin();
    waits.begin();
    let first = waits.notice(t0).expect("the first refusal is told");
    assert!(first.contains("2 run(s) wait for one now, 2 began waiting"), "{first}");
    assert_eq!(waits.notice(t0 + std::time::Duration::from_secs(30)), None, "once a minute at most");
    waits.waiting -= 1;
    let later = waits.notice(t0 + std::time::Duration::from_secs(61)).expect("a minute later");
    assert!(later.contains("1 run(s) wait for one now, 0 began waiting"), "{later}");
    Ok(())
}

/// The floor the refusal names is the real one (one MiB less is
/// refused, it is not), and the `memory` input's description states
/// the same number and the connection counts the formula gives.
fn least_memory() -> WeftResult<()> {
    let least = Tuning::for_machine(&format!("{LEAST_MEMORY_MIB}Mi"))?;
    assert_eq!((least.query_pool, least.watch_pool), (1, 1), "{least:?}");
    assert!(Tuning::for_machine(&format!("{}Mi", LEAST_MEMORY_MIB - 1)).is_err());
    let metadata: serde_json::Value = serde_json::from_str(include_str!("metadata.json")).expect("metadata.json reads");
    let memory = metadata["inputs"]
        .as_array()
        .and_then(|inputs| inputs.iter().find(|i| i["name"] == "memory"))
        .and_then(|i| i["description"].as_str())
        .expect("a memory input with a description");
    let says = format!("at least {LEAST_MEMORY_MIB}Mi.");
    assert!(memory.contains(&says), "the description says '{says}': {memory}");
    let serves = |at: &str| Tuning::for_machine(at).map(|t| t.max_connections);
    let says = format!("at 1Gi it serves {} connections, at 2Gi {}, at 4Gi {}", serves("1Gi")?, serves("2Gi")?, serves("4Gi")?);
    assert!(memory.contains(&says), "the description says '{says}': {memory}");
    Ok(())
}

async fn runs_tuned(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run_provision_infra(&PostgresDatabaseNode, config()).await.ok()?;
    let spec = outcome.infra_spec()?;
    let postgres = &spec.units[0].containers[0];
    assert_eq!(postgres.args[..postgres.args.len() - 2], Tuning::for_machine("1Gi")?.args());
    assert!(postgres.args.windows(2).any(|w| w == ["-c", "max_connections=23"]), "{:?}", postgres.args);
    assert!(postgres.args.windows(2).any(|w| w == ["-c", "superuser_reserved_connections=3"]), "{:?}", postgres.args);
    assert_eq!(postgres.limits.memory.as_deref(), Some("672Mi"));
    Ok(())
}

/// The first run: nothing published yet, so the password comes from
/// the database, gets published, and is retired only afterwards.
async fn first_run(rig: FakeRig) -> WeftResult<()> {
    endpoints(&rig);
    rig.answer_endpoint("credential", EndpointMethod::Get, CREDENTIAL_PATH, json!({ "password": "minted" }));
    rig.answer_endpoint("credential", EndpointMethod::Post, CREDENTIAL_STORED_PATH, json!({ "stored": true }));

    rig.run(&PostgresDatabaseNode, config()).await.ok()?;

    assert!(asked_for_the_password(&rig), "the first run has nowhere else to get it");
    assert_eq!(published_password(&rig).as_deref(), Some("minted"));
    let values = rig.published_values(SERVICE).expect("a connection was published");
    assert_eq!(values.get("host").map(String::as_str), Some("wi-db-sql"));
    assert_eq!(values.get("port").map(String::as_str), Some("5432"));
    assert_eq!(values.get("user").map(String::as_str), Some("app"), "the program's own role, never the superuser");
    assert_eq!(values.get("watchDatabase").map(String::as_str), Some("app_watch"));
    assert_eq!(retired(&rig), vec!["minted".to_string()], "retired once, after publishing");

    // Retiring AFTER publishing is the whole safety property: were it
    // the other way round, a failure in between would leave a
    // database nothing can sign in to.
    let calls = rig.endpoint_calls();
    let read = calls.iter().position(|c| c.path == CREDENTIAL_PATH).expect("read");
    let retire = calls.iter().position(|c| c.path == CREDENTIAL_STORED_PATH).expect("retire");
    assert!(read < retire, "the password is read before it is retired");
    Ok(())
}

/// A later run holds the connection it published, so it never asks
/// the database for the password again. That is what lets the
/// database stop handing it out at all.
async fn later_run(rig: FakeRig) -> WeftResult<()> {
    endpoints(&rig);
    rig.published_connection(SERVICE, &[
        ("host", "wi-db-sql"),
        ("port", "5432"),
        ("database", "app"),
        ("user", "weft"),
        ("password", "minted"),
        ("sslmode", "disable"),
    ]);
    // One retire, and no answer at all for reading the password: were
    // the node to ask, the run would fail rather than quietly get
    // away with it.
    rig.answer_endpoint("credential", EndpointMethod::Post, CREDENTIAL_STORED_PATH, json!({ "stored": true }));

    rig.run(&PostgresDatabaseNode, config()).await.ok()?;

    assert!(!asked_for_the_password(&rig), "the connection already holds it");
    assert_eq!(published_password(&rig).as_deref(), Some("minted"));
    // A connection published before the program had its own role is
    // brought to it: the same password, now for `app`.
    let values = rig.published_values(SERVICE).expect("published again");
    assert_eq!((values.get("user").map(String::as_str), values.get("watchDatabase").map(String::as_str)), (Some("app"), Some("app_watch")));
    Ok(())
}

/// A replaced disk means a fresh password, and the connection the
/// node holds is no longer the database's. It has to notice, rather
/// than go on handing out one nothing accepts.
async fn replaced_disk(rig: FakeRig) -> WeftResult<()> {
    endpoints(&rig);
    rig.published_connection(SERVICE, &[
        ("host", "wi-db-sql"),
        ("port", "5432"),
        ("database", "app"),
        ("user", "weft"),
        ("password", "from-the-old-disk"),
        ("sslmode", "disable"),
    ]);
    // The new disk disowns the old password, and offers its own. The
    // second retire, of the password it really made, is accepted.
    rig.answer_endpoint("credential", EndpointMethod::Post, CREDENTIAL_STORED_PATH, json!({ "stored": false }));
    rig.answer_endpoint("credential", EndpointMethod::Get, CREDENTIAL_PATH, json!({ "password": "from-the-new-disk" }));
    rig.answer_endpoint("credential", EndpointMethod::Post, CREDENTIAL_STORED_PATH, json!({ "stored": true }));

    rig.run(&PostgresDatabaseNode, config()).await.ok()?;

    assert!(asked_for_the_password(&rig), "a password the database disowns is worthless");
    assert_eq!(published_password(&rig).as_deref(), Some("from-the-new-disk"));
    assert_eq!(
        retired(&rig),
        vec!["from-the-old-disk".to_string(), "from-the-new-disk".to_string()],
        "the stale one is offered first, which is how the node finds out"
    );
    Ok(())
}

/// An answer that is neither a yes nor a no is a broken peer, not a
/// no. Read as a no it would send the node looking for a fresh
/// password and, on a database that has already retired its own, all
/// the way to telling the user their data is unreachable.
async fn garbled(rig: FakeRig) -> WeftResult<()> {
    endpoints(&rig);
    rig.published_connection(SERVICE, &[
        ("host", "wi-db-sql"),
        ("port", "5432"),
        ("database", "app"),
        ("user", "app"),
        ("password", "minted"),
        ("sslmode", "disable"),
    ]);
    rig.answer_endpoint("credential", EndpointMethod::Post, CREDENTIAL_STORED_PATH, json!({ "error": "busy" }));

    let failure = rig.run(&PostgresDatabaseNode, config()).await.failure()?;
    assert!(failure.contains("neither a yes nor a no"), "{failure}");
    assert!(!asked_for_the_password(&rig), "a broken answer is not a reason to start over");
    Ok(())
}

/// The credential server refuses while it has no password on the disk
/// yet. That is a failure, not an answer: acting on it would mean
/// publishing a connection to a database whose password nobody knows.
async fn refused(rig: FakeRig) -> WeftResult<()> {
    endpoints(&rig);
    rig.refuse_endpoint(
        "credential",
        EndpointMethod::Get,
        CREDENTIAL_PATH,
        503,
        r#"{"error": "no password on the shared volume yet"}"#,
    );

    let failure = rig.run(&PostgresDatabaseNode, config()).await.failure()?;
    assert!(failure.contains("no password on the shared volume yet"), "{failure}");
    assert!(rig.published_values(SERVICE).is_none(), "nothing was published");
    Ok(())
}

/// `sql`, the endpoint clients use, is the pooler's, on the port clients
/// already use; Postgres itself has no port anybody can reach, and
/// listens only on the unit's loopback, where the pooler dials it.
async fn one_door(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run_provision_infra(&PostgresDatabaseNode, reachable_config()).await.ok()?;
    let spec = outcome.infra_spec()?;
    let sql = spec.endpoints.iter().find(|e| e.name == "sql").expect("an sql endpoint");
    assert_eq!(
        sql.target,
        weft::infra::EndpointTarget::Unit { unit: "db".into(), container: "pooler".into(), port: "sql".into() }
    );
    let unit = &spec.units[0];
    let pooler = unit.containers.iter().find(|c| c.name == "pooler").expect("a pooler");
    assert_eq!(pooler.ports.iter().map(|p| p.port).collect::<Vec<_>>(), vec![5432]);
    assert_eq!(pooler.image, weft::infra::Image::Local { name: "pooler".into() });
    let postgres = &unit.containers[0];
    assert!(postgres.ports.is_empty(), "Postgres takes no client but the pooler: {:?}", postgres.ports);
    assert!(postgres.args.windows(2).any(|w| w == ["-c", "listen_addresses=127.0.0.1"]), "{:?}", postgres.args);
    // The pooler's settings carry no password: who may sign in is the
    // file the credential container writes from the database.
    for env in &pooler.env {
        assert!(!env.value.to_lowercase().contains("password"), "{env:?}");
    }
    assert!(pooler.args.iter().all(|a| !a.to_lowercase().contains("password")), "{:?}", pooler.args);
    Ok(())
}

/// Session pooling, so a firing's statements share one session as they
/// would on a connection of its own; both doors onto the one database,
/// each with its own share; and a client waits however long it takes.
fn pooler_settings() -> WeftResult<()> {
    let tuning = Tuning::for_machine("2Gi")?;
    let lines = pooler_config("shop", &tuning);
    let config = lines.join("\n");
    let line = |key: &str| lines.iter().find(|l| l.starts_with(&format!("{key} = "))).cloned().unwrap_or_else(|| panic!("{key} in {config}"));
    assert_eq!(line("pool_mode"), "pool_mode = session");
    assert_eq!(line("shop"), format!("shop = host=127.0.0.1 port=5433 dbname='shop' pool_size={}", tuning.query_pool));
    assert_eq!(
        line(&watch_database("shop")),
        format!("shop_watch = host=127.0.0.1 port=5433 dbname='shop' pool_size={}", tuning.watch_pool)
    );
    assert_eq!(line("listen_port"), "listen_port = 5432");
    assert_eq!(line("query_wait_timeout"), "query_wait_timeout = 0", "a client waits its turn, never refused for waiting");
    assert_eq!(line("server_reset_query"), "server_reset_query = DISCARD ALL");
    // The pooler lets its own idle connections go before the database
    // would close them for idling.
    let idle: u64 = line("server_idle_timeout").trim_start_matches("server_idle_timeout = ").parse().expect("seconds");
    assert!(idle < super::IDLE_SESSION_SECS, "{idle}");
    assert!(!config.to_lowercase().contains("password"), "{config}");
    // Handed over as the container's arguments, a line each, since no
    // platform carries a line break in an environment variable.
    let args = super::pooler_args("shop", &tuning);
    assert_eq!(args[0], "--config");
    assert_eq!(args[1..=lines.len()], lines[..]);
    assert_eq!(args[lines.len() + 1], "--hba");
    assert_eq!(args[lines.len() + 2..], pooler_hba("shop")[..]);
    assert!(args.iter().all(|a| !a.contains('\n')));
    Ok(())
}

/// The program's role, by password, to the program's database and its
/// watching door only; the superuser only to the pooler's console, over
/// its unix socket.
fn pooler_sign_ins() -> WeftResult<()> {
    assert_eq!(
        pooler_hba("shop"),
        ["local pgbouncer weft trust", "host shop,shop_watch app 0.0.0.0/0 scram-sha-256", "host shop,shop_watch app ::/0 scram-sha-256"]
    );
    Ok(())
}

/// Whatever name a database was made under is the one it keeps, since
/// a node pointing at another name points at a new, empty database. Only
/// a name no container's settings can carry is refused.
fn database_names() -> WeftResult<()> {
    for kept in ["app", "my-app", " spaced ", "it's \\ odd", "naïve", "2shop", "pgbouncer", &"a".repeat(70)] {
        assert_eq!(database_name(kept)?, kept);
    }
    for refused in ["", "two\nlines", "tab\there"] {
        assert!(matches!(database_name(refused), Err(weft::WeftError::Input(_))), "{refused:?}");
    }
    Ok(())
}

/// A name the pooler reads unquoted is its own door; any other gets one
/// made from it, the same on every build, and the pooler hands its
/// clients to the database under the database's own name, quoted.
fn doors() -> WeftResult<()> {
    for plain in ["app", "my-app", "Shop_2", &"a".repeat(57)] {
        assert_eq!(pooler_database(plain), plain);
        assert_eq!(watch_database(plain), format!("{plain}_watch"));
    }
    // Pinned: the door is published on the connection, so it must not
    // move between versions of weft.
    assert_eq!(pooler_database("my app"), "db_my_app_23dfc298");
    for odd in ["my app", "it's", "naïve", "pgbouncer", "sameuser", "all", "All", "replication", "a.b", &"a".repeat(58), &"é".repeat(70)] {
        let door = pooler_database(odd);
        assert!(door.starts_with("db_") && door.len() <= 57, "{door}");
        assert!(door.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-'), "{door}");
        assert_eq!(watch_database(odd), format!("{door}_watch"));
    }
    assert_ne!(pooler_database("a.b"), pooler_database("a b"), "two names never share a door");
    let tuning = Tuning::for_machine("1Gi")?;
    let lines = pooler_config("it's odd", &tuning);
    let door = pooler_database("it's odd");
    assert!(
        lines.contains(&format!("{door} = host=127.0.0.1 port=5433 dbname='it''s odd' pool_size={}", tuning.query_pool)),
        "{lines:?}"
    );
    assert_eq!(pooler_hba("it's odd")[1], format!("host {door},{door}_watch app 0.0.0.0/0 scram-sha-256"));
    Ok(())
}

/// The value a key of the credential server's settings has.
fn credential_setting(spec: &weft::infra::InfraSpec, name: &str) -> String {
    let credential = spec.units[0].containers.iter().find(|c| c.name == "credential").expect("declared");
    credential.env.iter().find(|e| e.name == name).unwrap_or_else(|| panic!("{name} is set")).value.clone()
}

/// Run the node's setup and its body against a database named `name`,
/// and hand back the spec it declared and the values it published.
async fn named(rig: &FakeRig, name: &str) -> WeftResult<(weft::infra::InfraSpec, std::collections::BTreeMap<String, String>)> {
    let mut settings = config();
    settings["database"] = json!(name);
    let spec = rig.run_provision_infra(&PostgresDatabaseNode, settings.clone()).await.ok()?.infra_spec()?.clone();
    endpoints(rig);
    rig.answer_endpoint("credential", EndpointMethod::Get, CREDENTIAL_PATH, json!({ "password": "minted" }));
    rig.answer_endpoint("credential", EndpointMethod::Post, CREDENTIAL_STORED_PATH, json!({ "stored": true }));
    rig.run(&PostgresDatabaseNode, settings).await.ok()?;
    Ok((spec, rig.published_values(SERVICE).expect("published")))
}

/// An existing database named with a dash keeps that name everywhere:
/// Postgres's own, the pooler's door, the connection.
async fn dashed(rig: FakeRig) -> WeftResult<()> {
    let (spec, values) = named(&rig, "my-app").await?;
    let postgres = &spec.units[0].containers[0];
    assert!(postgres.env.iter().any(|e| e.name == "POSTGRES_DB" && e.value == "my-app"), "{:?}", postgres.env);
    assert_eq!(credential_setting(&spec, "WEFT_DATABASE"), "my-app");
    assert_eq!(credential_setting(&spec, "WEFT_POOLER_DATABASE"), "my-app");
    assert_eq!(values["database"], "my-app");
    assert_eq!(values["watchDatabase"], "my-app_watch");
    Ok(())
}

/// A name the pooler cannot read as written stays Postgres's own, and the
/// connection carries the door the pooler lets clients in by.
async fn aliased(rig: FakeRig) -> WeftResult<()> {
    let (spec, values) = named(&rig, "my app").await?;
    let door = pooler_database("my app");
    let postgres = &spec.units[0].containers[0];
    assert!(postgres.env.iter().any(|e| e.name == "POSTGRES_DB" && e.value == "my app"), "{:?}", postgres.env);
    assert_eq!(credential_setting(&spec, "WEFT_DATABASE"), "my app");
    assert_eq!(credential_setting(&spec, "WEFT_POOLER_DATABASE"), door);
    assert_eq!(values["database"], door);
    assert_eq!(values["watchDatabase"], format!("{door}_watch"));
    let pooler = spec.units[0].containers.iter().find(|c| c.name == "pooler").expect("declared");
    assert!(pooler.args.iter().any(|a| a.starts_with(&format!("{door} = ")) && a.contains("dbname='my app'")), "{:?}", pooler.args);
    Ok(())
}

/// The extensions the node lists reach the credential server, which makes
/// them as the superuser on every start; none listed is an empty list.
async fn extensions(rig: FakeRig) -> WeftResult<()> {
    let mut settings = config();
    settings["extensions"] = json!(["postgres_fdw"]);
    let listed = rig.run_provision_infra(&PostgresDatabaseNode, settings).await.ok()?;
    assert_eq!(credential_setting(listed.infra_spec()?, "WEFT_EXTENSIONS"), r#"["postgres_fdw"]"#);
    let none = rig.run_provision_infra(&PostgresDatabaseNode, config()).await.ok()?;
    assert_eq!(credential_setting(none.infra_spec()?, "WEFT_EXTENSIONS"), "[]");
    assert!(extension_names(vec!["".into()]).is_err());
    assert!(extension_names(vec!["a\nb".into()]).is_err());
    Ok(())
}

fn versions() -> WeftResult<()> {
    for (raw, major) in [("17", 17), ("16.4", 16), ("14", 14), ("13", 13), ("9.6", 9)] {
        assert_eq!(postgres_version(raw)?, PostgresVersion { tag: raw.to_string(), major });
    }
    for refused in ["latest", " 17", "014", "17.4.1", "17.", ".4", "0", "17.04", "99999"] {
        assert!(postgres_version(refused).is_err(), "{refused:?}");
    }
    assert_eq!(postgres_version("14")?.note(), None);
    let note = postgres_version("13")?.note().expect("13 keeps idle sessions open");
    assert!(note.contains("14 or later") && note.contains("pg_dump") && note.contains("pg_restore"), "{note}");
    // The build warns of it with the same steps.
    let metadata: serde_json::Value = serde_json::from_str(include_str!("metadata.json")).expect("metadata reads");
    let rule = metadata["validate"]
        .as_array()
        .expect("rules")
        .iter()
        .find(|rule| rule["then"]["message"].as_str().is_some_and(|m| m.contains("older than 14")))
        .expect("a rule for an old version");
    assert_eq!(rule["then"]["severity"], "warning", "an existing database on it keeps running");
    let said = rule["then"]["message"].as_str().unwrap_or_default();
    assert!(said.contains(&super::MOVE_TO_14.replace("<node>", "{id}")), "{said}");
    Ok(())
}

/// A database on a Postgres older than 14 is declared, with a note for
/// `weft infra status` and the card saying what it lacks and how to move;
/// the credential container is told the major, so it refuses a new
/// database on it and skips the idle limit on an existing one. On 14 or
/// later there is nothing to say.
async fn old_postgres(rig: FakeRig) -> WeftResult<()> {
    let mut settings = config();
    settings["version"] = json!("13");
    let old = rig.run_provision_infra(&PostgresDatabaseNode, settings).await.ok()?;
    let spec = old.infra_spec()?;
    assert_eq!(spec.notes.len(), 1, "{:?}", spec.notes);
    assert!(spec.notes[0].contains("does not close a connection a client left idle"), "{}", spec.notes[0]);
    assert_eq!(credential_setting(spec, "WEFT_POSTGRES_MAJOR"), "13");
    assert_eq!(credential_setting(spec, "WEFT_POSTGRES_NOTE"), spec.notes[0]);
    let postgres = &spec.units[0].containers[0];
    assert_eq!(postgres.image, weft::infra::Image::Upstream { reference: "postgres:13-alpine".into() });
    let current = rig.run_provision_infra(&PostgresDatabaseNode, config()).await.ok()?;
    let spec = current.infra_spec()?;
    assert!(spec.notes.is_empty(), "{:?}", spec.notes);
    assert_eq!(credential_setting(spec, "WEFT_POSTGRES_MAJOR"), "17");
    assert_eq!(credential_setting(spec, "WEFT_POSTGRES_NOTE"), "");
    Ok(())
}

/// A connection idle in the pool for [`IDLE_CLOSE`] or longer is never
/// handed to a run: by then the database may have closed it, and a worker
/// without CPU between requests cannot have closed it itself on time.
fn idle_close() -> WeftResult<()> {
    use super::super::postgres::{still_fresh, IDLE_CLOSE};
    let since = std::time::Instant::now();
    assert!(still_fresh(since, since));
    assert!(still_fresh(since, since + IDLE_CLOSE - std::time::Duration::from_millis(1)));
    assert!(!still_fresh(since, since + IDLE_CLOSE));
    assert!(still_fresh(since + IDLE_CLOSE, since), "a clock read earlier is not idle at all");
    Ok(())
}

/// A unit an older weft set up has no role for the program: publishing
/// `app` for it would fail every sign-in with nothing saying why, so the
/// node refuses before publishing and names the upgrade.
async fn older_unit(rig: FakeRig) -> WeftResult<()> {
    rig.declare_endpoint("sql", "http://wi-db-sql:5432");
    rig.declare_endpoint("credential", "http://wi-db-credential:8099");
    rig.answer_endpoint("credential", EndpointMethod::Get, HEALTH_PATH, json!({ "ok": true }));
    let failure = rig.run(&PostgresDatabaseNode, config()).await.failure()?;
    assert!(failure.contains("weft infra upgrade"), "{failure}");
    assert!(rig.published_values(SERVICE).is_none(), "nothing was published");
    Ok(())
}
