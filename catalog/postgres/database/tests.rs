//! PostgresDatabase self-tests: the declared infrastructure, and the
//! two properties the whole design rests on. The running database
//! itself is exercised end to end.

use serde_json::json;

use weft::{EndpointMethod, FakeRig, NodeTest, WeftResult};

use super::{PostgresDatabaseNode, CREDENTIAL_PATH, CREDENTIAL_STORED_PATH};

/// The service this node declares it publishes, spelled here as the
/// test's own expectation rather than read from the node, so a change
/// to the declaration shows up as a failing test.
const SERVICE: &str = "postgres";

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("declares_a_database_that_reads_its_password_from_a_file", declares),
        NodeTest::fake("describes_itself_identically_every_time", stable),
        NodeTest::fake("keeps_the_credential_endpoint_inside_the_cluster", internal),
        NodeTest::fake("waits_for_postgres_to_answer_for_itself", ready),
        NodeTest::fake("the_first_run_reads_the_password_and_retires_it", first_run),
        NodeTest::fake("a_later_run_never_asks_the_database_again", later_run),
        NodeTest::fake("a_replaced_disk_makes_the_node_ask_again", replaced_disk),
        NodeTest::fake("an_answer_that_is_neither_yes_nor_no_fails", garbled),
        NodeTest::fake("a_refusal_from_the_credential_server_surfaces", refused),
    ]
}

fn config() -> serde_json::Value {
    json!({ "database": "app", "storage": "10Gi", "version": "17" })
}

/// The two addresses the node's own infrastructure answers on. The
/// SQL one is only ever split into host and port, never dialled here.
fn endpoints(rig: &FakeRig) {
    rig.declare_endpoint("sql", "http://db-sql.ns.svc.cluster.local:5432");
    rig.declare_endpoint("credential", "http://db-credential.ns.svc.cluster.local:8099");
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
    assert_eq!(unit.containers.len(), 2, "Postgres plus the server that hands the password over");
    let postgres = &unit.containers[0];
    assert!(
        postgres.env.iter().any(|e| matches!(e,
            weft::infra::EnvEntry::Literal { name, .. } if name == "POSTGRES_PASSWORD_FILE")),
        "Postgres reads the password from the shared volume"
    );
    assert!(
        !postgres.env.iter().any(|e| matches!(e,
            weft::infra::EnvEntry::Literal { name, .. } if name == "POSTGRES_PASSWORD")),
        "and never from a value carried in the spec"
    );
    assert_eq!(spec.volumes.len(), 2, "one disk for data and password, one pod-local socket dir");
    assert!(
        matches!(spec.volumes[1].kind, weft::infra::VolumeKind::EmptyDir { .. }),
        "the socket never lands on the disk"
    );
    let socket_dir = "/var/run/postgresql";
    for container in &unit.containers {
        assert!(
            container.mounts.iter().any(|m| m.path == socket_dir && m.volume == "socket"),
            "{} shares the socket: it is how a password nobody holds gets replaced",
            container.name
        );
    }
    let credential = &unit.containers[1];
    for name in ["WEFT_SOCKET_DIR", "WEFT_ADMIN_USER"] {
        assert!(
            credential.env.iter().any(|e| matches!(e,
                weft::infra::EnvEntry::Literal { name: n, .. } if n == name)),
            "the credential server is told {name} by the node"
        );
    }
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

async fn internal(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run_provision_infra(&PostgresDatabaseNode, config()).await.ok()?;
    for endpoint in &outcome.infra_spec()?.endpoints {
        assert!(
            matches!(endpoint.expose, weft::infra::Expose::ClusterInternal),
            "'{}' must stay inside the project",
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
    assert_eq!(values.get("host").map(String::as_str), Some("db-sql.ns.svc.cluster.local"));
    assert_eq!(values.get("port").map(String::as_str), Some("5432"));
    assert_eq!(values.get("user").map(String::as_str), Some("weft"));
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
        ("host", "db-sql.ns.svc.cluster.local"),
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
    Ok(())
}

/// A replaced disk means a fresh password, and the connection the
/// node holds is no longer the database's. It has to notice, rather
/// than go on handing out one nothing accepts.
async fn replaced_disk(rig: FakeRig) -> WeftResult<()> {
    endpoints(&rig);
    rig.published_connection(SERVICE, &[
        ("host", "db-sql.ns.svc.cluster.local"),
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
    rig.published_connection(SERVICE, &[("password", "minted")]);
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
