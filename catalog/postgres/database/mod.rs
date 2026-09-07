//! PostgresDatabase: a Postgres the project runs itself, handed out as
//! an ordinary connection.
//!
//! Three pieces make one Pod:
//!   - an init container that writes a password to the shared volume
//!     the first time and never again, so the spec carries no
//!     credential and stays identical on every start;
//!   - Postgres itself, reading that password file;
//!   - a small server beside it that hands the password over once.
//!
//! `run` reads its own published connection when it has one, and asks
//! the database for the password when it has none or when the one it
//! holds turns out not to be the database's any more. It retires the
//! password only AFTER a connection holds it, and on every run rather
//! than only the one that read it, so no failure in between can leave
//! a database nothing can sign in to.
//!
//! Nothing here is memoized with `ctx.run`, and it does not need to
//! be: a body is re-run from the top if its worker dies mid-node, and
//! every call this makes is safe to repeat. Reading the password is a
//! read; publishing REPLACES one row keyed by this node; retiring is
//! the same request with the same password, which the credential
//! server answers the same way however many times it arrives.

use async_trait::async_trait;
use std::collections::BTreeMap;

use weft::infra::{
    AccessMode, Container, ContainerPort, Endpoint, EnvEntry, Expose, Image, InfraSpec, Mount,
    PodOptions, PodSecurityContext, Probe, Protocol, Resources, Unit, UpgradeBehavior, Volume,
    VolumeKind,
};
use weft::node::NodeOutput;
use weft::{
    EndpointMethod, ExecutionContext, InfraProvisionContext, Node, NodeManifest, ValueBag,
    WeftResult,
};

const SQL_PORT: u16 = 5432;
const CREDENTIAL_PORT: u16 = 8099;
/// The one disk and its two halves, ordinary subdirectories rather
/// than separate volumes, so a restore can never bring back one
/// without the other.
const STORE_PATH: &str = "/store";
const DATA_PATH: &str = "/store/pgdata";
const SECRET_PATH: &str = "/store/secret";
/// Where the Postgres image puts its unix socket; the credential
/// container mounts the same directory.
const SOCKET_PATH: &str = "/var/run/postgresql";
/// The role Postgres creates on first boot, and the only one weft uses.
const ADMIN_USER: &str = "weft";
/// The group the shared disk belongs to, so the container that mints
/// the password can write it and Postgres, running as its own user,
/// can read it without the file being readable to everything. 70 is
/// the postgres user's uid and gid in the alpine images: a different
/// base image means a different number.
const POSTGRES_GID: i64 = 70;
// SYNC: credential routes <-> catalog/postgres/database/images/credential/bootstrap.py
//       CREDENTIAL_PATH / CREDENTIAL_STORED_PATH / HEALTH_PATH (the container's
//       /live and /action are the dispatcher's, named by features.liveEndpoint)
const CREDENTIAL_PATH: &str = "/credential";
const CREDENTIAL_STORED_PATH: &str = "/credential/stored";
const HEALTH_PATH: &str = "/health";

#[derive(NodeManifest)]
pub struct PostgresDatabaseNode;

#[cfg(feature = "node-tests")]
mod tests;

/// The one disk, whole. The data directory and the password live on
/// it together so a restore can never bring back one without the
/// other.
///
/// Whole rather than a `sub_path` per half, because only the volume
/// root is guaranteed to carry the Pod's shared group: a directory
/// the kubelet creates to satisfy a `sub_path` can land owned by root
/// and lock out the very container that has to write it.
fn store_mount() -> Mount {
    Mount {
        volume: "store".into(),
        path: STORE_PATH.into(),
        ..Default::default()
    }
}

/// The password directory alone, for the one container that listens
/// on a port. It has no business with the database's files, and the
/// blast radius of a flaw in something reachable over the network is
/// worth the narrower mount.
///
/// Safe as a `sub_path` where the whole-disk mount is not, because
/// the init container makes this directory on every boot before any
/// other container starts, so the kubelet never has to create it.
fn secret_mount() -> Mount {
    Mount {
        volume: "store".into(),
        path: SECRET_PATH.into(),
        sub_path: Some("secret".into()),
        ..Default::default()
    }
}

/// Where the credential container reads its settings from, so the
/// paths and the port are declared once, here, and the container
/// cannot drift from them.
fn credential_env() -> Vec<EnvEntry> {
    vec![
        EnvEntry::Literal { name: "WEFT_SECRET_DIR".into(), value: SECRET_PATH.into() },
        EnvEntry::Literal { name: "WEFT_PASSWORD_FILE".into(), value: password_file() },
        EnvEntry::Literal {
            name: "WEFT_CREDENTIAL_PORT".into(),
            value: CREDENTIAL_PORT.to_string(),
        },
        EnvEntry::Literal { name: "WEFT_SOCKET_DIR".into(), value: SOCKET_PATH.into() },
        EnvEntry::Literal { name: "WEFT_ADMIN_USER".into(), value: ADMIN_USER.into() },
    ]
}

/// Postgres's unix socket, shared between it and the credential
/// container. It is the one door into the database that needs no
/// password (initdb trusts local socket connections; only TCP asks
/// for one), and it is how a password nobody holds any more gets
/// replaced from the graph instead of by hand on the disk.
fn socket_mount() -> Mount {
    Mount {
        volume: "socket".into(),
        path: SOCKET_PATH.into(),
        ..Default::default()
    }
}

/// The file the password lives in. Named HERE and handed to both the
/// container that writes it and the one that reads it, so the name
/// exists once: two spellings would leave Postgres reading a file
/// nothing wrote, and nothing would catch it before a login failed.
fn password_file() -> String {
    format!("{SECRET_PATH}/password")
}

#[async_trait]
impl Node for PostgresDatabaseNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn provision_infra(
        &self,
        _ctx: InfraProvisionContext,
        input: ValueBag,
    ) -> WeftResult<InfraSpec> {
        let database: String = input.get("database")?;
        let storage: String = input.get("storage")?;
        let version: String = input.get("version")?;

        let credential = Image::Local { name: "credential".into() };
        Ok(InfraSpec {
            units: vec![Unit {
                name: "db".into(),
                // Two Postgres processes on one data directory would
                // corrupt it, so a new version must fully replace the
                // old one rather than run beside it.
                on_upgrade: UpgradeBehavior::Recreate,
                init_containers: vec![Container::new("mint", credential.clone())
                    .with_args(vec!["mint".into()])
                    .with_env(credential_env())
                    .with_mounts(vec![store_mount()])],
                containers: vec![
                    Container::new("postgres", Image::Upstream {
                        reference: format!("postgres:{version}-alpine"),
                    })
                    .with_env(vec![
                        EnvEntry::Literal {
                            name: "POSTGRES_USER".into(),
                            value: ADMIN_USER.into(),
                        },
                        EnvEntry::Literal {
                            name: "POSTGRES_DB".into(),
                            value: database,
                        },
                        // The file, never the value: the password is
                        // not in this spec and never will be.
                        EnvEntry::Literal {
                            name: "POSTGRES_PASSWORD_FILE".into(),
                            value: password_file(),
                        },
                        // Postgres refuses a data directory that is a
                        // mount point with lost+found in it, so it
                        // gets a subdirectory of the mount.
                        EnvEntry::Literal {
                            name: "PGDATA".into(),
                            value: DATA_PATH.into(),
                        },
                    ])
                    .with_ports(vec![ContainerPort {
                        name: "sql".into(),
                        port: SQL_PORT,
                        protocol: Protocol::Tcp,
                    }])
                    .with_resources(Resources {
                        cpu_request: Some("100m".into()),
                        memory_request: Some("256Mi".into()),
                        cpu_limit: Some("2".into()),
                        memory_limit: Some("2Gi".into()),
                        ..Default::default()
                    })
                    .with_mounts(vec![store_mount(), socket_mount()])
                    // Postgres accepts TCP while it is still starting up
                    // and refuses every connection, so the port being
                    // bound is not readiness. Ask it the question it
                    // answers itself.
                    .with_readiness(
                        // Over TCP, not the unix socket: the entrypoint
                        // runs a temporary socket-only server while it
                        // initialises, so a socket check reports ready
                        // while port 5432 is still closed.
                        Probe::exec(vec![
                            "pg_isready".into(),
                            "-h".into(),
                            "127.0.0.1".into(),
                            "-U".into(),
                            ADMIN_USER.into(),
                        ])
                        .with_initial_delay(5),
                    ),
                    Container::new("credential", credential)
                        .with_args(vec!["serve".into()])
                        .with_env(credential_env())
                        .with_ports(vec![ContainerPort {
                            name: "http".into(),
                            port: CREDENTIAL_PORT,
                            protocol: Protocol::Tcp,
                        }])
                        .with_resources(Resources {
                            cpu_request: Some("10m".into()),
                            memory_request: Some("32Mi".into()),
                            cpu_limit: Some("100m".into()),
                            memory_limit: Some("64Mi".into()),
                            ..Default::default()
                        })
                        .with_mounts(vec![secret_mount(), socket_mount()])
                        .with_readiness(
                            Probe::http(HEALTH_PATH, CREDENTIAL_PORT).with_initial_delay(2),
                        ),
                ],
                pod_options: PodOptions {
                    security_context: Some(PodSecurityContext {
                        fs_group: Some(POSTGRES_GID),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                ..Default::default()
            }],
            volumes: vec![
                Volume {
                    name: "store".into(),
                    kind: VolumeKind::Persistent {
                        size: storage,
                        storage_class: None,
                        access_modes: vec![AccessMode::ReadWriteOnce],
                    },
                },
                // The socket lives and dies with the pod.
                Volume { name: "socket".into(), kind: VolumeKind::EmptyDir { size_limit: None } },
            ],
            // Both cluster-internal. The credential endpoint in
            // particular is never published outside the project.
            endpoints: vec![
                Endpoint {
                    name: "sql".into(),
                    unit: "db".into(),
                    container: "postgres".into(),
                    port: "sql".into(),
                    expose: Expose::ClusterInternal,
                },
                Endpoint {
                    name: "credential".into(),
                    unit: "db".into(),
                    container: "credential".into(),
                    port: "http".into(),
                    expose: Expose::ClusterInternal,
                },
            ],
            ..Default::default()
        })
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let database: String = ctx.inputs.get("database")?;
        let sql = ctx.endpoint("sql").await?;
        let (host, port) = sql.host_and_port()?;
        let credential = ctx.endpoint("credential").await?;

        // The password comes from the connection this node published
        // last time when there is one, and from the database itself
        // when there is none, or when what we hold is no longer the
        // database's: a replaced disk (a lost or reprovisioned volume)
        // means a fresh password, and the old connection would go on
        // failing every sign-in for the life of the database with
        // nothing saying why.
        // Asking the database whether it still knows a password also
        // retires it, so a run that gets a yes here has already done
        // the retiring this run owes.
        let (password, retired) = match ctx.published_access().await? {
            Some(published) => {
                let held = ctx.open(&published).await?.value("password")?.to_string();
                if confirm_stored(&credential, &held).await? {
                    (held, true)
                } else {
                    (password_from_database(&ctx, &credential).await?, false)
                }
            }
            None => (password_from_database(&ctx, &credential).await?, false),
        };

        let mut values: BTreeMap<String, String> = BTreeMap::new();
        // The address is read fresh every run: a replaced instance
        // answers on a new one, and the connection must follow it.
        values.insert("host".to_string(), host);
        values.insert("port".to_string(), port.to_string());
        values.insert("database".to_string(), database);
        values.insert("user".to_string(), ADMIN_USER.to_string());
        values.insert("password".to_string(), password.clone());
        // Inside the project's own network; the database serves no TLS.
        values.insert("sslmode".to_string(), "disable".to_string());
        let access = ctx.publish_access(values).await?;

        // Retire the password now that a connection holds it, unless
        // this run already did. What has to be true when the run ends
        // is that the password is retired; a run that died between
        // publishing and confirming would leave it readable for the
        // life of the database, which is why this is not left to the
        // one run that first read it.
        if !retired && !confirm_stored(&credential, &password).await? {
            weft::node_bail!(
                "this database does not know the password just published for it, so its \
                 disk was replaced while this run was working. Start this node again: the \
                 next run reads the new disk's own password."
            );
        }

        ctx.pulse_downstream(NodeOutput::new().set("access", access)).await
    }
}

/// Hand `password` back to the database and answer whether it is
/// really the one the database made. A match also retires it, so it
/// stops being readable, which is why this is only ever sent for a
/// password some connection already holds.
///
/// A `false` is an ANSWER, not a failure: it is how a caller finds
/// out the disk it is talking to is not the one its connection was
/// made against. An answer that is not a yes or a no IS a failure,
/// and says so rather than being read as a no.
async fn confirm_stored(
    credential: &weft::EndpointHandle,
    password: &str,
) -> WeftResult<bool> {
    #[derive(serde::Deserialize)]
    struct Answer {
        stored: bool,
    }
    let answer = credential
        .call(
            EndpointMethod::Post,
            CREDENTIAL_STORED_PATH,
            Some(serde_json::json!({ "password": password })),
        )
        .await?;
    let answer: Answer = serde_json::from_value(answer.clone()).map_err(|e| {
        weft::WeftError::NodeExecution(format!(
            "the database answered '{answer}' when asked to retire its password, which is \
             neither a yes nor a no: {e}"
        ))
    })?;
    Ok(answer.stored)
}

/// The password, straight from the database that made it. Asked for
/// on the first run, and again on a run that finds the password it
/// holds is no longer the database's.
///
/// A database that has already handed it over answers so plainly
/// rather than failing, because that answer has to be acted on: it
/// means another run published the connection in the meantime, so
/// this run reads that connection instead of asking again.
async fn password_from_database(
    ctx: &ExecutionContext,
    credential: &weft::EndpointHandle,
) -> WeftResult<String> {
    let answer = credential.call(EndpointMethod::Get, CREDENTIAL_PATH, None).await?;
    if let Some(password) = answer.get("password").and_then(|v| v.as_str()) {
        return Ok(password.to_string());
    }
    if let Some(published) = ctx.published_access().await? {
        return Ok(ctx.open(&published).await?.value("password")?.to_string());
    }
    weft::node_bail!(
        "this database handed its password over once already, and no connection holds it \
         now, so nothing can sign in to it. The data is still on its disk. Press \
         `Reset password` on this node in the graph (the database mints a new one), then \
         `weft infra start`: this run reads the new password and publishes a fresh \
         connection."
    )
}
