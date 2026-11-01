//! PostgresDatabase: a Postgres the project runs itself, handed out as
//! an ordinary connection.
//!
//! Four pieces make one unit:
//!   - an init container that writes a password to the shared volume
//!     the first time and never again, so the spec carries no
//!     credential and stays identical on every start;
//!   - Postgres itself, reading that password file, listening only
//!     inside the unit;
//!   - a pooler (PgBouncer) in front of it, the one door every client
//!     comes through. Workers scale, a rollout runs old and new copies
//!     side by side, frontends connect too: the clients are many and
//!     change, while Postgres serves a fixed number of connections. The
//!     pooler holds at most that number and makes every client past it
//!     wait its turn, so the database is never asked for more than it
//!     serves, however many clients there are;
//!   - a small server beside them that hands the password over once,
//!     gives the program its own role (never the superuser), and shows
//!     the card.
//!
//! `run` reads its own published connection when it has one, and asks
//! the database for the password when it has none or when the one it
//! holds turns out not to be the database's any more. It retires the
//! password only AFTER a connection holds it, and on every run rather
//! than only the one that read it, so no failure in between can leave
//! a database nothing can sign in to.
//!
//! Its one output is baked (`weft_core::infra::bake`): the run that
//! applies its infra runs this body and saves the connection it emits,
//! and any later run that reads only that output uses the saved value
//! without running this node, so it asks the database nothing.
//!
//! The graph's `Reset password` button resets it in the database and
//! tells the pooler, and the credential container then pushes the new password to weft
//! (`weft_core::infra::VALUES_URL_ENV`), which writes it into the
//! connection this node published: no run of this node is involved.
//!
//! Nothing here is memoized with `ctx.run`, and it does not need to
//! be: the body never waits, so it is never replayed, and every call
//! it makes is safe to repeat when somebody runs the node again. Reading the password is a
//! read; publishing REPLACES one row keyed by this node; retiring is
//! the same request with the same password, which the credential
//! server answers the same way however many times it arrives.

use async_trait::async_trait;
use std::collections::BTreeMap;

use weft::infra::{
    Container, ContainerPort, Endpoint, EndpointTarget, EnvEntry, Expose, Image, InfraSpec, Limits, MachineShape, Mount, Probe,
    Protocol, Unit, Volume, VolumeKind,
};
use weft::node::NodeOutput;
use weft::{
    EndpointMethod, RunContext, InfraProvisionContext, Node, NodeManifest, ValueBag,
    WeftResult,
};

use super::postgres::{mistake, IDLE_CLOSE};

/// The port clients connect to: the pooler's. Postgres's own default, so
/// a connection string reads like any other Postgres one.
const SQL_PORT: u16 = 5432;
/// Postgres's own port, on the unit's loopback only: nothing but the
/// pooler and the unit's own checks ever dials it.
const POSTGRES_PORT: u16 = 5433;
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
/// The role Postgres creates on first boot: the superuser. It signs in
/// only over the unit's unix socket (the card, a reset, a person
/// operating the database), never through the pooler, so the slots
/// Postgres keeps for a superuser are always there for it.
const ADMIN_USER: &str = "weft";
/// The role the program and every other client sign in as, through the
/// pooler. It owns the program's database and is no superuser.
const APP_USER: &str = "app";
/// How long a session of [`APP_USER`] may sit idle before Postgres closes
/// it, in seconds. This is what frees a connection nobody is using
/// whoever holds it: a worker that lost its CPU between requests, a copy
/// left behind by a rollout, a frontend's pool. It runs inside Postgres,
/// which always has a CPU. A client that means to sit idle (a watching
/// query) says so for its own session.
const IDLE_SESSION_SECS: u64 = 60;
// A worker hands out only connections idle for less than `IDLE_CLOSE`,
// so one the database closed for idling is never handed to a run.
const _: () = assert!(IDLE_CLOSE.as_secs() * 4 <= IDLE_SESSION_SECS);
/// The directory the pooler shares with the credential container: the
/// pooler's unix socket (its admin console, for the card and for telling
/// it a new password) and the file of who may sign in. Scratch space,
/// written afresh on every start.
const POOLER_PATH: &str = "/run/pooler";
/// Where the pooler's entrypoint writes the settings this node hands it.
/// Inside the pooler's own container, so nothing else reads them.
const POOLER_CONFIG_FILE: &str = "/tmp/pgbouncer.ini";
const POOLER_HBA_FILE: &str = "/tmp/pgbouncer.hba";
/// How many clients the pooler takes at once, waiting or served: twenty
/// for each of a hundred workers. Measured on PgBouncer 1.25, two
/// thousand clients waiting to sign in take about 5MB.
const MOST_CLIENTS: u64 = 2000;
/// The pooler's memory limit, in MiB, ample for [`MOST_CLIENTS`].
const POOLER_MIB: u64 = 32;
/// How long the pooler keeps a connection to Postgres that no client
/// holds, in seconds: under [`IDLE_SESSION_SECS`], so it closes its own
/// rather than finding them closed by Postgres.
const POOLER_SERVER_IDLE_SECS: u64 = 30;
/// The connections Postgres keeps for the superuser
/// (`superuser_reserved_connections`, Postgres's own default, set so it
/// is visible): the operator's way in on a database every client is
/// waiting for.
const SUPERUSER_RESERVED: u64 = 3;
/// Connections left out of the pooler's on top of those: a connection
/// the pooler just closed still counts in Postgres for the moment its
/// process takes to exit, and a new one opened in that moment must not
/// find Postgres full.
const POOLER_MARGIN: u64 = 2;
/// The share of the pooler's connections kept for watching queries,
/// which each hold one for as long as their run watches: a quarter, at
/// least one. Kept apart so watchers can never take every connection
/// from queries, nor queries from watchers.
const WATCH_SHARE: u64 = 4;
/// The group the shared disk belongs to, so the container that mints
/// the password can write it and Postgres, running as its own user,
/// can read it without the file being readable to everything. 70 is
/// the postgres user's uid and gid in the alpine images: a different
/// base image means a different number.
const POSTGRES_GID: u32 = 70;
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
/// Whole rather than a `sub_path` per half: the volume root is what
/// carries the unit's shared group, and the init container makes the
/// halves inside it.
fn store_mount() -> Mount {
    Mount::new("store", STORE_PATH)
}

/// The password directory alone, for the one container that listens
/// on a port. It has no business with the database's files, and the
/// blast radius of a flaw in something reachable over the network is
/// worth the narrower mount.
///
/// The init container makes this directory on every boot before any
/// other container starts, so it is there to mount.
fn secret_mount() -> Mount {
    Mount { sub_path: Some("secret".into()), ..Mount::new("store", SECRET_PATH) }
}

/// Where the credential container reads its settings from, so the
/// paths, the ports and the names are declared once, here, and the
/// container cannot drift from them. The pooler's name for the database
/// and the program's user are what its card shows beside the password:
/// the three a client signs in with. The extensions and the roles are the
/// ones it makes on every start, as JSON, since a container's settings hold
/// no line break.
// SYNC: credential_env <-> catalog/postgres/database/images/credential/bootstrap.py (the container's settings)
fn credential_env(database: &str, extensions: &[String], roles: &[Role], version: &PostgresVersion) -> Vec<EnvEntry> {
    vec![
        EnvEntry::new("WEFT_POSTGRES_MAJOR", version.major.to_string()),
        EnvEntry::new("WEFT_CLOSES_IDLE_SESSIONS_FROM", PostgresVersion::CLOSES_IDLE_SESSIONS.to_string()),
        // Empty on a Postgres that closes idle sessions.
        EnvEntry::new("WEFT_POSTGRES_NOTE", version.note().unwrap_or_default()),
        EnvEntry::new("WEFT_DATA_DIR", DATA_PATH),
        EnvEntry::new("WEFT_DATABASE", database),
        EnvEntry::new("WEFT_POOLER_DATABASE", pooler_database(database)),
        EnvEntry::new("WEFT_WATCH_DATABASE", watch_database(database)),
        EnvEntry::new("WEFT_EXTENSIONS", serde_json::to_string(extensions).expect("a list of names serializes")),
        EnvEntry::new("WEFT_ROLES", serde_json::to_string(roles).expect("a list of roles serializes")),
        EnvEntry::new("WEFT_SECRET_DIR", SECRET_PATH),
        EnvEntry::new("WEFT_PASSWORD_FILE", password_file()),
        EnvEntry::new("WEFT_CREDENTIAL_PORT", CREDENTIAL_PORT.to_string()),
        EnvEntry::new("WEFT_SOCKET_DIR", SOCKET_PATH),
        EnvEntry::new("WEFT_POSTGRES_PORT", POSTGRES_PORT.to_string()),
        EnvEntry::new("WEFT_ADMIN_USER", ADMIN_USER),
        EnvEntry::new("WEFT_APP_USER", APP_USER),
        EnvEntry::new("WEFT_IDLE_SESSION_SECONDS", IDLE_SESSION_SECS.to_string()),
        EnvEntry::new("WEFT_POOLER_DIR", POOLER_PATH),
        EnvEntry::new("WEFT_POOLER_PORT", SQL_PORT.to_string()),
        EnvEntry::new("WEFT_POOLER_USERS_FILE", pooler_users_file()),
    ]
}

/// What the pooler's entrypoint reads beside its arguments
/// ([`pooler_args`]): where to write its settings and its sign-in rules,
/// the file of who may sign in that it waits for, and how many open files
/// its clients need.
// SYNC: pooler_env, pooler_args <-> catalog/postgres/database/images/pooler/entrypoint.sh
fn pooler_env(tuning: &Tuning) -> Vec<EnvEntry> {
    vec![
        EnvEntry::new("WEFT_POOLER_CONFIG_FILE", POOLER_CONFIG_FILE),
        EnvEntry::new("WEFT_POOLER_HBA_FILE", POOLER_HBA_FILE),
        EnvEntry::new("WEFT_POOLER_USERS_FILE", pooler_users_file()),
        // One per client and one per connection to Postgres, and room
        // for the pooler's own (its log, its sockets, the files above).
        EnvEntry::new("WEFT_POOLER_OPEN_FILES", (MOST_CLIENTS + tuning.query_pool + tuning.watch_pool + 64).to_string()),
    ]
}

/// The pooler's settings and sign-in rules, as its container's arguments,
/// one line each: `--config` and the lines of [`pooler_config`], then
/// `--hba` and the lines of [`pooler_hba`]. Arguments rather than
/// environment, which cannot carry a line break.
pub fn pooler_args(database: &str, tuning: &Tuning, roles: &[Role]) -> Vec<String> {
    std::iter::once("--config".to_string())
        .chain(pooler_config(database, tuning))
        .chain(std::iter::once("--hba".to_string()))
        .chain(pooler_hba(database, roles))
        .collect()
}

/// The name watching queries connect to `database` by: the same
/// database, through the pooler's second door with its own connections
/// ([`Tuning::watch_pool`]). Published on the connection as
/// `watchDatabase`, which the watching node reads.
pub fn watch_database(database: &str) -> String {
    format!("{}_watch", pooler_database(database))
}

/// PgBouncer's settings. Session pooling: a client keeps the one
/// Postgres connection it was handed until it lets go, so `SET`, a
/// temporary table, an advisory lock, `LISTEN` and a prepared
/// statement all hold across one firing's statements exactly as they
/// would on a connection of its own. The pooler's part is only that it
/// opens at most [`Tuning::query_pool`] connections for queries and
/// [`Tuning::watch_pool`] for watchers, and that a client past them
/// waits, however long, for one to come free: a run waits for the
/// database the way it waits for anything else, and stops when the run
/// does.
///
/// PgBouncer keeps a pool per user of a door, each up to `pool_size`, so
/// with a login role of the node's own ([`Role::login`]) beside `app`
/// the pools of a door together could open more than its share;
/// `max_db_connections` holds them all to that share, whoever signs in.
/// A client of one role never waits on another role's idle connections:
/// with the share full, PgBouncer (1.25, measured) closes an idle
/// connection of another pool to serve it (its log says `evicted`), so
/// only connections clients are holding make a client wait.
///
/// No password is in it: who may sign in is the file the credential
/// container writes from the database itself.
pub fn pooler_config(database: &str, tuning: &Tuning) -> Vec<String> {
    let (door, watch) = (pooler_database(database), watch_database(database));
    // The database's own name, quoted the way PgBouncer reads a value
    // (a quote doubled), so any name Postgres has reaches it.
    let backend = format!("host=127.0.0.1 port={POSTGRES_PORT} dbname='{}'", database.replace('\'', "''"));
    let share = |pool: u64| format!("pool_size={pool} max_db_connections={pool}");
    [
        "[databases]".to_string(),
        format!("{door} = {backend} {}", share(tuning.query_pool)),
        format!("{watch} = {backend} {}", share(tuning.watch_pool)),
        String::new(),
        "[pgbouncer]".to_string(),
        "listen_addr = *".to_string(),
        format!("listen_port = {SQL_PORT}"),
        format!("unix_socket_dir = {POOLER_PATH}"),
        "auth_type = hba".to_string(),
        format!("auth_hba_file = {POOLER_HBA_FILE}"),
        format!("auth_file = {}", pooler_users_file()),
        format!("admin_users = {ADMIN_USER}"),
        "pool_mode = session".to_string(),
        // What a client left on a connection goes before the next one
        // gets it.
        "server_reset_query = DISCARD ALL".to_string(),
        format!("max_client_conn = {MOST_CLIENTS}"),
        // A client waits for a connection as long as it takes.
        "query_wait_timeout = 0".to_string(),
        format!("server_idle_timeout = {POOLER_SERVER_IDLE_SECS}"),
        // A client whose machine vanished without closing (a worker
        // instance gone mid-request) is found within about a minute and
        // its connection given back, rather than after the two hours a
        // kernel waits by default.
        "tcp_keepalive = 1".to_string(),
        "tcp_keepidle = 30".to_string(),
        "tcp_keepintvl = 10".to_string(),
        "tcp_keepcnt = 3".to_string(),
        // Sent by JDBC and other drivers on every connect, and harmless
        // to leave out; anything else a client asks for at sign-in that
        // the pooler cannot carry is refused, never dropped.
        "ignore_startup_parameters = extra_float_digits".to_string(),
    ]
    .into()
}

/// Who may sign in to the pooler, and how. The program's role and each
/// login role the node lists, each with its own password, to the
/// program's database and its watching door, from anywhere the endpoint
/// lets in; the superuser only to the pooler's admin console, only over
/// the pooler's unix socket, which only the credential container mounts.
/// Nobody reaches the superuser through the pooler, and a role taken off
/// the list loses its lines here, so the pooler lets it in no more.
pub fn pooler_hba(database: &str, roles: &[Role]) -> Vec<String> {
    let (door, watch) = (pooler_database(database), watch_database(database));
    let signs_in = std::iter::once(APP_USER).chain(roles.iter().filter(|role| role.login).map(|role| role.name.as_str()));
    std::iter::once(format!("local pgbouncer {ADMIN_USER} trust"))
        .chain(signs_in.flat_map(|user| {
            ["0.0.0.0/0", "::/0"].map(|from| format!("host {door},{watch} {user} {from} scram-sha-256"))
        }))
        .collect()
}

/// The file of who may sign in to the pooler, written by the credential
/// container from the database's own records.
fn pooler_users_file() -> String {
    format!("{POOLER_PATH}/users")
}

/// The pooler's shared directory ([`POOLER_PATH`]).
fn pooler_mount() -> Mount {
    Mount::new("pooler", POOLER_PATH)
}

/// Postgres's unix socket, shared between it and the credential
/// container. It is the one door into the database that needs no
/// password (initdb trusts local socket connections; only TCP asks
/// for one), and it is how a password nobody holds any more gets
/// replaced from the graph instead of by hand on the disk.
fn socket_mount() -> Mount {
    Mount::new("socket", SOCKET_PATH)
}

/// The file the password lives in. Named HERE and handed to both the
/// container that writes it and the one that reads it, so the name
/// exists once: two spellings would leave Postgres reading a file
/// nothing wrote, and nothing would catch it before a login failed.
fn password_file() -> String {
    format!("{SECRET_PATH}/password")
}

/// What the machine keeps for everything that is not Postgres, in MiB:
/// the operating system, Docker and the agent that runs the unit (about
/// 256 together on the smallest cloud machine), plus the credential
/// container's 64Mi limit and the pooler's ([`POOLER_MIB`]).
const BESIDE_POSTGRES_MIB: u64 = 256 + 64 + POOLER_MIB;
/// What one connection costs Postgres before it sorts or hashes
/// anything, in MiB: the server process each connection gets, with its
/// caches of the tables and plans it has used.
const PER_CONNECTION_MIB: u64 = 8;
/// The most connections Postgres takes, however big the machine. Past a
/// few hundred, Postgres slows down from managing its connections alone;
/// more clients than that wait at the pooler instead.
const MOST_CONNECTIONS: u64 = 200;
/// The background cleaners (`autovacuum_max_workers`, Postgres's
/// default), each of which may use `maintenance_work_mem` at once.
const CLEANERS: u64 = 3;

/// Postgres's settings for a machine of `memory`, so it plans for the
/// memory it actually has. Left at the factory settings, it accepts 100
/// connections on any machine, and enough of them sorting at once on a
/// small one runs the machine out of memory: the kernel then kills a
/// Postgres process, and Postgres restarts and drops every connection,
/// where a full Postgres would have refused the next one cleanly.
#[derive(Debug, PartialEq, Eq)]
pub struct Tuning {
    /// Postgres's share of the machine: the container's memory limit.
    pub limit_mib: u64,
    pub max_connections: u64,
    pub shared_buffers_mib: u64,
    pub work_mem_kib: u64,
    pub maintenance_work_mem_mib: u64,
    pub effective_cache_size_mib: u64,
    /// The connections the pooler opens for queries, and for watching
    /// queries: together, everything Postgres serves but the
    /// superuser's [`SUPERUSER_RESERVED`] and the [`POOLER_MARGIN`].
    pub query_pool: u64,
    pub watch_pool: u64,
}

impl Tuning {
    /// The settings, following Postgres's own tuning guidance, inside
    /// the share of the machine left once [`BESIDE_POSTGRES_MIB`] is
    /// set aside:
    ///   - `shared_buffers`, the cache every connection shares: a
    ///     quarter of the share, the guidance's starting point for a
    ///     machine that runs a database;
    ///   - `maintenance_work_mem`, what a vacuum or an index build may
    ///     use: a sixteenth, between 16MB and the 1GB a vacuum can use at
    ///     most, held [`CLEANERS`] times over since the cleaners can all
    ///     run at once;
    ///   - what is left is the connections': each costs
    ///     [`PER_CONNECTION_MIB`] plus two `work_mem` (the memory one
    ///     sort or hash may use, and a hash takes twice that), and the
    ///     guidance is that every connection using its `work_mem` at
    ///     once must still fit. At Postgres's own 4MB `work_mem` that is
    ///     16MB a connection, which sets `max_connections`; the
    ///     connections' memory is then divided between them, so a
    ///     machine past [`MOST_CONNECTIONS`] gives each more `work_mem`;
    ///   - `effective_cache_size`, only a hint to the planner of how
    ///     much the disk cache holds: three quarters of the share.
    ///
    /// The pooler's connections are what `max_connections` leaves, split
    /// between queries and watchers ([`WATCH_SHARE`]).
    ///
    /// `memory` is what the node asks for, which a cloud rounds UP to
    /// the machine it picks, so the settings never assume more memory
    /// than there is. Refused when the share leaves the pooler no
    /// connection for queries or none for watchers.
    pub fn for_machine(memory: &str) -> WeftResult<Tuning> {
        let Split { share, shared_buffers_mib, maintenance_work_mem_mib, connections_mib, max_connections, query_pool, watch_pool } =
            split(memory_mib(memory)?);
        if query_pool == 0 || watch_pool == 0 {
            mistake!(
                "memory {memory} leaves Postgres room for {max_connections} connections, too few to keep \
                 {SUPERUSER_RESERVED} for the superuser and still give queries and watchers one each; raise \
                 `memory` to {LEAST_MEMORY_MIB}Mi or more"
            );
        }
        let each_kib = connections_mib * 1024 / max_connections;
        Ok(Tuning {
            limit_mib: share,
            max_connections,
            shared_buffers_mib,
            work_mem_kib: (each_kib - PER_CONNECTION_MIB * 1024) / 2,
            maintenance_work_mem_mib,
            effective_cache_size_mib: share * 3 / 4,
            query_pool,
            watch_pool,
        })
    }

    /// The settings as Postgres's command line (the image passes
    /// arguments that start with `-` to the server).
    pub fn args(&self) -> Vec<String> {
        [
            format!("max_connections={}", self.max_connections),
            format!("superuser_reserved_connections={SUPERUSER_RESERVED}"),
            format!("shared_buffers={}MB", self.shared_buffers_mib),
            format!("work_mem={}kB", self.work_mem_kib),
            format!("maintenance_work_mem={}MB", self.maintenance_work_mem_mib),
            format!("effective_cache_size={}MB", self.effective_cache_size_mib),
        ]
        .into_iter()
        .flat_map(|setting| ["-c".to_string(), setting])
        .collect()
    }
}

/// How a machine of `machine_mib` divides between Postgres's uses, as
/// [`Tuning::for_machine`] lays out: the one formula, which
/// [`LEAST_MEMORY_MIB`] is read from too.
struct Split {
    share: u64,
    shared_buffers_mib: u64,
    maintenance_work_mem_mib: u64,
    connections_mib: u64,
    max_connections: u64,
    query_pool: u64,
    watch_pool: u64,
}

const fn split(machine_mib: u64) -> Split {
    let share = machine_mib.saturating_sub(BESIDE_POSTGRES_MIB);
    let shared_buffers_mib = share / 4;
    let maintenance_work_mem_mib = match share / 16 {
        mib if mib < 16 => 16,
        mib if mib > 1024 => 1024,
        mib => mib,
    };
    let connections_mib = share.saturating_sub(shared_buffers_mib + CLEANERS * maintenance_work_mem_mib);
    let fit = connections_mib / (PER_CONNECTION_MIB + 2 * 4);
    let max_connections = if fit < MOST_CONNECTIONS { fit } else { MOST_CONNECTIONS };
    let pooled = max_connections.saturating_sub(SUPERUSER_RESERVED + POOLER_MARGIN);
    let watch_pool = match pooled / WATCH_SHARE {
        0 if pooled >= 2 => 1,
        share => share,
    };
    let query_pool = pooled - watch_pool;
    Split { share, shared_buffers_mib, maintenance_work_mem_mib, connections_mib, max_connections, query_pool, watch_pool }
}

/// The least `memory` the node takes, in MiB: the smallest machine whose
/// share gives the pooler a connection for queries and one for
/// watchers. Read off [`split`], so it moves with the formula; the
/// `memory` input's description states it too, and a test holds the two
/// together.
pub const LEAST_MEMORY_MIB: u64 = {
    let mut mib = BESIDE_POSTGRES_MIB;
    while split(mib).query_pool == 0 || split(mib).watch_pool == 0 {
        mib += 1;
    }
    mib
};

/// The `database` input as Postgres has the database: exactly as
/// written, whatever it holds, because a database made under a name is
/// found only under that name again, and a node pointing at another
/// name points at a new, empty database. Refused only when nothing
/// could carry it: empty, or with a line break or another control
/// character, which no container's settings can hold.
pub fn database_name(raw: &str) -> WeftResult<String> {
    if raw.is_empty() {
        mistake!("the `database` input is empty; name the database (app)");
    }
    if raw.chars().any(char::is_control) {
        mistake!("database {raw:?} has a line break or another control character in it; name it without (app)");
    }
    Ok(raw.to_string())
}

/// The longest name the pooler can let clients in by as written: its
/// watching door ([`watch_database`]) adds `_watch`, and Postgres and
/// the pooler both keep 63 characters of a name.
const MOST_DOOR_CHARS: usize = 63 - "_watch".len();

/// The name a client signs in to `database` by, through the pooler, and
/// so the one the connection carries. PgBouncer's settings take a
/// database's name unquoted, which holds letters, digits, `_` and `-`
/// (a space, a dot or a quote stops it reading its settings at all);
/// `pgbouncer` is its own console, and its sign-in rules read `all`,
/// `sameuser`, `samerole`, `samegroup` and `replication` as words of their
/// own ([`HBA_WORDS`]). So any other name like that, short enough for its
/// watching door, is the door itself; the rest are each given one made
/// from it, `db_`, the characters of it that fit, and 8 hex of a hash of
/// the whole name so two names never share a door, while the pooler
/// still hands every client to the database under its own name
/// ([`pooler_config`]).
pub fn pooler_database(database: &str) -> String {
    let plain = database.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    if plain && database.len() <= MOST_DOOR_CHARS && database != "pgbouncer" && !HBA_WORDS.iter().any(|word| word.eq_ignore_ascii_case(database)) {
        return database.to_string();
    }
    let kept: String = database
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
        .take(40)
        .collect();
    format!("db_{kept}_{:08x}", name_hash(database) as u32)
}

/// The database names PgBouncer's sign-in rules (`auth_hba_file`, read
/// the way Postgres reads `pg_hba.conf`) take as keywords rather than as a
/// database: a rule naming `sameuser` matches only a database named after
/// the user signing in. Compared ignoring case, which costs a name like
/// `All` nothing but its own door.
const HBA_WORDS: [&str; 5] = ["all", "sameuser", "samerole", "samegroup", "replication"];

/// A hash of `name` that every build of weft computes the same (64-bit
/// FNV-1a): the door [`pooler_database`] makes is published on the
/// connection, so it must not move between versions.
fn name_hash(name: &str) -> u64 {
    name.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3))
}

/// The `extensions` input: each extension the program's database needs,
/// as Postgres names it, made by the superuser on every start
/// (`bootstrap.py`, `converge`). An empty name, or one with a control
/// character, is refused here; one Postgres does not know fails the start
/// naming it.
pub fn extension_names(raw: Vec<String>) -> WeftResult<Vec<String>> {
    for name in &raw {
        if name.is_empty() || name.chars().any(char::is_control) {
            mistake!("extension {name:?} in `extensions` is not a name; write each as Postgres names it (pg_trgm)");
        }
    }
    Ok(raw)
}

/// One entry of the `roles` input: a Postgres role the program's database
/// needs beside `app`, made by the superuser on every start
/// (`bootstrap.py`, `converge`), never a superuser and never one that
/// makes roles, and granted to `app` so the program can `set role` to it.
// SYNC: Role <-> catalog/postgres/database/images/credential/bootstrap.py (WEFT_ROLES), catalog/postgres/database/metadata.json (the `roles` input)
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Role {
    pub name: String,
    /// Signs in by itself, through the pooler, with a password of its own
    /// the card shows: how a client outside the program (a site) signs in
    /// as a role other than `app`.
    #[serde(default)]
    pub login: bool,
    /// Reads and writes past every row-level security policy, `FORCE`
    /// ones included.
    #[serde(default)]
    pub bypass_row_level_security: bool,
}

/// The longest role name Postgres keeps whole.
const MOST_ROLE_CHARS: usize = 63;

/// The words Postgres reserves (`pg_get_keywords()`, category `R`, as of
/// Postgres 17): written unquoted, `set role user` or `create policy ... to
/// select` is a syntax error, so a role named one of them is one no program
/// can name the way the node tells it to.
const RESERVED_WORDS: [&str; 78] = [
    "all", "analyse", "analyze", "and", "any", "array", "as", "asc", "asymmetric", "both", "case", "cast", "check",
    "collate", "column", "constraint", "create", "current_catalog", "current_date", "current_role", "current_time",
    "current_timestamp", "current_user", "default", "deferrable", "desc", "distinct", "do", "else", "end", "except",
    "false", "fetch", "for", "foreign", "from", "grant", "group", "having", "in", "initially", "intersect", "into",
    "lateral", "leading", "limit", "localtime", "localtimestamp", "not", "null", "offset", "on", "only", "or",
    "order", "placing", "primary", "references", "returning", "select", "session_user", "some", "symmetric",
    "system_user", "table", "then", "to", "trailing", "true", "union", "unique", "user", "using", "variadic", "when",
    "where", "window", "with",
];

/// The `roles` input, each role checked. A name is what a program writes
/// unquoted in `set role` and a policy (lowercase letters, digits and `_`,
/// not starting with a digit, at most 63 characters, not a word Postgres
/// reserves), so the name it writes is the role it gets; the pooler's
/// sign-in rules take it as written too. Refused: `app` and `weft` (the
/// program's role and the superuser, which this node makes itself), the
/// names Postgres keeps (`public`, `none`, `pg_`, and its reserved words,
/// among them `all`, every user to the pooler, and the ones that mean the
/// current user), and a name listed twice.
pub fn role_list(raw: Vec<Role>) -> WeftResult<Vec<Role>> {
    const KEPT: [&str; 4] = [APP_USER, ADMIN_USER, "public", "none"];
    for (i, role) in raw.iter().enumerate() {
        let name = role.name.as_str();
        let plain = name.chars().next().is_some_and(|c| c.is_ascii_lowercase() || c == '_')
            && name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
            && name.len() <= MOST_ROLE_CHARS;
        if !plain {
            mistake!(
                "role {name:?} in `roles` is not a name a program can write as it is; use lowercase letters, digits \
                 and `_`, not starting with a digit, at most {MOST_ROLE_CHARS} characters (tenant_app)"
            );
        }
        if KEPT.contains(&name) || name.starts_with("pg_") {
            mistake!(
                "role {name:?} in `roles` is a name this node or Postgres keeps for itself (`{APP_USER}` is the \
                 program's own role, `{ADMIN_USER}` the superuser, `pg_` names and `public` and `none` are \
                 Postgres's); name it something else"
            );
        }
        if RESERVED_WORDS.contains(&name) {
            mistake!(
                "role {name:?} in `roles` is a word Postgres reserves, so `set role {name}` written as it is would be \
                 a syntax error; name it something else (tenant_{name})"
            );
        }
        if raw[..i].iter().any(|other| other.name == name) {
            mistake!("role {name:?} is listed twice in `roles`; list each role once, with all its settings");
        }
    }
    Ok(raw)
}

/// How a database on a Postgres older than 14 moves to 14 or later: dump
/// and restore, while it still runs (its card answers, and when it was set
/// up with `reachable` on its address is a door on this machine).
// SYNC: MOVE_TO_14 <-> catalog/postgres/database/metadata.json (the old `version` rule's message)
const MOVE_TO_14: &str = "Postgres cannot open an older version's data, so the data moves by dump and restore while \
     the old database still runs. Dump it: `weft infra press <node> reset_password`, then `weft infra env <node> \
     --into old.env --set PGUSER=User --set PGDATABASE=Database --set PGPASSWORD=Password`, and with those exported, `pg_dump \
     -Fc -h <host> -p <port> -f data.dump` at the address `weft infra list-doors` prints for it (turn `reachable` on \
     first if it is off). Restore it: add a PostgresDatabase on 17 with `reachable` on, wire it where the old one was, \
     start it (its setup takes the password), then press its reset_password and write its env the same way, and `pg_restore --no-owner -h <host> -p <port> -d \"$PGDATABASE\" data.dump`";

/// The Postgres a `version` input names: the image tag, as written, and
/// its major version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresVersion {
    pub tag: String,
    pub major: u32,
}

impl PostgresVersion {
    /// The first major that closes idle sessions (`idle_session_timeout`),
    /// which this node sets on the program's role. A NEW database is made
    /// on this or later only (the init container refuses an older one on an
    /// empty disk, the one place that can tell); an existing one on an older
    /// Postgres keeps running without the limit, said by [`Self::note`].
    pub const CLOSES_IDLE_SESSIONS: u32 = 14;

    /// What `weft infra status` and the card warn of a database on a
    /// Postgres that does not close idle sessions, or `None` on one that does.
    pub fn note(&self) -> Option<String> {
        (self.major < Self::CLOSES_IDLE_SESSIONS).then(|| {
            format!(
                "Postgres {} does not close a connection a client left idle, so connections an idle worker or an \
                 old revision holds stay taken until the pooler's run out; a new database needs 14 or later. To \
                 move to 14 or later: {MOVE_TO_14}",
                self.tag
            )
        })
    }
}

/// The `version` input as a Postgres this node can run, written as an
/// image tag names one: a major version of at most four digits and at
/// most one minor (`17`, `16.4`), with no leading zero, kept as written
/// for the tag.
// SYNC: postgres_version <-> catalog/postgres/database/metadata.json (the two `version` rules)
pub fn postgres_version(raw: &str) -> WeftResult<PostgresVersion> {
    let number = |part: &str| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()) && (part == "0" || !part.starts_with('0'));
    let (major, minor) = raw.split_once('.').map_or((raw, None), |(major, minor)| (major, Some(minor)));
    if !number(major) || major == "0" || major.len() > 4 || minor.is_some_and(|minor| !number(minor)) {
        mistake!("version '{raw}' is not a Postgres version; write its major version (17)");
    }
    match major.parse::<u32>() {
        Ok(major) => Ok(PostgresVersion { tag: raw.to_string(), major }),
        Err(_) => mistake!("version '{raw}' is not a Postgres version; write its major version (17)"),
    }
}

/// A memory size as the node's `memory` input writes it (`1Gi`, `512Mi`,
/// `2G`, `1500M`), in whole MiB, rounded down so nothing is planned for
/// memory that is not there.
pub fn memory_mib(raw: &str) -> WeftResult<u64> {
    match weft::infra::memory_mib(raw) {
        Some(mib) => Ok(mib.floor() as u64),
        None => mistake!("memory '{}' is not a size this node reads; write a number followed by Gi, Mi, G or M (1Gi)", raw.trim()),
    }
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
        let database = database_name(&input.get::<String>("database")?)?;
        let storage: String = input.get("storage")?;
        let version = postgres_version(&input.get::<String>("version")?)?;
        // Whether anything outside this program may connect. The
        // author's decision, read here because the spec IS the answer:
        // a person reading this node sees what is reachable, and
        // nothing after the fact can change it.
        let reachable: bool = input.get("reachable")?;
        // The machine, as the node's settings size it; the cloud picks
        // its cheapest that holds it.
        let memory: String = input.get("memory")?;
        let machine = MachineShape { cpu: input.opt("cpu")?, memory: Some(memory.clone()), kind: input.opt::<String>("machineType")?.filter(|t| !t.trim().is_empty()), gpu: None };
        let tuning = Tuning::for_machine(&memory)?;
        let extensions = extension_names(input.opt::<Vec<String>>("extensions")?.unwrap_or_default())?;
        let roles = role_list(input.opt::<Vec<Role>>("roles")?.unwrap_or_default())?;

        let credential = Image::Local { name: "credential".into() };
        Ok(InfraSpec {
            units: vec![Unit {
                // One copy: two Postgres processes on one data directory
                // would corrupt it, and a unit never runs beside its own
                // next version.
                name: "db".into(),
                init_containers: vec![Container::new("mint", credential.clone())
                    .with_args(vec!["mint".into()])
                    .with_env(credential_env(&database, &extensions, &roles, &version))
                    .with_mounts(vec![store_mount()])],
                containers: vec![
                    Container::new("postgres", Image::Upstream {
                        reference: format!("postgres:{}-alpine", version.tag),
                    })
                    // Only the unit's own loopback: every client comes
                    // through the pooler, which is what keeps the
                    // connections it is asked for to what it serves.
                    .with_args(tuning.args().into_iter().chain(["-c".into(), "listen_addresses=127.0.0.1".into()]).collect())
                    // Postgres's share of the machine, so going past it
                    // is an out-of-memory kill of this container that
                    // the platform can see and name, rather than the
                    // kernel picking something on a machine that ran out.
                    .with_limits(Limits { cpu: None, memory: Some(format!("{}Mi", tuning.limit_mib)) })
                    .with_env(vec![
                        EnvEntry::new("POSTGRES_USER", ADMIN_USER),
                        EnvEntry::new("POSTGRES_DB", database.clone()),
                        // The file, never the value: the password is
                        // not in this spec and never will be.
                        EnvEntry::new("POSTGRES_PASSWORD_FILE", password_file()),
                        // Postgres refuses a data directory that is a
                        // mount point with lost+found in it, so it
                        // gets a subdirectory of the mount.
                        EnvEntry::new("PGDATA", DATA_PATH),
                        // The port as the image's entrypoint reads it,
                        // for the server and for the setup it runs on
                        // first boot alike; the pooler takes 5432.
                        EnvEntry::new("PGPORT", POSTGRES_PORT.to_string()),
                    ])
                    .with_mounts(vec![store_mount(), socket_mount()])
                    // Postgres accepts TCP while it is still starting up
                    // and refuses every connection, so the port being
                    // bound is not readiness. Ask it the question it
                    // answers itself.
                    .with_readiness(
                        // Over TCP, not the unix socket: the entrypoint
                        // runs a temporary socket-only server while it
                        // initialises, so a socket check reports ready
                        // while the port is still closed. `-d postgres`
                        // because without one the check signs in to a
                        // database named after the user, which does not
                        // exist, and the server logs that on every check.
                        Probe::exec(vec![
                            "pg_isready".into(),
                            "-h".into(),
                            "127.0.0.1".into(),
                            "-p".into(),
                            POSTGRES_PORT.to_string(),
                            "-U".into(),
                            ADMIN_USER.into(),
                            "-d".into(),
                            "postgres".into(),
                        ])
                        .with_initial_delay(5),
                    ),
                    // Before the pooler: a unit says why it is not ready by its
                    // first container that is not, and the pooler only waits
                    // on this one, which knows why (`/health`).
                    Container::new("credential", credential)
                        .with_args(vec!["serve".into()])
                        .with_env(credential_env(&database, &extensions, &roles, &version))
                        .with_ports(vec![ContainerPort {
                            name: "http".into(),
                            port: CREDENTIAL_PORT,
                            protocol: Protocol::Tcp,
                        }])
                        .with_limits(Limits { cpu: Some("0.1".into()), memory: Some("64Mi".into()) })
                        .with_mounts(vec![secret_mount(), socket_mount(), pooler_mount()])
                        .with_readiness(
                            Probe::http(HEALTH_PATH, CREDENTIAL_PORT).with_initial_delay(2),
                        ),
                    Container::new("pooler", Image::Local { name: "pooler".into() })
                        .with_args(pooler_args(&database, &tuning, &roles))
                        .with_env(pooler_env(&tuning))
                        .with_ports(vec![ContainerPort {
                            name: "sql".into(),
                            port: SQL_PORT,
                            protocol: Protocol::Tcp,
                        }])
                        // No CPU limit: every byte a client and the
                        // database exchange goes through it.
                        .with_limits(Limits { cpu: None, memory: Some(format!("{POOLER_MIB}Mi")) })
                        .with_mounts(vec![pooler_mount()])
                        // Its entrypoint starts PgBouncer only once the
                        // database's sign-ins are written, so a bound
                        // port is a pooler that can let clients in. The
                        // credential container's own readiness waits for
                        // the same thing to be current.
                        .with_readiness(Probe::tcp(SQL_PORT).with_initial_delay(2)),
                ],
                fs_group: Some(POSTGRES_GID),
                machine,
                ..Default::default()
            }],
            volumes: vec![
                Volume { name: "store".into(), kind: VolumeKind::Disk { size: storage, class: None } },
                // The socket lives and dies with the unit.
                Volume { name: "socket".into(), kind: VolumeKind::Scratch { size_limit: None } },
                // So does the pooler's, with who may sign in, which the
                // credential container writes afresh on every start.
                Volume { name: "pooler".into(), kind: VolumeKind::Scratch { size_limit: None } },
            ],
            // `sql` is the only endpoint that may ever be reachable,
            // and only when the author asked: reaching the pooler there
            // still costs a password, so letting a client at it gives
            // nobody anything they did not already have. That is what
            // lets a frontend on the same machine use this database
            // for its own tables instead of standing up a second one.
            //
            // `credential` never is, and the difference is the whole
            // rule for an infra node: that endpoint HANDS OUT the
            // password, so reaching it would give the database away to
            // anything that could reach the port. It stays the
            // project's own whatever anybody asks for, which is why
            // `reachable` cannot touch it.
            endpoints: vec![
                Endpoint {
                    name: "sql".into(),
                    target: EndpointTarget::Unit { unit: "db".into(), container: "pooler".into(), port: "sql".into() },
                    expose: if reachable { Expose::SameNetwork } else { Expose::Project },
                },
                Endpoint {
                    name: "credential".into(),
                    target: EndpointTarget::Unit { unit: "db".into(), container: "credential".into(), port: "http".into() },
                    expose: Expose::Project,
                },
            ],
            keep_on_terminate: Vec::new(),
            health_rules: None,
            notes: version.note().into_iter().collect(),
        })
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let database = database_name(&ctx.inputs.get::<String>("database")?)?;
        // Three asks that need nothing from each other, so they go out
        // together.
        let (sql, credential, published) =
            tokio::try_join!(ctx.endpoint("sql"), ctx.endpoint("credential"), ctx.published_access())?;
        let (host, port) = sql.host_and_port()?;
        signs_in_as_app(&credential).await?;
        let published = match published {
            Some(access) => Some(ctx.open(&access).await?),
            None => None,
        };

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
        let (password, retired) = match &published {
            Some(opened) => {
                let held = opened.value("password")?.to_string();
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
        // The program signs in as its own role, through the pooler; the
        // superuser is never on a connection.
        values.insert("watchDatabase".to_string(), watch_database(&database));
        values.insert("database".to_string(), pooler_database(&database));
        values.insert("user".to_string(), APP_USER.to_string());
        // SYNC: the published `password` <-> images/credential/bootstrap.py (push_values)
        values.insert("password".to_string(), password.clone());
        // Inside the project's own network; the database serves no TLS.
        values.insert("sslmode".to_string(), "disable".to_string());
        // Published every run, values unchanged or not: publishing also
        // brings the connection's recipe and label up to this node's.
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

/// Refuse to publish a connection for a unit from before the program had
/// its own role: it has no `app`, and every sign-in would be refused with
/// nothing saying why. Such a unit is one this node's infra was not
/// upgraded for yet, and its credential server's health answer names no
/// role.
// SYNC: signs_in_as_app <-> catalog/postgres/database/images/credential/bootstrap.py (signsInAs)
async fn signs_in_as_app(credential: &weft::EndpointHandle) -> WeftResult<()> {
    let answer = credential.call(EndpointMethod::Get, HEALTH_PATH, None).await?;
    match answer.get("signsInAs").and_then(|v| v.as_str()) {
        Some(user) if user == APP_USER => Ok(()),
        other => weft::node_bail!(
            "this database's infra was set up by an older weft, which signs the program in as {}, not as \
             `{APP_USER}`. Run `weft infra upgrade <this node>`: the database keeps its data and gives the \
             program its own role, and the upgrade runs this node again.",
            other.map_or("its superuser".to_string(), |u| format!("`{u}`"))
        ),
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
        weft::WeftError::NodeFailed(format!(
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
    ctx: &RunContext,
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
         run again: the next run reads the new password and publishes a fresh \
         connection."
    )
}
