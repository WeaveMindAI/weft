//! `weft daemon start|stop|status|logs|remove`: a local install.
//!
//! A local install is plain processes and Docker. Three containers hold
//! state (Postgres, the object store, and when the public address is open
//! the tunnel), and one process runs every role weft has: `weft-runtime`,
//! kept alive by the machine's service manager (a systemd user unit on
//! Linux, a launchd agent on macOS). Workers and infra units are
//! containers the runtime starts itself.
//!
//! `start` is one reconcile: whatever exists is left as it is, whatever is
//! missing or out of date is made, and the runtime is (re)started on the
//! config it wrote. The config and the secrets live in the install's
//! directory (`config.json`, `secrets.env`).
//!
//! A machine that still holds the Kubernetes install an older weft ran is
//! refused: its database cannot be carried forward, and
//! `scripts/scrub-old-install.sh` wipes it (`refuse_an_older_install`).

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use tokio::process::Command;
use weft_platform_traits::config::{
    AuthMode, BuildConfig, EdgeConfig, InstallConfig, Listen, LocalPlatform, ObjectStoreSettings, PlatformConfig, S3StoreSettings, ProxyHops,
};

use super::Ctx;
use crate::images;

/// Names a NAMED install: one that sits beside the default install on the
/// same machine, with names, ports and files of its own (a test cell).
use weft_core::infra::INSTALL_ENV;
use weft_core::ports;

/// The Postgres every local install runs.
// SYNC: postgres image <-> scripts/lib/throwaway-postgres.sh (the scratch
//       image, 18), setup.sh (--purge --postgres, which reclaims it)
const POSTGRES_IMAGE: &str = "postgres:18-alpine";

/// The local database account.
// SYNC: local-dev PG credentials <-> crates/weft-e2e/src/platform.rs
//       (PG_USER/PG_PASSWORD/PG_DBNAME)
const PG_USER: &str = "weft";
const PG_PASSWORD: &str = "weft-local-dev";
const PG_DB: &str = "weft";

/// The object store every local install on this machine shares.
const OBJECT_STORE_CONTAINER: &str = "weft-object-store";
const OBJECT_STORE_IMAGE: &str = "chrislusf/seaweedfs:3.80";

/// The object store's development key pair.
// SYNC: the local store's dev key pair <-> scripts/run-e2e.sh (WEFT_E2E_S3_*)
const DEV_OBJECT_STORE_KEYS: (&str, &str) = ("weft-local", "weft-local-dev-secret");

/// The Docker network workers, infra units and the object store share.
// SYNC: NETWORK <-> crates/weft-platform-local/src/docker.rs (NETWORK)
const NETWORK: &str = "weft";

/// The tunnel that brings the open internet to the outside doors.
const TUNNEL_IMAGE: &str = "cloudflare/cloudflared:2025.8.1";

/// The Kubernetes cluster an older weft ran on this machine.
const OLD_KIND_CLUSTER: &str = "weft-local";

pub enum DaemonAction {
    /// One verb for boot AND refresh (`restart` is a CLI alias): the
    /// reconcile below is idempotent.
    Start { rebuild: bool, public_url: Option<bool> },
    Stop,
    /// Take a named install off this machine.
    Remove,
    Status,
    Logs { tail: usize, follow: bool },
}

pub async fn run(ctx: Ctx, action: DaemonAction) -> Result<()> {
    let install = Install::from_env()?;
    match action {
        DaemonAction::Start { rebuild, public_url } => {
            if let Some(choice) = public_url {
                anyhow::ensure!(
                    install.id.name().is_none(),
                    "--public-url / --no-public-url open or close the default install's public address; a named install has none"
                );
                set_public_url_choice(choice)?;
            }
            start(&install, rebuild).await
        }
        DaemonAction::Stop => stop(&install).await,
        DaemonAction::Remove => remove(&install).await,
        DaemonAction::Status => status(&ctx, &install).await,
        DaemonAction::Logs { tail, follow } => logs(&install, tail, follow).await,
    }
}

/// The root of weft's files on this machine.
pub fn data_dir() -> PathBuf {
    weft_core::infra::data_dir()
}

/// Where the default install's database files live.
pub fn postgres_data_dir() -> PathBuf {
    data_dir().join("postgres-data")
}

/// One install on this machine.
pub struct Install {
    /// Which install: the default one, or one named by `WEFT_INSTALL`.
    pub id: weft_core::infra::Install,
    /// Its files: `config.json`, `secrets.env`, the runtime's log, its
    /// database files for a named install.
    pub dir: PathBuf,
}

impl Install {
    pub fn from_env() -> Result<Self> {
        let id = weft_core::infra::Install::from_env().map_err(anyhow::Error::msg)?;
        let dir = id.dir();
        Ok(Self { id, dir })
    }

    fn config_path(&self) -> PathBuf {
        self.dir.join("config.json")
    }

    // SYNC: secrets.env <-> setup.sh (the --migration --release block reads WEFT_DATABASE_URL from it)
    fn secrets_path(&self) -> PathBuf {
        self.dir.join("secrets.env")
    }

    fn log_path(&self) -> PathBuf {
        self.dir.join("runtime.log")
    }

    fn postgres_dir(&self) -> PathBuf {
        match self.id.name() {
            None => postgres_data_dir(),
            Some(_) => self.dir.join("postgres-data"),
        }
    }

    fn prefix(&self) -> String {
        self.id.resource_prefix()
    }

    // SYNC: secrets.env <-> setup.sh (the --migration --release block reads WEFT_DATABASE_URL from it)
    fn postgres_container(&self) -> String {
        format!("{}-postgres", self.prefix())
    }

    fn tunnel_container(&self) -> String {
        format!("{}-tunnel", self.prefix())
    }

    /// The service manager's name for the runtime.
    fn service_name(&self) -> String {
        format!("{}-runtime", self.prefix())
    }

    fn bucket(&self) -> String {
        self.prefix()
    }
}

// ----- ports -------------------------------------------------------------

type Ports = weft_core::ports::InstallPorts;

fn env_port(name: &str, default: u16) -> Result<u16> {
    match std::env::var(name).ok().filter(|v| !v.trim().is_empty()) {
        Some(v) => v.trim().parse().map_err(|_| anyhow::anyhow!("{name}='{v}' is not a port")),
        None => Ok(default),
    }
}

fn free_port() -> Result<u16> {
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?.port())
}

/// One port the install is about to listen on, and what to tell the
/// person when something else already holds it.
struct PortUse {
    addr: std::net::SocketAddr,
    /// What weft serves there, in the person's words.
    what: &'static str,
    /// How to give weft another port instead.
    move_it: String,
}

impl PortUse {
    /// A port of `install`: the default install moves it with `env`, a
    /// named one in its `ports.json`.
    fn of(install: &Install, addr: std::net::SocketAddr, what: &'static str, env: &str) -> Self {
        let move_it = match install.id.name() {
            None => format!("{env}=<port> ./setup.sh (the install keeps that port from then on)"),
            Some(_) => format!("pick another port for this install in {}", Ports::path(&install.dir).display()),
        };
        Self { addr, what, move_it }
    }
}

/// Refuse to start while another program holds one of `uses`, naming the
/// port, what weft needs it for, what holds it when this machine can say,
/// and how to move weft elsewhere. Called only once weft's own holder of
/// those ports is stopped, so whatever is still there is someone else.
/// Without it, a taken port surfaces minutes later as a runtime that
/// never answers.
async fn refuse_taken_ports(uses: &[PortUse]) -> Result<()> {
    for u in uses {
        // Dropped at once: this only asks whether the bind would succeed.
        if std::net::TcpListener::bind(u.addr).is_ok() {
            continue;
        }
        let port = u.addr.port();
        if let Some(h) = port_holder(port).await {
            anyhow::bail!(
                "weft needs port {port} for {}, and something else is already listening on it: it is held by {h}. Stop that program, or give weft another port: {}",
                u.what,
                u.move_it
            );
        }
        if on_wsl() && windows_excludes(port).await {
            anyhow::bail!(
                "weft needs port {port} for {}, and Windows has reserved it: it falls in a range Windows keeps off limits (`netsh int ipv4 show excludedportrange protocol=tcp` lists them). Either, in an administrator terminal, reset Windows' dynamic range with `netsh int ipv4 set dynamic tcp start=49152 num=16384` and reboot; or reserve weft's ports for weft with `net stop winnat`, then `netsh int ipv4 add excludedportrange protocol=tcp startport=14111 numberofports=8`, then `net start winnat`. Or give weft another port: {}",
                u.what,
                u.move_it
            );
        }
        anyhow::bail!(
            "weft needs port {port} for {}, and something else is already listening on it: this machine does not say by what (on WSL it can be a Windows program). Stop that program, or give weft another port: {}",
            u.what,
            u.move_it
        );
    }
    Ok(())
}

/// Whether this Linux is WSL, where Windows can hold or reserve a port
/// that Linux tools cannot see.
fn on_wsl() -> bool {
    std::path::Path::new("/proc/sys/fs/binfmt_misc/WSLInterop").exists()
        || std::fs::read_to_string("/proc/version").is_ok_and(|v| v.to_lowercase().contains("microsoft"))
}

/// Whether Windows keeps `port` out of reach (Hyper-V and WinNAT reserve
/// ranges at boot, and a reserved port refuses every bind). A missing
/// netsh, or output it cannot read, answers no: the caller then says it
/// does not know.
async fn windows_excludes(port: u16) -> bool {
    let Ok(out) = Command::new("netsh.exe").args(["int", "ipv4", "show", "excludedportrange", "protocol=tcp"]).output().await else {
        return false;
    };
    excluded_by_netsh(&String::from_utf8_lossy(&out.stdout), port)
}

/// Whether `port` falls in one of the ranges `netsh int ipv4 show
/// excludedportrange` prints, one per line as `<start> <end>` with an
/// optional `*` for an administered exclusion.
fn excluded_by_netsh(output: &str, port: u16) -> bool {
    output.lines().any(|line| {
        let mut nums = line.split_whitespace().map(str::parse::<u16>);
        matches!((nums.next(), nums.next()), (Some(Ok(start)), Some(Ok(end))) if (start..=end).contains(&port))
    })
}

/// The program listening on `port`, as `ss` (Linux) or `lsof` (macOS)
/// names it; `None` when neither can tell.
async fn port_holder(port: u16) -> Option<String> {
    let ss = Command::new("ss").args(["-ltnpH", &format!("sport = :{port}")]).output().await.ok();
    if let Some(out) = ss.filter(|o| o.status.success()) {
        let text = String::from_utf8_lossy(&out.stdout);
        // `users:(("name",pid=123,fd=4))`: the name and pid, when this
        // user may see them.
        if let Some(users) = text.split("users:((").nth(1) {
            let mut parts = users.split(',');
            let name = parts.next().unwrap_or("").trim_matches('"');
            let pid = parts.next().and_then(|p| p.strip_prefix("pid=")).unwrap_or("?");
            if !name.is_empty() {
                return Some(format!("{name} (pid {pid})"));
            }
        }
        if !text.trim().is_empty() {
            return Some("a process of another user".to_string());
        }
    }
    let lsof = Command::new("lsof").args(["-nP", &format!("-iTCP:{port}"), "-sTCP:LISTEN", "-Fcp"]).output().await.ok()?;
    let text = String::from_utf8_lossy(&lsof.stdout);
    let pid = text.lines().find_map(|l| l.strip_prefix('p'))?;
    let name = text.lines().find_map(|l| l.strip_prefix('c')).unwrap_or("?");
    Some(format!("{name} (pid {pid})"))
}

/// The ports this start runs the install on, saved in its `ports.json`
/// so every later command (and the next start) finds the same address.
/// A first start seeds them: the block's ports for the default install,
/// free ports for a named one (it sits beside the default install, so the
/// block is taken). On the default install a `WEFT_*_PORT` set on this
/// start moves that port for good; a named install ignores them, because
/// a variable exported for the default install would otherwise land every
/// test cell on the same port. A named install's ports move by editing
/// its file.
fn ports(install: &Install) -> Result<Ports> {
    let saved = Ports::load(&install.dir).map_err(anyhow::Error::msg)?;
    let seeded = match (saved, install.id.name()) {
        (Some(saved), _) => saved,
        (None, None) => Ports::DEFAULT,
        (None, Some(_)) => Ports { public: free_port()?, internal: free_port()?, outside: free_port()?, postgres: free_port()? },
    };
    let chosen = match install.id.name() {
        Some(_) => seeded,
        None => moved_by_env(seeded, |name| std::env::var(name).ok())?,
    };
    if saved != Some(chosen) {
        chosen.save(&install.dir).with_context(|| format!("save {}", Ports::path(&install.dir).display()))?;
    }
    Ok(chosen)
}

/// `ports` with every port a `WEFT_*_PORT` variable sets moved there.
fn moved_by_env(ports: Ports, var: impl Fn(&str) -> Option<String>) -> Result<Ports> {
    let port = |name: &str, kept: u16| -> Result<u16> {
        match var(name).filter(|v| !v.trim().is_empty()) {
            Some(v) => v.trim().parse().map_err(|_| anyhow::anyhow!("{name}='{v}' is not a port")),
            None => Ok(kept),
        }
    };
    Ok(Ports {
        public: port("WEFT_PUBLIC_PORT", ports.public)?,
        internal: port("WEFT_INTERNAL_PORT", ports.internal)?,
        outside: port("WEFT_OUTSIDE_PORT", ports.outside)?,
        postgres: port("WEFT_POSTGRES_PORT", ports.postgres)?,
    })
}

fn object_store_port() -> Result<u16> {
    env_port("WEFT_SEAWEED_PORT", ports::OBJECT_STORE)
}

// ----- the public address ---------------------------------------------

fn public_url_marker() -> PathBuf {
    data_dir().join("public-url-enabled")
}

fn public_url_file() -> PathBuf {
    data_dir().join("public-url")
}

/// The public address the default install answers at, or `None` when it
/// is closed.
pub fn current_public_url() -> Option<String> {
    if !public_url_marker().exists() {
        return None;
    }
    std::fs::read_to_string(public_url_file()).ok().map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
}

fn set_public_url_choice(open: bool) -> Result<()> {
    if open {
        std::fs::create_dir_all(data_dir())?;
        std::fs::write(public_url_marker(), b"")?;
        return Ok(());
    }
    for path in [public_url_marker(), public_url_file()] {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// The operator's named tunnel (a stable hostname): its Cloudflare token
/// and the https hostname it routes. Both or neither.
fn named_tunnel_config() -> Result<Option<(String, String)>> {
    let token = std::env::var("WEFT_PUBLIC_TUNNEL_TOKEN").ok().filter(|v| !v.is_empty());
    let hostname = std::env::var("WEFT_PUBLIC_TUNNEL_HOSTNAME").ok().filter(|v| !v.is_empty());
    match (token, hostname) {
        (Some(token), Some(hostname)) => Ok(Some((token, canonical_tunnel_hostname(&hostname)?))),
        (None, None) => Ok(None),
        (Some(_), None) => anyhow::bail!(
            "WEFT_PUBLIC_TUNNEL_TOKEN is set but WEFT_PUBLIC_TUNNEL_HOSTNAME is not; set both \
             (the hostname the tunnel's Cloudflare config routes to weft)"
        ),
        (None, Some(_)) => anyhow::bail!(
            "WEFT_PUBLIC_TUNNEL_HOSTNAME is set but WEFT_PUBLIC_TUNNEL_TOKEN is not; set both \
             (the token from the tunnel's Cloudflare dashboard page)"
        ),
    }
}

/// `https://<host>` from the named tunnel's hostname, refusing a path,
/// query, port or scheme: the value is the base every public URL is
/// joined onto.
fn canonical_tunnel_hostname(raw: &str) -> Result<String> {
    let parsed = url::Url::parse(raw).map_err(|e| anyhow::anyhow!("WEFT_PUBLIC_TUNNEL_HOSTNAME '{raw}' is not a URL: {e}"))?;
    anyhow::ensure!(parsed.scheme() == "https", "WEFT_PUBLIC_TUNNEL_HOSTNAME must be https (got '{raw}')");
    let host = parsed
        .host_str()
        .filter(|h| h.contains('.'))
        .ok_or_else(|| anyhow::anyhow!("WEFT_PUBLIC_TUNNEL_HOSTNAME '{raw}' has no hostname"))?;
    anyhow::ensure!(parsed.port().is_none(), "WEFT_PUBLIC_TUNNEL_HOSTNAME must not carry a port (a named tunnel serves on 443); got '{raw}'");
    anyhow::ensure!(
        parsed.path() == "/" && parsed.query().is_none() && parsed.fragment().is_none(),
        "WEFT_PUBLIC_TUNNEL_HOSTNAME must be the bare hostname with no path or query (e.g. https://weft.example.com); got '{raw}'"
    );
    Ok(format!("https://{host}"))
}

// ----- docker helpers ----------------------------------------------------

async fn docker_ok(args: &[&str], what: &str) -> Result<String> {
    let out = images::docker().args(args).output().await.map_err(|e| anyhow::anyhow!("run docker: {e} (is Docker installed and running?)"))?;
    anyhow::ensure!(out.status.success(), "{what} failed: {}", String::from_utf8_lossy(&out.stderr).trim());
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

async fn container_state(name: &str) -> Result<Option<String>> {
    let out = images::docker().args(["inspect", "--format", "{{.State.Status}}", name]).output().await?;
    Ok(out.status.success().then(|| String::from_utf8_lossy(&out.stdout).trim().to_string()))
}

async fn remove_container(name: &str) -> Result<()> {
    let out = images::docker().args(["rm", "-f", name]).output().await?;
    anyhow::ensure!(
        out.status.success() || String::from_utf8_lossy(&out.stderr).contains("No such container"),
        "docker rm {name} failed: {}",
        String::from_utf8_lossy(&out.stderr).trim()
    );
    Ok(())
}

/// Whether `docker start`'s error says the container's network is gone.
fn stale_network(stderr: &str) -> bool {
    stderr.contains("network") && stderr.contains("not found")
}

async fn ensure_network() -> Result<()> {
    if images::docker().args(["network", "inspect", NETWORK]).output().await?.status.success() {
        return Ok(());
    }
    let out = images::docker().args(["network", "create", NETWORK]).output().await?;
    anyhow::ensure!(
        out.status.success() || String::from_utf8_lossy(&out.stderr).contains("already exists"),
        "docker network create {NETWORK} failed: {}",
        String::from_utf8_lossy(&out.stderr).trim()
    );
    Ok(())
}

/// Run a container from `args` unless one of that name already runs from
/// exactly these arguments. A container cannot change its ports, mounts
/// or image in place, so when the arguments move it is made again (its
/// named volumes and bind-mounted directories are untouched).
async fn ensure_container(name: &str, args: &[String], stamp_dir: &Path, published: Option<PortUse>) -> Result<()> {
    let stamp = stamp_dir.join(format!("{name}.run.sha256"));
    let want = format!("{:x}", Sha256::digest(args.join("\u{1f}").as_bytes()));
    let current = std::fs::read_to_string(&stamp).ok().map(|s| s.trim().to_string()) == Some(want.clone());
    let state = container_state(name).await?;
    // A container of ours at the wanted spec that runs holds the port
    // itself; any other case is about to take it, so the port is checked
    // once ours is out of the way.
    match state {
        Some(s) if current && s == "running" => return Ok(()),
        Some(_) if current => {
            refuse_taken_ports(published.as_slice()).await?;
            let out = images::docker().args(["start", name]).output().await?;
            if out.status.success() {
                return Ok(());
            }
            let stderr = String::from_utf8_lossy(&out.stderr);
            // A stopped container keeps the id of the network it was
            // attached to; when Docker's networks are made again (a Docker
            // or WSL reset, a moved disk) that id is gone and the container
            // can never start. Its state lives in named volumes and bind
            // mounts, so it is made again from the same arguments.
            anyhow::ensure!(stale_network(&stderr), "docker start {name} failed: {}", stderr.trim());
            eprintln!("{name} was attached to a network Docker no longer has; making it again (its data is kept)");
            remove_container(name).await?;
        }
        Some(_) => remove_container(name).await?,
        None => {}
    }
    refuse_taken_ports(published.as_slice()).await?;
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    docker_ok(&refs, &format!("docker run {name}")).await?;
    std::fs::create_dir_all(stamp_dir)?;
    std::fs::write(&stamp, want)?;
    Ok(())
}

/// A one-shot container over `dir`, for a path the host user cannot touch
/// (Postgres owns its files as its own user).
async fn run_in_alpine(dir: &Path, mount_spec: &str, script: &str) -> Result<std::process::Output> {
    let host = dir.display().to_string();
    anyhow::ensure!(
        !host.contains(':'),
        "cannot docker-mount {host}: docker's -v syntax cannot carry a path containing ':' (move the weft data directory)"
    );
    Ok(images::docker().args(["run", "--rm", "-v"]).arg(format!("{host}:{mount_spec}")).args(["alpine:3", "sh", "-c", script]).output().await?)
}

// ----- postgres ----------------------------------------------------------

/// The major version the Postgres image runs.
fn postgres_major() -> &'static str {
    POSTGRES_IMAGE.split(':').nth(1).and_then(|t| t.split(['-', '.']).next()).expect("POSTGRES_IMAGE carries a major")
}

/// Refuse to start Postgres on files another major wrote (it would refuse
/// itself, less clearly).
async fn guard_postgres_major(dir: &Path) -> Result<()> {
    if !dir.join("pgdata").is_dir() {
        return Ok(());
    }
    let out = run_in_alpine(dir, "/d:ro", "if [ -f /d/pgdata/PG_VERSION ]; then cat /d/pgdata/PG_VERSION; else echo __ABSENT__; fi").await?;
    anyhow::ensure!(
        out.status.success(),
        "docker could not read {}/pgdata to check its Postgres major version: {}",
        dir.display(),
        String::from_utf8_lossy(&out.stderr)
    );
    let on_disk = String::from_utf8_lossy(&out.stdout).trim().to_string();
    let data = dir.display();
    anyhow::ensure!(
        on_disk != "__ABSENT__",
        "{data}/pgdata exists but holds no PG_VERSION: Postgres died before finishing initdb. Remove it:\n  \
         docker run --rm -v {data}:/d alpine:3 sh -c 'rm -rf /d/pgdata'\nthen start again."
    );
    let wanted = postgres_major();
    anyhow::ensure!(
        on_disk == wanted,
        "the database files in {data} were written by Postgres {on_disk}, and weft runs Postgres {wanted}, which refuses \
         to start on them. Upgrade them with pg_upgrade, or start from an empty database by removing them:\n  \
         docker run --rm -v {data}:/d alpine:3 sh -c 'rm -rf /d/pgdata'"
    );
    Ok(())
}

async fn ensure_postgres(install: &Install, port: u16) -> Result<()> {
    let dir = install.postgres_dir();
    std::fs::create_dir_all(&dir)?;
    guard_postgres_major(&dir).await?;
    let name = install.postgres_container();
    let args: Vec<String> = [
        "run", "-d", "--name", &name, "--restart", "unless-stopped",
        "-p", &format!("127.0.0.1:{port}:5432"),
        "-v", &format!("{}:/var/lib/postgresql/data", dir.display()),
        "-e", &format!("POSTGRES_USER={PG_USER}"),
        "-e", &format!("POSTGRES_PASSWORD={PG_PASSWORD}"),
        "-e", &format!("POSTGRES_DB={PG_DB}"),
        "-e", "PGDATA=/var/lib/postgresql/data/pgdata",
        "--label", &format!("{}={}", weft_core::infra::INSTALL_LABEL, install.id.label_value()),
        POSTGRES_IMAGE,
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let published = PortUse::of(install, ([127, 0, 0, 1], port).into(), "its database", "WEFT_POSTGRES_PORT");
    ensure_container(&name, &args, &install.dir, Some(published)).await?;
    // Ready before the runtime connects: it applies the schema at boot.
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    loop {
        let ready = images::docker().args(["exec", &name, "pg_isready", "-U", PG_USER, "-d", PG_DB]).output().await?;
        if ready.status.success() {
            return Ok(());
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "Postgres did not become ready; its log: docker logs {name}"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn database_url(port: u16) -> String {
    format!("postgres://{PG_USER}:{PG_PASSWORD}@127.0.0.1:{port}/{PG_DB}")
}

// ----- object store ------------------------------------------------------

fn object_store_keys() -> (String, String) {
    let read = |name: &str, dev: &str| std::env::var(name).ok().filter(|v| !v.is_empty()).unwrap_or_else(|| dev.to_string());
    (
        read("WEFT_OBJECT_STORE_ACCESS_KEY", DEV_OBJECT_STORE_KEYS.0),
        read("WEFT_OBJECT_STORE_SECRET_KEY", DEV_OBJECT_STORE_KEYS.1),
    )
}

/// The S3 identities file the object store checks signatures against.
fn object_store_s3_config(access_key: &str, secret_key: &str) -> String {
    serde_json::json!({
        "identities": [{
            "name": "weft",
            "credentials": [{ "accessKey": access_key, "secretKey": secret_key }],
            "actions": ["Admin", "Read", "Write", "List", "Tagging"],
        }]
    })
    .to_string()
}

/// The object store: SeaweedFS's S3 gateway, one per machine. The runtime
/// reaches it on loopback, a worker by name on the install's network.
/// `-s3.externalUrl` stays unset so the gateway checks each presigned
/// request against its own Host header, which lets one store take URLs
/// signed for either address.
async fn ensure_object_store(port: u16) -> Result<()> {
    let cfg_dir = data_dir().join("object-store");
    let cfg_path = cfg_dir.join("s3.config.json");
    // Docker makes a missing bind-mount source a root-owned DIRECTORY,
    // which then cannot be mounted onto a file: heal it.
    if cfg_path.is_dir() && std::fs::remove_dir_all(&cfg_path).is_err() {
        let out = run_in_alpine(&cfg_dir, "/heal", "rm -rf /heal/s3.config.json").await?;
        anyhow::ensure!(
            out.status.success(),
            "the object-store config path {} is a directory Docker made and could not be removed; remove it by hand: sudo rm -rf {}",
            cfg_path.display(),
            cfg_dir.display()
        );
    }
    std::fs::create_dir_all(&cfg_dir)?;
    let (access, secret) = object_store_keys();
    std::fs::write(&cfg_path, object_store_s3_config(&access, &secret))?;
    let args: Vec<String> = [
        "run", "-d", "--name", OBJECT_STORE_CONTAINER, "--restart", "unless-stopped",
        "--network", NETWORK,
        "-p", &format!("127.0.0.1:{port}:8333"),
        "-v", &format!("{OBJECT_STORE_CONTAINER}-data:/data"),
        "-v", &format!("{}:/etc/seaweedfs/s3.config.json:ro", cfg_path.display()),
        OBJECT_STORE_IMAGE,
        "server", "-dir=/data", "-s3", "-s3.port=8333", "-s3.config=/etc/seaweedfs/s3.config.json",
        "-master.volumeSizeLimitMB=1024",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    // Shared by every install on this machine, so one variable moves it.
    let published = PortUse { addr: ([127, 0, 0, 1], port).into(), what: "its file store", move_it: "WEFT_SEAWEED_PORT=<port> ./setup.sh".into() };
    ensure_container(OBJECT_STORE_CONTAINER, &args, &data_dir(), Some(published)).await
}

/// Drop a named install's bucket from the object store. The store's
/// shell exits 0 on errors, so the listing afterwards says whether it is
/// gone.
async fn delete_bucket(bucket: &str) -> Result<()> {
    let weed = |command: String| {
        images::docker().args(["exec", OBJECT_STORE_CONTAINER, "sh", "-c", &format!("echo '{command}' | weed shell")]).output()
    };
    weed(format!("s3.bucket.delete -name {bucket}")).await?;
    let listing = weed("s3.bucket.list".to_string()).await?;
    anyhow::ensure!(listing.status.success(), "listing the object store's buckets failed: {}", String::from_utf8_lossy(&listing.stderr));
    anyhow::ensure!(
        !String::from_utf8_lossy(&listing.stdout).split_whitespace().any(|w| w == bucket),
        "the bucket {bucket} is still on the object store after deleting it"
    );
    Ok(())
}

// ----- secrets -----------------------------------------------------------

/// 32 random bytes, hex.
fn random_hex_32() -> String {
    format!("{}{}", uuid::Uuid::new_v4().simple(), uuid::Uuid::new_v4().simple())
}

fn read_env_file(path: &Path) -> std::collections::BTreeMap<String, String> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|l| l.split_once('='))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect()
}

fn write_private(path: &Path, content: &str) -> Result<()> {
    use std::io::Write as _;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut opts, 0o600);
    let mut f = opts.open(path).with_context(|| format!("write {}", path.display()))?;
    f.write_all(content.as_bytes())?;
    Ok(())
}

/// The shared-credentials file: `WEFT_ACCESS_APPS_FILE`, or
/// `access-apps.json` at the root of the weft checkout the install comes
/// from (never the working directory).
fn access_apps_file(repo_root: &Path) -> PathBuf {
    match std::env::var(weft_core::access::spec::APPS_FILE_ENV).ok().filter(|v| !v.trim().is_empty()) {
        Some(path) => PathBuf::from(path),
        None => repo_root.join("access-apps.json"),
    }
}

/// The runtime's environment: every secret it reads (`SECRET_ENV`) and
/// where it finds the weft source and the shared credentials. A key the
/// install made once (its identity key, the ticket secret) is kept as it
/// is: a new one would void every token already handed out.
fn secrets(
    install: &Install,
    ports: Ports,
    repo_root: &Path,
) -> Result<std::collections::BTreeMap<String, String>> {
    let old = read_env_file(&install.secrets_path());
    let keep_or = |name: &str, make: &mut dyn FnMut() -> String| old.get(name).filter(|v| !v.is_empty()).cloned().unwrap_or_else(make);
    let mut env = std::collections::BTreeMap::new();
    // SYNC: secrets.env <-> setup.sh (the --migration --release block reads WEFT_DATABASE_URL from it)
    env.insert("WEFT_DATABASE_URL".to_string(), database_url(ports.postgres));
    env.insert("WEFT_IDENTITY_KEY".to_string(), keep_or("WEFT_IDENTITY_KEY", &mut random_hex_32));
    env.insert("WEFT_CALLER_TOKEN_SECRET".to_string(), keep_or("WEFT_CALLER_TOKEN_SECRET", &mut random_hex_32));
    // The sealing key is the operator's (the shell or the `.env`); once
    // given it is kept, since rows sealed with it open under no other.
    if let Some(key) = std::env::var("CREDENTIAL_ENCRYPTION_KEY").ok().filter(|v| !v.is_empty()).or_else(|| old.get("CREDENTIAL_ENCRYPTION_KEY").cloned()) {
        env.insert("CREDENTIAL_ENCRYPTION_KEY".to_string(), key);
    }
    let (access, secret) = object_store_keys();
    env.insert("WEFT_OBJECT_STORE_ACCESS_KEY".to_string(), access);
    env.insert("WEFT_OBJECT_STORE_SECRET_KEY".to_string(), secret);
    env.insert(weft_core::access::spec::APPS_FILE_ENV.to_string(), access_apps_file(repo_root).display().to_string());
    env.insert("WEFT_REPO_ROOT".to_string(), repo_root.display().to_string());
    if let Ok(scale) = std::env::var(weft_core::time_scale::TIME_SCALE_ENV) {
        weft_core::time_scale::parse(Some(&scale)).map_err(anyhow::Error::msg)?;
        env.insert(weft_core::time_scale::TIME_SCALE_ENV.to_string(), scale);
    }
    Ok(env)
}

// ----- the config --------------------------------------------------------

/// The address a container reaches the machine's internal port at. On
/// Linux the machine IS the Docker host, and a container reaches it
/// through the `host-gateway` name the runner adds; Docker Desktop does
/// the same by itself.
fn container_internal_url(ports: Ports) -> String {
    format!("http://host.docker.internal:{}", ports.internal)
}

/// Where the internal port listens. A worker reaches it from its
/// container through the Docker bridge, which on Linux is not loopback,
/// so it listens on every interface there (every internal route checks
/// its caller's identity). Docker Desktop brings a container's calls to
/// the machine's loopback, so it stays on loopback there.
fn internal_listen(ports: Ports) -> std::net::SocketAddr {
    let host = if cfg!(target_os = "linux") { [0, 0, 0, 0] } else { [127, 0, 0, 1] };
    (host, ports.internal).into()
}

/// The edge settings: an environment variable when one is set, what the
/// install already had otherwise, the defaults on a new install.
fn edge_config(kept: Option<KeptEdge>) -> Result<EdgeConfig> {
    // The public port is reached straight from this machine; the outside
    // port through cloudflared, which reaches it from loopback and appends
    // the internet caller to `X-Forwarded-For`, so one hop is trusted
    // there or every internet caller would count as 127.0.0.1.
    let kept = kept.unwrap_or(KeptEdge { trusted_proxy_hops: KeptHops { public: 0, outside: 1 }, invalid_tokens_per_minute: Some(30) });
    let hops = std::env::var("WEFT_TRUSTED_PROXY_HOPS").ok().filter(|v| !v.trim().is_empty());
    let invalid = std::env::var("WEFT_INVALID_TOKENS_PER_MINUTE").ok().filter(|v| !v.trim().is_empty());
    Ok(EdgeConfig {
        trusted_proxy_hops: ProxyHops {
            public: match hops {
                Some(v) => v.trim().parse().map_err(|_| anyhow::anyhow!("WEFT_TRUSTED_PROXY_HOPS='{v}' is not a whole number"))?,
                None => kept.trusted_proxy_hops.public,
            },
            outside: kept.trusted_proxy_hops.outside,
            // A local install has no domains of its own.
            domains: 0,
        },
        invalid_tokens_per_minute: match invalid.as_deref() {
            Some("off") => None,
            Some(v) => Some(v.trim().parse().map_err(|_| anyhow::anyhow!("WEFT_INVALID_TOKENS_PER_MINUTE='{v}' is not a whole number (or `off`)"))?),
            None => kept.invalid_tokens_per_minute,
        },
    })
}

/// The levers a person sets in `config.json`, as the last start left
/// them: the worker defaults, the edge, how long idle workers stay up.
/// Only these are read from the file, each where it sits: the rest of it
/// is weft's own and written afresh every start, so a shape a newer weft
/// writes differently never stops the start that rewrites it.
#[derive(Debug, Clone, Default, PartialEq)]
struct Levers {
    worker_idle_stop_seconds: Option<u64>,
    workers: Option<weft_platform_traits::WorkerSettings>,
    edge: Option<KeptEdge>,
}

/// The edge settings a person may change, as kept from the last start's
/// file: the rest of `edge` is weft's own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
struct KeptEdge {
    #[serde(rename = "trustedProxyHops")]
    trusted_proxy_hops: KeptHops,
    #[serde(rename = "invalidTokensPerMinute", deserialize_with = "Option::deserialize")]
    invalid_tokens_per_minute: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
struct KeptHops {
    public: usize,
    outside: usize,
}

impl Levers {
    fn read(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        let file: serde_json::Value =
            serde_json::from_str(&raw).with_context(|| format!("{} is not JSON", path.display()))?;
        fn lever<T: serde::de::DeserializeOwned>(file: &serde_json::Value, path: &Path, at: &str) -> Result<Option<T>> {
            file.pointer(at)
                .filter(|v| !v.is_null())
                .map(|v| serde_json::from_value(v.clone()).with_context(|| format!("{} at {at} is not valid", path.display())))
                .transpose()
        }
        Ok(Self {
            worker_idle_stop_seconds: lever(&file, path, "/platform/workerIdleStopSeconds")?,
            workers: lever(&file, path, "/workers")?,
            edge: lever(&file, path, "/edge")?,
        })
    }
}

/// The install's config. Everything that follows from this machine is
/// set here; the levers a person sets in `config.json` are kept from
/// `previous`, what the last start wrote.
fn install_config(
    install: &Install,
    ports: Ports,
    runtime_image: String,
    builder_base: String,
    internet_url: Option<String>,
    previous: Levers,
) -> Result<InstallConfig> {
    let idle_stop = match (previous.worker_idle_stop_seconds, std::env::var("WEFT_WORKER_IDLE_STOP_SECONDS").ok().filter(|v| !v.trim().is_empty())) {
        (_, Some(v)) => v.trim().parse().map_err(|_| anyhow::anyhow!("WEFT_WORKER_IDLE_STOP_SECONDS='{v}' is not a whole number"))?,
        (Some(kept), None) => kept,
        (None, None) => 300,
    };
    let workers = previous.workers.unwrap_or_default();
    let edge = edge_config(previous.edge)?;
    let config = InstallConfig {
        install: install.id.clone(),
        platform: PlatformConfig::Local(LocalPlatform {
            data_dir: install.dir.clone(),
            container_internal_url: container_internal_url(ports),
            runtime_image,
            worker_idle_stop_seconds: idle_stop,
            listen: Listen {
                public: ([127, 0, 0, 1], ports.public).into(),
                internal: internal_listen(ports),
                // Served whether or not a tunnel carries it: on loopback it
                // answers a subset of what the public port does, and a
                // tunnel opened later finds it already there.
                outside: Some(([127, 0, 0, 1], ports.outside).into()),
            },
            internal_url: format!("http://127.0.0.1:{}", ports.internal),
        }),
        auth: AuthMode::Local,
        public_url: format!("http://127.0.0.1:{}", ports.public),
        internet_url,
        roles: Default::default(),
        role_urls: Default::default(),
        workers,
        holders: Default::default(),
        build: BuildConfig {
            compile_lanes: images::compile_lanes()?,
            builder_base_image: builder_base,
            runtime_base_image: weft_compiler::worker_image::DEFAULT_BASE_IMAGE.to_string(),
        },
        edge,
        object_store: ObjectStoreSettings::S3(S3StoreSettings {
            endpoint: format!("http://127.0.0.1:{}", object_store_port()?),
            bucket: install.bucket(),
            region: "us-east-1".into(),
            force_path_style: true,
            public_endpoint: None,
            worker_endpoint: Some(format!("http://{OBJECT_STORE_CONTAINER}:8333")),
            public_internet: false,
        }),
        source: None,
    };
    config.validate().map_err(|e| anyhow::anyhow!("the install config weft made is not valid: {e}"))?;
    Ok(config)
}

/// The ports a local install's one process serves.
fn listen_of(config: &InstallConfig) -> &Listen {
    match &config.platform {
        PlatformConfig::Local(local) => &local.listen,
        PlatformConfig::Gcp(_) => unreachable!("the daemon only ever writes a local install's config"),
    }
}

// ----- the tunnel --------------------------------------------------------

/// Bring the tunnel up (or down) to match the persisted choice, and
/// answer its address when it is up. The tunnel carries only the outside
/// port: the doors outside callers use, never the management API.
async fn reconcile_tunnel(install: &Install, ports: Ports) -> Result<Option<(String, bool)>> {
    let name = install.tunnel_container();
    if install.id.name().is_some() || !public_url_marker().exists() {
        remove_container(&name).await?;
        let _ = std::fs::remove_file(public_url_file());
        return Ok(None);
    }
    let named = named_tunnel_config()?;
    let (network, origin) = tunnel_origin(ports);
    let label = format!("{}={}", weft_core::infra::INSTALL_LABEL, install.id.label_value());
    let mut args: Vec<String> = ["run", "-d", "--name", &name, "--restart", "unless-stopped", "--network", &network, "--label", &label]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let env_file = install.dir.join("tunnel.env");
    if let Some((token, _)) = &named {
        write_private(&env_file, &format!("TUNNEL_TOKEN={token}\n"))?;
        args.extend(["--env-file".into(), env_file.display().to_string()]);
    }
    args.extend([TUNNEL_IMAGE.to_string(), "tunnel".into(), "--no-autoupdate".into()]);
    match &named {
        Some(_) => args.push("run".into()),
        None => args.extend(["--url".into(), origin]),
    }
    ensure_container(&name, &args, &install.dir, None).await?;
    let url = match named {
        Some((_, hostname)) => (hostname, true),
        None => (quick_tunnel_url(&name).await?, false),
    };
    Ok(Some(url))
}

/// The Docker network the tunnel runs on, and the address it reaches the
/// outside port at from there.
fn tunnel_origin(ports: Ports) -> (String, String) {
    if cfg!(target_os = "linux") {
        ("host".to_string(), format!("http://127.0.0.1:{}", ports.outside))
    } else {
        ("bridge".to_string(), format!("http://host.docker.internal:{}", ports.outside))
    }
}

/// The quick tunnel's minted address, from its log (the newest one: a
/// quick tunnel mints a new address when it reconnects).
async fn quick_tunnel_url(container: &str) -> Result<String> {
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    loop {
        let out = images::docker().args(["logs", "--tail", "200", container]).output().await?;
        let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
        if let Some(url) = text.split_whitespace().rfind(|w| w.starts_with("https://") && w.contains(".trycloudflare.com")) {
            return Ok(url.trim_end_matches('/').to_string());
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "the tunnel reported no public address; its log: docker logs {container}"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Prove the address reaches the outside doors, then record it.
async fn announce_tunnel(url: &str, named: bool, container: &str, ports: Ports) -> Result<()> {
    let client = reqwest::Client::builder().timeout(Duration::from_secs(10)).build()?;
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    loop {
        let seen = match client.get(format!("{url}/")).send().await {
            Ok(r) if r.status().is_success() => break,
            Ok(r) => format!("it answered {}", r.status()),
            Err(e) => format!("the request failed: {e}"),
        };
        if std::time::Instant::now() > deadline {
            anyhow::bail!(
                "{url} does not reach weft ({seen}).{} The tunnel's log: docker logs {container}",
                if named {
                    format!(
                        " In the Cloudflare dashboard, the tunnel's public hostname must send its traffic to {} \
                         (a tunnel set up for an older weft sends it to a Kubernetes service that no longer exists).",
                        tunnel_origin(ports).1
                    )
                } else {
                    String::new()
                }
            );
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    std::fs::create_dir_all(data_dir())?;
    std::fs::write(public_url_file(), url)?;
    println!("public address: {url}");
    println!("  it carries only the doors outside callers use: /events/... (provider event pushes), /signal/... (fire links), /signal-token/..., /public/files/... (shared file links), /connect/... and /live/... (live routes), /infra/... (public infra endpoints) and the OAuth callback. Rerun with --no-public-url to close it.");
    if !named {
        println!(
            "  NOTE: this free-tunnel address changes whenever the tunnel reconnects, and everything registered against it \
             (a provider's event push URL, an OAuth redirect) stops working until registered again. For a stable address, \
             set WEFT_PUBLIC_TUNNEL_TOKEN + WEFT_PUBLIC_TUNNEL_HOSTNAME (https://weavemindai.github.io/weft/build/public-address.html)."
        );
    }
    Ok(())
}

// ----- the service manager ---------------------------------------------

/// What keeps the runtime running.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Manager {
    /// A systemd user unit (Linux).
    Systemd,
    /// A launchd agent (macOS).
    Launchd,
    /// A process of its own that nothing restarts if it dies: for a
    /// machine with neither (a container, CI).
    Detached,
}

/// `WEFT_SERVICE_MANAGER` (`systemd`, `launchd`, `detached`), or the one
/// this machine has.
async fn manager() -> Result<Manager> {
    match std::env::var("WEFT_SERVICE_MANAGER").ok().filter(|v| !v.trim().is_empty()).as_deref() {
        Some("systemd") => return Ok(Manager::Systemd),
        Some("launchd") => return Ok(Manager::Launchd),
        Some("detached") => return Ok(Manager::Detached),
        Some(other) => anyhow::bail!("WEFT_SERVICE_MANAGER is '{other}'; it takes systemd, launchd or detached"),
        None => {}
    }
    if cfg!(target_os = "macos") {
        return Ok(Manager::Launchd);
    }
    let user_systemd = Command::new("systemctl").args(["--user", "show-environment"]).output().await;
    match user_systemd {
        Ok(out) if out.status.success() => Ok(Manager::Systemd),
        _ => anyhow::bail!(
            "this machine has no systemd user session to keep weft's runtime running (on WSL, enable systemd in \
             /etc/wsl.conf). Set WEFT_SERVICE_MANAGER=detached to run it as a plain background process instead \
             (nothing restarts it if it stops)."
        ),
    }
}

/// The `weft-runtime` binary: `WEFT_RUNTIME_BIN`, or the one installed
/// next to this CLI.
fn runtime_binary() -> Result<PathBuf> {
    if let Some(bin) = std::env::var_os("WEFT_RUNTIME_BIN").filter(|v| !v.is_empty()) {
        return Ok(PathBuf::from(bin));
    }
    let here = std::env::current_exe()?;
    let bin = here.with_file_name("weft-runtime");
    anyhow::ensure!(bin.is_file(), "no weft-runtime next to {} (./setup.sh installs it; WEFT_RUNTIME_BIN names another)", here.display());
    Ok(bin)
}

fn systemd_unit_path(install: &Install) -> PathBuf {
    let home = std::env::var_os("HOME").map(PathBuf::from).unwrap_or_default();
    home.join(".config/systemd/user").join(format!("{}.service", install.service_name()))
}

fn launchd_label(install: &Install) -> String {
    format!("ai.weavemind.{}", install.service_name())
}

fn launchd_plist_path(install: &Install) -> PathBuf {
    let home = std::env::var_os("HOME").map(PathBuf::from).unwrap_or_default();
    home.join("Library/LaunchAgents").join(format!("{}.plist", launchd_label(install)))
}

fn pid_path(install: &Install) -> PathBuf {
    install.dir.join("runtime.pid")
}

/// The shell line every manager runs: the secrets into the environment,
/// then the runtime, which keeps its log bounded itself (`--log`); what
/// escapes it (a panic's last words) is appended to the same file.
fn runtime_command(install: &Install, bin: &Path) -> String {
    format!(
        "set -a; . '{}'; set +a; exec '{}' serve --config '{}' --log '{}' >> '{}' 2>&1",
        install.secrets_path().display(),
        bin.display(),
        install.config_path().display(),
        install.log_path().display(),
        install.log_path().display()
    )
}

async fn run_checked(cmd: &mut Command, what: &str) -> Result<()> {
    let out = cmd.output().await.with_context(|| format!("run {what}"))?;
    anyhow::ensure!(out.status.success(), "{what} failed: {}", String::from_utf8_lossy(&out.stderr).trim());
    Ok(())
}

async fn start_runtime(install: &Install) -> Result<()> {
    let bin = runtime_binary()?;
    let line = runtime_command(install, &bin);
    match manager().await? {
        Manager::Systemd => {
            let unit = systemd_unit_path(install);
            std::fs::create_dir_all(unit.parent().expect("a unit path has a directory"))?;
            std::fs::write(
                &unit,
                format!(
                    "[Unit]\nDescription=weft runtime ({})\nAfter=network-online.target\n\n[Service]\nExecStart=/bin/sh -c \"{}\"\nRestart=always\nRestartSec=2\n\n[Install]\nWantedBy=default.target\n",
                    install.id.label_value(),
                    line.replace('"', "\\\"")
                ),
            )?;
            run_checked(Command::new("systemctl").args(["--user", "daemon-reload"]), "systemctl --user daemon-reload").await?;
            let name = format!("{}.service", install.service_name());
            run_checked(Command::new("systemctl").args(["--user", "enable", &name]), "systemctl --user enable").await?;
            run_checked(Command::new("systemctl").args(["--user", "restart", &name]), "systemctl --user restart").await
        }
        Manager::Launchd => {
            let plist = launchd_plist_path(install);
            std::fs::create_dir_all(plist.parent().expect("a plist path has a directory"))?;
            let label = launchd_label(install);
            let escaped = line.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;");
            std::fs::write(
                &plist,
                format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict>\n<key>Label</key><string>{label}</string>\n<key>ProgramArguments</key><array><string>/bin/sh</string><string>-c</string><string>{escaped}</string></array>\n<key>RunAtLoad</key><true/>\n<key>KeepAlive</key><true/>\n</dict></plist>\n"
                ),
            )?;
            let domain = format!("gui/{}", current_uid().await?);
            let _ = Command::new("launchctl").args(["bootout", &format!("{domain}/{label}")]).output().await;
            run_checked(Command::new("launchctl").args(["bootstrap", &domain, &plist.display().to_string()]), "launchctl bootstrap").await
        }
        Manager::Detached => {
            stop_detached(install).await?;
            let child = std::process::Command::new("/bin/sh")
                .args(["-c", &line])
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .context("start weft-runtime")?;
            std::fs::write(pid_path(install), child.id().to_string())?;
            Ok(())
        }
    }
}

async fn current_uid() -> Result<String> {
    let out = Command::new("id").arg("-u").output().await?;
    anyhow::ensure!(out.status.success(), "id -u failed");
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// Stop the runtime a detached start left, named by its pid file. The pid
/// is only signalled while it still runs this install's runtime: a pid
/// file outlives a reboot or a crash, and by then the number may belong to
/// any other process.
async fn stop_detached(install: &Install) -> Result<()> {
    let path = pid_path(install);
    let Ok(pid) = std::fs::read_to_string(&path) else { return Ok(()) };
    let pid = pid.trim().to_string();
    if runs_this_runtime(install, &pid).await? {
        run_checked(Command::new("kill").arg(&pid), &format!("kill weft's runtime (pid {pid})")).await?;
        // Gone before anything starts again on its ports. Bounded: a
        // runtime that ignores the signal this long is stuck, and saying
        // so beats waiting forever.
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        while runs_this_runtime(install, &pid).await? {
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "weft's runtime (pid {pid}) did not stop within a minute of being asked to; `kill -9 {pid}` ends it"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    std::fs::remove_file(&path).with_context(|| format!("remove {}", path.display()))?;
    Ok(())
}

/// Whether `pid` is this install's runtime: a live process whose command
/// line is the one [`runtime_command`] execs, on this install's config.
async fn runs_this_runtime(install: &Install, pid: &str) -> Result<bool> {
    let out = Command::new("ps").args(["-p", pid, "-o", "command="]).output().await.context("run ps")?;
    // `ps -p` exits non-zero when no such process runs.
    if !out.status.success() {
        return Ok(false);
    }
    let command = String::from_utf8_lossy(&out.stdout);
    Ok(is_runtime_command_line(install, command.trim()))
}

fn is_runtime_command_line(install: &Install, command: &str) -> bool {
    command.contains(&format!(" serve --config {} ", install.config_path().display()))
}

/// Stop the runtime, however this machine's service manager runs it, and
/// fail when it is still running afterwards.
async fn stop_runtime(install: &Install) -> Result<()> {
    match manager().await? {
        Manager::Systemd => {
            let name = format!("{}.service", install.service_name());
            let out = Command::new("systemctl").args(["--user", "stop", &name]).output().await.context("run systemctl")?;
            // A unit that was never loaded is already stopped; one that is
            // still active after a failed stop is not.
            if !out.status.success() {
                let active = Command::new("systemctl").args(["--user", "is-active", "--quiet", &name]).status().await?.success();
                anyhow::ensure!(!active, "systemctl --user stop {name} failed: {}", String::from_utf8_lossy(&out.stderr).trim());
            }
        }
        Manager::Launchd => {
            let target = format!("gui/{}/{}", current_uid().await?, launchd_label(install));
            let out = Command::new("launchctl").args(["bootout", &target]).output().await.context("run launchctl")?;
            if !out.status.success() {
                let loaded = Command::new("launchctl").args(["print", &target]).output().await?.status.success();
                anyhow::ensure!(!loaded, "launchctl bootout {target} failed: {}", String::from_utf8_lossy(&out.stderr).trim());
            }
        }
        Manager::Detached => stop_detached(install).await?,
    }
    Ok(())
}

/// Wait until the runtime answers, naming its log when it does not.
async fn wait_for_runtime(install: &Install, public: u16) -> Result<()> {
    let client = reqwest::Client::builder().timeout(Duration::from_secs(3)).build()?;
    let deadline = std::time::Instant::now() + Duration::from_secs(180);
    loop {
        if client.get(format!("http://127.0.0.1:{public}/install")).send().await.is_ok_and(|r| r.status().is_success()) {
            return Ok(());
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "weft's runtime did not answer on 127.0.0.1:{public}; its log: {}",
            install.log_path().display()
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

// ----- an older weft on this machine ------------------------------------

/// Refuse to start beside the Kubernetes install an older weft ran here:
/// its database cannot be carried forward, and its cluster would keep
/// running for nothing. Nothing to do on a machine that never had one.
/// The cluster is looked for by its node container, so a machine whose
/// `kind` binary is gone is still caught.
// SYNC: <-> scripts/lib/weft-cleanup.sh (weft_old_install_present)
async fn refuse_an_older_install(install: &Install) -> Result<()> {
    if install.id.name().is_some() {
        return Ok(());
    }
    let node = format!("{OLD_KIND_CLUSTER}-control-plane");
    let out = Command::new("docker")
        .args(["ps", "-aq", "--filter", &format!("name=^{node}$")])
        .output()
        .await
        .context("ask docker for an older weft's Kubernetes node")?;
    anyhow::ensure!(out.status.success(), "docker ps failed: {}", String::from_utf8_lossy(&out.stderr).trim());
    anyhow::ensure!(
        String::from_utf8_lossy(&out.stdout).trim().is_empty(),
        "an older weft's Kubernetes cluster '{OLD_KIND_CLUSTER}' is still on this machine, and its \
         database cannot be carried forward. Run ./setup.sh, which offers to wipe it (this \
         deletes its projects' run history and stored connections; your project folders are \
         untouched), or wipe it yourself with scripts/scrub-old-install.sh."
    );
    Ok(())
}

// ----- the verbs ---------------------------------------------------------

async fn start(install: &Install, rebuild: bool) -> Result<()> {
    let repo_root = weft_compiler::build::resolve_weft_root().map_err(|e| anyhow::anyhow!("resolve weft repo root: {e}"))?;
    std::fs::create_dir_all(&install.dir)?;
    refuse_an_older_install(install).await?;
    let ports = ports(install)?;

    let shared = images::ensure_all_shared_images(rebuild, None).await?;
    images::hold_standard_worker(&install.id, &shared.worker).await?;
    ensure_network().await?;
    ensure_postgres(install, ports.postgres).await?;
    ensure_object_store(object_store_port()?).await?;
    let tunnel = reconcile_tunnel(install, ports).await?;

    let previous = match install.config_path().exists() {
        true => Levers::read(&install.config_path())?,
        false => Levers::default(),
    };
    let config = install_config(install, ports, shared.runtime, shared.builder_base, tunnel.as_ref().map(|(u, _)| u.clone()), previous)?;
    std::fs::write(install.config_path(), serde_json::to_vec_pretty(&config)?)?;
    let env = secrets(install, ports, &repo_root)?;
    let body: String = env.iter().map(|(k, v)| format!("{k}={v}\n")).collect();
    write_private(&install.secrets_path(), &body)?;

    // The runtime that ran before holds these ports; once it is stopped,
    // anything still listening there is another program, and the new
    // runtime would never come up.
    stop_runtime(install).await?;
    let listen = listen_of(&config);
    let mut uses = vec![
        PortUse::of(install, listen.public, "its API and dashboard", "WEFT_PUBLIC_PORT"),
        PortUse::of(install, listen.internal, "the calls its own containers make to it", "WEFT_INTERNAL_PORT"),
    ];
    if let Some(outside) = listen.outside {
        uses.push(PortUse::of(install, outside, "the doors outside callers use (the tunnel's target)", "WEFT_OUTSIDE_PORT"));
    }
    refuse_taken_ports(&uses).await?;
    start_runtime(install).await?;
    wait_for_runtime(install, ports.public).await?;
    let local = crate::client::DispatcherClient::new(format!("http://127.0.0.1:{}", ports.public), None);
    super::catalog::preload_standard_library(&local)
        .await
        .context("store the standard library in this install's assets")?;
    if let Some((url, named)) = tunnel {
        announce_tunnel(&url, named, &install.tunnel_container(), ports).await?;
    }
    println!("weft is running at http://127.0.0.1:{} (install '{}')", ports.public, install.id.label_value());
    Ok(())
}

/// The containers the runtime started for an install: its workers, and
/// with `with_infra` its infra units too.
async fn install_containers(install: &Install, with_infra: bool) -> Result<Vec<String>> {
    let label = format!("label={}={}", weft_core::infra::INSTALL_LABEL, install.id.label_value());
    let listing = docker_ok(&["ps", "-a", "--filter", &label, "--format", "{{.Names}}\t{{.Label \"weft.role\"}}"], "docker ps").await?;
    Ok(listing
        .lines()
        .filter_map(|l| l.split_once('\t'))
        .filter(|(_, role)| matches!(*role, "worker" | "long") || (with_infra && *role == "infra"))
        .map(|(name, _)| name.to_string())
        .collect())
}

async fn stop(install: &Install) -> Result<()> {
    stop_runtime(install).await?;
    // Workers serve only the runtime; infra keeps running, as it would
    // across a runtime restart.
    for name in install_containers(install, false).await? {
        remove_container(&name).await?;
    }
    println!("weft's runtime stopped (install '{}'); the database and infra keep running", install.id.label_value());
    Ok(())
}

async fn remove(install: &Install) -> Result<()> {
    let Some(name) = install.id.name() else {
        anyhow::bail!(
            "`weft daemon remove` takes a named install ({INSTALL_ENV}) off this machine, and none is set, which means the \
             default install. That one holds every project on this machine; `./setup.sh --uninstall` is how it goes."
        );
    };
    stop_runtime(install).await?;
    for path in [systemd_unit_path(install), launchd_plist_path(install)] {
        let _ = std::fs::remove_file(path);
    }
    for c in install_containers(install, true).await? {
        remove_container(&c).await?;
    }
    let label = format!("label={}={}", weft_core::infra::INSTALL_LABEL, install.id.label_value());
    let volumes = docker_ok(&["volume", "ls", "-q", "--filter", &label], "docker volume ls").await?;
    for v in volumes.lines().filter(|l| !l.trim().is_empty()) {
        docker_ok(&["volume", "rm", "-f", v.trim()], "docker volume rm").await?;
    }
    remove_container(&install.postgres_container()).await?;
    delete_bucket(&install.bucket()).await?;
    // Postgres owned its files as its own user.
    if install.postgres_dir().exists() {
        let out = run_in_alpine(&install.dir, "/d", "rm -rf /d/postgres-data").await?;
        anyhow::ensure!(out.status.success(), "removing the database files failed: {}", String::from_utf8_lossy(&out.stderr));
    }
    std::fs::remove_dir_all(&install.dir).with_context(|| format!("remove {}", install.dir.display()))?;
    println!("install '{name}' removed");
    Ok(())
}

/// Reports on the install this machine's `WEFT_INSTALL` names (the one
/// its label, log path and public address describe), so its address comes
/// from that install's own ports, never from `--on`/`--dispatcher`, which
/// are refused rather than ignored. `WEFT_DISPATCHER_URL` is ambient
/// (setup.sh exports it) and is not consulted.
async fn status(ctx: &Ctx, install: &Install) -> Result<()> {
    ctx.refuse_other_install("weft daemon status")?;
    // A named install that never started has no address at all: saying
    // so is this command's answer, not an error. `local_public_url` reads
    // the same `WEFT_INSTALL` that built `install`.
    match weft_core::ports::local_public_url() {
        Err(e) => println!("weft: no address to reach: {e}"),
        Ok(url) => {
            // An unreadable credentials file is its own error, not "no address".
            let key = crate::credentials::operator_key_for(&url, None)?;
            report_reachable(&crate::client::DispatcherClient::new(url, key), install).await;
        }
    }
    if install.id.name().is_some() {
        println!("public address: none (a named install has none)");
        return Ok(());
    }
    match current_public_url() {
        Some(url) => println!("public address: {url} (./setup.sh --no-public-url closes it)"),
        None => println!("public address: closed"),
    }
    Ok(())
}

/// The `weft: running ...` / `weft: unreachable ...` line of
/// [`status`]; scripts/run-node-tests.sh greps `weft: running`.
async fn report_reachable(client: &crate::client::DispatcherClient, install: &Install) {
    match client.get_json("/projects").await {
        Ok(v) => println!(
            "weft: running at {} (install '{}'); {} project(s)",
            client.base(),
            install.id.label_value(),
            v.as_array().map(|a| a.len()).unwrap_or(0)
        ),
        Err(e) => println!("weft: unreachable at {}: {e} (its log: {})", client.base(), install.log_path().display()),
    }
}

async fn logs(install: &Install, tail: usize, follow: bool) -> Result<()> {
    let log = install.log_path();
    anyhow::ensure!(log.exists(), "no runtime log yet at {} (has `weft daemon start` run?)", log.display());
    let mut cmd = Command::new("tail");
    cmd.args(["-n", &tail.to_string()]);
    if follow {
        cmd.arg("-f");
    }
    let status = cmd.arg(&log).status().await?;
    anyhow::ensure!(status.success(), "tail {} exited {status}", log.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Docker's refusal for a container whose network was made again is
    /// told apart from every other start failure.
    #[test]
    fn a_vanished_network_is_told_apart() {
        assert!(stale_network(
            "Error response from daemon: failed to set up container networking: network cfc1be309d69 not found"
        ));
        assert!(!stale_network("Error response from daemon: driver failed programming external connectivity: port is already allocated"));
    }

    fn install(name: Option<&str>) -> Install {
        let id = match name {
            Some(n) => weft_core::infra::Install::named(n).unwrap(),
            None => weft_core::infra::Install::default_install(),
        };
        Install { id, dir: "/home/u/.local/share/weft".into() }
    }

    #[test]
    fn a_port_in_a_windows_excluded_range_is_found() {
        let out = "\r\nProtocol tcp Port Exclusion Ranges\r\n\r\nStart Port    End Port\r\n----------    --------\r\n      5357        5357\r\n     14080       14179\r\n     50000       50059     *\r\n\r\n* - Administered port exclusions.\r\n";
        assert!(excluded_by_netsh(out, 14111));
        assert!(excluded_by_netsh(out, 50059));
        assert!(!excluded_by_netsh(out, 14180));
        assert!(!excluded_by_netsh("", 14111));
    }

    #[test]
    fn the_config_a_start_writes_is_valid_and_keeps_the_management_api_off_the_tunnel() {
        let ports = Ports { public: 14111, internal: 14113, outside: 14112, postgres: 14114 };
        let closed = install_config(&install(None), ports, "weft-runtime:x".into(), "b".into(), None, Levers::default()).unwrap();
        assert_eq!(listen_of(&closed).outside, Some(([127, 0, 0, 1], 14112).into()), "the outside port is served without a tunnel too");
        assert_eq!(closed.public_url, "http://127.0.0.1:14111");
        let ObjectStoreSettings::S3(store) = &closed.object_store else { panic!("a local install's store is S3-compatible") };
        assert_eq!(store.worker_endpoint.as_deref(), Some("http://weft-object-store:8333"));
        let open = install_config(&install(None), ports, "weft-runtime:x".into(), "b".into(), Some("https://a.trycloudflare.com".into()), Levers::default()).unwrap();
        assert_eq!(listen_of(&open).outside, Some(([127, 0, 0, 1], 14112).into()));
        assert_eq!(listen_of(&open).public.ip(), std::net::IpAddr::from([127, 0, 0, 1]), "the management API stays on loopback");
        let v = serde_json::to_value(&open).unwrap();
        assert_eq!(serde_json::from_value::<InstallConfig>(v).unwrap(), open);

        // A lever a person set in the file survives the next start, read
        // from the file even when the rest of it is a shape this weft no
        // longer writes (an object store from before it named its kind).
        let mut edited = serde_json::to_value(&closed).unwrap();
        edited["workers"]["min_instances"] = serde_json::json!(1);
        assert_eq!(closed.edge.trusted_proxy_hops, ProxyHops { public: 0, outside: 1, domains: 0 }, "cloudflared is one hop in front of the outside port");
        edited["edge"]["trustedProxyHops"]["public"] = serde_json::json!(2);
        edited["objectStore"] = serde_json::json!({ "endpoint": "http://127.0.0.1:8333", "bucket": "weft" });
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("config.json");
        std::fs::write(&file, serde_json::to_vec(&edited).unwrap()).unwrap();
        let again = install_config(&install(None), ports, "weft-runtime:y".into(), "b".into(), None, Levers::read(&file).unwrap()).unwrap();
        assert_eq!(again.workers.min_instances, 1);
        assert_eq!(again.edge.trusted_proxy_hops, ProxyHops { public: 2, outside: 1, domains: 0 });
        assert_eq!(again.platform, PlatformConfig::Local(LocalPlatform { runtime_image: "weft-runtime:y".into(), ..match closed.platform { PlatformConfig::Local(l) => l, _ => unreachable!() } }));
    }

    /// A pid is this install's runtime only when its command line is the
    /// one the start execs on this install's config.
    #[test]
    fn a_port_variable_moves_only_its_own_port() {
        let kept = moved_by_env(Ports::DEFAULT, |_| None).unwrap();
        assert_eq!(kept, Ports::DEFAULT);
        let moved = moved_by_env(Ports::DEFAULT, |n| (n == "WEFT_PUBLIC_PORT").then(|| " 15000 ".to_string())).unwrap();
        assert_eq!(moved, Ports { public: 15000, ..Ports::DEFAULT });
        assert!(moved_by_env(Ports::DEFAULT, |n| (n == "WEFT_POSTGRES_PORT").then(|| "x".to_string())).is_err());
    }

    #[test]
    fn a_pid_is_the_runtime_only_on_its_own_command_line() {
        let d = install(None);
        let line = format!("/opt/weft/bin/weft-runtime serve --config {} --log {}", d.config_path().display(), d.log_path().display());
        assert!(is_runtime_command_line(&d, &line));
        assert!(!is_runtime_command_line(&d, "/usr/bin/vim notes.txt"));
        let other = Install { id: d.id.clone(), dir: "/elsewhere".into() };
        assert!(!is_runtime_command_line(&other, &line), "another install's runtime");
    }

    #[test]
    fn a_named_install_names_its_things_apart() {
        let d = install(None);
        let c = install(Some("cell3"));
        assert_eq!(d.postgres_container(), "weft-postgres");
        assert_eq!(c.postgres_container(), "weft-cell3-postgres");
        assert_eq!(c.bucket(), "weft-cell3");
        assert_ne!(d.service_name(), c.service_name());
    }

    #[test]
    fn the_runtime_line_loads_its_secrets_then_runs() {
        let line = runtime_command(&install(None), Path::new("/bin/weft-runtime"));
        assert!(line.starts_with("set -a; . '/home/u/.local/share/weft/secrets.env'; set +a; exec '/bin/weft-runtime' serve --config"));
        assert!(line.ends_with(">> '/home/u/.local/share/weft/runtime.log' 2>&1"));
    }

    #[test]
    fn the_postgres_major_is_read_off_the_image() {
        assert_eq!(postgres_major(), "18");
    }

    #[test]
    fn a_named_tunnel_hostname_is_the_bare_https_host() {
        assert_eq!(canonical_tunnel_hostname("https://weft.example.com").unwrap(), "https://weft.example.com");
        assert!(canonical_tunnel_hostname("http://weft.example.com").is_err());
        assert!(canonical_tunnel_hostname("https://weft.example.com/x").is_err());
        assert!(canonical_tunnel_hostname("https://weft.example.com:8443").is_err());
    }
}
