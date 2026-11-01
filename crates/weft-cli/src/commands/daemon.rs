//! `weft daemon start|stop|status|logs|remove|which`: a local install.
//!
//! A machine holds the default install and any number of named ones
//! beside it, each with names, ports and files of its own: one per weft
//! branch a person works on, and the e2e runner's test cells. Which one a
//! command acts on is settled before it gets here (`weft daemon which`
//! says): the CLI's front door sets `WEFT_INSTALL` for the whole process.
//!
//! A local install is plain processes and Docker. Three containers hold
//! state (Postgres, the object store, and when the public address is open
//! the tunnel), and one process runs every role weft has: `weft-runtime`,
//! kept alive by the machine's service manager (a systemd user unit on
//! Linux, a launchd agent on macOS). The runtime starts the projects'
//! workers itself, as processes compiled on the machine, and their infra
//! units as containers.
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
use tokio::process::Command;
// The object store's key pair goes by these in its `keys.env` and in every
// install's `secrets.env`: the names the runtime reads it by.
use weft_platform_traits::object_store::{OBJECT_STORE_ACCESS_KEY_ENV, OBJECT_STORE_SECRET_KEY_ENV};
use weft_platform_traits::config::{
    EdgeConfig, InstallConfig, Listen, LocalPlatform, ObjectStoreSettings, PlatformConfig, S3StoreSettings, ProxyHops,
};

use super::Ctx;
use crate::images;

use weft_core::infra::{Wanted, WantedFrom};
use weft_core::ports;

/// The Postgres every local install runs.
// SYNC: postgres image <-> scripts/lib/throwaway-postgres.sh (the scratch
//       image, 18), setup.sh (--purge --postgres, which reclaims it)
const POSTGRES_IMAGE: &str = "postgres:18-alpine";

/// The local database account.
const PG_USER: &str = "weft";
const PG_PASSWORD: &str = "weft-local-dev";
const PG_DB: &str = "weft";

/// The object store every local install on this machine shares.
const OBJECT_STORE_CONTAINER: &str = "weft-object-store";
// SYNC: OBJECT_STORE_IMAGE <-> setup.sh (the purge's `remove_docker_object image chrislusf/seaweedfs:...`)
const OBJECT_STORE_IMAGE: &str = "chrislusf/seaweedfs:4.51";


/// The tunnel that brings the open internet to the outside doors.
const TUNNEL_IMAGE: &str = "cloudflare/cloudflared:2026.10.5";

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
    /// Say which install this command acts on, touching nothing.
    Which,
}

/// `wanted` is the install the command acts on, as the front door chose
/// it (`main.rs` `prelude`).
pub async fn run(ctx: Ctx, action: DaemonAction, wanted: Wanted) -> Result<()> {
    let install = Install::of(wanted.install.clone());
    match action {
        DaemonAction::Start { rebuild, public_url } => {
            if let Some(choice) = public_url {
                set_public_url_choice(&install, choice)?;
            }
            start(&install, rebuild).await
        }
        DaemonAction::Stop => stop(&install).await,
        DaemonAction::Remove => remove(&install).await,
        DaemonAction::Status => status(&ctx, &install).await,
        DaemonAction::Logs { tail, follow } => logs(&install, tail, follow).await,
        DaemonAction::Which => which(&ctx, &wanted),
    }
}

/// The root of weft's files on this machine.
pub fn data_dir() -> PathBuf {
    weft_core::infra::data_dir()
}

/// One install on this machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Install {
    /// Which install: the default one, or a named one.
    pub id: weft_core::infra::Install,
    /// Its files: `config.json`, `secrets.env`, the runtime's log, its
    /// database files, its binaries (`bin/`).
    pub dir: PathBuf,
}

/// What every start writes into an install's dir: the default install
/// is on the machine when its dir holds this or its image record.
const CONFIG_FILE: &str = "config.json";

impl Install {
    pub fn of(id: weft_core::infra::Install) -> Self {
        let dir = id.dir();
        Self { id, dir }
    }

    /// Every install on the machine whose weft files are under
    /// `data_dir` (`data_dir()` outside tests): the default one when
    /// `data_dir` holds its config or its image record, then every folder
    /// of `installs/` named like an install, in name order. A named
    /// install counts from the moment its folder exists, before its first
    /// start writes anything there: a sweep that missed it then would
    /// drop what that start is about to use.
    pub fn every_on(data_dir: &Path) -> Result<Vec<Self>> {
        let mut found = Vec::new();
        if [CONFIG_FILE, weft_core::images::IMAGE_REFS_FILE].iter().any(|f| data_dir.join(f).is_file()) {
            found.push(Self { id: weft_core::infra::Install::default_install(), dir: data_dir.to_path_buf() });
        }
        let named = data_dir.join("installs");
        let entries = match std::fs::read_dir(&named) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(found),
            Err(e) => return Err(e).with_context(|| format!("list {}", named.display())),
        };
        let mut named_installs = Vec::new();
        for entry in entries {
            let dir = entry.with_context(|| format!("list {}", named.display()))?.path();
            let Some(id) = dir.file_name().and_then(|n| n.to_str()).and_then(|n| weft_core::infra::Install::named(n).ok()) else {
                continue;
            };
            if dir.is_dir() {
                named_installs.push(Self { id, dir });
            }
        }
        named_installs.sort_by(|a, b| a.id.name().cmp(&b.id.name()));
        found.extend(named_installs);
        Ok(found)
    }

    fn config_path(&self) -> PathBuf {
        self.dir.join(CONFIG_FILE)
    }

    fn secrets_path(&self) -> PathBuf {
        weft_core::infra::secrets_file(&self.dir)
    }

    fn log_path(&self) -> PathBuf {
        self.dir.join("runtime.log")
    }

    fn postgres_dir(&self) -> PathBuf {
        self.dir.join("postgres-data")
    }

    /// Where the install's Postgres puts its socket, shared with the host
    /// (`ensure_postgres`). Inside the database's own directory: Postgres
    /// owns it as its own user, and it goes wherever the database's files
    /// go (`remove_install`, `./setup.sh --purge`).
    fn postgres_socket_dir(&self) -> PathBuf {
        self.postgres_dir().join("socket")
    }

    fn prefix(&self) -> String {
        self.id.resource_prefix()
    }

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
                "weft needs port {port} for {}, and Windows has reserved it: it falls in a range Windows keeps off limits (`netsh int ipv4 show excludedportrange protocol=tcp` lists them). Either, in an administrator terminal, reset Windows' dynamic range with `netsh int ipv4 set dynamic tcp start=49152 num=16384` and reboot; or reserve weft's ports for weft with `net stop winnat`, then `netsh int ipv4 add excludedportrange protocol=tcp startport={} numberofports={}`, then `net start winnat`. Or give weft another port: {}",
                u.what,
                ports::BLOCK.start(),
                ports::BLOCK.len(),
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
/// free ports for a named one ([`ports::PortsTurn::free_ports`]: it sits
/// beside the default install, so the block is taken), picked and saved
/// while this start holds the machine's turn to pick ports. On the default
/// install a `WEFT_*_PORT` set on this start moves that port for good; a
/// named install ignores them, because a variable exported for the
/// default install would otherwise land every test cell on the same port.
/// A named install's ports move by editing its file. A file an older weft
/// wrote gets its quick tunnel's metrics port here, and only here
/// ([`ports::SavedPorts::to_start_on`]): every other command reads the
/// file as it is.
fn ports(install: &Install) -> Result<Ports> {
    // Held until the ports are saved: another start picking at the same
    // moment reads them there.
    let turn = ports::PortsTurn::take().map_err(anyhow::Error::msg)?;
    let saved = ports::SavedPorts::read(&install.dir).map_err(anyhow::Error::msg)?;
    let seeded = match (saved, install.id.name()) {
        (Some(saved), _) => saved.to_start_on(&install.id, &turn).map_err(anyhow::Error::msg)?,
        (None, None) => Ports::DEFAULT,
        (None, Some(_)) => {
            let [public, internal, outside, postgres, tunnel_metrics] = turn.free_ports(&[]).map_err(anyhow::Error::msg)?;
            Ports { public, internal, outside, postgres, tunnel_metrics }
        }
    };
    let chosen = match install.id.name() {
        Some(_) => seeded,
        None => moved_by_env(seeded, |name| std::env::var(name).ok())?,
    };
    if !saved.is_some_and(|saved| saved.says(&chosen)) {
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
        tunnel_metrics: port("WEFT_TUNNEL_METRICS_PORT", ports.tunnel_metrics)?,
    })
}

fn object_store_port() -> Result<u16> {
    env_port("WEFT_SEAWEED_PORT", ports::OBJECT_STORE)
}

// ----- the public address ---------------------------------------------

/// Present while the install's public address is open: the choice a
/// `--public-url` made, kept until `--no-public-url`.
// SYNC: public-url-enabled <-> setup.sh (reopens the address on reinstall)
fn public_url_marker(install: &Install) -> PathBuf {
    install.dir.join("public-url-enabled")
}

/// The address the open tunnel answers at, once proven to reach weft.
fn public_url_file(install: &Install) -> PathBuf {
    install.dir.join("public-url")
}

/// The install's record of its named tunnel ([`tunnel_record`]), which
/// is also the env file its tunnel container reads. A start writes it
/// when it opens a named tunnel, and a later start with neither
/// variable set opens that tunnel again from it; closing the address
/// (`--no-public-url`) removes it. A tunnel that went away otherwise (its
/// container removed by hand, its install's folder left behind) leaves
/// it, so [`refuse_a_shared_tunnel`] trusts it only beside a tunnel
/// container.
// SYNC: tunnel.env <-> scripts/scrub-old-install.sh (keep: kept across a scrub)
fn tunnel_env_file(install: &Install) -> PathBuf {
    install.dir.join("tunnel.env")
}

/// The public address `install` answers at, or `None` when it is closed.
fn current_public_url(install: &Install) -> Option<String> {
    if !public_url_marker(install).exists() {
        return None;
    }
    std::fs::read_to_string(public_url_file(install)).ok().map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
}

fn set_public_url_choice(install: &Install, open: bool) -> Result<()> {
    if open {
        std::fs::create_dir_all(&install.dir)?;
        std::fs::write(public_url_marker(install), b"")?;
        return Ok(());
    }
    for path in [public_url_marker(install), public_url_file(install)] {
        remove_if_present(&path)?;
    }
    Ok(())
}

/// Remove the file at `path`; one already gone is fine.
fn remove_if_present(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("remove {}", path.display())),
    }
}

/// The variable `tunnel.env` hands cloudflared its token in.
const TUNNEL_TOKEN_KEY: &str = "TUNNEL_TOKEN";

/// The variable `tunnel.env` keeps the tunnel's hostname in, for the
/// next start. The container gets it too and cloudflared ignores it: it
/// reads only its own variables (`TUNNEL_HOSTNAME` would be its
/// `--hostname`, so the key is weft's own).
const RECORDED_HOSTNAME_KEY: &str = "WEFT_PUBLIC_TUNNEL_HOSTNAME";

/// The line of a tunnel's env file carrying its token.
fn tunnel_env_line(token: &str) -> String {
    format!("{TUNNEL_TOKEN_KEY}={token}\n")
}

/// The whole of `tunnel.env` for `tunnel`: its token, then its hostname.
fn tunnel_record(tunnel: &NamedTunnel) -> String {
    format!("{}{RECORDED_HOSTNAME_KEY}={}\n", tunnel_env_line(&tunnel.token), tunnel.hostname)
}

/// Refuse to start `install`'s named tunnel on `token` while another
/// install on this machine runs a tunnel on the same one: two connectors
/// on one tunnel share its traffic, so callers of the address would land
/// on either install at random. Another install runs one when its
/// `tunnel.env` holds the token AND its tunnel container exists; a file
/// whose container is gone (a crash, a container removed by hand) is
/// left from a tunnel that no longer runs, so it is named in a warning
/// and blocks nothing. The caller holds the machine's tunnel lock
/// ([`reconcile_tunnel`]) from this check until its own container runs.
async fn refuse_a_shared_tunnel(install: &Install, token: &str) -> Result<()> {
    let mut others = Vec::new();
    for other in Install::every_on(&data_dir())? {
        if other.dir == install.dir {
            continue;
        }
        let Some(record) = read_tunnel_record(&tunnel_env_file(&other))? else { continue };
        let container = container_state(&other.tunnel_container()).await?.is_some();
        others.push(OtherTunnel { install: other, token: record.token, container });
    }
    let holders = tunnel_token_holders(token, &others);
    for stale in holders.stale {
        eprintln!(
            "warning: {} holds the same WEFT_PUBLIC_TUNNEL_TOKEN, but install '{}' has no tunnel container ({}), so \
             nothing runs on it; ignoring the file. That install's next start with neither variable set opens the \
             tunnel the file records, and is refused while this one runs it: give it a tunnel of its own, or close its address with \
             ./setup.sh --no-public-url in its checkout",
            tunnel_env_file(stale).display(),
            stale.id.label_value(),
            stale.tunnel_container()
        );
    }
    match holders.running {
        None => Ok(()),
        Some(holder) => anyhow::bail!(
            "install '{}' cannot open its named tunnel: install '{}' already runs a tunnel on the same \
             WEFT_PUBLIC_TUNNEL_TOKEN (container {}, token in {}), and two installs on one tunnel would each get a \
             random share of its callers. Give this checkout a tunnel of its own (its own WEFT_PUBLIC_TUNNEL_TOKEN and \
             WEFT_PUBLIC_TUNNEL_HOSTNAME in its .env.local, then ./setup.sh --public-url), or for a quick tunnel set \
             neither there and run ./setup.sh --no-public-url then ./setup.sh --public-url (until --no-public-url, \
             this install opens the tunnel it last ran again, from {}); the token may also come from the shared \
             .env, where the two lines do not belong",
            install.id.label_value(),
            holder.id.label_value(),
            holder.tunnel_container(),
            tunnel_env_file(holder).display(),
            tunnel_env_file(install).display()
        ),
    }
}

/// Another install's tunnel as [`refuse_a_shared_tunnel`] finds it: the
/// token its `tunnel.env` records, and whether its tunnel container
/// exists.
struct OtherTunnel {
    install: Install,
    token: Option<String>,
    container: bool,
}

/// The installs among `others` whose `tunnel.env` holds `token`: the one
/// whose tunnel container exists, and those whose container is gone.
#[derive(Debug, PartialEq, Eq)]
struct TokenHolders<'a> {
    running: Option<&'a Install>,
    stale: Vec<&'a Install>,
}

fn tunnel_token_holders<'a>(token: &str, others: &'a [OtherTunnel]) -> TokenHolders<'a> {
    let mut holders = TokenHolders { running: None, stale: Vec::new() };
    for other in others.iter().filter(|o| o.token.as_deref() == Some(token)) {
        match other.container {
            true => holders.running = holders.running.or(Some(&other.install)),
            false => holders.stale.push(&other.install),
        }
    }
    holders
}

/// The operator's named tunnel (a stable hostname): its Cloudflare token
/// and the https hostname it routes.
#[derive(Debug, Clone, PartialEq, Eq)]
struct NamedTunnel {
    token: String,
    hostname: String,
}

/// A named tunnel's two settings as one source holds them, an empty
/// value counting as unset.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct TunnelSettings {
    token: Option<String>,
    hostname: Option<String>,
}

impl TunnelSettings {
    /// What the environment of this command sets (the CLI loaded the
    /// checkout's `.env.local` and `.env` into it, when it runs in one).
    fn from_env() -> Self {
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
        Self { token: var("WEFT_PUBLIC_TUNNEL_TOKEN"), hostname: var("WEFT_PUBLIC_TUNNEL_HOSTNAME") }
    }
}

/// What the install recorded at `path` (`tunnel.env`), or `None` when it
/// recorded no named tunnel.
fn read_tunnel_record(path: &Path) -> Result<Option<TunnelSettings>> {
    let body = match std::fs::read_to_string(path) {
        Ok(body) => body,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
    };
    Ok(Some(tunnel_record_of(&body)))
}

/// The settings a `tunnel.env`'s text records: the one reading of the
/// file, for its own install's start and for another's shared-token check.
fn tunnel_record_of(body: &str) -> TunnelSettings {
    let mut values = weft_core::infra::env_file_values(body);
    let mut take = |key: &str| values.remove(key).filter(|v| !v.is_empty());
    TunnelSettings { token: take(TUNNEL_TOKEN_KEY), hostname: take(RECORDED_HOSTNAME_KEY) }
}

/// The named tunnel a start opens: the one the environment sets (both
/// variables, or neither), else the one the install recorded at `record`
/// when it last opened one, else none (a quick tunnel). A record missing
/// either setting (one an older weft wrote kept only the token) is
/// refused, naming the file: no hostname is guessed.
fn named_tunnel(env: TunnelSettings, recorded: Option<TunnelSettings>, record: &Path) -> Result<Option<NamedTunnel>> {
    match (env.token, env.hostname) {
        (Some(token), Some(hostname)) => return Ok(Some(NamedTunnel { token, hostname: canonical_tunnel_hostname(&hostname)? })),
        (None, None) => {}
        (Some(_), None) => anyhow::bail!(
            "WEFT_PUBLIC_TUNNEL_TOKEN is set but WEFT_PUBLIC_TUNNEL_HOSTNAME is not; set both \
             (the hostname the tunnel's Cloudflare config routes to weft)"
        ),
        (None, Some(_)) => anyhow::bail!(
            "WEFT_PUBLIC_TUNNEL_HOSTNAME is set but WEFT_PUBLIC_TUNNEL_TOKEN is not; set both \
             (the token from the tunnel's Cloudflare dashboard page)"
        ),
    }
    let Some(recorded) = recorded else { return Ok(None) };
    let fix = "run ./setup.sh --public-url from the checkout with WEFT_PUBLIC_TUNNEL_TOKEN and WEFT_PUBLIC_TUNNEL_HOSTNAME \
               in its .env.local, or set both for this command; for a quick tunnel instead, ./setup.sh --no-public-url \
               then ./setup.sh --public-url";
    match (recorded.token, recorded.hostname) {
        (Some(token), Some(hostname)) => {
            let hostname = canonical_tunnel_hostname(&hostname)
                .with_context(|| format!("the hostname of this install's named tunnel, recorded in {}; {fix}", record.display()))?;
            Ok(Some(NamedTunnel { token, hostname }))
        }
        (Some(_), None) => anyhow::bail!(
            "{} records this install's named tunnel without its hostname (a weft from before it kept the hostname wrote it); {fix}",
            record.display()
        ),
        (None, _) => anyhow::bail!("{} records this install's named tunnel without its {TUNNEL_TOKEN_KEY}; {fix}", record.display()),
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

/// Run a container from `args` unless one of that name already runs from
/// exactly these arguments. A container cannot change its ports, mounts
/// or image in place, so when the arguments move it is made again (its
/// named volumes and bind-mounted directories are untouched).
async fn ensure_container(name: &str, args: &[String], stamp_dir: &Path, published: Option<PortUse>) -> Result<()> {
    let stamp = stamp_dir.join(format!("{name}.run.sha256"));
    let want = weft_core::project::hash::sha256_hex(args.join("\u{1f}").as_bytes());
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

/// What the install's Postgres is started with, sized for the record's
/// batches: room for the write-ahead log between checkpoints (the defaults
/// asked for a checkpoint every couple of seconds under load), the log
/// compressed, and a buffer cache that holds the hot runs.
const POSTGRES_SETTINGS: [&str; 4] = ["max_wal_size=8GB", "wal_compression=lz4", "checkpoint_timeout=15min", "shared_buffers=1GB"];

async fn ensure_postgres(install: &Install, port: u16) -> Result<()> {
    let dir = install.postgres_dir();
    std::fs::create_dir_all(&dir)?;
    guard_postgres_major(&dir).await?;
    let name = install.postgres_container();
    let mut args: Vec<String> = [
        "run", "-d", "--name", &name, "--restart", "unless-stopped",
        // For a person's own tools (`psql`, a database client); the
        // runtime goes through the socket where it can.
        "-p", &format!("127.0.0.1:{port}:5432"),
        "-v", &format!("{}:/var/lib/postgresql/data", dir.display()),
        "-e", &format!("POSTGRES_USER={PG_USER}"),
        "-e", &format!("POSTGRES_PASSWORD={PG_PASSWORD}"),
        "-e", &format!("POSTGRES_DB={PG_DB}"),
        "-e", "PGDATA=/var/lib/postgresql/data/pgdata",
        "--label", &format!("{}={}", weft_core::infra::INSTALL_LABEL, install.id.label_value()),
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    if SOCKET_REACHES_POSTGRES {
        // The container's socket directory, seen from the host. The runtime connects there, past Docker's
        // port relay (a round trip through it costs about four times one
        // straight to Postgres). The image's entrypoint makes the directory
        // Postgres's own as it starts, and the socket in it is open to
        // every user.
        let socket = install.postgres_socket_dir();
        std::fs::create_dir_all(&socket)?;
        args.extend(["-v".to_string(), format!("{}:/var/run/postgresql", socket.display())]);
    }
    args.push(POSTGRES_IMAGE.to_string());
    args.push("postgres".to_string());
    for setting in POSTGRES_SETTINGS {
        args.extend(["-c".to_string(), setting.to_string()]);
    }
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

/// Whether the host can reach the Postgres container through a socket in
/// a shared directory (`ensure_postgres` shares it): on Linux, where the
/// containers run on the host's own kernel. Docker Desktop runs them in a machine of its own, and a socket
/// does not cross into it, so there the published port is the way in.
const SOCKET_REACHES_POSTGRES: bool = cfg!(target_os = "linux");

/// The runtime's address for its database: the shared socket where it
/// reaches Postgres ([`SOCKET_REACHES_POSTGRES`]), else the published
/// loopback `port`.
fn database_url(install: &Install, port: u16) -> String {
    if SOCKET_REACHES_POSTGRES {
        let socket: String = url::form_urlencoded::byte_serialize(install.postgres_socket_dir().display().to_string().as_bytes()).collect();
        // The socket is named by Postgres's own port inside the container.
        format!("postgres://{PG_USER}:{PG_PASSWORD}@localhost:5432/{PG_DB}?host={socket}")
    } else {
        format!("postgres://{PG_USER}:{PG_PASSWORD}@127.0.0.1:{port}/{PG_DB}")
    }
}

// ----- object store ------------------------------------------------------

/// The object store's key pair: made once per machine (every install on it
/// shares the store) and kept in `<data dir>/object-store/keys.env`,
/// readable by its owner only, so no web page or other person knows it.
/// Only [`ensure_object_store`] calls it, under the machine-wide lock: two
/// starts at once would otherwise each make a pair, and the one written
/// second would leave the other install holding keys the store refuses.
fn object_store_keys() -> Result<(String, String)> {
    let dir = data_dir().join("object-store");
    let path = dir.join("keys.env");
    let held = read_env_file(&path);
    let value = |name: &str| held.get(name).filter(|v| !v.is_empty()).cloned();
    if let (Some(access), Some(secret)) = (value(OBJECT_STORE_ACCESS_KEY_ENV), value(OBJECT_STORE_SECRET_KEY_ENV)) {
        return Ok((access, secret));
    }
    let (access, secret) = (format!("weft-{}", &random_hex_32()[..16]), random_hex_32());
    std::fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
    write_private(&path, &format!("{OBJECT_STORE_ACCESS_KEY_ENV}={access}\n{OBJECT_STORE_SECRET_KEY_ENV}={secret}\n"))?;
    Ok((access, secret))
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

/// The `docker run` arguments of the object store. Its master, volume and
/// filer servers answer without any key (the filer serves and takes every
/// file in the bucket, and its user service makes new S3 identities), so
/// they listen on the container's own loopback (`-ip=127.0.0.1`), where
/// only the S3 gateway beside them and `weed shell` under `docker exec`
/// reach them. The S3 gateway alone listens on the container's address,
/// for the port published on the machine's loopback: it checks every
/// request against the keys, and its gRPC port beside it refuses any
/// caller from outside the container. The container joins no network of
/// weft's, so an infra container cannot even reach that.
fn object_store_run_args(port: u16, config_label: &str, cfg_path: &Path) -> Vec<String> {
    [
        "run", "-d", "--name", OBJECT_STORE_CONTAINER, "--restart", "unless-stopped",
        "--label", config_label,
        "-p", &format!("127.0.0.1:{port}:8333"),
        "-v", &format!("{OBJECT_STORE_CONTAINER}-data:/data"),
        "-v", &format!("{}:/etc/seaweedfs/s3.config.json:ro", cfg_path.display()),
        OBJECT_STORE_IMAGE,
        "server", "-dir=/data", "-ip=127.0.0.1",
        "-s3", "-s3.ip.bind=0.0.0.0", "-s3.port=8333", "-s3.config=/etc/seaweedfs/s3.config.json",
        "-master.volumeSizeLimitMB=1024",
        // No usage reports leave the machine, and the Iceberg and Lance
        // servers 4.x starts beside S3 stay off: weft speaks only S3.
        "-master.telemetry=false", "-s3.port.iceberg=0", "-s3.port.lance=0",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

/// The object store: SeaweedFS's S3 gateway, one per machine. Only
/// processes on this machine reach it, at the S3 port published on
/// loopback: the runtime, the workers, and a caller the dispatcher's
/// files door passes on.
/// `-s3.externalUrl` stays unset so the gateway checks each presigned
/// request against its own Host header, which lets one store take URLs
/// signed for loopback and the ones the files door forwards with the
/// install's public host.
/// Machine-wide, from reading or making the keys until the store runs on
/// them, and answers the keys it runs on, which the install's secrets
/// then carry.
async fn ensure_object_store(port: u16) -> Result<(String, String)> {
    let _turn =
        images::hold_file_lock(data_dir().join("object-store.lock"), "another weft daemon start is setting up the object store; waiting for it")
            .await?;
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
    let (access, secret) = object_store_keys()?;
    let s3_config = object_store_s3_config(&access, &secret);
    std::fs::write(&cfg_path, &s3_config)?;
    // The store reads its keys once, at start: a label of their digest makes
    // new keys a new container spec, so the store starts again with them.
    let config_label = format!("weft.config={}", weft_core::project::hash::sha256_hex(s3_config.as_bytes()));
    let args = object_store_run_args(port, &config_label, &cfg_path);
    // Shared by every install on this machine, so one variable moves it.
    let published = PortUse { addr: ([127, 0, 0, 1], port).into(), what: "its file store", move_it: "WEFT_SEAWEED_PORT=<port> ./setup.sh".into() };
    ensure_container(OBJECT_STORE_CONTAINER, &args, &data_dir(), Some(published)).await?;
    Ok((access, secret))
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
    weft_core::infra::env_file_values(&std::fs::read_to_string(path).unwrap_or_default())
}

fn write_private(path: &Path, content: &str) -> Result<()> {
    let (Some(dir), Some(name)) = (path.parent(), path.file_name().and_then(|n| n.to_str())) else {
        anyhow::bail!("{} names no file in a folder", path.display());
    };
    weft_core::infra::write_whole(dir, name, content.as_bytes(), 0o600).with_context(|| format!("write {}", path.display()))
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
/// `object_store_keys` are the ones [`ensure_object_store`] runs the
/// store on.
fn secrets(
    install: &Install,
    ports: Ports,
    repo_root: &Path,
    object_store_keys: (String, String),
) -> Result<std::collections::BTreeMap<String, String>> {
    let old = read_env_file(&install.secrets_path());
    let keep_or = |name: &str, make: &mut dyn FnMut() -> String| old.get(name).filter(|v| !v.is_empty()).cloned().unwrap_or_else(make);
    let mut env = std::collections::BTreeMap::new();
    // Read back by `weft_core::infra::Install::secret` (a migration's
    // release, the e2e harness) and scripts/run-e2e.sh.
    env.insert("WEFT_DATABASE_URL".to_string(), database_url(install, ports.postgres));
    env.insert("WEFT_IDENTITY_KEY".to_string(), keep_or("WEFT_IDENTITY_KEY", &mut random_hex_32));
    env.insert("WEFT_CALLER_TOKEN_SECRET".to_string(), keep_or("WEFT_CALLER_TOKEN_SECRET", &mut random_hex_32));
    // The install's own operator key, which its management API asks of
    // every caller and a CLI on this machine reads back
    // (`crate::credentials`).
    let key = weft_core::signal_token::INSTALL_KEY_SECRET;
    env.insert(key.to_string(), keep_or(key, &mut weft_core::signal_token::generate_token));
    // The sealing key is the operator's (the shell or the `.env`); once
    // given it is kept, since rows sealed with it open under no other.
    if let Some(key) = std::env::var("CREDENTIAL_ENCRYPTION_KEY").ok().filter(|v| !v.is_empty()).or_else(|| old.get("CREDENTIAL_ENCRYPTION_KEY").cloned()) {
        env.insert("CREDENTIAL_ENCRYPTION_KEY".to_string(), key);
    }
    let (access, secret) = object_store_keys;
    env.insert(OBJECT_STORE_ACCESS_KEY_ENV.to_string(), access);
    env.insert(OBJECT_STORE_SECRET_KEY_ENV.to_string(), secret);
    env.insert(weft_core::access::spec::APPS_FILE_ENV.to_string(), access_apps_file(repo_root).display().to_string());
    env.insert("WEFT_REPO_ROOT".to_string(), repo_root.display().to_string());
    if let Ok(scale) = std::env::var(weft_core::time_scale::TIME_SCALE_ENV) {
        weft_core::time_scale::parse(Some(&scale)).map_err(anyhow::Error::msg)?;
        env.insert(weft_core::time_scale::TIME_SCALE_ENV.to_string(), scale);
    }
    // The machine's compile lanes, the same count this start's shell
    // gives every CLI it runs (`weft_platform_local::lanes`).
    let lanes = weft_platform_local::lanes::COMPILE_LANES_ENV;
    if let Ok(count) = std::env::var(lanes) {
        weft_platform_local::lanes::parse_lane_count(Some(&count))?;
        env.insert(lanes.to_string(), count);
    }
    Ok(env)
}

// ----- the config --------------------------------------------------------

/// The address a container (the agent beside an infra unit) reaches the
/// machine's internal port at. On Linux the machine IS the Docker host,
/// and a container reaches it through the `host-gateway` name the infra
/// host adds (`weft_platform_local::docker::host_gateway_args`); Docker
/// Desktop does the same by itself.
fn container_internal_url(ports: Ports) -> String {
    format!("http://host.docker.internal:{}", ports.internal)
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
/// them: the worker defaults, the edge, how long idle workers stay up,
/// the network address.
/// Only these are read from the file, each where it sits: the rest of it
/// is weft's own and written afresh every start, so a shape a newer weft
/// writes differently never stops the start that rewrites it.
#[derive(Debug, Clone, Default, PartialEq)]
struct Levers {
    worker_idle_stop_seconds: Option<u64>,
    workers: Option<weft_platform_traits::WorkerOverrides>,
    edge: Option<KeptEdge>,
    /// The network address, as the network url the last start wrote
    /// (`http://<address>:<outside port>`): the outside port may have
    /// moved since, so only the address is kept from it.
    network_url: Option<String>,
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
            network_url: lever(&file, path, "/platform/networkUrl")?,
        })
    }
}

/// The setting that opens the outside doors to this machine's network.
const NETWORK_ADDRESS_ENV: &str = "WEFT_NETWORK_ADDRESS";

/// The address devices on this machine's network reach it at, an IP:
/// `WEFT_NETWORK_ADDRESS` when this start sets it (`off` clears it), else
/// the one the last start kept in `kept_url`, its network url. Either way
/// it is checked to be an address another device can reach
/// ([`reachable_network_address`]).
fn network_address(set: Option<&str>, kept_url: Option<&str>) -> Result<Option<std::net::IpAddr>> {
    let (address, named) = match set.map(str::trim).filter(|v| !v.is_empty()) {
        Some("off") => return Ok(None),
        Some(v) => (check_network_address(v)?, format!("{NETWORK_ADDRESS_ENV}='{v}'")),
        None => match kept_url {
            Some(url) => (address_of_network_url(url)?, format!("the network address in config.json (networkUrl '{url}')")),
            None => return Ok(None),
        },
    };
    reachable_network_address(address, &named).map(Some)
}

/// `raw` as a network address: the IP of this machine that devices on its
/// network reach (the outside doors listen on it). A scheme, a port or a
/// path belongs to weft, which adds them itself.
fn check_network_address(raw: &str) -> Result<std::net::IpAddr> {
    raw.parse().map_err(|_| {
        anyhow::anyhow!(
            "{NETWORK_ADDRESS_ENV}='{raw}' is not a network address: it is the IP of this machine that devices on its \
             network reach, with no scheme, port or path (like 192.168.1.20); weft adds the outside port itself. \
             `{NETWORK_ADDRESS_ENV}=off` turns it off"
        )
    })
}

/// `address`, refused when no other device could reach the doors on it:
/// a loopback address answers only this machine (the outside doors are
/// on loopback already), an unspecified one (`0.0.0.0`, `::`) would put
/// them on every interface this machine has, and a link-local IPv6 one
/// (`fe80::`) needs a zone a link cannot carry. An IPv4 address written
/// as IPv6 (`::ffff:127.0.0.1`) is read as the IPv4 one it is, so it is
/// checked and opened as that. `named` says where the address came from.
fn reachable_network_address(address: std::net::IpAddr, named: &str) -> Result<std::net::IpAddr> {
    let address = address.to_canonical();
    let why = match address {
        a if a.is_loopback() => Some("a loopback address, which only this machine reaches (the outside doors are there already)"),
        a if a.is_unspecified() => Some("every address of this machine at once, which would open the outside doors on all of them"),
        std::net::IpAddr::V6(a) if a.is_unicast_link_local() => Some("a link-local address, which a link cannot name without the interface"),
        _ => None,
    };
    match why {
        Some(why) => anyhow::bail!(
            "{named} is {why}: give the IP of this machine that devices on its network reach (like 192.168.1.20), \
             or `{NETWORK_ADDRESS_ENV}=off` to turn it off"
        ),
        None => Ok(address),
    }
}

/// Refuse a network address this machine does not have: binding a port
/// on it fails at once here, where the runtime would otherwise fail to
/// open its listener minutes later. Any port does (0 lets the system pick
/// one), so another program on the outside port is left to
/// [`refuse_taken_ports`], which names it.
fn refuse_a_missing_network_address(address: std::net::IpAddr) -> Result<()> {
    match std::net::TcpListener::bind((address, 0)) {
        Ok(_) => Ok(()),
        Err(e) => anyhow::bail!(
            "this machine has no network address {address} ({e}): set {NETWORK_ADDRESS_ENV} to one of its IPs \
             (`ip -brief address` lists them), or `{NETWORK_ADDRESS_ENV}=off` to turn it off"
        ),
    }
}

/// The address a network url names (`http://<address>:<port>`).
fn address_of_network_url(url: &str) -> Result<std::net::IpAddr> {
    let parsed = url::Url::parse(url).map_err(|e| anyhow::anyhow!("networkUrl '{url}' in config.json is not a URL: {e}"))?;
    match parsed.host() {
        Some(url::Host::Ipv6(ip)) => Ok(ip.into()),
        Some(url::Host::Ipv4(ip)) => Ok(ip.into()),
        _ => anyhow::bail!("networkUrl '{url}' in config.json names no IP; `{NETWORK_ADDRESS_ENV}=<IP>` sets one"),
    }
}

/// Where the outside doors are reached from this machine's network: the
/// address and the outside port.
fn network_listen(address: std::net::IpAddr, outside: u16) -> std::net::SocketAddr {
    (address, outside).into()
}

/// The network listener as a link is built on.
fn network_url(listen: std::net::SocketAddr) -> String {
    format!("http://{listen}")
}

/// The install's config. Everything that follows from this machine is
/// set here; the levers a person sets in `config.json` are kept from
/// `previous`, what the last start wrote.
fn install_config(
    install: &Install,
    ports: Ports,
    runtime_image: String,
    tunnel: Option<&Tunnel>,
    previous: Levers,
) -> Result<InstallConfig> {
    // Only what a person set is written: a lever left out follows weft's
    // default (`LocalPlatform::worker_idle_stop`), so a changed default
    // reaches the install at its next start.
    let idle_stop = match (previous.worker_idle_stop_seconds, std::env::var("WEFT_WORKER_IDLE_STOP_SECONDS").ok().filter(|v| !v.trim().is_empty())) {
        (_, Some(v)) => Some(v.trim().parse().map_err(|_| anyhow::anyhow!("WEFT_WORKER_IDLE_STOP_SECONDS='{v}' is not a whole number"))?),
        (kept, None) => kept,
    };
    let workers = previous.workers.unwrap_or_default();
    let edge = edge_config(previous.edge)?;
    let network = network_address(std::env::var(NETWORK_ADDRESS_ENV).ok().as_deref(), previous.network_url.as_deref())?;
    let config = InstallConfig {
        install: install.id.clone(),
        platform: PlatformConfig::Local(Box::new(LocalPlatform {
            data_dir: install.dir.clone(),
            container_internal_url: container_internal_url(ports),
            runtime_image,
            worker_idle_stop_seconds: idle_stop,
            listen: Listen {
                public: ([127, 0, 0, 1], ports.public).into(),
                // Also where the project's containers reach this machine,
                // which the runtime asks Docker on every start
                // (`weft_platform_local::docker::containers_call_in_at`).
                internal: ([127, 0, 0, 1], ports.internal).into(),
                // Served whether or not a tunnel carries it: it answers a
                // subset of what the public port does, and a tunnel opened
                // later finds it already there.
                outside: Some(([127, 0, 0, 1], ports.outside).into()),
                // The same doors again on the machine's network address,
                // a listener of its own: its callers connect straight to
                // it, so it trusts no forwarded address, where the
                // tunnel's port trusts the one cloudflared adds.
                network: network.map(|address| network_listen(address, ports.outside)),
            },
            project_ports: install.id.name().is_none().then_some(weft_platform_traits::config::PortRange {
                first: weft_core::ports::PROJECTS.0,
                last: weft_core::ports::PROJECTS.1,
            }),
            internal_url: format!("http://127.0.0.1:{}", ports.internal),
            network_url: network.map(|address| network_url(network_listen(address, ports.outside))),
            quick_tunnel_metrics: tunnel.and_then(|t| t.metrics.clone()),
        })),
        public_url: format!("http://127.0.0.1:{}", ports.public),
        internet_url: tunnel.map(|t| t.url.clone()),
        roles: Default::default(),
        role_urls: Default::default(),
        workers,
        holders: Default::default(),
        // The one process takes the whole database.
        max_copies: Default::default(),
        edge,
        object_store: ObjectStoreSettings::S3(S3StoreSettings {
            endpoint: format!("http://127.0.0.1:{}", object_store_port()?),
            bucket: install.bucket(),
            region: "us-east-1".into(),
            force_path_style: true,
            public_endpoint: None,
            public_internet: false,
        }),
        source: None,
        alert_after_minutes: weft_platform_traits::config::DEFAULT_ALERT_AFTER_MINUTES,
    };
    config.validate().map_err(|e| anyhow::anyhow!("the install config weft made is not valid: {e}"))?;
    Ok(config)
}

/// The local half of a config the daemon wrote.
fn local_of(config: &InstallConfig) -> &LocalPlatform {
    match &config.platform {
        PlatformConfig::Local(local) => local,
        PlatformConfig::Gcp(_) => unreachable!("the daemon only ever writes a local install's config"),
    }
}

/// The ports a local install's one process serves.
fn listen_of(config: &InstallConfig) -> &Listen {
    &local_of(config).listen
}

// ----- the tunnel --------------------------------------------------------

/// The install's open tunnel, as a start found it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Tunnel {
    /// The address it answers at now.
    url: String,
    /// Whether it is a named tunnel, whose address is fixed.
    named: bool,
    /// For a quick tunnel, where its metrics server answers
    /// (`http://127.0.0.1:<port>`), which says its address as it is now:
    /// a quick tunnel's moves whenever it reconnects.
    metrics: Option<String>,
}

/// Bring the tunnel up (or down) to match the persisted choice, and
/// answer it when it is up. The tunnel carries only the outside port: the
/// doors outside callers use, never the management API.
async fn reconcile_tunnel(install: &Install, ports: Ports) -> Result<Option<Tunnel>> {
    let name = install.tunnel_container();
    if !public_url_marker(install).exists() {
        remove_container(&name).await?;
        remove_if_present(&public_url_file(install))?;
        remove_if_present(&tunnel_env_file(install))?;
        return Ok(None);
    }
    let env_file = tunnel_env_file(install);
    let named = named_tunnel(TunnelSettings::from_env(), read_tunnel_record(&env_file)?, &env_file)?;
    let (network, origin) = tunnel_origin(ports);
    let label = format!("{}={}", weft_core::infra::INSTALL_LABEL, install.id.label_value());
    let mut args: Vec<String> = ["run", "-d", "--name", &name, "--restart", "unless-stopped", "--network", &network, "--label", &label]
        .iter()
        .map(|s| s.to_string())
        .collect();
    // Machine-wide, from the check that no other install runs this token
    // until this install's connector runs on it: two starts at once
    // would otherwise both pass the check. Held to the end of this call.
    let _turn = match &named {
        Some(_) => Some(
            images::hold_file_lock(data_dir().join("tunnels.lock"), "another weft daemon start is opening its tunnel; waiting for it")
                .await?,
        ),
        None => None,
    };
    // With no named tunnel there is no record ([`named_tunnel`] opens the
    // recorded one), so nothing claims this install runs one.
    if let Some(tunnel) = &named {
        refuse_a_shared_tunnel(install, &tunnel.token).await?;
        write_private(&env_file, &tunnel_record(tunnel))?;
        // The container reads the token from the file once, when it is
        // made, so the token's fingerprint goes in its arguments: a new
        // token makes it again ([`ensure_container`]).
        let token = format!("weft.tunnel-token={}", weft_core::project::hash::sha256_hex(tunnel.token.as_bytes()));
        args.extend(["--label".into(), token, "--env-file".into(), env_file.display().to_string()]);
    }
    let metrics = QuickTunnelMetrics::on(ports.tunnel_metrics, cfg!(target_os = "linux"));
    if named.is_none() {
        args.extend(metrics.docker_args.iter().cloned());
    }
    // The origin goes to a named tunnel too: one made with `cloudflared
    // tunnel create` (how a branch's install gets its own address) has no
    // routing stored at Cloudflare and forwards to `--url`, while one set
    // up in the dashboard gets its routing from there, which replaces it.
    args.extend([TUNNEL_IMAGE.to_string(), "tunnel".into(), "--no-autoupdate".into()]);
    if named.is_none() {
        args.extend(["--metrics".into(), metrics.bind.clone()]);
    }
    args.extend(["--url".into(), origin]);
    if named.is_some() {
        args.push("run".into());
    }
    // A quick tunnel's metrics port is taken on loopback either way: on
    // the host's network by cloudflared itself, elsewhere by Docker.
    let published = named.is_none().then(|| {
        PortUse::of(install, ([127, 0, 0, 1], ports.tunnel_metrics).into(), "the quick tunnel's address", "WEFT_TUNNEL_METRICS_PORT")
    });
    ensure_container(&name, &args, &install.dir, published).await?;
    let tunnel = match named {
        Some(NamedTunnel { hostname, .. }) => Tunnel { url: hostname, named: true, metrics: None },
        None => Tunnel { url: quick_tunnel_url(&name, &metrics.url).await?, named: false, metrics: Some(metrics.url) },
    };
    Ok(Some(tunnel))
}

/// How a quick tunnel's metrics server is run so the install reaches it on
/// loopback, where it says the tunnel's address as it is now.
#[derive(Debug, Clone, PartialEq, Eq)]
struct QuickTunnelMetrics {
    /// What the tunnel container is run with besides: the published port,
    /// when it runs on Docker's bridge network.
    docker_args: Vec<String>,
    /// Where cloudflared listens, inside the container (`--metrics`).
    bind: String,
    /// Where the install asks it, from the machine.
    url: String,
}

impl QuickTunnelMetrics {
    /// On Linux the tunnel runs on the host's network ([`tunnel_origin`]),
    /// so it listens on loopback itself. Elsewhere it runs on Docker's
    /// bridge network: it listens on every interface inside its container,
    /// and Docker publishes the port to the machine's loopback only.
    fn on(port: u16, host_network: bool) -> Self {
        let url = format!("http://127.0.0.1:{port}");
        if host_network {
            Self { docker_args: Vec::new(), bind: format!("127.0.0.1:{port}"), url }
        } else {
            Self { docker_args: vec!["-p".into(), format!("127.0.0.1:{port}:{port}")], bind: format!("0.0.0.0:{port}"), url }
        }
    }
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

/// The quick tunnel's address, as its metrics server at `metrics` says it
/// now (`/quicktunnel`, the same question the install asks each time it
/// builds a link on it).
async fn quick_tunnel_url(container: &str, metrics: &str) -> Result<String> {
    let client = weft_core::net::http_client().timeout(Duration::from_secs(3)).build()?;
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    loop {
        // Not answering yet, or answering with no hostname: the tunnel is
        // still connecting.
        if let Ok(answer) = client.get(format!("{metrics}/quicktunnel")).send().await {
            if let Ok(QuickTunnelAnswer { hostname }) = answer.json().await {
                if !hostname.is_empty() {
                    return Ok(format!("https://{hostname}"));
                }
            }
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "the tunnel reported no public address at {metrics}/quicktunnel; its log: docker logs {container}"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// What a quick tunnel's `/quicktunnel` answers.
#[derive(serde::Deserialize)]
struct QuickTunnelAnswer {
    hostname: String,
}

/// Prove the address reaches the outside doors, then record it.
async fn announce_tunnel(install: &Install, Tunnel { url, named, .. }: &Tunnel, ports: Ports) -> Result<()> {
    let named = *named;
    let container = install.tunnel_container();
    let client = weft_core::net::http_client().timeout(Duration::from_secs(10)).build()?;
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
    std::fs::create_dir_all(&install.dir)?;
    std::fs::write(public_url_file(install), url)?;
    println!("public address: {url}");
    println!("  it carries only the doors outside callers use: /events/... (provider event pushes), /signal/... (fire links), /signal-token/..., /files/... (links to stored files, and visitors' uploads), /connect/... (live routes), /infra/... (public infra endpoints) and the OAuth callback. Rerun with --no-public-url to close it.");
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

/// The launchd job that runs `line` under `label`. What the shell prints
/// before the runtime's own redirect takes over (a secrets file it cannot
/// read, a binary gone) goes to `log` through `StandardErrorPath`.
fn launchd_plist(label: &str, line: &str, log: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict>\n<key>Label</key><string>{label}</string>\n<key>ProgramArguments</key><array><string>/bin/sh</string><string>-c</string><string>{line}</string></array>\n<key>StandardErrorPath</key><string>{log}</string>\n<key>RunAtLoad</key><true/>\n<key>KeepAlive</key><true/>\n</dict></plist>\n",
        label = xml_text(label),
        line = xml_text(line),
        log = xml_text(log),
    )
}

/// `text` as a plist's `<string>` holds it.
fn xml_text(text: &str) -> String {
    text.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

fn pid_path(install: &Install) -> PathBuf {
    install.dir.join("runtime.pid")
}

/// The shell line every manager runs: the person's PATH as this start
/// sees it (a systemd or launchd service starts with a bare one, and the
/// runtime's builds, needs checks and workers use the person's own tools:
/// cargo, python3, Homebrew's), the secrets into the environment, then the
/// runtime, which keeps its log bounded itself (`--log`); what escapes it
/// (a panic's last words) is appended to the same file, which the runtime
/// keeps its stderr on across each roll (`weft_runtime::log_file`).
fn runtime_command(install: &Install, bin: &Path) -> String {
    let path = std::env::var("PATH").unwrap_or_default().replace('\'', "'\\''");
    format!(
        "export PATH='{path}'; set -a; . '{}'; set +a; exec '{}' serve --config '{}' --log '{}' >> '{}' 2>&1",
        install.secrets_path().display(),
        bin.display(),
        install.config_path().display(),
        install.log_path().display(),
        install.log_path().display()
    )
}

async fn run_checked(cmd: &mut Command, what: &str) -> Result<()> {
    let out = cmd.output().await.with_context(|| format!("run {what}"))?;
    anyhow::ensure!(out.status.success(), "{}", failed(what, &out));
    Ok(())
}

/// A service manager's failure, whole: its exit status and everything it
/// printed. `launchctl` says why on stdout as often as on stderr, and an
/// error that kept only stderr once read `launchctl bootstrap failed: `
/// and nothing more.
fn failed(what: &str, out: &std::process::Output) -> String {
    let said: Vec<String> = [&out.stdout, &out.stderr]
        .into_iter()
        .map(|bytes| String::from_utf8_lossy(bytes).trim().to_string())
        .filter(|text| !text.is_empty())
        .collect();
    match said.is_empty() {
        true => format!("{what} failed ({}) and printed nothing", out.status),
        false => format!("{what} failed ({}): {}", out.status, said.join("\n")),
    }
}

async fn start_runtime(install: &Install) -> Result<()> {
    let bin = runtime_binary()?;
    let line = runtime_command(install, &bin);
    match manager().await? {
        Manager::Systemd => {
            let unit = systemd_unit_path(install);
            std::fs::create_dir_all(unit.parent().expect("a unit path has a directory"))?;
            // `KillMode=process`: a restart stops the runtime alone, and the
            // project workers it started (processes of their own) finish
            // their runs for its next life to find and let go, the way the
            // launchd and detached managers already leave them.
            std::fs::write(
                &unit,
                format!(
                    "[Unit]\nDescription=weft runtime ({})\nAfter=network-online.target\n\n[Service]\nExecStart=/bin/sh -c \"{}\"\nRestart=always\nRestartSec=2\nKillMode=process\n\n[Install]\nWantedBy=default.target\n",
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
            std::fs::write(&plist, launchd_plist(&label, &line, &install.log_path().display().to_string()))?;
            // Booted out already: the start stops the runtime before it
            // gets here (`stop_runtime`), and says so when it could not.
            let domain = format!("gui/{}", current_uid().await?);
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
                anyhow::ensure!(!active, "{}", failed(&format!("systemctl --user stop {name}"), &out));
            }
        }
        Manager::Launchd => {
            let target = format!("gui/{}/{}", current_uid().await?, launchd_label(install));
            let out = Command::new("launchctl").args(["bootout", &target]).output().await.context("run launchctl")?;
            // A service that was not loaded is already stopped; one still
            // loaded after a failed bootout is not.
            if !out.status.success() {
                let loaded = Command::new("launchctl").args(["print", &target]).output().await?.status.success();
                anyhow::ensure!(!loaded, "{}", failed(&format!("launchctl bootout {target}"), &out));
            }
        }
        Manager::Detached => stop_detached(install).await?,
    }
    Ok(())
}

/// Wait until the runtime answers, naming its log when it does not.
async fn wait_for_runtime(install: &Install, public: u16) -> Result<()> {
    let client = weft_core::net::http_client().timeout(Duration::from_secs(3)).build()?;
    let deadline = std::time::Instant::now() + Duration::from_secs(180);
    loop {
        if client.get(format!("http://127.0.0.1:{public}/health")).send().await.is_ok_and(|r| r.status().is_success()) {
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
/// Only the default install asks: that cluster held the default install's
/// projects, and a named install shares neither its data nor its ports.
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
    // The levers the last start left, read once: a network address that is
    // no good (set now or kept) is refused before anything starts.
    let previous = match install.config_path().exists() {
        true => Levers::read(&install.config_path())?,
        false => Levers::default(),
    };
    if let Some(address) = network_address(std::env::var(NETWORK_ADDRESS_ENV).ok().as_deref(), previous.network_url.as_deref())? {
        refuse_a_missing_network_address(address)?;
    }
    let ports = ports(install)?;

    // This install runs on these from now on, so they are recorded before
    // anything is pulled or built: a machine-wide sweep (`weft clean
    // --images --all`, `--build-plane`) from another checkout keeps them,
    // while this start makes them too. Its workers are programs compiled
    // on this machine (no builder image, no image compile cache, which the
    // record leaves empty); the runtime runs from its binary, and the
    // runtime image is what its infra units' agents run, built around
    // that same binary whenever it can start there.
    let runtime_image = images::local_runtime_image(&runtime_binary()?)?;
    let runtime = runtime_image.image_ref.clone();
    let packaging = weft_platform_local::MachineLayout::here().host_packaging();
    let standard_hash = weft_compiler::build::stock_binary_hash(&packaging)?;
    let standard = weft_compiler::build::worker_image_tag(&standard_hash);
    weft_core::images::ImageRefs {
        runtime: runtime.clone(),
        standard_worker: standard.clone(),
        builder_bases: Default::default(),
        compile_cache_keys: Default::default(),
    }
    .save(&install.dir)?;
    runtime_image.ensure(rebuild).await?;
    crate::host_programs::ensure_standard_program(&install.id, &standard_hash, rebuild).await?;
    weft_platform_local::docker::ensure_network(&weft_platform_local::DockerCli).await?;
    ensure_postgres(install, ports.postgres).await?;
    let object_store_keys = ensure_object_store(object_store_port()?).await?;
    let tunnel = reconcile_tunnel(install, ports).await?;

    let config = install_config(install, ports, runtime, tunnel.as_ref(), previous)?;
    std::fs::write(install.config_path(), serde_json::to_vec_pretty(&config)?)?;
    let env = secrets(install, ports, &repo_root, object_store_keys)?;
    // SYNC: secrets.env lines <-> crates/weft-core/src/infra/install.rs (SECRETS_FILE,
    //       env_file_values: the parser), scripts/run-e2e.sh (WEFT_E2E_DATABASE_URL)
    let body: String = env.iter().map(|(k, v)| format!("{k}={v}\n")).collect();
    write_private(&install.secrets_path(), &body)?;

    // The runtime that ran before holds these ports; once it is stopped,
    // anything still listening there is another program, and the new
    // runtime would never come up.
    stop_runtime(install).await?;
    remove_docker_workers(install).await?;
    let listen = listen_of(&config);
    let mut uses = vec![
        PortUse::of(install, listen.public, "its API and dashboard", "WEFT_PUBLIC_PORT"),
        PortUse::of(install, listen.internal, "the calls its own containers make to it", "WEFT_INTERNAL_PORT"),
    ];
    if let Some(outside) = listen.outside {
        uses.push(PortUse::of(install, outside, "the doors outside callers use (the tunnel's target)", "WEFT_OUTSIDE_PORT"));
    }
    if let Some(network) = listen.network {
        uses.push(PortUse::of(install, network, "the doors devices on this machine's network use", "WEFT_OUTSIDE_PORT"));
    }
    refuse_taken_ports(&uses).await?;
    start_runtime(install).await?;
    wait_for_runtime(install, ports.public).await?;
    let key = install.id.secret(weft_core::signal_token::INSTALL_KEY_SECRET).map_err(anyhow::Error::msg)?;
    let local = crate::client::DispatcherClient::new(format!("http://127.0.0.1:{}", ports.public), Some(key));
    super::catalog::preload_standard_library(&local)
        .await
        .context("store the standard library in this install's assets")?;
    if let Some(tunnel) = &tunnel {
        announce_tunnel(install, tunnel, ports).await?;
    }
    if let Some(url) = local_of(&config).network_url.as_deref() {
        println!("network address: {url} (the doors outside callers use, for devices on this machine's network; a start with {NETWORK_ADDRESS_ENV}=off closes it)");
    }
    println!("weft is running at http://127.0.0.1:{} (install '{}')", ports.public, install.id.label_value());
    Ok(())
}

/// Every container of `install` (everything carrying its install label),
/// with its `weft.role`: `infra` for its infra units, none for the ones a
/// start makes itself (its database and its tunnel), and `worker`, `long`
/// or `build-result` for one a weft from before workers were processes
/// left.
async fn install_containers(install: &Install) -> Result<Vec<(String, String)>> {
    let label = format!("label={}={}", weft_core::infra::INSTALL_LABEL, install.id.label_value());
    let listing = docker_ok(&["ps", "-a", "--filter", &label, "--format", "{{.Names}}\t{{.Label \"weft.role\"}}"], "docker ps").await?;
    Ok(listing.lines().filter_map(|l| l.split_once('\t')).map(|(name, role)| (name.to_string(), role.to_string())).collect())
}

/// Stop every worker process of `install`'s runtime.
async fn stop_workers(install: &Install) -> Result<()> {
    weft_platform_local::registry::stop_all(&weft_platform_local::MachineProcesses, &weft_platform_local::InstallLayout::new(&install.dir).workers()).await?;
    Ok(())
}

/// Remove what a weft from before workers were processes left in Docker
/// for `install`, at its start: its worker containers (one holding its
/// project's port would keep it from the project's new worker) and the
/// containers it made to read a build's result out of an image (one a
/// crash left would keep that image in use), then the worker images under
/// its own names, which nothing on this machine runs any more.
async fn remove_docker_workers(install: &Install) -> Result<()> {
    let left = install_containers(install).await?.into_iter().filter(|(_, role)| matches!(role.as_str(), "worker" | "long" | "build-result"));
    for (name, _) in left {
        remove_container(&name).await?;
    }
    let listing = crate::images::host_image_listing(&[]).await?;
    let stale = crate::images::docker_workers_of(&listing, &install.id);
    if !stale.is_empty() {
        println!("{}", crate::images::reclaim_host_images(&stale).await?.report("worker image(s) from before workers ran as processes"));
    }
    Ok(())
}

async fn stop(install: &Install) -> Result<()> {
    stop_runtime(install).await?;
    // Workers serve only the runtime; infra keeps running, as it would
    // across a runtime restart.
    stop_workers(install).await?;
    println!("weft's runtime stopped (install '{}'); the database and infra keep running", install.id.label_value());
    Ok(())
}

async fn remove(install: &Install) -> Result<()> {
    let Some(name) = install.id.name() else {
        anyhow::bail!(
            "`weft daemon remove` takes a named install off this machine (a branch checkout's, or one WEFT_INSTALL names), \
             and this command acts on the default install. That one holds the projects of everyone using weft on this \
             machine; `./setup.sh --uninstall` is how it goes."
        );
    };
    stop_runtime(install).await?;
    stop_workers(install).await?;
    for path in [systemd_unit_path(install), launchd_plist_path(install)] {
        let _ = std::fs::remove_file(path);
    }
    // Everything it runs: workers, infra units, its database, its tunnel
    // (which restarts by itself, and would keep serving its address).
    for (name, _) in install_containers(install).await? {
        remove_container(&name).await?;
    }
    let label = format!("label={}={}", weft_core::infra::INSTALL_LABEL, install.id.label_value());
    let volumes = docker_ok(&["volume", "ls", "-q", "--filter", &label], "docker volume ls").await?;
    for v in volumes.lines().filter(|l| !l.trim().is_empty()) {
        docker_ok(&["volume", "rm", "-f", v.trim()], "docker volume rm").await?;
    }
    delete_bucket(&install.bucket()).await?;
    // The caches its images compiled weft's crates in, which no other
    // install builds in (`weft_compiler::build::install_cache_key`).
    crate::images::drop_install_build_caches(&install.id, None).await?;
    // The images it holds under its own names, now that nothing of it
    // runs them (`Install::local_image_ref`).
    crate::images::remove_install_images(&install.id).await?;
    // Postgres owned its files as its own user.
    if install.postgres_dir().exists() {
        let out = run_in_alpine(&install.dir, "/d", "rm -rf /d/postgres-data").await?;
        anyhow::ensure!(out.status.success(), "removing the database files failed: {}", String::from_utf8_lossy(&out.stderr));
    }
    // Its binaries go with the rest, the CLI running this command
    // included when it is the install's own: on Unix a running program
    // keeps its file until it exits, whatever happens to the name.
    std::fs::remove_dir_all(&install.dir).with_context(|| format!("remove {}", install.dir.display()))?;
    println!("install '{name}' removed");
    Ok(())
}

/// Reports on the install this command acts on (the one its label, log
/// path and public address describe), so its address comes from that
/// install's own ports, never from `--on`/`--dispatcher`, which are
/// refused rather than ignored. `WEFT_DISPATCHER_URL` is ambient
/// (setup.sh exports it) and is not consulted.
async fn status(ctx: &Ctx, install: &Install) -> Result<()> {
    ctx.refuse_other_install("weft daemon status")?;
    // A named install that never started has no address at all: saying
    // so is this command's answer, not an error.
    match weft_core::ports::local_public_url_of(&install.id) {
        Err(e) => println!("weft: no address to reach: {e}"),
        Ok(url) => {
            // An unreadable credentials file is its own error, not "no address".
            let key = crate::credentials::operator_key_for(&url, None)?;
            report_reachable(&crate::client::DispatcherClient::new(url, key), install).await;
        }
    }
    match current_public_url(install) {
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

/// What `weft daemon which` answers, `--json` printing it as one object.
// SYNC: Which <-> setup.sh, scripts/run-e2e.sh, scripts/lib (read with `weft daemon which --json`)
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Which {
    /// `None` for the default install.
    pub name: Option<String>,
    /// What `WEFT_INSTALL` holds for it: `default`, or the name.
    pub env: String,
    /// Its files.
    pub dir: PathBuf,
    /// The folder of its own `weft` and `weft-runtime`.
    pub bin: PathBuf,
    /// The branch of the checkout that picked it, `None` when
    /// `WEFT_INSTALL` did or it was asked outside any checkout.
    pub branch: Option<String>,
    /// Whether its `weft` is in `bin`.
    pub installed: bool,
}

impl Which {
    fn of(wanted: &Wanted) -> Self {
        let install = &wanted.install;
        Self {
            name: install.name().map(str::to_string),
            env: install.env_value().to_string(),
            dir: install.dir(),
            bin: install.bin_dir(),
            branch: match &wanted.from {
                WantedFrom::Checkout { branch, .. } => Some(branch.clone()),
                WantedFrom::Env | WantedFrom::Outside => None,
            },
            installed: crate::startup::cli_of(install).is_file(),
        }
    }

    /// The human answer: which install, what picked it, where it lives.
    fn lines(&self, from: &WantedFrom) -> Vec<String> {
        let picked = match from {
            WantedFrom::Env => format!("WEFT_INSTALL={}", self.env),
            WantedFrom::Checkout { root, branch } => format!("the branch {branch} of the checkout {}", root.display()),
            WantedFrom::Outside => "standing outside every weft checkout".to_string(),
        };
        let state = match (self.installed, from) {
            (true, _) => "set up".to_string(),
            (false, WantedFrom::Checkout { root, .. }) => format!("not set up: run ./setup.sh in {}", root.display()),
            (false, _) => "not set up".to_string(),
        };
        vec![
            format!("install: {} (WEFT_INSTALL={})", self.name.as_deref().unwrap_or("the default install"), self.env),
            format!("picked by: {picked}"),
            format!("files: {}", self.dir.display()),
            format!("binaries: {} ({state})", self.bin.display()),
        ]
    }
}

/// `weft daemon which`: the install this command acts on, as the front
/// door chose it.
fn which(ctx: &Ctx, wanted: &Wanted) -> Result<()> {
    let answer = Which::of(wanted);
    if ctx.json_out(&answer)? {
        return Ok(());
    }
    for line in answer.lines(&wanted.from) {
        println!("{line}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The job's command line and log path reach launchd as written, with
    /// every character XML gives a meaning escaped.
    #[test]
    fn the_launchd_job_carries_its_command_and_log_escaped() {
        let plist = launchd_plist("ai.weavemind.weft", "a && b < c > d", "/Users/x/R&D <1>/runtime.log");
        assert!(plist.contains("<string>a &amp;&amp; b &lt; c &gt; d</string>"), "{plist}");
        assert!(plist.contains("<key>StandardErrorPath</key><string>/Users/x/R&amp;D &lt;1&gt;/runtime.log</string>"), "{plist}");
        assert!(!plist.contains("R&D"), "{plist}");
    }

    /// A service manager's failure keeps its status and what it printed on
    /// either stream, and says so when it printed nothing.
    #[test]
    fn a_failed_service_manager_is_quoted_whole() {
        use std::os::unix::process::ExitStatusExt;
        let out = |stdout: &str, stderr: &str| std::process::Output {
            status: std::process::ExitStatus::from_raw(5 << 8),
            stdout: stdout.as_bytes().to_vec(),
            stderr: stderr.as_bytes().to_vec(),
        };
        let both = failed("launchctl bootstrap", &out("Bootstrap failed: 5: Input/output error\n", "  try again\n"));
        assert!(both.contains("exit status: 5"), "{both}");
        assert!(both.ends_with("Bootstrap failed: 5: Input/output error\ntry again"), "{both}");
        assert!(failed("launchctl bootstrap", &out("", "")).ends_with("and printed nothing"));
    }

    /// Docker's refusal for a container whose network was made again is
    /// told apart from every other start failure.
    #[test]
    fn a_vanished_network_is_told_apart() {
        assert!(stale_network(
            "Error response from daemon: failed to set up container networking: network cfc1be309d69 not found"
        ));
        assert!(!stale_network("Error response from daemon: driver failed programming external connectivity: port is already allocated"));
    }

    /// The object store answers outside its container only through S3:
    /// it joins no network of weft's (where infra containers run), its
    /// keyless servers listen on its own loopback, and S3 alone on its
    /// address, published on the machine's loopback.
    #[test]
    fn object_store_answers_outside_only_through_s3() {
        let args = object_store_run_args(14115, "weft.config=abc", Path::new("/data/object-store/s3.config.json"));
        assert!(!args.iter().any(|a| a == "--network" || a.starts_with("--network=")), "{args:?}");
        assert!(args.windows(2).any(|w| w == ["-p", "127.0.0.1:14115:8333"]), "{args:?}");
        assert!(args.iter().any(|a| a == "-ip=127.0.0.1"), "{args:?}");
        assert!(!args.iter().any(|a| a.starts_with("-ip.bind") || a.starts_with("-master.ip") || a.starts_with("-filer.ip") || a.starts_with("-volume.ip")), "{args:?}");
        assert!(args.iter().any(|a| a == "-s3.ip.bind=0.0.0.0"), "{args:?}");
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
        let ports = Ports { public: 14111, internal: 14113, outside: 14112, postgres: 14114, tunnel_metrics: 14117 };
        let closed = install_config(&install(None), ports, "weft-runtime:x".into(), None, Levers::default()).unwrap();
        assert_eq!(local_of(&closed).network_url, None, "no network address unless a person sets one");
        assert_eq!(listen_of(&closed).outside, Some(([127, 0, 0, 1], 14112).into()), "the outside port is served without a tunnel too");
        assert_eq!(closed.public_url, "http://127.0.0.1:14111");
        let ObjectStoreSettings::S3(store) = &closed.object_store else { panic!("a local install's store is S3-compatible") };
        assert_eq!(store.endpoint, format!("http://127.0.0.1:{}", object_store_port().unwrap()), "workers reach it where the runtime does");
        let quick = Tunnel { url: "https://a.trycloudflare.com".into(), named: false, metrics: Some("http://127.0.0.1:14117".into()) };
        let open = install_config(&install(None), ports, "weft-runtime:x".into(), Some(&quick), Levers::default()).unwrap();
        assert_eq!(open.internet_url.as_deref(), Some("https://a.trycloudflare.com"));
        assert_eq!(local_of(&open).quick_tunnel_metrics.as_deref(), Some("http://127.0.0.1:14117"), "a quick tunnel's address is asked each time");
        let named = Tunnel { url: "https://weft.example.com".into(), named: true, metrics: None };
        let fixed = install_config(&install(None), ports, "weft-runtime:x".into(), Some(&named), Levers::default()).unwrap();
        assert_eq!(local_of(&fixed).quick_tunnel_metrics, None, "a named tunnel's address is fixed");
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
        let again = install_config(&install(None), ports, "weft-runtime:y".into(), None, Levers::read(&file).unwrap()).unwrap();
        assert_eq!(again.workers.min_instances, Some(1));
        assert_eq!(again.worker_settings().concurrency, weft_platform_traits::WorkerSettings::default().concurrency, "what the file leaves out follows weft's default");
        let written = serde_json::to_value(&closed).unwrap();
        assert!(written.get("workers").is_none() && written["platform"].get("workerIdleStopSeconds").is_none(), "a lever nobody set is not written");
        assert_eq!(again.edge.trusted_proxy_hops, ProxyHops { public: 2, outside: 1, domains: 0 });
        assert_eq!(again.platform, PlatformConfig::Local(Box::new(LocalPlatform { runtime_image: "weft-runtime:y".into(), ..match closed.platform { PlatformConfig::Local(l) => *l, _ => unreachable!() } })));
    }

    /// On Linux the runtime reaches its database through the socket the
    /// container shares (its path carried whole, spaces and all), named by
    /// Postgres's own port; elsewhere through the published one.
    #[test]
    fn the_runtime_reaches_its_database_past_the_port_relay() {
        let spaced = Install { id: install(Some("cell3")).id, dir: "/home/u/my data/weft".into() };
        let url = database_url(&spaced, 15432);
        if SOCKET_REACHES_POSTGRES {
            assert_eq!(url, format!("postgres://{PG_USER}:{PG_PASSWORD}@localhost:5432/{PG_DB}?host=%2Fhome%2Fu%2Fmy+data%2Fweft%2Fpostgres-data%2Fsocket"));
            let parsed = url::Url::parse(&url).unwrap();
            let host = parsed.query_pairs().find(|(k, _)| k == "host").map(|(_, v)| v.into_owned());
            assert_eq!(host.as_deref(), Some("/home/u/my data/weft/postgres-data/socket"));
        } else {
            assert_eq!(url, format!("postgres://{PG_USER}:{PG_PASSWORD}@127.0.0.1:15432/{PG_DB}"));
        }
    }

    #[test]
    fn a_port_variable_moves_only_its_own_port() {
        let kept = moved_by_env(Ports::DEFAULT, |_| None).unwrap();
        assert_eq!(kept, Ports::DEFAULT);
        let moved = moved_by_env(Ports::DEFAULT, |n| (n == "WEFT_PUBLIC_PORT").then(|| " 15000 ".to_string())).unwrap();
        assert_eq!(moved, Ports { public: 15000, ..Ports::DEFAULT });
        assert!(moved_by_env(Ports::DEFAULT, |n| (n == "WEFT_POSTGRES_PORT").then(|| "x".to_string())).is_err());
    }

    /// A pid is this install's runtime only when its command line is the
    /// one the start execs on this install's config.
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
    fn the_runtime_line_takes_this_path_loads_its_secrets_then_runs() {
        let line = runtime_command(&install(None), Path::new("/bin/weft-runtime"));
        let path = std::env::var("PATH").unwrap_or_default().replace('\'', "'\\''");
        assert!(line.starts_with(&format!("export PATH='{path}'; set -a; . '/home/u/.local/share/weft/secrets.env'; set +a; exec '/bin/weft-runtime' serve --config")), "{line}");
        assert!(line.ends_with(">> '/home/u/.local/share/weft/runtime.log' 2>&1"));
    }

    #[test]
    fn the_postgres_major_is_read_off_the_image() {
        assert_eq!(postgres_major(), "18");
    }

    /// The answer scripts read: the field names and shapes of the
    /// contract, and back again.
    #[test]
    fn which_answers_one_object_scripts_read() {
        let deploy = Which {
            name: Some("deploy".into()),
            env: "deploy".into(),
            dir: "/h/.local/share/weft/installs/deploy".into(),
            bin: "/h/.local/share/weft/installs/deploy/bin".into(),
            branch: Some("deploy".into()),
            installed: false,
        };
        let wire = serde_json::to_value(&deploy).unwrap();
        assert_eq!(
            wire,
            serde_json::json!({
                "name": "deploy", "env": "deploy", "dir": "/h/.local/share/weft/installs/deploy",
                "bin": "/h/.local/share/weft/installs/deploy/bin", "branch": "deploy", "installed": false,
            })
        );
        assert_eq!(serde_json::from_value::<Which>(wire).unwrap(), deploy);
        let default = Which::of(&Wanted { install: weft_core::infra::Install::default_install(), from: WantedFrom::Outside });
        let wire = serde_json::to_value(&default).unwrap();
        assert_eq!((wire["name"].clone(), wire["env"].clone(), wire["branch"].clone()), (serde_json::Value::Null, "default".into(), serde_json::Value::Null));
        assert_eq!(default.bin, default.dir.join("bin"));

        let from = WantedFrom::Checkout { root: "/w/deploy".into(), branch: "deploy".into() };
        let lines = deploy.lines(&from);
        assert!(lines[1].contains("branch deploy") && lines[3].contains("run ./setup.sh in /w/deploy"), "{lines:?}");
    }

    #[test]
    fn every_install_is_a_folder_under_installs_or_the_default_one_once_it_wrote_a_file() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path();
        assert!(Install::every_on(data).unwrap().is_empty(), "a machine with no weft has no install");
        for folder in ["installs/zeta", "installs/alpha", "installs/Bad"] {
            std::fs::create_dir_all(data.join(folder)).unwrap();
        }
        std::fs::write(data.join("installs/zeta").join(CONFIG_FILE), "{}").unwrap();
        std::fs::write(data.join("installs/beta"), "a file, not a folder").unwrap();
        let names = |found: Vec<Install>| found.into_iter().map(|i| (i.id.label_value().to_string(), i.dir)).collect::<Vec<_>>();
        assert_eq!(
            names(Install::every_on(data).unwrap()),
            vec![("alpha".into(), data.join("installs/alpha")), ("zeta".into(), data.join("installs/zeta"))],
            "a folder whose first start has not written anything yet is an install already"
        );
        std::fs::write(data.join(weft_core::images::IMAGE_REFS_FILE), "{}").unwrap();
        assert_eq!(names(Install::every_on(data).unwrap())[0], ("default".into(), data.to_path_buf()), "the default install, by its record");
        std::fs::remove_file(data.join(weft_core::images::IMAGE_REFS_FILE)).unwrap();
        std::fs::write(data.join(CONFIG_FILE), "{}").unwrap();
        assert_eq!(names(Install::every_on(data).unwrap())[0], ("default".into(), data.to_path_buf()), "the default install, by its config");
    }

    /// The protection against two installs on one tunnel: another
    /// install whose `tunnel.env` carries the same token and whose tunnel
    /// container exists names it; one whose container is gone is stale
    /// and blocks nothing; another token, or a token that only starts the
    /// same, matches nothing.
    #[test]
    fn another_install_running_the_same_tunnel_token_is_found() {
        let other = |name: Option<&str>, env: String, container: bool| OtherTunnel { install: install(name), token: tunnel_record_of(&env).token, container };
        let others = [
            other(Some("deploy"), tunnel_env_line("tok-b"), true),
            other(None, format!("# kept by weft\n{}", tunnel_env_line("tok-a")), true),
            other(Some("gone"), tunnel_env_line("tok-a"), false),
            other(Some("old"), tunnel_env_line("tok-c"), false),
            other(Some("kept"), tunnel_record(&NamedTunnel { token: "tok-d".into(), hostname: "https://d.example.com".into() }), true),
        ];
        let label = |i: Option<&Install>| i.map(|i| i.id.label_value().to_string());
        let labels = |v: Vec<&Install>| v.into_iter().map(|i| i.id.label_value().to_string()).collect::<Vec<_>>();
        let a = tunnel_token_holders("tok-a", &others);
        assert_eq!((label(a.running), labels(a.stale)), (Some("default".into()), vec!["gone".to_string()]));
        assert_eq!(label(tunnel_token_holders("tok-b", &others).running), Some("deploy".into()));
        let c = tunnel_token_holders("tok-c", &others);
        assert_eq!((label(c.running), labels(c.stale)), (None, vec!["old".to_string()]), "a file whose container is gone blocks nothing");
        assert_eq!(label(tunnel_token_holders("tok-d", &others).running), Some("kept".into()), "a whole record holds its token too");
        assert_eq!(tunnel_token_holders("tok", &others), TokenHolders { running: None, stale: vec![] });
        assert_eq!(tunnel_token_holders("tok-a", &[]), TokenHolders { running: None, stale: vec![] });
    }

    /// The network address is an IP of the machine: set, it wins and
    /// is checked; `off` clears it; unset, the last start's is kept, read
    /// back off the network url it wrote, and checked the same way.
    #[test]
    fn a_network_address_is_an_ip_kept_across_starts() {
        for good in ["192.168.1.20", "fd00::1"] {
            assert_eq!(check_network_address(good).unwrap(), good.parse::<std::net::IpAddr>().unwrap());
        }
        for bad in ["http://192.168.1.20", "192.168.1.20:14112", "box/weft", "my-laptop.local", ""] {
            let e = check_network_address(bad).unwrap_err().to_string();
            assert!(e.contains("WEFT_NETWORK_ADDRESS"), "{bad}: {e}");
        }
        let ip = |s: &str| s.parse::<std::net::IpAddr>().unwrap();
        let kept = Some("http://192.168.1.20:14112");
        assert_eq!(network_address(None, kept).unwrap(), Some(ip("192.168.1.20")));
        assert_eq!(network_address(Some(" "), kept).unwrap(), Some(ip("192.168.1.20")), "empty is unset");
        assert_eq!(network_address(Some("off"), kept).unwrap(), None);
        assert_eq!(network_address(Some("10.0.0.2"), kept).unwrap(), Some(ip("10.0.0.2")));
        assert!(network_address(Some("box:1"), kept).is_err());
        assert_eq!(network_address(None, None).unwrap(), None);
        let v6 = network_url(network_listen(ip("fd00::1"), 14112));
        assert_eq!(v6, "http://[fd00::1]:14112");
        assert_eq!(network_address(None, Some(&v6)).unwrap(), Some(ip("fd00::1")));
        assert_eq!(network_address(Some("::ffff:192.168.1.20"), None).unwrap(), Some(ip("192.168.1.20")), "read as the IPv4 it is");
        // No other device reaches these: refused, naming where they came from.
        for unreachable in ["127.0.0.1", "::1", "0.0.0.0", "::", "fe80::1", "::ffff:127.0.0.1", "::ffff:0.0.0.0"] {
            let e = network_address(Some(unreachable), None).unwrap_err().to_string();
            assert!(e.contains(&format!("WEFT_NETWORK_ADDRESS='{unreachable}'")), "{unreachable}: {e}");
        }
        let e = network_address(None, Some("http://127.0.0.1:14112")).unwrap_err().to_string();
        assert!(e.contains("config.json"), "a kept one too: {e}");
        assert!(network_address(None, Some("not a url")).is_err(), "an unparsable kept one too");
    }

    /// An address this machine has passes the probe; one it does not
    /// (TEST-NET-3, never assigned) is refused before anything starts.
    #[test]
    fn a_network_address_this_machine_lacks_is_refused() {
        refuse_a_missing_network_address("127.0.0.1".parse().unwrap()).unwrap();
        let e = refuse_a_missing_network_address("203.0.113.7".parse().unwrap()).unwrap_err().to_string();
        assert!(e.contains("has no network address 203.0.113.7") && e.contains("WEFT_NETWORK_ADDRESS"), "{e}");
    }

    /// Only the outside doors go on the network, on a listener of their
    /// own: the tunnel's port stays on loopback, and so does the public
    /// port, which carries the management API.
    #[test]
    fn a_network_address_opens_only_the_outside_doors() {
        let ports = Ports { public: 14111, internal: 14113, outside: 14112, postgres: 14114, tunnel_metrics: 14117 };
        let previous = Levers { network_url: Some("http://192.168.1.20:1".into()), ..Levers::default() };
        let config = install_config(&install(None), ports, "weft-runtime:x".into(), None, previous).unwrap();
        assert_eq!(listen_of(&config).outside, Some(([127, 0, 0, 1], 14112).into()));
        assert_eq!(listen_of(&config).network, Some(([192, 168, 1, 20], 14112).into()));
        assert_eq!(listen_of(&config).public, ([127, 0, 0, 1], 14111).into());
        assert_eq!(local_of(&config).network_url.as_deref(), Some("http://192.168.1.20:14112"), "on the outside port as it is now");
        assert_eq!(config.own_base_url(), "http://192.168.1.20:14112");
        let none = install_config(&install(None), ports, "weft-runtime:x".into(), None, Levers::default()).unwrap();
        assert_eq!(listen_of(&none).network, None);
    }

    /// The machine asks a quick tunnel's metrics on loopback: on the
    /// host's network cloudflared listens there itself; on Docker's bridge
    /// it listens inside its container and Docker publishes the port to
    /// loopback only.
    #[test]
    fn the_quick_tunnel_metrics_answer_on_loopback() {
        let host = QuickTunnelMetrics::on(14117, true);
        assert_eq!((host.docker_args.is_empty(), host.bind.as_str(), host.url.as_str()), (true, "127.0.0.1:14117", "http://127.0.0.1:14117"));
        let bridge = QuickTunnelMetrics::on(14117, false);
        assert_eq!(bridge.docker_args, vec!["-p".to_string(), "127.0.0.1:14117:14117".to_string()]);
        assert_eq!((bridge.bind.as_str(), bridge.url.as_str()), ("0.0.0.0:14117", "http://127.0.0.1:14117"));
    }

    /// A ports.json an older weft wrote has no metrics port: a named
    /// install gets a free port of its own, saved and kept by the next
    /// start, and the default install's moves with its variable.
    #[test]
    fn an_older_ports_file_gets_a_tunnel_metrics_port() {
        let dir = tempfile::tempdir().unwrap();
        let old = r#"{"public":15000,"internal":15001,"outside":15002,"postgres":15003}"#;
        std::fs::write(Ports::path(dir.path()), old).unwrap();
        let named = Install { id: install(Some("cell3")).id, dir: dir.path().to_path_buf() };
        let picked = ports(&named).unwrap();
        assert!(![15000, 15001, 15002, 15003].contains(&picked.tunnel_metrics));
        assert!(ports::SavedPorts::read(dir.path()).unwrap().unwrap().says(&picked), "saved for the next start");
        assert_eq!(ports(&named).unwrap(), picked, "and kept by it");
        let moved = moved_by_env(Ports::DEFAULT, |n| (n == "WEFT_TUNNEL_METRICS_PORT").then(|| "15117".to_string())).unwrap();
        assert_eq!(moved.tunnel_metrics, 15117);
    }

    /// The named tunnel a start opens: the environment's when it sets
    /// both (over any record), the recorded one when it sets neither, a
    /// quick tunnel when there is neither. One variable alone is refused,
    /// and so is a record missing either setting, naming the file.
    #[test]
    fn a_start_opens_the_named_tunnel_the_environment_sets_else_the_recorded_one() {
        let record = Path::new("/i/tunnel.env");
        let set = |token: Option<&str>, hostname: Option<&str>| TunnelSettings {
            token: token.map(str::to_string),
            hostname: hostname.map(str::to_string),
        };
        let tunnel = |token: &str, hostname: &str| Some(NamedTunnel { token: token.into(), hostname: hostname.into() });
        let kept = Some(set(Some("tok-kept"), Some("https://kept.example.com")));
        let env = set(Some("tok-env"), Some("https://env.example.com/"));
        assert_eq!(named_tunnel(env.clone(), kept.clone(), record).unwrap(), tunnel("tok-env", "https://env.example.com"), "the environment wins");
        assert_eq!(named_tunnel(env, None, record).unwrap(), tunnel("tok-env", "https://env.example.com"));
        assert_eq!(named_tunnel(TunnelSettings::default(), kept, record).unwrap(), tunnel("tok-kept", "https://kept.example.com"));
        assert_eq!(named_tunnel(TunnelSettings::default(), None, record).unwrap(), None, "a quick tunnel");
        for alone in [set(Some("tok-env"), None), set(None, Some("https://env.example.com"))] {
            let e = named_tunnel(alone.clone(), None, record).unwrap_err().to_string();
            assert!(e.contains("set both"), "{alone:?}: {e}");
        }
        let older = named_tunnel(TunnelSettings::default(), Some(set(Some("tok-kept"), None)), record).unwrap_err().to_string();
        assert!(older.contains("/i/tunnel.env") && older.contains("without its hostname") && older.contains("./setup.sh --public-url"), "{older}");
        let no_token = named_tunnel(TunnelSettings::default(), Some(set(None, Some("https://kept.example.com"))), record).unwrap_err().to_string();
        assert!(no_token.contains("/i/tunnel.env") && no_token.contains("TUNNEL_TOKEN"), "{no_token}");
        let bad = named_tunnel(TunnelSettings::default(), Some(set(Some("tok-kept"), Some("http://kept.example.com"))), record).unwrap_err();
        assert!(format!("{bad:#}").contains("/i/tunnel.env"), "{bad:#}");
    }

    /// The record a named tunnel leaves reads back as what was written,
    /// only its owner may read it (it holds the token), and the token's
    /// line is the one the container and the shared-tunnel check read. No
    /// file is no record; one that cannot be read is an error naming it.
    #[test]
    fn the_tunnel_record_reads_back_what_a_start_wrote() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tunnel.env");
        assert_eq!(read_tunnel_record(&path).unwrap(), None);
        let tunnel = NamedTunnel { token: "eyJh=b==".into(), hostname: "https://weft.example.com".into() };
        write_private(&path, &tunnel_record(&tunnel)).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
        let body = std::fs::read_to_string(&path).unwrap();
        assert!(body.lines().any(|l| l == tunnel_env_line("eyJh=b==").trim()), "{body}");
        let read = read_tunnel_record(&path).unwrap();
        assert_eq!(read, Some(TunnelSettings { token: Some(tunnel.token.clone()), hostname: Some(tunnel.hostname.clone()) }));
        assert_eq!(named_tunnel(TunnelSettings::default(), read, &path).unwrap(), Some(tunnel));
        let older = dir.path().join("older.env");
        std::fs::write(&older, tunnel_env_line("tok")).unwrap();
        assert_eq!(read_tunnel_record(&older).unwrap(), Some(TunnelSettings { token: Some("tok".into()), hostname: None }));
        let unreadable = dir.path().join("a-folder");
        std::fs::create_dir(&unreadable).unwrap();
        let e = read_tunnel_record(&unreadable).unwrap_err().to_string();
        assert!(e.contains("a-folder"), "{e}");
    }

    #[test]
    fn a_named_tunnel_hostname_is_the_bare_https_host() {
        assert_eq!(canonical_tunnel_hostname("https://weft.example.com").unwrap(), "https://weft.example.com");
        assert!(canonical_tunnel_hostname("http://weft.example.com").is_err());
        assert!(canonical_tunnel_hostname("https://weft.example.com/x").is_err());
        assert!(canonical_tunnel_hostname("https://weft.example.com:8443").is_err());
    }
}
