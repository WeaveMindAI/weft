//! Every fixed port weft picks on a machine, in one place.
//!
//! They sit in 14111-14118 ([`BLOCK`]), a block chosen from data: unassigned at IANA,
//! never seen open in nmap's frequency tables, and absent from the
//! Prometheus port list and the default lists of the usual dev tools, so a
//! fresh machine almost never has one of them taken. The default install
//! starts on them; setting `WEFT_PUBLIC_PORT`, `WEFT_OUTSIDE_PORT`,
//! `WEFT_INTERNAL_PORT`, `WEFT_POSTGRES_PORT` or `WEFT_TUNNEL_METRICS_PORT`
//! on a `weft daemon start`
//! moves that port, and every install keeps the ports it runs on in its
//! `ports.json` ([`InstallPorts`]), which is where the CLI looks for it.
//! `WEFT_SEAWEED_PORT` moves the object store, which all installs share.
//!
//! SYNC: these numbers <-> extension-browser/src/entrypoints/popup/App.svelte (the bare-token
//! default), and the prose that names them: docs/src/**,
//! tangle/*/ (skills and personas), README files, CHANGELOG.md.

/// The API and dashboard of the default install (`WEFT_PUBLIC_PORT`).
pub const PUBLIC: u16 = 14111;
/// The doors outside callers use, the port a tunnel forwards to
/// (`WEFT_OUTSIDE_PORT`).
pub const OUTSIDE: u16 = 14112;
/// The port weft's own roles, workers and containers call it on
/// (`WEFT_INTERNAL_PORT`; on GCP the machine's private port).
pub const INTERNAL: u16 = 14113;
/// The default install's Postgres, on the host (`WEFT_POSTGRES_PORT`);
/// inside its container Postgres keeps 5432.
pub const POSTGRES: u16 = 14114;
/// The object store, on the host (`WEFT_SEAWEED_PORT`); inside its
/// container it keeps 8333.
pub const OBJECT_STORE: u16 = 14115;
/// The agent beside every infra unit. On a GCP unit machine a user's
/// container publishes its ports as is, so the agent must stay off any
/// port a user's image is likely to use.
pub const UNIT_AGENT: u16 = 14116;
/// Where the default install's quick tunnel says its current address
/// (cloudflared's metrics server, on loopback;
/// `WEFT_TUNNEL_METRICS_PORT`).
pub const TUNNEL_METRICS: u16 = 14117;

/// The block weft keeps for its fixed ports, one spare after the last:
/// what a Windows machine reserves for weft, and what a named install's
/// ports ([`NAMED`]) stay out of. The test below keeps every
/// constant inside it.
pub const BLOCK: std::ops::RangeInclusive<u16> = PUBLIC..=PUBLIC + 7;

/// The block the default install gives its projects their own ports
/// from, one each, both ends included (`projectPorts` in its config). An
/// install beside it takes free ports instead, since this block is the
/// default install's.
pub const PROJECTS: (u16, u16) = (14200, 14399);

/// Where the default install answers on this machine until it is started
/// on another port. A literal because a `&str` const cannot be formatted;
/// the test below keeps it on [`PUBLIC`].
pub const LOCAL_PUBLIC_URL: &str = "http://127.0.0.1:14111";

/// The host ports one install listens on, saved in its `ports.json` by
/// `weft daemon start` and read back ([`SavedPorts`]) by every command
/// that talks to it.
/// Saved for every install, the default one included, so a port moved
/// once stays moved: the address is the file's, never whatever the
/// environment of the latest command happens to say.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InstallPorts {
    pub public: u16,
    pub internal: u16,
    pub outside: u16,
    pub postgres: u16,
    /// [`TUNNEL_METRICS`] for this install.
    #[serde(rename = "tunnelMetrics")]
    pub tunnel_metrics: u16,
}

/// A `ports.json` as it is on disk, read and never written back: a file
/// an older weft wrote, before the quick tunnel had a metrics port, has
/// none until the next `weft daemon start` gives it one
/// ([`SavedPorts::to_start_on`]). Every other reader needs only
/// [`SavedPorts::public`], so none of them ever writes the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
pub struct SavedPorts {
    pub public: u16,
    internal: u16,
    outside: u16,
    postgres: u16,
    #[serde(default, rename = "tunnelMetrics")]
    tunnel_metrics: Option<u16>,
}

impl SavedPorts {
    /// The ports `install` saved in `dir`, `None` when it was never
    /// started. A file that exists and cannot be read is an error, never
    /// a silent return to the defaults.
    pub fn read(dir: &std::path::Path) -> Result<Option<Self>, String> {
        let path = InstallPorts::path(dir);
        match std::fs::read_to_string(&path) {
            Ok(raw) => serde_json::from_str(&raw).map(Some).map_err(|e| format!("{} is not valid: {e}", path.display())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(format!("cannot read {}: {e}", path.display())),
        }
    }

    /// The ports a start of `install` runs on: the file's, with a metrics
    /// port given to a file that has none (the block's for the default
    /// install, a free one, [`PortsTurn::free_ports`], for a named one).
    /// Only `weft daemon start` calls this, holding `turn`, and it saves
    /// what it runs on before letting go, so the port picked here is the
    /// one its tunnel gets and every later read finds.
    #[cfg(feature = "runtime")]
    pub fn to_start_on(self, install: &crate::infra::Install, turn: &PortsTurn) -> Result<InstallPorts, String> {
        let Self { public, internal, outside, postgres, tunnel_metrics } = self;
        let tunnel_metrics = match (tunnel_metrics, install.name()) {
            (Some(port), _) => port,
            (None, None) => TUNNEL_METRICS,
            (None, Some(_)) => {
                let [port] = turn.free_ports(&[public, internal, outside, postgres])?;
                port
            }
        };
        Ok(InstallPorts { public, internal, outside, postgres, tunnel_metrics })
    }

    /// Whether the file already says exactly `ports`, so a start on them
    /// has nothing to save.
    pub fn says(&self, ports: &InstallPorts) -> bool {
        self.tunnel_metrics == Some(ports.tunnel_metrics)
            && (self.public, self.internal, self.outside, self.postgres) == (ports.public, ports.internal, ports.outside, ports.postgres)
    }

    /// Every port the file names.
    #[cfg(feature = "runtime")]
    fn all(&self) -> impl Iterator<Item = u16> {
        [self.public, self.internal, self.outside, self.postgres].into_iter().chain(self.tunnel_metrics)
    }
}

/// The file an install keeps its ports in, in its dir.
const PORTS_FILE: &str = "ports.json";

/// One of an infra unit's ports, as the unit names it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum UnitPort {
    /// A process's port, by its name (`infra::ProcessPort`).
    Process(String),
    /// A container port, by its number: a unit's containers share one
    /// network, so a number is one port of the whole unit.
    Container(u16),
}

/// The host port a local install keeps for one port of one of its infra
/// units, from the unit's first start until it is removed or its copy
/// terminated. Every one an install keeps is in its `infra-ports.json`,
/// written by its runtime (`weft_platform_local::kept_ports`), and taken
/// for every port any install on the machine picks after
/// ([`PortsTurn::free_ports`]).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct KeptInfraPort {
    pub copy_id: String,
    pub unit: String,
    pub port: UnitPort,
    pub host: u16,
}

/// The file an install keeps its infra units' ports in, in its dir.
const INFRA_PORTS_FILE: &str = "infra-ports.json";

impl KeptInfraPort {
    /// Where an install whose files live in `dir` keeps them.
    pub fn path(dir: &std::path::Path) -> std::path::PathBuf {
        dir.join(INFRA_PORTS_FILE)
    }

    /// Every port the install in `dir` keeps; none when it kept none yet.
    /// A file that exists and cannot be read is an error.
    pub fn read(dir: &std::path::Path) -> Result<Vec<Self>, String> {
        let path = Self::path(dir);
        match std::fs::read(&path) {
            Ok(raw) => serde_json::from_slice(&raw).map_err(|e| {
                format!(
                    "{} is not the list of infra ports weft keeps ({e}); delete it, and every infra started again gets new ports",
                    path.display()
                )
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(format!("cannot read {}: {e}", path.display())),
        }
    }

    /// `all`, as every port the install in `dir` keeps, written whole
    /// ([`crate::infra::write_whole`]): another install's start reads the
    /// file while it picks its ports.
    pub fn save(dir: &std::path::Path, all: &[Self]) -> std::io::Result<()> {
        crate::infra::write_whole(dir, INFRA_PORTS_FILE, &serde_json::to_vec_pretty(all)?, 0o644)
    }
}

impl InstallPorts {
    /// The block's ports: what the default install starts on.
    pub const DEFAULT: Self =
        Self { public: PUBLIC, internal: INTERNAL, outside: OUTSIDE, postgres: POSTGRES, tunnel_metrics: TUNNEL_METRICS };

    /// Where an install whose files live in `dir` keeps its ports.
    pub fn path(dir: &std::path::Path) -> std::path::PathBuf {
        dir.join(PORTS_FILE)
    }

    /// Written whole ([`crate::infra::write_whole`]): the editor's watcher,
    /// the CLI and setup.sh read this file while an install starts.
    pub fn save(&self, dir: &std::path::Path) -> std::io::Result<()> {
        crate::infra::write_whole(dir, PORTS_FILE, &serde_json::to_vec(self)?, 0o644)
    }

    /// The address the API and dashboard answer on.
    pub fn public_url(&self) -> String {
        public_url_on(self.public)
    }
}

/// The address the API and dashboard answer on when they listen on `port`.
fn public_url_on(port: u16) -> String {
    format!("http://127.0.0.1:{port}")
}

/// Where a named install's ports come from: below the range the
/// operating system hands out for outgoing connections and for a bind to
/// port 0 (from 32768 on Linux, 49152 on macOS and Windows by default),
/// and above weft's own block and [`PROJECTS`]. A port in that range is
/// free only until the next program on the machine connects anywhere: an
/// install handed one once found it taken the few seconds later its
/// runtime came to bind it, by something the machine could not even name.
/// Ports here are handed out only by programs that pick them on purpose,
/// so one free when it is picked stays free.
pub const NAMED: std::ops::RangeInclusive<u16> = 15000..=32767;

/// The turn to hand out ports on this machine: one `weft daemon start` at
/// a time picks ports and saves them in its install's `ports.json` while
/// it holds it, so two installs set up at once (test cells, two branches)
/// can never both pick a port the other has not saved yet. A lock on a
/// file in weft's data dir ([`crate::file_lock`]), let go on drop, a
/// crash included.
#[cfg(feature = "runtime")]
pub struct PortsTurn {
    /// The folder whose `ports.json` files, and whose `installs/*/ports.json`
    /// files, say which ports are taken: weft's data dir.
    root: std::path::PathBuf,
    _lock: std::fs::File,
}

#[cfg(feature = "runtime")]
impl PortsTurn {
    /// Wait for the turn on this machine.
    pub fn take() -> Result<Self, String> {
        Self::take_in(&crate::infra::data_dir())
    }

    /// Wait for the turn among the installs under `root`.
    fn take_in(root: &std::path::Path) -> Result<Self, String> {
        let path = root.join("ports.lock");
        let lock = crate::file_lock::hold(&path, || eprintln!("another weft daemon start is picking its ports; waiting for it"))
            .map_err(|e| format!("lock {}: {e}", path.display()))?;
        Ok(Self { root: root.to_path_buf(), _lock: lock })
    }

    /// `N` free ports, all different, for an install beside the default
    /// one or for an infra unit's port ([`KeptInfraPort`]), from [`NAMED`]
    /// minus whatever part of it this machine hands out on its own. None
    /// is in `besides`, saved in the `ports.json` of any install on this
    /// machine (a stopped install's ports are free right now only because
    /// its runtime is down, and two installs given the same one could
    /// never run at once), or kept in its `infra-ports.json` (the same for
    /// a stopped unit's), and each binds right now both on loopback and on
    /// every address, since an install listens on either. The caller saves
    /// them before it lets go of the turn.
    pub fn free_ports<const N: usize>(&self, besides: &[u16]) -> Result<[u16; N], String> {
        let taken: std::collections::HashSet<u16> = saved_under(&self.root)?.into_iter().chain(besides.iter().copied()).collect();
        let os = os_handed_out()?;
        let candidates: Vec<u16> = NAMED.filter(|p| !os.contains(p) && !taken.contains(p)).collect();
        if candidates.is_empty() {
            return Err(format!(
                "no port left for this install: weft gives named installs ports from {}-{}, and this machine hands out {}-{} to every program on its own (`ip_local_port_range` on Linux, `net.inet.ip.portrange` on macOS); move that range up to free some",
                NAMED.start(),
                NAMED.end(),
                os.start(),
                os.end()
            ));
        }
        // A random place to start, so the installs on a machine spread
        // out instead of all asking for the same first ports.
        let start = (uuid::Uuid::new_v4().as_u128() % candidates.len() as u128) as usize;
        let picked: Vec<u16> = candidates[start..].iter().chain(&candidates[..start]).copied().filter(|&p| binds_now(p)).take(N).collect();
        picked.try_into().map_err(|picked: Vec<u16>| {
            format!("only {} of the {N} ports this install needs are free in {}-{}", picked.len(), NAMED.start(), NAMED.end())
        })
    }
}

/// Whether nothing on this machine holds `port` on loopback or on any
/// address. Asked one after the other and let go at once: whoever else
/// could pick the port waits for the turn this caller holds.
#[cfg(feature = "runtime")]
fn binds_now(port: u16) -> bool {
    std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() && std::net::TcpListener::bind(("0.0.0.0", port)).is_ok()
}

/// The ports this machine hands out on its own, to outgoing connections
/// and binds to port 0.
#[cfg(all(feature = "runtime", target_os = "linux"))]
fn os_handed_out() -> Result<std::ops::RangeInclusive<u16>, String> {
    const PATH: &str = "/proc/sys/net/ipv4/ip_local_port_range";
    let raw = std::fs::read_to_string(PATH).map_err(|e| format!("cannot read {PATH}: {e}"))?;
    parse_range(&raw).ok_or_else(|| format!("{PATH} says '{}', not two ports", raw.trim()))
}

/// The ports this machine hands out on its own, to outgoing connections
/// and binds to port 0.
#[cfg(all(feature = "runtime", not(target_os = "linux")))]
fn os_handed_out() -> Result<std::ops::RangeInclusive<u16>, String> {
    let out = std::process::Command::new("sysctl")
        .args(["-n", "net.inet.ip.portrange.first", "net.inet.ip.portrange.last"])
        .output()
        .map_err(|e| format!("cannot run sysctl to read this machine's port range: {e}"))?;
    let raw = String::from_utf8_lossy(&out.stdout);
    parse_range(&raw).ok_or_else(|| format!("sysctl net.inet.ip.portrange says '{}', not two ports", raw.trim()))
}

/// Two ports separated by white space, as both `ip_local_port_range` and
/// `sysctl -n` print them.
#[cfg(feature = "runtime")]
fn parse_range(raw: &str) -> Option<std::ops::RangeInclusive<u16>> {
    let mut nums = raw.split_whitespace().map(str::parse::<u16>);
    match (nums.next(), nums.next(), nums.next()) {
        (Some(Ok(first)), Some(Ok(last)), None) => Some(first..=last),
        _ => None,
    }
}

/// Every port a `ports.json` or an `infra-ports.json` under `root` saves:
/// the default install's, in weft's data dir, and each named one's under
/// `installs/`.
#[cfg(feature = "runtime")]
fn saved_under(root: &std::path::Path) -> Result<Vec<u16>, String> {
    let installs = root.join("installs");
    let mut dirs = vec![root.to_path_buf()];
    match std::fs::read_dir(&installs) {
        Ok(entries) => {
            for entry in entries {
                dirs.push(entry.map_err(|e| format!("cannot read {}: {e}", installs.display()))?.path());
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(format!("cannot read {}: {e}", installs.display())),
    }
    let mut ports = Vec::new();
    for dir in dirs {
        if let Some(saved) = SavedPorts::read(&dir)? {
            ports.extend(saved.all());
        }
        ports.extend(KeptInfraPort::read(&dir)?.into_iter().map(|k| k.host));
    }
    Ok(ports)
}

/// Where the install this process acts on ([`Install::from_env`])
/// answers on this machine: the public port it saved. A default install
/// never started answers nowhere yet, and its first start takes
/// [`PUBLIC`] unless told otherwise, so that is its address until then;
/// a named install never started has no port at all, and saying so beats
/// pointing at the default install's.
///
/// [`Install::from_env`]: crate::infra::Install::from_env
pub fn local_public_url() -> Result<String, String> {
    local_public_url_of(&crate::infra::Install::from_env()?)
}

/// Where `install` answers on this machine, the way [`local_public_url`]
/// reads it for the install this process acts on.
pub fn local_public_url_of(install: &crate::infra::Install) -> Result<String, String> {
    local_public_url_in(install, &install.dir())
}

fn local_public_url_in(install: &crate::infra::Install, dir: &std::path::Path) -> Result<String, String> {
    match (SavedPorts::read(dir)?, install.name()) {
        (Some(saved), _) => Ok(public_url_on(saved.public)),
        (None, None) => Ok(LOCAL_PUBLIC_URL.to_string()),
        (None, Some(name)) => Err(format!(
            "install '{name}' has no ports yet ({} does not exist); start it with `weft daemon start`",
            InstallPorts::path(dir).display()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_public_url_is_on_the_public_port() {
        assert_eq!(LOCAL_PUBLIC_URL, format!("http://127.0.0.1:{PUBLIC}"));
    }

    #[test]
    fn the_local_url_is_the_saved_public_port_once_there_is_one() {
        use crate::infra::Install;
        let dir = std::env::temp_dir().join(format!("weft-ports-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let default = Install::default_install();
        let root = tempfile::tempdir().unwrap();
        let turn = PortsTurn::take_in(root.path()).unwrap();
        assert_eq!(local_public_url_in(&default, &dir).unwrap(), LOCAL_PUBLIC_URL);
        let named = Install::named("cell1").unwrap();
        assert!(local_public_url_in(&named, &dir).unwrap_err().contains("weft daemon start"));
        let moved = InstallPorts { public: 15000, ..InstallPorts::DEFAULT };
        moved.save(&dir).unwrap();
        let saved = SavedPorts::read(&dir).unwrap().unwrap();
        assert!(saved.says(&moved));
        assert_eq!(saved.to_start_on(&default, &turn).unwrap(), moved, "a full file starts on what it says");
        // The partial file it wrote first was renamed over ports.json.
        let files: Vec<_> = std::fs::read_dir(&dir).unwrap().map(|e| e.unwrap().file_name()).collect();
        assert_eq!(files, vec![std::ffi::OsString::from("ports.json")]);
        assert_eq!(local_public_url_in(&default, &dir).unwrap(), "http://127.0.0.1:15000");
        assert_eq!(local_public_url_in(&named, &dir).unwrap(), "http://127.0.0.1:15000");
        // A file an older weft wrote, before the tunnel's metrics port, is
        // read as it is: only a start gives it a port (the block's on the
        // default install, a free one of its own on a named one), and the
        // start saves it.
        let old = r#"{"public":15000,"internal":15001,"outside":15002,"postgres":15003}"#;
        std::fs::write(InstallPorts::path(&dir), old).unwrap();
        assert_eq!(local_public_url_in(&named, &dir).unwrap(), "http://127.0.0.1:15000");
        assert_eq!(std::fs::read_to_string(InstallPorts::path(&dir)).unwrap(), old, "a read writes nothing");
        let saved = SavedPorts::read(&dir).unwrap().unwrap();
        let filled = saved.to_start_on(&default, &turn).unwrap();
        assert_eq!(filled, InstallPorts { public: 15000, internal: 15001, outside: 15002, postgres: 15003, tunnel_metrics: TUNNEL_METRICS });
        assert!(!saved.says(&filled), "so the start saves it");
        let port = saved.to_start_on(&named, &turn).unwrap().tunnel_metrics;
        assert!(![15000, 15001, 15002, 15003].contains(&port) && !BLOCK.contains(&port));
        std::fs::write(InstallPorts::path(&dir), "{").unwrap();
        assert!(SavedPorts::read(&dir).unwrap_err().contains("is not valid"));
        assert!(local_public_url_in(&default, &dir).unwrap_err().contains("is not valid"));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn free_ports_are_distinct_saved_ones_are_skipped_and_none_is_the_machines_own() {
        let root = tempfile::tempdir().unwrap();
        let turn = PortsTurn::take_in(root.path()).unwrap();
        let [a, b, c] = turn.free_ports(&[]).unwrap();
        assert!(a != b && b != c && a != c);
        let os = os_handed_out().unwrap();
        assert!([a, b, c].iter().all(|p| NAMED.contains(p) && !os.contains(p)));
        let [d] = turn.free_ports(&[a, b, c]).unwrap();
        assert!(![a, b, c].contains(&d));
        InstallPorts { public: a, internal: b, outside: c, postgres: d, tunnel_metrics: d }.save(&root.path().join("installs/x")).unwrap();
        let again: [u16; 8] = turn.free_ports(&[]).unwrap();
        assert!(again.iter().all(|p| ![a, b, c, d].contains(p)), "a port saved by an install is never handed out again");
    }

    /// Two installs set up at once (test cells started side by side, two
    /// branches) each pick their ports and save them while holding the
    /// turn, so none of them can be handed a port another one picked.
    #[test]
    fn installs_set_up_at_once_never_share_a_port() {
        let root = tempfile::tempdir().unwrap();
        let picks: Vec<[u16; 5]> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..16)
                .map(|i| {
                    let root = root.path();
                    s.spawn(move || {
                        let turn = PortsTurn::take_in(root).unwrap();
                        let [public, internal, outside, postgres, tunnel_metrics] = turn.free_ports(&[]).unwrap();
                        let ports = InstallPorts { public, internal, outside, postgres, tunnel_metrics };
                        ports.save(&root.join(format!("installs/cell{i}"))).unwrap();
                        [public, internal, outside, postgres, tunnel_metrics]
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let mut all: Vec<u16> = picks.concat();
        let n = all.len();
        all.sort();
        all.dedup();
        assert_eq!(all.len(), n, "a port went to two installs: {picks:?}");
    }

    /// A port an install keeps for one of its infra units is taken for
    /// every pick on the machine, its own install's and every other's, as
    /// much as the ports an install saved for itself.
    #[test]
    fn a_port_an_install_keeps_for_its_infra_is_never_handed_out() {
        let root = tempfile::tempdir().unwrap();
        let turn = PortsTurn::take_in(root.path()).unwrap();
        let [a, b] = turn.free_ports(&[]).unwrap();
        let kept = |host, copy: &str| KeptInfraPort { copy_id: copy.into(), unit: "main".into(), port: UnitPort::Container(5432), host };
        KeptInfraPort::save(root.path(), &[kept(a, "wn-1")]).unwrap();
        KeptInfraPort::save(&root.path().join("installs/x"), &[kept(b, "wn-2")]).unwrap();
        assert_eq!(KeptInfraPort::read(&root.path().join("installs/x")).unwrap(), [kept(b, "wn-2")]);
        let taken = saved_under(root.path()).unwrap();
        assert!(taken.contains(&a) && taken.contains(&b), "{taken:?}");
        let again: [u16; 8] = turn.free_ports(&[]).unwrap();
        assert!(again.iter().all(|p| ![a, b].contains(p)));
        std::fs::write(KeptInfraPort::path(root.path()), "{").unwrap();
        assert!(saved_under(root.path()).unwrap_err().contains("delete it"), "a file weft cannot read stops the pick");
    }

    #[test]
    fn the_machines_port_range_is_read_as_two_ports() {
        assert_eq!(parse_range("32768\t60999\n"), Some(32768..=60999));
        assert_eq!(parse_range("49152\n65535\n"), Some(49152..=65535));
        assert_eq!(parse_range("49152"), None);
        assert!(!NAMED.contains(&PUBLIC) && !NAMED.contains(BLOCK.end()) && !NAMED.contains(&PROJECTS.1));
    }

    #[test]
    fn every_port_is_in_the_block_and_distinct() {
        let all = [PUBLIC, OUTSIDE, INTERNAL, POSTGRES, OBJECT_STORE, UNIT_AGENT, TUNNEL_METRICS];
        assert!(all.iter().all(|p| BLOCK.contains(p)));
        let mut sorted = all.to_vec();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), all.len());
    }
}
