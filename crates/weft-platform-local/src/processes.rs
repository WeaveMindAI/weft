//! The machine's processes, as the worker runner starts and stops them.
//!
//! The runner decides which worker to start, with what, and when to stop
//! it; every one of those decisions is tested against the fake here, and
//! only [`MachineProcesses`] touches the operating system.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Stdio;

use async_trait::async_trait;

/// One program to start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spawn {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    /// Its whole environment: nothing of the starter's own is passed on.
    pub env: BTreeMap<String, String>,
    /// Where everything it prints goes, stdout and stderr together,
    /// appended.
    pub log: PathBuf,
}

/// How a process ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exit {
    /// It exited of its own, with this code.
    Code(i32),
    /// A signal ended it (killed from outside, out of memory).
    Signal(i32),
}

/// A process just started.
pub struct Spawned {
    pub pid: u32,
    /// `Some` once it ended.
    pub exit: tokio::sync::watch::Receiver<Option<Exit>>,
}

/// What a process is told.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    /// Stop when you can (`SIGTERM`), to the process alone: it winds its
    /// own work down, the programs it started among it. What is left of
    /// its group once it exited is the caller's to `Kill`, right after it
    /// saw it go, while the group's number is still the process's own.
    Term,
    /// Stop now (`SIGKILL`), to the process and every program it started
    /// (its process group, which [`Processes::spawn`] made its own), so
    /// nothing it ran outlives it.
    Kill,
}

#[async_trait]
pub trait Processes: Send + Sync {
    /// Start `spec` in a process group of its own, so a signal meant for
    /// weft's own process (a Ctrl+C in its terminal) never reaches it.
    fn spawn(&self, spec: &Spawn) -> anyhow::Result<Spawned>;

    /// Send `signal` to `pid`, a process [`Self::spawn`] started (a
    /// `Kill` to its process group with it). A process already gone is no
    /// error.
    fn signal(&self, pid: u32, signal: Signal) -> anyhow::Result<()>;

    /// When the process `pid` started, as a mark two processes that held
    /// the same pid one after the other never share; `None` when no
    /// process has it. What tells a worker an earlier life of weft started
    /// from a program that reused its pid since.
    async fn started(&self, pid: u32) -> anyhow::Result<Option<String>>;

    /// A loopback port nothing listens on right now.
    fn free_port(&self) -> anyhow::Result<u16>;

    /// Whether another program holds the loopback port `port` right now:
    /// binding it there is refused as an address in use.
    fn port_taken(&self, port: u16) -> anyhow::Result<bool>;

    /// Whether any process is left in the process group whose leader was
    /// `pid` (a process [`Self::spawn`] started).
    fn group_left(&self, pid: u32) -> anyhow::Result<bool>;
}

/// How long a killed process group may take to be gone. `SIGKILL` cannot
/// be caught, so a group still there past this is stuck in the kernel,
/// and saying so beats starting its unit again on ports it still holds.
const KILLED_WITHIN: std::time::Duration = std::time::Duration::from_secs(5);

/// Kill the process group of `pid` ([`Signal::Kill`]) and answer once
/// none of it is left, so whatever starts next (the same unit again, on
/// its kept ports) never finds the killed one still holding them.
async fn kill_group(processes: &dyn Processes, pid: u32) -> anyhow::Result<()> {
    processes.signal(pid, Signal::Kill)?;
    let killed = std::time::Instant::now();
    while processes.group_left(pid)? {
        if killed.elapsed() >= KILLED_WITHIN {
            anyhow::bail!(
                "process group {pid} is still there {}s after it was killed; end it by hand with `kill -9 -- -{pid}` (as root, `sudo`, when part of it runs as another user)",
                KILLED_WITHIN.as_secs()
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    Ok(())
}

/// Whether the process `pid` that started at the mark `started`
/// (`Processes::started`) still runs: its pid is held by that process,
/// not by one that took the pid since.
pub async fn runs(processes: &dyn Processes, pid: u32, started: &str) -> anyhow::Result<bool> {
    Ok(processes.started(pid).await?.as_deref() == Some(started))
}

/// Stop the process `pid` (started at the mark `started`) the way a
/// platform stops one: told to stop, given `grace` to exit on its own,
/// then its whole process group killed, whatever it started with it.
/// Answers whether it still ran when asked; one gone already (ended on its
/// own, or a pid another program took since) is sent nothing.
pub async fn stop(processes: &dyn Processes, pid: u32, started: &str, grace: std::time::Duration) -> anyhow::Result<bool> {
    if !runs(processes, pid, started).await? {
        return Ok(false);
    }
    processes.signal(pid, Signal::Term)?;
    let asked = std::time::Instant::now();
    while runs(processes, pid, started).await? && asked.elapsed() < grace {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    // Its process group, whether it exited within the grace or not: a
    // program it started can outlive it. Sent the moment it was seen gone
    // (or still there), so its group's number is still its own: one found
    // long dead above is never sent this.
    kill_group(processes, pid).await?;
    Ok(true)
}

/// Kill what is left of the process group of `pid`, a process
/// [`Processes::spawn`] started (at the mark `started`) that is gone: the
/// programs it started, which keep its group alive after it ended (a
/// child it never took down with it, because something killed it first).
/// Answers whether it sent anything.
///
/// The group's number is the gone process's pid, and the kernel never
/// hands out as a new pid a number a process group still holds. So while
/// no process has the pid, a group by that number can only be what is
/// left of the gone one, and killing it touches nothing else; once a
/// process has the pid again (whatever its mark), that group emptied
/// before the number was reused, nothing of the gone one is left in it,
/// and it is sent nothing. A process that still runs as `started` is not
/// gone, and is sent nothing either: stopping it is [`stop`]'s.
pub async fn kill_leftovers(processes: &dyn Processes, pid: u32, started: &str) -> anyhow::Result<bool> {
    match processes.started(pid).await? {
        None => {
            kill_group(processes, pid).await?;
            Ok(true)
        }
        Some(mark) if mark == started => anyhow::bail!("process {pid} still runs, so what it started is not left over: stop it instead"),
        Some(_) => Ok(false),
    }
}

/// The machine's own processes.
pub struct MachineProcesses;

#[async_trait]
impl Processes for MachineProcesses {
    fn spawn(&self, spec: &Spawn) -> anyhow::Result<Spawned> {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&spec.log)
            .map_err(|e| anyhow::anyhow!("open {}: {e}", spec.log.display()))?;
        let both = log.try_clone().map_err(|e| anyhow::anyhow!("open {} twice: {e}", spec.log.display()))?;
        let mut child = tokio::process::Command::new(&spec.program)
            .args(&spec.args)
            .env_clear()
            .envs(&spec.env)
            .current_dir(&spec.cwd)
            .stdin(Stdio::null())
            .stdout(log)
            .stderr(both)
            .process_group(0)
            .spawn()
            .map_err(|e| anyhow::anyhow!("start {} in {}: {e}", spec.program.display(), spec.cwd.display()))?;
        let pid = child.id().ok_or_else(|| anyhow::anyhow!("{} ended before its pid could be read", spec.program.display()))?;
        let (tx, rx) = tokio::sync::watch::channel(None);
        let program = spec.program.clone();
        // Waited on for as long as it runs, so it never lingers as a zombie
        // and its end is known the moment it happens.
        tokio::spawn(async move {
            let exit = match child.wait().await {
                Ok(status) => exit_of(status),
                Err(e) => {
                    tracing::error!(target: "weft_platform_local::processes", pid, program = %program.display(), error = %e, "could not wait on a worker process");
                    Exit::Code(-1)
                }
            };
            let _ = tx.send(Some(exit));
        });
        Ok(Spawned { pid, exit: rx })
    }

    fn signal(&self, pid: u32, signal: Signal) -> anyhow::Result<()> {
        let pid = libc::pid_t::try_from(pid).map_err(|_| anyhow::anyhow!("pid {pid} is out of range"))?;
        // A negative pid is the process group whose leader `pid` is.
        let (to, number) = match signal {
            Signal::Term => (pid, libc::SIGTERM),
            Signal::Kill => (-pid, libc::SIGKILL),
        };
        // SAFETY: kill(2) takes plain integers and touches no memory of ours.
        if unsafe { libc::kill(to, number) } == 0 {
            return Ok(());
        }
        let e = std::io::Error::last_os_error();
        if e.raw_os_error() == Some(libc::ESRCH) {
            return Ok(());
        }
        anyhow::bail!("signal process {pid} ({signal:?}): {e}")
    }

    async fn started(&self, pid: u32) -> anyhow::Result<Option<String>> {
        started_mark(pid).await
    }

    fn free_port(&self) -> anyhow::Result<u16> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").map_err(|e| anyhow::anyhow!("find a free loopback port: {e}"))?;
        Ok(listener.local_addr()?.port())
    }

    fn port_taken(&self, port: u16) -> anyhow::Result<bool> {
        loopback_taken(port)
    }

    fn group_left(&self, pid: u32) -> anyhow::Result<bool> {
        let group = libc::pid_t::try_from(pid).map_err(|_| anyhow::anyhow!("pid {pid} is out of range"))?;
        // Signal 0 sends nothing and only says whether the group exists.
        // SAFETY: kill(2) takes plain integers and touches no memory of ours.
        if unsafe { libc::kill(-group, 0) } == 0 {
            return Ok(true);
        }
        let e = std::io::Error::last_os_error();
        match e.raw_os_error() {
            Some(libc::ESRCH) => Ok(false),
            // Some of it is there, run by a user weft may not signal.
            Some(libc::EPERM) => Ok(true),
            _ => anyhow::bail!("look for process group {pid}: {e}"),
        }
    }
}

/// Whether the loopback port `port` is held right now: binding it is
/// refused as an address in use. The bind is let go at once.
fn loopback_taken(port: u16) -> anyhow::Result<bool> {
    match std::net::TcpListener::bind(("127.0.0.1", port)) {
        Ok(_) => Ok(false),
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => Ok(true),
        Err(e) => anyhow::bail!("bind loopback port {port}: {e}"),
    }
}

fn exit_of(status: std::process::ExitStatus) -> Exit {
    use std::os::unix::process::ExitStatusExt as _;
    match (status.code(), status.signal()) {
        (Some(code), _) => Exit::Code(code),
        (None, Some(signal)) => Exit::Signal(signal),
        (None, None) => Exit::Code(-1),
    }
}

/// The start time the kernel keeps for `pid` (`/proc/<pid>/stat`'s 22nd
/// field, in clock ticks since boot).
#[cfg(target_os = "linux")]
async fn started_mark(pid: u32) -> anyhow::Result<Option<String>> {
    let path = format!("/proc/{pid}/stat");
    let stat = match tokio::fs::read_to_string(&path).await {
        Ok(stat) => stat,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => anyhow::bail!("read {path}: {e}"),
    };
    Ok(Some(start_of_stat(&stat).ok_or_else(|| anyhow::anyhow!("{path} holds no start time: {stat:?}"))?.to_string()))
}

/// The start time `ps` prints for `pid` (macOS has no `/proc`).
#[cfg(not(target_os = "linux"))]
async fn started_mark(pid: u32) -> anyhow::Result<Option<String>> {
    let out = tokio::process::Command::new("ps")
        .args(["-o", "lstart=", "-p", &pid.to_string()])
        .stdin(Stdio::null())
        .output()
        .await
        .map_err(|e| anyhow::anyhow!("run `ps` to tell whether process {pid} is still the one weft started: {e}"))?;
    let printed = String::from_utf8_lossy(&out.stdout).trim().to_string();
    // `ps` exits 1 and prints nothing for a pid no process has.
    Ok((!printed.is_empty()).then_some(printed))
}

/// The 22nd field of a `/proc/<pid>/stat` line. The second field (the
/// program's name, in parentheses) may hold spaces and parentheses, so
/// the count starts after its last `)`.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn start_of_stat(stat: &str) -> Option<&str> {
    let after_name = &stat[stat.rfind(')')? + 1..];
    // Fields 3 onwards; the start time is field 22.
    after_name.split_whitespace().nth(22 - 3)
}

#[cfg(test)]
pub mod fake {
    use super::*;
    use parking_lot::Mutex;
    use std::collections::{HashMap, HashSet};

    /// How the next spawned process behaves.
    #[derive(Debug, Clone)]
    pub enum Behaviour {
        /// Serves `/_weft/healthz` on its `PORT`, answering `driving`.
        Serves,
        /// Serves the same on the port its variable `0` holds.
        ServesOn(String),
        /// Runs, serving nothing, until a signal ends it.
        Runs,
        /// Prints `printed` to its log and ends with `exit` at once.
        Ends { printed: String, exit: Exit },
    }

    /// A dumb machine: every process lives in a map until a signal ends
    /// it, and a serving one answers its health check on its `PORT`
    /// through a real loopback listener.
    pub struct FakeProcesses {
        next_pid: Mutex<u32>,
        alive: Mutex<HashMap<u32, String>>,
        servers: Mutex<HashMap<u32, tokio::task::AbortHandle>>,
        spawns: Mutex<Vec<Spawn>>,
        signals: Mutex<Vec<(u32, Signal)>>,
        behaviour: Mutex<Behaviour>,
        /// Pids that ignore `Signal::Term`.
        deaf: Mutex<HashSet<u32>>,
        /// Pids no signal can be sent to: `signal` fails.
        refusing: Mutex<HashSet<u32>>,
        /// What every serving process says it drives.
        pub driving: std::sync::Arc<std::sync::atomic::AtomicU64>,
        /// The worker key presented on the last health check.
        pub seen_key: std::sync::Arc<Mutex<Option<String>>>,
        /// The ports `free_port` handed out, each still bound until a
        /// process serves on it, so no other test on the machine takes it
        /// in between.
        reserved: Mutex<HashMap<u16, std::net::TcpListener>>,
        /// Ports a test listens on in place of this fake's processes.
        stand_ins: Mutex<HashSet<u16>>,
    }

    impl Default for FakeProcesses {
        fn default() -> Self {
            Self {
                next_pid: Mutex::new(1000),
                alive: Mutex::default(),
                servers: Mutex::default(),
                spawns: Mutex::default(),
                signals: Mutex::default(),
                behaviour: Mutex::new(Behaviour::Serves),
                deaf: Mutex::default(),
                refusing: Mutex::default(),
                driving: Default::default(),
                seen_key: Default::default(),
                reserved: Mutex::default(),
                stand_ins: Mutex::default(),
            }
        }
    }

    impl FakeProcesses {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn behave(&self, behaviour: Behaviour) {
            *self.behaviour.lock() = behaviour;
        }

        /// A process this fake did not start (an earlier life's worker).
        pub fn running(&self, pid: u32, started: &str) {
            self.alive.lock().insert(pid, started.to_string());
        }

        /// The test listens on `port` in place of this fake's processes
        /// (to answer a readiness check itself): never taken by another
        /// program.
        pub fn stand_in_on(&self, port: u16) {
            self.stand_ins.lock().insert(port);
        }

        /// `pid` ignores being told to stop.
        pub fn deaf(&self, pid: u32) {
            self.deaf.lock().insert(pid);
        }

        /// Signalling `pid` fails.
        pub fn refusing(&self, pid: u32) {
            self.refusing.lock().insert(pid);
        }

        pub fn is_alive(&self, pid: u32) -> bool {
            self.alive.lock().contains_key(&pid)
        }

        pub fn spawns(&self) -> Vec<Spawn> {
            self.spawns.lock().clone()
        }

        pub fn signals(&self) -> Vec<(u32, Signal)> {
            self.signals.lock().clone()
        }

        /// The pids spawned so far, in order.
        pub fn pids(&self) -> Vec<u32> {
            (1000..*self.next_pid.lock()).collect()
        }

        fn end(&self, pid: u32) {
            self.alive.lock().remove(&pid);
            if let Some(server) = self.servers.lock().remove(&pid) {
                server.abort();
            }
        }
    }

    #[async_trait]
    impl Processes for FakeProcesses {
        fn spawn(&self, spec: &Spawn) -> anyhow::Result<Spawned> {
            self.spawns.lock().push(spec.clone());
            let pid = {
                let mut next = self.next_pid.lock();
                *next += 1;
                *next - 1
            };
            let (tx, rx) = tokio::sync::watch::channel(None);
            match self.behaviour.lock().clone() {
                Behaviour::Ends { printed, exit } => {
                    std::fs::write(&spec.log, printed)?;
                    tx.send(Some(exit)).unwrap();
                }
                Behaviour::Runs => {
                    self.alive.lock().insert(pid, format!("t{pid}"));
                    // Held, never sent: the process has not ended.
                    std::mem::forget(tx);
                }
                behaviour @ (Behaviour::Serves | Behaviour::ServesOn(_)) => {
                    self.alive.lock().insert(pid, format!("t{pid}"));
                    let variable = match &behaviour {
                        Behaviour::ServesOn(variable) => variable.as_str(),
                        _ => "PORT",
                    };
                    let port: u16 = spec.env[variable].parse()?;
                    let reserved = self.reserved.lock().remove(&port);
                    let listener = match reserved {
                        Some(listener) => listener,
                        None => std::net::TcpListener::bind(("127.0.0.1", port))?,
                    };
                    listener.set_nonblocking(true)?;
                    let (driving, seen) = (self.driving.clone(), self.seen_key.clone());
                    let app = axum::Router::new().route(
                        "/_weft/healthz",
                        axum::routing::get(move |headers: axum::http::HeaderMap| {
                            let (driving, seen) = (driving.clone(), seen.clone());
                            async move {
                                *seen.lock() = headers
                                    .get(weft_platform_traits::WORKER_AUTH_HEADER)
                                    .and_then(|v| v.to_str().ok())
                                    .map(str::to_string);
                                axum::Json(serde_json::json!({ "driving": driving.load(std::sync::atomic::Ordering::SeqCst) }))
                            }
                        }),
                    );
                    let server = tokio::spawn(async move {
                        let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                        axum::serve(listener, app).await.unwrap();
                        drop(tx);
                    });
                    self.servers.lock().insert(pid, server.abort_handle());
                }
            }
            Ok(Spawned { pid, exit: rx })
        }

        fn signal(&self, pid: u32, signal: Signal) -> anyhow::Result<()> {
            if self.refusing.lock().contains(&pid) {
                anyhow::bail!("signal process {pid} ({signal:?}): operation not permitted");
            }
            self.signals.lock().push((pid, signal));
            if signal == Signal::Kill || !self.deaf.lock().contains(&pid) {
                self.end(pid);
            }
            Ok(())
        }

        async fn started(&self, pid: u32) -> anyhow::Result<Option<String>> {
            // A turn for the runtime, so the server of a process that just
            // ended lets go of its port before anything binds it again.
            tokio::task::yield_now().await;
            Ok(self.alive.lock().get(&pid).cloned())
        }

        fn free_port(&self) -> anyhow::Result<u16> {
            let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
            let port = listener.local_addr()?.port();
            self.reserved.lock().insert(port, listener);
            Ok(port)
        }

        /// A port this fake handed out is its own until a process serves
        /// on it, and one a test stands in on is its processes', neither
        /// taken; any other is the machine's say.
        fn port_taken(&self, port: u16) -> anyhow::Result<bool> {
            if self.reserved.lock().contains_key(&port) || self.stand_ins.lock().contains(&port) {
                return Ok(false);
            }
            super::loopback_taken(port)
        }

        /// A process of this fake is its whole group, gone once it ended.
        fn group_left(&self, pid: u32) -> anyhow::Result<bool> {
            Ok(self.alive.lock().contains_key(&pid))
        }
    }

    /// The ports a process unit keeps come from the same machine, held
    /// the same way until its process serves on one.
    impl crate::kept_ports::PickPorts for FakeProcesses {
        fn turn(&self) -> anyhow::Result<Box<dyn crate::kept_ports::Turn + '_>> {
            Ok(Box::new(self))
        }
    }

    impl crate::kept_ports::Turn for &FakeProcesses {
        fn free_port(&self, besides: &[u16]) -> anyhow::Result<u16> {
            loop {
                let port = Processes::free_port(*self)?;
                if !besides.contains(&port) {
                    return Ok(port);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_start_time_is_read_past_a_name_holding_spaces_and_parentheses() {
        let stat = "4242 (my (odd) prog) S 1 4242 4242 0 -1 4194304 100 0 0 0 1 2 0 0 20 0 1 0 98765 1000 50";
        assert_eq!(start_of_stat(stat), Some("98765"));
        assert_eq!(start_of_stat("4242 (cut"), None);
    }

    /// This process's own mark is there and stays the same; a pid no
    /// process has is `None`.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_live_process_has_a_steady_mark() {
        let me = std::process::id();
        let mark = MachineProcesses.started(me).await.unwrap();
        assert!(mark.is_some());
        assert_eq!(MachineProcesses.started(me).await.unwrap(), mark);
        assert_eq!(MachineProcesses.started(u32::MAX / 2).await.unwrap(), None);
    }

    /// A stop answers only once the whole group is gone, the program its
    /// process started included, so nothing of it still holds a port the
    /// next start binds.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_stop_answers_once_nothing_of_the_group_is_left() {
        let dir = tempfile::tempdir().unwrap();
        let spec = Spawn {
            program: PathBuf::from("/bin/sh"),
            // The shell ignores the stop it is told, and leaves a child of
            // its own in the group.
            args: vec!["-c".into(), "trap '' TERM; sleep 30 & wait".into()],
            cwd: dir.path().to_path_buf(),
            env: BTreeMap::from([("PATH".to_string(), "/bin:/usr/bin".to_string())]),
            log: dir.path().join("log"),
        };
        let spawned = MachineProcesses.spawn(&spec).unwrap();
        let started = MachineProcesses.started(spawned.pid).await.unwrap().unwrap();
        assert!(MachineProcesses.group_left(spawned.pid).unwrap());
        assert!(stop(&MachineProcesses, spawned.pid, &started, std::time::Duration::from_millis(100)).await.unwrap());
        assert!(!MachineProcesses.group_left(spawned.pid).unwrap(), "the shell's sleep went with it");
    }
}
