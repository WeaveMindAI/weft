//! Infra units that run one process on the machine
//! (`weft_core::infra::Process`), the way a local install runs them.
//!
//! Each unit's process runs under an agent of its own (`weft-runtime
//! unit-agent process`, the process unit's counterpart of the agent beside
//! a container unit), which this host starts in a process group of its
//! own. The agent starts the process in the same group, stamps every line
//! it prints into the unit's log, starts it again when it exits on its own
//! (as Docker brings back a container, `--restart unless-stopped`), and
//! keeps how it is doing in a file this host reads
//! (`weft_platform_traits::unit_agent::ProcessAgentState`). The agent
//! outlives weft's runtime the way a container outlives it: every unit
//! this host holds is in a registry file, so the runtime's next life finds
//! an agent still running and adopts it rather than starting a second
//! one, telling it from a program that took its pid since by when it
//! started (`processes::runs`).
//!
//! On disk, under the install's `infra-processes/`
//! (`InstallLayout::process_units`):
//!
//! - `units.json`: every unit held, running or stopped ([`Held`]).
//! - `<copy>/<unit>/work/`: the process's folder, its working directory,
//!   with the spec's files written into it before every start. What the
//!   process writes there itself stays across stops and restarts.
//! - `<copy>/<unit>/process.log` (and `process.log.1`, the agent keeping
//!   it to a bounded size): what the process printed, stamped.
//! - `<copy>/<unit>/agent.json`: the agent's state; `agent.log`: what the
//!   agent itself printed.
//! - `<copy>/<unit>/setup.json`: the needs of the process weft installs
//!   itself (`weft_core::needs::Recipe`), which the agent meets before it
//!   starts the process (`ProcessSetup`): installed under weft's data
//!   folder, shared by every install on the machine, the unit reading as
//!   starting ("installing ...") meanwhile.
//!
//! Stop ends the agent and its process and keeps the folder; remove and
//! terminate take the folder too.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use weft_core::infra::wire::{LogLine, LogMark, LogStream, LogsFrom};
use weft_core::infra::{EndpointTarget, Expose, NodeRef, Process, ProbeKind, ResolvedNode, ResolvedUnit};
use weft_core::needs::System;
use weft_platform_traits::unit_agent::{ProcessAgentState, ProcessSetup, PROCESS_AGENT_COMMAND};
use weft_platform_traits::{Clock, EndpointAt, InfraHost, MachineNeeds, RestartCause, UnitObservation, UnitRestart, UnitRunState};

use crate::processes::{Processes, Signal, Spawn};

/// The variables of the machine's own environment a process gets besides
/// `PATH` and `HOME` (which it always gets), each when the machine sets
/// it. A process unit runs a program as the person, under their own login
/// (a coding agent's CLI), whether the person installed it or weft did
/// under its needs folder, and such a program reads these: who the person is (`USER`, `LOGNAME`), the shell
/// it runs commands in (`SHELL`), the language and character set it prints
/// in (`LANG`, `LC_ALL`, `LC_CTYPE`), where it keeps temporary files
/// (`TMPDIR`, the person's own folder on macOS), and where it keeps its
/// configuration, data, state and caches when the person moved them off
/// `HOME`'s defaults (`XDG_*`). Nothing else of the runtime's environment
/// is passed: it holds weft's own secrets.
pub const PASSED_ENV: &[&str] = &[
    "USER",
    "LOGNAME",
    "SHELL",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "TMPDIR",
    "XDG_CONFIG_HOME",
    "XDG_DATA_HOME",
    "XDG_STATE_HOME",
    "XDG_CACHE_HOME",
    "XDG_RUNTIME_DIR",
];

/// What a unit's log stream is called (`LogStream::source`), and the
/// "container" a restart of its process names (`UnitRestart::container`).
pub const PROCESS_SOURCE: &str = "process";

/// How many of a process's last lines a look at one that exited carries.
const TAIL_LINES: usize = 20;

/// How much of a unit's log one read from its end takes at a time.
const LOG_BLOCK: usize = 64 * 1024;

/// How a unit stood when [`ProcessUnits::look`] saw it, with the registry
/// locked: what it could not say without a slow read is left for
/// [`ProcessUnits::finish`], off the lock, so a readiness check that waits
/// its whole timeout never holds up a stop or an apply of any unit.
enum Seen {
    /// Its state, known already.
    Is(UnitRunState),
    /// Its process exited `how` and its agent starts it again shortly:
    /// said with its last lines.
    Exited { how: String },
    /// Its process runs, and how it stands is its readiness check's.
    Runs(Check),
}

struct Check {
    probe: ProbeKind,
    timeout: Duration,
    /// Until when a failing check still reads as starting.
    starting_until_unix: i64,
}

#[derive(Clone)]
pub struct ProcessUnitsConfig {
    /// Where the registry and every unit's folder are
    /// (`InstallLayout::process_units`).
    pub dir: PathBuf,
    /// The program each unit's agent runs from: this install's
    /// `weft-runtime`.
    pub agent_program: PathBuf,
    /// What every process gets of the machine's environment: `PATH`,
    /// `HOME`, and the ones of [`PASSED_ENV`] the machine sets.
    pub machine_env: BTreeMap<String, String>,
    /// The system this machine runs, as far as installing a need goes
    /// (`System::here`), or why it is not known: what a refusal of a
    /// missing need words its install command for.
    pub system: Result<System, String>,
    /// How long a stop waits for a unit's agent and process to exit before
    /// killing them.
    pub stop_grace: Duration,
    /// Where the needs weft installs itself go (`MachineLayout::needs`).
    pub needs_root: PathBuf,
}

/// One unit this host holds, running or stopped.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Held {
    pub node: NodeRef,
    pub unit: String,
    /// The hash of the resolved unit it runs (`ResolvedUnit::hash`).
    pub hash: String,
    /// The process as applied: what a start after the agent was found
    /// gone runs again, with no spec at hand.
    pub process: Process,
    /// The loopback port picked for each of the process's ports, by name:
    /// kept for as long as the unit is held, so the address its endpoints
    /// were given stays its address across restarts and stops.
    pub ports: BTreeMap<String, u16>,
    /// What of it runs.
    pub run: Run,
    /// The latest time the host found the agent gone (the machine
    /// restarted, or something killed it) and started it again, or tried
    /// to, since weft last applied the unit.
    pub revived: Option<Revived>,
}

/// What of a unit runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub enum Run {
    /// Its agent, started by this host and running until a look finds it
    /// gone.
    Agent(Agent),
    /// Nothing: it was stopped.
    Stopped,
    /// Nothing: the start of an apply failed, for `why`. What is wrong is
    /// what was applied (a program it names that is not there, an agent
    /// that cannot start with it), so it is kept as it is until the next
    /// apply, and every look says the failure and its reason: a unit that
    /// reads as stopped is one a person stopped, which the supervisor
    /// leaves alone.
    Failed { why: String },
    /// Nothing: its agent was found gone and starting it again failed,
    /// for `why`. Unlike [`Run::Failed`], what was applied had run, so the
    /// unit is still one weft brings back (as Docker keeps bringing back a
    /// container whose restart failed): every look reads as failed with
    /// the reason and tries the start again, so a cause that clears on its
    /// own (a reboot that put the program back on the `PATH` a little
    /// later) brings the unit back with no `weft infra start` by hand. A
    /// look is one supervisor tick, so this adds no timer of its own.
    Reviving { why: String },
}

impl Run {
    fn agent(&self) -> Option<&Agent> {
        match self {
            Run::Agent(agent) => Some(agent),
            Run::Stopped | Run::Failed { .. } | Run::Reviving { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Agent {
    pub pid: u32,
    /// When it started (`Processes::started`).
    pub started: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Revived {
    pub at_unix: i64,
    pub count: u32,
}

impl Held {
    fn is(&self, node: &NodeRef, unit: &str) -> bool {
        self.node.copy_id == node.copy_id && self.unit == unit
    }
}

/// The registry's file name in the host's folder.
const REGISTRY: &str = "units.json";

/// Every unit the registry in `dir` holds; none when there is no file yet.
fn load(dir: &Path) -> anyhow::Result<Vec<Held>> {
    let path = dir.join(REGISTRY);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => anyhow::bail!("read {}: {e}", path.display()),
    };
    serde_json::from_slice(&bytes).map_err(|e| {
        anyhow::anyhow!(
            "{} is not the list of process units weft keeps ({e}); stop the agents it names by hand \
             (`kill -- -<pid>`), then delete the file and start the infra again",
            path.display()
        )
    })
}

/// Kill every unit's agent the registry in `dir` names, at once, with its
/// process and whatever that started (`SIGKILL` to its group), and what
/// is left of an agent found gone (`processes::kill_leftovers`): what a
/// person removing the install asks for, since nothing would ever stop
/// them after its folder is gone. A pid another program took since is
/// left alone. Answers how many agents were running.
// SYNC: kill_all <-> scripts/lib/weft-cleanup.sh (weft_stop_process_units)
pub async fn kill_all(processes: &dyn Processes, dir: &Path) -> anyhow::Result<usize> {
    let mut killed = 0;
    for held in load(dir)? {
        let Some(agent) = held.run.agent() else { continue };
        if crate::processes::runs(processes, agent.pid, &agent.started).await? {
            processes.signal(agent.pid, Signal::Kill)?;
            killed += 1;
        } else {
            crate::processes::kill_leftovers(processes, agent.pid, &agent.started).await?;
        }
    }
    Ok(killed)
}

/// Every process unit of `project` the host whose folder is `dir` holds
/// (`InstallLayout::process_units`), running or not, as the node it is of
/// (as the program spells it), the unit, and what its process needs: what
/// `weft deps-install` installs for a project's infra. A unit whose apply
/// was refused before it ever started is not held, and not among them.
pub fn held_needs(dir: &Path, project: uuid::Uuid) -> anyhow::Result<Vec<(String, String, Vec<weft_core::needs::Need>)>> {
    Ok(load(dir)?.into_iter().filter(|h| h.node.project == project).map(|h| (h.node.node, h.unit, h.process.needs)).collect())
}

/// The process units of a local install.
pub struct ProcessUnits {
    processes: Arc<dyn Processes>,
    needs: Arc<dyn MachineNeeds>,
    clock: Arc<dyn Clock>,
    cfg: ProcessUnitsConfig,
    /// Held across every read and write of the registry: an apply, a stop
    /// and a look at the same units never interleave.
    changing: tokio::sync::Mutex<()>,
}

impl ProcessUnits {
    pub fn new(processes: Arc<dyn Processes>, needs: Arc<dyn MachineNeeds>, clock: Arc<dyn Clock>, cfg: ProcessUnitsConfig) -> Self {
        Self { processes, needs, clock, cfg, changing: tokio::sync::Mutex::new(()) }
    }

    fn save(&self, held: &[Held]) -> anyhow::Result<()> {
        // It holds the processes' environments.
        crate::registry::write_private(&self.cfg.dir.join(REGISTRY), &serde_json::to_vec_pretty(held).expect("units serialize"))
    }

    fn unit_dir(&self, node: &NodeRef, unit: &str) -> PathBuf {
        self.cfg.dir.join(&node.copy_id).join(unit)
    }

    fn work_dir(&self, node: &NodeRef, unit: &str) -> PathBuf {
        self.unit_dir(node, unit).join("work")
    }

    fn log_path(&self, node: &NodeRef, unit: &str) -> PathBuf {
        self.unit_dir(node, unit).join("process.log")
    }

    fn state_path(&self, node: &NodeRef, unit: &str) -> PathBuf {
        self.unit_dir(node, unit).join("agent.json")
    }

    fn setup_path(&self, node: &NodeRef, unit: &str) -> PathBuf {
        self.unit_dir(node, unit).join("setup.json")
    }

    /// Whether this host holds `unit` of `node`, running or stopped: what
    /// tells a local install which of its hosts a unit is on.
    pub async fn holds(&self, node: &NodeRef, unit: &str) -> anyhow::Result<bool> {
        let _changing = self.changing.lock().await;
        Ok(load(&self.cfg.dir)?.iter().any(|h| h.is(node, unit)))
    }

    /// Refuse a unit this host cannot run: one that runs containers.
    pub fn check_unit(&self, node: &ResolvedNode, unit: &ResolvedUnit) -> Result<(), String> {
        match unit.unit.process {
            Some(_) => Ok(()),
            None => Err(format!("unit '{}' of '{}' runs containers, which the process host does not run", unit.unit.name, node.node.node)),
        }
    }

    /// Start `held`'s agent: its folder made, the process's files written
    /// into it, the last agent's state cleared. `held.run` is the new agent,
    /// or the failure and its reason (`failed` making the run it is kept
    /// as), which is also the error answered.
    async fn start(&self, held: &mut Held, failed: fn(String) -> Run) -> anyhow::Result<()> {
        let started = self.start_agent(held).await;
        held.run = match &started {
            Ok(agent) => Run::Agent(agent.clone()),
            Err(e) => failed(format!("{e:#}")),
        };
        started.map(|_| ())
    }

    /// Start again `held`'s agent, found gone: the state a look says, the
    /// new agent pushed on `revived`, or the failure kept as
    /// [`Run::Reviving`] for the next look to try again.
    async fn revive(&self, held: &mut Held, revived: &mut Vec<Agent>) -> UnitRunState {
        match self.start(held, |why| Run::Reviving { why }).await {
            Ok(()) => {
                revived.extend(held.run.agent().cloned());
                UnitRunState::Starting {
                    step: "its process was gone (the machine restarted, or something stopped it) and is starting again".into(),
                }
            }
            // Said from the run it is kept as, as every later look says it.
            Err(_) => registered(&held.run),
        }
    }

    async fn start_agent(&self, held: &Held) -> anyhow::Result<Agent> {
        self.find_program(&held.node, &held.unit, &held.process)?;
        let (node, unit) = (&held.node, held.unit.as_str());
        let work = self.work_dir(node, unit);
        std::fs::create_dir_all(&work).map_err(|e| anyhow::anyhow!("create {}: {e}", work.display()))?;
        for (path, content) in &held.process.files {
            let at = work.join(path);
            if let Some(parent) = at.parent() {
                std::fs::create_dir_all(parent).map_err(|e| anyhow::anyhow!("create {}: {e}", parent.display()))?;
            }
            std::fs::write(&at, content).map_err(|e| anyhow::anyhow!("write {}: {e}", at.display()))?;
        }
        let state = self.state_path(node, unit);
        match std::fs::remove_file(&state) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => anyhow::bail!("remove {}: {e}", state.display()),
        }
        let installed: Vec<_> = held.process.needs.iter().filter(|n| n.recipe.is_some()).cloned().collect();
        let setup = (!installed.is_empty()).then(|| self.setup_path(node, unit));
        if let Some(path) = &setup {
            let setup = ProcessSetup { root: self.cfg.needs_root.clone(), needs: installed };
            std::fs::write(path, serde_json::to_vec_pretty(&setup).expect("a setup serializes")).map_err(|e| anyhow::anyhow!("write {}: {e}", path.display()))?;
        }
        let agent_log = self.unit_dir(node, unit).join("agent.log");
        let spawn = agent_spawn(&self.cfg, held, &work, &state, &self.log_path(node, unit), &agent_log, setup.as_deref());
        let spawned = self
            .processes
            .spawn(&spawn)
            .map_err(|e| e.context(format!("start the agent of unit '{unit}' of '{}'", node.node)))?;
        match self.processes.started(spawned.pid).await {
            Ok(Some(started)) => Ok(Agent { pid: spawned.pid, started }),
            Ok(None) => anyhow::bail!(
                "the agent of unit '{unit}' of '{}' exited as it started; what it printed is in {}",
                node.node,
                agent_log.display()
            ),
            // It runs, and without its mark nothing could ever tell it from
            // a program that takes its pid later, so nothing would stop it.
            Err(e) => Err(self.kill_untracked(spawned.pid, &e.context("read when the agent started"))),
        }
    }

    /// Refuse to start `process` (of `unit` of `node`) when the program its
    /// command names is not on the `PATH` it gets: started anyway, it would
    /// exit at once and be started again for ever. A program one of its
    /// needs weft installs runs (that need's check names it) is that need's
    /// to find, once the agent installed it.
    fn find_program(&self, node: &NodeRef, unit: &str, process: &Process) -> anyhow::Result<()> {
        let program = process.command.first().map(String::as_str).unwrap_or_default();
        if process.needs.iter().any(|n| n.recipe.is_some() && n.check.first().map(String::as_str) == Some(program)) {
            return Ok(());
        }
        let path = process_env(&self.cfg.machine_env, process, &BTreeMap::new()).remove("PATH").unwrap_or_default();
        if program_found(program, &path, &self.work_dir(node, unit)) {
            return Ok(());
        }
        anyhow::bail!(
            "unit '{unit}' of '{}' runs `{program}`, which is not on its PATH ({path}); install it, or name it in the \
             process's needs so its absence is refused saying how to install it, then start the infra again. A program \
             installed since weft's runtime started is found once it starts again from a shell that finds it \
             (`weft daemon start`)",
            weft_core::project::plain_id(&node.node)
        )
    }

    /// Stop `held`'s agent, and its process with it, when it runs; when
    /// it is gone, kill what it left.
    async fn stop_agent(&self, held: &mut Held) -> anyhow::Result<()> {
        if let Some(agent) = held.run.agent() {
            if !crate::processes::stop(self.processes.as_ref(), agent.pid, &agent.started, self.cfg.stop_grace).await? {
                crate::processes::kill_leftovers(self.processes.as_ref(), agent.pid, &agent.started).await?;
            }
        }
        held.run = Run::Stopped;
        Ok(())
    }

    /// Save `all`; when that fails, every agent of `started` (just
    /// started, so written down nowhere) is killed, since nothing would
    /// ever stop it.
    fn save_or_kill(&self, all: &[Held], started: &[Agent]) -> anyhow::Result<()> {
        let Err(e) = self.save(all) else { return Ok(()) };
        let mut e = e;
        for agent in started {
            e = self.kill_untracked(agent.pid, &e);
        }
        Err(e)
    }

    /// Kill the agent `pid`, which weft cannot keep track of because of
    /// `e`, and answer the error saying so.
    fn kill_untracked(&self, pid: u32, e: &anyhow::Error) -> anyhow::Error {
        let lost = format!("could not keep track of a process unit's agent (pid {pid}): {e:#}");
        match self.processes.signal(pid, Signal::Kill) {
            Ok(()) => anyhow::anyhow!("{lost}; it was killed"),
            Err(kill) => anyhow::anyhow!("{lost}; killing it failed too ({kill:#}): stop it by hand with `kill -9 -- -{pid}`"),
        }
    }

    /// The lines the process of `unit` of `node` printed, in the order
    /// written (the agent's older file, then its current one), read from
    /// the end: the latest first, for as long as `more` answers true for
    /// the line just read. A read of the last few lines of a big log reads
    /// those lines and no more.
    fn lines(&self, node: &NodeRef, unit: &str, more: impl FnMut(&LogLine) -> bool) -> anyhow::Result<Vec<LogLine>> {
        let current = self.log_path(node, unit);
        let mut older = current.as_os_str().to_owned();
        older.push(".1");
        read_back(&[current, PathBuf::from(older)], LOG_BLOCK, more)
    }

    /// The last lines the process of `unit` of `node` printed, as text.
    fn tail(&self, node: &NodeRef, unit: &str) -> String {
        let mut left = TAIL_LINES;
        match self.lines(node, unit, take(&mut left)) {
            Ok(lines) => lines.iter().map(|l| l.text.as_str()).collect::<Vec<_>>().join("\n"),
            Err(e) => format!("(its log could not be read: {e:#})"),
        }
    }

    /// How `held`'s agent is doing, as far as its own state says, and its
    /// latest restart; what is left to find out off the lock (a readiness
    /// check, the last lines of a process that exited) is in what is
    /// answered. An agent found gone is started again here (the way Docker
    /// brings back a container that was running when its daemon went),
    /// after what it left is killed, which changes `held`; the new agent
    /// is pushed on `revived`. One whose start again failed is tried again.
    async fn look(&self, held: &mut Held, revived: &mut Vec<Agent>) -> anyhow::Result<(Seen, Option<UnitRestart>)> {
        let agent = match &held.run {
            Run::Stopped => return Ok((Seen::Is(UnitRunState::Stopped), None)),
            Run::Failed { .. } => return Ok((Seen::Is(registered(&held.run)), self.restart(held, None))),
            // Not counted as another restart: the unit was found gone once,
            // and what it left was killed then; nothing of it ran since.
            Run::Reviving { .. } => {
                let state = self.revive(held, revived).await;
                return Ok((Seen::Is(state), self.restart(held, None)));
            }
            Run::Agent(agent) => agent.clone(),
        };
        if !crate::processes::runs(self.processes.as_ref(), agent.pid, &agent.started).await? {
            // An agent that died without taking its process along (killed
            // outright on a machine where nothing kills a child with its
            // parent) left it running on the unit's port: a second one
            // started beside it would never get the port, and nothing
            // would ever stop the first.
            crate::processes::kill_leftovers(self.processes.as_ref(), agent.pid, &agent.started).await?;
            let count = held.revived.map_or(0, |r| r.count) + 1;
            held.revived = Some(Revived { at_unix: self.clock.now_unix(), count });
            let state = self.revive(held, revived).await;
            return Ok((Seen::Is(state), self.restart(held, None)));
        }
        let state_path = self.state_path(&held.node, &held.unit);
        let agent_state = ProcessAgentState::read(&state_path).map_err(|e| {
            e.context(format!(
                "the agent of unit '{}' of '{}' keeps a state this weft cannot read, so it is not this weft's own; \
                 stop the node's infra and start it again (`weft infra stop --force`, then `weft infra start`)",
                held.unit,
                weft_core::project::plain_id(&held.node.node)
            ))
        })?;
        let Some(agent_state) = agent_state else {
            return Ok((Seen::Is(UnitRunState::Starting { step: "its process is starting".into() }), self.restart(held, None)));
        };
        // Before its process first starts: meeting the needs weft installs.
        if let Some(why) = &agent_state.setup_failed {
            return Ok((Seen::Is(UnitRunState::Failed { why: why.clone() }), self.restart(held, None)));
        }
        if let Some(step) = &agent_state.setting_up {
            return Ok((Seen::Is(UnitRunState::Starting { step: step.clone() }), self.restart(held, None)));
        }
        let restart = self.restart(held, Some(&agent_state));
        let Some(run) = agent_state.running else {
            let how = agent_state.last_exit.map_or_else(|| "before it started".to_string(), |exit| format!("with code {}", exit.code));
            return Ok((Seen::Exited { how }, restart));
        };
        let Some(readiness) = &held.process.readiness else { return Ok((Seen::Is(UnitRunState::Ready), restart)) };
        let port = *held
            .ports
            .get(&readiness.port)
            .ok_or_else(|| anyhow::anyhow!("unit '{}' of '{}' holds no port '{}'", held.unit, held.node.node, readiness.port))?;
        let check = Check {
            probe: ProbeKind::Http { path: readiness.path.clone(), port },
            timeout: Duration::from_secs(u64::from(readiness.timeout_seconds.max(1))),
            starting_until_unix: run.started_at_unix + i64::from(readiness.startup_seconds),
        };
        Ok((Seen::Runs(check), restart))
    }

    /// The state of a unit [`Self::look`] saw as `seen`: what it left to
    /// find out found out now, with no lock held.
    async fn finish(&self, node: &NodeRef, unit: &str, seen: Seen) -> UnitRunState {
        match seen {
            Seen::Is(state) => state,
            Seen::Exited { how } => {
                UnitRunState::NotReady { why: format!("its process exited {how} and starts again shortly; its last lines:\n{}", self.tail(node, unit)) }
            }
            Seen::Runs(check) => {
                let answer = crate::probe::run_probe(&check.probe, check.timeout).await;
                if answer.ok {
                    UnitRunState::Ready
                } else if self.clock.now_unix() < check.starting_until_unix {
                    UnitRunState::Starting { step: "its process has not answered its health check yet".into() }
                } else {
                    UnitRunState::NotReady { why: format!("its process: {}", answer.why.unwrap_or_else(|| "not ready".into())) }
                }
            }
        }
    }

    /// The latest restart of `held`'s process: its agent starting it again
    /// after it exited (`agent`, its state), or the host starting the agent
    /// again after it was gone.
    fn restart(&self, held: &Held, agent: Option<&ProcessAgentState>) -> Option<UnitRestart> {
        let revived = held.revived.map_or(0, |r| r.count);
        let count = revived + agent.map_or(0, |a| a.restarts);
        let exited = agent.and_then(|a| a.last_exit).map(|exit| (exit.at_unix, RestartCause::Exited { code: i64::from(exit.code) }));
        let gone = held.revived.map(|r| (r.at_unix, RestartCause::Unknown));
        let (at, cause) = [exited, gone].into_iter().flatten().max_by_key(|(at, _)| *at)?;
        Some(UnitRestart { container: PROCESS_SOURCE.to_string(), stopped_at_unix: at, cause, count })
    }
}

/// How a unit stands as far as its registry entry alone says (`run`):
/// what a look reads for one that changed while its process was checked.
fn registered(run: &Run) -> UnitRunState {
    match run {
        Run::Agent(_) => UnitRunState::Starting { step: "it was started again while weft looked at it; its process is starting".into() },
        Run::Stopped => UnitRunState::Stopped,
        Run::Failed { why } => UnitRunState::Failed { why: why.clone() },
        Run::Reviving { why } => UnitRunState::Failed { why: format!("its process was gone and starting it again failed (weft tries again at every look): {why}") },
    }
}

/// What starts `held`'s agent: this install's `weft-runtime` as the agent,
/// told where to keep its state and the process's log (and its `setup`,
/// when the process has needs weft installs), then the process's command;
/// in the process's folder, with the process's environment.
fn agent_spawn(cfg: &ProcessUnitsConfig, held: &Held, work: &Path, state: &Path, log: &Path, agent_log: &Path, setup: Option<&Path>) -> Spawn {
    let mut args: Vec<String> = PROCESS_AGENT_COMMAND.iter().map(|word| word.to_string()).collect();
    args.extend(["--state".into(), state.display().to_string(), "--log".into(), log.display().to_string()]);
    if let Some(setup) = setup {
        args.extend(["--setup".into(), setup.display().to_string()]);
    }
    args.push("--".into());
    args.extend(held.process.command.iter().cloned());
    Spawn {
        program: cfg.agent_program.clone(),
        args,
        cwd: work.to_path_buf(),
        env: process_env(&cfg.machine_env, &held.process, &held.ports),
        log: agent_log.to_path_buf(),
    }
}

/// Whether `program` is a file that can run: one of the folders of `path`
/// (a `PATH`) holds it, or, when it names a folder itself (`./run`,
/// `/usr/bin/env`), there it is, from `cwd`.
fn program_found(program: &str, path: &str, cwd: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt as _;
    let runs = |at: &Path| std::fs::metadata(at).is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0);
    if program.contains('/') {
        return runs(&cwd.join(program));
    }
    !program.is_empty() && path.split(':').filter(|dir| !dir.is_empty()).any(|dir| runs(&Path::new(dir).join(program)))
}

/// A process's whole environment: what it gets of the machine's
/// (`machine`), what its spec sets (which wins over the machine's), and the
/// number of each of its ports in the variable the port names.
fn process_env(machine: &BTreeMap<String, String>, process: &Process, ports: &BTreeMap<String, u16>) -> BTreeMap<String, String> {
    let mut env = machine.clone();
    for e in &process.env {
        env.insert(e.name.clone(), e.value.clone());
    }
    for p in &process.ports {
        if let Some(number) = ports.get(&p.name) {
            env.insert(p.env.clone(), number.to_string());
        }
    }
    env
}

#[async_trait]
impl InfraHost for ProcessUnits {
    fn check(&self, node: &ResolvedNode) -> Result<(), String> {
        node.units.iter().try_for_each(|u| self.check_unit(node, u))
    }

    async fn apply_unit(&self, node: &ResolvedNode, unit: &str) -> anyhow::Result<()> {
        let resolved = node.unit(unit).ok_or_else(|| anyhow::anyhow!("node '{}' declares no unit '{unit}'", node.node.node))?;
        let process = resolved
            .unit
            .process
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("unit '{unit}' of '{}' runs no process", node.node.node))?;
        let _changing = self.changing.lock().await;
        let mut all = load(&self.cfg.dir)?;
        let at = all.iter().position(|h| h.is(&node.node, unit));
        if let Some(Held { hash, run: Run::Agent(agent), .. }) = at.map(|i| &all[i]) {
            // One whose setup failed is set up again: what it lacked may
            // be there now (`weft deps-install`).
            let failed_setup = matches!(
                ProcessAgentState::read(&self.state_path(&node.node, unit)),
                Ok(Some(ProcessAgentState { setup_failed: Some(_), .. }))
            );
            if *hash == resolved.hash && !failed_setup && crate::processes::runs(self.processes.as_ref(), agent.pid, &agent.started).await? {
                return Ok(());
            }
        }
        // A need weft installs is the agent's to meet, before the process
        // starts, as long as it takes; only the others are refused here.
        let checked: Vec<_> = process.needs.iter().filter(|n| n.recipe.is_none()).cloned().collect();
        let missing = self.needs.unmet(&checked).await?;
        if !missing.is_empty() {
            anyhow::bail!("{}", weft_core::needs::process_refusal(&node.node.node, unit, &missing, &self.cfg.system));
        }
        // Before the running one stops: a new version that cannot start
        // leaves the old one up.
        self.find_program(&node.node, unit, process)?;
        let mut held = match at {
            Some(i) => all.remove(i),
            None => Held {
                node: node.node.clone(),
                unit: unit.to_string(),
                hash: String::new(),
                process: process.clone(),
                ports: BTreeMap::new(),
                run: Run::Stopped,
                revived: None,
            },
        };
        self.stop_agent(&mut held).await?;
        let mut ports = BTreeMap::new();
        for p in &process.ports {
            let number = match held.ports.get(&p.name) {
                Some(number) => *number,
                None => self.processes.free_port()?,
            };
            ports.insert(p.name.clone(), number);
        }
        held = Held { node: node.node.clone(), hash: resolved.hash.clone(), process: process.clone(), ports, revived: None, ..held };
        // A failure is kept on the unit (`Run::Failed`) as well as
        // answered, so every look says it until the next apply.
        let started = self.start(&mut held, |why| Run::Failed { why }).await;
        let agent: Vec<Agent> = held.run.agent().cloned().into_iter().collect();
        all.push(held);
        self.save_or_kill(&all, &agent)?;
        started
    }

    async fn stop_unit(&self, node: &NodeRef, unit: &str) -> anyhow::Result<()> {
        let _changing = self.changing.lock().await;
        let mut all = load(&self.cfg.dir)?;
        let Some(held) = all.iter_mut().find(|h| h.is(node, unit)) else { return Ok(()) };
        self.stop_agent(held).await?;
        self.save(&all)
    }

    async fn remove_unit(&self, node: &NodeRef, unit: &str) -> anyhow::Result<()> {
        let _changing = self.changing.lock().await;
        let mut all = load(&self.cfg.dir)?;
        let Some(at) = all.iter().position(|h| h.is(node, unit)) else { return Ok(()) };
        let mut held = all.remove(at);
        self.stop_agent(&mut held).await?;
        remove_dir(&self.unit_dir(node, unit))?;
        self.save(&all)
    }

    async fn terminate(&self, node: &NodeRef, _keep_disks: &[String]) -> anyhow::Result<()> {
        let _changing = self.changing.lock().await;
        let (mut gone, kept): (Vec<Held>, Vec<Held>) = load(&self.cfg.dir)?.into_iter().partition(|h| h.node.copy_id == node.copy_id);
        for held in &mut gone {
            self.stop_agent(held).await?;
        }
        remove_dir(&self.cfg.dir.join(&node.copy_id))?;
        self.save(&kept)
    }

    async fn observe(&self, _tenant: &str, project: uuid::Uuid) -> anyhow::Result<Vec<UnitObservation>> {
        let mut seen = Vec::new();
        {
            let _changing = self.changing.lock().await;
            let mut all = load(&self.cfg.dir)?;
            let before = all.clone();
            let mut revived = Vec::new();
            for held in all.iter_mut().filter(|h| h.node.project == project) {
                // One unit that cannot be looked at (an agent's state this
                // weft cannot read, its process's start time unreadable) is
                // that unit failed, never every unit of the project unseen.
                let (now, restart) = match self.look(held, &mut revived).await {
                    Ok(looked) => looked,
                    Err(e) => (Seen::Is(UnitRunState::Failed { why: format!("{e:#}") }), self.restart(held, None)),
                };
                seen.push((held.clone(), now, restart));
            }
            if all != before {
                self.save_or_kill(&all, &revived)?;
            }
        }
        // Each unit as it was seen, how it stands, its latest restart, and
        // whether how it stands was found out off the lock.
        let mut out = Vec::new();
        for (held, now, restart) in seen {
            let off_lock = !matches!(now, Seen::Is(_));
            let state = self.finish(&held.node, &held.unit, now).await;
            out.push((held, state, restart, off_lock));
        }
        if out.iter().any(|(.., off_lock)| *off_lock) {
            // What was found out off the lock is of the unit as the look saw
            // it. A stop or an apply that ran meanwhile made it another one
            // (stopped, or a new version starting), and the check's answer
            // said of it under the old hash would read as the old version
            // going bad, which the health loop can count as the unit
            // turning flaky. So each is checked against the registry once
            // more, under the lock, and one that changed reads as the
            // registry says it is now.
            let _changing = self.changing.lock().await;
            let all = load(&self.cfg.dir)?;
            out.retain_mut(|(held, state, restart, off_lock)| {
                if !*off_lock {
                    return true;
                }
                let Some(now) = all.iter().find(|h| h.is(&held.node, &held.unit)) else {
                    // Removed meanwhile: no longer a unit this host holds.
                    return false;
                };
                if now.hash != held.hash || now.run != held.run {
                    *state = registered(&now.run);
                    *restart = self.restart(now, None);
                    *held = now.clone();
                }
                true
            });
        }
        Ok(out
            .into_iter()
            .map(|(held, state, restart, _)| UnitObservation { copy_id: held.node.copy_id, unit: held.unit, hash: held.hash, state, restart })
            .collect())
    }

    async fn copies(&self) -> anyhow::Result<Vec<NodeRef>> {
        let _changing = self.changing.lock().await;
        let mut out: Vec<NodeRef> = Vec::new();
        for held in load(&self.cfg.dir)? {
            if !out.iter().any(|n| n.copy_id == held.node.copy_id) {
                out.push(held.node);
            }
        }
        Ok(out)
    }

    async fn endpoint(&self, node: &ResolvedNode, endpoint: &str) -> anyhow::Result<EndpointAt> {
        let ep = node
            .spec
            .endpoints
            .iter()
            .find(|e| e.name == endpoint)
            .ok_or_else(|| anyhow::anyhow!("node '{}' declares no endpoint '{endpoint}'", node.node.node))?;
        let EndpointTarget::Process { unit, port } = &ep.target else {
            anyhow::bail!("endpoint '{endpoint}' of '{}' is not a process's port", node.node.node);
        };
        let _changing = self.changing.lock().await;
        let all = load(&self.cfg.dir)?;
        let held = all
            .iter()
            .find(|h| h.is(&node.node, unit))
            .ok_or_else(|| anyhow::anyhow!("unit '{unit}' of '{}' has not started yet", node.node.node))?;
        let number = held
            .ports
            .get(port)
            .ok_or_else(|| anyhow::anyhow!("unit '{unit}' of '{}' holds no port '{port}'; start its infra again", node.node.node))?;
        // The process is on the machine: weft's runtime and the project's
        // workers reach it on loopback, and so does the install's network,
        // which on a local install is the machine itself.
        let on_machine = format!("127.0.0.1:{number}");
        Ok(EndpointAt { url: format!("http://{on_machine}"), same_network: matches!(ep.expose, Expose::SameNetwork).then_some(on_machine) })
    }

    async fn logs(&self, node: &NodeRef, unit: &str, from: &LogsFrom) -> anyhow::Result<Vec<LogStream>> {
        let mark = match from {
            LogsFrom::Tail(_) => None,
            LogsFrom::After(marks) => marks.get(PROCESS_SOURCE).copied(),
        };
        // Taken before the read, on the clock the agent stamps lines by
        // (this machine's): a line written while the read runs is at or
        // after it.
        let start = match mark {
            Some(mark) => mark,
            None => LogMark::start(chrono::DateTime::from_timestamp(self.clock.now_unix(), 0).expect("a clock reading is a time")),
        };
        let lines = match (from, mark) {
            (LogsFrom::Tail(n), _) => {
                let mut left = *n;
                self.lines(node, unit, take(&mut left))?
            }
            // Read back to the first line before the mark's instant: the
            // agent stamps each line as it writes it, so every line from
            // there on was written after it (`LogMark::unseen` drops what
            // was delivered at the mark's own instant).
            (LogsFrom::After(_), Some(mark)) => mark.unseen(self.lines(node, unit, |l| l.at >= mark.at)?),
            // No mark: a unit that appeared since the follow started, so
            // all it wrote is new.
            (LogsFrom::After(_), None) => self.lines(node, unit, |_| true)?,
        };
        let mark = start.advance(&lines);
        Ok(vec![LogStream { source: PROCESS_SOURCE.to_string(), lines, mark }])
    }
}

/// A `more` for [`ProcessUnits::lines`] that takes `left` lines.
fn take(left: &mut usize) -> impl FnMut(&LogLine) -> bool + '_ {
    move |_| match left.checked_sub(1) {
        Some(next) => {
            *left = next;
            true
        }
        None => false,
    }
}

/// The stamped lines of the files `newest_first` (a unit's current log,
/// then the one it rolled over to), read back from the end of the first
/// `block` bytes at a time, for as long as `more` answers true for the
/// latest line read (the line it answers false for is not kept); answered
/// in the order written.
///
/// The last line of a file that does not end its line yet is one the
/// agent is writing as this reads: it is left for the next read, which
/// reads it whole. Any other line the agent did not write (a stamp that
/// does not read, a file someone else wrote into) is skipped, and the
/// runtime's log says which: a log with one bad line still reads, and no
/// line of it ever has to be made up to stand in for the bad one.
fn read_back(newest_first: &[PathBuf], block: usize, more: impl FnMut(&LogLine) -> bool) -> anyhow::Result<Vec<LogLine>> {
    read_opened_back(&mut open_together(newest_first, |_| {})?, block, more)
}

/// One file of a log, opened: `None` when there was none.
struct Opened<'a> {
    path: &'a Path,
    file: Option<std::fs::File>,
}

/// Open every file of `newest_first` as one consistent set: each opened
/// while no roll happened, so the older file is the one the current one
/// rolled over from. The agent rolls by renaming its current file onto
/// the older one's name and opening a fresh one, so a roll landing
/// between two opens would hand the older name the very file opened as
/// current, read twice. The set is checked once all are open (every path
/// still names the file opened from it: a roll renames the current file
/// away, so it shows) and opened again when it is not; once open, a roll
/// changes nothing of what is read, since a file renamed or unlinked
/// keeps its content for whoever has it open. A roll takes a whole log's
/// worth of new lines, so a second open lands between two of them.
/// `opened` is handed the index of each file just opened (a test rolls
/// the log there).
fn open_together<'a>(newest_first: &'a [PathBuf], mut opened: impl FnMut(usize)) -> anyhow::Result<Vec<Opened<'a>>> {
    use std::os::unix::fs::MetadataExt as _;
    let identity = |meta: std::fs::Metadata| (meta.dev(), meta.ino());
    let named = |path: &Path| match std::fs::metadata(path) {
        Ok(meta) => Ok(Some(identity(meta))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(anyhow::anyhow!("read {}: {e}", path.display())),
    };
    loop {
        let mut files = Vec::new();
        for (i, path) in newest_first.iter().enumerate() {
            let file = match std::fs::File::open(path) {
                Ok(file) => Some(file),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                Err(e) => anyhow::bail!("read {}: {e}", path.display()),
            };
            opened(i);
            files.push(Opened { path, file });
        }
        let mut consistent = true;
        for one in &files {
            let had = match &one.file {
                Some(file) => Some(identity(file.metadata().map_err(|e| anyhow::anyhow!("read {}: {e}", one.path.display()))?)),
                None => None,
            };
            consistent &= named(one.path)? == had;
        }
        if consistent {
            return Ok(files);
        }
    }
}

/// [`read_back`] of files opened already.
fn read_opened_back(newest_first: &mut [Opened<'_>], block: usize, mut more: impl FnMut(&LogLine) -> bool) -> anyhow::Result<Vec<LogLine>> {
    let mut lines = Vec::new();
    for opened in newest_first {
        let Some(file) = &mut opened.file else { continue };
        let path = opened.path;
        let mut done = false;
        lines_back(file, path, block, |raw| {
            let Some(line) = LogLine::read_stamped(&String::from_utf8_lossy(raw)) else {
                tracing::warn!(
                    target: "weft_platform_local::process_units",
                    file = %path.display(), line = %String::from_utf8_lossy(&raw[..raw.len().min(200)]),
                    "a process unit's log holds a line its agent did not write; skipped"
                );
                return true;
            };
            if !more(&line) {
                done = true;
                return false;
            }
            lines.push(line);
            true
        })?;
        if done {
            break;
        }
    }
    lines.reverse();
    Ok(lines)
}

/// Hand `each` every whole line of `file` (opened from `path`), without
/// its newline, the last first, read back from its end `block` bytes at a
/// time, until it answers false. An unfinished last line (no newline yet)
/// is not handed.
fn lines_back(file: &mut std::fs::File, path: &Path, block: usize, mut each: impl FnMut(&[u8]) -> bool) -> anyhow::Result<()> {
    use std::io::{Read as _, Seek as _};
    let read = |e: std::io::Error| anyhow::anyhow!("read {}: {e}", path.display());
    // Up to the size it has now: what the agent appends while this reads
    // is the next read's.
    let mut unread = file.metadata().map_err(read)?.len();
    // The bytes read and not handed yet, from `unread` on: the start of
    // the line being put together, which ends where the last one handed
    // began.
    let mut held: Vec<u8> = Vec::new();
    // Whether `held` still runs to the end of the file, so the line in it
    // is the last one, which has no newline when the agent is writing it.
    let mut at_end = true;
    loop {
        while let Some(newline) = held.iter().rposition(|&b| b == b'\n') {
            if !at_end && !each(&held[newline + 1..]) {
                return Ok(());
            }
            at_end = false;
            held.truncate(newline);
        }
        if unread == 0 {
            // What is left is the file's first line, whole unless it is
            // also the last.
            if !at_end && !held.is_empty() {
                each(&held);
            }
            return Ok(());
        }
        let from = unread.saturating_sub(block as u64);
        let mut chunk = vec![0; (unread - from) as usize];
        file.seek(std::io::SeekFrom::Start(from)).map_err(read)?;
        file.read_exact(&mut chunk).map_err(read)?;
        chunk.extend_from_slice(&held);
        held = chunk;
        unread = from;
    }
}

/// Remove the folder `path` and everything in it; one gone already is
/// fine.
fn remove_dir(path: &Path) -> anyhow::Result<()> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => anyhow::bail!("remove {}: {e}", path.display()),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::processes::fake::{Behaviour, FakeProcesses};
    use weft_core::infra::wire::Pipe;
    use weft_core::infra::{Endpoint, EnvEntry, InfraSpec, ProcessPort, ProcessReadiness, Unit};
    use weft_core::needs::{Need, NeedStage, PackageManager};
    use weft_platform_traits::unit_agent::{ProcessExit, ProcessRun};
    use weft_platform_traits::{FakeClock, FakeMachineNeeds};

    struct Rig {
        dir: tempfile::TempDir,
        processes: Arc<FakeProcesses>,
        needs: Arc<FakeMachineNeeds>,
        clock: Arc<FakeClock>,
    }

    impl Rig {
        fn new(behaviour: Behaviour) -> Self {
            let processes = Arc::new(FakeProcesses::new());
            processes.behave(behaviour);
            let dir = tempfile::tempdir().unwrap();
            program_in(&dir.path().join("bin"), "python3");
            Self { dir, processes, needs: Arc::new(FakeMachineNeeds::new()), clock: FakeClock::new() }
        }

        fn bin(&self) -> String {
            self.dir.path().join("bin").display().to_string()
        }

        /// One life of the runtime's host, on the rig's folder.
        fn host(&self) -> ProcessUnits {
            ProcessUnits::new(
                self.processes.clone(),
                self.needs.clone(),
                self.clock.clone(),
                ProcessUnitsConfig {
                    dir: self.dir.path().join("infra-processes"),
                    agent_program: "/opt/weft/weft-runtime".into(),
                    machine_env: [("PATH", self.bin()), ("HOME", "/home/ada".to_string())].map(|(k, v)| (k.to_string(), v)).into(),
                    system: Ok(System { manager: PackageManager::Apt, distro: Some("debian_12".into()) }),
                    stop_grace: Duration::from_millis(200),
                    needs_root: self.dir.path().join("needs"),
                },
            )
        }

        fn unit_dir(&self) -> PathBuf {
            self.dir.path().join("infra-processes").join(copy().copy_id).join("server")
        }

        fn agent_state(&self, state: &ProcessAgentState) {
            state.write(&self.unit_dir().join("agent.json")).unwrap();
        }
    }

    fn copy() -> NodeRef {
        NodeRef { tenant: "local".into(), project: uuid::Uuid::from_u128(1), node: "agent".into(), copy_id: "wn-agent-1".into() }
    }

    /// An executable file `name` in the folder `bin`, made if missing.
    pub(crate) fn program_in(bin: &Path, name: &str) {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::create_dir_all(bin).unwrap();
        std::fs::write(bin.join(name), "#!/bin/sh\n").unwrap();
        std::fs::set_permissions(bin.join(name), std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    fn python() -> Need {
        let apt = [(weft_core::needs::DEFAULT_DISTRO.to_string(), vec!["python3".to_string()])].into();
        Need { name: "python3".into(), stage: NeedStage::Run, check: vec!["python3".into(), "--version".into()], install: [(PackageManager::Apt, apt)].into(), how: None, recipe: None }
    }

    /// The agent node's own shape: a small server on a port its readiness
    /// check reads, behind a public endpoint.
    fn node(script: &str, ready_on: &str) -> ResolvedNode {
        let spec = InfraSpec {
            units: vec![Unit {
                name: "server".into(),
                process: Some(
                    Process::new(vec!["python3".into(), "server.py".into()])
                        .with_file("server.py", script)
                        .with_file("lib/util.py", "pass\n")
                        .with_env(vec![EnvEntry::new("MODE", "prod"), EnvEntry::new("HOME", "/home/ada/agent")])
                        .with_ports(vec![ProcessPort::new("http", "PORT")])
                        .with_readiness(ProcessReadiness::http("http", ready_on))
                        .with_needs(vec![python()]),
                ),
                ..Default::default()
            }],
            endpoints: vec![Endpoint {
                name: "api".into(),
                target: EndpointTarget::Process { unit: "server".into(), port: "http".into() },
                expose: Expose::SameNetwork,
            }],
            ..Default::default()
        };
        weft_core::infra::resolve(&spec, &copy(), &BTreeMap::new()).unwrap()
    }

    async fn look(host: &ProcessUnits) -> Vec<UnitObservation> {
        host.observe("local", copy().project).await.unwrap()
    }

    /// An apply writes the process's files into its folder and starts its
    /// agent there, told where to keep its state and log and what to run,
    /// with the machine's environment, the spec's (which wins), and the
    /// port it picked; the endpoint answers that port on loopback. A second
    /// apply of the same unit, still running, starts nothing.
    #[tokio::test]
    async fn an_apply_starts_the_agent_in_the_units_folder_with_its_environment() {
        let rig = Rig::new(Behaviour::Runs);
        let host = rig.host();
        let node = node("serve()\n", "/health");
        host.apply_unit(&node, "server").await.unwrap();
        let spawns = rig.processes.spawns();
        assert_eq!(spawns.len(), 1);
        let spawn = &spawns[0];
        let unit = rig.unit_dir();
        assert_eq!(spawn.program, PathBuf::from("/opt/weft/weft-runtime"));
        let path = |p: &str| unit.join(p).display().to_string();
        assert_eq!(
            spawn.args,
            ["unit-agent", "process", "--state", &path("agent.json"), "--log", &path("process.log"), "--", "python3", "server.py"]
        );
        assert_eq!(spawn.cwd, unit.join("work"));
        assert_eq!(spawn.log, unit.join("agent.log"));
        let port: u16 = spawn.env["PORT"].parse().unwrap();
        assert_eq!(spawn.env["PATH"], rig.bin());
        assert_eq!(spawn.env["HOME"], "/home/ada/agent", "the spec's value wins over the machine's");
        assert_eq!(spawn.env["MODE"], "prod");
        assert_eq!(spawn.env.len(), 4, "nothing else: {:?}", spawn.env);
        assert_eq!(std::fs::read_to_string(unit.join("work/server.py")).unwrap(), "serve()\n");
        assert_eq!(std::fs::read_to_string(unit.join("work/lib/util.py")).unwrap(), "pass\n");
        let at = host.endpoint(&node, "api").await.unwrap();
        assert_eq!(at, EndpointAt { url: format!("http://127.0.0.1:{port}"), same_network: Some(format!("127.0.0.1:{port}")) });

        host.apply_unit(&node, "server").await.unwrap();
        assert_eq!(rig.processes.spawns().len(), 1, "it runs as asked already");
        // A changed script is a changed unit: the old agent stops, the new
        // one starts on the same port, so the endpoint's address holds.
        host.apply_unit(&self::node("serve(2)\n", "/health"), "server").await.unwrap();
        let first = rig.processes.pids()[0];
        assert_eq!(rig.processes.signals(), [(first, Signal::Term), (first, Signal::Kill)]);
        let again = &rig.processes.spawns()[1];
        assert_eq!(again.env["PORT"], port.to_string());
        assert_eq!(std::fs::read_to_string(unit.join("work/server.py")).unwrap(), "serve(2)\n");
    }

    /// A need weft installs itself is never refused at the apply, even
    /// missing: the agent is handed it (`--setup`, a file naming weft's
    /// data folder and the need) and installs it before the process
    /// starts, which a look says, as it says an install that failed (a
    /// failure an apply of the same unit then tries again).
    #[tokio::test]
    async fn a_need_weft_installs_is_the_agents_to_meet_and_a_look_says_how_it_goes() {
        let rig = Rig::new(Behaviour::Runs);
        let host = rig.host();
        let recipe = weft_core::needs::Recipe {
            folder: "agents/fake".into(),
            script: [(weft_core::needs::Os::Linux, "true".to_string())].into(),
            bin: vec!["bin".into()],
            prelude: String::new(),
        };
        let fake = Need { name: "Fake".into(), stage: NeedStage::Run, check: vec!["fakeprog".into()], install: BTreeMap::new(), how: None, recipe: Some(recipe) };
        rig.needs.lacks("Fake");
        let mut node = node("serve()\n", "/health");
        let process = node.units[0].unit.process.as_mut().unwrap();
        process.needs.push(fake.clone());
        process.command = vec!["fakeprog".into(), "serve".into()];
        host.apply_unit(&node, "server").await.unwrap();
        let unit = rig.unit_dir();
        let path = |p: &str| unit.join(p).display().to_string();
        assert_eq!(
            rig.processes.spawns()[0].args,
            ["unit-agent", "process", "--state", &path("agent.json"), "--log", &path("process.log"), "--setup", &path("setup.json"), "--", "fakeprog", "serve"],
            "a program the need installs is not looked for on the PATH"
        );
        let setup: ProcessSetup = serde_json::from_slice(&std::fs::read(unit.join("setup.json")).unwrap()).unwrap();
        assert_eq!(setup, ProcessSetup { root: rig.dir.path().join("needs"), needs: vec![fake] }, "only the need weft installs");

        rig.agent_state(&ProcessAgentState { setting_up: Some("installing Fake (its log: /x)".into()), ..Default::default() });
        assert_eq!(look(&host).await[0].state, UnitRunState::Starting { step: "installing Fake (its log: /x)".into() });
        rig.agent_state(&ProcessAgentState { setup_failed: Some("installing Fake failed".into()), ..Default::default() });
        assert_eq!(look(&host).await[0].state, UnitRunState::Failed { why: "installing Fake failed".into() });
        host.apply_unit(&node, "server").await.unwrap();
        assert_eq!(rig.processes.spawns().len(), 2, "a failed setup is set up again by the next apply");
    }

    /// A need the machine lacks refuses the start, saying how to install it
    /// here, and nothing starts.
    #[tokio::test]
    async fn a_missing_need_refuses_the_start_naming_how_to_install_it() {
        let rig = Rig::new(Behaviour::Runs);
        rig.needs.lacks("python3");
        let e = format!("{:#}", rig.host().apply_unit(&node("serve()\n", "/health"), "server").await.unwrap_err());
        assert!(e.starts_with("infra node 'agent' runs a process (unit 'server') that needs things"), "{e}");
        assert!(e.contains("install them with:  sudo apt-get install -y python3"), "{e}");
        assert!(rig.processes.spawns().is_empty());
    }

    /// A program the command names that is not on the process's `PATH`
    /// refuses the start, naming it, and a running older version stays up;
    /// one named by its folder is looked for there.
    #[tokio::test]
    async fn a_program_not_on_the_path_refuses_the_start() {
        let rig = Rig::new(Behaviour::Runs);
        let host = rig.host();
        host.apply_unit(&node("serve()\n", "/health"), "server").await.unwrap();
        let mut missing = node("serve(2)\n", "/health");
        missing.units[0].unit.process.as_mut().unwrap().command[0] = "python9".into();
        missing.units[0].hash = "changed".into();
        let e = format!("{:#}", host.apply_unit(&missing, "server").await.unwrap_err());
        assert!(e.starts_with("unit 'server' of 'agent' runs `python9`, which is not on its PATH"), "{e}");
        assert!(rig.processes.signals().is_empty(), "the running one stays up");
        assert!(program_found("python3", &format!("/nowhere:{}", rig.bin()), Path::new("/")));
        assert!(!program_found("python3", "", Path::new("/")));
        assert!(program_found("bin/python3", "", rig.dir.path()));
        std::fs::write(rig.dir.path().join("bin/plain"), "x").unwrap();
        assert!(!program_found("plain", &rig.bin(), Path::new("/")), "a file that cannot run is no program");
    }

    /// A look reads the agent's state: starting until it wrote one, then
    /// the readiness check on the process's port (starting inside its
    /// window, not ready after it, ready once it answers), and a process
    /// that exited is said with its exit and its last lines, and counted
    /// as a restart.
    #[tokio::test]
    async fn a_look_reads_the_agent_and_the_readiness_check() {
        let rig = Rig::new(Behaviour::ServesOn("PORT".into()));
        let host = rig.host();
        host.apply_unit(&node("serve()\n", "/missing"), "server").await.unwrap();
        let state = |seen: &[UnitObservation]| seen[0].state.clone();
        assert_eq!(state(&look(&host).await), UnitRunState::Starting { step: "its process is starting".into() });
        let pid = rig.processes.pids()[0];
        let now = rig.clock.now_unix();
        rig.agent_state(&ProcessAgentState { running: Some(ProcessRun { pid: pid + 1, started_at_unix: now }), ..Default::default() });
        assert_eq!(state(&look(&host).await), UnitRunState::Starting { step: "its process has not answered its health check yet".into() });
        rig.clock.advance(Duration::from_secs(31));
        let UnitRunState::NotReady { why } = state(&look(&host).await) else { panic!("not ready past its window") };
        assert!(why.starts_with("its process: GET /missing on") && why.contains("answered 404"), "{why}");

        host.apply_unit(&node("serve()\n", "/_weft/healthz"), "server").await.unwrap();
        let pid = *rig.processes.pids().last().unwrap();
        rig.agent_state(&ProcessAgentState { running: Some(ProcessRun { pid: pid + 1, started_at_unix: now }), ..Default::default() });
        let seen = look(&host).await;
        assert_eq!((seen[0].copy_id.as_str(), seen[0].unit.as_str(), seen[0].state.clone()), ("wn-agent-1", "server", UnitRunState::Ready));
        assert_eq!(seen[0].hash, node("serve()\n", "/_weft/healthz").units[0].hash);
        assert_eq!(seen[0].restart, None);

        let boom = LogLine { at: chrono::DateTime::from_timestamp(now, 0).unwrap(), pipe: Pipe::Stderr, text: "Traceback: boom".into() };
        std::fs::write(rig.unit_dir().join("process.log"), format!("{}\n", boom.stamped())).unwrap();
        rig.agent_state(&ProcessAgentState { running: None, restarts: 1, last_exit: Some(ProcessExit { code: 3, at_unix: now + 5 }), ..Default::default() });
        let seen = look(&host).await;
        let UnitRunState::NotReady { why } = &seen[0].state else { panic!("{:?}", seen[0].state) };
        assert!(why.starts_with("its process exited with code 3 and starts again shortly; its last lines:\nTraceback: boom"), "{why}");
        assert_eq!(
            seen[0].restart,
            Some(UnitRestart { container: PROCESS_SOURCE.into(), stopped_at_unix: now + 5, cause: RestartCause::Exited { code: 3 }, count: 1 })
        );
    }

    /// A stop tells the agent to stop, kills its group once the grace
    /// ran out, keeps the unit's folder, and the unit reads as stopped.
    #[tokio::test]
    async fn a_stop_sends_term_then_kill_and_keeps_the_folder() {
        let rig = Rig::new(Behaviour::Runs);
        let host = rig.host();
        host.apply_unit(&node("serve()\n", "/health"), "server").await.unwrap();
        let pid = rig.processes.pids()[0];
        rig.processes.deaf(pid);
        host.stop_unit(&copy(), "server").await.unwrap();
        assert_eq!(rig.processes.signals(), [(pid, Signal::Term), (pid, Signal::Kill)]);
        assert!(!rig.processes.is_alive(pid));
        assert_eq!(look(&host).await[0].state, UnitRunState::Stopped);
        assert!(rig.unit_dir().join("work/server.py").is_file(), "what it keeps stays");
        assert!(host.holds(&copy(), "server").await.unwrap());
        host.stop_unit(&copy(), "server").await.unwrap();
        assert_eq!(rig.processes.signals().len(), 2, "a stopped unit is sent nothing");
    }

    /// The runtime's next life finds the agent the last one started still
    /// running and adopts it: an apply of the same unit starts nothing,
    /// and its looks read it. An agent found gone is started again, and
    /// counted as a restart for a reason the host does not know.
    #[tokio::test]
    async fn the_next_life_adopts_a_running_agent_and_brings_back_a_gone_one() {
        let rig = Rig::new(Behaviour::Runs);
        let node = node("serve()\n", "/health");
        rig.host().apply_unit(&node, "server").await.unwrap();
        let next = rig.host();
        next.apply_unit(&node, "server").await.unwrap();
        assert_eq!(rig.processes.spawns().len(), 1, "adopted, not started twice");
        assert_eq!(look(&next).await[0].state, UnitRunState::Starting { step: "its process is starting".into() });

        let pid = rig.processes.pids()[0];
        rig.processes.signal(pid, Signal::Kill).unwrap();
        let seen = look(&next).await;
        let UnitRunState::Starting { step } = &seen[0].state else { panic!("{:?}", seen[0].state) };
        assert!(step.contains("was gone"), "{step}");
        assert_eq!(seen[0].restart.as_ref().map(|r| (r.cause.clone(), r.count)), Some((RestartCause::Unknown, 1)));
        assert_eq!(rig.processes.spawns().len(), 2);
        assert_eq!(look(&next).await[0].state, UnitRunState::Starting { step: "its process is starting".into() }, "the new agent runs");
    }

    /// What an agent node's server keeps of its conversation, under its
    /// working folder (`state/`, its default home), as it writes it: its
    /// state, the agent's private home with a login and a session, the
    /// files weft writes for an agent (a plugin), and its logs beside.
    const CONVERSATION: &[&str] = &[
        "work/state/state.json",
        "work/state/codex-home/auth.json",
        "work/state/codex-home/sessions/2026/11/rollout.jsonl",
        "work/state/hermes-home/plugins/weft/__init__.py",
        "work/state/opencode-files/plugin/weft.js",
        "process.log",
        "process.log.1",
        "agent.log",
    ];

    fn write_conversation(rig: &Rig) {
        for path in CONVERSATION {
            let at = rig.unit_dir().join(path);
            std::fs::create_dir_all(at.parent().unwrap()).unwrap();
            std::fs::write(at, "kept\n").unwrap();
        }
    }

    /// A stop keeps everything a unit's process wrote, its conversation
    /// and logs included, for the next start to pick up.
    #[tokio::test]
    async fn a_stop_keeps_what_the_process_wrote() {
        let rig = Rig::new(Behaviour::Runs);
        let host = rig.host();
        host.apply_unit(&node("serve()\n", "/health"), "server").await.unwrap();
        write_conversation(&rig);
        host.stop_unit(&copy(), "server").await.unwrap();
        for path in CONVERSATION {
            assert!(rig.unit_dir().join(path).is_file(), "{path} stays through a stop");
        }
    }

    /// A terminate kills the copy's agents, removes their folders (with
    /// everything their process wrote) and forgets them; a remove does
    /// the same for one unit.
    #[tokio::test]
    async fn a_terminate_forgets_the_copy_and_its_folders() {
        let rig = Rig::new(Behaviour::Runs);
        let host = rig.host();
        host.apply_unit(&node("serve()\n", "/health"), "server").await.unwrap();
        write_conversation(&rig);
        assert_eq!(host.copies().await.unwrap(), [copy()]);
        host.terminate(&copy(), &[]).await.unwrap();
        assert!(!rig.dir.path().join("infra-processes").join(copy().copy_id).exists(), "nothing of the copy is left");
        assert!(rig.dir.path().join("infra-processes/units.json").is_file(), "the registry of every other unit stays");
        assert!(!rig.processes.is_alive(rig.processes.pids()[0]));
        assert!(!rig.unit_dir().parent().unwrap().exists());
        assert!(host.copies().await.unwrap().is_empty());
        assert!(look(&host).await.is_empty());
        assert!(!host.holds(&copy(), "server").await.unwrap());

        host.apply_unit(&node("serve()\n", "/health"), "server").await.unwrap();
        host.remove_unit(&copy(), "server").await.unwrap();
        assert!(!rig.unit_dir().exists() && !host.holds(&copy(), "server").await.unwrap());
    }

    /// The logs are the agent's stamped lines, its older file first: the
    /// last N, or exactly what came after a follower's mark.
    #[tokio::test]
    async fn the_logs_are_the_agents_stamped_lines() {
        let rig = Rig::new(Behaviour::Runs);
        let host = rig.host();
        host.apply_unit(&node("serve()\n", "/health"), "server").await.unwrap();
        let line = |secs: i64, text: &str| LogLine { at: chrono::DateTime::from_timestamp(secs, 0).unwrap(), pipe: Pipe::Stdout, text: text.into() };
        let file = |lines: &[LogLine]| lines.iter().map(|l| format!("{}\n", l.stamped())).collect::<String>();
        std::fs::write(rig.unit_dir().join("process.log.1"), file(&[line(1, "a")])).unwrap();
        std::fs::write(rig.unit_dir().join("process.log"), file(&[line(2, "b"), line(3, "c")])).unwrap();
        let texts = |streams: &[LogStream]| streams[0].lines.iter().map(|l| l.text.clone()).collect::<Vec<_>>();
        let tail = host.logs(&copy(), "server", &LogsFrom::Tail(2)).await.unwrap();
        assert_eq!((tail[0].source.as_str(), texts(&tail)), (PROCESS_SOURCE, vec!["b".to_string(), "c".to_string()]));
        let all = host.logs(&copy(), "server", &LogsFrom::Tail(10)).await.unwrap();
        assert_eq!(texts(&all), ["a", "b", "c"]);
        let mark: BTreeMap<String, LogMark> = [(PROCESS_SOURCE.to_string(), tail[0].mark)].into();
        std::fs::write(rig.unit_dir().join("process.log"), file(&[line(2, "b"), line(3, "c"), line(4, "d")])).unwrap();
        assert_eq!(texts(&host.logs(&copy(), "server", &LogsFrom::After(mark.clone())).await.unwrap()), ["d"]);
        // A line its agent did not write is skipped, and the last line,
        // unfinished while the agent writes it, is left for the next read.
        let unfinished = line(5, "e").stamped();
        std::fs::write(rig.unit_dir().join("process.log"), format!("{}not stamped\n{}{}", file(&[line(2, "b")]), file(&[line(4, "d")]), &unfinished[..10]))
            .unwrap();
        assert_eq!(texts(&host.logs(&copy(), "server", &LogsFrom::Tail(10)).await.unwrap()), ["a", "b", "d"]);
        assert_eq!(texts(&host.logs(&copy(), "server", &LogsFrom::After(mark.clone())).await.unwrap()), ["d"]);
        std::fs::write(rig.unit_dir().join("process.log"), format!("{}{}{unfinished}\n", file(&[line(2, "b")]), file(&[line(4, "d")]))).unwrap();
        assert_eq!(texts(&host.logs(&copy(), "server", &LogsFrom::After(mark)).await.unwrap()), ["d", "e"], "read whole once written");
    }

    /// A log is read back from its end a block at a time, a line split
    /// across blocks read whole, stopping at the line the reader answers
    /// false for and going on into the older file only when it needs to.
    #[test]
    fn a_log_is_read_back_from_its_end_across_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let line = |secs: i64, text: &str| LogLine { at: chrono::DateTime::from_timestamp(secs, 0).unwrap(), pipe: Pipe::Stdout, text: text.into() };
        let file = |lines: &[LogLine]| lines.iter().map(|l| format!("{}\n", l.stamped())).collect::<String>();
        let (current, older) = (dir.path().join("process.log"), dir.path().join("process.log.1"));
        std::fs::write(&older, file(&[line(1, "one"), line(2, "two")])).unwrap();
        let long = "x".repeat(100);
        std::fs::write(&current, file(&[line(3, &long), line(4, ""), line(5, "five")])).unwrap();
        let files = [current, older];
        let texts = |lines: Vec<LogLine>| lines.into_iter().map(|l| l.text).collect::<Vec<_>>();
        for block in [1, 7, 64, LOG_BLOCK] {
            let mut left = 2;
            assert_eq!(texts(read_back(&files, block, take(&mut left)).unwrap()), ["", "five"], "block {block}");
            let mut left = 4;
            assert_eq!(texts(read_back(&files, block, take(&mut left)).unwrap()), ["two", long.as_str(), "", "five"], "block {block}");
            let all = texts(read_back(&files, block, |_| true).unwrap());
            assert_eq!(all, ["one", "two", long.as_str(), "", "five"], "block {block}");
            let after = texts(read_back(&files, block, |l| l.at.timestamp() >= 4).unwrap());
            assert_eq!(after, ["", "five"], "block {block}");
        }
        let mut read = Vec::new();
        lines_back(&mut std::fs::File::open(&files[0]).unwrap(), &files[0], 7, |l| {
            read.push(l.len());
            false
        })
        .unwrap();
        assert_eq!(read.len(), 1, "it stops when told");
        assert!(read_back(&[dir.path().join("none")], 7, |_| true).unwrap().is_empty(), "no file is no lines");
    }

    /// A roll of the log between the opens of its two files (the current
    /// one renamed onto the older one's name, a fresh one made) would hand
    /// the older name the very file just opened as current: the files are
    /// opened again, and every line is read once, in the order written.
    #[test]
    fn a_roll_between_the_opens_reads_every_line_once() {
        let dir = tempfile::tempdir().unwrap();
        let line = |secs: i64, text: &str| LogLine { at: chrono::DateTime::from_timestamp(secs, 0).unwrap(), pipe: Pipe::Stdout, text: text.into() };
        let file = |lines: &[LogLine]| lines.iter().map(|l| format!("{}\n", l.stamped())).collect::<String>();
        let (current, older) = (dir.path().join("process.log"), dir.path().join("process.log.1"));
        std::fs::write(&older, file(&[line(1, "one")])).unwrap();
        std::fs::write(&current, file(&[line(2, "two")])).unwrap();
        let files = [current.clone(), older.clone()];
        let mut opens = 0;
        let mut opened = open_together(&files, |i| {
            opens += 1;
            // The current file just opened, the first time: the agent rolls.
            if i == 0 && opens == 1 {
                std::fs::rename(&current, &older).unwrap();
                std::fs::write(&current, file(&[line(3, "three")])).unwrap();
            }
        })
        .unwrap();
        assert_eq!(opens, 4, "opened again after the roll");
        let texts = read_opened_back(&mut opened, 7, |_| true).unwrap().into_iter().map(|l| l.text).collect::<Vec<_>>();
        assert_eq!(texts, ["two", "three"]);
    }

    /// The registry the next life reads keeps every unit the host holds,
    /// and the install's removal kills each running agent's group.
    #[tokio::test]
    async fn removing_the_install_kills_every_agent() {
        let rig = Rig::new(Behaviour::Runs);
        rig.host().apply_unit(&node("serve()\n", "/health"), "server").await.unwrap();
        let pid = rig.processes.pids()[0];
        assert_eq!(kill_all(rig.processes.as_ref(), &rig.dir.path().join("infra-processes")).await.unwrap(), 1);
        assert_eq!(rig.processes.signals(), [(pid, Signal::Kill)]);
        // Gone already: what it left is killed, while no program has its
        // pid, and nothing once another one took it.
        assert_eq!(kill_all(rig.processes.as_ref(), &rig.dir.path().join("infra-processes")).await.unwrap(), 0);
        assert_eq!(rig.processes.signals(), [(pid, Signal::Kill), (pid, Signal::Kill)]);
        rig.processes.running(pid, "another program");
        assert_eq!(kill_all(rig.processes.as_ref(), &rig.dir.path().join("infra-processes")).await.unwrap(), 0);
        assert_eq!(rig.processes.signals().len(), 2, "another program's process is left alone");
    }

    /// An agent found gone has what it left (its process, when it died
    /// without taking it along) killed before the next one starts, so no
    /// two of the unit's process ever run; once another program took its
    /// pid, that program is left alone, and the next agent starts all the
    /// same.
    #[tokio::test]
    async fn what_a_gone_agent_left_is_killed_before_the_next_one_starts() {
        let rig = Rig::new(Behaviour::Runs);
        let host = rig.host();
        host.apply_unit(&node("serve()\n", "/health"), "server").await.unwrap();
        let first = rig.processes.pids()[0];
        rig.processes.signal(first, Signal::Kill).unwrap();
        let seen = look(&host).await;
        assert!(matches!(&seen[0].state, UnitRunState::Starting { step } if step.contains("was gone")), "{:?}", seen[0].state);
        assert_eq!(rig.processes.signals(), [(first, Signal::Kill), (first, Signal::Kill)], "its group, before the next start");
        assert_eq!(rig.processes.spawns().len(), 2);

        let second = rig.processes.pids()[1];
        rig.processes.signal(second, Signal::Kill).unwrap();
        rig.processes.running(second, "another program");
        let told = rig.processes.signals().len();
        let seen = look(&host).await;
        assert!(matches!(&seen[0].state, UnitRunState::Starting { .. }), "{:?}", seen[0].state);
        assert_eq!(rig.processes.signals().len(), told, "a pid another program holds is sent nothing");
        assert!(rig.processes.is_alive(second));
        assert_eq!(rig.processes.spawns().len(), 3);
    }

    /// An agent found gone whose start again fails reads as failed with
    /// the reason, never as stopped, and every look tries the start again
    /// (counted as one restart, the time it was found gone), so it comes
    /// back by itself once the cause clears. A start that fails at an apply
    /// is what was applied being wrong: every look says it, and none tries
    /// it again until the next apply.
    #[tokio::test]
    async fn a_failed_revive_is_tried_at_every_look_and_a_failed_apply_waits_for_the_next_apply() {
        let rig = Rig::new(Behaviour::Runs);
        let host = rig.host();
        let node = node("serve()\n", "/health");
        host.apply_unit(&node, "server").await.unwrap();
        rig.processes.signal(rig.processes.pids()[0], Signal::Kill).unwrap();
        std::fs::remove_file(rig.dir.path().join("bin/python3")).unwrap();
        let failed = |seen: &[UnitObservation]| {
            let UnitRunState::Failed { why } = &seen[0].state else { panic!("{:?}", seen[0].state) };
            assert!(
                why.starts_with(
                    "its process was gone and starting it again failed (weft tries again at every look): \
                     unit 'server' of 'agent' runs `python3`, which is not on its PATH"
                ),
                "{why}"
            );
            assert_eq!(seen[0].restart.as_ref().map(|r| (r.cause.clone(), r.count)), Some((RestartCause::Unknown, 1)));
        };
        for _ in 0..2 {
            failed(&look(&host).await);
        }
        // The runtime's next life reads the same, and tries too.
        failed(&look(&rig.host()).await);
        assert_eq!(rig.processes.spawns().len(), 1, "no program, so no agent was started");

        // The program is back (the reboot's PATH caught up): the next look
        // brings the unit back with no apply.
        program_in(&rig.dir.path().join("bin"), "python3");
        let seen = look(&host).await;
        assert!(matches!(&seen[0].state, UnitRunState::Starting { step } if step.contains("was gone")), "{:?}", seen[0].state);
        assert_eq!(seen[0].restart.as_ref().map(|r| r.count), Some(1));
        assert_eq!(rig.processes.spawns().len(), 2);
        assert_eq!(look(&host).await[0].state, UnitRunState::Starting { step: "its process is starting".into() });

        rig.processes.behave(Behaviour::Ends { printed: "boom\n".into(), exit: crate::processes::Exit::Code(1) });
        let e = format!("{:#}", host.apply_unit(&self::node("serve(2)\n", "/health"), "server").await.unwrap_err());
        assert!(e.contains("exited as it started"), "{e}");
        let spawned = rig.processes.spawns().len();
        for _ in 0..2 {
            let UnitRunState::Failed { why } = look(&host).await[0].state.clone() else { panic!("failed") };
            assert_eq!(why, e);
        }
        assert_eq!(rig.processes.spawns().len(), spawned, "a look does not start a failed apply again");
        host.stop_unit(&copy(), "server").await.unwrap();
        assert_eq!(look(&host).await[0].state, UnitRunState::Stopped, "a stop is a stop");
    }

    /// What a look finds out with the registry unlocked (a readiness check)
    /// is of the unit as it saw it: a stop, or an apply of a new version,
    /// landing while the check waits for its answer makes the look say what
    /// the registry says now, with its hash, never the old one's answer.
    #[tokio::test]
    async fn a_stop_or_apply_during_a_readiness_check_reads_as_the_registry_says_now() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let rig = Rig::new(Behaviour::Runs);
        let host = rig.host();
        host.apply_unit(&node("serve()\n", "/health"), "server").await.unwrap();
        // The check goes to a port the test answers itself, so the test
        // knows when it is in flight.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut all = load(&host.cfg.dir).unwrap();
        all[0].ports.insert("http".into(), listener.local_addr().unwrap().port());
        host.save(&all).unwrap();
        let running = || {
            let pid = *rig.processes.pids().last().unwrap();
            rig.agent_state(&ProcessAgentState { running: Some(ProcessRun { pid: pid + 1, started_at_unix: rig.clock.now_unix() }), ..Default::default() });
        };
        // Answer the check ready once `meanwhile` is done.
        async fn answer(listener: &tokio::net::TcpListener, meanwhile: impl std::future::Future<Output = ()>) {
            let (mut conn, _) = listener.accept().await.unwrap();
            meanwhile.await;
            let mut request = Vec::new();
            while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                let mut buf = [0; 1024];
                let n = conn.read(&mut buf).await.unwrap();
                assert!(n > 0, "the check hung up");
                request.extend_from_slice(&buf[..n]);
            }
            conn.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n").await.unwrap();
        }

        running();
        let (seen, ()) = tokio::join!(look(&host), answer(&listener, async {}));
        assert_eq!(seen[0].state, UnitRunState::Ready, "nothing changed: the check's answer stands");

        let (seen, ()) = tokio::join!(look(&host), answer(&listener, async { host.stop_unit(&copy(), "server").await.unwrap() }));
        assert_eq!(seen[0].state, UnitRunState::Stopped);
        assert_eq!(seen[0].hash, node("serve()\n", "/health").units[0].hash);

        host.apply_unit(&node("serve()\n", "/health"), "server").await.unwrap();
        running();
        let newer = node("serve(2)\n", "/health");
        let (seen, ()) = tokio::join!(look(&host), answer(&listener, async { host.apply_unit(&newer, "server").await.unwrap() }));
        assert_eq!(
            (seen[0].state.clone(), seen[0].hash.clone()),
            (UnitRunState::Starting { step: "it was started again while weft looked at it; its process is starting".into() }, newer.units[0].hash.clone())
        );
    }

    /// One unit whose agent keeps a state this weft cannot read (one from
    /// another weft) reads as failed, saying what to do, and every other
    /// unit of the project is still looked at.
    #[tokio::test]
    async fn one_unit_that_cannot_be_looked_at_fails_alone() {
        let rig = Rig::new(Behaviour::Runs);
        let host = rig.host();
        let node = node("serve()\n", "/health");
        host.apply_unit(&node, "server").await.unwrap();
        let other_copy = NodeRef { copy_id: "wn-agent-2".into(), ..copy() };
        let other = ResolvedNode { node: other_copy.clone(), ..node.clone() };
        host.apply_unit(&other, "server").await.unwrap();
        std::fs::write(rig.unit_dir().join("agent.json"), r#"{"running":null,"restarts":0,"lastExit":null,"since":"a later weft"}"#).unwrap();
        let seen = look(&host).await;
        assert_eq!(seen.len(), 2);
        let UnitRunState::Failed { why } = &seen.iter().find(|o| o.copy_id == "wn-agent-1").unwrap().state else { panic!("{seen:?}") };
        assert!(why.contains("keeps a state this weft cannot read") && why.contains("weft infra stop --force"), "{why}");
        let fine = seen.iter().find(|o| o.copy_id == "wn-agent-2").unwrap();
        assert_eq!(fine.state, UnitRunState::Starting { step: "its process is starting".into() });
    }
}
