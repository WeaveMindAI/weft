//! The small agent that sits beside every infra unit and answers, from
//! inside the unit's own network, whether a readiness or liveness check
//! passes.
//!
//! A unit's ports are reachable only on the network its containers share,
//! which the process running weft is not on (Docker Desktop keeps
//! container networks inside its own VM; a cloud machine keeps them on
//! the machine). The agent is on it: it holds the unit's network, so a
//! check it runs against `127.0.0.1:<port>` reaches exactly what the
//! unit's own containers reach. Weft asks it over one port published for
//! the purpose. The agent is `weft-runtime unit-agent`.
//!
//! A unit that runs a process on the machine instead of containers has an
//! agent too, which runs the process and keeps how it is doing in a file
//! ([`ProcessAgentState`]).

use serde::{Deserialize, Serialize};
use weft_core::infra::ProbeKind;

/// Where the agent answers a check.
pub const PROBE_PATH: &str = "/probe";

/// Run one check: a `Tcp` or `Http` probe (an `Exec` probe runs inside
/// its own container, never through the agent).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeRequest {
    pub probe: ProbeKind,
    pub timeout_seconds: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeAnswer {
    pub ok: bool,
    /// Why it failed, in a few words (a refused connection, a status).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub why: Option<String>,
}

/// Where a unit's own containers tell weft that something its infra node
/// handed weft changed, with no node running: `POST` a
/// `weft_core::infra::bake::PushedValues` (a password the container
/// changed itself goes in `connection`, a baked output in `outputs`). The
/// agent passes it straight on to the broker as its copy
/// (`identity::Principal::InfraCopy`), which writes it over that copy's
/// values only, and answers with what the broker said: `200` once it
/// landed, the broker's refusal (naming what it could not write)
/// otherwise. The agent holds the unit's network, so a container reaches
/// it at `127.0.0.1` on the agent's port (`weft_core::ports::UNIT_AGENT`).
pub const VALUES_PATH: &str = "/values";

/// Where the agent reaches the broker, set by whoever starts it.
pub const AGENT_BROKER_URL_ENV: &str = "WEFT_BROKER_URL";

/// The agent's identity (`token:<token>`, minted for its copy, or
/// `gcp-metadata`, the machine it runs on), set by whoever starts it.
pub const AGENT_IDENTITY_ENV: &str = "WEFT_AGENT_IDENTITY";

/// What a cloud machine runs: one unit of one copy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnitAssignment {
    pub node: weft_core::infra::ResolvedNode,
    pub unit: String,
}

/// Read the assignment again and bring the unit to it.
pub const HOST_APPLY: &str = "/host/apply";
/// How the unit is doing: a list of `UnitObservation`.
pub const HOST_OBSERVE: &str = "/host/observe";
/// The unit's output: `?from=<LogsFrom as JSON>`, answered as a JSON
/// list of `LogStream` (`InfraHost::logs`).
pub const HOST_LOGS: &str = "/host/logs";

/// How the process of a process unit (`weft_core::infra::Process`) is
/// doing, as the agent that runs it (`weft-runtime unit-agent process`)
/// keeps it in a file of the unit's for the host to read: written whole
/// and renamed over the last one, so a reader sees one state or the next.
/// The agent outlives weft's runtime, which is why it is a file and not
/// an answer: the runtime's next life reads what the agent wrote while it
/// was gone.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProcessAgentState {
    /// The process running now; `None` while the agent waits to start it
    /// again after it exited.
    pub running: Option<ProcessRun>,
    /// How many times it exited on its own and was started again since
    /// the agent started.
    pub restarts: u32,
    /// The latest time it exited on its own.
    pub last_exit: Option<ProcessExit>,
    /// What the agent is doing before it first starts the process: meeting
    /// the needs weft installs itself ([`ProcessSetup`]), said as a person
    /// reads it (`weft_core::needs::setup::SetupStep::words`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setting_up: Option<String>,
    /// Why meeting those needs failed: the agent never starts the process
    /// and waits to be stopped, so the unit reads as failed for this
    /// reason until it is applied again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setup_failed: Option<String>,
}

/// What a process unit's agent meets before it starts the process: the
/// needs of the process weft installs itself (`weft_core::needs::Recipe`),
/// under the needs folder `root` (`needs/` in weft's data folder), each
/// one's program folders then put
/// first on the process's `PATH`. Written by the host into a file the agent
/// is pointed at (`--setup`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProcessSetup {
    pub root: std::path::PathBuf,
    pub needs: Vec<weft_core::needs::Need>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProcessRun {
    pub pid: u32,
    pub started_at_unix: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProcessExit {
    /// Its exit code; for a process a signal ended, 128 plus the signal,
    /// as a shell and Docker say it.
    pub code: i32,
    pub at_unix: i64,
}

impl ProcessAgentState {
    /// The state the agent keeps at `path`; `None` before it wrote one.
    pub fn read(path: &std::path::Path) -> anyhow::Result<Option<Self>> {
        match std::fs::read(path) {
            Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(|e| anyhow::anyhow!("{} is no process agent's state: {e}", path.display())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => anyhow::bail!("read {}: {e}", path.display()),
        }
    }

    /// Make this the state at `path`: written beside it, then renamed over
    /// it.
    pub fn write(&self, path: &std::path::Path) -> anyhow::Result<()> {
        let mut partial = path.as_os_str().to_owned();
        partial.push(".tmp");
        std::fs::write(&partial, serde_json::to_vec(self).expect("a state serializes"))
            .and_then(|()| std::fs::rename(&partial, path))
            .map_err(|e| anyhow::anyhow!("write {}: {e}", path.display()))
    }
}

/// The words after the program a process unit's agent is started with
/// (`weft-runtime`), before its own arguments: `--state <file>` (where it
/// keeps [`ProcessAgentState`]), `--log <file>` (where it writes what the
/// process prints, each line stamped as `LogLine::stamped` writes it),
/// `--setup <file>` when the process has needs weft installs
/// ([`ProcessSetup`]), then `--` and the process's command.
// SYNC: PROCESS_AGENT_COMMAND <-> crates/weft-runtime/src/main.rs (AgentCommand::Process)
pub const PROCESS_AGENT_COMMAND: [&str; 2] = ["unit-agent", "process"];

#[cfg(test)]
mod tests {
    use super::*;

    /// The state the agent writes reads back whole; none written yet reads
    /// as none.
    #[test]
    fn a_process_agents_state_round_trips_through_its_file() {
        let dir = std::env::temp_dir().join(format!("weft-agent-state-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("agent.json");
        assert_eq!(ProcessAgentState::read(&path).unwrap(), None);
        let state = ProcessAgentState {
            running: Some(ProcessRun { pid: 7, started_at_unix: 100 }),
            restarts: 2,
            last_exit: Some(ProcessExit { code: 137, at_unix: 99 }),
            setting_up: None,
            setup_failed: Some("installing Codex failed".into()),
        };
        state.write(&path).unwrap();
        assert_eq!(ProcessAgentState::read(&path).unwrap(), Some(state));
        let wire: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(wire["lastExit"]["atUnix"], 99);
        assert_eq!(wire["setupFailed"], "installing Codex failed");
        assert!(wire.get("settingUp").is_none(), "left out when not setting up");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_probe_request_round_trips() {
        let r = ProbeRequest { probe: ProbeKind::Http { path: "/health".into(), port: 8090 }, timeout_seconds: 1 };
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v["probe"]["kind"], "http");
        assert_eq!(serde_json::from_value::<ProbeRequest>(v).unwrap(), r);
        let a: ProbeAnswer = serde_json::from_value(serde_json::json!({ "ok": true })).unwrap();
        assert_eq!(a, ProbeAnswer { ok: true, why: None });
    }
}
