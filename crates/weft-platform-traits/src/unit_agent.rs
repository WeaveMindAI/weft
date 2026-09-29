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

/// What a cloud machine runs: one unit of one copy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnitAssignment {
    pub node: weft_core::infra::ResolvedNode,
    pub unit: String,
}

/// Read the assignment again and bring the unit to it.
pub const HOST_APPLY: &str = "/host/apply";
/// Restart the unit's containers as they are.
pub const HOST_RESTART: &str = "/host/restart";
/// How the unit is doing: a list of `UnitObservation`.
pub const HOST_OBSERVE: &str = "/host/observe";
/// The unit's output: `?from=<LogsFrom as JSON>`, answered as a JSON
/// list of `LogStream` (`InfraHost::logs`).
pub const HOST_LOGS: &str = "/host/logs";

#[cfg(test)]
mod tests {
    use super::*;

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
