//! Where a project's infrastructure runs.
//!
//! The supervisor decides WHAT should run (which copy of which node, at
//! which version, started or stopped) and keeps the rows that say so. The
//! host only does it: runs a resolved unit, stops it, removes it, and says
//! how each unit is doing. Locally a unit is a group of containers on the
//! Docker daemon; on a cloud it is a machine of its own running the same
//! containers (the only place a GPU is attached).

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use weft_core::infra::wire::{LogStream, LogsFrom};
use weft_core::infra::{NodeRef, ResolvedNode};

/// How one unit is doing, as the host sees it right now.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum UnitRunState {
    /// Being created or started; not ready yet.
    Starting,
    /// Running and every readiness check passes.
    Ready,
    /// Running, but a readiness check fails (or has not passed yet).
    NotReady { why: String },
    /// Stopped on purpose; disks kept.
    Stopped,
    /// Exited or could not start; `why` is what the host saw.
    Failed { why: String },
}

/// One unit the host runs for a project.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnitObservation {
    pub instance: String,
    pub unit: String,
    /// The hash of the resolved unit it runs (`ResolvedUnit::hash`).
    pub hash: String,
    pub state: UnitRunState,
}

#[async_trait]
pub trait InfraHost: Send + Sync {
    /// Refuse, before anything starts, a node this host cannot run (a
    /// GPU on a machine that has none, a machine shape the platform
    /// cannot give), naming what to change.
    fn check(&self, node: &ResolvedNode) -> Result<(), String>;

    /// Bring `unit` of `node` up as resolved: its disks created if
    /// missing, the running copy replaced when its hash differs, started
    /// when stopped. Returns once started; readiness is read with
    /// [`InfraHost::observe`].
    async fn apply_unit(&self, node: &ResolvedNode, unit: &str) -> anyhow::Result<()>;

    /// Stop `unit`, keeping its disks. Stopping a stopped or absent unit
    /// is not an error.
    async fn stop_unit(&self, node: &NodeRef, unit: &str) -> anyhow::Result<()>;

    /// Restart `unit`'s containers as they are (a health protocol's
    /// "kick the process"), keeping its disks. A unit that is not running
    /// is started.
    async fn restart_unit(&self, node: &NodeRef, unit: &str) -> anyhow::Result<()>;

    /// Remove `unit` (a unit the spec no longer declares), keeping every
    /// disk. Idempotent.
    async fn remove_unit(&self, node: &NodeRef, unit: &str) -> anyhow::Result<()>;

    /// Remove everything the copy runs and owns, disks included except
    /// those named in `keep_disks`. The caller decides what to keep: a
    /// terminate of a copy that may come back keeps what its node listed
    /// in `keepOnTerminate`, and the sweep of a copy gone for good keeps
    /// nothing. Idempotent.
    async fn terminate(&self, node: &NodeRef, keep_disks: &[String]) -> anyhow::Result<()>;

    /// Every unit this host runs for `project`, whatever its state.
    async fn observe(&self, tenant: &str, project: uuid::Uuid) -> anyhow::Result<Vec<UnitObservation>>;

    /// Every copy this host holds anything for, whatever its project's
    /// fate, a copy that holds only the disks a terminate kept included:
    /// what the supervisor sweeps for copies that are gone for good.
    async fn copies(&self) -> anyhow::Result<Vec<NodeRef>>;

    /// Where `endpoint` of `node` answers for the project's workers (a
    /// URL, or `tcp://host:port`), and for a `SameNetwork` endpoint the
    /// address on the install's network.
    async fn endpoint(&self, node: &ResolvedNode, endpoint: &str) -> anyhow::Result<EndpointAt>;

    /// What `unit`'s containers wrote, one stream per container in the
    /// order written: the last lines of each (`LogsFrom::Tail`), or
    /// exactly the lines after a follower's marks (`LogsFrom::After`).
    /// Each stream carries the mark a follower resumes from, made here
    /// because only the host reads the clock that stamps the lines.
    async fn logs(&self, node: &NodeRef, unit: &str, from: &LogsFrom) -> anyhow::Result<Vec<LogStream>>;
}

/// Where one endpoint answers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointAt {
    /// For the project's own workers.
    pub url: String,
    /// For weft's own roles (the same as `url` unless the workers sit on
    /// a network weft's roles are not on).
    pub install_url: String,
    /// For callers on the install's network, for a `SameNetwork` endpoint.
    pub same_network: Option<String>,
}

#[cfg(any(test, feature = "test-helpers"))]
pub mod fake {
    use super::*;
    use parking_lot::Mutex;
    use std::collections::BTreeMap;

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum HostCall {
        Apply { instance: String, unit: String, hash: String },
        Stop { instance: String, unit: String },
        Restart { instance: String, unit: String },
        Remove { instance: String, unit: String },
        Terminate { instance: String, keep: Vec<String> },
    }

    /// Records every call and keeps a plain map of what "runs". An apply
    /// lands a unit `Ready`; a test sets another state with
    /// [`FakeInfraHost::set_state`].
    #[derive(Default)]
    pub struct FakeInfraHost {
        calls: Mutex<Vec<HostCall>>,
        units: Mutex<BTreeMap<(uuid::Uuid, String, String), UnitObservation>>,
        /// The copy each applied instance belongs to, kept after a
        /// terminate that kept disks (the copy still holds them).
        copies: Mutex<BTreeMap<String, NodeRef>>,
        /// The disks each terminated copy still holds.
        kept: Mutex<BTreeMap<String, Vec<String>>>,
        refuse: Mutex<Option<String>>,
        fail_apply: Mutex<Option<String>>,
        /// Copies whose terminate fails (recorded, nothing removed).
        fail_terminate: Mutex<std::collections::BTreeSet<String>>,
        hang_restarts: Mutex<bool>,
    }

    impl FakeInfraHost {
        pub fn new() -> Self {
            Self::default()
        }
        pub fn calls(&self) -> Vec<HostCall> {
            self.calls.lock().clone()
        }
        /// Set how a unit is doing, as if it ran already (it is added
        /// when absent: a unit the supervisor applied before this fake
        /// existed, from a seeded row).
        pub fn set_state(&self, node: &NodeRef, unit: &str, state: UnitRunState) {
            self.copies.lock().insert(node.instance.clone(), node.clone());
            let mut units = self.units.lock();
            let o = units.entry((node.project, node.instance.clone(), unit.to_string())).or_insert_with(|| {
                UnitObservation { instance: node.instance.clone(), unit: unit.into(), hash: String::new(), state: UnitRunState::Ready }
            });
            o.state = state;
        }
        /// Forget everything the fake runs for `project`, recording
        /// nothing: a test resetting what the host reports.
        pub fn clear_project(&self, project: uuid::Uuid) {
            self.units.lock().retain(|(p, _, _), _| *p != project);
            self.copies.lock().retain(|_, c| c.project != project);
        }
        /// The disks a terminate left `instance` holding.
        pub fn kept_disks(&self, instance: &str) -> Vec<String> {
            self.kept.lock().get(instance).cloned().unwrap_or_default()
        }
        /// Make every restart record its call and then never return.
        pub fn hang_restarts(&self) {
            *self.hang_restarts.lock() = true;
        }
        /// Make the next applies fail with `why`.
        pub fn fail_applies_with(&self, why: Option<&str>) {
            *self.fail_apply.lock() = why.map(str::to_string);
        }
        /// Make every terminate of `instance` fail.
        pub fn fail_terminates_of(&self, instance: &str) {
            self.fail_terminate.lock().insert(instance.to_string());
        }
    }

    #[async_trait]
    impl InfraHost for FakeInfraHost {
        fn check(&self, _node: &ResolvedNode) -> Result<(), String> {
            match self.refuse.lock().clone() {
                Some(why) => Err(why),
                None => Ok(()),
            }
        }
        async fn apply_unit(&self, node: &ResolvedNode, unit: &str) -> anyhow::Result<()> {
            let hash = node.unit(unit).map(|u| u.hash.clone()).unwrap_or_default();
            self.calls.lock().push(HostCall::Apply { instance: node.node.instance.clone(), unit: unit.into(), hash: hash.clone() });
            if let Some(why) = self.fail_apply.lock().clone() {
                anyhow::bail!(why);
            }
            self.copies.lock().insert(node.node.instance.clone(), node.node.clone());
            self.units.lock().insert(
                (node.node.project, node.node.instance.clone(), unit.to_string()),
                UnitObservation { instance: node.node.instance.clone(), unit: unit.into(), hash, state: UnitRunState::Ready },
            );
            Ok(())
        }
        async fn stop_unit(&self, node: &NodeRef, unit: &str) -> anyhow::Result<()> {
            self.calls.lock().push(HostCall::Stop { instance: node.instance.clone(), unit: unit.into() });
            if let Some(o) = self.units.lock().get_mut(&(node.project, node.instance.clone(), unit.to_string())) {
                o.state = UnitRunState::Stopped;
            }
            Ok(())
        }
        async fn restart_unit(&self, node: &NodeRef, unit: &str) -> anyhow::Result<()> {
            self.calls.lock().push(HostCall::Restart { instance: node.instance.clone(), unit: unit.into() });
            if *self.hang_restarts.lock() {
                std::future::pending::<()>().await;
            }
            Ok(())
        }
        async fn remove_unit(&self, node: &NodeRef, unit: &str) -> anyhow::Result<()> {
            self.calls.lock().push(HostCall::Remove { instance: node.instance.clone(), unit: unit.into() });
            self.units.lock().remove(&(node.project, node.instance.clone(), unit.to_string()));
            Ok(())
        }
        async fn terminate(&self, node: &NodeRef, keep_disks: &[String]) -> anyhow::Result<()> {
            self.calls.lock().push(HostCall::Terminate { instance: node.instance.clone(), keep: keep_disks.to_vec() });
            if self.fail_terminate.lock().contains(&node.instance) {
                anyhow::bail!("terminating {} failed", node.instance);
            }
            self.units.lock().retain(|(p, i, _), _| !(*p == node.project && *i == node.instance));
            // A copy that held something keeps the disks it is told to,
            // and with them its place in `copies`.
            let held = self.copies.lock().contains_key(&node.instance);
            if keep_disks.is_empty() || !held {
                self.copies.lock().remove(&node.instance);
                self.kept.lock().remove(&node.instance);
            } else {
                self.kept.lock().insert(node.instance.clone(), keep_disks.to_vec());
            }
            Ok(())
        }
        async fn observe(&self, _tenant: &str, project: uuid::Uuid) -> anyhow::Result<Vec<UnitObservation>> {
            Ok(self.units.lock().iter().filter(|((p, _, _), _)| *p == project).map(|(_, o)| o.clone()).collect())
        }
        async fn copies(&self) -> anyhow::Result<Vec<NodeRef>> {
            Ok(self.copies.lock().values().cloned().collect())
        }
        async fn endpoint(&self, node: &ResolvedNode, endpoint: &str) -> anyhow::Result<EndpointAt> {
            Ok(EndpointAt {
                url: format!("http://{}.{endpoint}.fake", node.node.instance),
                install_url: format!("http://{}.{endpoint}.install.fake", node.node.instance),
                same_network: Some(format!("{}.{endpoint}.fake:1", node.node.instance)),
            })
        }
        async fn logs(&self, node: &NodeRef, unit: &str, _from: &LogsFrom) -> anyhow::Result<Vec<LogStream>> {
            Ok(vec![LogStream {
                source: "app".into(),
                lines: vec![weft_core::infra::wire::LogLine {
                    at: Default::default(),
                    pipe: weft_core::infra::wire::Pipe::Stdout,
                    text: format!("logs of {} {unit}", node.instance),
                }],
                mark: weft_core::infra::wire::LogMark { at: Default::default(), stdout_seen: 1, stderr_seen: 0 },
            }])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_unit_state_round_trips() {
        for s in [
            UnitRunState::Starting,
            UnitRunState::Ready,
            UnitRunState::NotReady { why: "probe".into() },
            UnitRunState::Stopped,
            UnitRunState::Failed { why: "exit 1".into() },
        ] {
            let v = serde_json::to_value(&s).unwrap();
            assert_eq!(serde_json::from_value::<UnitRunState>(v).unwrap(), s);
        }
    }
}
