//! Terminating a node that lives inside another node's endpoint.
//!
//! Such a node (`EndpointTarget::Within`) runs no unit of its own: it put
//! a server into the other node (an agent server a node installed and
//! started in a computer), which hands the endpoint's path on to it. The
//! host has nothing of it to remove, so a terminate that only forgot the
//! row would leave all of it running inside the other node, its files on
//! that node's disk, started again at that node's next start. So before
//! the row goes, the supervisor asks the other node to remove it: one
//! `DELETE` of the endpoint's own path (`/apps/agent-x`, never anything
//! below it, which the other node hands on to what lives there). The
//! other node answers 2xx once nothing of it is left, also when nothing
//! was there: a terminate that stopped midway is run again whole.
//!
//! What it cannot reach is said before anything is touched: the other
//! node has a copy that does not answer (stopped, failed, starting), so
//! the terminate fails naming it and how to go on, and the row stays as
//! it was. The one copy it never needs to reach is one with nothing left
//! of what lived inside it: no copy at all (terminated, or never
//! started), or one this same terminate took down first keeping no disk
//! ([`terminate_order`] puts such a node after the one it lives in).

use async_trait::async_trait;
use weft_broker_client::protocol::SupervisorInfraNode;
use weft_core::infra::{EndpointRecord, InfraHandle, WithinRecord};
use weft_core::instance::InstanceId;

/// How the supervisor asks an endpoint to remove what lives at an
/// address. Behind a trait so the lifecycle loop runs against a fake.
#[async_trait]
pub trait InsideRemover: Send + Sync {
    /// `DELETE url`: Ok once the node answering there removed what lives
    /// at it (a 2xx), the error saying what it answered otherwise.
    async fn remove(&self, url: &str) -> anyhow::Result<()>;
}

/// The real one, over HTTP.
pub struct HttpInsideRemover {
    http: reqwest::Client,
}

/// The longest one removal may take: the other node stops what lives
/// there (its server gets a grace to stop its own processes) and deletes
/// its folder. A service-to-service wait nobody controls, so it has an end.
const REMOVE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

impl HttpInsideRemover {
    pub fn new() -> anyhow::Result<Self> {
        let http = weft_core::net::http_client().timeout(REMOVE_TIMEOUT).build()?;
        Ok(Self { http })
    }
}

#[async_trait]
impl InsideRemover for HttpInsideRemover {
    async fn remove(&self, url: &str) -> anyhow::Result<()> {
        let answer = self.http.delete(url).send().await.map_err(|e| anyhow::anyhow!("DELETE {url}: {e}"))?;
        let status = answer.status();
        if status.is_success() {
            return Ok(());
        }
        let body = answer.text().await.unwrap_or_default();
        anyhow::bail!("DELETE {url} answered {status}: {}", body.trim())
    }
}

/// One removal a terminate asks for: the node that endpoint `endpoint`
/// lives inside (`host`, at `path` under its endpoint), and the address
/// the `DELETE` goes to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Removal {
    pub endpoint: String,
    pub host: InfraHandle,
    pub path: String,
    pub url: String,
}

/// A copy this terminate already took down: whether it kept a disk,
/// which then still holds what lived inside it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TakenDown {
    pub node_id: String,
    pub instance: Option<InstanceId>,
    pub kept_disks: bool,
}

/// The copy of `nodes` a handle names.
fn copy_of<'a>(nodes: &'a [SupervisorInfraNode], handle: &InfraHandle) -> Option<&'a SupervisorInfraNode> {
    nodes.iter().find(|n| n.node_id == handle.place() && n.instance.as_ref() == handle.instance())
}

/// What `node` lives inside, once per other node's endpoint and path.
fn inside(node: &SupervisorInfraNode) -> impl Iterator<Item = (&String, &WithinRecord)> {
    node.addresses.urls.iter().filter_map(|(name, record)| match record {
        EndpointRecord::Within(within) => Some((name, within)),
        EndpointRecord::Url(_) => None,
    })
}

/// The removals a terminate of `node` asks for, from the project's rows
/// (`nodes`) and the copies this terminate took down already
/// (`taken_down`), or why it cannot ask them, for a person. Never more
/// than one per address: two endpoints of one node at the same path are
/// one thing living there.
pub fn removals(node: &SupervisorInfraNode, nodes: &[SupervisorInfraNode], taken_down: &[TakenDown]) -> Result<Vec<Removal>, String> {
    let name = weft_core::project::plain_id(&node.node_id);
    let mut out: Vec<Removal> = Vec::new();
    for (endpoint, WithinRecord { within, path }) in inside(node) {
        let host = weft_core::project::plain_id(within.place());
        let taken = taken_down.iter().find(|t| t.node_id == within.place() && t.instance.as_ref() == within.instance());
        if let Some(taken) = taken {
            if taken.kept_disks {
                return Err(format!(
                    "'{name}' lives inside '{host}' (endpoint '{endpoint}' at {path}), which this terminate took down \
                     keeping its disks: what '{name}' put there is still on them, and nothing answers to remove it. \
                     Start '{host}' again (`weft infra start {host}`) and terminate '{name}', or terminate '{host}' \
                     deleting its disks"
                ));
            }
            // Gone with its disk: nothing of this node is left there.
            continue;
        }
        let Some(inner) = copy_of(nodes, within) else {
            // No copy of it at all: nothing of this node can be in it.
            continue;
        };
        if !inner.status.answers() {
            return Err(format!(
                "'{name}' lives inside '{host}' (endpoint '{endpoint}' at {path}), which is {}, so what '{name}' put \
                 there cannot be removed and '{name}' was left as it was. Start '{host}' (`weft infra start {host}`) \
                 and terminate '{name}' again, or terminate '{host}' too, which removes everything inside it",
                inner.status.as_str()
            ));
        }
        let url = match inner.addresses.urls.get(within.endpoint()) {
            Some(EndpointRecord::Url(url)) => weft_core::infra::within_url(url, path),
            Some(EndpointRecord::Within(_)) => {
                return Err(format!(
                    "'{name}' lives inside {within}, which is itself inside another node's endpoint: an endpoint \
                     inside one is followed one step only, so nothing can be asked to remove it"
                ))
            }
            None => return Err(format!("'{name}' lives inside {within}, which that node does not declare")),
        };
        if out.iter().all(|r| r.url != url) {
            out.push(Removal { endpoint: endpoint.clone(), host: within.clone(), path: path.clone(), url });
        }
    }
    Ok(out)
}

/// The order one terminate takes `targets` down in. A node living inside
/// another that answers goes first, so it is asked to remove it while it
/// still runs; one living inside another this terminate also takes down
/// and that does not answer goes after it, so it finds that copy gone
/// rather than one it cannot reach (and a cancel between the two never
/// leaves its things inside a copy that stays). Otherwise the order is
/// kept.
pub fn terminate_order<'a>(targets: &[&'a SupervisorInfraNode]) -> Vec<&'a SupervisorInfraNode> {
    // Ranked: 0 a node inside others that answer (or that this terminate
    // leaves alone), 1 every other node, 2 a node inside one this
    // terminate takes down that does not answer.
    let rank = |n: &SupervisorInfraNode| {
        let mut hosts = inside(n).map(|(_, w)| &w.within).peekable();
        if hosts.peek().is_none() {
            return 1;
        }
        let after_its_host = hosts.any(|h| targets.iter().any(|t| t.node_id == h.place() && t.instance.as_ref() == h.instance() && !t.status.answers()));
        if after_its_host {
            2
        } else {
            0
        }
    };
    let mut ordered = targets.to_vec();
    ordered.sort_by_key(|n| rank(n));
    ordered
}

#[cfg(any(test, feature = "test-helpers"))]
pub mod fake {
    use super::*;

    /// Removes nothing: records every address asked, and answers each
    /// with what was set for it (Ok when nothing was).
    #[derive(Default)]
    pub struct FakeInsideRemover {
        asked: std::sync::Mutex<Vec<String>>,
        failing: std::sync::Mutex<std::collections::BTreeMap<String, String>>,
    }

    impl FakeInsideRemover {
        /// Every address a removal was asked of, in order.
        pub fn asked(&self) -> Vec<String> {
            self.asked.lock().unwrap().clone()
        }

        /// Answer a removal of `url` with the error `why` from now on.
        pub fn fail(&self, url: &str, why: &str) {
            self.failing.lock().unwrap().insert(url.to_string(), why.to_string());
        }

        /// Answer a removal of `url` with success again.
        pub fn heal(&self, url: &str) {
            self.failing.lock().unwrap().remove(url);
        }
    }

    #[async_trait]
    impl InsideRemover for FakeInsideRemover {
        async fn remove(&self, url: &str) -> anyhow::Result<()> {
            self.asked.lock().unwrap().push(url.to_string());
            match self.failing.lock().unwrap().get(url) {
                Some(why) => anyhow::bail!("{why}"),
                None => Ok(()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_broker_client::protocol::{AppliedEndpoints, InfraNodeStatus};

    fn row(node_id: &str, status: InfraNodeStatus, urls: Vec<(&str, EndpointRecord)>) -> SupervisorInfraNode {
        SupervisorInfraNode {
            node_id: node_id.into(),
            instance: None,
            copy_id: format!("wn-{node_id}"),
            status,
            applied_spec_hash: None,
            applied_at_unix: None,
            addresses: AppliedEndpoints { urls: urls.into_iter().map(|(n, r)| (n.to_string(), r)).collect(), ..Default::default() },
            keep_disks: Vec::new(),
            units: Default::default(),
            health_rules: None,
        }
    }

    fn computer(status: InfraNodeStatus) -> SupervisorInfraNode {
        row("box", status, vec![("api", EndpointRecord::Url("http://127.0.0.1:4100/".into()))])
    }

    fn agent(path: &str) -> SupervisorInfraNode {
        let within = WithinRecord { within: InfraHandle::new("box", "api", None), path: path.into() };
        row("claude", InfraNodeStatus::Running, vec![("api", EndpointRecord::Within(within))])
    }

    #[test]
    fn a_node_inside_a_running_one_is_removed_at_its_own_path() {
        let nodes = vec![computer(InfraNodeStatus::Running), agent("/apps/agent-claude")];
        let asked = removals(&nodes[1], &nodes, &[]).unwrap();
        assert_eq!(asked.len(), 1);
        assert_eq!(asked[0].url, "http://127.0.0.1:4100/apps/agent-claude");
        assert_eq!(asked[0].host, InfraHandle::new("box", "api", None));
        let flaky = vec![computer(InfraNodeStatus::Flaky), agent("/apps/agent-claude")];
        assert_eq!(removals(&flaky[1], &flaky, &[]).unwrap().len(), 1, "a flaky copy answers");
    }

    #[test]
    fn a_node_with_no_endpoint_inside_another_asks_nothing() {
        let nodes = vec![computer(InfraNodeStatus::Running)];
        assert!(removals(&nodes[0], &nodes, &[]).unwrap().is_empty());
    }

    #[test]
    fn two_endpoints_at_one_path_are_one_removal() {
        let within = WithinRecord { within: InfraHandle::new("box", "api", None), path: "/apps/a".into() };
        let node = row("claude", InfraNodeStatus::Running, vec![
            ("api", EndpointRecord::Within(within.clone())),
            ("live", EndpointRecord::Within(within)),
        ]);
        let nodes = vec![computer(InfraNodeStatus::Running), node];
        assert_eq!(removals(&nodes[1], &nodes, &[]).unwrap().len(), 1);
    }

    #[test]
    fn a_stopped_host_refuses_the_terminate_saying_how_to_go_on() {
        let nodes = vec![computer(InfraNodeStatus::Stopped), agent("/apps/agent-claude")];
        let why = removals(&nodes[1], &nodes, &[]).unwrap_err();
        assert!(why.contains("'claude' lives inside 'box'") && why.contains("which is stopped"), "{why}");
        assert!(why.contains("weft infra start box") && why.contains("terminate 'box' too"), "{why}");
    }

    #[test]
    fn a_host_with_no_copy_or_taken_down_with_its_disk_asks_nothing() {
        let alone = vec![agent("/apps/agent-claude")];
        assert!(removals(&alone[0], &alone, &[]).unwrap().is_empty(), "no copy: nothing of it is left there");
        let nodes = vec![computer(InfraNodeStatus::Stopped), agent("/apps/agent-claude")];
        let gone = TakenDown { node_id: "box".into(), instance: None, kept_disks: false };
        assert!(removals(&nodes[1], &nodes, &[gone]).unwrap().is_empty());
        let kept = TakenDown { node_id: "box".into(), instance: None, kept_disks: true };
        let why = removals(&nodes[1], &nodes, &[kept]).unwrap_err();
        assert!(why.contains("keeping its disks"), "{why}");
    }

    #[test]
    fn a_host_inside_another_is_refused() {
        let inner = WithinRecord { within: InfraHandle::new("outer", "api", None), path: "/x".into() };
        let nodes = vec![row("box", InfraNodeStatus::Running, vec![("api", EndpointRecord::Within(inner))]), agent("/apps/a")];
        assert!(removals(&nodes[1], &nodes, &[]).unwrap_err().contains("followed one step only"));
        let undeclared = vec![row("box", InfraNodeStatus::Running, vec![]), agent("/apps/a")];
        assert!(removals(&undeclared[1], &undeclared, &[]).unwrap_err().contains("does not declare"));
    }

    #[test]
    fn a_node_inside_another_goes_first_unless_that_one_does_not_answer() {
        let (up, plain) = (computer(InfraNodeStatus::Running), row("db", InfraNodeStatus::Running, vec![]));
        let claude = agent("/apps/a");
        let order: Vec<&str> = terminate_order(&[&up, &plain, &claude]).iter().map(|n| n.node_id.as_str()).collect();
        assert_eq!(order, ["claude", "box", "db"]);
        let down = computer(InfraNodeStatus::Stopped);
        let order: Vec<&str> = terminate_order(&[&claude, &down, &plain]).iter().map(|n| n.node_id.as_str()).collect();
        assert_eq!(order, ["box", "db", "claude"], "after the one it lives in, which goes with its disk");
        let order: Vec<&str> = terminate_order(&[&claude]).iter().map(|n| n.node_id.as_str()).collect();
        assert_eq!(order, ["claude"], "a host this terminate does not take down orders nothing");
    }
}
