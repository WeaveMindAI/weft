//! Infra node lifecycle: provision, wait-to-running, read outputs, terminate.
//!
//! Infra nodes are long-running backing services the platform provisions on
//! k8s. The rig drives them through the real CLI (`weft infra start|terminate`,
//! the user's path) and observes status through the dispatcher API
//! (`GET /projects/{id}/infra/status` -> `{ nodes: [{ node_id, status,
//! endpoint_url, ... }] }`). The status values track the supervisor's state
//! machine: provisioning -> running, stopped, flaky, failed, (gone after
//! terminate).

use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde_json::Value;
use uuid::Uuid;

use crate::client::{poll_until, Dispatcher};
use crate::project::Project;

/// How long to wait for an infra node to become `running`. Provisioning pulls /
/// builds an image, applies manifests, and waits for the pod's readiness probe,
/// so this is generous. It is an internal transition the rig controls, so a
/// bound is correct.
const INFRA_RUNNING_DEADLINE: Duration = Duration::from_secs(300);
const INFRA_POLL: Duration = Duration::from_millis(750);

/// One infra node's status entry from `/infra/status`.
#[derive(Debug, Clone)]
pub struct InfraNode(pub Value);

impl InfraNode {
    pub fn node_id(&self) -> Option<&str> {
        self.0.get("node_id").and_then(Value::as_str)
    }
    pub fn status(&self) -> Option<&str> {
        self.0.get("status").and_then(Value::as_str)
    }
    pub fn endpoint_url(&self) -> Option<&str> {
        self.0.get("endpoint_url").and_then(Value::as_str)
    }
}

/// Fetch the current infra status for a project.
pub async fn status(disp: &Dispatcher, project_id: &Uuid) -> Result<Vec<InfraNode>> {
    let path = format!("/projects/{project_id}/infra/status");
    let resp: Value = disp.get_json(&path).await?;
    let nodes = resp
        .get("nodes")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    Ok(nodes.into_iter().map(InfraNode).collect())
}

/// Provision the project's infra (`weft infra start`) and wait until `node_id`
/// reports `running`, returning its resolved endpoint URL. Errors loudly if it
/// reaches a terminal-bad status (`failed`) or never comes up.
pub async fn start_and_wait_running(
    project: &mut Project,
    node_id: &str,
) -> Result<String> {
    project.weft(&["infra", "start"]).await?;
    let disp = project.dispatcher().clone();
    let pid = project.id();
    let nid = node_id.to_string();
    poll_until(
        &format!("infra node '{node_id}' to reach status=running"),
        INFRA_RUNNING_DEADLINE,
        INFRA_POLL,
        || {
            let disp = disp.clone();
            let nid = nid.clone();
            async move {
                let nodes = status(&disp, &pid).await?;
                let node = nodes.iter().find(|n| n.node_id() == Some(nid.as_str()));
                match node.and_then(InfraNode::status) {
                    Some("running") => {
                        let url = node
                            .and_then(InfraNode::endpoint_url)
                            .context("infra node running but no endpoint_url")?;
                        Ok(Some(url.to_string()))
                    }
                    Some("failed") => {
                        bail!("infra node '{nid}' reached status=failed: {:?}", node.map(|n| &n.0))
                    }
                    // provisioning / stopped / flaky / not-yet-present: keep waiting.
                    _ => Ok(None),
                }
            }
        },
    )
    .await
}

/// Call an HTTP route on an infra endpoint URL (e.g. `/outputs`, `/health`, a
/// node's `/action`). Returns the raw bytes. The endpoint URL is the
/// cluster-internal service URL the dispatcher resolved; the rig reaches it
/// through the same gateway/ingress the dispatcher exposes for it. Endpoints
/// not exposed outside the cluster cannot be hit directly; in that case assert
/// via the run's `/outputs`-fed node output ports instead.
pub async fn call_endpoint(disp: &Dispatcher, endpoint_url: &str, path: &str) -> Result<Vec<u8>> {
    let url = format!("{}/{}", endpoint_url.trim_end_matches('/'), path.trim_start_matches('/'));
    let (status, bytes) = disp.get_abs_raw(&url).await?;
    if !status.is_success() {
        bail!(
            "infra endpoint GET {url} -> HTTP {status}: {}",
            String::from_utf8_lossy(&bytes)
        );
    }
    Ok(bytes)
}

/// Terminate the project's infra (`weft infra terminate`) and wait until the
/// node is gone from `/infra/status` (the row is removed on successful
/// terminate). Asserts cleanup actually happened rather than trusting the verb.
pub async fn terminate_and_wait_gone(project: &Project, node_id: &str) -> Result<()> {
    project.weft(&["infra", "terminate"]).await?;
    let disp = project.dispatcher().clone();
    let pid = project.id();
    let nid = node_id.to_string();
    poll_until(
        &format!("infra node '{node_id}' to be gone after terminate"),
        INFRA_RUNNING_DEADLINE,
        INFRA_POLL,
        || {
            let disp = disp.clone();
            let nid = nid.clone();
            async move {
                let nodes = status(&disp, &pid).await?;
                let present = nodes.iter().any(|n| n.node_id() == Some(nid.as_str()));
                if present {
                    Ok(None)
                } else {
                    Ok(Some(()))
                }
            }
        },
    )
    .await
}
