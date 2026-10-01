//! Infra node lifecycle: provision, wait-to-running, read outputs, terminate.
//!
//! Infra nodes are long-running backing services the platform provisions
//! (Docker containers on a local install). The rig drives them through the real CLI (`weft infra start|terminate`,
//! the user's path) and observes status through the dispatcher API
//! (`GET /projects/{id}/infra/status` -> `{ nodes: [{ node, status,
//! endpoint_url, ... }] }`, `node` being the node's PLACEMENT spelled
//! the way a person writes it: `svc`, or `one.svc` inside the file the
//! site `one` includes). The status values track the supervisor's state
//! machine: provisioning -> running, stopped, flaky, failed, (gone after
//! terminate).

use std::time::Duration;

use anyhow::{bail, Context, Result};
use uuid::Uuid;
use weft_core::infra::wire::{InfraStatus, InfraStatusEntry};

use crate::client::{poll_until, Dispatcher};
use crate::project::Project;

/// How long to wait for an infra node to become `running`. Provisioning pulls /
/// builds an image, starts the unit's containers, and waits for its readiness probe,
/// so this is generous. It is an internal transition the rig controls, so a
/// bound is correct.
const INFRA_RUNNING_DEADLINE: Duration = Duration::from_secs(300);
const INFRA_POLL: Duration = Duration::from_millis(750);

/// Fetch every infra copy of a project, and its state.
pub async fn status(disp: &Dispatcher, project_id: &Uuid) -> Result<Vec<InfraStatusEntry>> {
    let path = format!("/projects/{project_id}/infra/status");
    let status: InfraStatus = disp.get_json(&path).await?;
    Ok(status.nodes)
}

/// Provision the project's infra (`weft infra start`) and wait until the
/// placement at `node` (a place spelling, `one.svc`) reports `running`,
/// returning its resolved endpoint URL. Errors loudly if it reaches a
/// terminal-bad status (`failed`) or never comes up.
pub async fn start_and_wait_running(
    project: &mut Project,
    node: &str,
) -> Result<String> {
    project.weft(&["infra", "start"]).await?;
    wait_running(project, node).await
}

/// Wait until the placement at `node` reports `running` (infra already
/// started), returning its endpoint URL.
pub async fn wait_running(project: &Project, node: &str) -> Result<String> {
    let disp = project.dispatcher().clone();
    let pid = project.id();
    let nid = node.to_string();
    poll_until(
        &format!("infra node '{node}' to reach status=running"),
        INFRA_RUNNING_DEADLINE,
        INFRA_POLL,
        || {
            let disp = disp.clone();
            let nid = nid.clone();
            async move {
                let nodes = status(&disp, &pid).await?;
                let node = nodes.iter().find(|n| n.node == nid);
                match node.map(|n| n.status.as_str()) {
                    Some("running") => {
                        let url = node
                            .and_then(|n| n.endpoint_url.clone())
                            .context("infra node running but no endpoint_url")?;
                        Ok(Some(url))
                    }
                    Some("failed") => {
                        bail!("infra node '{nid}' reached status=failed: {node:?}")
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
/// address the install shows for it: on a local install, the unit's port
/// published on this machine's loopback, which the rig reaches directly.
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
/// instance at `node` is gone from `/infra/status` (the row is removed on
/// successful terminate). Asserts cleanup actually happened rather than
/// trusting the verb.
pub async fn terminate_and_wait_gone(project: &Project, node: &str) -> Result<()> {
    project.weft(&["infra", "terminate", "--yes"]).await?;
    wait_gone(project, node).await
}

/// Wait until the placement at `node` has no row any more.
pub async fn wait_gone(project: &Project, node: &str) -> Result<()> {
    let disp = project.dispatcher().clone();
    let pid = project.id();
    let nid = node.to_string();
    poll_until(
        &format!("infra node '{node}' to be gone after terminate"),
        INFRA_RUNNING_DEADLINE,
        INFRA_POLL,
        || {
            let disp = disp.clone();
            let nid = nid.clone();
            async move {
                let nodes = status(&disp, &pid).await?;
                let present = nodes.iter().any(|n| n.node == nid);
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
