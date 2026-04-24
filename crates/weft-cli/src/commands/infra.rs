//! `weft infra start | stop | terminate | status`. Three lifecycle
//! verbs matching v1 semantics:
//!
//!   - **start**: provision the sidecars if missing, or scale them
//!     back to 1 if they were stopped. Refuses if already running.
//!   - **stop**: scale the Deployments to 0. Keeps PVCs / Services
//!     so a later `start` resumes the same instance. Refuses if
//!     already stopped.
//!   - **terminate**: delete every k8s resource the sidecars own,
//!     PVCs included. Irreversible. Idempotent (safe to re-run).
//!   - **status**: print the current lifecycle state of every
//!     infra node for the cwd project.
//!
//! `start` auto-builds any sidecar images it needs first. Users
//! don't have to remember `weft daemon start` as a prerequisite;
//! `ensure_registered` upstream handles project registration.

use std::collections::BTreeSet;

use anyhow::Result;
use tokio::process::Command;

use super::Ctx;
use crate::images;
use weft_catalog::stdlib_catalog;

const CLUSTER_NAME: &str = "weft-local";

pub enum InfraAction {
    Start,
    Stop,
    Terminate,
    Status,
}

pub async fn run(ctx: Ctx, action: InfraAction) -> Result<()> {
    match action {
        InfraAction::Start => {
            // `start` is the only verb that's allowed to trigger a
            // compile: it brings infra up from scratch, so the
            // worker image must exist for any subsequent activate
            // to spawn workers. ensure_registered builds the
            // project + uploads to the dispatcher.
            let handle = super::ensure::ensure_registered(&ctx).await?;
            ensure_sidecar_images(&handle.project).await?;
            let resp = handle
                .client
                .post_json_empty(&format!("/projects/{}/infra/start", handle.id))
                .await?;
            print_status(&handle.name, &handle.id, &resp);
        }
        InfraAction::Stop | InfraAction::Terminate | InfraAction::Status => {
            // These three just talk to the dispatcher about
            // already-existing cluster state. Compiling from source
            // would be wasted work (and confusing for `status`,
            // which is read-only). Look up the project id from the
            // cwd's weft.toml without invoking the compiler.
            let (client, id, name) = resolve_project(&ctx)?;
            match action {
                InfraAction::Stop => {
                    let resp = client
                        .post_json_empty(&format!("/projects/{id}/infra/stop"))
                        .await?;
                    print_status(&name, &id, &resp);
                }
                InfraAction::Terminate => {
                    client
                        .post_empty(&format!("/projects/{id}/infra/terminate"))
                        .await?;
                    println!("infra terminated for {name}");
                }
                InfraAction::Status => {
                    let resp: serde_json::Value = client
                        .get_json(&format!("/projects/{id}/infra/status"))
                        .await?;
                    print_status(&name, &id, &resp);
                }
                InfraAction::Start => unreachable!(),
            }
        }
    }
    Ok(())
}

/// Read the cwd project's id + name directly from `weft.toml`,
/// without compiling. Used by observational / destructive verbs
/// that have no reason to rebuild.
fn resolve_project(ctx: &Ctx) -> Result<(crate::client::DispatcherClient, String, String)> {
    let cwd = std::env::current_dir()?;
    let project = weft_compiler::project::Project::discover(&cwd)
        .map_err(|e| anyhow::anyhow!("discover project: {e}"))?;
    let dispatcher = ctx
        .dispatcher
        .clone()
        .unwrap_or_else(|| project.dispatcher_url());
    let client = crate::client::DispatcherClient::new(dispatcher);
    Ok((
        client,
        project.id().to_string(),
        project.manifest.package.name.clone(),
    ))
}

fn print_status(name: &str, id: &str, resp: &serde_json::Value) {
    let nodes = resp
        .get("nodes")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    if nodes.is_empty() {
        println!("no infra nodes in this project");
        return;
    }
    println!("infra for {name} ({id}):");
    for n in nodes {
        let node = n.get("node_id").and_then(|v| v.as_str()).unwrap_or("?");
        let status = n.get("status").and_then(|v| v.as_str()).unwrap_or("?");
        let url = n
            .get("endpoint_url")
            .and_then(|v| v.as_str())
            .unwrap_or("(no endpoint)");
        println!("  {node} [{status}] → {url}");
    }
}

/// Build + kind-load every sidecar image the project's infra nodes
/// reference. Uses docker's layer cache so rebuilds are fast when
/// source is unchanged; kind_load is idempotent against matching
/// image IDs.
async fn ensure_sidecar_images(project: &weft_compiler::project::Project) -> Result<()> {
    let source = project
        .read_main_weft()
        .map_err(|e| anyhow::anyhow!("read main.weft: {e}"))?;
    let node_types = collect_node_types(&source);

    let catalog = stdlib_catalog().map_err(|e| anyhow::anyhow!("load catalog: {e}"))?;
    let mut seen_sidecars: BTreeSet<String> = BTreeSet::new();

    for nt in &node_types {
        let Some(entry) = catalog.entry(nt) else { continue };
        if !entry.metadata.requires_infra {
            continue;
        }
        let Some(sidecar_spec) = entry.metadata.features.sidecar.as_ref() else {
            continue;
        };
        let sidecar_name = sidecar_spec.name.clone();
        if !seen_sidecars.insert(sidecar_name.clone()) {
            continue;
        }

        let tag = format!("ghcr.io/weavemindai/sidecar-{sidecar_name}:latest");
        let sidecar_dir = entry.source_dir.join("sidecar");
        let dockerfile = sidecar_dir.join("Dockerfile");
        if !dockerfile.is_file() {
            anyhow::bail!(
                "node type '{nt}' declares sidecar '{sidecar_name}' but no Dockerfile found at {}",
                dockerfile.display()
            );
        }

        println!("  {sidecar_name}: building image {tag}");
        let status = Command::new("docker")
            .args(["build", "-t", &tag, "-f"])
            .arg(&dockerfile)
            .arg(&sidecar_dir)
            .status()
            .await?;
        if !status.success() {
            anyhow::bail!("docker build for sidecar '{sidecar_name}' failed");
        }
        images::kind_load(CLUSTER_NAME, &tag).await?;
        println!("  {sidecar_name}: loaded into kind");
    }
    Ok(())
}

/// Quick heuristic: scan `main.weft` for `= <NodeType>` patterns.
/// Good enough to find trigger + infra nodes without re-running
/// the enrich pass. Matches the grammar: identifier after `=`
/// starting with an uppercase letter.
fn collect_node_types(source: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for line in source.lines() {
        let line = line.split('#').next().unwrap_or("");
        let Some(eq_idx) = line.find('=') else { continue };
        let after = line[eq_idx + 1..].trim();
        let ident: String = after
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        if ident.chars().next().is_some_and(|c| c.is_ascii_uppercase()) {
            out.insert(ident);
        }
    }
    out
}
