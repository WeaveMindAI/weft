//! Helper functions for infrastructure node authors.
//!
//! These handle the boilerplate of K8s provisioning and sidecar communication
//! so that node implementations only need to provide their InfrastructureSpec.
//!
//! Usage:
//!   async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
//!       if ctx.isInfraSetup {
//!           infra_provision(&ctx, &Self::infrastructure_spec()).await
//!       } else {
//!           infra_query_outputs(&ctx).await
//!       }
//!   }
//!
//! TODO: When we need more control (custom timeouts, pre/post hooks, extra
//! outputs to merge, etc.), add an InfraProvisionConfig struct as a third
//! parameter to infra_provision (and similar for infra_query_outputs).
//! The current two-arg signatures stay as convenience wrappers that pass
//! a default config. Node authors who need customization pass their own.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use weft_core::node::InfrastructureSpec;
use weft_core::k8s_provisioner::{self, ProvisionContext};
use weft_core::infrastructure::infra_instance_id;
use crate::node::ExecutionContext;
use crate::NodeResult;

/// Default sidecar container port (all current sidecars use this).
const SIDECAR_CONTAINER_PORT: u16 = 8090;

/// Kill any existing kubectl port-forward bound to this local port.
/// Uses `lsof` to find the PID, then `kill -9` to terminate it.
fn kill_port_forward(port: u16) {
    let output = std::process::Command::new("lsof")
        .args(["-ti", &format!("tcp:{}", port)])
        .output();
    if let Ok(out) = output {
        let pids = String::from_utf8_lossy(&out.stdout);
        for pid in pids.split_whitespace() {
            if pid.parse::<u32>().is_ok() {
                tracing::info!("Killing stale port-forward on port {} (pid {})", port, pid);
                let _ = std::process::Command::new("kill")
                    .args(["-9", pid])
                    .output();
            }
        }
    }
}

/// Derive a deterministic unique local port from an instance_id.
/// Range: 10000-59999 (50000 slots, collision-resistant for typical projects).
fn local_port_for_instance(instance_id: &str) -> u16 {
    let mut hasher = DefaultHasher::new();
    instance_id.hash(&mut hasher);
    let h = hasher.finish();
    10000 + (h % 50000) as u16
}

/// Provision K8s resources for an infrastructure node during setup.
///
/// Handles: resolve projectId/namespace, create K8s client, ensure namespace,
/// apply manifests, poll pod readiness, build endpointUrl (with local dev rewrite),
/// poll sidecar /health.
///
/// **Local Development (INFRASTRUCTURE_TARGET=local):**
/// Each infra sidecar gets a unique local port derived from its instance_id
/// (deterministic hash in range 10000-59999). A `kubectl port-forward` is
/// auto-started in the background, mapping `localhost:{local_port}` to the
/// sidecar's cluster-internal port. This avoids port collisions when multiple
/// infrastructure nodes are active.
///
/// Returns NodeResult with `{ instanceId, endpointUrl }`.
pub async fn infra_provision(
    ctx: &ExecutionContext,
    spec: &InfrastructureSpec,
) -> NodeResult {
    let project_id = match ctx.input.get("projectId").and_then(|v| v.as_str()) {
        Some(id) => id.to_string(),
        None => match ctx.projectId.as_deref() {
            Some(id) => id.to_string(),
            None => return NodeResult::failed("No projectId available for infrastructure provisioning"),
        },
    };

    let namespace = ctx.input.get("namespace")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .or_else(|| ctx.userId.as_ref().map(|uid| format!("wm-{}", uid.to_lowercase())))
        .unwrap_or_else(|| "wm-local".to_string());

    let instance_id = infra_instance_id(&project_id, &ctx.nodeId);

    let pctx = ProvisionContext {
        instanceId: instance_id.clone(),
        namespace: namespace.clone(),
        userId: ctx.userId.clone().unwrap_or_else(|| "local".to_string()),
        projectId: project_id.clone(),
        nodeId: ctx.nodeId.clone(),
    };

    let k8s_client = match kube::Client::try_default().await {
        Ok(c) => c,
        Err(e) => return NodeResult::failed(&format!("Failed to create K8s client: {}", e)),
    };

    if let Err(e) = k8s_provisioner::ensure_namespace(&k8s_client, &namespace).await {
        return NodeResult::failed(&format!("Failed to ensure namespace: {}", e));
    }

    if let Err(e) = k8s_provisioner::apply_manifests(&k8s_client, spec, &pctx).await {
        return NodeResult::failed(&format!("Failed to apply K8s manifests: {}", e));
    }

    // Poll pod readiness (up to 120s)
    for _ in 0..60 {
        match k8s_provisioner::check_ready(&k8s_client, &namespace, &instance_id).await {
            Ok(true) => break,
            Ok(false) => tokio::time::sleep(std::time::Duration::from_secs(2)).await,
            Err(e) => {
                tracing::warn!("Readiness check error for {}: {}", instance_id, e);
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }
    }

    // Build the action endpoint URL
    let sidecar_port = spec.actionEndpoint.port;
    let is_local = std::env::var("INFRASTRUCTURE_TARGET").as_deref() == Ok("local");
    let endpoint_url = if is_local {
        let local_port = local_port_for_instance(&instance_id);
        // Kill any stale port-forward on this port (from a previous start cycle)
        kill_port_forward(local_port);
        // Auto-start kubectl port-forward in the background
        let svc_name = instance_id.clone();
        let ns = namespace.clone();
        tracing::info!(
            "Starting port-forward: localhost:{} -> svc/{} port {} in ns {}",
            local_port, svc_name, sidecar_port, ns
        );
        let _handle = std::process::Command::new("kubectl")
            .args([
                "port-forward",
                &format!("svc/{}", svc_name),
                &format!("{}:{}", local_port, sidecar_port),
                "-n", &ns,
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn();
        if let Err(e) = &_handle {
            tracing::warn!("Failed to start port-forward for {}: {}", svc_name, e);
        }
        // Give the port-forward a moment to bind
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
        format!("http://localhost:{}{}", local_port, spec.actionEndpoint.path)
    } else {
        format!(
            "http://{}.{}.svc.cluster.local:{}{}",
            instance_id, namespace, sidecar_port, spec.actionEndpoint.path
        )
    };

    // Poll sidecar /health until reachable.
    // No timeout: the user can terminate infra manually if stuck.
    // We check ctx.is_cancelled() periodically so we exit cleanly if the
    // executor was cancelled (e.g. user clicked Terminate during startup).
    let health_url = endpoint_url.replace(&spec.actionEndpoint.path, "/health");
    let http_client = reqwest::Client::new();
    let mut health_attempt: u32 = 0;
    loop {
        health_attempt += 1;

        // Check for cancellation every 5 attempts (avoid spamming the executor)
        if health_attempt % 5 == 0 && ctx.is_cancelled().await {
            tracing::info!("Execution cancelled during /health polling for {}", instance_id);
            return NodeResult::failed("Execution cancelled");
        }

        match http_client.get(&health_url)
            .timeout(std::time::Duration::from_secs(2))
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => break,
            _ => tokio::time::sleep(std::time::Duration::from_secs(2)).await,
        }
    }

    // Deep readiness: call the "ping" action to verify the sidecar can process
    // requests. This blocks until the sidecar confirms readiness (like HumanQuery
    // blocks until the user responds). The user can terminate infra manually if stuck.
    // /health can pass while the sidecar is still initializing its connections
    // or while K8s service endpoints are still propagating.
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;

        // Check for cancellation every 3 attempts
        if attempt % 3 == 0 && ctx.is_cancelled().await {
            tracing::info!("Execution cancelled during ping polling for {}", instance_id);
            return NodeResult::failed("Execution cancelled");
        }

        match http_client.post(&endpoint_url)
            .json(&serde_json::json!({"action": "ping", "payload": {}}))
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => {
                if let Ok(body) = r.json::<serde_json::Value>().await {
                    let ready = body.get("result")
                        .and_then(|r| r.get("ready"))
                        .and_then(|r| r.as_bool())
                        .unwrap_or(false);
                    if ready {
                        tracing::info!(
                            "Sidecar {} ping ready after {} attempts", instance_id, attempt
                        );
                        break;
                    }
                    let reason = body.get("result")
                        .and_then(|r| r.get("reason"))
                        .and_then(|r| r.as_str())
                        .unwrap_or("unknown");
                    tracing::debug!(
                        "Sidecar ping attempt {} for {}: ready=false ({})",
                        attempt, instance_id, reason
                    );
                }
            }
            other => {
                tracing::debug!(
                    "Sidecar ping attempt {} for {}: {:?}",
                    attempt, instance_id, other.err()
                );
            }
        }
        // Exponential backoff: 2s, 4s, 8s, 16s, capped at 30s
        let delay = std::cmp::min(2u64.saturating_pow(attempt), 30);
        tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
    }

    NodeResult::completed(serde_json::json!({
        "instanceId": instance_id,
        "endpointUrl": endpoint_url,
    }))
}

/// Query the sidecar's /outputs endpoint during normal project execution.
///
/// Reads `_endpointUrl` from the node's input (injected by the executor),
/// calls GET /outputs on the sidecar, and merges the response with
/// platform-managed values (instanceId, endpointUrl).
///
/// For local dev, ensures the port-forward is running (it may have died
/// if the orchestrator restarted since provisioning).
///
/// Returns NodeResult with `{ instanceId, endpointUrl, ...sidecarOutputs }`.
pub async fn infra_query_outputs(ctx: &ExecutionContext) -> NodeResult {
    let endpoint_url = match ctx.input.get("_endpointUrl").and_then(|v| v.as_str()) {
        Some(url) => url.to_string(),
        None => return NodeResult::failed(
            "No _endpointUrl in input. Infrastructure may not have been started."
        ),
    };

    let project_id = ctx.projectId.as_deref().unwrap_or("");
    let instance_id = infra_instance_id(project_id, &ctx.nodeId);

    // For local dev, ensure port-forward is alive (idempotent: if port is already
    // bound, kubectl will fail harmlessly and we proceed with the existing forward).
    if std::env::var("INFRASTRUCTURE_TARGET").as_deref() == Ok("local") {
        let local_port = local_port_for_instance(&instance_id);
        let namespace = ctx.input.get("namespace")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| ctx.userId.as_ref().map(|uid| format!("wm-{}", uid.to_lowercase())))
            .unwrap_or_else(|| "wm-local".to_string());

        // Check if port is already bound (fast TCP connect check)
        let addr = format!("127.0.0.1:{}", local_port);
        let port_alive = tokio::net::TcpStream::connect(&addr).await.is_ok();
        if !port_alive {
            tracing::info!(
                "Port-forward on {} not alive, restarting for svc/{}",
                local_port, instance_id
            );
            let _ = std::process::Command::new("kubectl")
                .args([
                    "port-forward",
                    &format!("svc/{}", instance_id),
                    &format!("{}:{}", local_port, SIDECAR_CONTAINER_PORT),
                    "-n", &namespace,
                ])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn();
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }

    let outputs_url = endpoint_url.replace("/action", "/outputs");
    let http_client = reqwest::Client::new();

    let resp = http_client.get(&outputs_url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await;

    let sidecar_outputs = match resp {
        Ok(r) if r.status().is_success() => {
            match r.json::<serde_json::Value>().await {
                Ok(v) => v,
                Err(e) => return NodeResult::failed(&format!(
                    "Failed to parse /outputs response from {}: {}", outputs_url, e
                )),
            }
        }
        Ok(r) => return NodeResult::failed(&format!(
            "Sidecar /outputs returned {} at {}", r.status(), outputs_url
        )),
        Err(e) => return NodeResult::failed(&format!(
            "Failed to call /outputs at {}: {}", outputs_url, e
        )),
    };

    // Merge sidecar outputs with platform values
    let mut output = serde_json::json!({
        "instanceId": instance_id,
        "endpointUrl": endpoint_url,
    });
    if let (Some(base), Some(extra)) = (output.as_object_mut(), sidecar_outputs.as_object()) {
        for (k, v) in extra {
            base.insert(k.clone(), v.clone());
        }
    }

    NodeResult::completed(output)
}
