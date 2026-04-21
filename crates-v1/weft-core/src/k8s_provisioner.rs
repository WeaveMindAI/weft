//! Kubernetes provisioner,applies and deletes K8s resources from an InfrastructureSpec.
//!
//! This module is the bridge between node-defined manifests and the K8s API.
//! It takes raw JSON manifests from InfrastructureSpec, resolves the GVK
//! (Group/Version/Kind), and applies them via kube-rs dynamic API.
//!
//! Design:
//! - Manifests are applied in order (e.g., PVC before Deployment).
//! - Deletion is reverse order.
//! - All resources are labeled for ownership tracking.
//! - Pod readiness is checked via check_ready() which polls K8s pod conditions.

use kube::{
    Client,
    api::{Api, DynamicObject, Patch, PatchParams, DeleteParams, ListParams},
    discovery::ApiResource,
    ResourceExt,
};
use k8s_openapi::api::core::v1::Pod;
use serde_json::Value;

use crate::node::InfrastructureSpec;

const LABEL_MANAGED_BY: &str = "weavemind.ai/managed-by";
const LABEL_INSTANCE: &str = "weavemind.ai/instance";
const LABEL_USER: &str = "weavemind.ai/user";
const LABEL_PROJECT: &str = "weavemind.ai/project";
const LABEL_NODE: &str = "weavemind.ai/node";

/// Build sidecar image name from sidecarName.
/// Uses SIDECAR_IMAGE_REGISTRY env var if set (cloud), otherwise defaults to ghcr.io/weavemindai (local).
fn build_sidecar_image(sidecar_name: &str) -> String {
    let registry = std::env::var("SIDECAR_IMAGE_REGISTRY")
        .unwrap_or_else(|_| "ghcr.io/weavemindai".to_string());
    format!("{}/sidecar-{}:latest", registry, sidecar_name)
}

#[derive(Debug, Clone)]
pub struct ProvisionContext {
    pub instanceId: String,
    pub namespace: String,
    pub userId: String,
    pub projectId: String,
    pub nodeId: String,
}

/// Apply all manifests from an InfrastructureSpec into the given namespace.
/// Injects ownership labels into every resource.
/// Returns the list of (apiVersion, kind, name) tuples for tracking.
pub async fn apply_manifests(
    client: &Client,
    spec: &InfrastructureSpec,
    pctx: &ProvisionContext,
) -> Result<Vec<(String, String, String)>, String> {
    let mut applied = Vec::new();

    for kube_manifest in &spec.manifests {
        let mut manifest = kube_manifest.manifest.clone();

        // Resolve placeholders throughout the manifest.
        // Nodes declare specs with placeholders; the provisioner fills in identity and sidecar image.
        let json_str = serde_json::to_string(&manifest)
            .map_err(|e| format!("Failed to serialize manifest: {}", e))?;
        let resolved = json_str
            .replace("__INSTANCE_ID__", &pctx.instanceId)
            .replace("__SIDECAR_IMAGE__", &build_sidecar_image(&spec.sidecarName));
        manifest = serde_json::from_str(&resolved)
            .map_err(|e| format!("Failed to parse resolved manifest: {}", e))?;

        inject_labels(&mut manifest, pctx);
        inject_namespace(&mut manifest, &pctx.namespace);

        let api_version = manifest.get("apiVersion")
            .and_then(|v| v.as_str())
            .ok_or("Manifest missing apiVersion")?
            .to_string();
        let kind = manifest.get("kind")
            .and_then(|v| v.as_str())
            .ok_or("Manifest missing kind")?
            .to_string();
        let name = manifest.get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .ok_or("Manifest missing metadata.name")?
            .to_string();

        let manifest_json = serde_json::to_value(&manifest)
            .map_err(|e| format!("Failed to serialize manifest: {}", e))?;

        apply_single_resource(client, &pctx.namespace, &api_version, &kind, &manifest_json).await?;

        tracing::info!(
            "Applied {}/{} '{}' in namespace {}",
            api_version, kind, name, pctx.namespace
        );
        applied.push((api_version, kind, name));
    }

    Ok(applied)
}

/// Delete all resources that were previously applied for this instance.
/// Deletes in reverse order (Deployment before PVC, etc.).
pub async fn delete_manifests(
    client: &Client,
    spec: &InfrastructureSpec,
    pctx: &ProvisionContext,
) -> Result<(), String> {
    for kube_manifest in spec.manifests.iter().rev() {
        let manifest = &kube_manifest.manifest;

        let api_version = manifest.get("apiVersion")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let kind = manifest.get("kind")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let name = manifest.get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .unwrap_or_default();

        if name.is_empty() || kind.is_empty() {
            continue;
        }

        if let Err(e) = delete_single_resource(client, &pctx.namespace, api_version, kind, name).await {
            tracing::warn!(
                "Failed to delete {}/{} '{}' in namespace {}: {}",
                api_version, kind, name, pctx.namespace, e
            );
        } else {
            tracing::info!(
                "Deleted {}/{} '{}' in namespace {}",
                api_version, kind, name, pctx.namespace
            );
        }
    }

    Ok(())
}

/// Scale a Deployment to 0 replicas (stop without destroying data).
pub async fn scale_deployment_to_zero(
    client: &Client,
    namespace: &str,
    deployment_name: &str,
) -> Result<(), String> {
    let patch = serde_json::json!({
        "spec": { "replicas": 0 }
    });

    let api_resource = ApiResource::from_gvk(
        &kube::api::GroupVersionKind::gvk("apps", "v1", "Deployment"),
    );
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &api_resource);

    api.patch(
        deployment_name,
        &PatchParams::apply("weavemind"),
        &Patch::Merge(&patch),
    ).await.map_err(|e| format!("Failed to scale deployment to 0: {}", e))?;

    tracing::info!("Scaled deployment '{}' to 0 replicas in namespace {}", deployment_name, namespace);
    Ok(())
}

/// Delete all K8s resources for an instance by label selector.
/// Deletes Deployments, Services, and PVCs matching the instance label.
/// Waits for all resources to actually be gone before returning.
pub async fn delete_instance_resources(
    client: &Client,
    namespace: &str,
    instance_id: &str,
) -> Result<(), String> {
    let label_selector = format!("{}={}", LABEL_INSTANCE, instance_id);
    let lp = ListParams::default().labels(&label_selector);
    let dp = DeleteParams::default();

    // Delete Deployments
    let deploy_api: Api<DynamicObject> = Api::namespaced_with(
        client.clone(), namespace,
        &ApiResource::from_gvk(&kube::api::GroupVersionKind::gvk("apps", "v1", "Deployment")),
    );
    if let Ok(list) = deploy_api.list(&lp).await {
        for item in list.items {
            let name = item.name_any();
            if let Err(e) = deploy_api.delete(&name, &dp).await {
                tracing::warn!("Failed to delete deployment {}: {}", name, e);
            } else {
                tracing::info!("Deleted deployment {} in {}", name, namespace);
            }
        }
    }

    // Delete Services
    let svc_api: Api<DynamicObject> = Api::namespaced_with(
        client.clone(), namespace,
        &ApiResource::from_gvk(&kube::api::GroupVersionKind::gvk("", "v1", "Service")),
    );
    if let Ok(list) = svc_api.list(&lp).await {
        for item in list.items {
            let name = item.name_any();
            if let Err(e) = svc_api.delete(&name, &dp).await {
                tracing::warn!("Failed to delete service {}: {}", name, e);
            } else {
                tracing::info!("Deleted service {} in {}", name, namespace);
            }
        }
    }

    // Delete PVCs
    let pvc_api: Api<DynamicObject> = Api::namespaced_with(
        client.clone(), namespace,
        &ApiResource::from_gvk(&kube::api::GroupVersionKind::gvk("", "v1", "PersistentVolumeClaim")),
    );
    if let Ok(list) = pvc_api.list(&lp).await {
        for item in list.items {
            let name = item.name_any();
            if let Err(e) = pvc_api.delete(&name, &dp).await {
                tracing::warn!("Failed to delete PVC {}: {}", name, e);
            } else {
                tracing::info!("Deleted PVC {} in {}", name, namespace);
            }
        }
    }

    // Wait for all resources with this label to actually be gone (up to 60s).
    // K8s delete is async, resources enter Terminating state before disappearing.
    // If we return early, start_all will apply manifests on top of dying resources.
    for i in 0..30 {
        let remaining = count_resources_with_label(client, namespace, &label_selector).await;
        if remaining == 0 {
            tracing::info!("All resources for instance {} fully deleted", instance_id);
            return Ok(());
        }
        if i == 0 {
            tracing::info!(
                "Waiting for {} resource(s) to finish terminating for instance {}",
                remaining, instance_id
            );
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }

    tracing::warn!(
        "Timed out waiting for resources to terminate for instance {}",
        instance_id
    );
    Ok(())
}

async fn count_resources_with_label(
    client: &Client,
    namespace: &str,
    label_selector: &str,
) -> usize {
    let lp = ListParams::default().labels(label_selector);
    let mut count = 0;

    let deploy_api: Api<DynamicObject> = Api::namespaced_with(
        client.clone(), namespace,
        &ApiResource::from_gvk(&kube::api::GroupVersionKind::gvk("apps", "v1", "Deployment")),
    );
    if let Ok(list) = deploy_api.list(&lp).await {
        count += list.items.len();
    }

    let svc_api: Api<DynamicObject> = Api::namespaced_with(
        client.clone(), namespace,
        &ApiResource::from_gvk(&kube::api::GroupVersionKind::gvk("", "v1", "Service")),
    );
    if let Ok(list) = svc_api.list(&lp).await {
        count += list.items.len();
    }

    let pvc_api: Api<DynamicObject> = Api::namespaced_with(
        client.clone(), namespace,
        &ApiResource::from_gvk(&kube::api::GroupVersionKind::gvk("", "v1", "PersistentVolumeClaim")),
    );
    if let Ok(list) = pvc_api.list(&lp).await {
        count += list.items.len();
    }

    count
}

/// Scale all Deployments for an instance to 0 replicas by label selector.
pub async fn scale_instance_deployments_to_zero(
    client: &Client,
    namespace: &str,
    instance_id: &str,
) -> Result<(), String> {
    let label_selector = format!("{}={}", LABEL_INSTANCE, instance_id);
    let lp = ListParams::default().labels(&label_selector);

    let deploy_api: Api<DynamicObject> = Api::namespaced_with(
        client.clone(), namespace,
        &ApiResource::from_gvk(&kube::api::GroupVersionKind::gvk("apps", "v1", "Deployment")),
    );

    if let Ok(list) = deploy_api.list(&lp).await {
        for item in list.items {
            let name = item.name_any();
            scale_deployment_to_zero(client, namespace, &name).await?;
        }
    }

    Ok(())
}

/// Check once whether the infrastructure pod is ready.
/// Returns Ok(true) if ready, Ok(false) if not yet, Err on API failure.
pub async fn check_ready(
    client: &Client,
    namespace: &str,
    instance_id: &str,
) -> Result<bool, String> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let label_selector = format!("{}={}", LABEL_INSTANCE, instance_id);

    let list = pods.list(&ListParams::default().labels(&label_selector)).await
        .map_err(|e| format!("Failed to list pods: {}", e))?;

    for pod in &list.items {
        // Skip terminating pods: they may still show Ready=True during graceful
        // shutdown but will die soon. Without this filter, a stop→restart cycle
        // in GKE (where graceful shutdown is ~30s) finds the old dying pod and
        // returns "ready" instantly, before the new pod has started.
        if pod.metadata.deletion_timestamp.is_some() {
            tracing::debug!(
                "Skipping terminating pod {} in namespace {}",
                pod.name_any(), namespace
            );
            continue;
        }

        if let Some(status) = &pod.status {
            if let Some(conditions) = &status.conditions {
                let ready = conditions.iter().any(|c| c.type_ == "Ready" && c.status == "True");
                if ready {
                    tracing::info!(
                        "Pod {} is ready in namespace {}",
                        pod.name_any(), namespace
                    );
                    return Ok(true);
                }
            }
        }
    }

    Ok(false)
}

/// Ensure a K8s namespace exists for a user. Creates it with ResourceQuota,
/// LimitRange, and NetworkPolicy if it doesn't exist yet.
///
/// Namespace format: `wm-{userId}`
/// The userId is stored as a label so we can validate ownership later.
pub async fn ensure_namespace(client: &Client, namespace: &str) -> Result<(), String> {
    let ns_api: Api<k8s_openapi::api::core::v1::Namespace> = Api::all(client.clone());

    match ns_api.get(namespace).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(err)) if err.code == 404 => {
            // Extract userId from namespace name (wm-{userId})
            let user_id = namespace.strip_prefix("wm-").unwrap_or(namespace);

            let ns = serde_json::json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": namespace,
                    "labels": {
                        LABEL_MANAGED_BY: "weavemind",
                        LABEL_USER: user_id
                    }
                }
            });
            ns_api.create(&Default::default(), &serde_json::from_value(ns)
                .map_err(|e| format!("Failed to build namespace object: {}", e))?)
                .await
                .map_err(|e| format!("Failed to create namespace {}: {}", namespace, e))?;
            tracing::info!("Created namespace {} for user {}", namespace, user_id);

            // Apply isolation resources (best-effort, don't fail namespace creation)
            if let Err(e) = apply_namespace_isolation(client, namespace).await {
                tracing::warn!("Failed to apply isolation to namespace {}: {}", namespace, e);
            }

            Ok(())
        }
        Err(e) => Err(format!("Failed to check namespace {}: {}", namespace, e)),
    }
}

/// Apply ResourceQuota, LimitRange, and NetworkPolicy to a namespace.
async fn apply_namespace_isolation(client: &Client, namespace: &str) -> Result<(), String> {
    // ResourceQuota, cap resource usage per namespace
    let quota = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": {
            "name": "weavemind-quota",
            "namespace": namespace
        },
        "spec": {
            "hard": {
                "pods": "20",
                "requests.cpu": "4",
                "requests.memory": "8Gi",
                "limits.cpu": "8",
                "limits.memory": "16Gi",
                "persistentvolumeclaims": "10",
                "requests.storage": "50Gi"
            }
        }
    });
    apply_single_resource(client, namespace, "v1", "ResourceQuota", &quota).await?;
    tracing::info!("Applied ResourceQuota to namespace {}", namespace);

    // LimitRange, default resource limits for containers
    let limit_range = serde_json::json!({
        "apiVersion": "v1",
        "kind": "LimitRange",
        "metadata": {
            "name": "weavemind-limits",
            "namespace": namespace
        },
        "spec": {
            "limits": [{
                "type": "Container",
                "default": {
                    "cpu": "500m",
                    "memory": "512Mi"
                },
                "defaultRequest": {
                    "cpu": "100m",
                    "memory": "128Mi"
                },
                "max": {
                    "cpu": "2",
                    "memory": "4Gi"
                }
            }]
        }
    });
    apply_single_resource(client, namespace, "v1", "LimitRange", &limit_range).await?;
    tracing::info!("Applied LimitRange to namespace {}", namespace);

    // NetworkPolicy, deny all ingress by default, allow only from weavemind system
    let network_policy = serde_json::json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {
            "name": "weavemind-isolation",
            "namespace": namespace
        },
        "spec": {
            "podSelector": {},
            "policyTypes": ["Ingress"],
            "ingress": [
                {
                    // Allow traffic from pods in the same namespace
                    "from": [{
                        "podSelector": {}
                    }]
                },
                {
                    // Allow traffic from the weavemind system namespace
                    "from": [{
                        "namespaceSelector": {
                            "matchLabels": {
                                LABEL_MANAGED_BY: "weavemind-system"
                            }
                        }
                    }]
                }
            ]
        }
    });
    apply_single_resource(client, namespace, "networking.k8s.io/v1", "NetworkPolicy", &network_policy).await?;
    tracing::info!("Applied NetworkPolicy to namespace {}", namespace);

    Ok(())
}

// =============================================================================
// INTERNAL HELPERS
// =============================================================================

fn inject_labels(manifest: &mut Value, pctx: &ProvisionContext) {
    if let Some(metadata) = manifest.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        let labels = metadata.entry("labels").or_insert_with(|| serde_json::json!({}));
        if let Some(labels_obj) = labels.as_object_mut() {
            labels_obj.insert(LABEL_MANAGED_BY.to_string(), serde_json::json!("weavemind"));
            labels_obj.insert(LABEL_INSTANCE.to_string(), serde_json::json!(pctx.instanceId));
            labels_obj.insert(LABEL_USER.to_string(), serde_json::json!(pctx.userId));
            labels_obj.insert(LABEL_PROJECT.to_string(), serde_json::json!(pctx.projectId));
            labels_obj.insert(LABEL_NODE.to_string(), serde_json::json!(pctx.nodeId));
        }
    }

    // Also inject labels into pod template spec (for Deployments/StatefulSets)
    if let Some(spec) = manifest.get_mut("spec") {
        if let Some(template) = spec.get_mut("template") {
            if let Some(tmeta) = template.get_mut("metadata").and_then(|m| m.as_object_mut()) {
                let labels = tmeta.entry("labels").or_insert_with(|| serde_json::json!({}));
                if let Some(labels_obj) = labels.as_object_mut() {
                    labels_obj.insert(LABEL_MANAGED_BY.to_string(), serde_json::json!("weavemind"));
                    labels_obj.insert(LABEL_INSTANCE.to_string(), serde_json::json!(pctx.instanceId));
                    labels_obj.insert(LABEL_USER.to_string(), serde_json::json!(pctx.userId));
                    labels_obj.insert(LABEL_PROJECT.to_string(), serde_json::json!(pctx.projectId));
                    labels_obj.insert(LABEL_NODE.to_string(), serde_json::json!(pctx.nodeId));
                }
            }
        }

        // Inject into selector.matchLabels for Deployments
        if let Some(selector) = spec.get_mut("selector") {
            if let Some(match_labels) = selector.get_mut("matchLabels").and_then(|m| m.as_object_mut()) {
                match_labels.insert(LABEL_INSTANCE.to_string(), serde_json::json!(pctx.instanceId));
            }
        }

        // Inject into spec.selector for Services (flat key-value, not matchLabels)
        if let Some(selector) = spec.get_mut("selector").and_then(|s| s.as_object_mut()) {
            // Services have a flat selector (no matchLabels wrapper)
            // Only inject if this looks like a Service (no matchLabels key)
            if !selector.contains_key("matchLabels") {
                selector.insert(LABEL_INSTANCE.to_string(), serde_json::json!(pctx.instanceId));
            }
        }
    }
}

fn inject_namespace(manifest: &mut Value, namespace: &str) {
    if let Some(metadata) = manifest.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        metadata.insert("namespace".to_string(), serde_json::json!(namespace));
    }
}

async fn apply_single_resource(
    client: &Client,
    namespace: &str,
    api_version: &str,
    kind: &str,
    manifest: &Value,
) -> Result<(), String> {
    let gvk = parse_gvk(api_version, kind);
    let api_resource = ApiResource::from_gvk(&gvk);
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &api_resource);

    api.patch(
        manifest.get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .ok_or("Manifest missing metadata.name")?,
        &PatchParams::apply("weavemind").force(),
        &Patch::Apply(manifest),
    ).await.map_err(|e| format!("Failed to apply {}/{}: {}", api_version, kind, e))?;

    Ok(())
}

async fn delete_single_resource(
    client: &Client,
    namespace: &str,
    api_version: &str,
    kind: &str,
    name: &str,
) -> Result<(), String> {
    let gvk = parse_gvk(api_version, kind);
    let api_resource = ApiResource::from_gvk(&gvk);
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &api_resource);

    api.delete(name, &DeleteParams::default()).await
        .map_err(|e| format!("Failed to delete {}/{} '{}': {}", api_version, kind, name, e))?;

    Ok(())
}

/// Count running infra deployments (replicas > 0) across all namespaces.
pub async fn count_running_infra_deployments(client: &Client) -> Result<usize, String> {
    let label_selector = format!("{}=weavemind", LABEL_MANAGED_BY);
    let lp = ListParams::default().labels(&label_selector);
    let deploy_api: Api<DynamicObject> = Api::all_with(
        client.clone(),
        &ApiResource::from_gvk(&kube::api::GroupVersionKind::gvk("apps", "v1", "Deployment")),
    );

    let list = deploy_api.list(&lp).await
        .map_err(|e| format!("Failed to list deployments: {}", e))?;

    let count = list.items.iter().filter(|d| {
        d.data.get("spec")
            .and_then(|s| s.get("replicas"))
            .and_then(|r| r.as_i64())
            .unwrap_or(0) > 0
    }).count();

    Ok(count)
}

/// Count running infra deployments (replicas > 0) for a specific user.
pub async fn count_running_infra_deployments_for_user(
    client: &Client,
    user_id: &str,
) -> Result<usize, String> {
    let label_selector = format!("{}=weavemind,{}={}", LABEL_MANAGED_BY, LABEL_USER, user_id);
    let lp = ListParams::default().labels(&label_selector);
    let deploy_api: Api<DynamicObject> = Api::all_with(
        client.clone(),
        &ApiResource::from_gvk(&kube::api::GroupVersionKind::gvk("apps", "v1", "Deployment")),
    );

    let list = deploy_api.list(&lp).await
        .map_err(|e| format!("Failed to list deployments for user {}: {}", user_id, e))?;

    let count = list.items.iter().filter(|d| {
        d.data.get("spec")
            .and_then(|s| s.get("replicas"))
            .and_then(|r| r.as_i64())
            .unwrap_or(0) > 0
    }).count();

    Ok(count)
}

fn parse_gvk(api_version: &str, kind: &str) -> kube::api::GroupVersionKind {
    let (group, version) = if let Some(slash_pos) = api_version.find('/') {
        (&api_version[..slash_pos], &api_version[slash_pos + 1..])
    } else {
        ("", api_version.as_ref())
    };
    kube::api::GroupVersionKind::gvk(group, version, kind)
}
