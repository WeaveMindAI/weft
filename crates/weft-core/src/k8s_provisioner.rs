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

/// Who a running deployment belongs to. Both labels arrive together or not at
/// all, so they live together: a deployment cannot know its project and not
/// its user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InfraOwner {
    pub user_id: String,
    pub project_id: String,
}

/// One infra deployment that is running right now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunningInfra {
    /// Absent when the deployment carries no owner labels. It still costs
    /// money, so it still fills a slot, but nothing can be attributed to a
    /// user or reaped on their behalf, and only deleting it by hand frees the
    /// slot back up.
    pub owner: Option<InfraOwner>,
    pub namespace: String,
    pub name: String,
    /// When the deployment object was created. Lets a caller tell recently
    /// started infrastructure apart from infrastructure that has been up long
    /// enough to have been accounted for.
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl RunningInfra {
    /// How this deployment reads in a log line.
    pub fn describe(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }
}

/// Read one managed deployment, owner labels included when it carries them.
///
/// An unlabelled deployment is a real problem, but not one worth refusing
/// every other user over: it is counted against the spend guard, reported by
/// name, and left out of anyone's personal ceiling.
fn running_from_item(item: &DynamicObject) -> RunningInfra {
    let label = |key: &str| item.metadata.labels.as_ref().and_then(|l| l.get(key)).cloned();
    let owner = match (label(LABEL_USER), label(LABEL_PROJECT)) {
        (Some(user_id), Some(project_id)) => Some(InfraOwner { user_id, project_id }),
        _ => None,
    };
    RunningInfra {
        owner,
        namespace: item.metadata.namespace.clone().unwrap_or_default(),
        name: item.name_any(),
        started_at: item
            .metadata
            .creation_timestamp
            .as_ref()
            .and_then(|t| chrono::DateTime::from_timestamp(t.0.as_second(), 0)),
    }
}

/// Whether a deployment currently has pods asked for.
///
/// An absent `replicas` means one replica, which is what Kubernetes itself
/// does with the field. Reading it as zero would hide a running deployment.
fn is_running(item: &DynamicObject) -> bool {
    item.data
        .get("spec")
        .and_then(|s| s.get("replicas"))
        .and_then(|r| r.as_i64())
        .unwrap_or(1)
        > 0
}

/// Every infra deployment currently running, across all namespaces.
///
/// The cluster is the only place that knows this. The transitional flags kept
/// alongside a start or stop are cleared the moment the transition lands, so a
/// settled, running deployment leaves no record anywhere else.
pub async fn list_running_infra(client: &Client) -> Result<Vec<RunningInfra>, String> {
    let label_selector = format!("{}=weavemind", LABEL_MANAGED_BY);
    let lp = ListParams::default().labels(&label_selector);
    let deploy_api: Api<DynamicObject> = Api::all_with(
        client.clone(),
        &ApiResource::from_gvk(&kube::api::GroupVersionKind::gvk("apps", "v1", "Deployment")),
    );

    let list = deploy_api.list(&lp).await
        .map_err(|e| format!("Failed to list deployments: {}", e))?;

    let running: Vec<RunningInfra> =
        list.items.iter().filter(|d| is_running(d)).map(running_from_item).collect();

    for orphan in running.iter().filter(|r| r.owner.is_none()) {
        tracing::error!(
            "Deployment {} is running and managed but carries no owner labels: it costs money \
             that cannot be attributed, and nothing can stop it on an owner's behalf.",
            orphan.describe(),
        );
    }

    Ok(running)
}

/// Ceiling on infra PROJECTS running at once, across all users. The cluster
/// provisions nodes on demand, so this is a spend guard rather than a capacity
/// one: every project past it costs real money.
pub const MAX_GLOBAL_INFRA_PROJECTS: usize = 25;

/// Ceiling on infra projects running at once for one user. A project can hold
/// several infra nodes, and they come up and down together, so the unit a user
/// recognises is the project, not the deployment.
pub const MAX_USER_INFRA_PROJECTS: usize = 2;

/// Why a project cannot start right now, and how far over the line it is.
///
/// The sentence lives here because more than one place answers a user with it,
/// and two copies would drift. The status code and the error code stay with
/// whoever is answering.
#[derive(Debug, Clone)]
pub enum NoCapacity {
    /// The ceiling across all users, with the count that reached it.
    Global { running: usize, ceiling: usize },
    /// This user's own ceiling, with their count.
    User { running: usize, ceiling: usize },
}

impl std::fmt::Display for NoCapacity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NoCapacity::Global { running, ceiling } => write!(
                f, "Infrastructure is at capacity ({running}/{ceiling} projects running)."
            ),
            NoCapacity::User { running, ceiling } => write!(
                f,
                "You already have {running} projects running infrastructure (limit {ceiling}). \
                 Stop or terminate one to free a slot."
            ),
        }
    }
}

/// How many infra projects are running, from the two angles the ceilings care
/// about. Counted in projects, matching the ceilings.
#[derive(Debug, Clone, Copy)]
pub struct InfraInventory {
    pub global: usize,
    pub for_user: usize,
}

/// Count running projects, cluster-wide and for one user.
///
/// `starting_project` is left out of both counts: restarting a project must
/// never be refused because that same project is already up. Pure, so the
/// counting is testable without a cluster.
pub fn inventory_of(
    running: &[RunningInfra],
    user_id: Option<&str>,
    starting_project: &str,
) -> InfraInventory {
    let others = || {
        running
            .iter()
            .filter(|r| r.owner.as_ref().map(|o| o.project_id.as_str()) != Some(starting_project))
    };

    let mut projects: std::collections::HashSet<&str> = std::collections::HashSet::new();
    // A deployment nobody owns still costs money, so it fills a slot of its
    // own. It cannot count toward a user's ceiling: we do not know whose.
    let mut unattributed = 0usize;
    for infra in others() {
        match &infra.owner {
            Some(owner) => {
                projects.insert(owner.project_id.as_str());
            }
            None => unattributed += 1,
        }
    }

    let for_user = match user_id {
        Some(uid) => others()
            .filter_map(|r| r.owner.as_ref())
            .filter(|o| o.user_id == uid)
            .map(|o| o.project_id.as_str())
            .collect::<std::collections::HashSet<&str>>()
            .len(),
        None => 0,
    };

    InfraInventory { global: projects.len() + unattributed, for_user }
}

/// Whether there is room to start one more project.
pub fn check_capacity(inventory: InfraInventory) -> Result<(), NoCapacity> {
    if inventory.global >= MAX_GLOBAL_INFRA_PROJECTS {
        return Err(NoCapacity::Global {
            running: inventory.global,
            ceiling: MAX_GLOBAL_INFRA_PROJECTS,
        });
    }
    if inventory.for_user >= MAX_USER_INFRA_PROJECTS {
        return Err(NoCapacity::User {
            running: inventory.for_user,
            ceiling: MAX_USER_INFRA_PROJECTS,
        });
    }
    Ok(())
}

#[cfg(test)]
mod capacity_tests {
    use super::*;

    fn infra(user: &str, project: &str) -> RunningInfra {
        RunningInfra {
            owner: Some(InfraOwner { user_id: user.into(), project_id: project.into() }),
            namespace: format!("wm-{}", user.to_lowercase()),
            name: format!("wf-{project}-db"),
            started_at: None,
        }
    }

    fn orphan(name: &str) -> RunningInfra {
        RunningInfra {
            owner: None,
            namespace: "wm-old".into(),
            name: name.into(),
            started_at: None,
        }
    }

    fn deployment(replicas: Option<i64>, labels: &[(&str, &str)]) -> DynamicObject {
        let mut obj = DynamicObject::new(
            "wf-test-db",
            &ApiResource::from_gvk(&kube::api::GroupVersionKind::gvk("apps", "v1", "Deployment")),
        );
        obj.metadata.namespace = Some("wm-test".into());
        obj.metadata.labels = Some(labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect());
        obj.data = match replicas {
            Some(r) => serde_json::json!({ "spec": { "replicas": r } }),
            None => serde_json::json!({ "spec": {} }),
        };
        obj
    }

    #[test]
    fn an_omitted_replica_count_means_one() {
        assert!(is_running(&deployment(None, &[])), "Kubernetes defaults replicas to 1");
        assert!(is_running(&deployment(Some(1), &[])));
        assert!(!is_running(&deployment(Some(0), &[])));
    }

    #[test]
    fn owner_labels_are_read_off_the_deployment() {
        let got = running_from_item(&deployment(Some(1), &[
            (LABEL_USER, "UserA"), (LABEL_PROJECT, "p1"),
        ]));
        assert_eq!(got.owner, Some(InfraOwner { user_id: "UserA".into(), project_id: "p1".into() }));
        assert_eq!(got.describe(), "wm-test/wf-test-db");
    }

    #[test]
    fn a_deployment_missing_either_label_has_no_owner() {
        assert!(running_from_item(&deployment(Some(1), &[(LABEL_PROJECT, "p1")])).owner.is_none());
        assert!(running_from_item(&deployment(Some(1), &[(LABEL_USER, "UserA")])).owner.is_none());
    }

    #[test]
    fn an_unowned_deployment_fills_a_slot_but_no_ones_ceiling() {
        // It costs money, so the spend guard must see it. Nobody can be held to
        // it, so it must not push a user over their own limit.
        let running = [orphan("wf-ancient-db"), infra("UserA", "p1")];
        let inv = inventory_of(&running, Some("UserA"), "p2");
        assert_eq!(inv.global, 2);
        assert_eq!(inv.for_user, 1);
    }

    #[test]
    fn one_unowned_deployment_does_not_refuse_everyone_else() {
        // The state production is actually in: a single unlabelled deployment
        // left over from long ago must not stop anyone from starting.
        let running = [orphan("wf-d7ce5e2a6591-db")];
        assert!(check_capacity(inventory_of(&running, Some("UserA"), "p1")).is_ok());
    }

    #[test]
    fn several_deployments_of_one_project_count_once() {
        let running = [infra("UserA", "p1"), infra("UserA", "p1"), infra("UserA", "p2")];
        let inv = inventory_of(&running, Some("UserA"), "p3");
        assert_eq!(inv.global, 2);
        assert_eq!(inv.for_user, 2);
    }

    #[test]
    fn restarting_a_running_project_does_not_count_against_itself() {
        let running = [infra("UserA", "p1"), infra("UserA", "p1")];
        let inv = inventory_of(&running, Some("UserA"), "p1");
        assert_eq!(inv.for_user, 0, "a project must not block its own restart");
        assert_eq!(inv.global, 0);
        assert!(check_capacity(inv).is_ok());
    }

    #[test]
    fn user_ids_are_matched_exactly() {
        let running = [infra("UserA", "p1"), infra("usera", "p2")];
        let inv = inventory_of(&running, Some("UserA"), "p3");
        assert_eq!(inv.for_user, 1, "ids differing only in case are different users");
        assert_eq!(inv.global, 2);
    }

    #[test]
    fn another_users_projects_fill_only_the_global_ceiling() {
        let running: Vec<_> = (0..MAX_GLOBAL_INFRA_PROJECTS)
            .map(|i| infra("Someone", &format!("p{i}")))
            .collect();
        let inv = inventory_of(&running, Some("UserA"), "mine");
        assert_eq!(inv.for_user, 0);
        assert!(matches!(check_capacity(inv), Err(NoCapacity::Global { .. })));
    }

    #[test]
    fn the_global_ceiling_bites_before_the_users_own() {
        let err = check_capacity(InfraInventory {
            global: MAX_GLOBAL_INFRA_PROJECTS,
            for_user: MAX_USER_INFRA_PROJECTS,
        }).unwrap_err();
        assert!(matches!(err, NoCapacity::Global { .. }), "{err:?}");
    }

    #[test]
    fn a_users_own_ceiling_stops_them_while_the_cluster_is_free() {
        let err = check_capacity(InfraInventory { global: 0, for_user: MAX_USER_INFRA_PROJECTS })
            .unwrap_err();
        assert!(matches!(err, NoCapacity::User { .. }), "{err:?}");
    }

    #[test]
    fn the_last_free_slot_is_usable() {
        assert!(check_capacity(InfraInventory {
            global: MAX_GLOBAL_INFRA_PROJECTS - 1,
            for_user: MAX_USER_INFRA_PROJECTS - 1,
        }).is_ok());
    }
}

fn parse_gvk(api_version: &str, kind: &str) -> kube::api::GroupVersionKind {
    let (group, version) = if let Some(slash_pos) = api_version.find('/') {
        (&api_version[..slash_pos], &api_version[slash_pos + 1..])
    } else {
        ("", api_version.as_ref())
    };
    kube::api::GroupVersionKind::gvk(group, version, kind)
}

/// Reads the real cluster the way the ceilings do. Ignored by default because
/// it needs credentials; run it before a deploy to confirm the listing works
/// against the cluster you are deploying to:
///   KUBECONFIG=... cargo test -p weft-core live_cluster_inventory -- --ignored --nocapture
#[cfg(test)]
mod live_tests {
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn live_cluster_inventory() {
        let client = Client::try_default().await.expect("no cluster credentials");
        let running = list_running_infra(&client).await.expect("listing failed");

        println!("running infra deployments: {}", running.len());
        for infra in &running {
            match &infra.owner {
                Some(o) => println!("  {} user={} project={}", infra.describe(), o.user_id, o.project_id),
                None => println!("  {} UNATTRIBUTED", infra.describe()),
            }
        }

        let inventory = inventory_of(&running, None, "not-a-project");
        println!("global={}/{}", inventory.global, MAX_GLOBAL_INFRA_PROJECTS);
        assert!(
            inventory.global <= running.len(),
            "a project cannot count more slots than it has deployments"
        );
        println!("capacity says: {:?}", check_capacity(inventory));
    }
}
