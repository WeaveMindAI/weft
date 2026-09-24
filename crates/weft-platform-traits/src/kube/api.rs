//! Production impl: talks to the Kubernetes API in process, through one
//! client per process (`kube`), authenticated as the pod's service
//! account in the cluster and through the kubeconfig outside it. Every
//! call is a request on that client's connection pool; nothing is
//! spawned, and a watch is one long-lived request rather than a list
//! repeated on a timer.
//!
//! Manifests are applied server side (field manager [`FIELD_MANAGER`]):
//! the apiserver merges what weft declares with what others own, the
//! same result `kubectl apply` reached by merging on the client.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, StatefulSet};
use k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{
    ConfigMap, Namespace, PersistentVolumeClaim, Pod, Secret, Service,
};
use k8s_openapi::api::networking::v1::NetworkPolicy;
use kube::api::{
    Api, ApiResource, DeleteParams, DynamicObject, GroupVersionKind, ListParams, LogParams, Patch,
    PatchParams, PropagationPolicy,
};
use kube::runtime::{reflector, watcher, WatchStreamExt};
use kube::Resource;
use serde::de::DeserializeOwned;

use super::{
    DeleteOpts, KubeClient, KubeReader, KubeWriter, NamedKind, NodePortHolder, ReplicaWatch, WorkloadKind,
    WorkloadReplicaState,
};

/// Who weft's server-side applies are recorded under.
const FIELD_MANAGER: &str = "weft";

pub struct KubeApiClient {
    client: kube::Client,
    /// What the apiserver said about each kind a manifest named: its
    /// resource path, and whether it lives in a namespace. Asked once
    /// per kind per process.
    kinds: Mutex<HashMap<GroupVersionKind, (ApiResource, bool)>>,
}

impl KubeApiClient {
    /// The client for wherever this process runs: the pod's service
    /// account in the cluster, the kubeconfig's current context outside
    /// it.
    pub async fn connect() -> Result<Arc<dyn KubeClient>> {
        let client = kube::Client::try_default()
            .await
            .context("connect to the Kubernetes API (in-cluster service account, or the kubeconfig)")?;
        Ok(Arc::new(Self { client, kinds: Mutex::new(HashMap::new()) }))
    }

    /// Where a manifest's kind lives, from the apiserver's own discovery.
    async fn resource_of(&self, gvk: &GroupVersionKind) -> Result<(ApiResource, bool)> {
        if let Some(known) = self.kinds.lock().expect("the kind cache is never poisoned").get(gvk) {
            return Ok(known.clone());
        }
        let (resource, capabilities) = kube::discovery::pinned_kind(&self.client, gvk)
            .await
            .with_context(|| format!("the cluster does not serve {}/{} {}", gvk.group, gvk.version, gvk.kind))?;
        let found = (resource, capabilities.scope == kube::discovery::Scope::Namespaced);
        self.kinds.lock().expect("the kind cache is never poisoned").insert(gvk.clone(), found.clone());
        Ok(found)
    }

    /// Apply one object server side.
    async fn apply_object(&self, manifest: &serde_json::Value) -> Result<()> {
        let object: DynamicObject =
            serde_json::from_value(manifest.clone()).context("a manifest that is not a Kubernetes object")?;
        let types = object.types.as_ref().ok_or_else(|| anyhow!("a manifest with no apiVersion/kind"))?;
        let gvk = GroupVersionKind::try_from(types).map_err(|e| anyhow!("manifest apiVersion/kind: {e}"))?;
        let name = object.metadata.name.as_deref().ok_or_else(|| anyhow!("a {} with no name", gvk.kind))?;
        let (resource, namespaced) = self.resource_of(&gvk).await?;
        let api: Api<DynamicObject> = if namespaced {
            let namespace = object
                .metadata
                .namespace
                .as_deref()
                .ok_or_else(|| anyhow!("{} '{name}' names no namespace", gvk.kind))?;
            Api::namespaced_with(self.client.clone(), namespace, &resource)
        } else {
            Api::all_with(self.client.clone(), &resource)
        };
        api.patch(name, &PatchParams::apply(FIELD_MANAGER).force(), &Patch::Apply(manifest))
            .await
            .with_context(|| format!("apply {} '{name}'", gvk.kind))?;
        Ok(())
    }

    fn namespaced<K>(&self, namespace: &str) -> Api<K>
    where
        K: Resource<Scope = k8s_openapi::NamespaceResourceScope>,
        <K as Resource>::DynamicType: Default,
    {
        Api::namespaced(self.client.clone(), namespace)
    }
}

/// Whether a failed call failed because the object is not there.
fn is_not_found(e: &kube::Error) -> bool {
    matches!(e, kube::Error::Api(status) if status.code == 404)
}

/// A Deployment's or StatefulSet's replica state, as the health loop
/// reads it.
fn replica_state<K: Resource<DynamicType = ()>>(
    kind: WorkloadKind,
    object: &K,
    desired: Option<i32>,
    ready: Option<i32>,
) -> Result<WorkloadReplicaState> {
    let meta = object.meta();
    Ok(WorkloadReplicaState {
        kind,
        // A nameless workload cannot be scaled or deleted by name.
        name: meta.name.clone().ok_or_else(|| anyhow!("a {kind:?} with no name"))?,
        namespace: meta.namespace.clone().unwrap_or_default(),
        desired: desired.unwrap_or(0) as i64,
        ready: ready.unwrap_or(0) as i64,
        labels: meta.labels.clone().unwrap_or_default().into_iter().collect(),
    })
}

fn deployment_state(d: &Deployment) -> Result<WorkloadReplicaState> {
    replica_state(
        WorkloadKind::Deployment,
        d,
        d.spec.as_ref().and_then(|s| s.replicas),
        d.status.as_ref().and_then(|s| s.ready_replicas),
    )
}

fn statefulset_state(s: &StatefulSet) -> Result<WorkloadReplicaState> {
    replica_state(
        WorkloadKind::StatefulSet,
        s,
        s.spec.as_ref().and_then(|s| s.replicas),
        s.status.as_ref().and_then(|s| s.ready_replicas),
    )
}

/// What one watch event means for the set a watch hands out.
enum Seen {
    /// Part of a (re)list: the store is not a whole answer yet.
    MidList,
    /// The store changed and is whole again.
    Settled,
}

/// Classify a watch event, noting in `listed` when this kind's first
/// list completed.
fn noted<K>(listed: Arc<std::sync::atomic::AtomicBool>) -> impl FnMut(watcher::Event<K>) -> Seen {
    move |event| match event {
        watcher::Event::Init | watcher::Event::InitApply(_) => Seen::MidList,
        watcher::Event::InitDone => {
            listed.store(true, std::sync::atomic::Ordering::SeqCst);
            Seen::Settled
        }
        watcher::Event::Apply(_) | watcher::Event::Delete(_) => Seen::Settled,
    }
}

/// Whether a Deployment has rolled out: the controller has seen its
/// latest spec, and every wanted replica is updated and available. The
/// condition `kubectl rollout status` waits for.
fn rolled_out(deployment: Option<&Deployment>) -> bool {
    let Some(d) = deployment else { return false };
    let (Some(spec), Some(status)) = (d.spec.as_ref(), d.status.as_ref()) else { return false };
    let wanted = spec.replicas.unwrap_or(1);
    status.observed_generation.unwrap_or(0) >= d.metadata.generation.unwrap_or(0)
        && status.updated_replicas.unwrap_or(0) >= wanted
        && status.available_replicas.unwrap_or(0) >= wanted
        && status.replicas.unwrap_or(0) <= wanted
}

/// Delete every object of one kind matching `selector`, each by name,
/// not waiting for finalizers. One that is already gone is fine.
async fn delete_matching<K>(api: Api<K>, selector: &str) -> Result<()>
where
    K: Resource + Clone + DeserializeOwned + std::fmt::Debug,
{
    let listed = api.list_metadata(&ListParams::default().labels(selector)).await?;
    for object in listed.items {
        let Some(name) = object.metadata.name else { continue };
        match api.delete(&name, &DeleteParams::background()).await {
            Ok(_) => {}
            Err(e) if is_not_found(&e) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// The API a `TenantPublic` endpoint's route lives on; not a kind the
/// generated bindings carry.
fn http_routes(client: kube::Client, namespace: &str) -> Api<DynamicObject> {
    let resource = ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("gateway.networking.k8s.io", "v1", "HTTPRoute"),
        "httproutes",
    );
    Api::namespaced_with(client, namespace, &resource)
}

#[async_trait]
impl KubeReader for KubeApiClient {
    async fn list_replica_state(
        &self,
        namespace: &str,
        selector: &str,
    ) -> Result<Vec<WorkloadReplicaState>> {
        // Fail loud on an error. An empty answer on a failed call would
        // read as "no workloads, ratio 1.0, healthy" to the health loop,
        // hiding an RBAC regression or an apiserver outage.
        let params = ListParams::default().labels(selector);
        let deployments = self.namespaced::<Deployment>(namespace).list(&params).await
            .with_context(|| format!("list deployments -l {selector} in {namespace}"))?;
        let statefulsets = self.namespaced::<StatefulSet>(namespace).list(&params).await
            .with_context(|| format!("list statefulsets -l {selector} in {namespace}"))?;
        let mut out = Vec::with_capacity(deployments.items.len() + statefulsets.items.len());
        for d in &deployments.items {
            out.push(deployment_state(d)?);
        }
        for s in &statefulsets.items {
            out.push(statefulset_state(s)?);
        }
        Ok(out)
    }

    async fn watch_replica_state(&self, namespace: &str, selector: &str) -> Result<ReplicaWatch> {
        let config = watcher::Config::default().labels(selector);
        let (deployments, deployments_writer) = reflector::store::<Deployment>();
        let (statefulsets, statefulsets_writer) = reflector::store::<StatefulSet>();
        // Each kind has listed once (and so its store is a whole answer)
        // once its first `InitDone` passed. A relist keeps the previous
        // answer in the store until its own `InitDone` swaps it.
        let deployments_listed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let statefulsets_listed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let deployment_events = reflector(
            deployments_writer,
            watcher(self.namespaced::<Deployment>(namespace), config.clone()),
        )
        .default_backoff()
        .map_ok(noted(deployments_listed.clone()));
        let statefulset_events = reflector(
            statefulsets_writer,
            watcher(self.namespaced::<StatefulSet>(namespace), config),
        )
        .default_backoff()
        .map_ok(noted(statefulsets_listed.clone()));
        let label = format!("{namespace} -l {selector}");
        let changes = futures::stream::select(deployment_events, statefulset_events).filter_map(move |event| {
            let set = match event {
                Err(e) => Some(Err(anyhow!("watch of workloads in {label}: {e}"))),
                // Mid-list: the store is not a whole answer yet.
                Ok(Seen::MidList) => None,
                // Until both kinds have listed once, the set would be
                // missing one of them.
                Ok(Seen::Settled)
                    if !deployments_listed.load(std::sync::atomic::Ordering::SeqCst)
                        || !statefulsets_listed.load(std::sync::atomic::Ordering::SeqCst) =>
                {
                    None
                }
                Ok(Seen::Settled) => Some(
                    deployments
                        .state()
                        .iter()
                        .map(|d| deployment_state(d))
                        .chain(statefulsets.state().iter().map(|s| statefulset_state(s)))
                        .collect::<Result<Vec<_>>>(),
                ),
            };
            std::future::ready(set)
        });
        Ok(changes.boxed())
    }

    async fn pod_waiting_reason(&self, namespace: &str, pod_name: &str) -> Result<Option<String>> {
        let pod = self
            .namespaced::<Pod>(namespace)
            .get_opt(pod_name)
            .await
            .with_context(|| format!("get pod {pod_name} (ns {namespace})"))?;
        Ok(pod
            .and_then(|p| p.status)
            .and_then(|s| s.container_statuses)
            .and_then(|c| c.into_iter().next())
            .and_then(|c| c.state)
            .and_then(|s| s.waiting)
            .and_then(|w| w.reason)
            .filter(|r| !r.is_empty()))
    }

    async fn pod_phase(&self, namespace: &str, pod_name: &str) -> Result<Option<String>> {
        // `None` means exactly "the pod does not exist". Any other
        // failure (apiserver error, expired credentials, network
        // partition) propagates: a caller acting on "the pod vanished"
        // must never be fed a transient failure dressed up as absence.
        let pod = self
            .namespaced::<Pod>(namespace)
            .get_opt(pod_name)
            .await
            .with_context(|| format!("get pod {pod_name} (ns {namespace})"))?;
        Ok(pod.and_then(|p| p.status).and_then(|s| s.phase).filter(|p| !p.is_empty()))
    }

    async fn node_ports(&self) -> Result<Vec<NodePortHolder>> {
        let services = Api::<Service>::all(self.client.clone())
            .list(&ListParams::default())
            .await
            .context("list services in every namespace")?;
        let mut holders = Vec::new();
        for service in services.items {
            let namespace = service.metadata.namespace.clone().unwrap_or_default();
            let name = service.metadata.name.clone().unwrap_or_default();
            for port in service.spec.iter().flat_map(|s| s.ports.iter().flatten()) {
                let Some(node_port) = port.node_port else { continue };
                // A node port outside u16 is the API having changed
                // under us; skipping it would let a door pick a port
                // somebody already holds.
                let port = u16::try_from(node_port)
                    .map_err(|_| anyhow!("service {namespace}/{name} holds node port {node_port}"))?;
                holders.push(NodePortHolder { namespace: namespace.clone(), service: name.clone(), port });
            }
        }
        Ok(holders)
    }

    async fn pod_logs(&self, namespace: &str, pod_name: &str, container: &str) -> Result<String> {
        let params = LogParams { container: Some(container.to_string()), ..LogParams::default() };
        self.namespaced::<Pod>(namespace)
            .logs(pod_name, &params)
            .await
            .with_context(|| format!("logs of {pod_name} (ns {namespace})"))
    }
}

#[async_trait]
impl KubeWriter for KubeApiClient {
    async fn scale_workload(
        &self,
        namespace: &str,
        kind: WorkloadKind,
        name: &str,
        replicas: u32,
    ) -> Result<()> {
        let scale = Patch::Merge(serde_json::json!({ "spec": { "replicas": replicas } }));
        let params = PatchParams::default();
        match kind {
            WorkloadKind::Deployment => {
                self.namespaced::<Deployment>(namespace).patch_scale(name, &params, &scale).await?;
            }
            WorkloadKind::StatefulSet => {
                self.namespaced::<StatefulSet>(namespace).patch_scale(name, &params, &scale).await?;
            }
        }
        Ok(())
    }

    async fn delete_named(&self, namespace: &str, kind: NamedKind, name: &str, opts: DeleteOpts) -> Result<()> {
        let params = if opts.cascades() {
            DeleteParams { propagation_policy: Some(PropagationPolicy::Foreground), ..DeleteParams::default() }
        } else {
            DeleteParams::background()
        };
        match kind {
            NamedKind::Pod => delete_one(self.namespaced::<Pod>(namespace), name, &params, opts.waits()).await,
            NamedKind::Service => delete_one(self.namespaced::<Service>(namespace), name, &params, opts.waits()).await,
            NamedKind::Deployment => delete_one(self.namespaced::<Deployment>(namespace), name, &params, opts.waits()).await,
        }
        .with_context(|| format!("delete {kind}/{name} in namespace {namespace}"))
    }

    async fn delete_by_label(&self, namespace: &str, selector: &str, preserve_pvcs: &[String]) -> Result<()> {
        let c = || self.client.clone();
        delete_matching(Api::<Deployment>::namespaced(c(), namespace), selector).await?;
        delete_matching(Api::<StatefulSet>::namespaced(c(), namespace), selector).await?;
        delete_matching(Api::<DaemonSet>::namespaced(c(), namespace), selector).await?;
        delete_matching(Api::<Job>::namespaced(c(), namespace), selector).await?;
        delete_matching(Api::<Service>::namespaced(c(), namespace), selector).await?;
        delete_matching(Api::<ConfigMap>::namespaced(c(), namespace), selector).await?;
        delete_matching(Api::<Secret>::namespaced(c(), namespace), selector).await?;
        delete_matching(http_routes(c(), namespace), selector).await?;
        delete_matching(Api::<HorizontalPodAutoscaler>::namespaced(c(), namespace), selector).await?;
        delete_matching(Api::<NetworkPolicy>::namespaced(c(), namespace), selector).await?;
        delete_matching(Api::<Pod>::namespaced(c(), namespace), selector).await?;
        // PVCs: every match except the ones the node's lifecycle keeps.
        let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(c(), namespace);
        let listed = pvcs.list_metadata(&ListParams::default().labels(selector)).await?;
        for pvc in listed.items {
            let Some(name) = pvc.metadata.name else { continue };
            if preserve_pvcs.contains(&name) {
                continue;
            }
            match pvcs.delete(&name, &DeleteParams::background()).await {
                Ok(_) => {}
                Err(e) if is_not_found(&e) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    async fn delete_pods(&self, namespace: &str, selector: &str) -> Result<()> {
        // Pods only. The Deployment / Service / ConfigMap / Secret /
        // PVC stay; the controller respawns Pods with the same spec.
        delete_matching(self.namespaced::<Pod>(namespace), selector).await
    }

    async fn apply(&self, manifest: &serde_json::Value) -> Result<()> {
        self.apply_object(manifest).await
    }

    async fn apply_yaml(&self, manifest: &str) -> Result<()> {
        // Read every document first: the reader cannot be held across a
        // request.
        let documents = serde_yaml::Deserializer::from_str(manifest)
            .map(|document| {
                <serde_json::Value as serde::Deserialize>::deserialize(document)
                    .context("a manifest document that is not YAML")
            })
            .collect::<Result<Vec<_>>>()?;
        for document in documents.iter().filter(|d| !d.is_null()) {
            self.apply_object(document).await?;
        }
        Ok(())
    }

    async fn delete_namespace(&self, name: &str) -> Result<()> {
        // The namespace finalizer reaps what is inside asynchronously;
        // nothing waits on it here.
        match Api::<Namespace>::all(self.client.clone()).delete(name, &DeleteParams::background()).await {
            Ok(_) => Ok(()),
            Err(e) if is_not_found(&e) => Ok(()),
            Err(e) => Err(anyhow::Error::from(e).context(format!("delete namespace {name}"))),
        }
    }

    async fn wait_rollout_status(&self, namespace: &str, deployment: &str, timeout_seconds: u32) -> Result<()> {
        let rolled = kube::runtime::wait::await_condition(
            self.namespaced::<Deployment>(namespace),
            deployment,
            rolled_out,
        );
        match tokio::time::timeout(Duration::from_secs(timeout_seconds as u64), rolled).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(anyhow!("watch the rollout of {deployment}: {e}")),
            Err(_) => anyhow::bail!("{deployment} did not reach Ready within {timeout_seconds}s"),
        }
    }
}

/// Delete one object; one that is already gone is fine. With `wait`,
/// return only once it is gone (its dependents too, under a foreground
/// cascade).
async fn delete_one<K>(api: Api<K>, name: &str, params: &DeleteParams, wait: bool) -> Result<()>
where
    K: Resource + Clone + DeserializeOwned + std::fmt::Debug + Send + 'static,
    <K as Resource>::DynamicType: Default,
{
    let deleting = match api.delete(name, params).await {
        Ok(either) => either,
        Err(e) if is_not_found(&e) => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    if !wait {
        return Ok(());
    }
    // `Right` is the apiserver saying it is already gone; `Left` is the
    // object, still there while it is being deleted.
    let Some(object) = deleting.left() else { return Ok(()) };
    // Without its uid there is no telling this object from a successor
    // of the same name, so the wait could not be honest.
    let Some(uid) = object.meta().uid.clone() else {
        anyhow::bail!("the apiserver answered the delete of {name} with an object that has no uid, so its deletion cannot be waited on");
    };
    kube::runtime::wait::await_condition(api, name, kube::runtime::wait::conditions::is_deleted(&uid))
        .await
        .map_err(|e| anyhow!("wait for {name} to be deleted: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::apps::v1::{DeploymentSpec, DeploymentStatus};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    fn deployment(generation: i64, wanted: i32, status: DeploymentStatus) -> Deployment {
        Deployment {
            metadata: ObjectMeta { name: Some("d".into()), generation: Some(generation), ..ObjectMeta::default() },
            spec: Some(DeploymentSpec { replicas: Some(wanted), ..DeploymentSpec::default() }),
            status: Some(status),
        }
    }

    #[test]
    fn a_deployment_has_rolled_out_once_every_wanted_replica_is_updated_and_available() {
        let done = DeploymentStatus {
            observed_generation: Some(2),
            replicas: Some(2),
            updated_replicas: Some(2),
            available_replicas: Some(2),
            ..DeploymentStatus::default()
        };
        assert!(rolled_out(Some(&deployment(2, 2, done.clone()))));
        assert!(!rolled_out(Some(&deployment(3, 2, done.clone()))), "the controller has not seen the new spec");
        let old_pod_still_up = DeploymentStatus { replicas: Some(3), ..done.clone() };
        assert!(!rolled_out(Some(&deployment(2, 2, old_pod_still_up))));
        let not_available = DeploymentStatus { available_replicas: Some(1), ..done };
        assert!(!rolled_out(Some(&deployment(2, 2, not_available))));
        assert!(!rolled_out(None));
    }

    #[test]
    fn a_workload_reads_its_wanted_and_ready_replicas_and_labels() {
        let mut d = deployment(1, 3, DeploymentStatus { ready_replicas: Some(2), ..DeploymentStatus::default() });
        d.metadata.namespace = Some("ns".into());
        d.metadata.labels = Some([("weft.dev/unit".to_string(), "db".to_string())].into_iter().collect());
        let state = deployment_state(&d).unwrap();
        assert_eq!((state.kind, state.desired, state.ready), (WorkloadKind::Deployment, 3, 2));
        assert_eq!(state.labels.get("weft.dev/unit").map(String::as_str), Some("db"));
        let nameless = Deployment::default();
        assert!(deployment_state(&nameless).is_err());
    }
}
