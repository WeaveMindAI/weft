//! Compile an `InfraSpec` into a list of kubernetes manifest JSON
//! documents. Pure function: same inputs → same outputs. The apply
//! executor passes the result to `kubectl apply`.
//!
//! Every emitted manifest is stamped with the `weft.dev/*` label
//! set. Node authors do NOT add these labels themselves; if they
//! do, we overwrite to keep the trust contract clean.


use serde_json::{json, Map, Value};

use super::types::{
    AccessMode, AutoscaleMetric, ConfigSource, Container, ContainerPort, EgressRule, EnvEntry,
    Expose, Image, IngressRule, InfraSpec, Mount, ProbeKind, Protocol, Resources,
    Unit, UnitKind, UpgradeBehavior, Volume, VolumeKind,
};

/// Per-apply context the compiler needs but isn't on the spec
/// itself. Threaded in by the apply executor.
pub struct CompileContext<'a> {
    pub tenant_id: &'a str,
    pub project_id: &'a str,
    pub node_id: &'a str,
    /// Stable instance id for this provision. Used as the base name
    /// for emitted resources (Deployment name, Service name prefix,
    /// PVC name prefix). The compiler appends suffixes like
    /// `-<endpoint_name>` for per-endpoint Services.
    pub instance_id: &'a str,
    pub namespace: &'a str,
    /// Resolution map for `Image::Local { name }` references. Maps
    /// the local name (`"bridge"`) to the concrete docker tag
    /// (`"weft-infra-bridge:abc123"`). Provided by the CLI. Must be
    /// a `BTreeMap` so iteration is deterministic (the downstream
    /// hash mixes it in and HashMap order would randomize).
    /// `Image::Upstream(...)` references bypass this map.
    pub local_image_tags: &'a std::collections::BTreeMap<String, String>,
}

#[derive(Debug, thiserror::Error)]
pub enum CompileError {
    #[error("infra spec for node '{node}': no image declared with local name '{name}'; \
             add it to NodeMetadata.images and provide a Dockerfile")]
    MissingLocalImage { node: String, name: String },
    #[error("infra spec for node '{node}': endpoint '{endpoint}' references unit '{unit}' \
             but no such Unit was declared")]
    EndpointUnitMissing {
        node: String,
        endpoint: String,
        unit: String,
    },
    #[error("infra spec for node '{node}': endpoint '{endpoint}' targets container \
             '{container}' in unit '{unit}', but the Unit has no such container")]
    EndpointContainerMissing {
        node: String,
        endpoint: String,
        unit: String,
        container: String,
    },
    #[error("infra spec for node '{node}': endpoint '{endpoint}' references port name \
             '{port}' on container '{container}'/unit '{unit}', but no ContainerPort with \
             that name was declared")]
    EndpointPortMissing {
        node: String,
        endpoint: String,
        unit: String,
        container: String,
        port: String,
    },
    #[error(
        "infra spec for node '{node}': resource name '{name}' is {len} chars, max {limit} \
         (k8s DNS-1123 label limit). Built from: {details}. Shorten the {source_kind} name."
    )]
    NameTooLong {
        node: String,
        name: String,
        len: usize,
        limit: usize,
        /// What the long name was built from (e.g. "instance + unit",
        /// "instance + endpoint", "instance + volume").
        details: String,
        /// The user-facing field to shorten (e.g. "unit", "endpoint",
        /// "volume", "instance_id").
        source_kind: &'static str,
    },
    #[error(
        "infra spec for node '{node}': resource name '{name}' has invalid characters for a \
         k8s DNS-1123 label (only lowercase alphanumeric and '-', must start/end with \
         alphanumeric). Built from: {details}. Fix the {source_kind} name."
    )]
    NameInvalid {
        node: String,
        name: String,
        details: String,
        source_kind: &'static str,
    },
    #[error(
        "infra spec for node '{node}': two {k8s_kind} resources both named '{name}' \
         (from {source_kind}). k8s requires unique metadata.name per (kind, namespace); \
         a duplicate of the same kind would fail at apply. Common cause: two Units with \
         empty `name` (the bare instance_id is used as a fallback). Set distinct names."
    )]
    DuplicateName {
        node: String,
        name: String,
        source_kind: &'static str,
        k8s_kind: &'static str,
    },
    #[error(
        "infra spec for node '{node}': unit '{unit}' declares on_upgrade but its kind \
         is {unit_kind} (not Deployment). on_upgrade is only honored on Deployment; \
         declaring it on {unit_kind} would be silently ignored. Either drop on_upgrade \
         from this unit, or change the unit's kind to Deployment."
    )]
    UpgradeBehaviorOnNonDeployment {
        node: String,
        unit: String,
        unit_kind: &'static str,
    },
    #[error(
        "infra spec for node '{node}': unit '{unit}' declares autoscale \
         but no metrics. An HPA with an empty metrics list is rejected by \
         the kube apiserver at apply time. Add at least one metric \
         (e.g. cpu utilization) or drop the autoscale block."
    )]
    AutoscaleWithoutMetrics { node: String, unit: String },
}

/// k8s DNS-1123 label limit. Service, Deployment, StatefulSet,
/// DaemonSet, Pod, PVC, ConfigMap, Secret, HPA, Ingress,
/// NetworkPolicy names all sit under this cap.
const DNS_1123_LABEL_MAX: usize = 63;

fn check_name(
    name: &str,
    ctx: &CompileContext<'_>,
    details: impl Into<String>,
    source_kind: &'static str,
) -> Result<(), CompileError> {
    if name.len() > DNS_1123_LABEL_MAX {
        return Err(CompileError::NameTooLong {
            node: ctx.node_id.to_string(),
            name: name.to_string(),
            len: name.len(),
            limit: DNS_1123_LABEL_MAX,
            details: details.into(),
            source_kind,
        });
    }
    if !is_dns1123_label(name) {
        return Err(CompileError::NameInvalid {
            node: ctx.node_id.to_string(),
            name: name.to_string(),
            details: details.into(),
            source_kind,
        });
    }
    Ok(())
}

/// DNS-1123 label: lowercase alphanumeric or '-', must start and
/// end with alphanumeric, max 63 chars. The length check is done
/// separately in `check_name`; this only validates the character
/// class + edge anchors.
fn is_dns1123_label(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    let bytes = name.as_bytes();
    let first = bytes[0];
    let last = bytes[bytes.len() - 1];
    let is_alnum = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit();
    if !is_alnum(first) || !is_alnum(last) {
        return false;
    }
    bytes.iter().all(|&b| is_alnum(b) || b == b'-')
}

/// Compile an `InfraSpec` into a list of k8s manifests. Each entry
/// is a fully-stamped manifest ready for `kubectl apply`.
///
/// Determinism: every map in the spec is a `BTreeMap` and serde_json
/// (without the `preserve_order` feature) emits object keys sorted,
/// so the output is byte-stable across calls with the same input.
/// This is what makes the infra hash a reliable drift key; see
/// `weft-compiler/src/hash.rs` (`compute_infra_hash`) and
/// `spec_serializes_map_keys_in_sorted_order`.
pub fn compile(spec: &InfraSpec, ctx: &CompileContext<'_>) -> Result<Vec<Value>, CompileError> {
    // Pre-flight: every name we will stamp on a `metadata.name` is
    // enumerated by `emitted_names`. Length-check the lot here so
    // kubectl never sees an over-long name. The enumerator is the
    // single source of truth: if compile.rs grows a new resource
    // kind, the contract is "add it to emitted_names" : drift
    // shows up at the call-site of `check_name`, not deep in some
    // kubectl error message.
    // Per-name validation (length + char class) AND pairwise
    // dedup keyed by `(k8s kind, name)`. k8s requires unique
    // `metadata.name` per (kind, namespace): a Deployment "x"
    // and a ConfigMap "x" coexist legally. Keying by `name` alone
    // would over-enforce.
    let names = emitted_names(spec, ctx);
    let mut seen: std::collections::HashSet<(K8sKind, String)> =
        std::collections::HashSet::new();
    for entry in &names {
        check_name(&entry.name, ctx, entry.details.clone(), entry.source_kind)?;
        if !seen.insert((entry.kind, entry.name.clone())) {
            return Err(CompileError::DuplicateName {
                node: ctx.node_id.to_string(),
                name: entry.name.clone(),
                source_kind: entry.source_kind,
                k8s_kind: entry.kind.display(),
            });
        }
    }

    // -- Per-unit validation: on_upgrade only applies to Deployment.
    // Setting it on StatefulSet / DaemonSet / Job would be silently
    // dropped (the `compile_workload` strategy stamp is gated on
    // `UnitKind::Deployment`). Fail loud instead so the user knows
    // their intent isn't taking effect.
    for unit in &spec.units {
        if !unit.on_upgrade.is_default() && !matches!(unit.kind, UnitKind::Deployment) {
            return Err(CompileError::UpgradeBehaviorOnNonDeployment {
                node: ctx.node_id.to_string(),
                unit: unit.name.clone(),
                unit_kind: K8sKind::from_unit_kind(unit.kind).display(),
            });
        }
        // An HPA with zero metrics is rejected by the apiserver at
        // apply. Fail loud at compile with the offending unit named,
        // rather than at apply with a generic k8s error.
        if let Some(auto) = &unit.scaling.autoscale {
            if auto.metrics.is_empty() {
                return Err(CompileError::AutoscaleWithoutMetrics {
                    node: ctx.node_id.to_string(),
                    unit: unit.name.clone(),
                });
            }
        }
    }

    let mut out = Vec::new();

    // -- ConfigMap / Secret literals (created in the project ns) --
    for cfg in &spec.config {
        if let Some(m) = compile_config(cfg, ctx) {
            out.push(m);
        }
    }

    // -- Workloads + their PVCs --
    for unit in &spec.units {
        out.push(compile_workload(unit, spec, ctx)?);
    }
    for vol in &spec.volumes {
        if let Some(m) = compile_volume(vol, ctx) {
            out.push(m);
        }
    }

    // -- Services + optional Ingress per endpoint --
    for ep in &spec.endpoints {
        validate_endpoint(spec, ep, ctx)?;
        out.push(compile_service(ep, spec, ctx));
        if let Expose::TenantPublic { path } = &ep.expose {
            out.push(compile_ingress(ep, path, ctx));
        }
    }

    // -- HorizontalPodAutoscalers per Unit that opts in --
    for unit in &spec.units {
        if let Some(hpa) = compile_hpa(unit, ctx) {
            out.push(hpa);
        }
    }

    // -- NetworkPolicy: per-node access overlay on top of the
    //    namespace's default-deny + per-role baselines.
    out.push(compile_network_policy(spec, ctx));

    // Every typed-emitter manifest has a namespace by construction
    // at this point. Stamp weft labels on each.
    for m in &mut out {
        stamp_weft_labels(m, ctx);
    }

    Ok(out)
}

// -----------------------------------------------------------------
// Workload compilation
// -----------------------------------------------------------------

fn compile_workload(
    unit: &Unit,
    spec: &InfraSpec,
    ctx: &CompileContext<'_>,
) -> Result<Value, CompileError> {
    let unit_name = unit_name(unit, ctx);
    let pod_template = compile_pod_template(unit, spec, ctx)?;

    let replicas = match unit.kind {
        // Jobs ignore replicas; k8s defaults to 1 and is governed by
        // completions/parallelism. We don't expose those yet; if a
        // user needs them, declare them on the Unit directly.
        UnitKind::Job => 1,
        // DaemonSet replicas is per-node; the spec.replicas field
        // doesn't apply. Use 1 as a placeholder; k8s ignores it.
        UnitKind::DaemonSet => 1,
        UnitKind::Deployment | UnitKind::StatefulSet => unit.scaling.replicas,
    };

    let kind = match unit.kind {
        UnitKind::Deployment => "Deployment",
        UnitKind::StatefulSet => "StatefulSet",
        UnitKind::DaemonSet => "DaemonSet",
        UnitKind::Job => "Job",
    };
    let api_version = match unit.kind {
        UnitKind::Deployment | UnitKind::StatefulSet | UnitKind::DaemonSet => "apps/v1",
        UnitKind::Job => "batch/v1",
    };

    // Selector + template labels. Both reference weft.dev/unit so
    // multi-unit nodes don't share a selector.
    let selector_labels = json!({
        "weft.dev/instance": ctx.instance_id,
        "weft.dev/unit": unit.name,
    });

    let mut spec_obj = Map::new();
    // Omit `replicas` when an HPA owns this workload: a static
    // `spec.replicas` + an HPA targeting the same workload is the
    // classic autoscaler fight (every apply resets replicas, the
    // HPA scales it back). The HPA's `minReplicas` governs the
    // floor instead. DaemonSet/Job never carry replicas anyway.
    if !matches!(unit.kind, UnitKind::DaemonSet | UnitKind::Job) && !should_emit_hpa(unit) {
        spec_obj.insert("replicas".into(), json!(replicas));
    }
    if !matches!(unit.kind, UnitKind::Job) {
        spec_obj.insert(
            "selector".into(),
            json!({ "matchLabels": selector_labels }),
        );
    }
    spec_obj.insert("template".into(), pod_template);
    if matches!(unit.kind, UnitKind::Deployment) {
        // Configure strategy from this Unit's on_upgrade. Per-Unit
        // because a node with multiple Deployments may want
        // different strategies per workload.
        if let Some(strategy) = compile_deployment_strategy(&unit.on_upgrade) {
            spec_obj.insert("strategy".into(), strategy);
        }
        if let UpgradeBehavior::Rolling {
            progress_deadline_seconds,
            ..
        } = unit.on_upgrade
        {
            spec_obj.insert(
                "progressDeadlineSeconds".into(),
                json!(progress_deadline_seconds),
            );
        }
    }
    if matches!(unit.kind, UnitKind::StatefulSet) {
        spec_obj.insert("serviceName".into(), json!(unit_name.clone()));
    }

    Ok(json!({
        "apiVersion": api_version,
        "kind": kind,
        "metadata": {
            "name": unit_name,
            "namespace": ctx.namespace,
            "labels": { "weft.dev/unit": unit.name },
        },
        "spec": spec_obj,
    }))
}

fn compile_deployment_strategy(behavior: &UpgradeBehavior) -> Option<Value> {
    match behavior {
        UpgradeBehavior::Rolling {
            max_unavailable,
            max_surge,
            ..
        } => {
            let mut rolling = Map::new();
            if let Some(mu) = max_unavailable.clone() {
                rolling.insert("maxUnavailable".into(), json!(mu));
            }
            if let Some(ms) = max_surge.clone() {
                rolling.insert("maxSurge".into(), json!(ms));
            }
            Some(json!({
                "type": "RollingUpdate",
                "rollingUpdate": rolling,
            }))
        }
        UpgradeBehavior::Recreate => Some(json!({ "type": "Recreate" })),
    }
}

fn compile_pod_template(
    unit: &Unit,
    spec: &InfraSpec,
    ctx: &CompileContext<'_>,
) -> Result<Value, CompileError> {
    let mut containers = Vec::new();
    for c in &unit.containers {
        containers.push(compile_container(c, unit, ctx)?);
    }
    let mut init = Vec::new();
    for c in &unit.init_containers {
        init.push(compile_container(c, unit, ctx)?);
    }

    let volumes = compile_pod_volumes(unit, spec, ctx);

    let mut pod_spec = Map::new();
    pod_spec.insert("containers".into(), json!(containers));
    if !init.is_empty() {
        pod_spec.insert("initContainers".into(), json!(init));
    }
    pod_spec.insert(
        "serviceAccountName".into(),
        json!(unit
            .pod_options
            .service_account
            .clone()
            .unwrap_or_else(|| "weft-infra-sa".to_string())),
    );
    if !volumes.is_empty() {
        pod_spec.insert("volumes".into(), json!(volumes));
    }
    if let Some(ns) = &unit.pod_options.node_selector {
        pod_spec.insert("nodeSelector".into(), json!(ns));
    }
    if !unit.pod_options.tolerations.is_empty() {
        pod_spec.insert("tolerations".into(), json!(unit.pod_options.tolerations));
    }
    if let Some(pc) = &unit.pod_options.priority_class {
        pod_spec.insert("priorityClassName".into(), json!(pc));
    }
    if let Some(sc) = &unit.pod_options.security_context {
        pod_spec.insert("securityContext".into(), json!(sc));
    }
    if let Some(tg) = unit.pod_options.termination_grace_period_seconds {
        pod_spec.insert("terminationGracePeriodSeconds".into(), json!(tg));
    }

    Ok(json!({
        "metadata": {
            "labels": {
                "weft.dev/instance": ctx.instance_id,
                "weft.dev/unit": unit.name,
                "weft.dev/tenant": ctx.tenant_id,
                "weft.dev/project": ctx.project_id,
                "weft.dev/node": ctx.node_id,
                "weft.dev/role": "infra",
            }
        },
        "spec": pod_spec,
    }))
}

fn compile_container(
    c: &Container,
    _unit: &Unit,
    ctx: &CompileContext<'_>,
) -> Result<Value, CompileError> {
    let image = resolve_image(&c.image, ctx)?;
    let mut obj = Map::new();
    obj.insert("name".into(), json!(c.name));
    obj.insert("image".into(), json!(image));
    if let Some(cmd) = &c.command {
        obj.insert("command".into(), json!(cmd));
    }
    if !c.args.is_empty() {
        obj.insert("args".into(), json!(c.args));
    }
    if !c.env.is_empty() {
        obj.insert("env".into(), json!(compile_env(&c.env)));
    }
    if !c.ports.is_empty() {
        obj.insert("ports".into(), json!(compile_ports(&c.ports)));
    }
    if let Some(res) = compile_resources(&c.resources) {
        obj.insert("resources".into(), res);
    }
    if !c.mounts.is_empty() {
        obj.insert("volumeMounts".into(), json!(compile_mounts(&c.mounts)));
    }
    if let Some(p) = &c.readiness {
        obj.insert("readinessProbe".into(), compile_probe(&p.kind, p));
    }
    if let Some(p) = &c.liveness {
        obj.insert("livenessProbe".into(), compile_probe(&p.kind, p));
    }
    if let Some(p) = &c.startup {
        obj.insert("startupProbe".into(), compile_probe(&p.kind, p));
    }
    if let Some(sc) = &c.security_context {
        obj.insert("securityContext".into(), json!(sc));
    }
    if let Some(pre) = &c.pre_stop {
        let handler = match pre {
            super::types::PreStopHook::Http { path, port } => json!({
                "httpGet": {
                    "path": path,
                    "port": port,
                }
            }),
            super::types::PreStopHook::Exec { command } => json!({
                "exec": { "command": command }
            }),
        };
        obj.insert("lifecycle".into(), json!({ "preStop": handler }));
    }
    Ok(Value::Object(obj))
}

fn resolve_image(image: &Image, ctx: &CompileContext<'_>) -> Result<String, CompileError> {
    match image {
        Image::Upstream { reference } => Ok(reference.clone()),
        Image::Local { name } => ctx
            .local_image_tags
            .get(name)
            .cloned()
            .ok_or_else(|| CompileError::MissingLocalImage {
                node: ctx.node_id.to_string(),
                name: name.clone(),
            }),
    }
}

fn compile_env(env: &[EnvEntry]) -> Vec<Value> {
    env.iter()
        .map(|e| match e {
            EnvEntry::Literal { name, value } => json!({ "name": name, "value": value }),
            EnvEntry::FromConfigMap {
                name,
                config_map,
                key,
            } => json!({
                "name": name,
                "valueFrom": {
                    "configMapKeyRef": { "name": config_map, "key": key }
                }
            }),
            EnvEntry::FromSecret { name, secret, key } => json!({
                "name": name,
                "valueFrom": {
                    "secretKeyRef": { "name": secret, "key": key }
                }
            }),
            EnvEntry::Downward { name, field_path } => json!({
                "name": name,
                "valueFrom": {
                    "fieldRef": { "fieldPath": field_path }
                }
            }),
        })
        .collect()
}

fn compile_ports(ports: &[ContainerPort]) -> Vec<Value> {
    ports
        .iter()
        .map(|p| {
            let proto = match p.protocol {
                Protocol::Tcp => "TCP",
                Protocol::Udp => "UDP",
                Protocol::Sctp => "SCTP",
            };
            json!({
                "name": p.name,
                "containerPort": p.port,
                "protocol": proto,
            })
        })
        .collect()
}

fn compile_resources(r: &Resources) -> Option<Value> {
    let has_request = r.cpu_request.is_some() || r.memory_request.is_some();
    let has_limit = r.cpu_limit.is_some() || r.memory_limit.is_some() || r.gpu.is_some()
        || !r.extra.is_empty();
    if !has_request && !has_limit {
        return None;
    }
    let mut obj = Map::new();
    if has_request {
        let mut req = Map::new();
        if let Some(c) = &r.cpu_request {
            req.insert("cpu".into(), json!(c));
        }
        if let Some(m) = &r.memory_request {
            req.insert("memory".into(), json!(m));
        }
        obj.insert("requests".into(), Value::Object(req));
    }
    if has_limit {
        let mut lim = Map::new();
        if let Some(c) = &r.cpu_limit {
            lim.insert("cpu".into(), json!(c));
        }
        if let Some(m) = &r.memory_limit {
            lim.insert("memory".into(), json!(m));
        }
        if let Some(g) = r.gpu {
            lim.insert("nvidia.com/gpu".into(), json!(g));
        }
        for (k, v) in &r.extra {
            lim.insert(k.clone(), json!(v));
        }
        obj.insert("limits".into(), Value::Object(lim));
    }
    Some(Value::Object(obj))
}

fn compile_mounts(mounts: &[Mount]) -> Vec<Value> {
    mounts
        .iter()
        .map(|m| {
            let mut obj = Map::new();
            obj.insert("name".into(), json!(m.volume));
            obj.insert("mountPath".into(), json!(m.path));
            if let Some(sp) = &m.sub_path {
                obj.insert("subPath".into(), json!(sp));
            }
            if m.read_only {
                obj.insert("readOnly".into(), json!(true));
            }
            Value::Object(obj)
        })
        .collect()
}

fn compile_probe(kind: &ProbeKind, probe: &super::types::Probe) -> Value {
    let mut probe_obj = Map::new();
    match kind {
        ProbeKind::Http {
            path,
            port,
            http_headers,
        } => {
            let mut http = Map::new();
            http.insert("path".into(), json!(path));
            http.insert("port".into(), json!(port));
            if !http_headers.is_empty() {
                http.insert("httpHeaders".into(), json!(http_headers));
            }
            probe_obj.insert("httpGet".into(), Value::Object(http));
        }
        ProbeKind::Tcp { port } => {
            probe_obj.insert("tcpSocket".into(), json!({ "port": port }));
        }
        ProbeKind::Exec { command } => {
            probe_obj.insert("exec".into(), json!({ "command": command }));
        }
    }
    probe_obj.insert("initialDelaySeconds".into(), json!(probe.initial_delay_seconds));
    probe_obj.insert("periodSeconds".into(), json!(probe.period_seconds));
    probe_obj.insert("timeoutSeconds".into(), json!(probe.timeout_seconds));
    probe_obj.insert("successThreshold".into(), json!(probe.success_threshold));
    probe_obj.insert("failureThreshold".into(), json!(probe.failure_threshold));
    Value::Object(probe_obj)
}

fn compile_pod_volumes(
    unit: &Unit,
    spec: &InfraSpec,
    ctx: &CompileContext<'_>,
) -> Vec<Value> {
    // Find every Volume referenced by any container in this Unit.
    let mut referenced: std::collections::BTreeSet<String> = Default::default();
    for c in unit.containers.iter().chain(unit.init_containers.iter()) {
        for m in &c.mounts {
            referenced.insert(m.volume.clone());
        }
    }
    spec.volumes
        .iter()
        .filter(|v| referenced.contains(&v.name))
        .map(|v| {
            let source = match &v.kind {
                VolumeKind::Persistent { .. } => json!({
                    "persistentVolumeClaim": { "claimName": pvc_name(v, ctx) }
                }),
                VolumeKind::EmptyDir { size_limit } => {
                    let mut ed = Map::new();
                    if let Some(s) = size_limit {
                        ed.insert("sizeLimit".into(), json!(s));
                    }
                    json!({ "emptyDir": Value::Object(ed) })
                }
                VolumeKind::ConfigMap { name, items } => {
                    let mut cm = Map::new();
                    cm.insert("name".into(), json!(name));
                    if let Some(items) = items {
                        let entries: Vec<Value> = items
                            .iter()
                            .map(|(k, p)| json!({ "key": k, "path": p }))
                            .collect();
                        cm.insert("items".into(), json!(entries));
                    }
                    json!({ "configMap": Value::Object(cm) })
                }
                VolumeKind::Secret { name, items } => {
                    let mut s = Map::new();
                    s.insert("secretName".into(), json!(name));
                    if let Some(items) = items {
                        let entries: Vec<Value> = items
                            .iter()
                            .map(|(k, p)| json!({ "key": k, "path": p }))
                            .collect();
                        s.insert("items".into(), json!(entries));
                    }
                    json!({ "secret": Value::Object(s) })
                }
            };
            let mut obj = Map::new();
            obj.insert("name".into(), json!(v.name));
            if let Some(map) = source.as_object() {
                for (k, v) in map {
                    obj.insert(k.clone(), v.clone());
                }
            }
            Value::Object(obj)
        })
        .collect()
}

// -----------------------------------------------------------------
// PVCs
// -----------------------------------------------------------------

fn compile_volume(v: &Volume, ctx: &CompileContext<'_>) -> Option<Value> {
    let VolumeKind::Persistent {
        size,
        storage_class,
        access_modes,
    } = &v.kind
    else {
        // EmptyDir / ConfigMap / Secret have no PVC; they're rendered
        // inline in the Pod spec by `compile_pod_volumes`.
        return None;
    };
    let modes: Vec<&'static str> = access_modes
        .iter()
        .map(|a| match a {
            AccessMode::ReadWriteOnce => "ReadWriteOnce",
            AccessMode::ReadWriteMany => "ReadWriteMany",
            AccessMode::ReadOnlyMany => "ReadOnlyMany",
            AccessMode::ReadWriteOncePod => "ReadWriteOncePod",
        })
        .collect();
    let mut spec = Map::new();
    spec.insert("accessModes".into(), json!(modes));
    spec.insert(
        "resources".into(),
        json!({ "requests": { "storage": size } }),
    );
    if let Some(sc) = storage_class {
        spec.insert("storageClassName".into(), json!(sc));
    }
    Some(json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": pvc_name(v, ctx),
            "namespace": ctx.namespace,
        },
        "spec": spec,
    }))
}

fn pvc_name(v: &Volume, ctx: &CompileContext<'_>) -> String {
    format!("{}-{}", ctx.instance_id, v.name)
}

// -----------------------------------------------------------------
// ConfigMap / Secret literals
// -----------------------------------------------------------------

fn compile_config(cfg: &ConfigSource, ctx: &CompileContext<'_>) -> Option<Value> {
    match cfg {
        ConfigSource::SecretLiteral { name, data } => Some(json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": { "name": name, "namespace": ctx.namespace },
            "stringData": data,
        })),
        ConfigSource::ConfigMapLiteral { name, data } => Some(json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": { "name": name, "namespace": ctx.namespace },
            "data": data,
        })),
        // Refs don't emit anything; they assume the named object
        // already exists in the project namespace.
        ConfigSource::SecretRef { .. } | ConfigSource::ConfigMapRef { .. } => None,
    }
}

// -----------------------------------------------------------------
// Services + Ingress
// -----------------------------------------------------------------

fn compile_service(ep: &super::types::Endpoint, spec: &InfraSpec, ctx: &CompileContext<'_>) -> Value {
    let svc_name = service_name(ep, ctx);
    // Resolve the container port number from the Unit + Container +
    // port-name reference. `validate_endpoint` already checked it.
    let (port_number, protocol) = resolve_endpoint_port(spec, ep)
        .expect("endpoint validated by compile() entry path");
    let proto_str = match protocol {
        Protocol::Tcp => "TCP",
        Protocol::Udp => "UDP",
        Protocol::Sctp => "SCTP",
    };
    let service_type = match &ep.expose {
        Expose::ClusterInternal | Expose::TenantPublic { .. } => "ClusterIP",
        Expose::NodePort { .. } => "NodePort",
    };
    let mut port_obj = Map::new();
    port_obj.insert("name".into(), json!(ep.name));
    port_obj.insert("port".into(), json!(port_number));
    port_obj.insert("targetPort".into(), json!(port_number));
    port_obj.insert("protocol".into(), json!(proto_str));
    if let Expose::NodePort { port } = ep.expose {
        port_obj.insert("nodePort".into(), json!(port));
    }
    json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": svc_name,
            "namespace": ctx.namespace,
        },
        "spec": {
            "type": service_type,
            "selector": {
                "weft.dev/instance": ctx.instance_id,
                "weft.dev/unit": ep.unit,
            },
            "ports": [Value::Object(port_obj)],
        },
    })
}

fn compile_ingress(ep: &super::types::Endpoint, path: &str, ctx: &CompileContext<'_>) -> Value {
    let svc_name = service_name(ep, ctx);
    // The actual host comes from the tenant's ingress config; we
    // emit a path-only Ingress with the right service backend. The
    // dispatcher's ingress-controller config layer can rewrite host
    // matching as needed.
    json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": {
            "name": format!("{}-{}", ctx.instance_id, ep.name),
            "namespace": ctx.namespace,
        },
        "spec": {
            "rules": [{
                "http": {
                    "paths": [{
                        "path": path,
                        "pathType": "Prefix",
                        "backend": {
                            "service": {
                                "name": svc_name,
                                "port": { "name": ep.name }
                            }
                        }
                    }]
                }
            }]
        }
    })
}

fn service_name(ep: &super::types::Endpoint, ctx: &CompileContext<'_>) -> String {
    format!("{}-{}", ctx.instance_id, ep.name)
}

/// Resolve an endpoint to its (port-number, protocol). Returns None
/// when the spec is invalid; `validate_endpoint` catches this before
/// `compile_service` so the .expect() is safe.
fn resolve_endpoint_port(spec: &InfraSpec, ep: &super::types::Endpoint) -> Option<(u16, Protocol)> {
    let unit = spec.units.iter().find(|u| u.name == ep.unit)?;
    let container = unit.containers.iter().find(|c| c.name == ep.container)?;
    container
        .ports
        .iter()
        .find(|p| p.name == ep.port)
        .map(|p| (p.port, p.protocol))
}

fn validate_endpoint(
    spec: &InfraSpec,
    ep: &super::types::Endpoint,
    ctx: &CompileContext<'_>,
) -> Result<(), CompileError> {
    let unit = spec
        .units
        .iter()
        .find(|u| u.name == ep.unit)
        .ok_or_else(|| CompileError::EndpointUnitMissing {
            node: ctx.node_id.to_string(),
            endpoint: ep.name.clone(),
            unit: ep.unit.clone(),
        })?;
    let container = unit
        .containers
        .iter()
        .find(|c| c.name == ep.container)
        .ok_or_else(|| CompileError::EndpointContainerMissing {
            node: ctx.node_id.to_string(),
            endpoint: ep.name.clone(),
            unit: ep.unit.clone(),
            container: ep.container.clone(),
        })?;
    container
        .ports
        .iter()
        .find(|p| p.name == ep.port)
        .ok_or_else(|| CompileError::EndpointPortMissing {
            node: ctx.node_id.to_string(),
            endpoint: ep.name.clone(),
            unit: ep.unit.clone(),
            container: ep.container.clone(),
            port: ep.port.clone(),
        })?;
    Ok(())
}

// -----------------------------------------------------------------
// HPA
// -----------------------------------------------------------------

/// Single source of truth: does this Unit emit a HorizontalPodAutoscaler?
/// Called both by `compile_hpa` (decides whether to emit the
/// manifest) and by `emitted_names` (decides whether to pre-flight
/// the `<unit>-hpa` name). Keeping them in sync via one function
/// means pre-flight and emit can never drift.
fn should_emit_hpa(unit: &Unit) -> bool {
    unit.scaling.autoscale.is_some()
        && matches!(unit.kind, UnitKind::Deployment | UnitKind::StatefulSet)
}

fn compile_hpa(unit: &Unit, ctx: &CompileContext<'_>) -> Option<Value> {
    if !should_emit_hpa(unit) {
        return None;
    }
    let auto = unit
        .scaling
        .autoscale
        .as_ref()
        .expect("should_emit_hpa checks autoscale");
    let target_kind = match unit.kind {
        UnitKind::Deployment => "Deployment",
        UnitKind::StatefulSet => "StatefulSet",
        // should_emit_hpa rejects these; unreachable.
        UnitKind::DaemonSet | UnitKind::Job => unreachable!(
            "should_emit_hpa returned true for kind {:?}",
            unit.kind
        ),
    };
    let metrics: Vec<Value> = auto
        .metrics
        .iter()
        .map(|m| match m {
            AutoscaleMetric::CpuUtilization { target_percent } => json!({
                "type": "Resource",
                "resource": {
                    "name": "cpu",
                    "target": {
                        "type": "Utilization",
                        "averageUtilization": target_percent,
                    }
                }
            }),
            AutoscaleMetric::MemoryUtilization { target_percent } => json!({
                "type": "Resource",
                "resource": {
                    "name": "memory",
                    "target": {
                        "type": "Utilization",
                        "averageUtilization": target_percent,
                    }
                }
            }),
            AutoscaleMetric::Custom { name, target } => json!({
                "type": "Pods",
                "pods": {
                    "metric": { "name": name },
                    "target": { "type": "AverageValue", "averageValue": target }
                }
            }),
        })
        .collect();
    let mut spec = Map::new();
    spec.insert(
        "scaleTargetRef".into(),
        json!({
            "apiVersion": "apps/v1",
            "kind": target_kind,
            "name": unit_name(unit, ctx),
        }),
    );
    spec.insert("minReplicas".into(), json!(auto.min_replicas));
    spec.insert("maxReplicas".into(), json!(auto.max_replicas));
    spec.insert("metrics".into(), json!(metrics));
    if let Some(b) = &auto.behavior {
        spec.insert(
            "behavior".into(),
            json!({
                "scaleUp": {
                    "stabilizationWindowSeconds": b.scale_up_stabilization_seconds
                },
                "scaleDown": {
                    "stabilizationWindowSeconds": b.scale_down_stabilization_seconds
                }
            }),
        );
    }
    Some(json!({
        "apiVersion": "autoscaling/v2",
        "kind": "HorizontalPodAutoscaler",
        "metadata": {
            "name": format!("{}-hpa", unit_name(unit, ctx)),
            "namespace": ctx.namespace,
        },
        "spec": spec,
    }))
}

// -----------------------------------------------------------------
// NetworkPolicy
// -----------------------------------------------------------------

fn compile_network_policy(spec: &InfraSpec, ctx: &CompileContext<'_>) -> Value {
    let mut ingress_rules = Vec::new();
    for rule in &spec.access.ingress {
        ingress_rules.push(match rule {
            IngressRule::FromWorkers => json!({
                "from": [{
                    "podSelector": {
                        "matchLabels": { "weft.dev/role": "worker" }
                    }
                }]
            }),
            IngressRule::FromNode { node_id } => json!({
                "from": [{
                    "podSelector": {
                        "matchLabels": { "weft.dev/node": node_id }
                    }
                }]
            }),
            IngressRule::FromInternet => json!({
                "from": [{
                    "ipBlock": { "cidr": "0.0.0.0/0" }
                }]
            }),
            IngressRule::FromCidrs(cidrs) => json!({
                "from": cidrs.iter().map(|c| json!({
                    "ipBlock": { "cidr": c }
                })).collect::<Vec<_>>()
            }),
            IngressRule::FromLabel { key, value } => json!({
                "from": [{
                    "podSelector": {
                        "matchLabels": { key: value }
                    }
                }]
            }),
        });
    }

    let mut egress_rules = Vec::new();
    for rule in &spec.access.egress {
        egress_rules.push(match rule {
            EgressRule::ToInternet => json!({
                "to": [{ "ipBlock": { "cidr": "0.0.0.0/0" } }]
            }),
            EgressRule::ToNode { node_id } => json!({
                "to": [{
                    "podSelector": {
                        "matchLabels": { "weft.dev/node": node_id }
                    }
                }]
            }),
            EgressRule::ToCidrs(cidrs) => json!({
                "to": cidrs.iter().map(|c| json!({
                    "ipBlock": { "cidr": c }
                })).collect::<Vec<_>>()
            }),
        });
    }

    json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {
            "name": format!("{}-access", ctx.instance_id),
            "namespace": ctx.namespace,
        },
        "spec": {
            "podSelector": {
                "matchLabels": { "weft.dev/instance": ctx.instance_id }
            },
            "policyTypes": ["Ingress", "Egress"],
            "ingress": ingress_rules,
            "egress": egress_rules,
        }
    })
}

// -----------------------------------------------------------------
// Naming + labels
// -----------------------------------------------------------------

fn unit_name(unit: &Unit, ctx: &CompileContext<'_>) -> String {
    // Single-unit nodes get the bare instance id (cleaner names);
    // multi-unit nodes append the unit name.
    if unit.name.is_empty() {
        ctx.instance_id.to_string()
    } else {
        format!("{}-{}", ctx.instance_id, unit.name)
    }
}

/// k8s resource kinds that own a name space in apiserver. Two
/// objects with the same `metadata.name` collide ONLY if they are
/// the same kind in the same namespace. The pre-flight dedup keys
/// by `(K8sKind, name)` so a Deployment "foo" and a ConfigMap
/// "foo" don't trigger a false-positive `DuplicateName`.
///
/// Each variant maps to one k8s `kind` string. The four workload
/// kinds (Deployment / StatefulSet / DaemonSet / Job) are split
/// because k8s treats them as distinct name-spaces: a Deployment
/// "foo" and a StatefulSet "foo" coexist legally. Collapsing them
/// into one variant would over-reject.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum K8sKind {
    Deployment,
    StatefulSet,
    DaemonSet,
    Job,
    HorizontalPodAutoscaler,
    PersistentVolumeClaim,
    Service,
    NetworkPolicy,
    Secret,
    ConfigMap,
}

impl K8sKind {
    fn display(self) -> &'static str {
        match self {
            Self::Deployment => "Deployment",
            Self::StatefulSet => "StatefulSet",
            Self::DaemonSet => "DaemonSet",
            Self::Job => "Job",
            Self::HorizontalPodAutoscaler => "HorizontalPodAutoscaler",
            Self::PersistentVolumeClaim => "PersistentVolumeClaim",
            Self::Service => "Service",
            Self::NetworkPolicy => "NetworkPolicy",
            Self::Secret => "Secret",
            Self::ConfigMap => "ConfigMap",
        }
    }

    fn from_unit_kind(kind: super::types::UnitKind) -> Self {
        use super::types::UnitKind;
        match kind {
            UnitKind::Deployment => Self::Deployment,
            UnitKind::StatefulSet => Self::StatefulSet,
            UnitKind::DaemonSet => Self::DaemonSet,
            UnitKind::Job => Self::Job,
        }
    }
}

/// One pre-flight entry: the k8s name + kind compile() will stamp
/// plus a `details` string for error messages. `source_kind` is
/// the user-facing field name (unit / volume / endpoint / etc.)
/// the user would shorten or rename to fix a length / char / dedup
/// violation.
struct EmittedName {
    name: String,
    kind: K8sKind,
    source_kind: &'static str,
    details: String,
}

/// Enumerate every k8s `metadata.name` `compile()` will stamp.
/// Used by the preflight length / char / dedup check. If
/// `compile.rs` grows a new resource kind, add it here too.
fn emitted_names<'a>(spec: &'a InfraSpec, ctx: &'a CompileContext<'a>) -> Vec<EmittedName> {
    let mut out: Vec<EmittedName> = Vec::new();

    for unit in &spec.units {
        let u = unit_name(unit, ctx);
        out.push(EmittedName {
            name: u.clone(),
            kind: K8sKind::from_unit_kind(unit.kind),
            source_kind: "unit",
            details: format!("instance_id '{}' + unit '{}'", ctx.instance_id, unit.name),
        });
        // Mirror `compile_hpa` exactly via the shared
        // `should_emit_hpa` predicate. Pre-flight and emit can't
        // drift: if the predicate ever changes (e.g. allow HPA on
        // DaemonSet someday), both sites see it together.
        if should_emit_hpa(unit) {
            out.push(EmittedName {
                name: format!("{u}-hpa"),
                kind: K8sKind::HorizontalPodAutoscaler,
                source_kind: "unit",
                details: format!(
                    "instance_id '{}' + unit '{}' (with k8s '-hpa' suffix)",
                    ctx.instance_id, unit.name
                ),
            });
        }
    }

    for vol in &spec.volumes {
        if matches!(vol.kind, VolumeKind::Persistent { .. }) {
            out.push(EmittedName {
                name: format!("{}-{}", ctx.instance_id, vol.name),
                kind: K8sKind::PersistentVolumeClaim,
                source_kind: "volume",
                details: format!("instance_id '{}' + volume '{}'", ctx.instance_id, vol.name),
            });
        }
    }

    for ep in &spec.endpoints {
        // Service + (optional) Ingress share the same name. k8s
        // treats them as distinct kinds (no collision); we only
        // emit one EmittedName under K8sKind::Service because the
        // length/char checks are identical and the user-facing
        // "shorten the endpoint name" guidance is the same.
        out.push(EmittedName {
            name: format!("{}-{}", ctx.instance_id, ep.name),
            kind: K8sKind::Service,
            source_kind: "endpoint",
            details: format!("instance_id '{}' + endpoint '{}'", ctx.instance_id, ep.name),
        });
    }

    out.push(EmittedName {
        name: format!("{}-access", ctx.instance_id),
        kind: K8sKind::NetworkPolicy,
        source_kind: "instance_id",
        details: format!(
            "instance_id '{}' (with k8s '-access' suffix for NetworkPolicy)",
            ctx.instance_id
        ),
    });

    for cfg in &spec.config {
        let (name, kind, display) = match cfg {
            ConfigSource::SecretLiteral { name, .. } => (name, K8sKind::Secret, "secret literal"),
            ConfigSource::ConfigMapLiteral { name, .. } => {
                (name, K8sKind::ConfigMap, "config-map literal")
            }
            ConfigSource::SecretRef { .. } | ConfigSource::ConfigMapRef { .. } => continue,
        };
        out.push(EmittedName {
            name: name.clone(),
            kind,
            source_kind: "config",
            details: format!("{display} '{name}'"),
        });
    }

    out
}

/// Stamp weft.dev/* labels on a manifest emitted by one of the
/// typed `compile_*` helpers. Asserts namespace is already present
/// (every emitter sets it at construction time).
fn stamp_weft_labels(manifest: &mut Value, ctx: &CompileContext<'_>) {
    let Some(md) = manifest_metadata_mut(manifest) else {
        return;
    };
    // Schema-drift guard: a new compile_* helper that forgets
    // `metadata.namespace` fails here loudly in debug builds.
    debug_assert!(
        md.contains_key("namespace"),
        "compile.rs emitted a manifest without metadata.namespace; \
         every compile_* helper must include it explicitly"
    );
    stamp_labels_into(md, ctx);
}

fn manifest_metadata_mut(manifest: &mut Value) -> Option<&mut serde_json::Map<String, Value>> {
    let obj = manifest.as_object_mut()?;
    let metadata = obj
        .entry("metadata".to_string())
        .or_insert_with(|| json!({}));
    metadata.as_object_mut()
}

fn stamp_labels_into(md: &mut serde_json::Map<String, Value>, ctx: &CompileContext<'_>) {
    let labels = md.entry("labels".to_string()).or_insert_with(|| json!({}));
    let Some(lbls) = labels.as_object_mut() else {
        return;
    };
    lbls.insert("weft.dev/role".into(), json!("infra"));
    lbls.insert("weft.dev/tenant".into(), json!(ctx.tenant_id));
    lbls.insert("weft.dev/project".into(), json!(ctx.project_id));
    lbls.insert("weft.dev/node".into(), json!(ctx.node_id));
    lbls.insert("weft.dev/instance".into(), json!(ctx.instance_id));
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::types::*;

    fn ctx() -> CompileContext<'static> {
        // We need 'static lifetimes for the simple test; we leak the
        use std::collections::BTreeMap;
        use std::sync::OnceLock;
        static EMPTY: OnceLock<BTreeMap<String, String>> = OnceLock::new();
        let empty = EMPTY.get_or_init(BTreeMap::new);
        CompileContext {
            tenant_id: "tenantA",
            project_id: "projB",
            node_id: "nodeC",
            instance_id: "inst1",
            namespace: "wft-project-tenantA-projB",
            local_image_tags: empty,
        }
    }

    fn ctx_with_tags(
        tags: std::collections::BTreeMap<String, String>,
    ) -> CompileContext<'static> {
        let leaked: &'static std::collections::BTreeMap<String, String> =
            Box::leak(Box::new(tags));
        CompileContext {
            tenant_id: "tenantA",
            project_id: "projB",
            node_id: "nodeC",
            instance_id: "inst1",
            namespace: "wft-project-tenantA-projB",
            local_image_tags: leaked,
        }
    }

    #[test]
    fn empty_spec_compiles_to_networkpolicy_only() {
        let spec = InfraSpec::default();
        let out = compile(&spec, &ctx()).expect("compile ok");
        assert_eq!(out.len(), 1, "only the per-node NetworkPolicy");
        assert_eq!(out[0]["kind"], "NetworkPolicy");
    }

    #[test]
    fn single_container_node_compiles() {
        let spec = InfraSpec {
            units: vec![Unit {
                name: "bridge".into(),
                kind: UnitKind::Deployment,
                containers: vec![Container {
                    ports: vec![ContainerPort {
                        name: "http".into(),
                        port: 80,
                        protocol: Protocol::Tcp,
                    }],
                    ..Container::new(
                        "main",
                        Image::Upstream {
                            reference: "nginx:1.27".into(),
                        },
                    )
                }],
                ..Default::default()
            }],
            endpoints: vec![Endpoint {
                name: "api".into(),
                unit: "bridge".into(),
                container: "main".into(),
                port: "http".into(),
                expose: Expose::ClusterInternal,
            }],
            ..Default::default()
        };
        let out = compile(&spec, &ctx()).expect("compile ok");
        // Deployment + Service + NetworkPolicy.
        assert_eq!(out.len(), 3);
        let kinds: Vec<&str> = out
            .iter()
            .map(|m| m["kind"].as_str().unwrap_or(""))
            .collect();
        assert!(kinds.contains(&"Deployment"));
        assert!(kinds.contains(&"Service"));
        assert!(kinds.contains(&"NetworkPolicy"));
    }

    #[test]
    fn local_image_resolves_through_tag_map() {
        let mut tags = std::collections::BTreeMap::new();
        tags.insert("bridge".to_string(), "weft-infra-bridge:abc123".to_string());
        let spec = InfraSpec {
            units: vec![Unit {
                name: "u".into(),
                kind: UnitKind::Deployment,
                containers: vec![Container::new(
                    "c",
                    Image::Local { name: "bridge".into() },
                )],
                ..Default::default()
            }],
            ..Default::default()
        };
        let out = compile(&spec, &ctx_with_tags(tags)).expect("compile ok");
        let deploy = out
            .iter()
            .find(|m| m["kind"] == "Deployment")
            .expect("Deployment present");
        let image = &deploy["spec"]["template"]["spec"]["containers"][0]["image"];
        assert_eq!(image, "weft-infra-bridge:abc123");
    }

    #[test]
    fn missing_local_image_errors() {
        let spec = InfraSpec {
            units: vec![Unit {
                name: "u".into(),
                kind: UnitKind::Deployment,
                containers: vec![Container::new(
                    "c",
                    Image::Local { name: "missing".into() },
                )],
                ..Default::default()
            }],
            ..Default::default()
        };
        let err = compile(&spec, &ctx()).unwrap_err();
        assert!(matches!(err, CompileError::MissingLocalImage { .. }));
    }

    #[test]
    fn pvc_emitted_for_persistent_volume() {
        let spec = InfraSpec {
            units: vec![Unit {
                name: "u".into(),
                kind: UnitKind::Deployment,
                containers: vec![Container {
                    mounts: vec![Mount {
                        volume: "data".into(),
                        path: "/data".into(),
                        ..Default::default()
                    }],
                    ..Container::new("c", Image::Upstream { reference: "x:1".into() })
                }],
                ..Default::default()
            }],
            volumes: vec![Volume {
                name: "data".into(),
                kind: VolumeKind::Persistent {
                    size: "1Gi".into(),
                    storage_class: None,
                    access_modes: vec![AccessMode::ReadWriteOnce],
                },
            }],
            ..Default::default()
        };
        let out = compile(&spec, &ctx()).expect("compile ok");
        let pvc = out
            .iter()
            .find(|m| m["kind"] == "PersistentVolumeClaim")
            .expect("PVC present");
        assert_eq!(pvc["metadata"]["name"], "inst1-data");
    }

    #[test]
    fn labels_stamped_on_every_manifest() {
        let spec = InfraSpec {
            units: vec![Unit {
                name: "u".into(),
                kind: UnitKind::Deployment,
                containers: vec![Container::new(
                    "c",
                    Image::Upstream { reference: "x:1".into() },
                )],
                ..Default::default()
            }],
            ..Default::default()
        };
        let out = compile(&spec, &ctx()).expect("compile ok");
        for m in &out {
            let labels = &m["metadata"]["labels"];
            assert_eq!(labels["weft.dev/instance"], "inst1");
            assert_eq!(labels["weft.dev/project"], "projB");
            assert_eq!(labels["weft.dev/tenant"], "tenantA");
            assert_eq!(labels["weft.dev/node"], "nodeC");
        }
    }


    #[test]
    fn config_literal_names_get_length_checked() {
        // The preflight loop must include user-provided
        // ConfigMap / Secret names; otherwise an over-long name
        // slips through compile and kubectl rejects far downstream.
        let too_long = "x".repeat(64);
        let spec = InfraSpec {
            config: vec![ConfigSource::ConfigMapLiteral {
                name: too_long.clone(),
                data: Default::default(),
            }],
            ..Default::default()
        };
        let err = compile(&spec, &ctx()).unwrap_err();
        assert!(
            matches!(err, CompileError::NameTooLong { .. }),
            "expected NameTooLong, got {err:?}",
        );
    }

    #[test]
    fn config_ref_names_skip_length_check() {
        // SecretRef / ConfigMapRef reference pre-existing resources;
        // their names are NOT stamped onto any manifest we emit, so
        // they're not length-relevant on the preflight side.
        let too_long = "x".repeat(64);
        let spec = InfraSpec {
            config: vec![ConfigSource::SecretRef {
                name: too_long,
            }],
            ..Default::default()
        };
        // No NameTooLong error: we let kubectl resolve the ref at
        // apply time.
        assert!(compile(&spec, &ctx()).is_ok());
    }

    /// Two Units with empty names both collapse to the bare
    /// `instance_id` via `unit_name`. The dedup pass must catch
    /// this as `DuplicateName` (k8s would fail at apply since
    /// both are workloads of the same kind).
    #[test]
    fn duplicate_unit_names_rejected() {
        let spec = InfraSpec {
            units: vec![
                Unit {
                    name: String::new(),
                    kind: UnitKind::Deployment,
                    ..Default::default()
                },
                Unit {
                    name: String::new(),
                    kind: UnitKind::Deployment,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let err = compile(&spec, &ctx()).expect_err("expected DuplicateName");
        assert!(
            matches!(err, CompileError::DuplicateName { .. }),
            "got {err:?}"
        );
    }

    /// A Unit and a ConfigMapLiteral with the SAME name don't
    /// collide in k8s (different kinds), and the pre-flight must
    /// not reject this as `DuplicateName`. Round-7 fix: the
    /// HashSet is keyed by `(K8sKind, name)`, not by `name` alone.
    #[test]
    fn same_name_different_kind_allowed() {
        let spec = InfraSpec {
            units: vec![Unit {
                name: "creds".into(),
                kind: UnitKind::Deployment,
                containers: vec![Container::new("c", Image::Upstream { reference: "nginx:1".into() })],
                ..Default::default()
            }],
            config: vec![ConfigSource::ConfigMapLiteral {
                name: "creds".into(),
                data: Default::default(),
            }],
            ..Default::default()
        };
        // Compile must succeed even with the same name reused
        // across distinct k8s kinds.
        compile(&spec, &ctx()).expect("Deployment 'creds' + ConfigMap 'creds' is legal in k8s");
    }

    /// Invalid characters (uppercase, underscore) in a unit name
    /// surface as `NameInvalid`, not as a kubectl apply failure.
    #[test]
    fn name_invalid_uppercase_underscore() {
        let spec = InfraSpec {
            units: vec![Unit {
                name: "My_Unit".into(),
                kind: UnitKind::Deployment,
                containers: vec![Container::new("c", Image::Upstream { reference: "nginx:1".into() })],
                ..Default::default()
            }],
            ..Default::default()
        };
        let err = compile(&spec, &ctx()).expect_err("expected NameInvalid");
        assert!(matches!(err, CompileError::NameInvalid { .. }), "got {err:?}");
    }

    /// `compile_config` stamps `metadata.namespace` at construction
    /// time. Round-6 reshape: namespace lives on the emitter, not
    /// in `stamp_weft_labels`' silent backfill.
    #[test]
    fn config_literal_carries_namespace() {
        let mut values = std::collections::BTreeMap::new();
        values.insert("key".to_string(), "value".to_string());
        let spec = InfraSpec {
            config: vec![ConfigSource::ConfigMapLiteral {
                name: "settings".into(),
                data: values,
            }],
            ..Default::default()
        };
        let out = compile(&spec, &ctx()).expect("compile ok");
        let cm = out
            .iter()
            .find(|m| m["kind"] == "ConfigMap")
            .expect("ConfigMap emitted");
        assert_eq!(cm["metadata"]["namespace"], "wft-project-tenantA-projB");
    }

    /// Per-Unit replicas: a 2-Unit spec with different replica
    /// counts must compile each Unit's count onto its own
    /// workload. Round-6 reshape: ScalingPolicy moved from spec to
    /// Unit, so this property is now possible to express.
    #[test]
    fn per_unit_replicas_compile_independently() {
        let spec = InfraSpec {
            units: vec![
                Unit {
                    name: "primary".into(),
                    kind: UnitKind::Deployment,
                    containers: vec![Container::new("c", Image::Upstream { reference: "nginx:1".into() })],
                    scaling: ScalingPolicy {
                        replicas: 1,
                        autoscale: None,
                    },
                    ..Default::default()
                },
                Unit {
                    name: "sentinel".into(),
                    kind: UnitKind::Deployment,
                    containers: vec![Container::new("c", Image::Upstream { reference: "nginx:1".into() })],
                    scaling: ScalingPolicy {
                        replicas: 3,
                        autoscale: None,
                    },
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let out = compile(&spec, &ctx()).expect("compile ok");
        let primary = out
            .iter()
            .find(|m| m["metadata"]["name"] == "inst1-primary")
            .expect("primary emitted");
        let sentinel = out
            .iter()
            .find(|m| m["metadata"]["name"] == "inst1-sentinel")
            .expect("sentinel emitted");
        assert_eq!(primary["spec"]["replicas"], 1);
        assert_eq!(sentinel["spec"]["replicas"], 3);
    }

    fn unit_named(name: &str, kind: UnitKind) -> Unit {
        Unit {
            name: name.into(),
            kind,
            containers: vec![Container::new(
                "c",
                Image::Upstream { reference: "nginx:1".into() },
            )],
            ..Default::default()
        }
    }

    /// An autoscaled Deployment must NOT carry a static
    /// `spec.replicas` (the HPA owns the replica count; emitting
    /// both makes the apply and the autoscaler fight). The HPA is
    /// emitted instead.
    #[test]
    fn autoscaled_workload_omits_static_replicas() {
        let mut unit = unit_named("api", UnitKind::Deployment);
        unit.scaling.autoscale = Some(AutoscaleSpec {
            min_replicas: 2,
            max_replicas: 10,
            metrics: vec![AutoscaleMetric::CpuUtilization { target_percent: 70 }],
            behavior: None,
        });
        let spec = InfraSpec { units: vec![unit], ..Default::default() };
        let out = compile(&spec, &ctx()).expect("compile ok");
        let deploy = out
            .iter()
            .find(|m| m["kind"] == "Deployment")
            .expect("Deployment present");
        assert!(
            deploy["spec"].get("replicas").is_none(),
            "autoscaled Deployment must omit static replicas, got {}",
            deploy["spec"]
        );
        assert!(
            out.iter().any(|m| m["kind"] == "HorizontalPodAutoscaler"),
            "HPA must be emitted for an autoscaled workload"
        );
    }

    /// Autoscale with zero metrics fails loud at compile (an empty
    /// HPA metrics list is rejected by the apiserver at apply).
    #[test]
    fn autoscale_without_metrics_rejected() {
        let mut unit = unit_named("api", UnitKind::Deployment);
        unit.scaling.autoscale = Some(AutoscaleSpec {
            min_replicas: 1,
            max_replicas: 5,
            metrics: vec![],
            behavior: None,
        });
        let spec = InfraSpec { units: vec![unit], ..Default::default() };
        let err = compile(&spec, &ctx()).expect_err("expected AutoscaleWithoutMetrics");
        assert!(
            matches!(err, CompileError::AutoscaleWithoutMetrics { .. }),
            "got {err:?}"
        );
    }

    /// Without autoscale, the static replicas IS emitted.
    #[test]
    fn non_autoscaled_workload_keeps_static_replicas() {
        let mut unit = unit_named("api", UnitKind::Deployment);
        unit.scaling.replicas = 3;
        let spec = InfraSpec { units: vec![unit], ..Default::default() };
        let out = compile(&spec, &ctx()).expect("compile ok");
        let deploy = out.iter().find(|m| m["kind"] == "Deployment").unwrap();
        assert_eq!(deploy["spec"]["replicas"], 3);
        assert!(!out.iter().any(|m| m["kind"] == "HorizontalPodAutoscaler"));
    }

    /// Default-Rolling on a StatefulSet is fine: the user didn't
    /// express an upgrade intent, so there's nothing to silently
    /// drop.
    #[test]
    fn default_on_upgrade_on_statefulset_ok() {
        let spec = InfraSpec {
            units: vec![unit_named("db", UnitKind::StatefulSet)],
            ..Default::default()
        };
        compile(&spec, &ctx()).expect("default Rolling on StatefulSet is legal");
    }

    /// A NON-default on_upgrade on a non-Deployment kind fails loud
    /// rather than being silently ignored (the strategy stamp is
    /// Deployment-only).
    #[test]
    fn recreate_on_statefulset_rejected() {
        let mut unit = unit_named("db", UnitKind::StatefulSet);
        unit.on_upgrade = UpgradeBehavior::Recreate;
        let spec = InfraSpec { units: vec![unit], ..Default::default() };
        let err = compile(&spec, &ctx()).expect_err("expected UpgradeBehaviorOnNonDeployment");
        assert!(
            matches!(err, CompileError::UpgradeBehaviorOnNonDeployment { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn custom_rolling_on_daemonset_rejected() {
        let mut unit = unit_named("agent", UnitKind::DaemonSet);
        unit.on_upgrade = UpgradeBehavior::Rolling {
            max_unavailable: Some("25%".into()),
            max_surge: None,
            progress_deadline_seconds: 600,
        };
        let spec = InfraSpec { units: vec![unit], ..Default::default() };
        let err = compile(&spec, &ctx()).expect_err("expected UpgradeBehaviorOnNonDeployment");
        assert!(
            matches!(err, CompileError::UpgradeBehaviorOnNonDeployment { .. }),
            "got {err:?}"
        );
    }

}
