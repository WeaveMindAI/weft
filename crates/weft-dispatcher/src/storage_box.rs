//! Per-tenant storage-box lifecycle. The dispatcher is the box's
//! control plane and NOTHING else: it provisions (lazily, on first
//! need), renders the k8s bundle (PVCs + Deployment + Service +
//! Ingress + NetworkPolicy), serves the box's grow/shrink disk
//! requests, runs the scale-to-zero reaper and the durable terminate
//! sweep, and brokers the user-download handshake. Bulk bytes NEVER
//! pass through here.
//!
//! State of record is Postgres: `storage_box` (one row per
//! provisioned box), `storage_disk` (one row per backing PVC),
//! `storage_profile` (per-tenant StorageClass + disk unit), and
//! `storage_sweep` (the durable un-kept-exec sweep queue). Pod-local
//! RAM holds nothing a sibling dispatcher Pod would need.

use anyhow::{anyhow, Context, Result};
use sqlx::PgPool;

use crate::project_namespace::SafeLabel;
use crate::state::DispatcherState;
use crate::tenant::TenantId;


pub async fn migrate(pool: &PgPool) -> Result<()> {
    sqlx::raw_sql(
        r#"
        CREATE TABLE IF NOT EXISTS storage_box (
            tenant_id TEXT PRIMARY KEY,
            -- Monotonic source of disk names (disk-0, disk-1, ...).
            -- Never reused within a tenant, so a released PVC's name
            -- doesn't come back and confuse an in-flight drain.
            disk_seq BIGINT NOT NULL DEFAULT 0,
            created_at_unix BIGINT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS storage_disk (
            tenant_id TEXT NOT NULL,
            name TEXT NOT NULL,
            size_bytes BIGINT NOT NULL,
            -- NULL = the cluster's default StorageClass.
            storage_class TEXT,
            PRIMARY KEY (tenant_id, name)
        );
        CREATE TABLE IF NOT EXISTS storage_profile (
            tenant_id TEXT PRIMARY KEY,
            storage_class TEXT,
            disk_unit_bytes BIGINT NOT NULL
        );
        -- Durable terminate-sweep queue: a row per terminated color
        -- whose un-kept exec files still need sweeping. Inserted by
        -- the journal bridge (the durable observer of terminate),
        -- deleted by the sweep reaper once the box confirmed (or no
        -- box exists).
        CREATE TABLE IF NOT EXISTS storage_sweep (
            color TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            enqueued_at_unix BIGINT NOT NULL
        );
        "#,
    )
    .execute(pool)
    .await
    .context("storage_box migrate")?;
    Ok(())
}

// ---------- profile ----------

#[derive(Debug, Clone)]
pub struct StorageProfile {
    pub storage_class: Option<String>,
    pub disk_unit_bytes: i64,
}

impl Default for StorageProfile {
    fn default() -> Self {
        // The authoritative runtime value is whatever the profile row
        // says; this is only the size of one disk for a tenant that has
        // never set a profile. The config const is u64; the row is i64.
        Self {
            storage_class: None,
            disk_unit_bytes: weft_storage::config::DEFAULT_DISK_UNIT_BYTES as i64,
        }
    }
}

pub async fn profile(pool: &PgPool, tenant: &str) -> Result<StorageProfile> {
    let row: Option<(Option<String>, i64)> = sqlx::query_as(
        "SELECT storage_class, disk_unit_bytes FROM storage_profile WHERE tenant_id = $1",
    )
    .bind(tenant)
    .fetch_optional(pool)
    .await?;
    Ok(row
        .map(|(storage_class, disk_unit_bytes)| StorageProfile { storage_class, disk_unit_bytes })
        .unwrap_or_default())
}

/// Set the tenant's profile. Applies to DISKS PROVISIONED FROM NOW
/// ON; existing PVCs keep their class/size (the pool migrates
/// naturally through grow/shrink cycles).
pub async fn set_profile(
    pool: &PgPool,
    tenant: &str,
    storage_class: Option<String>,
    disk_unit_bytes: i64,
) -> Result<()> {
    if disk_unit_bytes <= 0 {
        return Err(anyhow!("disk_unit_bytes must be positive"));
    }
    sqlx::query(
        "INSERT INTO storage_profile (tenant_id, storage_class, disk_unit_bytes) \
         VALUES ($1, $2, $3) \
         ON CONFLICT (tenant_id) DO UPDATE \
         SET storage_class = EXCLUDED.storage_class, \
             disk_unit_bytes = EXCLUDED.disk_unit_bytes",
    )
    .bind(tenant)
    .bind(storage_class)
    .bind(disk_unit_bytes)
    .execute(pool)
    .await?;
    Ok(())
}

// ---------- box rows ----------

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DiskRow {
    pub name: String,
    pub size_bytes: i64,
    pub storage_class: Option<String>,
}

async fn disks(pool: &PgPool, tenant: &str) -> Result<Vec<DiskRow>> {
    Ok(sqlx::query_as(
        "SELECT name, size_bytes, storage_class FROM storage_disk \
         WHERE tenant_id = $1 ORDER BY name",
    )
    .bind(tenant)
    .fetch_all(pool)
    .await?)
}

pub async fn box_exists(pool: &PgPool, tenant: &str) -> Result<bool> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT tenant_id FROM storage_box WHERE tenant_id = $1")
            .bind(tenant)
            .fetch_optional(pool)
            .await?;
    Ok(row.is_some())
}

/// In-cluster URL of a tenant's box.
pub fn box_url(state: &DispatcherState, tenant: &TenantId) -> String {
    let ns = state.namespace_mapper.namespace_for(tenant);
    let port = weft_storage::config::STORAGE_PORT;
    format!("http://weft-storage.{ns}.svc.cluster.local:{port}")
}

/// Public base URL of a tenant's box: the SAME external host the
/// dispatcher is reachable on, with the tenant's storage path. The
/// tenant-namespace Ingress claims this path, so presigned/capability
/// downloads route from the ingress controller STRAIGHT to the box
/// (the dispatcher's catch-all Ingress only sees what no more
/// specific path claims).
pub fn public_base_url(state: &DispatcherState, tenant: &SafeLabel) -> String {
    format!("{}/storage/{tenant}", state.public_base_url.trim_end_matches('/'))
}

// ---------- ensure / provision ----------

/// Ensure the tenant's box exists and its manifests are applied.
/// Lazy: the first storage use provisions one fresh disk + the pod;
/// later calls re-apply idempotently (kubectl apply reconciles).
/// Returns the box's in-cluster URL.
pub async fn ensure_box(state: &DispatcherState, tenant: &TenantId) -> Result<String> {
    let pool = &state.pg_pool;
    let now = crate::lease::now_unix();
    let prof = profile(pool, tenant.as_str()).await?;
    // Box row + first disk row are created in ONE transaction so a
    // concurrent ensure_box either sees the box AND its disk together
    // or neither: the loser's `INSERT ... ON CONFLICT DO NOTHING`
    // blocks on the winner's uncommitted box row, and once the winner
    // commits, the loser's `apply_box` reads the committed disk row
    // (never an empty-disk-list window). The first INSERT is the
    // serialization point (one provision per tenant).
    let mut tx = pool.begin().await?;
    let inserted = sqlx::query(
        "INSERT INTO storage_box (tenant_id, created_at_unix) VALUES ($1, $2) \
         ON CONFLICT (tenant_id) DO NOTHING",
    )
    .bind(tenant.as_str())
    .bind(now)
    .execute(&mut *tx)
    .await?
    .rows_affected()
        > 0;
    if inserted {
        add_disk_row_tx(&mut tx, tenant.as_str(), &prof).await?;
    }
    tx.commit().await?;
    apply_box(state, tenant).await?;
    Ok(box_url(state, tenant))
}

/// Allocate the next disk name + row per the tenant's profile, inside
/// `tx`. The `disk_seq` bump + the disk INSERT are one unit so the
/// name is unique even under concurrent grows.
async fn add_disk_row_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant: &str,
    prof: &StorageProfile,
) -> Result<String> {
    let seq: i64 = sqlx::query_scalar(
        "UPDATE storage_box SET disk_seq = disk_seq + 1 WHERE tenant_id = $1 \
         RETURNING disk_seq",
    )
    .bind(tenant)
    .fetch_one(&mut **tx)
    .await?;
    let name = format!("disk-{}", seq - 1);
    sqlx::query(
        "INSERT INTO storage_disk (tenant_id, name, size_bytes, storage_class) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(tenant)
    .bind(&name)
    .bind(prof.disk_unit_bytes)
    .bind(&prof.storage_class)
    .execute(&mut **tx)
    .await?;
    Ok(name)
}

/// Allocate one more disk (used by grow), in its own transaction.
async fn add_disk_row(pool: &PgPool, tenant: &str, prof: &StorageProfile) -> Result<String> {
    let mut tx = pool.begin().await?;
    let name = add_disk_row_tx(&mut tx, tenant, prof).await?;
    tx.commit().await?;
    Ok(name)
}

/// Render + apply the tenant's full box bundle from the CURRENT disk
/// rows. Adding/removing a disk re-applies the Deployment, which
/// restarts the single pod with the new mount set (PVCs are RWO;
/// the index lives on the disks, so a restart is a rescan).
async fn apply_box(state: &DispatcherState, tenant: &TenantId) -> Result<()> {
    let ns = state.namespace_mapper.namespace_for(tenant);
    let tenant_label = SafeLabel::new(tenant.as_str(), 63);
    let disk_rows = disks(&state.pg_pool, tenant.as_str()).await?;
    if disk_rows.is_empty() {
        return Err(anyhow!(
            "storage box for tenant '{tenant}' has no disk rows; provisioning is broken \
             (the box row exists without its first disk)"
        ));
    }
    let prof = profile(&state.pg_pool, tenant.as_str()).await?;
    let manifest = render_box_bundle(&RenderArgs {
        namespace: &ns,
        tenant: &tenant_label,
        image: &state.storage_image,
        disks: &disk_rows,
        disk_unit_bytes: prof.disk_unit_bytes,
        public_base_url: &public_base_url(state, &tenant_label),
        dispatcher_internal_url: &state.internal_base_url,
        ingress_path: &format!("/storage/{tenant_label}"),
    });
    state.kube.apply_yaml(&manifest).await?;
    // Gate on the box pod actually rolling out before returning. A
    // cold box (first provision or post-scale-to-zero wake) takes
    // seconds-to-minutes to schedule, pull, bind its PVC, and start
    // serving. Without this wait, ensure_box hands the caller a
    // box_url whose pod isn't listening yet, and the worker's first
    // request fails connection-refused (the put body stream can't be
    // replayed, so it fails the node). A disk add/remove restarts the
    // single pod, same race. Deadline is fine here: this is an
    // internal dispatcher->k8s wait the user can't influence, and a
    // rollout that never completes is a provisioning bug, not a
    // legitimately-long user operation. Matches the listener spawn,
    // which gates its admin-URL probe the same way.
    state.kube.wait_rollout_status(&ns, "weft-storage", 120).await
}

/// Full teardown: pod, service, ingress, EVERY backing PVC, and the
/// DB rows. Only the scale-to-zero reaper (zero bytes) and `weft rm
/// --all`-style cleanup call this.
pub async fn teardown_box(state: &DispatcherState, tenant: &TenantId) -> Result<()> {
    let ns = state.namespace_mapper.namespace_for(tenant);
    let tenant_label = SafeLabel::new(tenant.as_str(), 63);
    let opts = weft_platform_traits::DeleteOpts::no_wait();
    state.kube.delete_named(&ns, "deployment", "weft-storage", opts.clone()).await?;
    state.kube.delete_named(&ns, "service", "weft-storage", opts.clone()).await?;
    state.kube.delete_named(&ns, "ingress", "weft-storage", opts.clone()).await?;
    // Reclaim PVCs by LABEL, not by disk row. A shrink that deleted a
    // disk row but then failed to re-apply leaves an orphaned PVC whose
    // row is gone; a row-driven teardown would never reclaim it. The
    // label selector catches every PVC the box ever provisioned for
    // this tenant, orphans included. (preserve_pvcs empty: a full
    // teardown keeps nothing. The selector also re-targets the already
    // name-deleted deployment/service/ingress, which is an idempotent
    // no-op.)
    state
        .kube
        .delete_by_label(
            &ns,
            &format!("weft.dev/role=storage,weft.dev/tenant={tenant_label}"),
            &[],
        )
        .await?;
    sqlx::query("DELETE FROM storage_disk WHERE tenant_id = $1")
        .bind(tenant.as_str())
        .execute(&state.pg_pool)
        .await?;
    sqlx::query("DELETE FROM storage_box WHERE tenant_id = $1")
        .bind(tenant.as_str())
        .execute(&state.pg_pool)
        .await?;
    tracing::info!(
        target: "weft_dispatcher::storage",
        tenant = %tenant,
        "storage box torn down (every PVC reclaimed by label)"
    );
    Ok(())
}

// ---------- grow / shrink (serving the box's watcher) ----------

/// The box asked for one more disk: provision a PVC row per the
/// profile and re-apply (pod restarts with the new mount).
pub async fn grow(state: &DispatcherState, tenant: &TenantId) -> Result<()> {
    if !box_exists(&state.pg_pool, tenant.as_str()).await? {
        return Err(anyhow!("no storage box for tenant '{tenant}'"));
    }
    let prof = profile(&state.pg_pool, tenant.as_str()).await?;
    let name = add_disk_row(&state.pg_pool, tenant.as_str(), &prof).await?;
    tracing::info!(
        target: "weft_dispatcher::storage",
        tenant = %tenant, disk = %name,
        "growing storage box"
    );
    apply_box(state, tenant).await
}

/// The box finished evacuating `disk`: drop the row, re-apply (pod
/// restarts without the mount), then delete the PVC.
pub async fn shrink(state: &DispatcherState, tenant: &TenantId, disk: &str) -> Result<()> {
    let removed = sqlx::query("DELETE FROM storage_disk WHERE tenant_id = $1 AND name = $2")
        .bind(tenant.as_str())
        .bind(disk)
        .execute(&state.pg_pool)
        .await?
        .rows_affected();
    if removed == 0 {
        return Err(anyhow!("unknown disk '{disk}' for tenant '{tenant}'"));
    }
    // The row must go BEFORE apply (apply renders the manifest from the
    // remaining rows, so the dropped disk is what makes the pod unmount
    // it). If apply fails here, the row is gone but the PVC is still
    // bound: it is NOT un-reclaimable junk, teardown_box reclaims every
    // box PVC by label (orphans included), so a later scale-to-zero or
    // `weft rm` cleans it up. The error still surfaces loudly.
    apply_box(state, tenant).await?;
    let ns = state.namespace_mapper.namespace_for(tenant);
    state
        .kube
        .delete_named(&ns, "pvc", &pvc_name(disk), weft_platform_traits::DeleteOpts::no_wait())
        .await?;
    tracing::info!(
        target: "weft_dispatcher::storage",
        tenant = %tenant, disk = %disk,
        "shrunk storage box (PVC released)"
    );
    Ok(())
}

// ---------- reaper + sweep queue ----------

/// Scale-to-zero reaper body: for every provisioned box, ask its
/// usage; a box holding ZERO bytes (no kept survivors, no
/// project/shared data, no live scratch) that has been idle past the
/// window is fully torn down. Unreachable boxes are skipped (the
/// next tick retries); the teardown itself is idempotent.
pub async fn sweep_boxes(state: DispatcherState) -> Result<()> {
    let tenants: Vec<String> = sqlx::query_scalar("SELECT tenant_id FROM storage_box")
        .fetch_all(&state.pg_pool)
        .await?;
    for tenant in tenants {
        let tenant = TenantId(tenant);
        let url = box_url(&state, &tenant);
        let usage = match state.storage_admin.usage(&url).await {
            Ok(u) => u,
            Err(e) => {
                tracing::debug!(
                    target: "weft_dispatcher::storage",
                    tenant = %tenant, error = %e,
                    "box usage unreachable; skipping this tick"
                );
                continue;
            }
        };
        let idle_for = crate::lease::now_unix() - usage.last_activity_unix;
        if usage.stored_bytes == 0
            && usage.file_count == 0
            && idle_for >= weft_storage::config::SCALE_TO_ZERO_IDLE.as_secs() as i64
        {
            teardown_box(&state, &tenant).await?;
        }
    }
    Ok(())
}

/// Enqueue a terminate sweep for `color`. Called by the journal
/// bridge when it observes a terminal exec event; idempotent.
pub async fn enqueue_sweep(pool: &PgPool, tenant: &str, color: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO storage_sweep (color, tenant_id, enqueued_at_unix) \
         VALUES ($1, $2, $3) ON CONFLICT (color) DO NOTHING",
    )
    .bind(color)
    .bind(tenant)
    .bind(crate::lease::now_unix())
    .execute(pool)
    .await?;
    Ok(())
}

/// Sweep-queue reaper body: tell each pending color's box to sweep
/// its un-kept exec files. A missing box means nothing to sweep
/// (storage can't outlive the box); an unreachable box stays queued
/// for the next tick. Rows are removed only after the box confirmed.
pub async fn process_sweep_queue(state: DispatcherState) -> Result<()> {
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT color, tenant_id FROM storage_sweep ORDER BY enqueued_at_unix")
            .fetch_all(&state.pg_pool)
            .await?;
    for (color, tenant) in rows {
        let tenant_id = TenantId(tenant.clone());
        let done = if !box_exists(&state.pg_pool, &tenant).await? {
            true
        } else {
            let url = box_url(&state, &tenant_id);
            match state.storage_admin.sweep_exec(&url, &color).await {
                Ok(swept) => {
                    if swept > 0 {
                        tracing::info!(
                            target: "weft_dispatcher::storage",
                            %color, tenant = %tenant, swept,
                            "terminate sweep removed un-kept exec files"
                        );
                    }
                    true
                }
                Err(e) => {
                    tracing::debug!(
                        target: "weft_dispatcher::storage",
                        %color, tenant = %tenant, error = %e,
                        "terminate sweep deferred (box unreachable)"
                    );
                    false
                }
            }
        };
        if done {
            sqlx::query("DELETE FROM storage_sweep WHERE color = $1")
                .bind(&color)
                .execute(&state.pg_pool)
                .await?;
        }
    }
    Ok(())
}

// ---------- manifests ----------

fn pvc_name(disk: &str) -> String {
    format!("weft-storage-{disk}")
}

pub struct RenderArgs<'a> {
    pub namespace: &'a str,
    pub tenant: &'a SafeLabel,
    pub image: &'a str,
    pub disks: &'a [DiskRow],
    pub disk_unit_bytes: i64,
    pub public_base_url: &'a str,
    pub dispatcher_internal_url: &'a str,
    pub ingress_path: &'a str,
}

/// Render the full bundle. PVC sizes/classes come from each row (a
/// profile change affects only NEW disks); the Deployment mounts
/// every disk under `/disks/<name>` and uses `Recreate` (RWO PVCs
/// cannot attach to two pods during a rolling update).
pub fn render_box_bundle(args: &RenderArgs<'_>) -> String {
    let RenderArgs {
        namespace,
        tenant,
        image,
        disks,
        disk_unit_bytes,
        public_base_url,
        dispatcher_internal_url,
        ingress_path,
    } = args;
    // One source for the box's listen port: the containerPort, the
    // Service, the ingress backend, and the worker URL all use it, so
    // they can't drift from the binary's actual bind port.
    let port = weft_storage::config::STORAGE_PORT;
    let mut out = String::new();

    for d in *disks {
        let pvc = pvc_name(&d.name);
        let class_line = match &d.storage_class {
            Some(c) => format!("  storageClassName: {c}\n"),
            None => String::new(),
        };
        out.push_str(&format!(
            r#"---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: {pvc}
  namespace: {namespace}
  labels:
    weft.dev/role: storage
    weft.dev/tenant: "{tenant}"
spec:
  accessModes: ["ReadWriteOnce"]
{class_line}  resources:
    requests:
      storage: "{size}"
"#,
            size = d.size_bytes,
        ));
    }

    let volume_mounts: String = disks
        .iter()
        .map(|d| format!("            - name: {0}\n              mountPath: /disks/{0}\n", d.name))
        .collect();
    let volumes: String = disks
        .iter()
        .map(|d| {
            format!(
                "        - name: {0}\n          persistentVolumeClaim:\n            claimName: {1}\n",
                d.name,
                pvc_name(&d.name)
            )
        })
        .collect();

    out.push_str(&format!(
        r#"---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: weft-storage
  namespace: {namespace}
  labels:
    weft.dev/role: storage
    weft.dev/tenant: "{tenant}"
spec:
  replicas: 1
  # RWO PVCs: the old pod must release the disks before the new one
  # mounts them. Recreate accepts the seconds-long gap; the worker
  # client re-ensures + retries once on box-unreachable.
  strategy:
    type: Recreate
  selector:
    matchLabels:
      weft.dev/role: storage
      weft.dev/tenant: "{tenant}"
  template:
    metadata:
      labels:
        weft.dev/role: storage
        weft.dev/tenant: "{tenant}"
    spec:
      serviceAccountName: weft-storage-sa
      containers:
        - name: storage
          image: {image}
          imagePullPolicy: IfNotPresent
          ports:
            - containerPort: {port}
          env:
            - name: WEFT_TENANT_ID
              value: "{tenant}"
            - name: WEFT_BROKER_URL
              value: "http://weft-broker.weft-db.svc.cluster.local:9090"
            - name: WEFT_BROKER_TOKEN_PATH
              value: "/var/run/weft/sa/token"
            - name: WEFT_DISPATCHER_URL
              value: "{dispatcher_internal_url}"
            - name: WEFT_STORAGE_PUBLIC_BASE_URL
              value: "{public_base_url}"
            - name: WEFT_STORAGE_DISK_UNIT_BYTES
              value: "{disk_unit_bytes}"
          volumeMounts:
            - name: weft-sa-token
              mountPath: /var/run/weft/sa
              readOnly: true
{volume_mounts}      volumes:
        - name: weft-sa-token
          projected:
            sources:
              - serviceAccountToken:
                  audience: weft-broker
                  expirationSeconds: 3600
                  path: token
{volumes}---
apiVersion: v1
kind: Service
metadata:
  name: weft-storage
  namespace: {namespace}
  labels:
    weft.dev/role: storage
    weft.dev/tenant: "{tenant}"
spec:
  selector:
    weft.dev/role: storage
    weft.dev/tenant: "{tenant}"
  ports:
    - port: {port}
      targetPort: {port}
---
# Public (capability-gated) download path. Claims
# `{ingress_path}/...` on the shared external host, so presigned /
# handshake downloads stream from the ingress controller STRAIGHT to
# the box; the dispatcher's catch-all never sees these bytes. The
# capability inside the URL is the real gate.
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: weft-storage
  namespace: {namespace}
  # Bundle labels so teardown's by-label sweep reclaims this too,
  # keeping every box resource uniformly labeled (the name-delete
  # also gets it, but uniform labels are the orphan safety net).
  labels:
    weft.dev/role: storage
    weft.dev/tenant: "{tenant}"
  annotations:
    nginx.ingress.kubernetes.io/rewrite-target: /$2
    nginx.ingress.kubernetes.io/proxy-body-size: "0"
    nginx.ingress.kubernetes.io/proxy-read-timeout: "3600"
spec:
  ingressClassName: nginx
  rules:
    - http:
        paths:
          - path: {ingress_path}(/|$)(.*)
            pathType: ImplementationSpecific
            backend:
              service:
                name: weft-storage
                port:
                  number: {port}
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: storage-policy
  namespace: {namespace}
  # Carry the bundle labels so teardown_box's by-label sweep reclaims
  # this policy too; without them it would orphan in the namespace on
  # every scale-to-zero (every other resource here is labeled).
  labels:
    weft.dev/role: storage
    weft.dev/tenant: "{tenant}"
spec:
  podSelector:
    matchLabels:
      weft.dev/role: storage
  policyTypes:
    - Ingress
    - Egress
  ingress:
    # Worker pods in ANY of this tenant's namespaces (data path).
    - from:
        - namespaceSelector:
            matchLabels:
              weft.dev/tenant: "{tenant}"
          podSelector:
            matchLabels:
              weft.dev/role: worker
      ports:
        - protocol: TCP
          port: {port}
    # Dispatcher (admin surface: mint, sweeps, usage).
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: weft-system
          podSelector:
            matchLabels:
              weft.dev/role: dispatcher
      ports:
        - protocol: TCP
          port: {port}
    # Ingress controller (capability-gated public downloads).
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: ingress-nginx
      ports:
        - protocol: TCP
          port: {port}
  egress:
    # Broker (caller-token verification relay).
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: weft-db
          podSelector:
            matchLabels:
              weft.dev/role: broker
      ports:
        - protocol: TCP
          port: 9090
    # Dispatcher (grow/shrink disk requests). Scoped to the
    # dispatcher's internal port (9999), matching the dispatcher-ingress
    # rule that admits this exact traffic; every other rule in this
    # policy pins a port, so this one does too (no all-ports egress to
    # the control-plane namespace).
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: weft-system
          podSelector:
            matchLabels:
              weft.dev/role: dispatcher
      ports:
        - protocol: TCP
          port: 9999
    # DNS.
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: kube-system
      ports:
        - protocol: UDP
          port: 53
        - protocol: TCP
          port: 53
"#,
    ));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args<'a>(disks: &'a [DiskRow], tenant: &'a SafeLabel) -> RenderArgs<'a> {
        RenderArgs {
            namespace: "wm-alice",
            tenant,
            image: "weft-storage:local",
            disks,
            disk_unit_bytes: 10 << 30,
            public_base_url: "http://dispatcher.weft.local/storage/alice",
            dispatcher_internal_url: "http://weft-dispatcher.weft-system.svc.cluster.local:9999",
            ingress_path: "/storage/alice",
        }
    }

    fn rows() -> Vec<DiskRow> {
        vec![
            DiskRow { name: "disk-0".into(), size_bytes: 10 << 30, storage_class: None },
            DiskRow {
                name: "disk-1".into(),
                size_bytes: 20 << 30,
                storage_class: Some("fast-ssd".into()),
            },
        ]
    }

    #[test]
    fn renders_one_pvc_per_disk_with_profile_class() {
        let tenant = SafeLabel::new("alice", 63);
        let disks = rows();
        let yaml = render_box_bundle(&args(&disks, &tenant));
        assert!(yaml.contains("name: weft-storage-disk-0"));
        assert!(yaml.contains("name: weft-storage-disk-1"));
        assert!(yaml.contains("storageClassName: fast-ssd"));
        // disk-0 has NO storageClassName (cluster default).
        let pvc0 = yaml.split("weft-storage-disk-0").nth(1).unwrap();
        let pvc0_block = pvc0.split("---").next().unwrap();
        assert!(!pvc0_block.contains("storageClassName"));
    }

    #[test]
    fn deployment_mounts_every_disk_and_uses_recreate() {
        let tenant = SafeLabel::new("alice", 63);
        let disks = rows();
        let yaml = render_box_bundle(&args(&disks, &tenant));
        assert!(yaml.contains("mountPath: /disks/disk-0"));
        assert!(yaml.contains("mountPath: /disks/disk-1"));
        assert!(yaml.contains("claimName: weft-storage-disk-0"));
        assert!(yaml.contains("type: Recreate"));
        assert!(yaml.contains("serviceAccountName: weft-storage-sa"));
        assert!(yaml.contains("audience: weft-broker"));
        // The container port, the Service, and the ingress backend all
        // render the ONE storage port constant (so they can't drift
        // from the binary's bind port). NOT an env var: kubernetes
        // injects WEFT_STORAGE_PORT=tcp://... for the Service, which
        // would collide. Assert the constant appears and the colliding
        // env var never does.
        let port = weft_storage::config::STORAGE_PORT;
        assert!(yaml.contains(&format!("containerPort: {port}")));
        assert!(yaml.contains(&format!("targetPort: {port}")));
        assert!(
            !yaml.contains("WEFT_STORAGE_PORT"),
            "must not set WEFT_STORAGE_PORT (collides with k8s service-link injection)"
        );
    }

    #[test]
    fn ingress_claims_the_tenant_storage_path_only() {
        let tenant = SafeLabel::new("alice", 63);
        let disks = rows();
        let yaml = render_box_bundle(&args(&disks, &tenant));
        assert!(yaml.contains("path: /storage/alice(/|$)(.*)"));
        assert!(yaml.contains("rewrite-target: /$2"));
    }

    #[test]
    fn network_policy_walls_ingress_to_workers_dispatcher_ingress() {
        let tenant = SafeLabel::new("alice", 63);
        let disks = rows();
        let yaml = render_box_bundle(&args(&disks, &tenant));
        let policy = yaml.split("name: storage-policy").nth(1).unwrap();
        assert!(policy.contains("weft.dev/role: worker"));
        assert!(policy.contains("weft.dev/role: dispatcher"));
        assert!(policy.contains("ingress-nginx"));
        assert!(policy.contains("weft.dev/role: broker"));
        // Must carry the bundle labels so teardown's by-label sweep
        // reclaims it (else it orphans on scale-to-zero).
        let meta = policy.split("spec:").next().unwrap();
        assert!(meta.contains("weft.dev/role: storage"), "storage-policy needs role label");
        assert!(meta.contains("weft.dev/tenant: \"alice\""), "storage-policy needs tenant label");
    }
}
