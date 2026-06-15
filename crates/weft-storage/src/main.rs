//! `weft-storage` binary: the per-tenant storage box. Parses env,
//! opens the disk pool (scan + index rebuild), starts the resize
//! watcher + expiry sweep, serves the HTTP surface.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use weft_platform_traits::SystemClock;
use weft_storage::auth::BrokerAuth;
use weft_storage::config::{DEFAULT_DISK_UNIT_BYTES, STORAGE_PORT};
use weft_storage::disk::{DiskPoolOps, LocalDiskPool};
use weft_storage::resize::{run_expiry_loop, ResizeWatcher};
use weft_storage::service::{router, ServiceState};
use weft_storage::store::Store;

fn env_var(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("missing env {name}"))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "weft_storage=info".into()),
        )
        .init();

    let tenant_id = env_var("WEFT_TENANT_ID")?;
    let broker_url = env_var("WEFT_BROKER_URL")?;
    let dispatcher_url = env_var("WEFT_DISPATCHER_URL")?;
    let public_base_url = env_var("WEFT_STORAGE_PUBLIC_BASE_URL")?;
    let disks_root = std::env::var("WEFT_STORAGE_DISKS_ROOT").unwrap_or_else(|_| "/disks".into());
    let token_path = std::env::var("WEFT_BROKER_TOKEN_PATH")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("/var/run/weft/sa/token"));
    // The listen port is a FIXED contract, not configurable: the
    // Service, the Deployment's containerPort, the worker's in-cluster
    // URL, and the tenant ingress all hardcode 8080. It is deliberately
    // NOT read from an env var: kubernetes injects a Docker-link
    // service-discovery variable `WEFT_STORAGE_PORT=tcp://<ip>:8080`
    // for the `weft-storage` Service into every pod in the namespace,
    // which would collide with (and is non-numeric, so would crash) a
    // same-named config var.
    let port: u16 = STORAGE_PORT;
    let disk_unit_bytes: u64 = std::env::var("WEFT_STORAGE_DISK_UNIT_BYTES")
        .ok()
        .map(|v| v.parse().context("WEFT_STORAGE_DISK_UNIT_BYTES must be a number"))
        .transpose()?
        .unwrap_or(DEFAULT_DISK_UNIT_BYTES);

    let pool: Arc<dyn DiskPoolOps> = Arc::new(
        LocalDiskPool::new(&disks_root, dispatcher_url, tenant_id.clone(), token_path)
            .context("open disk pool")?,
    );
    let clock = SystemClock::new();
    let store = Arc::new(Store::open(pool, clock).await.context("open store (scan disks)")?);

    tokio::spawn(ResizeWatcher::new(store.clone(), disk_unit_bytes).run_loop());
    tokio::spawn(run_expiry_loop(store.clone()));

    let state = Arc::new(ServiceState {
        store,
        auth: BrokerAuth::new(broker_url),
        public_base_url,
    });
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!(target: "weft_storage", %addr, tenant = %tenant_id, "storage box up");
    let listener = tokio::net::TcpListener::bind(addr).await.context("bind")?;
    axum::serve(listener, router(state)).await.context("serve")?;
    Ok(())
}
