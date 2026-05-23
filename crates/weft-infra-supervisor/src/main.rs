//! Per-tenant infra supervisor pod entry point.
//!
//! Discovers projects via the broker, polls k8s API for replica
//! state, evaluates health protocols, and executes
//! `infra_lifecycle_command` rows (apply / stop / terminate).
//!
//! The binary is thin: it parses args, constructs a
//! `SupervisorState` from production trait impls, and spawns the
//! two loops. Everything testable lives in the library
//! (`src/lib.rs`).

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use weft_broker_client::client::BrokerSupervisorClient;
use weft_broker_client::token::TokenSource;
use weft_infra_supervisor::{broker_ops, health, lifecycle, SupervisorState};
use weft_platform_traits::clock::SystemClock;
use weft_platform_traits::kube;

#[derive(Debug, Parser)]
#[command(name = "weft-infra-supervisor", version)]
struct Args {
    /// Broker URL (cross-namespace to `weft-db`).
    #[arg(long, env = "WEFT_BROKER_URL")]
    broker_url: String,
    /// Projected SA token for broker auth.
    #[arg(
        long,
        env = "WEFT_BROKER_TOKEN_PATH",
        default_value = "/var/run/weft/sa/token"
    )]
    broker_token_path: String,
    /// Tenant id this supervisor is scoped to.
    #[arg(long, env = "WEFT_TENANT_ID")]
    tenant_id: String,
    /// k8s pod name (downward API).
    #[arg(long, env = "WEFT_POD_NAME")]
    pod_name: String,
    /// How often to poll for new projects, lifecycle commands, and
    /// health changes.
    #[arg(long, default_value_t = 5)]
    poll_interval_seconds: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "weft_infra_supervisor=info".into()),
        )
        .init();

    let args = Args::parse();
    tracing::info!(
        tenant = %args.tenant_id,
        pod = %args.pod_name,
        "weft-infra-supervisor starting"
    );

    let token = TokenSource::new(std::path::PathBuf::from(&args.broker_token_path));
    let broker = BrokerSupervisorClient::new(args.broker_url.clone(), token);

    let kube_client = kube::in_cluster().await.context("init kube client")?;

    let supervisor = SupervisorState {
        broker: broker_ops::production(broker),
        tenant_id: args.tenant_id.clone(),
        pod_name: args.pod_name.clone(),
        kube: kube_client,
        clock: SystemClock::new(),
        poll_interval: Duration::from_secs(args.poll_interval_seconds),
        health: Arc::new(tokio::sync::Mutex::new(health::HealthRegistry::default())),
    };

    let lifecycle_state = supervisor.clone();
    let lifecycle_handle = tokio::spawn(async move {
        if let Err(e) = lifecycle::run_loop(lifecycle_state).await {
            tracing::error!(error = %e, "lifecycle loop exited");
        }
    });

    let health_state = supervisor.clone();
    let health_handle = tokio::spawn(async move {
        if let Err(e) = health::run_loop(health_state).await {
            tracing::error!(error = %e, "health loop exited");
        }
    });

    // Both loops are `loop {}` that only return on a propagated
    // error, and the spawned tasks only resolve early on a panic.
    // Either is an abnormal condition that leaves the supervisor
    // half-broken (e.g. lifecycle dead → no applies; health dead →
    // no SLO monitoring). Exit NON-ZERO so the failure is visible
    // to exit-code alerting and k8s restarts the whole pod into a
    // clean state, rather than falling through to Ok(()) (which
    // would exit 0 and mask the crash). SIGINT is the only clean
    // exit.
    fn died(loop_name: &str) -> ! {
        tracing::error!("{loop_name} loop ended unexpectedly; exiting non-zero for restart");
        std::process::exit(1);
    }
    tokio::select! {
        _ = lifecycle_handle => died("lifecycle"),
        _ = health_handle => died("health"),
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("SIGINT; exiting");
            Ok(())
        }
    }
}
