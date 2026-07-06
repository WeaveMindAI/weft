//! `weft-broker` binary. The trusted proxy that fronts Postgres for
//! every user-namespace pod (worker, listener, infra). Validates
//! the caller's projected ServiceAccount token via TokenReview,
//! resolves it to a (tenant, role), and runs each authenticated
//! request through a per-endpoint scope check before touching the DB.

use anyhow::Context;
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "weft_broker=info".into()),
        )
        .init();

    let database_url = std::env::var("WEFT_DATABASE_URL")
        .context("WEFT_DATABASE_URL is required")?;
    let port: u16 = std::env::var("WEFT_BROKER_PORT")
        .unwrap_or_else(|_| "9090".into())
        .parse()
        .context("WEFT_BROKER_PORT must be a port")?;
    // Audience the broker requires on every projected SA token. Pods
    // mount their token with `audience: weft-broker`; TokenReview
    // verifies the audience matches before we accept it. Default is
    // safe for in-cluster; override via env if you deploy with a
    // different audience claim.
    let audience = std::env::var("WEFT_BROKER_AUDIENCE")
        .unwrap_or_else(|_| "weft-broker".into());

    // The object-store slot (the bundled SeaweedFS in open weft / local dev).
    // Required for the runtime-file plane; a deploy without it boots but the
    // runtime-storage routes fail loud (set WEFT_OBJECT_STORE_ENDPOINT + creds).
    let object_store = weft_platform_traits::object_store_from_env()
        .await
        .context("init object store slot")?;
    // The storage-budget policy: a generous host-bounded default. A
    // per-tenant source can be passed instead.
    let entitlements =
        std::sync::Arc::new(weft_broker::entitlement::LocalEntitlementSource::from_env());

    let state = weft_broker::BrokerState::new(
        &database_url,
        weft_broker::AuthConfig { audience },
        object_store,
        entitlements,
    )
    .await?;

    weft_broker::spawn_expiry_sweep(state.clone());
    let app = weft_broker::router(state);
    let addr: SocketAddr = ([0, 0, 0, 0], port).into();
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind {addr}"))?;
    tracing::info!(target: "weft_broker", %addr, "broker listening");
    axum::serve(listener, app).await.context("axum serve")
}
