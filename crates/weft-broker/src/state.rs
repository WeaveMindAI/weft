//! Shared broker state. Owns the Postgres pool, the trait clients
//! that wrap it, the auth config, and per-scope caches.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use sqlx::postgres::{PgPool, PgPoolOptions};

use weft_infra::{InfraReader, PostgresInfraReader};
use weft_journal::{JournalClient, PostgresJournalClient};
use weft_task_store::{
    PostgresTaskStoreClient, PostgresWorkerPodClient, TaskStoreClient, WorkerPodClient,
};

use weft_platform_traits::ObjectStore;

use crate::auth::{AuthConfig, IdentityCache};
use crate::entitlement::EntitlementSource;
use crate::runtime_store::RuntimeStore;
use crate::scope::ScopeCache;

pub struct BrokerState {
    pub pool: PgPool,
    pub journal: Arc<dyn JournalClient>,
    pub tasks: Arc<dyn TaskStoreClient>,
    pub worker_pods: Arc<dyn WorkerPodClient>,
    pub infra: Arc<dyn InfraReader>,
    pub auth: AuthConfig,
    pub identity_cache: IdentityCache,
    pub scope_cache: ScopeCache,
    pub kube_client: kube_client::KubeClient,
    /// The object-store slot: where runtime-file bytes live. Pointed at the
    /// configured bucket (the bundled SeaweedFS by default, or any S3-compatible
    /// endpoint). `None` only when no storage slot is configured, which the
    /// runtime-file routes reject loud.
    pub object_store: Option<Arc<dyn ObjectStore>>,
    /// The runtime-file plane (`ctx.storage`): PG metadata + bucket bytes,
    /// quota-enforced. `None` iff `object_store` is `None`.
    pub runtime_store: Option<Arc<RuntimeStore>>,
    /// Resolves a tenant's runtime-storage caps.
    pub entitlements: Arc<dyn EntitlementSource>,
}

impl BrokerState {
    /// Build the broker state. `object_store` is the deploy-time slot (from
    /// `object_store_from_env`); `entitlements` is the budget policy (the default
    /// binary passes the local default; a per-tenant source can be passed instead).
    /// When the slot is set, the runtime-file plane is wired over it.
    pub async fn new(
        database_url: &str,
        auth: AuthConfig,
        object_store: Option<Arc<dyn ObjectStore>>,
        entitlements: Arc<dyn EntitlementSource>,
    ) -> anyhow::Result<Arc<Self>> {
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        let pool = loop {
            match PgPoolOptions::new()
                .max_connections(32)
                .acquire_timeout(Duration::from_secs(5))
                .connect(database_url)
                .await
            {
                Ok(p) => break p,
                Err(e) if std::time::Instant::now() < deadline => {
                    tracing::warn!(
                        target: "weft_broker",
                        error = %e,
                        "postgres not ready yet; retrying"
                    );
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
                Err(e) => return Err(anyhow::anyhow!(e)).context("postgres connect"),
            }
        };

        let journal: Arc<dyn JournalClient> = Arc::new(PostgresJournalClient::new(pool.clone()));
        let tasks: Arc<dyn TaskStoreClient> =
            Arc::new(PostgresTaskStoreClient::new(pool.clone()));
        let worker_pods: Arc<dyn WorkerPodClient> =
            Arc::new(PostgresWorkerPodClient::new(pool.clone()));
        let infra: Arc<dyn InfraReader> = Arc::new(PostgresInfraReader::new(pool.clone()));

        // Wire the runtime-file plane over the slot when one is configured. The
        // broker OWNS the `runtime_file` table (it is the only reader/writer),
        // so it runs that table's migration here, at boot, before serving. The
        // clock is the real system clock (the broker is the data path; the
        // expiry math runs against wall time here, and is unit-tested against a
        // fake clock at the store layer).
        let runtime_store = match object_store.clone() {
            Some(bucket) => {
                crate::runtime_store::migrate(&pool).await.context("runtime_file migrate")?;
                Some(Arc::new(RuntimeStore::new(
                    pool.clone(),
                    bucket,
                    Arc::new(weft_platform_traits::clock::SystemClock),
                )))
            }
            None => None,
        };

        Ok(Arc::new(Self {
            pool,
            journal,
            tasks,
            worker_pods,
            infra,
            auth,
            identity_cache: IdentityCache::new()?,
            scope_cache: ScopeCache::new(),
            kube_client: kube_client::KubeClient::new()?,
            object_store,
            runtime_store,
            entitlements,
        }))
    }
}

/// Minimal in-process k8s API client. Implemented as an HTTP fetch
/// against the in-cluster apiserver using the Pod's projected
/// service-account token; avoids pulling in the full `kube` crate
/// (and its async Discovery surface) for the one operation the
/// broker needs.
pub mod kube_client {
    use anyhow::{Context, Result};
    use serde::{Deserialize, Serialize};

    /// Path the kubelet mounts the in-cluster CA cert at.
    const CA_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt";
    /// Path the kubelet mounts the broker's own SA token at. Used
    /// to authenticate the TokenReview request itself.
    const SELF_TOKEN_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/token";

    #[derive(Clone)]
    pub struct KubeClient {
        client: reqwest::Client,
        api_server: String,
    }

    impl KubeClient {
        pub fn new() -> Result<Self> {
            let host = std::env::var("KUBERNETES_SERVICE_HOST")
                .context("KUBERNETES_SERVICE_HOST not set; broker must run in-cluster")?;
            let port = std::env::var("KUBERNETES_SERVICE_PORT")
                .unwrap_or_else(|_| "443".into());
            let api_server = format!("https://{host}:{port}");

            // The broker MUST run in-cluster; the CA file is always
            // present in normal operation. Falling back to system
            // roots silently would either fail with an opaque error
            // (distroless / no CA bundle) or trust any publicly-CA-
            // issued cert for `kubernetes.default.svc`. Fail fast.
            let pem = std::fs::read(CA_PATH).context("read in-cluster CA cert")?;
            let cert = reqwest::Certificate::from_pem(&pem).context("parse CA cert")?;
            let client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .add_root_certificate(cert)
                .build()
                .context("reqwest build")?;
            Ok(Self { client, api_server })
        }

        /// Run a TokenReview for the presented projected token. Returns
        /// the verified `(namespace, sa_name)` pair on success or an
        /// error if the token is invalid / expired / wrong audience.
        pub async fn token_review(
            &self,
            token: &str,
            audience: &str,
        ) -> Result<TokenReviewOutcome> {
            let self_token = std::fs::read_to_string(SELF_TOKEN_PATH)
                .context("read broker SA token")?;
            let url = format!(
                "{}/apis/authentication.k8s.io/v1/tokenreviews",
                self.api_server
            );
            let body = TokenReviewRequest {
                api_version: "authentication.k8s.io/v1".into(),
                kind: "TokenReview".into(),
                spec: TokenReviewSpec {
                    token: token.to_string(),
                    audiences: vec![audience.to_string()],
                },
            };
            let resp = self
                .client
                .post(&url)
                .bearer_auth(self_token.trim())
                .json(&body)
                .send()
                .await
                .context("tokenreview send")?;
            if !resp.status().is_success() {
                let code = resp.status();
                let txt = resp.text().await.unwrap_or_default();
                anyhow::bail!("tokenreview {code}: {txt}");
            }
            let parsed: TokenReviewResponse =
                resp.json().await.context("tokenreview parse")?;
            if !parsed.status.authenticated {
                anyhow::bail!(
                    "tokenreview rejected: {}",
                    parsed
                        .status
                        .error
                        .as_deref()
                        .unwrap_or("(no error message from kube-apiserver)")
                );
            }
            // Audience verification: kube-apiserver returns the
            // INTERSECTION of the requested and accepted audiences in
            // `status.audiences`. A token whose original projection
            // doesn't include `audience` will come back authenticated
            // but with that field empty (or non-overlapping). Without
            // this check, ANY valid SA token in the cluster (kubelet,
            // daemonsets, anything) is accepted as long as the SA name
            // matches our role table. This is the security boundary
            // of TokenReview; skipping it is what unguarded SA-token
            // auth looks like.
            if !parsed.status.audiences.iter().any(|a| a == audience) {
                anyhow::bail!(
                    "tokenreview audience mismatch: token does not carry '{audience}' \
                     (apiserver returned audiences={:?})",
                    parsed.status.audiences,
                );
            }
            let user = parsed
                .status
                .user
                .ok_or_else(|| anyhow::anyhow!("tokenreview returned no user"))?;
            // user.username is `system:serviceaccount:<ns>:<sa-name>`.
            let parts: Vec<&str> = user.username.splitn(4, ':').collect();
            if parts.len() != 4 || parts[0] != "system" || parts[1] != "serviceaccount" {
                anyhow::bail!("unexpected username shape: {}", user.username);
            }
            // The kubelet adds `authentication.kubernetes.io/pod-name`
            // (and `pod-uid`) to a projected SA token's `user.extra`
            // map when the token is bound to a pod (i.e. minted via
            // serviceAccountToken volume projection, which is how every
            // weft tenant pod gets its token). Absence here means the
            // caller is using a raw legacy SA token (not bound to a
            // specific pod), which under our deployment model should
            // not exist; treat as None and let downstream handlers
            // refuse pod-bound operations.
            let pod_name = user
                .extra
                .as_ref()
                .and_then(|e| e.get("authentication.kubernetes.io/pod-name"))
                .and_then(|v| v.first())
                .cloned();
            Ok(TokenReviewOutcome {
                namespace: parts[2].to_string(),
                sa_name: parts[3].to_string(),
                pod_name,
            })
        }
    }

    pub struct TokenReviewOutcome {
        pub namespace: String,
        pub sa_name: String,
        pub pod_name: Option<String>,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct TokenReviewRequest {
        api_version: String,
        kind: String,
        spec: TokenReviewSpec,
    }

    #[derive(Serialize)]
    struct TokenReviewSpec {
        token: String,
        audiences: Vec<String>,
    }

    #[derive(Deserialize)]
    struct TokenReviewResponse {
        status: TokenReviewStatus,
    }

    #[derive(Deserialize)]
    struct TokenReviewStatus {
        #[serde(default)]
        authenticated: bool,
        #[serde(default)]
        audiences: Vec<String>,
        #[serde(default)]
        user: Option<TokenReviewUser>,
        #[serde(default)]
        error: Option<String>,
    }

    #[derive(Deserialize)]
    struct TokenReviewUser {
        username: String,
        #[serde(default)]
        extra: Option<std::collections::BTreeMap<String, Vec<String>>>,
    }
}
