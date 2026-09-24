//! Shared broker state. Owns the Postgres pool, the trait clients
//! that wrap it, the auth config, and per-scope caches.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use sqlx::postgres::{PgPool, PgPoolOptions};

use weft_journal::{JournalClient, PostgresJournalClient};
use weft_task_store::pg_signal::PgSignalWatch;
use weft_task_store::{
    InfraReader, PostgresInfraReader, PostgresTaskStoreClient, PostgresWorkerPodClient,
    TaskStoreClient, WorkerPodClient,
};

use weft_platform_traits::ObjectStore;

use crate::auth::{AuthConfig, IdentityCache};
use crate::credential::CredentialSource;
use crate::entitlement::EntitlementSource;
use crate::runtime_store::RuntimeStore;
use crate::scope::ScopeCache;

/// Every channel the broker listens on, all on its one `LISTEN`
/// connection: what a held request from a pod (which has no database
/// connection of its own) waits for.
pub const BROKER_CHANNELS: &[&str] = &[
    weft_task_store::tasks::TASK_READY_CHANNEL,
    weft_task_store::terminal::TERMINAL_CHANNEL,
    weft_journal::EXEC_EVENT_CHANNEL,
    weft_broker_client::lifecycle_command::INFRA_COMMAND_CHANNEL,
];

pub struct BrokerState {
    pub pool: PgPool,
    /// The broker's one Postgres `LISTEN` connection, on
    /// [`BROKER_CHANNELS`]; every held request sleeps on it.
    pub signals: Arc<PgSignalWatch>,
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
    /// Resolves the runtime's provider keys for nodes that asked the
    /// runtime to supply one (default: the host env).
    pub credentials: Arc<dyn CredentialSource>,
    /// The SOLE trusted source of the registered shared-door apps
    /// (default: the shared-credentials file). A shared connect only
    /// ever resolves its app here; project metadata never supplies
    /// one (the own door carries the user's own app instead).
    pub app_provider: Arc<dyn crate::app_provider::AppProvider>,
    /// The stable base URL users hit for this weft
    /// (`WEFT_DISPATCHER_PUBLIC_BASE_URL`, the same value the
    /// dispatcher publishes). Event subscribe calls tell the provider
    /// to post to `<base>/events/<service>/<topic>` when this is
    /// reachable from the internet; a weft with no internet-reachable
    /// address refuses those subscriptions loudly, naming the fix.
    pub public_base_url: Option<String>,
    /// An ADDITIONAL internet-reachable address (a public tunnel's
    /// minted URL, `WEFT_DISPATCHER_INTERNET_URL`), preferred over the
    /// base when telling a provider where to post.
    pub internet_url: Option<String>,
    /// True iff the object store's presigned EXTERNAL-audience URLs are
    /// reachable from the open internet (`WEFT_OBJECT_STORE_PUBLIC_INTERNET`):
    /// the operator's declaration that the bucket's public endpoint is a
    /// real internet host, letting public file links skip the relay and
    /// point straight at the bucket. A local install never sets it.
    pub object_store_public_internet: bool,
}

impl BrokerState {
    /// The base URL the OPEN INTERNET reaches this weft at, or `None`
    /// when there is none: the tunnel's minted address when one is up,
    /// else the stable base when it is not a loopback (a real cluster's
    /// ingress host). A loopback base counts as "not reachable".
    pub fn internet_base(&self) -> Option<&str> {
        self.internet_url.as_deref().or_else(|| {
            self.public_base_url
                .as_deref()
                .filter(|b| !weft_core::net::is_loopback_url(b))
        })
    }
    /// Build the broker state. `object_store` is the deploy-time slot (from
    /// `object_store_from_env`); `entitlements` is the budget policy (the default
    /// binary passes the local default; a per-tenant source can be passed instead).
    /// When the slot is set, the runtime-file plane is wired over it.
    pub async fn new(
        database_url: &str,
        auth: AuthConfig,
        object_store: Option<Arc<dyn ObjectStore>>,
        entitlements: Arc<dyn EntitlementSource>,
        credentials: Arc<dyn CredentialSource>,
        app_provider: Arc<dyn crate::app_provider::AppProvider>,
    ) -> anyhow::Result<Arc<Self>> {
        let public_base_url = std::env::var("WEFT_DISPATCHER_PUBLIC_BASE_URL")
            .ok()
            .filter(|v| !v.trim().is_empty());
        let internet_url = std::env::var("WEFT_DISPATCHER_INTERNET_URL")
            .ok()
            .filter(|v| !v.trim().is_empty());
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

        let signals = PgSignalWatch::start(&pool, BROKER_CHANNELS)
            .await
            .context("listen for Postgres signals")?;
        let journal: Arc<dyn JournalClient> =
            Arc::new(PostgresJournalClient::new(pool.clone(), signals.clone())?);
        let tasks: Arc<dyn TaskStoreClient> =
            Arc::new(PostgresTaskStoreClient::new(pool.clone(), signals.clone())?);
        let worker_pods: Arc<dyn WorkerPodClient> =
            Arc::new(PostgresWorkerPodClient::new(pool.clone()));
        // The front door a `TenantPublic` endpoint hangs off is the
        // dispatcher's stable base: both are served by the gateway's
        // `local` listener (deploy/k8s/gateway.yaml).
        let infra: Arc<dyn InfraReader> =
            Arc::new(PostgresInfraReader::new(pool.clone(), public_base_url.clone()));

        // Wire the runtime-file plane over the slot when one is configured. The
        // broker OWNS the `runtime_file` table (it is the only reader/writer),
        // so it runs that table's migration here, at boot, before serving. The
        // clock is the real system clock (the broker is the data path; the
        // expiry math runs against wall time here, and is unit-tested against a
        // fake clock at the store layer).
        let runtime_store = match object_store.clone() {
            Some(bucket) => {
                weft_task_store::apply_groups(&pool, &[&crate::runtime_store::GROUP])
                    .await
                    .context("apply runtime_file schema group")?;
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
            signals,
            journal,
            tasks,
            worker_pods,
            infra,
            auth,
            identity_cache: IdentityCache::new()?,
            scope_cache: ScopeCache::new(),
            kube_client: kube_client::KubeClient::connect().await?,
            object_store,
            runtime_store,
            entitlements,
            credentials,
            app_provider,
            public_base_url,
            internet_url,
            object_store_public_internet: std::env::var("WEFT_OBJECT_STORE_PUBLIC_INTERNET")
                .map(|v| v == "1" || v == "true")
                .unwrap_or(false),
        }))
    }
}

/// The broker's one Kubernetes call: TokenReview, through the same
/// in-process client every weft service uses to talk to the cluster.
pub mod kube_client {
    use std::time::Duration;

    use anyhow::{Context, Result};
    use k8s_openapi::api::authentication::v1::{TokenReview, TokenReviewSpec};
    use kube::api::{Api, PostParams};

    /// How long one TokenReview may take before the request it gates
    /// fails.
    const REVIEW_TIMEOUT: Duration = Duration::from_secs(5);

    #[derive(Clone)]
    pub struct KubeClient {
        reviews: Api<TokenReview>,
    }

    impl KubeClient {
        /// The broker MUST run in-cluster: it reviews tokens as its own
        /// service account, against the cluster's own CA.
        pub async fn connect() -> Result<Self> {
            let mut config = kube::Config::incluster()
                .context("read the in-cluster Kubernetes config; the broker must run in-cluster")?;
            config.read_timeout = Some(REVIEW_TIMEOUT);
            config.connect_timeout = Some(REVIEW_TIMEOUT);
            let client = kube::Client::try_from(config).context("build the Kubernetes client")?;
            Ok(Self { reviews: Api::all(client) })
        }

        /// Run a TokenReview for the presented projected token. Returns
        /// the verified `(namespace, sa_name)` pair on success or an
        /// error if the token is invalid / expired / wrong audience.
        pub async fn token_review(
            &self,
            token: &str,
            audience: &str,
        ) -> Result<TokenReviewOutcome> {
            let review = TokenReview {
                spec: TokenReviewSpec {
                    token: Some(token.to_string()),
                    audiences: Some(vec![audience.to_string()]),
                },
                ..TokenReview::default()
            };
            let reviewed = self
                .reviews
                .create(&PostParams::default(), &review)
                .await
                .context("tokenreview")?;
            let status = reviewed.status.unwrap_or_default();
            if !status.authenticated.unwrap_or(false) {
                anyhow::bail!(
                    "tokenreview rejected: {}",
                    status
                        .error
                        .as_deref()
                        .unwrap_or("(no error message from kube-apiserver)")
                );
            }
            outcome_of(status, audience)
        }
    }

    /// What an authenticated review says about its caller. Audience
    /// verification: kube-apiserver returns the INTERSECTION of the
    /// requested and accepted audiences in `status.audiences`. A token
    /// whose original projection doesn't include `audience` comes back
    /// authenticated but with that field empty (or non-overlapping).
    /// Without this check, ANY valid SA token in the cluster (kubelet,
    /// daemonsets, anything) is accepted as long as the SA name matches
    /// our role table. This is the security boundary of TokenReview;
    /// skipping it is what unguarded SA-token auth looks like.
    fn outcome_of(
        status: k8s_openapi::api::authentication::v1::TokenReviewStatus,
        audience: &str,
    ) -> Result<TokenReviewOutcome> {
        let audiences = status.audiences.unwrap_or_default();
        if !audiences.iter().any(|a| a == audience) {
            anyhow::bail!(
                "tokenreview audience mismatch: token does not carry '{audience}' \
                 (apiserver returned audiences={audiences:?})",
            );
        }
        let user = status.user.ok_or_else(|| anyhow::anyhow!("tokenreview returned no user"))?;
        let username = user.username.unwrap_or_default();
        // username is `system:serviceaccount:<ns>:<sa-name>`.
        let parts: Vec<&str> = username.splitn(4, ':').collect();
        if parts.len() != 4 || parts[0] != "system" || parts[1] != "serviceaccount" {
            anyhow::bail!("unexpected username shape: {username}");
        }
        // The kubelet adds `authentication.kubernetes.io/pod-name`
        // (and `pod-uid`) to a projected SA token's `user.extra` map
        // when the token is bound to a pod (i.e. minted via
        // serviceAccountToken volume projection, which is how every
        // weft tenant pod gets its token). A token minted without a
        // pod binding carries no pod name: it still identifies its
        // service account, and every pod-bound operation refuses it
        // (`require_pod_name_matches`, the color-owner gates).
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

    pub struct TokenReviewOutcome {
        pub namespace: String,
        pub sa_name: String,
        pub pod_name: Option<String>,
    }

    #[cfg(test)]
    mod tests {
        use super::outcome_of;
        use k8s_openapi::api::authentication::v1::{TokenReviewStatus, UserInfo};

        fn status(audiences: &[&str], username: &str) -> TokenReviewStatus {
            TokenReviewStatus {
                authenticated: Some(true),
                audiences: Some(audiences.iter().map(|a| a.to_string()).collect()),
                user: Some(UserInfo {
                    username: Some(username.into()),
                    extra: Some(
                        [("authentication.kubernetes.io/pod-name".to_string(), vec!["pod-1".to_string()])]
                            .into_iter()
                            .collect(),
                    ),
                    ..UserInfo::default()
                }),
                ..TokenReviewStatus::default()
            }
        }

        #[test]
        fn a_reviewed_token_names_its_service_account_and_pod() {
            let outcome = outcome_of(status(&["weft-broker"], "system:serviceaccount:ns:sa"), "weft-broker").unwrap();
            assert_eq!((outcome.namespace.as_str(), outcome.sa_name.as_str()), ("ns", "sa"));
            assert_eq!(outcome.pod_name.as_deref(), Some("pod-1"));
        }

        #[test]
        fn a_token_without_the_broker_audience_is_refused() {
            assert!(outcome_of(status(&["other"], "system:serviceaccount:ns:sa"), "weft-broker").is_err());
            assert!(outcome_of(status(&[], "system:serviceaccount:ns:sa"), "weft-broker").is_err());
        }

        #[test]
        fn a_user_that_is_not_a_service_account_is_refused() {
            assert!(outcome_of(status(&["weft-broker"], "alice"), "weft-broker").is_err());
        }
    }
}
