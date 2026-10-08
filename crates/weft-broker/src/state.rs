//! Shared broker state. Owns the Postgres pool, the trait clients
//! that wrap it, the auth config, and per-scope caches.

use std::sync::Arc;

use anyhow::Context;
use sqlx::postgres::PgPool;

use weft_task_store::pg_signal::PgSignalWatch;
use weft_task_store::{PostgresInfraReader, PostgresTaskStoreClient, TaskStoreClient};

use weft_platform_traits::{CallerIdentity, ObjectStore};

use crate::auth::{AuthConfig, IdentityCache};
use crate::credential::CredentialSource;
use crate::entitlement::EntitlementSource;
use crate::runtime_store::RuntimeStore;
use crate::scope::ScopeCache;

/// Every channel the broker listens on: what a held request from a
/// worker or a role (which have no database connection of their own)
/// waits for, and what its lines push. The process's one `LISTEN`
/// connection must hold them all.
pub const BROKER_CHANNELS: &[&str] = &[
    weft_task_store::tasks::TASK_READY_CHANNEL,
    weft_task_store::terminal::TERMINAL_CHANNEL,
    // What a worker holding for the answers of a run it drives waits on
    // (`crate::records`).
    weft_task_store::parked_fires::PARKED_FIRE_CHANNEL,
    weft_broker_client::lifecycle_command::INFRA_COMMAND_CHANNEL,
    // What a line pushes to the workers following it (`crate::line`).
    weft_broker_client::line::INFRA_STATUS_CHANNEL,
    weft_broker_client::line::ACCESS_CHANNEL,
    weft_broker_client::line::CANCEL_CHANNEL,
    weft_broker_client::line::TRIGGERS_CHANNEL,
];

pub struct BrokerState {
    pub pool: PgPool,
    /// A pool for locks held across a slow call (a provider round trip),
    /// whose connections do no work while they hold one.
    pub lock_pool: PgPool,
    /// The pool workers' batches of records are written on
    /// (`crate::records`): a burst of records never starves the broker's
    /// other work of connections.
    pub record_pool: PgPool,
    /// What the broker tells the dispatcher once a batch commits.
    pub notices: Arc<crate::notices::Notices>,
    /// The process's one Postgres `LISTEN` connection, on (at least)
    /// [`BROKER_CHANNELS`]; every held request sleeps on it.
    pub signals: Arc<PgSignalWatch>,
    pub tasks: Arc<dyn TaskStoreClient>,
    pub infra: Arc<PostgresInfraReader>,
    pub auth: AuthConfig,
    /// Verifies every caller's bearer token: the platform's answer to
    /// "who is this".
    pub identity: Arc<dyn CallerIdentity>,
    pub identity_cache: IdentityCache,
    pub scope_cache: ScopeCache,
    /// What infra each program a project registered declares, keyed by the
    /// project and a digest of its definition: a definition never changes
    /// under its digest, so an entry is never stale, and a program asking
    /// for an endpoint on every run reads its definition once.
    pub declared_infra: weft_core::content_cache::ContentCache<(uuid::Uuid, String), weft_core::project::DeclaredInfra>,
    /// The connections live routes are gated by, kept until they change
    /// (`crate::caller_auth::HeldVerifiers`).
    pub verifiers: Arc<crate::caller_auth::HeldVerifiers>,
    /// The lines that follow notifications (`crate::line::LineFanout`).
    pub lines: Arc<crate::line::LineFanout>,
    /// Where runtime-file bytes live: the install's bucket.
    pub object_store: Arc<dyn ObjectStore>,
    /// The runtime-file plane (`ctx.storage`): PG metadata + bucket bytes,
    /// quota-enforced.
    pub runtime_store: Arc<RuntimeStore>,
    /// Resolves a tenant's runtime-storage caps.
    pub entitlements: Arc<dyn EntitlementSource>,
    /// Resolves the runtime's provider keys for nodes that asked the
    /// runtime to supply one.
    pub credentials: Arc<dyn CredentialSource>,
    /// The SOLE trusted source of the registered shared-door apps
    /// (default: the shared-credentials file). A shared connect only
    /// ever resolves its app here; project metadata never supplies
    /// one (the own door carries the user's own app instead).
    pub app_provider: Arc<dyn crate::app_provider::AppProvider>,
    /// The stable base URL users hit for this weft. Event subscribe calls
    /// tell the provider to post to `<base>/events/<service>/<topic>`
    /// when this is reachable from the internet; a weft with no
    /// internet-reachable address refuses those subscriptions loudly,
    /// naming the fix.
    pub public_base_url: String,
    /// An ADDITIONAL internet-reachable address (a tunnel's), preferred
    /// over the base when telling a provider where to post.
    pub internet_url: Option<String>,
    /// True iff the object store's presigned EXTERNAL-audience URLs are
    /// reachable from the open internet: public file links then skip the
    /// relay and point straight at the bucket. A local install never
    /// sets it.
    pub object_store_public_internet: bool,
}

/// What the broker is built from, besides the pool and the signal watch
/// the process shares with its other roles.
pub struct BrokerSettings {
    pub auth: AuthConfig,
    pub identity: Arc<dyn CallerIdentity>,
    pub object_store: Arc<dyn ObjectStore>,
    pub entitlements: Arc<dyn EntitlementSource>,
    pub credentials: Arc<dyn CredentialSource>,
    pub app_provider: Arc<dyn crate::app_provider::AppProvider>,
    pub public_base_url: String,
    pub internet_url: Option<String>,
    pub object_store_public_internet: bool,
}

impl BrokerState {
    /// The base URL the OPEN INTERNET reaches this weft at, or `None`
    /// when there is none: the tunnel's address when one is up, else the
    /// stable base when it is not a loopback. A loopback base counts as
    /// "not reachable".
    pub fn internet_base(&self) -> Option<&str> {
        self.internet_url
            .as_deref()
            .or_else(|| Some(self.public_base_url.as_str()).filter(|b| !weft_core::net::is_loopback_url(b)))
    }

    /// Build the broker state over the process's pool and signal watch,
    /// applying the broker's own schema group (the runtime-file table).
    pub async fn new(
        pool: PgPool,
        lock_pool: PgPool,
        record_pool: PgPool,
        signals: Arc<PgSignalWatch>,
        settings: BrokerSettings,
    ) -> anyhow::Result<Arc<Self>> {
        for channel in BROKER_CHANNELS {
            signals.require(channel)?;
        }
        let notices = crate::notices::Notices::start(pool.clone());
        let verifiers = weft_task_store::held_copy::HeldCopy::follow(
            &signals,
            weft_broker_client::line::ACCESS_CHANNEL,
            4096,
            crate::caller_auth::verifiers_changed,
            |_| true,
        )?;
        let lines = crate::line::LineFanout::start(&signals);
        let tasks: Arc<dyn TaskStoreClient> = Arc::new(PostgresTaskStoreClient::new(pool.clone(), signals.clone())?);
        // A public infra endpoint's address is handed to whoever calls in
        // (a provider's webhook target), so it is built on the address the
        // open internet reaches, never the local one.
        let front_door = settings.internet_url.clone().unwrap_or_else(|| settings.public_base_url.clone());
        let infra = Arc::new(PostgresInfraReader::new(pool.clone(), Some(front_door)));
        weft_task_store::apply_groups(&pool, &[&crate::runtime_store::GROUP])
            .await
            .context("apply runtime_file schema group")?;
        let runtime_store = Arc::new(RuntimeStore::new(
            pool.clone(),
            settings.object_store.clone(),
            Arc::new(weft_platform_traits::clock::SystemClock),
        ));
        Ok(Arc::new(Self {
            pool,
            lock_pool,
            record_pool,
            notices,
            signals,
            tasks,
            infra,
            auth: settings.auth,
            identity: settings.identity,
            identity_cache: IdentityCache::new()?,
            scope_cache: ScopeCache::new(),
            declared_infra: weft_core::content_cache::ContentCache::new(256),
            verifiers,
            lines,
            object_store: settings.object_store,
            runtime_store,
            entitlements: settings.entitlements,
            credentials: settings.credentials,
            app_provider: settings.app_provider,
            public_base_url: settings.public_base_url,
            internet_url: settings.internet_url,
            object_store_public_internet: settings.object_store_public_internet,
        }))
    }
}
