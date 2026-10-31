//! Shared broker state. Owns the Postgres pool, the trait clients
//! that wrap it, the auth config, and per-scope caches.

use std::sync::Arc;

use weft_task_store::db::Pool;

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
    // An answer handed to a run a worker drives, which its line pushes
    // (`crate::line`).
    weft_task_store::parked_fires::PARKED_FIRE_CHANNEL,
    weft_broker_client::lifecycle_command::INFRA_COMMAND_CHANNEL,
    // What a line pushes to the workers following it (`crate::line`).
    weft_broker_client::line::INFRA_STATUS_CHANNEL,
    weft_broker_client::line::ACCESS_CHANNEL,
    weft_broker_client::line::CANCEL_CHANNEL,
    weft_broker_client::line::TRIGGERS_CHANNEL,
    // A stored file's expiry set sooner than the expiry loop's next look
    // (`crate::runtime_store`).
    crate::runtime_store::FILE_EXPIRY_CHANNEL,
    // The install's domains, whose certificate decides the address its
    // links are built on (`weft_task_store::install_address`).
    weft_task_store::install_address::DOMAINS_CHANNEL,
];

pub struct BrokerState {
    pub pool: Pool,
    /// A pool for locks held across a slow call (a provider round trip),
    /// whose connections do no work while they hold one.
    pub lock_pool: Pool,
    /// The pool workers' batches of records are written on
    /// (`crate::records`): a burst of records never starves the broker's
    /// other work of connections.
    pub record_pool: Pool,
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
    /// The address the install hands out in its links, and where the open
    /// internet reaches it as it is now (a quick tunnel's is asked each
    /// time; the install's domain once its certificate is issued,
    /// `weft_task_store::install_address`). Event subscribe calls tell the
    /// provider to post to `<internet>/events/<service>/<topic>`, and a
    /// link to a stored file goes through it when its project declares no
    /// visitor address; a weft with none refuses those subscriptions
    /// loudly, naming the fix. A public infra endpoint's address hangs off
    /// the stable address when the open internet does not reach the
    /// install ([`Self::front_door`]).
    pub address: weft_task_store::install_address::InstallAddress,
    /// The install's own address for a link outside callers open when
    /// neither a site nor the internet carries it (the network address
    /// when one is set): such a link says `public: false`.
    pub own_base_url: String,
    /// True iff the object store's own presigned links are reachable from
    /// the open internet (Cloud Storage): a link to a stored file for the
    /// outside is then the store's, straight, and never goes through the
    /// install. A local install never sets it.
    pub object_store_public_internet: bool,
    /// What signs and reads the upload slots pages send files through
    /// (`weft_core::storage::slot`).
    pub slot_key: weft_core::storage::slot::SlotKey,
    /// See [`BrokerSettings::supervisor_booking`].
    pub supervisor_booking: Option<weft_task_store::drain::Booking>,
    /// See [`BrokerSettings::aside`].
    pub aside: weft_task_store::aside::StepAside,
    /// When the log last said a sender's calls do not read
    /// (`crate::wire`).
    pub unreadable_shapes: crate::wire::UnreadableShapes,
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
    /// The platform's own address for this weft, from the config
    /// (`InstallConfig::public_url`).
    pub service_url: String,
    /// The address the open internet reaches this weft at when no domain
    /// is the address, read live.
    pub internet: weft_platform_traits::config::InternetAddress,
    pub own_base_url: String,
    pub object_store_public_internet: bool,
    pub slot_key: weft_core::storage::slot::SlotKey,
    /// How long this process's platform lets one line live before cutting
    /// it, `None` when it never does (`crate::line::LineFanout::start`).
    pub line_max_age: Option<std::time::Duration>,
    /// The platform's alarm when the dispatcher scales to zero, `None`
    /// when it does not: a notice to it that could not be sent is handed
    /// to the alarm as a ring of its tick (`crate::notices`).
    pub dispatcher_alarm: Option<Arc<dyn weft_platform_traits::Alarm>>,
    /// Books the supervisor's next pass (`role_loop_due` row
    /// `('supervisor', SUPERVISOR_PASS_LOOP)`) as the broker answers its
    /// ownership sync, on a supervisor that scales to zero.
    pub supervisor_booking: Option<weft_task_store::drain::Booking>,
    /// Whether this copy of the broker was replaced
    /// (`weft_task_store::aside`): once it is, each of its lines takes no
    /// new call, answers the ones in flight and closes, and its caller's
    /// next call opens a line on a current copy (`crate::line`). Never set
    /// on a machine.
    pub aside: weft_task_store::aside::StepAside,
}

impl BrokerState {
    /// The base URL the OPEN INTERNET reaches this weft at, now, or
    /// `None` when there is none.
    pub async fn internet_base(&self) -> anyhow::Result<Option<String>> {
        self.address.internet().await
    }

    /// The base a public infra endpoint's address handed to a caller (a
    /// provider's webhook target) is built on: the address the open
    /// internet reaches this weft at now, else its stable address.
    pub async fn front_door(&self) -> anyhow::Result<String> {
        self.address.outside().await
    }

    /// Build the broker state over the process's pool and signal watch.
    /// The schema (the runtime-file plane's group included) is already
    /// applied: the boot's one pass runs before any role is built.
    pub fn new(
        pool: Pool,
        lock_pool: Pool,
        record_pool: Pool,
        signals: Arc<PgSignalWatch>,
        settings: BrokerSettings,
    ) -> anyhow::Result<Arc<Self>> {
        for channel in BROKER_CHANNELS {
            signals.require(channel)?;
        }
        let notices = crate::notices::Notices::start(pool.clone(), settings.dispatcher_alarm);
        let verifiers = weft_task_store::held_copy::HeldCopy::follow(
            &signals,
            weft_broker_client::line::ACCESS_CHANNEL,
            4096,
            crate::caller_auth::verifiers_changed,
            |_| true,
        )?;
        let lines = crate::line::LineFanout::start(signals.subscribe(), settings.line_max_age);
        let tasks: Arc<dyn TaskStoreClient> = Arc::new(PostgresTaskStoreClient::new(pool.clone(), signals.clone())?);
        let infra = Arc::new(PostgresInfraReader::new(pool.clone()));
        let runtime_store = Arc::new(RuntimeStore::new(
            pool.clone(),
            settings.object_store.clone(),
            Arc::new(weft_platform_traits::clock::SystemClock),
        ));
        let address = weft_task_store::install_address::InstallAddress::follow(pool.clone(), &signals, &settings.service_url, settings.internet)?;
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
            address,
            own_base_url: settings.own_base_url.trim_end_matches('/').to_string(),
            object_store_public_internet: settings.object_store_public_internet,
            slot_key: settings.slot_key,
            supervisor_booking: settings.supervisor_booking,
            aside: settings.aside,
            unreadable_shapes: crate::wire::UnreadableShapes::default(),
        }))
    }
}
