//! The rows a live call reads on every request, held in this process's
//! memory and read again the moment the database says they changed
//! (`weft_task_store::held_copy`): a tenant's routes, the install's
//! domains, a project's worker levers, which of its infra copies are up,
//! the connections the install picked for it and what its instances
//! provide.
//! With these held, a call reaches the database once, to be admitted and
//! born. An answer that refuses on what a copy says reads the rows again
//! first (`HeldCopy::load_fresh`): a change can be committed a few
//! milliseconds before it is heard.

use std::sync::Arc;

use weft_core::install::Domain;
use weft_task_store::held_copy::{Changed, HeldCopy};
use weft_task_store::pg_signal::PgSignalWatch;

/// Announced with a tenant's id when one of its routes changes (the
/// signal, project and activation groups' triggers).
// SYNC: ROUTES_CHANNEL <-> crates/weft-dispatcher/src/journal/postgres.rs (routes_notify_tenant), crates/weft-dispatcher/src/activation_store.rs (trigger_activation_routes_notify)
pub const ROUTES_CHANNEL: &str = "weft_routes";

/// Announced with a project's id when its worker levers change or it
/// goes (the project group's trigger).
// SYNC: WORKER_SETTINGS_CHANNEL <-> crates/weft-dispatcher/src/project_store.rs (project_worker_settings_notify)
pub const WORKER_SETTINGS_CHANNEL: &str = "weft_worker_settings";

/// Announced with a project's id when one of its infra copies comes, goes,
/// changes status or answers at another address (the infra_node group's
/// trigger), and when it is registered with another program (the project
/// group's). The broker pushes it on to the project's workers too, which
/// keep the copies' addresses.
pub use weft_broker_client::line::INFRA_STATUS_CHANNEL;

/// Announced with a project's id when one of its connections, its install
/// picks or its instances' values change (`tenant:<tenant>` for a
/// connection shared across projects, which drops every project's copy).
pub use weft_broker_client::line::ACCESS_CHANNEL;

/// How many tenants' routes and projects' levers a process keeps; past
/// that the least recently called go and are read again when called.
const KEPT: usize = 4096;

/// See the module doc.
pub struct Held {
    /// Every public entry of a tenant, by tenant
    /// (`crate::api::signal::read_tenant_routes`).
    pub(crate) routes: Arc<HeldCopy<String, Vec<crate::api::signal::HeldRoute>>>,
    /// The install's domains.
    pub(crate) domains: Arc<HeldCopy<(), Vec<Domain>>>,
    /// A project's worker levers, `None` for a project that is not
    /// registered.
    pub(crate) worker_overrides: Arc<HeldCopy<uuid::Uuid, Option<weft_platform_traits::WorkerOverrides>>>,
    /// Every infra copy of a project and its status.
    pub(crate) infra_status: Arc<HeldCopy<uuid::Uuid, Vec<crate::infra_node::CopyStatus>>>,
    /// The connections this install picked for a project's access nodes.
    pub(crate) picks: Arc<HeldCopy<uuid::Uuid, OfTenant<weft_core::picks::Picks>>>,
    /// What one instance of a project has stored for the fields its
    /// program leaves to instances. A change names only the project, so it
    /// drops every instance's.
    pub(crate) instance_values: Arc<HeldCopy<(uuid::Uuid, weft_core::instance::InstanceId), OfTenant<weft_core::instance::InstanceValues>>>,
}

/// Rows of one project, with the tenant that owns it: a change to one of
/// the tenant's shared connections (which these rows show, by name) is
/// announced for the tenant, not for each project.
pub(crate) struct OfTenant<T> {
    pub tenant: String,
    pub rows: T,
}

impl Held {
    pub fn follow(signals: &PgSignalWatch) -> anyhow::Result<Self> {
        Ok(Self {
            // A tenant with no route is not kept: a made-up tenant name
            // (a scanner's) would otherwise push real ones out.
            routes: HeldCopy::follow(signals, ROUTES_CHANNEL, KEPT, |tenant| Changed::Key(tenant.to_string()), |routes: &Vec<crate::api::signal::HeldRoute>| !routes.is_empty())?,
            domains: HeldCopy::follow(signals, crate::domains::DOMAINS_CHANNEL, 1, |_| Changed::Everything, |_| true)?,
            worker_overrides: HeldCopy::follow(signals, WORKER_SETTINGS_CHANNEL, KEPT, project_of, |_| true)?,
            infra_status: HeldCopy::follow(signals, INFRA_STATUS_CHANNEL, KEPT, project_of, |_| true)?,
            picks: HeldCopy::follow(signals, ACCESS_CHANNEL, KEPT, |payload| access_changed(payload, |project, key: &uuid::Uuid| *key == project), |_| true)?,
            instance_values: HeldCopy::follow(signals, ACCESS_CHANNEL, KEPT, |payload| access_changed(payload, |project, key: &(uuid::Uuid, weft_core::instance::InstanceId)| key.0 == project), |_| true)?,
        })
    }
}

/// What a notice on [`ACCESS_CHANNEL`] drops of a copy of one project's
/// rows (`of_project` says whether a key is the project's): a tenant's
/// shared connections changed for every project of the tenant, a project's
/// own for that project. A payload that names neither drops everything.
// SYNC: the payloads <-> crates/weft-access-store/src/lib.rs (access_notify), crates/weft-broker/src/caller_auth.rs (names), crates/weft-broker/src/line.rs (audience)
fn access_changed<K: 'static, T: 'static>(payload: &str, of_project: fn(uuid::Uuid, &K) -> bool) -> Changed<K, OfTenant<T>> {
    match payload.strip_prefix("tenant:") {
        Some(tenant) => {
            let tenant = tenant.to_string();
            Changed::Matching(Box::new(move |_: &K, held: &OfTenant<T>| held.tenant == tenant))
        }
        None => match payload.parse::<uuid::Uuid>() {
            Ok(project) => Changed::Matching(Box::new(move |key: &K, _: &OfTenant<T>| of_project(project, key))),
            Err(_) => Changed::Everything,
        },
    }
}

/// The project a notification names, or everything when it names none.
fn project_of<V>(payload: &str) -> Changed<uuid::Uuid, V> {
    match payload.parse() {
        Ok(project) => Changed::Key(project),
        Err(_) => Changed::Everything,
    }
}
