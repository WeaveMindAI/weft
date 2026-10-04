//! The rows a live call reads on every request, held in this process's
//! memory and read again the moment the database says they changed
//! (`weft_task_store::held_copy`): a tenant's routes, the install's
//! domains, a project's worker levers, which of its infra copies are up.
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

/// Announced with a project's id when one of its infra copies comes, goes
/// or changes status (the infra_node group's trigger).
// SYNC: INFRA_STATUS_CHANNEL <-> crates/weft-dispatcher/src/infra_node.rs (infra_node_status_notify)
pub const INFRA_STATUS_CHANNEL: &str = "weft_infra_status";

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
        })
    }
}

/// The project a notification names, or everything when it names none.
fn project_of(payload: &str) -> Changed<uuid::Uuid> {
    match payload.parse() {
        Ok(project) => Changed::Key(project),
        Err(_) => Changed::Everything,
    }
}
