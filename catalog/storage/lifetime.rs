//! The two settings the storage package's nodes share: which scope a file
//! is stored in (`scope`), and how long it lives (`ttlDays`). One reading
//! of each, so the nodes that offer them cannot drift apart.

use serde::Deserialize;

use weft::storage::{KeepTtl, StorageScope};

/// A `scope` select. Each node's select lists the scopes it offers
/// (UploadSlot alone offers `instance`, the run's own instance).
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    Run,
    Project,
    Instance,
}

impl From<Scope> for StorageScope {
    fn from(scope: Scope) -> Self {
        match scope {
            Scope::Run => StorageScope::Run,
            Scope::Project => StorageScope::Project,
            Scope::Instance => StorageScope::instance(),
        }
    }
}

/// The lifetime a `ttlDays` value names: `0` never expires, any other
/// number of days is a window every access renews.
pub fn ttl_of_days(days: u64) -> KeepTtl {
    match days {
        0 => KeepTtl::Never,
        days => KeepTtl::Secs { secs: days * 24 * 3600 },
    }
}
