//! The two settings the storage package's nodes share: which scope a file
//! is stored in (`scope`), and how long it lives (`ttlDays`). One reading
//! of each, so the nodes that offer them cannot drift apart.

use weft::storage::{KeepTtl, StorageScope};
use weft::{WeftError, WeftResult};

/// The scope a `scope` setting names: `run` or `project`.
pub fn scope_named(name: &str) -> WeftResult<StorageScope> {
    match StorageScope::named(name) {
        Some(scope @ (StorageScope::Run | StorageScope::Project)) => Ok(scope),
        _ => Err(WeftError::Input(format!("`scope` is '{name}'; it is `run` or `project`"))),
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
