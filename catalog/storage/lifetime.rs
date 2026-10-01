//! The two settings the storage package's nodes share: which scope a file
//! is stored in (`scope`), and how long it lives (`ttl_days`). One reading
//! of each, so the nodes that offer them cannot drift apart.

use weft::storage::{KeepTtl, StorageScope};
use weft::{WeftError, WeftResult};

/// The scope a `scope` setting names: `execution` or `project`.
pub fn scope_named(name: &str) -> WeftResult<StorageScope> {
    match name {
        "execution" => Ok(StorageScope::Execution),
        "project" => Ok(StorageScope::Project),
        other => Err(WeftError::Input(format!("`scope` is '{other}'; it is `execution` or `project`"))),
    }
}

/// The lifetime a `ttl_days` value names: `0` never expires, any other
/// number of days is a window every access renews.
pub fn ttl_of_days(days: u64) -> KeepTtl {
    match days {
        0 => KeepTtl::Never,
        days => KeepTtl::Secs { secs: days * 24 * 3600 },
    }
}
