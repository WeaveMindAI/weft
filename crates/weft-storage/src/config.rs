//! Every timing/size knob of the storage plane, in ONE place.
//! Nothing here is sprinkled at call sites; tune here only.

use std::time::Duration;

/// The box's HTTP listen port. A FIXED contract shared by the box
/// binary, the Deployment's containerPort, the Service, the tenant
/// ingress backend, and the in-cluster worker URL. One source so
/// those sites can't drift. Deliberately NOT an env var: kubernetes
/// injects `WEFT_STORAGE_PORT=tcp://<ip>:<port>` (Docker-link service
/// discovery) for the `weft-storage` Service into every pod in the
/// namespace, which would collide with a same-named config var.
pub const STORAGE_PORT: u16 = 8080;

/// A file is stored as ordered chunks of at most this size; chunks
/// spill across backing disks as each fills. Smaller = finer
/// evacuation resume granularity + smaller RAM ceiling per in-flight
/// chunk write; larger = fewer files. Tune once real usage exists.
pub const CHUNK_SIZE_BYTES: u64 = 64 * 1024 * 1024;

/// Default backing-disk unit size (the grow/shrink granularity).
/// A tenant's storage profile can override it.
pub const DEFAULT_DISK_UNIT_BYTES: u64 = 10 * 1024 * 1024 * 1024;

/// Default TTL of a KEPT execution-scoped file. Any access bumps the
/// expiry back to now + TTL. `KeepTtl::Default` resolves to this.
pub const DEFAULT_KEEP_TTL: Duration = Duration::from_secs(30 * 24 * 3600);

/// Default lifetime of a presigned URL / download capability when
/// the requester doesn't choose one.
pub const DEFAULT_CAPABILITY_TTL: Duration = Duration::from_secs(15 * 60);

/// Hard ceiling on a requested capability lifetime. A presign is an
/// explicit, EXPIRING artifact; a year-long one would be a durable
/// public link wearing a costume.
pub const MAX_CAPABILITY_TTL: Duration = Duration::from_secs(7 * 24 * 3600);

/// The box is torn down (pod + every backing PVC) when it has been
/// idle this long AND holds zero persistent bytes.
pub const SCALE_TO_ZERO_IDLE: Duration = Duration::from_secs(30 * 60);

/// Grow when the pool's free space falls below this fraction of one
/// disk unit (i.e. "less than ~half a fresh disk left").
pub const GROW_FREE_THRESHOLD_FRACTION: f64 = 0.5;

/// Shrink only when the pool could lose a disk and still keep this
/// fraction of a disk unit free. Strictly above
/// `GROW_FREE_THRESHOLD_FRACTION` so a shrink can never trigger an
/// immediate grow (no oscillation band).
pub const SHRINK_HEADROOM_FRACTION: f64 = 0.6;

/// Shrink requires the free-space condition to hold CONTINUOUSLY for
/// this long (kills the delete-then-redownload thrash case).
pub const SHRINK_DWELL: Duration = Duration::from_secs(30 * 60);

/// Minimum time between two shrinks.
pub const SHRINK_COOLDOWN: Duration = Duration::from_secs(3600);

/// Resize watcher tick interval.
pub const RESIZE_TICK_INTERVAL: Duration = Duration::from_secs(60);

/// Kept-file expiry sweep interval.
pub const EXPIRY_SWEEP_INTERVAL: Duration = Duration::from_secs(60);
