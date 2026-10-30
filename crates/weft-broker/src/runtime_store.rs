//! The runtime-file plane (`ctx.storage`): the files a running project
//! reads and writes (assets it fetches, run outputs, scratch). Bytes live
//! in the object-store bucket under the `runtime/` prefix; the file's
//! metadata (mime, name, size, keep/expiry) lives in the `runtime_file`
//! Postgres table. The broker is the single gatekeeper: a worker holds no
//! bucket credentials; the broker verifies the caller (the pure `key` wall),
//! charges the tenant's byte quota at reservation time, and mints presigned
//! URLs whose EXACT byte size is signed in, so the bucket itself enforces
//! every reservation. Bytes never transit the broker: uploads are multipart
//! (resumable, one signed URL per part) direct to the bucket.
//!
//! Why Postgres holds the metadata (not a sibling object next to each blob):
//! every interesting question (list a scope, a tenant's usage, am I over
//! quota, wipe a run's scratch) becomes one indexed query instead of a
//! bucket scan, so the plane scales with the number of tenants, not with the
//! number of stored files. The bucket holds ONLY opaque bytes.
//!
//! This is the runtime-file plane: the broker serves it directly. A
//! versioned-projects plane (pack a folder into a content tree, a version
//! graph) is built as a separate plane around this broker; this module is
//! only the runtime-file plane.

use std::sync::Arc;

use anyhow::{Context, Result};
use weft_task_store::db::{self, Client, FromRow, Pool, Row, Transaction};

use weft_core::storage::key::{CallerAuth, ParsedKey};
use weft_core::storage::{KeepTtl, PartAsk, PresignedPart, StorageScope, StoredFileMeta};
use weft_platform_traits::{CopyProgress, GetLink, ObjectStore, PresignAudience};

use crate::entitlement::{lock_tenant_storage, EntitlementSource};

/// Default TTL of a kept file or retired source asset (30 days). Access bumps
/// the expiry back to now + TTL, so an actively-used survivor never expires.
/// `KeepTtl::Default` resolves to this; defined beside `KeepTtl` in core.
pub use weft_core::storage::DEFAULT_KEEP_TTL_SECS;

/// How long an UN-KEPT completed run file lingers after its run terminates
/// before it expires. The terminate sweep stamps
/// `expires_at_unix = now + this` instead of deleting outright, so a user can
/// still open the files list and download a run's output right after the run
/// ends. It reads as gone once the deadline passes and the expiry loop
/// removes it then, and the stamped expiry is what the file lists surface as
/// the remaining lifetime.
pub const RUN_LINGER_TTL_SECS: i64 = 5 * 60;

/// How long the link to upload one part works: a PUT may start until then.
/// Every part link is signed for this ([`RuntimeStore::reserve_parts`],
/// [`RuntimeStore::resume_upload`]), each signing stamps the upload's
/// progress, and a pending upload is abandoned only past it
/// ([`abandoned_at`]).
pub const PART_LINK_SECS: u64 = 15 * 60;

pub use weft_core::storage::DEFAULT_LINK_SECS;

pub use weft_platform_traits::object_store::MAX_PRESIGN_TTL_SECS;

/// How long one part may take to land once its link's last moment to start
/// passed ([`PART_LINK_SECS`]): a 'pending' upload with no progress (no part
/// link signed, no part reported) for both is abandoned, and the expiry loop
/// reaps it (aborts its multipart upload, deletes its row, frees its quota
/// reservation). A PUT still running past it is cut off with the upload.
pub const PENDING_RESERVE_GRACE_SECS: i64 = 60 * 60;

/// Default part size for multipart uploads: 8 MiB. Every part is exactly this
/// size except the final one (which may be smaller). Above the 5 MiB floor
/// buckets impose on non-final parts, a 256 KiB multiple, and small enough
/// that a retry re-sends little. For a KNOWN total size the part size scales
/// up so the plan stays under the 10,000-part ceiling.
pub const DEFAULT_PART_SIZE_BYTES: u64 = 8 * 1024 * 1024;

/// Hard ceiling on part numbers (the S3 multipart limit).
// SYNC: MAX_PARTS <-> packages/weft-connect/src/server/passthrough.ts LARGEST_PART_BYTES
const MAX_PARTS: u64 = 10_000;

/// How much of a file's start is read to tell what it is
/// ([`weft_core::storage::sniff_mime`]).
const SNIFF_BYTES: u64 = 4096;

/// The piece a file is read back in to be hashed.
const HASH_PIECE_BYTES: u64 = 8 * 1024 * 1024;

/// The largest file the store takes: 5 TiB, the largest object a bucket
/// holds. Its parts are then [`MAX_PARTS`] of 525 MiB.
pub const LARGEST_FILE_BYTES: u64 = 5 << 40;

/// Refuse a declared size past [`LARGEST_FILE_BYTES`], before anything is
/// reserved or opened.
fn refuse_too_large(declared_size: Option<u64>) -> StoreResult<()> {
    match declared_size {
        Some(size) if size > LARGEST_FILE_BYTES => Err(RuntimeStoreError::Invalid(format!(
            "a stored file weighs at most {LARGEST_FILE_BYTES} bytes (5 TiB), and this one says {size}"
        ))),
        _ => Ok(()),
    }
}

/// The part size for an upload: the default, scaled up (in whole MiB) when a
/// known total would otherwise exceed the part-count ceiling.
// SYNC: part_size_for <-> packages/weft-connect/src/server/passthrough.ts LARGEST_PART_BYTES
fn part_size_for(declared_size: Option<u64>) -> u64 {
    match declared_size {
        Some(total) if total > DEFAULT_PART_SIZE_BYTES * MAX_PARTS => {
            let mib = 1024 * 1024;
            // Smallest whole-MiB part size that fits `total` in MAX_PARTS parts.
            total.div_ceil(MAX_PARTS).div_ceil(mib) * mib
        }
        _ => DEFAULT_PART_SIZE_BYTES,
    }
}

/// The bucket prefix every runtime-file object lives under. The version
/// plane uses `chunks/` + `trees/`; the runtime plane uses `runtime/`, so
/// the two planes share one bucket without ever colliding.
const RUNTIME_PREFIX: &str = "runtime/";

/// The bucket object key for a canonical storage key: `runtime/<tenant>/...`.
/// The scope a key lives in: everything before its id. What an
/// identity is unique within (the same message fetched by two projects
/// is two files), and the expression the identity index is built on.
fn scope_prefix(key: &str) -> &str {
    key.rsplit_once('/').map(|(prefix, _)| prefix).unwrap_or(key)
}

fn object_key(key: &str) -> String {
    format!("{RUNTIME_PREFIX}{key}")
}

/// When a link asked for at `now` to live `ttl` seconds is signed, and how
/// long it works from then.
///
/// Links are signed at the start of a window as long as their life (so
/// the same file asked for twice within it gets the same link), and work
/// a window longer than asked, so whenever in the window one is asked
/// for it still works at least `ttl` from then. A life past half the
/// longest a signature allows gets a window that keeps the whole within
/// it; the longest life itself is signed at the moment it is asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LinkWindow {
    /// Unix seconds.
    pub signed_at: i64,
    /// Seconds after `signed_at`.
    pub works_for: u64,
}

impl LinkWindow {
    /// Unix seconds at which the link stops working.
    pub fn until(&self) -> i64 {
        self.signed_at + self.works_for as i64
    }
}

pub fn link_window(now: i64, ttl: u64) -> LinkWindow {
    let window = ttl.min(MAX_PRESIGN_TTL_SECS.saturating_sub(ttl)) as i64;
    if window == 0 {
        return LinkWindow { signed_at: now, works_for: ttl };
    }
    LinkWindow { signed_at: now - now.rem_euclid(window), works_for: ttl + window as u64 }
}

/// The loop that removes expired files ([`RuntimeStore::expire`]).
pub const FILE_EXPIRY_LOOP: &str = "runtime_file_expiry";

/// Announced when a write sets a file's expiry sooner than the expiry
/// loop may next look (as far as this broker copy knows): the loop looks
/// at once and books its next look at the soonest expiry.
pub const FILE_EXPIRY_CHANNEL: &str = "weft_file_expiry";

/// What wakes the expiry loop.
pub static FILE_EXPIRY_WAKE: &[weft_task_store::drain::WakeOn] = &[weft_task_store::drain::WakeOn::any(FILE_EXPIRY_CHANNEL)];

/// What the expiry loop does, in a person's words.
pub const FILE_EXPIRY_LABEL: &str = "expire stored files";

/// The pauses between the expiry loop's tries of one row's reap: three
/// tries in all, after which the row stays marked and the pass goes on. A
/// reap on a person's upload is tried once: it never waits on these.
const REAP_RETRY_PAUSES: [std::time::Duration; 2] = [std::time::Duration::from_millis(500), std::time::Duration::from_secs(2)];

/// SQL: the row is fenced for removal ('reaping') and the call that holds
/// it is taken to have died at `now_param` (a bind holding the store's
/// clock): its claim lapsed ([`CLAIM_LEASE_SECS`]) because its reap
/// crashed. A claim still running is some call's reap in progress, and
/// nothing else takes it. A marked row holds no claim, and is a rule of
/// its own ([`MARKED`]).
fn reap_lapsed(now_param: &str) -> String {
    format!("(status = 'reaping' AND progressed_at_unix > 0 AND progressed_at_unix < {now_param}::BIGINT - {CLAIM_LEASE_SECS})")
}

/// SQL condition: the row is marked, a reap the bucket refused that
/// nobody holds now ([`RuntimeStore::mark_reap_failed`]): no claim, so no
/// look is booked for it and the expiry loop never retries it on its own.
/// It is shown in `weft status`, and retried, once each time, by exactly
/// these: a delete of its key ([`RuntimeStore::delete`]), a wipe of a
/// prefix holding it ([`RuntimeStore::wipe_prefix`]), the end-of-run sweep
/// of its run ([`RuntimeStore::sweep_run`]), and a begin (or a slot's
/// check) of its tenant refused for space ([`RuntimeStore::reap_marked`]).
const MARKED: &str = "(status = 'reaping' AND reap_error IS NOT NULL AND progressed_at_unix = 0)";

/// SQL: when the call holding an in-flight row ('pending', 'completing'
/// or 'reaping') is taken to have died: a pending upload once the last
/// part link it was handed can no longer start a PUT ([`PART_LINK_SECS`])
/// and one more part's time to land has passed
/// ([`PENDING_RESERVE_GRACE_SECS`]); a claimed completion or reap once its
/// claim is [`CLAIM_LEASE_SECS`] old. The expiry loop ends every row past
/// it and books its next look at the soonest one ahead
/// ([`RuntimeStore::expire`]). A marked row holds no claim ([`MARKED`]):
/// it is never abandoned, only waiting for something to need it.
fn abandoned_at() -> String {
    format!(
        "progressed_at_unix + CASE status WHEN 'pending' THEN {} ELSE {CLAIM_LEASE_SECS} END",
        PART_LINK_SECS as i64 + PENDING_RESERVE_GRACE_SECS
    )
}

/// SQL condition: the row is in flight, a call holds it, and that call
/// is taken to have died by `now_param` (a bind holding the store's clock,
/// [`abandoned_at`]).
fn abandoned(now_param: &str) -> String {
    format!("({HELD} AND {} < {now_param})", abandoned_at())
}

/// SQL condition: the row is in flight and some call holds it, every row
/// the expiry loop books a look for. A marked row has no claim
/// ([`MARKED`]).
const HELD: &str = "(status IN ('pending', 'completing', 'reaping') AND progressed_at_unix > 0)";

/// SQL: fence row `$1` for removal ('reaping'), claimed at `$2` (the
/// store's clock), where `condition` (more `AND` clauses, or nothing) still
/// holds. Every flip to 'reaping' goes through here, so every reap holds a
/// claim ([`reap_lapsed`]).
fn fence_sql(condition: &str) -> String {
    format!("UPDATE runtime_file SET status = 'reaping', progressed_at_unix = $2 WHERE key = $1 {condition}")
}

/// SQL condition: the row has not expired by `now_param` (a bind holding
/// the store's clock). Every read of a file a person or a node sees
/// carries it, so a file is gone the moment it expires whenever the
/// expiry loop gets to it. Asset reads leave it out: an asset's life
/// follows the sources that reference it, and publishing brings it back.
fn unexpired(now_param: &str) -> String {
    format!("(expires_at_unix IS NULL OR expires_at_unix >= {now_param})")
}

/// A key's upload row in any status, as [`PendingUpload`] reads it; callers
/// add the status they accept.
const PENDING_ROW_SELECT_ANY: &str = "SELECT status, tenant_id, mime_type, filename, keep_ttl_secs, upload_id, part_size, declared_size, replaces, \
     slot_accepts, receives FROM runtime_file WHERE key = $1";

/// How long a claim on a row ('completing' or 'reaping', stamped in
/// `progressed_at_unix`) holds before another call takes the row over.
/// Only a claim whose process crashed or whose request was dropped
/// mid-way stays that long (a failed reap gives its claim back at once,
/// [`MARKED`]); a live one finishes in a few bucket calls, or, filled by an in-bucket
/// copy, renews its claim each time a piece of the copy lands
/// ([`CopyClaim`]). A completion driven early is harmless (two drives of one
/// upload agree), so for it this only spares work a live completion is
/// doing; a reap is never run twice at once.
pub const CLAIM_LEASE_SECS: i64 = 5 * 60;

/// The statement deleting the row at key `$1` that also matches
/// `condition`, for every path that ends an upload without completing
/// it. A replacement's multipart writes onto the replaced file's own
/// object, so by the time its row goes (an abort after a failed
/// completion, a reap) the file's bytes may already be the new ones:
/// the file moves to its next version either way, so a value read
/// before can never pass for the content now there.
/// Answers one row: whether the row deleted carried a failed reap's mark
/// (`reap_error`).
fn delete_upload_row(condition: &str) -> String {
    format!(
        "WITH gone AS (DELETE FROM runtime_file WHERE key = $1 {condition} RETURNING replaces, reap_error), \
         moved AS (UPDATE runtime_file SET version = version + 1 \
                   WHERE key IN (SELECT replaces FROM gone WHERE replaces IS NOT NULL)) \
         SELECT EXISTS (SELECT 1 FROM gone WHERE reap_error IS NOT NULL)"
    )
}

/// A `LIKE` pattern matching every key under `prefix`. `\` escapes any LIKE
/// metachar in the prefix (keys are tenant/scope/owner/id of validated segments,
/// so this is belt + suspenders, never a real escape need).
fn like_prefix(prefix: &str) -> String {
    format!("{}%", prefix.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_"))
}

/// Failure modes of a runtime-store operation, mapped to HTTP status by the
/// handler. Distinct so the worker can tell a real denial / not-found from a
/// quota rejection.
#[derive(Debug, thiserror::Error)]
pub enum RuntimeStoreError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("denied: {0}")]
    Denied(String),
    #[error("invalid: {0}")]
    Invalid(String),
    #[error("quota exceeded: {0}")]
    QuotaExceeded(String),
    /// The content-addressed key already exists (an asset begin raced another
    /// sync, or the content is simply already uploaded). Distinguishable so
    /// the sync can treat "already active" as the idempotent success it is.
    #[error("conflict: {0}")]
    Conflict(String),
    /// A replacement named the version of the file it was based on, and
    /// the file has moved on since: the writer re-reads and tries again.
    #[error("stale: {0}")]
    Stale(String),
    /// A completion has claimed the upload and has not landed yet: the
    /// upload cannot change, and asking `complete` again is the way on. A
    /// wipe that left only rows a live claim holds answers it too: they
    /// clear by themselves, so asking again shortly is the way on.
    /// The HTTP layer marks it (`x-weft-completing`) so a client tells it
    /// apart from every other conflict without reading the text.
    #[error("completing: {0}")]
    Completing(String),
    /// A claimed completion whose outcome the bucket cannot account for:
    /// the upload was ended and its reservation freed. Final; re-upload.
    #[error("lost: {0}")]
    Lost(String),
    /// An upload into a content-addressed key (an asset, whose id is the
    /// sha256 of its bytes) assembled bytes that hash to something else:
    /// the upload was ended and its key freed. Final; upload the right
    /// bytes.
    #[error("not its content: {0}")]
    NotItsContent(String),
    /// A slot has no upload to receive: nothing came through it, or its
    /// upload ended with it. Said in those words, never as a missing
    /// file key.
    #[error("no upload: {0}")]
    NoUpload(String),
    /// A removal (a delete, a wipe) left rows the storage bucket refused to
    /// remove, named by key (marked, shown in `weft status`; the bucket's
    /// words stay in the log). Waiting does not clear it: removing again
    /// tries again.
    #[error("removal incomplete: {0}")]
    RemovalIncomplete(String),
    /// A failure before the op wrote anything (its first connection, or
    /// a read ahead of its first write), so sending the same call again
    /// does it once: a connection that could not be had answers that
    /// nothing was done, and the caller waits and sends it again. Said
    /// only where the op knows it has written nothing yet; an op that
    /// calls another after writing of its own turns that one's into
    /// [`Self::Other`] ([`Self::after_writes`]).
    #[error(transparent)]
    BeforeWrites(anyhow::Error),
    /// Any other failure, which may come after the op committed part of
    /// its work (a delete fences its row, removes the object, then drops
    /// the row), so it never says nothing was done.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl RuntimeStoreError {
    /// This error, raised by a step that came after writes of the caller's
    /// own: a [`Self::BeforeWrites`] of that step is no longer true of the
    /// whole op.
    fn after_writes(self) -> Self {
        match self {
            Self::BeforeWrites(e) => Self::Other(e),
            e => e,
        }
    }
}

/// What one upload IS, independent of transport: scope + metadata + declared
/// size + (for the content-addressed asset scope) the explicit hash id.
/// Collapses `begin_upload`'s parameter list; both the worker data path and
/// the control-plane admin path build one of these from their wire envelope.
#[derive(Debug, Clone, Copy)]
pub struct UploadSpec<'a> {
    pub scope: &'a StorageScope,
    pub mime: &'a str,
    pub filename: &'a str,
    pub keep: Option<KeepTtl>,
    pub declared_size: Option<u64>,
    /// The sha256 id for `StorageScope::Asset` (required there, refused
    /// elsewhere); every other scope mints a uuid.
    pub content_hash: Option<&'a str>,
    /// What the file is a copy OF (a message id, a document id), so a
    /// second begin naming the same identity in the same scope answers
    /// the file already there. Refused on the asset scope, which is
    /// content-addressed already.
    pub identity: Option<&'a str>,
    /// The slot a page uploads through, when it is one
    /// ([`weft_core::storage::slot`]): the upload is the slot's one, and
    /// once it lands it waits ('uploaded') for the run that receives it.
    pub slot: Option<SlotUpload<'a>>,
    /// A file copied from another install (`weft migrate files`), which
    /// keeps the id, version and birth it had there: a stored-file value
    /// names a file by key and version, so the values a person carries
    /// over still name it. Never on the asset scope, whose id is its
    /// content.
    pub imported: Option<ImportedFile<'a>>,
}

/// What a file copied from another install keeps ([`UploadSpec::imported`]):
/// its key there, with this install's tenant in it.
#[derive(Debug, Clone, Copy)]
pub struct ImportedFile<'a> {
    pub key: &'a weft_core::storage::key::ParsedKey,
    pub version: u64,
    pub created_at_unix: i64,
}

/// The slot an upload goes through: its id, when it ends (an upload
/// nobody received by then goes), the most it takes, the type it accepts
/// (the slot's `accepts`, which the landed upload's first bytes are held
/// to), and the sha256 the page states for the file it sends, when it
/// states one.
#[derive(Debug, Clone, Copy)]
pub struct SlotUpload<'a> {
    pub sid: &'a str,
    pub ends_unix: i64,
    pub max_bytes: u64,
    pub accepts: &'a str,
    pub sha256: Option<&'a str>,
}

impl SlotUpload<'_> {
    /// Refuse a file heavier than the slot takes.
    fn refuse_too_heavy(&self, size: u64) -> StoreResult<()> {
        if size > self.max_bytes {
            return Err(RuntimeStoreError::QuotaExceeded(format!(
                "the file weighs {size} bytes, and this upload takes at most {}",
                self.max_bytes
            )));
        }
        Ok(())
    }
}

/// What a begin answered: a fresh reservation to upload into, or (asset
/// scope only) the content already stored ACTIVE under its hash, in which
/// case there is nothing to transfer and `key` is the existing file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BeginUpload {
    Ready { key: String, part_size: u64 },
    AlreadyStored { key: String },
    /// This content is already part way up, under this key: carry on with
    /// it ([`RuntimeStore::resume_upload`]) instead of starting again.
    ///
    /// Answered rather than refused because it is SAFE to carry on, even
    /// while another uploader is doing the same: the key is the content's
    /// hash, so both hold identical bytes, and a part is reserved by
    /// number, so naming part 3 twice reserves it once and both writers
    /// put the same bytes in it. Refusing instead (which is what this
    /// used to do) left the second publish of the same asset failing for
    /// as long as the first one's leftovers sat there, up to the hour the
    /// sweep takes to clear an abandoned upload.
    Resume { key: String, part_size: u64 },
}

/// Internal outcome of the begin's reservation transaction.
enum Reserved {
    Fresh,
    /// The file is already there, under this key: a content-addressed
    /// begin whose content is active (the minted key), or an identified
    /// begin whose identity the scope holds (that file's key).
    AlreadyActive(String),
    /// A content-addressed key whose upload is part way up, with the part
    /// size it was begun with.
    Pending(String, u64),
}

type StoreResult<T> = Result<T, RuntimeStoreError>;

fn validate_stream_layout(parts: &std::collections::BTreeMap<i32, u64>, part_size: u64) -> StoreResult<()> {
    for (index, (number, size)) in parts.iter().enumerate() {
        if *number != index as i32 + 1 {
            return Err(RuntimeStoreError::Invalid("stream parts must form a contiguous sequence starting at 1".into()));
        }
        if index + 1 < parts.len() && *size != part_size {
            return Err(RuntimeStoreError::Invalid(format!("stream part {number} is short; only the final part can be short")));
        }
    }
    Ok(())
}

#[cfg(test)]
mod link_window_tests {
    use super::{link_window, MAX_PRESIGN_TTL_SECS};

    /// Every ask within one window gets the same signing moment, so the
    /// same link; the next window a new one. Whenever it is asked, a link
    /// works at least as long as asked from then.
    #[test]
    fn a_window_gives_one_link_that_outlives_every_ask_in_it() {
        let start = 1_700_000_100 / 900 * 900;
        for now in [start, start + 1, start + 899] {
            let w = link_window(now, 900);
            assert_eq!(w.signed_at, start, "{now}");
            assert_eq!(w.works_for, 1800);
            assert!(w.until() >= now + 900, "{now}");
        }
        assert_eq!(link_window(start + 900, 900).signed_at, start + 900);
    }

    /// A long life gets a window that keeps the whole under the longest a
    /// signature allows; the longest life is signed at the moment.
    #[test]
    fn a_long_life_stays_within_what_a_signature_allows() {
        let six_days = 6 * 24 * 3600;
        let w = link_window(1_700_000_000, six_days);
        assert_eq!(w.works_for, MAX_PRESIGN_TTL_SECS);
        assert!(w.until() >= 1_700_000_000 + six_days as i64);
        let longest = link_window(1_700_000_123, MAX_PRESIGN_TTL_SECS);
        assert_eq!((longest.signed_at, longest.works_for), (1_700_000_123, MAX_PRESIGN_TTL_SECS));
    }

    /// Just past half the longest life, the window shrinks to what is
    /// left: the link works exactly the longest a signature allows, and
    /// still as long as asked from any moment in its window.
    #[test]
    fn a_life_just_past_half_the_longest_fills_it_exactly() {
        let ttl = MAX_PRESIGN_TTL_SECS / 2 + 1;
        let window = (MAX_PRESIGN_TTL_SECS - ttl) as i64;
        let start = 1_700_000_000 / window * window;
        for now in [start, start + window - 1] {
            let w = link_window(now, ttl);
            assert_eq!((w.signed_at, w.works_for), (start, MAX_PRESIGN_TTL_SECS), "{now}");
            assert!(w.until() >= now + ttl as i64, "{now}");
        }
    }
}

#[cfg(test)]
mod stream_layout_tests {
    use super::*;

    #[test]
    fn only_a_contiguous_prefix_with_a_short_final_part_is_valid() {
        let parts = |items: &[(i32, u64)]| items.iter().copied().collect();
        assert!(validate_stream_layout(&parts(&[(1, 10), (2, 3)]), 10).is_ok());
        assert!(validate_stream_layout(&parts(&[(2, 10)]), 10).is_err());
        assert!(validate_stream_layout(&parts(&[(1, 3), (2, 10)]), 10).is_err());
        assert!(validate_stream_layout(&parts(&[(1, 10), (3, 10)]), 10).is_err());
    }
}

/// The columns a [`FileRow`] reads, in every SELECT and RETURNING that
/// builds one, so a column added to the file's metadata is added once.
const FILE_ROW_COLUMNS: &str =
    "key, mime_type, filename, size_bytes, keep, expires_at_unix, keep_ttl_secs, created_at_unix, version";

/// The runtime-file plane's schema. The broker owns it (it is the only
/// reader/writer); the boot applies it with every other group, in one pass
/// over the install's one list (`weft_runtime::schema`). The canonical CREATE
/// lives here, edited in place; an existing database is carried to it by
/// a migration written with `./setup.sh --migration <name>` (the whole
/// contract is `weft_task_store::schema_guard`'s header).
pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "runtime_file",
    tables: &["runtime_file", "runtime_file_part", "asset_reference"],
    ddl: &[
        r#"
        -- One row per runtime file. `key` is the canonical
        -- `<tenant>/<scope>/<owner>/<id>` string (also the bucket object key
        -- under the `runtime/` prefix). `tenant_id` is the first key segment,
        -- denormalized so per-tenant usage + listing are indexed lookups.
        CREATE TABLE IF NOT EXISTS runtime_file (
            key                TEXT PRIMARY KEY,
            tenant_id          TEXT NOT NULL,
            mime_type          TEXT NOT NULL,
            filename           TEXT NOT NULL,
            size_bytes         BIGINT NOT NULL,
            -- Upload lifecycle. 'pending': the row was reserved at upload begin,
            -- BEFORE any bytes; it carries the multipart resume state below and
            -- its reserved_bytes are already charged against the tenant's byte
            -- quota. 'active': the upload completed (bytes assembled + sized).
            -- The row exists FIRST and bytes land SECOND, so the bucket never
            -- holds an object with no row; a 'pending' row whose upload never
            -- completed is reaped by the row-driven sweeps (which also abort its
            -- multipart upload). 'completing': a completion claimed the
            -- upload (its parts are final) and is having the bucket assemble
            -- the object; nothing may abort it, change its parts, or remove
            -- the file it replaces until it lands, and progressed_at_unix
            -- holds the claim time, from which the expiry loop drives a
            -- completion its caller abandoned. 'reaping': a call fenced the row for removal
            -- (writers and reads are locked out; the bucket state goes next,
            -- then the row), and progressed_at_unix holds the claim time; a
            -- reap that crashed or failed leaves the row in 'reaping', and
            -- once its claim lapsed (or a failed reap gave it back) the
            -- expiry loop or a wipe takes it over. Only 'active' rows
            -- appear in user listings / gets; ALL statuses count toward the
            -- byte quota ('active' by size_bytes, others by reserved_bytes),
            -- which is what makes an in-flight upload unable to blow past the
            -- cap. 'uploaded': a page's upload through a slot (upload_slot)
            -- landed and waits for the run that receives it; nothing reads
            -- it until then, and it goes when its slot ends (its expiry).
            status             TEXT NOT NULL DEFAULT 'active',
            -- True iff this run-scoped file is flagged to survive the
            -- terminate sweep. Always false in the other scopes, which
            -- outlive runs without a flag. Set at begin; a PENDING kept
            -- row is still sweepable (only kept ACTIVE files are spared).
            keep               BOOLEAN NOT NULL DEFAULT FALSE,
            -- Unix seconds at which the file expires (access-bumped), in
            -- any scope: a kept run file, a project/shared/instance
            -- file stored with a lifetime, a retired asset, an upload
            -- nobody received (its slot's end, set at begin). NULL = no
            -- expiry. Otherwise set at complete, never on a pending row.
            expires_at_unix    BIGINT,
            -- The file's TTL so an access can recompute expiry. NULL when
            -- there is no expiry.
            keep_ttl_secs      BIGINT,
            created_at_unix    BIGINT NOT NULL,
            -- Multipart upload state, present on a 'pending' row (NULL once
            -- active). upload_id is the bucket's multipart handle (the resume
            -- handle); part_size is the fixed size of every non-final part.
            upload_id          TEXT,
            part_size          BIGINT,
            -- The total size declared at begin, NULL for an unknown-length
            -- stream. A declared upload's parts must slice exactly to it.
            declared_size      BIGINT,
            -- The bytes CHARGED against the tenant quota for this in-flight
            -- upload: the declared total (known size) or the running sum of
            -- reserved parts (stream). Every reserved part's exact size is
            -- signed into its URL, so the bucket enforces this number.
            reserved_bytes     BIGINT NOT NULL DEFAULT 0,
            -- Progress clock for the abandoned-pending reap: bumped whenever
            -- part links are signed or a part is reported, so a long
            -- multi-part upload that is still moving is never reaped
            -- mid-flight. On a 'completing' or 'reaping' row, the time of its
            -- claim; 0 on a 'reaping' row whose reap failed, which holds no
            -- claim until something needs it removed.
            progressed_at_unix BIGINT NOT NULL DEFAULT 0,
            -- What the file is a copy OF, when the writer said (a message
            -- id, a document id at a provider): a begin naming an identity
            -- the scope already holds answers that file instead of minting
            -- another. NULL for a file that is its own thing.
            identity           TEXT,
            -- On a 'pending' row only: the key of the ACTIVE file this
            -- upload will overwrite (`StorageHandle::replace`). The upload
            -- writes straight to that file's object, which the bucket swaps
            -- in whole when the multipart completes, so readers see the old
            -- bytes until then and the new ones after. Completion folds the
            -- new size into that file's row and deletes this one. NULL for
            -- an upload that makes a new file.
            replaces           TEXT,
            -- On an 'active' row only: the key of the last replacement
            -- upload folded into this file (that upload's own row is gone
            -- once folded). A retried complete of that upload finds the
            -- file here and answers with it. NULL until a replacement lands.
            last_replacement   TEXT,
            -- Which write of this file's content the row describes: 1 when
            -- the file is made, and one more at the end of every
            -- replacement (folded, aborted or reaped alike, since the
            -- bytes may have been swapped either way). A key is never
            -- reused, so (key, version) names one content. Every stored
            -- file value carries it; a replacement that names the version
            -- it read is refused when the file has moved on since.
            version            BIGINT NOT NULL DEFAULT 1,
            -- On a 'reaping' row only: why a reap of it failed. While any
            -- row is marked, a failure naming them is written down for
            -- `weft status`. A failed reap gives its claim back, and no
            -- timer retries it: a removal, a wipe, its run's end or its
            -- tenant running short of space takes it again. The mark
            -- stays while that reap runs, so its record clears only once
            -- the row is gone.
            reap_error         TEXT,
            -- The latest moment a link handed out for this file works
            -- until. A link is a promise the bytes stay fetchable for its
            -- life, so no write makes the file expire before it. NULL
            -- until a link is handed out.
            links_until_unix   BIGINT,
            -- On a page's upload through a slot: the slot's own id. One
            -- upload per slot (the index below), received once.
            upload_slot        TEXT,
            -- On a page's upload through a slot: the type its slot accepts
            -- (`Image`, `Video | Audio`), which the upload's first bytes
            -- are held to before it waits ('uploaded'), whichever call
            -- lands it.
            slot_accepts       TEXT,
            -- The run that received the slot's upload (status 'uploaded'
            -- until then): the same run receiving it again gets the same
            -- file, another run is refused. Also on a run's copy of the
            -- upload while it is made (see `receives`).
            slot_received_by   TEXT,
            -- On a run's copy of a slot's upload while the bucket makes it
            -- ('completing'): the key of the upload it copies. No removal
            -- takes that upload until the copy is received or ended. NULL
            -- once received.
            receives           TEXT,
            -- The sha256 of the file's bytes, hex, when weft read them back
            -- itself (an upload through a slot, once it landed). Only such
            -- a hash is ever matched: a page sending the same content again
            -- skips the upload.
            content_sha256     TEXT
        );
        -- One upload per slot. A row being removed no longer holds its
        -- slot: a run receiving a run-scoped upload makes the run's own
        -- row as the slot's row goes.
        CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_file_upload_slot
            ON runtime_file(upload_slot) WHERE upload_slot IS NOT NULL AND status <> 'reaping';
        -- "Is a run copying this upload?": what every removal asks.
        CREATE INDEX IF NOT EXISTS idx_runtime_file_receives
            ON runtime_file(receives) WHERE receives IS NOT NULL;
        -- The content a page's upload may skip sending, per tenant.
        CREATE INDEX IF NOT EXISTS idx_runtime_file_content
            ON runtime_file(tenant_id, content_sha256) WHERE content_sha256 IS NOT NULL;
        -- One replacement in flight per file: its multipart writes onto
        -- the file's own object, so a second one would race it there.
        CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_file_one_replacement
            ON runtime_file(replaces) WHERE replaces IS NOT NULL;
        -- A replacement folds into exactly one file, and a key is never
        -- reused: the lookup a retried complete makes.
        CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_file_last_replacement
            ON runtime_file(last_replacement) WHERE last_replacement IS NOT NULL;
        -- One file per identity per scope (the key minus its id): the
        -- lookup begin makes, and the guarantee that two runs fetching the
        -- same thing at once cannot both land.
        CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_file_identity
            ON runtime_file((regexp_replace(key, '/[^/]+$', '')), identity)
            WHERE identity IS NOT NULL;
        -- One row per RESERVED part of a pending upload: the exact size signed
        -- into its URL, and the etag once the caller reports it landed (NULL =
        -- reserved but not yet landed, i.e. what resume re-presigns). Rows are
        -- deleted at complete; ON DELETE CASCADE ties them to the file row for
        -- every sweep/abort path.
        CREATE TABLE IF NOT EXISTS runtime_file_part (
            key           TEXT NOT NULL REFERENCES runtime_file(key) ON DELETE CASCADE,
            part_number   INT NOT NULL,
            size_bytes    BIGINT NOT NULL,
            etag          TEXT,
            PRIMARY KEY (key, part_number)
        );
        -- Per-tenant usage + listing range over the key prefix; the index on
        -- (tenant_id, key) serves the tenant-usage sum and the prefix list.
        CREATE INDEX IF NOT EXISTS idx_runtime_file_tenant ON runtime_file(tenant_id);
        -- The expiry loop ranges files by their expiry.
        CREATE INDEX IF NOT EXISTS idx_runtime_file_expiry
            ON runtime_file(expires_at_unix) WHERE expires_at_unix IS NOT NULL;
        -- Which projects reference which of the tenant's assets
        -- (`<tenant>/asset/<sha256>`): one row per (project, asset), the
        -- project's whole set replaced at every publish. An asset is one
        -- file per tenant whichever projects hold its content, so it lives
        -- while ANY project of the tenant has a row for it (no expiry), and
        -- the publish that removes its last row starts its countdown. No
        -- foreign key to `runtime_file`: a row records what the project's
        -- sources and versions name, which stays true when the file itself
        -- is gone (the next publish reports it missing).
        CREATE TABLE IF NOT EXISTS asset_reference (
            tenant_id   TEXT NOT NULL,
            project_id  TEXT NOT NULL,
            key         TEXT NOT NULL,
            PRIMARY KEY (tenant_id, project_id, key)
        );
        -- "Does any project still reference this asset?": the question a
        -- publish asks of every asset it stops referencing.
        CREATE INDEX IF NOT EXISTS idx_asset_reference_key ON asset_reference(key);
        "#,
    ],
    seed: &[],
    data_steps: &[],
};

/// A clock seam so the expiry math is testable without wall-clock. The broker
/// already carries a `weft_platform_traits::Clock`; the store takes one.
pub use weft_platform_traits::Clock;

/// One slot a run receives the upload of ([`RuntimeStore::claim_uploads`]):
/// the slot as the call sent it, what its token says, and the type of the
/// place its file goes to.
#[derive(Debug, Clone, Copy)]
pub struct ClaimedSlot<'a> {
    pub slot: &'a str,
    pub claims: &'a weft_core::storage::slot::UploadSlotClaims,
    pub fits: &'a weft_core::weft_type::WeftType,
}

/// `message`, said of the slot `slot` (as the call sent it): a call naming
/// several says which one was refused.
pub fn of_slot_message(slot: &str, message: &str) -> String {
    format!("upload slot '{slot}': {message}")
}

/// A refusal of the slot `slot` ([`of_slot_message`]). A missing file
/// names its key, and an internal error is never shown, so both stay as
/// they are.
fn of_slot(slot: &str, e: RuntimeStoreError) -> RuntimeStoreError {
    let say = |message: String| of_slot_message(slot, &message);
    match e {
        RuntimeStoreError::Denied(m) => RuntimeStoreError::Denied(say(m)),
        RuntimeStoreError::Invalid(m) => RuntimeStoreError::Invalid(say(m)),
        RuntimeStoreError::QuotaExceeded(m) => RuntimeStoreError::QuotaExceeded(say(m)),
        RuntimeStoreError::Conflict(m) => RuntimeStoreError::Conflict(say(m)),
        RuntimeStoreError::Stale(m) => RuntimeStoreError::Stale(say(m)),
        RuntimeStoreError::Completing(m) => RuntimeStoreError::Completing(say(m)),
        RuntimeStoreError::Lost(m) => RuntimeStoreError::Lost(say(m)),
        RuntimeStoreError::NotItsContent(m) => RuntimeStoreError::NotItsContent(say(m)),
        RuntimeStoreError::NoUpload(m) => RuntimeStoreError::NoUpload(say(m)),
        RuntimeStoreError::RemovalIncomplete(m) => RuntimeStoreError::RemovalIncomplete(say(m)),
        e @ (RuntimeStoreError::NotFound(_) | RuntimeStoreError::BeforeWrites(_) | RuntimeStoreError::Other(_)) => e,
    }
}

/// A run's copy of a slot's upload ([`RuntimeStore::claim_uploads`]):
/// the slot, the upload it copies, and the key of the run's own file it
/// becomes, fresh for each call, so the row there is this call's alone and
/// only this call lands or ends it (a call that died leaves it to the
/// lapse of its claim, [`RuntimeStore::expire`]). `failed` says why the
/// bucket copy failed, when it did: the claim's transaction answers it
/// only when the upload still waits, since a same-run call that received
/// the upload meanwhile removes the source under this copy.
struct RunCopy {
    sid: String,
    upload: String,
    target: String,
    failed: Option<String>,
}

/// The refusal of a slot with no upload to receive.
fn nothing_uploaded() -> RuntimeStoreError {
    RuntimeStoreError::NoUpload(
        "nothing was uploaded through this slot (the page calls the route once uploadFile answered), or its upload \
         ended with the slot"
            .into(),
    )
}

/// An in-bucket copy's claim on the upload it fills
/// ([`RuntimeStore::fill_by_copy`]), renewed each time the bucket finishes
/// a piece of the copy: however long a big copy runs, the expiry loop's
/// end of abandoned rows ([`abandoned_at`]) takes it for dead only once no
/// piece has landed for a lease.
///
/// `held` is every row the call making the copy holds claimed, `key`
/// among them: a claim copying several slots into a run holds one lease
/// for all its copies, so a small copy finished early is never taken for
/// dead while a big one after it still runs.
struct CopyClaim<'a> {
    pool: &'a Pool,
    clock: &'a dyn Clock,
    key: &'a str,
    held: &'a [&'a str],
}

#[async_trait::async_trait]
impl CopyProgress for CopyClaim<'_> {
    /// Renew the claim while the row is still the copy's. A row a drive
    /// landed (the copy is whole) needs no claim; a row that went (a reap
    /// ended the upload) stops the copy, which has nothing left to fill.
    async fn copied_piece(&self) -> Result<()> {
        let renewed: Vec<String> = self
            .pool
            .scalars(
                "UPDATE runtime_file SET progressed_at_unix = $2 WHERE key = ANY($1) AND status = 'completing' RETURNING key",
                &[&self.held, &self.clock.now_unix()],
            )
            .await
            .context("runtime copy: renew the claim")?;
        if renewed.iter().any(|key| key == self.key) {
            return Ok(());
        }
        let landed: bool = self
            .pool
            .scalar("SELECT EXISTS (SELECT 1 FROM runtime_file WHERE key = $1 AND status IN ('active', 'uploaded'))", &[&self.key])
            .await
            .context("runtime copy: read what became of the upload")?;
        anyhow::ensure!(landed, "the upload '{}' a copy fills was ended while it copied", self.key);
        Ok(())
    }
}

/// The runtime-file store: PG metadata + bucket bytes, one gatekeeper.
pub struct RuntimeStore {
    pool: Pool,
    bucket: Arc<dyn ObjectStore>,
    clock: Arc<dyn Clock>,
    /// The soonest expiry this copy rang the expiry loop for
    /// ([`Self::ring_for`]).
    rung: std::sync::Mutex<crate::notices::EarliestRung>,
}

/// One stored-file metadata row, the in-Rust shape of a `runtime_file` row.
#[derive(Debug, Clone)]
struct FileRow {
    key: String,
    mime_type: String,
    filename: String,
    size_bytes: i64,
    keep: bool,
    expires_at_unix: Option<i64>,
    keep_ttl_secs: Option<i64>,
    created_at_unix: i64,
    version: i64,
}

impl FromRow for FileRow {
    fn from_row(row: Row) -> db::Result<Self> {
        Self::read(&row)
    }
}

impl FileRow {
    /// The [`FILE_ROW_COLUMNS`] of `row`.
    fn read(row: &Row) -> db::Result<Self> {
        Ok(Self {
            key: row.try_get("key")?,
            mime_type: row.try_get("mime_type")?,
            filename: row.try_get("filename")?,
            size_bytes: row.try_get("size_bytes")?,
            keep: row.try_get("keep")?,
            expires_at_unix: row.try_get("expires_at_unix")?,
            keep_ttl_secs: row.try_get("keep_ttl_secs")?,
            created_at_unix: row.try_get("created_at_unix")?,
            version: row.try_get("version")?,
        })
    }

    fn to_meta(&self) -> StoredFileMeta {
        StoredFileMeta {
            key: self.key.clone(),
            mime_type: self.mime_type.clone(),
            size_bytes: self.size_bytes as u64,
            filename: self.filename.clone(),
            keep: self.keep,
            expires_at_unix: self.expires_at_unix,
            keep_ttl_secs: self.keep_ttl_secs.map(|s| s as u64),
            created_at_unix: self.created_at_unix,
            version: self.version as u64,
        }
    }
}

/// A file's row as a link reads it, with the visitor address of the
/// project the link is for, when it asked for one.
#[derive(Debug)]
struct LinkedRow {
    file: FileRow,
    visitor_address: Option<String>,
}

impl FromRow for LinkedRow {
    fn from_row(row: Row) -> db::Result<Self> {
        Ok(Self { file: FileRow::read(&row)?, visitor_address: row.try_get("visitor_address")? })
    }
}

/// A link [`RuntimeStore::grant_link`] granted and
/// [`RuntimeStore::sign_link`] signs: the file, the window the link is
/// signed for, and the visitor address of the project asked about.
#[derive(Debug, Clone)]
pub struct GrantedLink {
    pub file: StoredFileMeta,
    pub window: LinkWindow,
    pub visitor_address: Option<String>,
}

/// What a sweep needs to know per row: spare it (kept ACTIVE file) or reap
/// its bucket state (aborting the in-flight upload of a pending row).
#[derive(Debug)]
struct SweepEntry {
    key: String,
    kept_active: bool,
    status: String,
    upload_id: Option<String>,
    /// The file a replacement upload overwrites: its multipart targets
    /// THAT file's object, so reaping it aborts the upload and must never
    /// delete the object (the file it replaces still owns it).
    replaces: Option<String>,
}

/// One in-flight upload's row: the metadata captured at begin plus the
/// multipart resume state. The in-Rust shape of a 'pending' or
/// 'completing' `runtime_file` row.
#[derive(Debug, Clone)]
struct PendingUpload {
    /// 'pending' while parts can still be reserved and reported,
    /// 'completing' once a completion has claimed it.
    status: String,
    tenant_id: String,
    keep_ttl_secs: Option<i64>,
    upload_id: Option<String>,
    /// The size of every part but the last, on an upload begun for parts
    /// ([`Self::part_size`]); none on a run's copy of a slot's upload.
    part_size: Option<i64>,
    declared_size: Option<i64>,
    /// The active file this upload overwrites, when it is a replacement.
    replaces: Option<String>,
    /// The type the upload was begun as.
    mime_type: String,
    /// The name the upload was begun with.
    filename: String,
    /// For a page's upload through a slot, the type its slot accepts
    /// ([`RuntimeStore::landed_mime`]).
    slot_accepts: Option<String>,
    /// For a run's copy of a slot's upload, the upload it copies
    /// ([`RuntimeStore::claim_uploads`]).
    receives: Option<String>,
}

impl FromRow for PendingUpload {
    fn from_row(row: Row) -> db::Result<Self> {
        Ok(Self {
            status: row.try_get("status")?,
            tenant_id: row.try_get("tenant_id")?,
            keep_ttl_secs: row.try_get("keep_ttl_secs")?,
            upload_id: row.try_get("upload_id")?,
            part_size: row.try_get("part_size")?,
            declared_size: row.try_get("declared_size")?,
            replaces: row.try_get("replaces")?,
            mime_type: row.try_get("mime_type")?,
            filename: row.try_get("filename")?,
            slot_accepts: row.try_get("slot_accepts")?,
            receives: row.try_get("receives")?,
        })
    }
}

impl PendingUpload {
    /// The bucket object this upload writes to: the replaced file's own
    /// object for a replacement (the bucket swaps it in whole at
    /// completion), else the upload's own.
    fn object_key(&self, key: &str) -> String {
        object_key(self.replaces.as_deref().unwrap_or(key))
    }

    /// Refuse a change to an upload a completion has claimed: its parts
    /// are what the bucket is assembling.
    fn refuse_if_completing(&self, key: &str) -> StoreResult<()> {
        if self.status == "completing" {
            return Err(RuntimeStoreError::Completing(format!(
                "upload '{key}' is completing; it cannot change now. Call complete again to \
                 get the file once it lands"
            )));
        }
        Ok(())
    }

    /// The size of every part but the last: what reserving and resuming
    /// slice by, on every upload begun for parts.
    fn part_size(&self, key: &str) -> Result<u64> {
        self.part_size.map(|size| size as u64).ok_or_else(|| {
            anyhow::anyhow!("the upload '{key}' takes no parts (a run's copy of a slot's upload is made in the bucket)")
        })
    }

    /// The row as a reap sees it.
    fn sweep_entry(&self, key: &str) -> SweepEntry {
        SweepEntry {
            key: key.to_string(),
            kept_active: false,
            status: self.status.clone(),
            upload_id: self.upload_id.clone(),
            replaces: self.replaces.clone(),
        }
    }
}

/// What the bucket made of a claimed completion.
enum Bucket {
    /// The object is assembled, this many bytes.
    Assembled(u64),
    /// The final verdict: the upload cannot be completed. `overwritten`
    /// when a replaced file's object may no longer hold that file's bytes.
    Lost { why: String, overwritten: bool },
}

/// What the end-of-run sweep did ([`RuntimeStore::sweep_run`]).
#[derive(Debug)]
pub struct RunSweep {
    /// Rows removed now.
    pub reaped: u64,
    /// Finished files stamped to expire after the run's linger.
    pub lingering: u64,
    /// Rows the bucket refused to remove, each `'<key>': <why>`, left
    /// marked ([`MARKED`]) and shown in `weft status`.
    pub refused: Vec<String>,
}

/// What one reap of a fenced row did ([`RuntimeStore::reap_unrecorded`]).
struct Reaped {
    /// Why the bucket refused, the row then left marked; `None` when the
    /// row went.
    refused: Option<anyhow::Error>,
    /// Whether the record of failed reaps `weft status` shows changed (a
    /// row marked, or a marked row gone), so the caller writes it again
    /// ([`RuntimeStore::record_reap_failures`]).
    changes_record: bool,
}

/// What [`RuntimeStore::fence_for_reap`] did with a row.
#[derive(Debug, PartialEq, Eq)]
enum Fence {
    /// Flipped to 'reaping': the caller reaps it.
    Fenced,
    /// No longer matching the sweep's condition.
    Spared,
    /// No row left under the key.
    Gone,
    /// Left alone because a completion is landing on it.
    Completing,
}

/// What [`RuntimeStore::lock_for_removal`] found under a key.
enum Removal {
    /// No row.
    Gone,
    /// A completion forbids the removal, for this reason.
    Blocked(String),
    /// The row is locked and may be removed.
    Free,
}

impl FromRow for SweepEntry {
    fn from_row(row: Row) -> db::Result<Self> {
        Ok(Self {
            key: row.try_get("key")?,
            kept_active: row.try_get("kept_active")?,
            status: row.try_get("status")?,
            upload_id: row.try_get("upload_id")?,
            replaces: row.try_get("replaces")?,
        })
    }
}

impl SweepEntry {
    /// A finished file's row as a reap sees it: an object, no upload.
    fn file(key: &str) -> Self {
        Self { key: key.to_string(), kept_active: false, status: "active".into(), upload_id: None, replaces: None }
    }
}

/// SQL fragment: the later of `param` (a bind like `$1`) and the end of
/// the longest-lived link handed out for the row being updated
/// (`links_until_unix`). Every write to `runtime_file.expires_at_unix`
/// that could SHORTEN a deadline goes through this, so no path (access
/// bump, keep, linger stamp) can pull a file's death forward under a live
/// link: a link is a promise the bytes stay fetchable for its life.
fn expiry_honoring_links(param: &str) -> String {
    format!("GREATEST({param}, COALESCE(links_until_unix, 0))")
}


impl RuntimeStore {
    pub fn new(pool: Pool, bucket: Arc<dyn ObjectStore>, clock: Arc<dyn Clock>) -> Self {
        Self { pool, bucket, clock, rung: Default::default() }
    }

    /// A write just gave a file the expiry `at`: ring the expiry loop when
    /// no ring of this copy's covers it ([`crate::notices::EarliestRung`]).
    /// Only writes that can make an expiry sooner call this (a keep, a new
    /// lifetime, the linger stamp); an access pushes an expiry later and
    /// rings nothing. A ring that cannot be sent is logged: the loop then
    /// looks when its booking or a later ring says.
    async fn ring_for(&self, at: Option<i64>) {
        let Some(at) = at else { return };
        if !self.rung.lock().expect("expiry rung").needs(at, self.clock.now_unix()) {
            return;
        }
        match self.pool.execute("SELECT pg_notify($1, '')", &[&FILE_EXPIRY_CHANNEL]).await {
            Ok(_) => self.rung.lock().expect("expiry rung").rang(at),
            Err(e) => tracing::error!(
                target: "weft_broker::runtime_store", error = %e, at,
                "could not ring the file expiry loop; a file expiring then is removed at the loop's next look"
            ),
        }
    }

    /// A call just took a claim on a row in flight that it may die
    /// holding, abandoned `lapse` seconds from now unless it makes progress
    /// ([`abandoned_at`]): ring the expiry loop for that lapse, so a loop
    /// asleep with nothing booked looks then and ends the row if the call
    /// died ([`Self::ring_for`]). A row's birth (a begin, a run's copy)
    /// and a reap ring; a completion's claim on a row already in flight is
    /// found by the look booked for that row, which books the claim's lapse
    /// in turn.
    async fn ring_for_abandon(&self, lapse: i64) {
        self.ring_for(Some(self.clock.now_unix() + lapse)).await;
    }

    /// One tenant's live footprint (file_count, charged_bytes). Bytes count an
    /// ACTIVE file by its size and a PENDING upload by its reserved (quota-
    /// charged) bytes, so an in-flight upload is visible in usage the moment
    /// it reserves, exactly as the quota check sees it. An expired file
    /// counts for nothing. The quota check + the usage view read this, so the
    /// two can never disagree. An abandoned upload counts until the expiry
    /// loop ends it, at its lapse ([`abandoned_at`]).
    pub async fn tenant_usage(&self, tenant: &str) -> StoreResult<(u64, u64)> {
        let now = self.clock.now_unix();
        let count = count_files(&self.pool, tenant, now).await.map_err(RuntimeStoreError::BeforeWrites)?;
        let bytes = charged_bytes_for(&self.pool, tenant, now).await.map_err(RuntimeStoreError::BeforeWrites)?;
        Ok((count, bytes))
    }

    /// Run `decide`, a quota-checked admission of `tenant`'s new bytes, and
    /// when it is refused for space, retry the tenant's marked reaps once
    /// ([`Self::reap_marked`]) and decide again if any went. A begin that
    /// fits pays nothing, and a refused one at most one pass over the
    /// marked rows.
    async fn deciding_with_space<T, Fut>(&self, tenant: &str, decide: impl Fn() -> Fut) -> StoreResult<T>
    where
        Fut: std::future::Future<Output = StoreResult<T>>,
    {
        let decided = decide().await;
        if !matches!(decided, Err(RuntimeStoreError::QuotaExceeded(_))) {
            return decided;
        }
        match self.reap_marked(tenant).await.map_err(RuntimeStoreError::Other)? {
            0 => decided,
            // The reaps just committed come before this decision.
            _ => decide().await.map_err(RuntimeStoreError::after_writes),
        }
    }

    /// Try `tenant`'s marked reaps ([`MARKED`]) once each, with no lock
    /// held: what a begin refused for space does before it decides again
    /// ([`Self::deciding_with_space`]). The first row the bucket still
    /// refuses stays marked and ends the pass (a bucket refusing one
    /// removal is all but always refusing the rest). Answers how many went.
    async fn reap_marked(&self, tenant: &str) -> Result<u64> {
        let entries: Vec<SweepEntry> = self
            .pool
            .all(
                &format!(
                    "SELECT key, FALSE AS kept_active, status, upload_id, replaces FROM runtime_file WHERE tenant_id = $1 AND {MARKED} \
                     ORDER BY key"
                ),
                &[&tenant],
            )
            .await
            .context("marked reap scan")?;
        let now = self.clock.now_unix();
        let mut record = false;
        let pass = async {
            let mut removed = 0;
            for entry in &entries {
                if self.fence_for_reap(&entry.key, &format!("AND {MARKED}"), now).await? != Fence::Fenced {
                    continue;
                }
                let reaped = self.reap_unrecorded(entry, &[]).await?;
                record |= reaped.changes_record;
                if reaped.refused.is_some() {
                    break;
                }
                removed += 1;
            }
            Ok(removed)
        }
        .await;
        self.finish_pass(record, pass).await
    }

    /// The end of a pass over many rows: when a reap in it changed the
    /// record of failed reaps ([`Reaped::changes_record`]), write it now,
    /// whatever the pass answered, so a row marked before a later error is
    /// never missing from `weft status` (a marked row books no look, so
    /// nothing else would write it soon). Then the pass's own answer; a
    /// failure to write the record is the answer only when the pass went
    /// through.
    async fn finish_pass<T, E: From<anyhow::Error>>(&self, record: bool, pass: Result<T, E>) -> Result<T, E> {
        if record {
            if let Err(e) = self.record_reap_failures().await {
                if pass.is_ok() {
                    return Err(e.into());
                }
                tracing::error!(
                    target: "weft_broker::runtime_store", error = %format!("{e:#}"),
                    "a pass that failed could not write down the reaps it marked; the next removal that marks or \
                     clears one writes them"
                );
            }
        }
        pass
    }

    /// Would storing `incoming` more bytes push the tenant over their disk cap,
    /// counting their WHOLE account (every storage plane), read on `tx` under
    /// the caller's lock so the total is one fresh number. This is the single
    /// place the account-wide byte check is assembled; both upload entry points
    /// call it. `account_used_bytes` already includes this plane's charged
    /// bytes, so the check is just `total + incoming > cap`.
    async fn account_would_exceed(
        &self,
        tx: &Transaction<'_>,
        entitlements: &dyn EntitlementSource,
        tenant: &str,
        incoming: u64,
    ) -> Result<bool> {
        let total = entitlements.account_used_bytes(tx, tenant, self.clock.now_unix()).await?;
        Ok(entitlements.caps(tenant).await?.disk_bytes_would_exceed(total, incoming))
    }

    /// Whether the tenant may store one more file of `declared_size`
    /// (unknown for a stream, which is charged part by part): its file
    /// cap, then its byte quota, read on `tx`. A begin runs it under the
    /// tenant lock; a slot's check before a begin runs it without.
    async fn admit_new_file(
        &self,
        tx: &Transaction<'_>,
        entitlements: &dyn EntitlementSource,
        tenant: &str,
        declared_size: Option<u64>,
        now: i64,
    ) -> StoreResult<()> {
        let count = count_files(tx, tenant, now)
            .await
            .context("runtime begin_upload: count tenant files")
            .map_err(RuntimeStoreError::Other)?;
        let caps = entitlements
            .caps(tenant)
            .await
            .context("runtime begin_upload: resolve tenant caps")
            .map_err(RuntimeStoreError::Other)?;
        if caps.file_count_would_exceed(count) {
            return Err(RuntimeStoreError::QuotaExceeded(format!(
                "tenant '{tenant}' is at its file cap ({} files); delete files or raise the cap",
                caps.file_cap
            )));
        }
        if let Some(declared) = declared_size {
            if self.account_would_exceed(tx, entitlements, tenant, declared).await.map_err(RuntimeStoreError::Other)? {
                return Err(RuntimeStoreError::QuotaExceeded(format!(
                    "tenant '{tenant}' would exceed its storage quota ({} bytes) by storing {declared} more",
                    caps.disk_bytes_cap
                )));
            }
        }
        Ok(())
    }

    /// What a begin of `filename`, `size` bytes, through `slot` meets,
    /// read on `tx`: `None` when the slot has no upload yet, else the
    /// begin's answer (carry on with the one part way up, or the one
    /// already landed), or its refusal. The slot's upload is carried on
    /// only for the same file: the same name and size, and the same sha256
    /// when the page states one and weft hashed the upload, so a page that
    /// picked another file is never answered with the first.
    async fn slot_upload_state(
        tx: &Transaction<'_>,
        slot: &SlotUpload<'_>,
        filename: &str,
        size: u64,
    ) -> StoreResult<Option<Reserved>> {
        // key, status, part size, filename, the size declared while it is
        // on its way up (its own once landed), content sha256
        type SlotRow = (String, String, Option<i64>, String, i64, Option<String>);
        let existing: Option<SlotRow> = tx
            .opt(
                "SELECT key, status, part_size, filename, COALESCE(declared_size, size_bytes) AS size, content_sha256 FROM runtime_file \
                 WHERE upload_slot = $1 AND status <> 'reaping'",
                &[&slot.sid],
            )
            .await
            .context("runtime begin_upload: look for the slot's upload")
            .map_err(RuntimeStoreError::Other)?;
        let Some((key, status, part_size, held, held_size, content_sha256)) = existing else {
            return Ok(None);
        };
        let other_hash = matches!((slot.sha256, content_sha256.as_deref()), (Some(stated), Some(hashed)) if stated != hashed);
        if held != filename || held_size != size as i64 || other_hash {
            return Err(RuntimeStoreError::Conflict(format!(
                "this upload slot already holds another file ('{held}'); ask the page's route for a new slot"
            )));
        }
        match (status.as_str(), part_size) {
            ("pending", Some(part_size)) => Ok(Some(Reserved::Pending(key, part_size as u64))),
            ("uploaded", _) => Ok(Some(Reserved::AlreadyActive(key))),
            ("completing", _) => Err(RuntimeStoreError::Completing("this slot's upload is completing; call complete again".into())),
            _ => Err(RuntimeStoreError::Conflict("this slot's upload was already received; a slot uploads one file".into())),
        }
    }

    /// Whether a begin of `spec`'s slot upload would be accepted, refused
    /// exactly as the begin refuses it, reserving nothing: what a page
    /// asks before it spends time hashing the file. A slot whose upload
    /// is part way up or landed is accepted (the begin carries on with
    /// it); otherwise the tenant's caps decide, after the same retry of its
    /// marked reaps a begin refused for space makes.
    pub async fn check_slot_upload(
        &self,
        caller: &CallerAuth,
        spec: &UploadSpec<'_>,
        entitlements: &dyn EntitlementSource,
    ) -> StoreResult<()> {
        let (Some(slot), CallerAuth::Worker { tenant, .. }) = (spec.slot, caller) else {
            return Err(RuntimeStoreError::Invalid("a slot's check names its slot and its project".into()));
        };
        refuse_too_large(spec.declared_size)?;
        let Some(size) = spec.declared_size else {
            return Err(RuntimeStoreError::Invalid("an upload through a slot names its size".into()));
        };
        slot.refuse_too_heavy(size)?;
        self.deciding_with_space(tenant, || async {
            let mut conn = self.pool.get().await.context("runtime slot check: begin").map_err(RuntimeStoreError::BeforeWrites)?;
            let tx = conn.transaction().await.context("runtime slot check: begin").map_err(RuntimeStoreError::BeforeWrites)?;
            if Self::slot_upload_state(&tx, &slot, spec.filename, size).await?.is_some() {
                return Ok(());
            }
            self.admit_new_file(&tx, entitlements, tenant, spec.declared_size, self.clock.now_unix()).await
        })
        .await
    }

    /// Re-wall a caller-supplied key: parse it through the grammar and confirm
    /// the caller may touch it. Every key-addressed upload verb (parts /
    /// part-done / complete / resume / abort) goes through here.
    fn wall_key(caller: &CallerAuth, key: &str) -> StoreResult<ParsedKey> {
        let parsed =
            weft_core::storage::key::parse_key(key).map_err(RuntimeStoreError::Denied)?;
        weft_core::storage::key::check_key_access(caller, &parsed)
            .map_err(RuntimeStoreError::Denied)?;
        Ok(parsed)
    }

    /// Begin a multipart upload: mint the key (tenant wall stamped in, the
    /// caller can't choose it), gate the file count, charge a KNOWN total size
    /// against the byte quota (a declared over-cap upload is rejected before a
    /// single byte can land anywhere), reserve the 'pending' row with the
    /// file's metadata, and open the bucket's multipart upload. Returns the
    /// key + the fixed part size the caller must slice to, or
    /// [`BeginUpload::AlreadyStored`] when a content-addressed (asset) begin
    /// names content that is already ACTIVE: same content = same asset, so
    /// the begin is that upload's idempotent success and there is nothing to
    /// transfer. A PENDING collision (another upload of this content is mid
    /// flight) stays a loud conflict.
    ///
    /// An unknown-length stream (`declared_size = None`) charges nothing here;
    /// each part is charged as it is reserved in [`Self::reserve_parts`].
    pub async fn begin_upload(
        &self,
        caller: &CallerAuth,
        spec: &UploadSpec<'_>,
        entitlements: &dyn EntitlementSource,
    ) -> StoreResult<BeginUpload> {
        let UploadSpec { scope, mime, filename, keep, declared_size, content_hash, identity, slot, imported } = *spec;
        refuse_too_large(declared_size)?;
        if let Some(imported) = imported {
            let (Some(_), None, None, None, false) = (declared_size, content_hash, identity, slot, matches!(scope, StorageScope::Asset)) else {
                return Err(RuntimeStoreError::Invalid(
                    "a copied file names its size, and is no asset, slot upload or identified file".into(),
                ));
            };
            if imported.version < weft_core::storage::FIRST_FILE_VERSION {
                return Err(RuntimeStoreError::Invalid(format!("a file's version starts at {}", weft_core::storage::FIRST_FILE_VERSION)));
            }
        }
        if let Some(slot) = slot {
            let (Some(size), None, None, None) = (declared_size, identity, content_hash, keep) else {
                return Err(RuntimeStoreError::Invalid(
                    "an upload through a slot names its size, and nothing of what the slot itself decides".into(),
                ));
            };
            slot.refuse_too_heavy(size)?;
        }
        if identity.is_some() && matches!(scope, StorageScope::Asset) {
            return Err(RuntimeStoreError::Invalid(
                "an identity does not apply to the asset scope, which is addressed by content".into(),
            ));
        }
        if identity.is_some_and(|i| i.is_empty() || i.len() > 512) {
            return Err(RuntimeStoreError::Invalid(
                "a file identity is a non-empty label of at most 512 characters".into(),
            ));
        }
        // A lifetime applies in every scope a node writes: on a run
        // file it also spares it from the end-of-run sweep, on the others
        // (which outlive runs already) it is simply when the file expires.
        // The asset scope's lifetime belongs to the source that references
        // it, so a node-chosen one is refused rather than dropped.
        if keep.is_some() && matches!(scope, StorageScope::Asset) {
            return Err(RuntimeStoreError::Invalid(
                "an asset's lifetime follows the source that references it; it takes no keep".into(),
            ));
        }
        // The file's id: the ASSET scope is content-addressed (the id IS the
        // sha256, supplied by the pre-build sync), every other scope mints a
        // uuid. A hash on a non-asset scope (or a missing/malformed hash on
        // the asset scope) is a caller bug, refused loud.
        let id = match (scope, content_hash) {
            (StorageScope::Asset, Some(hash)) => {
                if !weft_core::storage::is_content_hash(hash) {
                    return Err(RuntimeStoreError::Invalid(format!(
                        "asset id must be a 64-hex sha256 content hash, got '{hash}'"
                    )));
                }
                hash.to_string()
            }
            (StorageScope::Asset, None) => {
                return Err(RuntimeStoreError::Invalid(
                    "asset uploads carry their content hash as the id".into(),
                ));
            }
            (_, Some(_)) => {
                return Err(RuntimeStoreError::Invalid(
                    "a content hash id only applies to the asset scope".into(),
                ));
            }
            (_, None) => uuid::Uuid::new_v4().to_string(),
        };
        let parsed = match imported {
            // A copied file's key is the one it had, under the tenant
            // copying it, which only that tenant's admin caller names.
            Some(imported) => match caller {
                CallerAuth::Copying { tenant }
                    if *tenant == imported.key.tenant && imported.key.scope.storage_scope().as_ref() == Ok(scope) =>
                {
                    imported.key.clone()
                }
                _ => return Err(RuntimeStoreError::Denied("a copied file is written by its tenant's copy, in its own scope".into())),
            },
            None => weft_core::storage::key::key_for_put(caller, scope, &id).map_err(RuntimeStoreError::Denied)?,
        };
        let key = parsed.to_key();
        let tenant = parsed.tenant.clone();
        let part_size = part_size_for(declared_size);
        // An asset starts on the default countdown the moment it completes:
        // an upload the sync never publishes (the build failed after the
        // transfer) would otherwise sit with no expiry and nothing to reap
        // it. Publishing (`set_asset_references`) clears the countdown for
        // every referenced asset, so a current source asset has no expiry.
        let ttl = match scope {
            StorageScope::Asset => Some(DEFAULT_KEEP_TTL_SECS),
            _ => keep.and_then(KeepTtl::secs),
        };

        // Before the reservation, with no lock held (it may call the
        // bucket): an expired file under this identity goes, so the identity
        // is free for this upload.
        if let Some(identity) = identity {
            self.reap_expired_identity(&key, identity).await?;
        }

        // Open the bucket multipart FIRST, with NO lock held and NO row yet, so we
        // have its id (the resume handle) in hand before we write the row. This is
        // what keeps "a committed pending row always carries its upload handle"
        // atomic: we never commit a row without its `upload_id`, and we never do
        // bucket I/O while holding the tenant lock (the gates + insert below do). If
        // anything after this point fails before the row commits, we abort this
        // multipart so the bucket is never left holding upload state no row points
        // at.
        let now = self.clock.now_unix();
        let upload_id = self
            .bucket
            .create_multipart(&object_key(&key), mime)
            .await
            .context("runtime begin_upload: open multipart upload")
            .map_err(RuntimeStoreError::Other)?;

        // Gates + reservation, ATOMIC per tenant: the file-count gate, the
        // byte-quota check (known size), and the pending-row insert run in one
        // transaction under the tenant lock, so K concurrent begins serialize and
        // each sees the previous reservations. The row carries the file's metadata
        // (mime/filename/keep) AND its `upload_id` from the start; only the size and
        // expiry are finalized at complete. `keep` on a PENDING row does NOT spare
        // it from sweeps (they spare kept ACTIVE files only), so an abandoned
        // kept-file upload is still reaped. Any rejection/failure here aborts the
        // multipart opened above (`abort_reserve`), so a rejected begin leaves
        // nothing in the bucket.
        let reserve = self
            .deciding_with_space(&tenant, || async {
                // Nothing written before this connection outlives a failure
                // to get it: the multipart opened above is aborted below,
                // and an expired file reaped above was gone either way (the
                // begin sent again finds nothing to reap).
                let mut conn = self
                    .pool
                    .get()
                    .await
                    .context("runtime begin_upload: begin reserve tx")
                    .map_err(RuntimeStoreError::BeforeWrites)?;
                let tx = conn
                    .transaction()
                    .await
                    .context("runtime begin_upload: begin reserve tx")
                    .map_err(RuntimeStoreError::BeforeWrites)?;
                lock_tenant_storage(&tx, &tenant)
                    .await
                    .map_err(RuntimeStoreError::Other)?;
                // A content-addressed (asset) key can legitimately collide: the
                // same content uploaded twice IS the same file. Check under the
                // lock: an ACTIVE row is this begin's idempotent success (nothing
                // to upload), a PENDING row is another upload of the same content
                // mid-flight (a loud conflict; rerun once it settles). Answered
                // structurally instead of via a raw PK violation.
                if content_hash.is_some() {
                    let existing: Option<String> = tx
                        .scalar_opt("SELECT status FROM runtime_file WHERE key = $1", &[&key])
                        .await
                        .context("runtime begin_upload: check content-addressed key")
                    .map_err(RuntimeStoreError::Other)?;
                    match existing.as_deref() {
                        Some("active") => return Ok(Reserved::AlreadyActive(key.clone())),
                        // Part way up. The caller is handed the key and carries
                        // on with it; see `BeginUpload::Resume`. A row a sweep
                        // has already fenced for reaping ('reaping') is NOT
                        // resumable: its multipart is about to be aborted.
                        Some("pending") => {
                            let part_size: i64 = tx
                                .scalar("SELECT part_size FROM runtime_file WHERE key = $1", &[&key])
                                .await
                                .context("runtime begin_upload: read the pending part size")
                            .map_err(RuntimeStoreError::Other)?;
                            return Ok(Reserved::Pending(key.clone(), part_size as u64));
                        }
                        // Another publish of these same bytes has claimed the
                        // completion: the asset lands in a moment, and the
                        // caller waits for it (marked, so it knows to).
                        Some("completing") => {
                            return Err(RuntimeStoreError::Completing(format!(
                                "asset '{key}' is being completed by another upload of the same content; it lands shortly"
                            )));
                        }
                        Some(status) => {
                            return Err(RuntimeStoreError::Conflict(format!(
                                "asset '{key}' already exists ({status}); it is being cleared from \
                                 the store, so this content's key frees itself shortly"
                            )));
                        }
                        None => {}
                    }
                }
                // A copied file keeps its key, so the key may be here
                // already: the same copy made before (the same version and
                // size) is this begin's answer, one cut off part way up
                // carries on, anything else under the key is refused.
                if let Some(imported) = imported {
                    let existing: Option<(String, i64, i64)> = tx
                        .opt(
                            "SELECT status, version, CASE WHEN status = 'active' THEN size_bytes ELSE COALESCE(declared_size, -1) END \
                             FROM runtime_file WHERE key = $1",
                            &[&key],
                        )
                        .await
                        .context("runtime begin_upload: check the copied file's key")
                    .map_err(RuntimeStoreError::Other)?;
                    let same = |version: i64, size: i64| {
                        u64::try_from(version).ok() == Some(imported.version) && u64::try_from(size).ok() == declared_size
                    };
                    match existing {
                        Some((status, version, size)) if status == "active" && same(version, size) => {
                            return Ok(Reserved::AlreadyActive(key.clone()));
                        }
                        Some((status, version, size)) if status == "pending" && same(version, size) => {
                            let part_size: i64 = tx
                                .scalar("SELECT part_size FROM runtime_file WHERE key = $1", &[&key])
                                .await
                                .context("runtime begin_upload: read the pending part size")
                                .map_err(RuntimeStoreError::Other)?;
                            return Ok(Reserved::Pending(key.clone(), part_size as u64));
                        }
                        // An earlier copy's completion claimed it: the file
                        // lands in a moment, and the copy run again then
                        // answers against what landed.
                        Some((status, _, _)) if status == "completing" => {
                            return Err(RuntimeStoreError::Completing(format!(
                                "'{key}' is being completed by an earlier upload; run the copy again once it has landed"
                            )));
                        }
                        Some((status, _, _)) if status == "reaping" => {
                            return Err(RuntimeStoreError::Conflict(format!(
                                "'{key}' is being cleared from the store; its key frees itself shortly, then run the copy again"
                            )));
                        }
                        Some((status, _, _)) => {
                            return Err(RuntimeStoreError::Conflict(format!(
                                "'{key}' is already here ({status}) holding another version or size; remove it here \
                                 (`weft files rm {key}`) to copy it again"
                            )));
                        }
                        None => {}
                    }
                }
                // An identified begin: the same source stored twice in one
                // scope is one file. Checked under the lock, like the
                // content-addressed case: an ACTIVE row is this begin's
                // answer, a row still uploading (or being reaped) is a
                // conflict to retry once it settles. The scope is the key's
                // prefix (everything before the id), the same expression the
                // unique index is built on. A file that expired since it was
                // reaped above is not this begin's answer: it is on its way out.
                if let Some(identity) = identity {
                    let existing: Option<(String, String)> = tx
                        .opt(
                            &format!(
                                "SELECT key, CASE WHEN status = 'active' AND NOT {live} THEN 'expired' ELSE status END \
                                 FROM runtime_file \
                                 WHERE regexp_replace(key, '/[^/]+$', '') = $1 AND identity = $2",
                                live = unexpired("$3"),
                            ),
                            &[&scope_prefix(&key), &identity, &now],
                        )
                        .await
                        .context("runtime begin_upload: check identity")
                    .map_err(RuntimeStoreError::Other)?;
                    match existing {
                        Some((existing_key, status)) if status == "active" => {
                            return Ok(Reserved::AlreadyActive(existing_key));
                        }
                        Some((existing_key, status)) => {
                            return Err(RuntimeStoreError::Conflict(format!(
                                "a file with identity '{identity}' is already {status} in this scope \
                                 ('{existing_key}'); retry once it settles"
                            )));
                        }
                        None => {}
                    }
                }
                // A slot's one upload: begun again (a page that lost its
                // place), it carries on where it is.
                if let (Some(slot), Some(size)) = (slot, declared_size) {
                    if let Some(reserved) = Self::slot_upload_state(&tx, &slot, filename, size).await? {
                        return Ok(reserved);
                    }
                }
                self.admit_new_file(&tx, entitlements, &tenant, declared_size, now).await?;
                tx.execute(
                    "INSERT INTO runtime_file \
                     (key, tenant_id, mime_type, filename, size_bytes, status, keep, expires_at_unix, \
                      keep_ttl_secs, created_at_unix, upload_id, part_size, declared_size, \
                      reserved_bytes, progressed_at_unix, identity, upload_slot, slot_accepts, version) \
                     VALUES ($1, $2, $3, $4, 0, 'pending', $5, $13, $6, COALESCE($16::BIGINT, $7), $8, $9, $10, $11, $7, $12, $14, $15, $17)",
                    &[
                        &key,
                        &tenant,
                        &mime,
                        &filename,
                        // The flag is the end-of-run sweep's exemption, which
                        // only a run file needs; elsewhere a lifetime is just
                        // a ttl.
                        &(keep.is_some() && matches!(scope, StorageScope::Run)),
                        &ttl.map(|s| s as i64),
                        &now,
                        &upload_id,
                        &(part_size as i64),
                        &declared_size.map(|s| s as i64),
                        &(declared_size.unwrap_or(0) as i64),
                        &identity,
                        &slot.map(|s| s.ends_unix),
                        &slot.map(|s| s.sid),
                        &slot.map(|s| s.accepts),
                        &imported.map(|i| i.created_at_unix),
                        &(imported.map_or(weft_core::storage::FIRST_FILE_VERSION, |i| i.version) as i64),
                    ],
                )
                .await
                .context("runtime begin_upload: reserve pending row")
                .map_err(RuntimeStoreError::Other)?;
                tx.commit()
                    .await
                    .context("runtime begin_upload: commit reservation")
                    .map_err(RuntimeStoreError::Other)?;
                Ok(Reserved::Fresh)
            })
            .await;

        // Whenever no fresh row committed (rejection, failure, or an
        // already-active asset), abort the multipart we opened so the bucket
        // is not left holding an upload no row references (the very invariant
        // this ordering exists to hold). Best-effort: on abort failure the
        // bucket lifecycle rule is the backstop.
        let abort_opened_multipart = || async {
            if let Err(ab) = self.bucket.abort_multipart(&object_key(&key), &upload_id).await {
                tracing::error!(
                    target: "weft_broker::runtime_store",
                    key = %key, error = %ab,
                    "failed to abort multipart after an uncommitted begin; \
                     the bucket lifecycle rule will reap it"
                );
            }
        };
        match reserve {
            Ok(Reserved::Fresh) => {
                self.ring_for_abandon(PENDING_RESERVE_GRACE_SECS).await;
                Ok(BeginUpload::Ready { key, part_size })
            }
            Ok(Reserved::AlreadyActive(existing)) => {
                abort_opened_multipart().await;
                Ok(BeginUpload::AlreadyStored { key: existing })
            }
            // The multipart THIS begin opened is aborted: the caller is
            // going to carry on with the one already in flight, which has
            // its own upload id and its own part rows.
            Ok(Reserved::Pending(existing, part_size)) => {
                abort_opened_multipart().await;
                Ok(BeginUpload::Resume { key: existing, part_size })
            }
            Err(e) => {
                abort_opened_multipart().await;
                Err(e)
            }
        }
    }

    /// Begin overwriting the ACTIVE file `replaced` with new bytes: same
    /// key, same scope, name, type and lifetime; only the content and its
    /// size change. The upload is an ordinary pending one under a key of
    /// its own (so quota, resume, abort and every sweep treat it exactly
    /// like any upload), except that its multipart writes to the replaced
    /// file's OBJECT. The bucket swaps that object in whole when the
    /// multipart completes, so a reader sees the old bytes or the new ones,
    /// never a mix; completion then folds the new size into the replaced
    /// file's row ([`Self::fold_replacement`]).
    ///
    /// The declared size is charged up front like any upload, so for the
    /// length of the upload the tenant pays for both versions. There is no
    /// file-count gate: a replacement adds no file.
    ///
    /// One replacement of a file is in flight at a time (both write onto
    /// the file's own object): a begin while another is in flight is a
    /// [`RuntimeStoreError::Conflict`], to retry once it ends. With
    /// `expected_version` (an edit, which read the file first), a file
    /// whose version moved on since is [`RuntimeStoreError::Stale`]: no
    /// other replacement can start until this one ends, so the version
    /// checked here is the one the new bytes replace.
    pub async fn begin_replace(
        &self,
        caller: &CallerAuth,
        replaced: &str,
        declared_size: Option<u64>,
        expected_version: Option<u64>,
        entitlements: &dyn EntitlementSource,
    ) -> StoreResult<(String, u64)> {
        refuse_too_large(declared_size)?;
        let target = Self::wall_key(caller, replaced)?;
        if matches!(target.scope, weft_core::storage::key::KeyScope::Asset) {
            return Err(RuntimeStoreError::Denied(
                "the asset scope is managed by the pre-build asset sync; an asset is never replaced \
                 by node code"
                    .into(),
            ));
        }
        let parsed = ParsedKey {
            tenant: target.tenant.clone(),
            scope: target.scope.clone(),
            id: uuid::Uuid::new_v4().to_string(),
        };
        let key = parsed.to_key();
        let replaced = target.to_key();
        let tenant = parsed.tenant.clone();
        let part_size = part_size_for(declared_size);
        let now = self.clock.now_unix();
        // The object keeps the file's type through the swap; a file's
        // type never changes (the locked read below finds it still there).
        let mime: String = self
            .pool
            .scalar_opt("SELECT mime_type FROM runtime_file WHERE key = $1 AND status = 'active'", &[&replaced])
            .await
            .context("runtime begin_replace: read the replaced file's type")
            .map_err(RuntimeStoreError::BeforeWrites)?
            .ok_or_else(|| RuntimeStoreError::NotFound(replaced.clone()))?;
        // Same ordering as `begin_upload`: the multipart first, with no
        // lock and no row, so a committed pending row always carries its
        // upload handle; anything that stops the row committing aborts it.
        let upload_id = self
            .bucket
            .create_multipart(&object_key(&replaced), &mime)
            .await
            .context("runtime begin_replace: open multipart upload")
            .map_err(RuntimeStoreError::Other)?;
        let reserve = self
            .deciding_with_space(&tenant, || async {
                // The multipart opened above is aborted below when this fails.
                let mut conn = self
                    .pool
                    .get()
                    .await
                    .context("runtime begin_replace: begin reserve tx")
                    .map_err(RuntimeStoreError::BeforeWrites)?;
                let tx = conn
                    .transaction()
                    .await
                    .context("runtime begin_replace: begin reserve tx")
                    .map_err(RuntimeStoreError::BeforeWrites)?;
                lock_tenant_storage(&tx, &tenant).await.map_err(RuntimeStoreError::Other)?;
                let existing: Option<(String, String, i64)> = tx
                    .opt(
                        &format!(
                            "SELECT mime_type, filename, version FROM runtime_file WHERE key = $1 AND status = 'active' AND {live}",
                            live = unexpired("$2"),
                        ),
                        &[&replaced, &now],
                    )
                    .await
                    .context("runtime begin_replace: read the replaced file")
                .map_err(RuntimeStoreError::Other)?;
                let Some((mime, filename, version)) = existing else {
                    return Err(RuntimeStoreError::NotFound(replaced.clone()));
                };
                let in_flight: Option<String> = tx
                    .scalar_opt("SELECT key FROM runtime_file WHERE replaces = $1", &[&replaced])
                    .await
                    .context("runtime begin_replace: look for a replacement in flight")
                    .map_err(RuntimeStoreError::Other)?;
                if in_flight.is_some() {
                    return Err(RuntimeStoreError::Conflict(format!(
                        "'{replaced}' is being changed by another write; retry once it ends"
                    )));
                }
                if let Some(expected) = expected_version {
                    if expected as i64 != version {
                        return Err(RuntimeStoreError::Stale(format!(
                            "'{replaced}' is at version {version}, not the version {expected} this \
                             change was made from"
                        )));
                    }
                }
                if let Some(declared) = declared_size {
                    if self.account_would_exceed(&tx, entitlements, &tenant, declared)
                        .await
                        .map_err(RuntimeStoreError::Other)?
                    {
                        let cap = entitlements
                            .caps(&tenant)
                            .await
                            .map(|c| c.disk_bytes_cap.to_string())
                            .unwrap_or_else(|_| "?".to_string());
                        return Err(RuntimeStoreError::QuotaExceeded(format!(
                            "tenant '{tenant}' would exceed its storage quota ({cap} bytes) by \
                             storing {declared} more while '{replaced}' is replaced; the old \
                             content counts until the new one lands"
                        )));
                    }
                }
                tx.execute(
                    "INSERT INTO runtime_file \
                     (key, tenant_id, mime_type, filename, size_bytes, status, keep, expires_at_unix, \
                      keep_ttl_secs, created_at_unix, upload_id, part_size, declared_size, \
                      reserved_bytes, progressed_at_unix, identity, replaces) \
                     VALUES ($1, $2, $3, $4, 0, 'pending', FALSE, NULL, NULL, $5, $6, $7, $8, $9, $5, NULL, $10)",
                    &[
                        &key,
                        &tenant,
                        &mime,
                        &filename,
                        &now,
                        &upload_id,
                        &(part_size as i64),
                        &declared_size.map(|s| s as i64),
                        &(declared_size.unwrap_or(0) as i64),
                        &replaced,
                    ],
                )
                .await
                .context("runtime begin_replace: reserve pending row")
                .map_err(RuntimeStoreError::Other)?;
                tx.commit()
                    .await
                    .context("runtime begin_replace: commit reservation")
                    .map_err(RuntimeStoreError::Other)
            })
            .await;
        if let Err(e) = reserve {
            if let Err(ab) = self.bucket.abort_multipart(&object_key(&replaced), &upload_id).await {
                tracing::error!(
                    target: "weft_broker::runtime_store",
                    key = %replaced, error = %ab,
                    "failed to abort multipart after an uncommitted replace; \
                     the bucket lifecycle rule will reap it"
                );
            }
            return Err(e);
        }
        self.ring_for_abandon(PENDING_RESERVE_GRACE_SECS).await;
        Ok((key, part_size))
    }

    /// ASSEMBLY: create a stored file by concatenating EXISTING objects of
    /// this store, the bytes never leaving the process that runs it (bucket
    /// -> this process -> bucket, one part-sized buffer at a time). The same
    /// ledger lifecycle as a caller-driven upload (begin reservation, part
    /// rows, complete), so quota, sweeps, and listings see it identically;
    /// only the byte transport differs (direct `upload_part`, no presigning
    /// to an external caller).
    ///
    /// `sources` are `(object key, expected size)` pairs, concatenated in
    /// order; `spec.declared_size` must equal their sum (the reservation is
    /// exact) and a fetched object whose size disagrees aborts loudly. Any
    /// failure aborts the upload, freeing the reservation.
    pub async fn assemble(
        &self,
        caller: &CallerAuth,
        spec: &UploadSpec<'_>,
        sources: &[(String, u64)],
        entitlements: &dyn EntitlementSource,
    ) -> StoreResult<StoredFileMeta> {
        let total: u64 = sources.iter().map(|(_, s)| *s).sum();
        if spec.declared_size != Some(total) {
            return Err(RuntimeStoreError::Invalid(format!(
                "assemble: declared_size {:?} must equal the sources' total {total}",
                spec.declared_size
            )));
        }
        let (key, part_size) = match self.begin_upload(caller, spec, entitlements).await? {
            BeginUpload::Ready { key, part_size } => (key, part_size),
            // The content is already stored ACTIVE: assembling it again would
            // produce the same file, so the existing file's meta IS this
            // assembly's result (idempotent, like the begin itself).
            BeginUpload::AlreadyStored { key } => {
                let parsed = weft_core::storage::key::parse_key(&key)
                    .map_err(RuntimeStoreError::Denied)?;
                return self.meta(&parsed).await;
            }
            // An assembly is not resumable: it writes parts DIRECTLY (no
            // presigned URLs) from sources it walks in order, and the
            // in-flight upload under this key belongs to whoever opened it,
            // with its own multipart handle. Refused loudly rather than
            // joined; the content is addressed by its hash, so the other
            // upload is putting the same bytes there and this call's answer
            // arrives on the next attempt.
            BeginUpload::Resume { key, .. } => {
                return Err(RuntimeStoreError::Conflict(format!(
                    "'{key}' is already part way up; wait for that upload to finish or be \
                     cleared, then assemble again"
                )));
            }
        };
        let assembled = async {
            // The multipart handle, committed with the pending row by begin.
            let upload_id = Self::pending_row(&self.pool, &key)
                .await
                .context("assemble: read pending row")
                .map_err(RuntimeStoreError::Other)?
                .and_then(|p| p.upload_id)
                .ok_or_else(|| {
                    RuntimeStoreError::Other(anyhow::anyhow!(
                        "assemble: begin committed no upload handle for '{key}'"
                    ))
                })?;

            // Concatenate sources into exactly part_size-d parts (only the
            // final one may be smaller; `reserve_parts` validates the
            // slicing), each reserved in the ledger and uploaded directly.
            // Each source is size-checked up front and then read in
            // part-sized ranges, so memory stays bounded at ~one part
            // regardless of source-object and asset size.
            let mut buf: Vec<u8> = Vec::with_capacity(part_size as usize);
            // Parts are numbered from 1, in the order they are written.
            let mut part_number: i32 = 1;
            for (src, expected) in sources {
                let actual = self
                    .bucket
                    .size(src)
                    .await
                    .with_context(|| format!("assemble: stat source {src}"))
                    .map_err(RuntimeStoreError::Other)?
                    .ok_or_else(|| {
                        RuntimeStoreError::Invalid(format!(
                            "assemble: source object '{src}' does not exist"
                        ))
                    })?;
                if actual != *expected {
                    return Err(RuntimeStoreError::Invalid(format!(
                        "assemble: source '{src}' is {actual} bytes, expected {expected}"
                    )));
                }
                let mut off: u64 = 0;
                while off < *expected {
                    let end = (*expected).min(off + part_size);
                    let bytes = self
                        .bucket
                        .get_range(src, off, end)
                        .await
                        .with_context(|| format!("assemble: read source {src} [{off}..{end})"))
                        .map_err(RuntimeStoreError::Other)?
                        .ok_or_else(|| {
                            RuntimeStoreError::Invalid(format!(
                                "assemble: source object '{src}' vanished mid-read"
                            ))
                        })?;
                    if bytes.is_empty() {
                        return Err(RuntimeStoreError::Invalid(format!(
                            "assemble: source '{src}' ended at {off} bytes, expected {expected}"
                        )));
                    }
                    off += bytes.len() as u64;
                    buf.extend_from_slice(&bytes);
                    while buf.len() as u64 >= part_size {
                        let chunk: Vec<u8> = buf.drain(..part_size as usize).collect();
                        self.assemble_part(caller, &key, &upload_id, part_number, chunk, entitlements)
                            .await?;
                        part_number += 1;
                    }
                }
            }
            if !buf.is_empty() {
                let chunk = std::mem::take(&mut buf);
                self.assemble_part(caller, &key, &upload_id, part_number, chunk, entitlements)
                    .await?;
            }
            self.complete_upload(caller, &key).await
        }
        .await;
        match assembled {
            Ok(meta) => Ok(meta),
            Err(e) => {
                // Free the reservation; nothing must linger on a failed assembly.
                if let Err(ab) = self.abort_upload(caller, &key).await {
                    tracing::error!(
                        target: "weft_broker::runtime_store",
                        key = %key, error = %ab,
                        "failed to abort assembly after error; the expiry loop reaps it once abandoned, or \
                         drives it if a completion had claimed it"
                    );
                }
                // Every step here comes after the begin's reservation.
                Err(e.after_writes())
            }
        }
    }

    /// One assembled part: ledger reservation (validates the slicing exactly
    /// like a caller-driven part) + direct upload + etag record.
    async fn assemble_part(
        &self,
        caller: &CallerAuth,
        key: &str,
        upload_id: &str,
        part_number: i32,
        chunk: Vec<u8>,
        entitlements: &dyn EntitlementSource,
    ) -> StoreResult<()> {
        let size = chunk.len() as u64;
        // The assembler counts its own parts: it is the only writer of this
        // key and it walks the sources in order, so the part it is on is the
        // part it says it is on.
        self.reserve_parts(
            caller,
            key,
            &[PartAsk { part_number, size_bytes: size }],
            entitlements,
            &PresignAudience::Install,
        )
        .await?;
        let etag = self
            .bucket
            .upload_part(&object_key(key), upload_id, part_number, bytes::Bytes::from(chunk))
            .await
            .context("assemble: direct part upload")
            .map_err(RuntimeStoreError::Other)?;
        self.record_part(caller, key, part_number, &etag).await
    }

    /// Reserve + presign the parts the caller NAMES. Each URL is signed with
    /// its part's exact size, so the bucket enforces the reservation
    /// byte-for-byte.
    ///
    /// The caller names the part number, and a part number IS a position:
    /// part `n` carries the file's bytes from `(n - 1) * part_size`. So
    /// reserving is idempotent, and that is the point. Asking for "the next
    /// part" meant two uploaders of the same key (the key is a content hash,
    /// so they hold identical bytes) each extended the reservation, together
    /// reserved more parts than the file has, and both then failed on a
    /// total that no longer added up, leaving the upload unfinishable until
    /// the hourly sweep removed it. Naming the part makes the second asker
    /// get the same part as the first, write the same bytes to it, and
    /// finish.
    ///
    /// A KNOWN-size upload was charged in full at begin, so its parts must
    /// slice exactly to the declared total (no re-charge here). An unknown-
    /// length stream is charged part-by-part under the tenant lock; the
    /// reservation that would cross the cap ABORTS the whole upload (frees
    /// everything) and returns QuotaExceeded, so a stream can never inch past
    /// the cap.
    pub async fn reserve_parts(
        &self,
        caller: &CallerAuth,
        key: &str,
        asks: &[PartAsk],
        entitlements: &dyn EntitlementSource,
        audience: &PresignAudience,
    ) -> StoreResult<Vec<PresignedPart>> {
        Self::wall_key(caller, key)?;
        if asks.is_empty() {
            return Err(RuntimeStoreError::Invalid("no parts requested".into()));
        }
        let mut seen: std::collections::BTreeSet<i32> = std::collections::BTreeSet::new();
        for ask in asks {
            if ask.part_number < 1 || ask.part_number as u64 > MAX_PARTS {
                return Err(RuntimeStoreError::Invalid(format!(
                    "part number {} is out of range: parts are numbered 1 to {MAX_PARTS}",
                    ask.part_number
                )));
            }
            if !seen.insert(ask.part_number) {
                return Err(RuntimeStoreError::Invalid(format!(
                    "part {} is named twice in one reservation",
                    ask.part_number
                )));
            }
        }
        let now = self.clock.now_unix();
        let mut conn = self.pool.get().await.context("runtime reserve_parts: begin tx").map_err(RuntimeStoreError::BeforeWrites)?;
        let tx = conn.transaction().await.context("runtime reserve_parts: begin tx").map_err(RuntimeStoreError::BeforeWrites)?;
        // The row is locked before the tenant: a completion claim (which
        // takes no tenant lock) then waits on this short transaction or
        // this one sees its mark, and no bucket call ever sits under it.
        let pending = Self::pending_row_locked(&tx, key)
            .await
            .map_err(RuntimeStoreError::Other)?
            .ok_or_else(|| {
                RuntimeStoreError::Invalid(format!(
                    "no in-flight upload for key '{key}'; begin an upload first"
                ))
            })?;
        pending.refuse_if_completing(key)?;
        lock_tenant_storage(&tx, &pending.tenant_id)
            .await
            .map_err(RuntimeStoreError::Other)?;
        let upload_id = pending.upload_id.clone().ok_or_else(|| {
            RuntimeStoreError::Other(anyhow::anyhow!(
                "pending row for '{key}' unexpectedly has no upload id (a committed \
                 pending row always carries one); abort this upload and begin again"
            ))
        })?;
        let part_size = pending.part_size(key).map_err(RuntimeStoreError::Other)?;

        // A part must be at least 1 byte: a multipart part can never be zero
        // (S3 rejects it), and an empty object uploads ZERO parts and is
        // written directly at complete.
        for ask in asks {
            if ask.size_bytes == 0 {
                return Err(RuntimeStoreError::Invalid(
                    "a part must be at least 1 byte; an empty object uploads zero parts".into(),
                ));
            }
            if ask.size_bytes > part_size {
                return Err(RuntimeStoreError::Invalid(format!(
                    "part {} is {} bytes, more than this upload's {part_size}-byte part size",
                    ask.part_number, ask.size_bytes
                )));
            }
        }
        // Which of the named parts are already reserved. A re-ask is the
        // whole point of naming parts, so it must not be charged twice: a
        // stream is charged per part as it goes, and charging a part the
        // caller already paid for would inch the tenant's usage up on every
        // retry.
        let already: Vec<(i32, i64)> = tx
            .all("SELECT part_number, size_bytes FROM runtime_file_part WHERE key = $1 ORDER BY part_number", &[&key])
            .await
            .context("runtime reserve_parts: read the parts already reserved")
        .map_err(RuntimeStoreError::Other)?;
        for ask in asks {
            if let Some((_, size)) = already.iter().find(|(number, _)| *number == ask.part_number) {
                if *size as u64 != ask.size_bytes {
                    return Err(RuntimeStoreError::Invalid(format!(
                        "part {} was reserved as {size} bytes; its size cannot change to {}", ask.part_number, ask.size_bytes
                    )));
                }
            }
        }
        let incoming: u64 =
            asks.iter().filter(|a| !already.iter().any(|(number, _)| *number == a.part_number)).map(|a| a.size_bytes).sum();
        if let Some(declared) = pending.declared_size {
            // Known size: the layout is arithmetic, so each named part has
            // exactly one correct size and the reservation can be checked
            // against it without reading what else is reserved. Part `n`
            // starts at `(n - 1) * part_size`; it is a whole part unless the
            // file ends inside it.
            let declared = declared as u64;
            for ask in asks {
                let offset = (ask.part_number as u64 - 1) * part_size;
                if offset >= declared {
                    return Err(RuntimeStoreError::Invalid(format!(
                        "part {} starts at byte {offset}, past the end of the declared \
                         {declared}-byte total",
                        ask.part_number
                    )));
                }
                let expected = part_size.min(declared - offset);
                if ask.size_bytes != expected {
                    return Err(RuntimeStoreError::Invalid(format!(
                        "part {} starts at byte {offset} and must be {expected} bytes to slice \
                         the declared {declared}-byte total; got {}",
                        ask.part_number, ask.size_bytes
                    )));
                }
            }
        } else {
            let mut layout: std::collections::BTreeMap<_, _> = already.iter().map(|(number, size)| (*number, *size as u64)).collect();
            layout.extend(asks.iter().map(|ask| (ask.part_number, ask.size_bytes)));
            validate_stream_layout(&layout, part_size)?;
            // Stream: charge these parts now, under the lock. The account check
            // sums THIS plane's charged bytes (already including this upload's
            // reserved_bytes) plus the other plane's, both read on this tx.
            if self.account_would_exceed(&tx, entitlements, &pending.tenant_id, incoming)
                .await
                .map_err(RuntimeStoreError::Other)?
            {
                // The stream cannot fit: abort the WHOLE upload now (delete the
                // row in-tx so the freed charge is serialized; cascade drops the
                // parts), then abort the bucket's multipart upload. The caller
                // gets a loud quota error; nothing is left to clean.
                tx.execute(&delete_upload_row("AND status = 'pending'"), &[&key])
                    .await
                    .context("runtime reserve_parts: delete over-quota upload")
                    .map_err(RuntimeStoreError::Other)?;
                tx.commit()
                    .await
                    .context("runtime reserve_parts: commit over-quota abort")
                    .map_err(RuntimeStoreError::Other)?;
                drop(conn);
                if let Err(abort) =
                    self.bucket.abort_multipart(&pending.object_key(key), &upload_id).await
                {
                    tracing::error!(
                        target: "weft_broker::runtime_store",
                        key = %key, error = %abort,
                        "failed to abort over-quota multipart upload; \
                         the bucket lifecycle rule will reap it"
                    );
                }
                // Best-effort cap readout for the message; the quota verdict
                // above already stands regardless.
                let cap_display = entitlements
                    .caps(&pending.tenant_id)
                    .await
                    .map(|c| c.disk_bytes_cap.to_string())
                    .unwrap_or_else(|_| "?".to_string());
                return Err(RuntimeStoreError::QuotaExceeded(format!(
                    "tenant '{}' would exceed its storage quota ({cap_display} bytes) by \
                     streaming {incoming} more; the upload was aborted",
                    pending.tenant_id
                )));
            }
            tx.execute(
                "UPDATE runtime_file SET reserved_bytes = reserved_bytes + $2 \
                 WHERE key = $1 AND status = 'pending'",
                &[&key, &(incoming as i64)],
            )
            .await
            .context("runtime reserve_parts: charge stream parts")
            .map_err(RuntimeStoreError::Other)?;
        }
        let mut reserved = Vec::with_capacity(asks.len());
        for ask in asks {
            // Idempotent by part number: a part already reserved keeps the
            // size it was reserved with, and this hands back a fresh URL for
            // it. That is what lets two uploaders of identical content, or
            // one uploader retrying, name the same part and agree.
            tx.execute(
                "INSERT INTO runtime_file_part (key, part_number, size_bytes, etag) \
                 VALUES ($1, $2, $3, NULL) \
                 ON CONFLICT (key, part_number) DO NOTHING",
                &[&key, &ask.part_number, &(ask.size_bytes as i64)],
            )
            .await
            .context("runtime reserve_parts: insert part row")
            .map_err(RuntimeStoreError::Other)?;
            reserved.push((ask.part_number, ask.size_bytes));
        }
        // A reservation is progress: refresh the abandoned-pending clock. The
        // row is locked, so a sweep's fence waits for this commit and its
        // WHERE re-check then sees the bumped clock and spares the row; the
        // gate on 'pending' stays as an assertion of that.
        let alive = tx
            .execute("UPDATE runtime_file SET progressed_at_unix = $2 WHERE key = $1 AND status = 'pending'", &[&key, &now])
            .await
            .context("runtime reserve_parts: bump progress clock")
            .map_err(RuntimeStoreError::Other)?;
        if alive == 0 {
            return Err(RuntimeStoreError::Invalid(format!(
                "upload '{key}' was swept mid-flight (idle past the reserve grace); \
                 begin the upload again"
            )));
        }
        tx.commit()
            .await
            .context("runtime reserve_parts: commit reservations")
            .map_err(RuntimeStoreError::Other)?;
        drop(conn);

        // Presign AFTER the commit (no bucket I/O under the tenant lock). If a
        // presign fails here the reservations stand: the caller resumes (which
        // re-presigns exactly these parts), so nothing is stranded.
        let (offsets, _sizes, _missing) = self
            .part_offsets(key, &pending)
            .await
            .map_err(RuntimeStoreError::Other)?;
        let mut parts = Vec::with_capacity(reserved.len());
        for (part_number, size) in reserved {
            let offset_bytes = *offsets.get(&part_number).ok_or_else(|| {
                RuntimeStoreError::Other(anyhow::anyhow!(
                    "part {part_number} of '{key}' was just reserved and has no row to \
                     place it in the file; abort this upload and begin again"
                ))
            })?;
            let url = self
                .bucket
                .presign_part(
                    &pending.object_key(key),
                    &upload_id,
                    part_number,
                    size,
                    audience,
                    PART_LINK_SECS,
                )
                .await
                .context("runtime reserve_parts: presign part (the reservation stands; resume the upload to re-presign)")
                .map_err(RuntimeStoreError::Other)?;
            parts.push(PresignedPart { part_number, size_bytes: size, offset_bytes, url });
        }
        Ok(parts)
    }

    /// Record a landed part's etag (the bucket's response header, verbatim).
    /// The part's size comes from OUR reservation, never from the caller.
    /// Idempotent: re-reporting a part overwrites its etag (a re-uploaded part
    /// number overwrites in the bucket too, so the latest etag is the truth).
    pub async fn record_part(
        &self,
        caller: &CallerAuth,
        key: &str,
        part_number: i32,
        etag: &str,
    ) -> StoreResult<()> {
        Self::wall_key(caller, key)?;
        if etag.is_empty() {
            return Err(RuntimeStoreError::Invalid("empty etag".into()));
        }
        // Under the upload row's lock: a completion claim reads the etags,
        // so a report must land before it or be refused after it, never
        // change an etag the bucket is being handed.
        let mut conn = self.pool.get().await.context("runtime record_part: begin tx").map_err(RuntimeStoreError::BeforeWrites)?;
        let tx = conn.transaction().await.context("runtime record_part: begin tx").map_err(RuntimeStoreError::BeforeWrites)?;
        let pending = Self::pending_row_locked(&tx, key)
            .await
            .map_err(RuntimeStoreError::Other)?
            .ok_or_else(|| {
                RuntimeStoreError::Invalid(format!(
                    "upload '{key}' is no longer in flight (completed, aborted, or swept idle \
                     past the reserve grace); begin the upload again"
                ))
            })?;
        pending.refuse_if_completing(key)?;
        let updated = tx
            .execute("UPDATE runtime_file_part SET etag = $3 WHERE key = $1 AND part_number = $2", &[&key, &part_number, &etag])
            .await
            .context("runtime record_part")
            .map_err(RuntimeStoreError::Other)?;
        if updated == 0 {
            return Err(RuntimeStoreError::Invalid(format!(
                "part {part_number} of '{key}' was never reserved"
            )));
        }
        // A landed part is progress: refresh the abandoned-pending clock.
        tx.execute("UPDATE runtime_file SET progressed_at_unix = $2 WHERE key = $1", &[&key, &self.clock.now_unix()])
            .await
            .context("runtime record_part: bump progress clock")
            .map_err(RuntimeStoreError::Other)?;
        tx.commit()
            .await
            .context("runtime record_part: commit")
            .map_err(RuntimeStoreError::Other)?;
        Ok(())
    }

    /// Finalize an upload: every reserved part must have been reported done
    /// (and, for a known size, the parts must sum to the declared total).
    /// Idempotent on retry: a key that already completed returns the file
    /// it became ([`Self::completed_meta`]).
    ///
    /// Three steps, none holding a lock across a bucket call:
    /// 1. [`Self::claim_completion`] checks the parts and marks the row
    ///    'completing' in a short transaction. From then on an abort, a
    ///    part reservation or report, a delete of the replaced file and
    ///    every sweep see the mark and leave the upload alone.
    /// 2. [`Self::drive_completion`] has the bucket assemble the object.
    /// 3. [`Self::finish_completion`] folds the result into the rows.
    ///
    /// A complete that finds the row already 'completing' (a retry, or a
    /// second caller racing the first) drives the same completion: the
    /// drive is safe to run twice at once. A completion that fails without
    /// a verdict (the bucket unreachable, or refusing) stays 'completing'
    /// and answers [`RuntimeStoreError::Completing`]; the row never goes
    /// back to 'pending', since another drive may be landing its bytes. A
    /// crashed, dropped or refused completion is driven by the expiry loop
    /// once the claim is [`CLAIM_LEASE_SECS`] old ([`Self::expire`]), and
    /// that drive gives the verdict.
    pub async fn complete_upload(
        &self,
        caller: &CallerAuth,
        key: &str,
    ) -> StoreResult<StoredFileMeta> {
        Self::wall_key(caller, key)?;
        self.claim_completion(key).await?;
        match self.drive_completion(key, false).await {
            // Past the claim, a failure that is not a verdict leaves the
            // upload 'completing': the caller asks again (or the sweep
            // finishes it), so it hears exactly that.
            Err(RuntimeStoreError::Other(e)) => {
                tracing::error!(
                    target: "weft_broker::runtime_store",
                    key = %key, error = format!("{e:#}"),
                    "a claimed completion did not land yet; it stays 'completing'"
                );
                Err(RuntimeStoreError::Completing(format!(
                    "upload '{key}' is completing but has not landed yet; call complete again"
                )))
            }
            other => other,
        }
    }

    /// Step 1 of a completion: check every reserved part landed (and
    /// slices the declared total), then mark the row 'completing' with the
    /// claim time in `progressed_at_unix`. A replacement also locks the
    /// file it replaces, which must still be active: a delete or sweep of
    /// that file locks it too and then refuses on the mark, so the two
    /// cannot cross. A row already 'completing' is left as it is; a key
    /// with no in-flight row is answered by the drive (a retry or unknown).
    async fn claim_completion(&self, key: &str) -> StoreResult<()> {
        let mut conn = self.pool.get().await.context("runtime complete_upload: begin claim").map_err(RuntimeStoreError::BeforeWrites)?;
        let tx = conn.transaction().await.context("runtime complete_upload: begin claim").map_err(RuntimeStoreError::BeforeWrites)?;
        let Some(pending) = Self::pending_row_locked(&tx, key)
            .await
            .map_err(RuntimeStoreError::Other)?
        else {
            return Ok(());
        };
        if pending.status == "completing" {
            return Ok(());
        }
        if let Some(replaced) = &pending.replaces {
            let status: Option<String> = tx
                .scalar_opt("SELECT status FROM runtime_file WHERE key = $1 FOR UPDATE", &[replaced])
                .await
                .context("runtime complete_upload: lock the replaced file")
                    .map_err(RuntimeStoreError::Other)?;
            if status.as_deref() != Some("active") {
                // The file was deleted or expired while the new bytes were
                // on their way: the replacement has nothing left to land
                // on, so it is ended here like an abort.
                tx.execute(&fence_sql(""), &[&key, &self.clock.now_unix()])
                    .await
                    .context("runtime complete_upload: fence an orphaned replacement")
                    .map_err(RuntimeStoreError::Other)?;
                tx.commit()
                    .await
                    .context("runtime complete_upload: commit orphaned replacement fence")
                    .map_err(RuntimeStoreError::Other)?;
                drop(conn);
                self.reap_fenced(&pending.sweep_entry(key)).await.map_err(RuntimeStoreError::Other)?;
                return Err(RuntimeStoreError::NotFound(format!(
                    "'{replaced}' was removed while its replacement '{key}' was uploading; the \
                     replacement was cancelled"
                )));
            }
        }
        let parts = Self::completion_parts(&tx, key).await?;
        if parts.is_empty() {
            // A declared NON-zero size with no parts is an incomplete upload,
            // not an empty file.
            if let Some(declared) = pending.declared_size.filter(|d| *d > 0) {
                return Err(RuntimeStoreError::Invalid(format!(
                    "upload '{key}' declared {declared} bytes but no parts were uploaded; \
                     resume the upload to finish it, or abort it"
                )));
            }
        } else {
            let missing: Vec<i32> =
                parts.iter().filter(|(_, etag, _)| etag.is_none()).map(|(n, _, _)| *n).collect();
            if !missing.is_empty() {
                return Err(RuntimeStoreError::Invalid(format!(
                    "upload '{key}' is incomplete: parts {missing:?} were never uploaded; \
                     resume the upload to finish them, or abort it"
                )));
            }
            let total: u64 = parts.iter().map(|(_, _, s)| *s as u64).sum();
            if let Some(declared) = pending.declared_size {
                if total != declared as u64 {
                    return Err(RuntimeStoreError::Invalid(format!(
                        "upload '{key}' reserved {total} bytes of a declared {declared}; \
                         upload the remaining parts before completing"
                    )));
                }
            }
        }
        tx.execute(
            "UPDATE runtime_file SET status = 'completing', progressed_at_unix = $2 \
             WHERE key = $1 AND status = 'pending'",
            &[&key, &self.clock.now_unix()],
        )
        .await
        .context("runtime complete_upload: mark completing")
        .map_err(RuntimeStoreError::Other)?;
        tx.commit()
            .await
            .context("runtime complete_upload: commit claim")
            .map_err(RuntimeStoreError::Other)?;
        Ok(())
    }

    /// An upload's part rows, ascending: (number, etag once landed, size).
    async fn completion_parts(db: &impl Client, key: &str) -> StoreResult<Vec<(i32, Option<String>, i64)>> {
        db.all(
            "SELECT part_number, etag, size_bytes FROM runtime_file_part \
             WHERE key = $1 ORDER BY part_number",
            &[&key],
        )
        .await
        .context("runtime complete_upload: read parts")
        .map_err(RuntimeStoreError::Other)
    }

    /// Steps 2 and 3 of a completion, for a row marked 'completing': get
    /// the object assembled ([`Self::bucket_completion`]), then fold it into
    /// the rows ([`Self::finish_completion`]). Safe to run while another
    /// drive of the same upload runs: both see the multipart open, the
    /// bucket completes it once, and the other drive's completion fails
    /// with `NoSuchUpload`, after which it finds the upload gone and reads
    /// the object; the rows fold once. With no 'completing' row, answers as
    /// a retry of a finished completion.
    ///
    /// `recovering` is the drive of a claim past its lease
    /// ([`Self::expire`]): a bucket refusal then is final (the
    /// claim's own drive already had its chance), where a live drive's
    /// refusal is left for that one.
    ///
    /// A page's upload through a slot is held to its slot's type before it
    /// lands ([`Self::landed_mime`]), so whichever drive lands it, it waits
    /// checked; refused, it goes. A run's copy of a slot's upload lands only
    /// with the claim that made it ([`Self::claim_uploads`]): a live drive
    /// leaves it, and past its lease that claim died, so the copy goes.
    async fn drive_completion(&self, key: &str, recovering: bool) -> StoreResult<StoredFileMeta> {
        let row: Option<PendingUpload> = self
            .pool
            .opt(&format!("{PENDING_ROW_SELECT_ANY} AND status = 'completing'"), &[&key])
            .await
            .context("runtime complete_upload: read the completing row")
        .map_err(RuntimeStoreError::Other)?;
        let Some(pending) = row else {
            return Self::completed_meta(&self.pool, key).await?.ok_or_else(|| {
                RuntimeStoreError::NotFound(format!(
                    "no upload in flight for key '{key}' (never begun, aborted, swept, or a \
                     replacement a later one has since overtaken)"
                ))
            });
        };
        if let Some(upload) = &pending.receives {
            if !recovering {
                return Err(RuntimeStoreError::Other(anyhow::anyhow!(
                    "runtime complete_upload: a run is still receiving '{upload}' into '{key}'"
                )));
            }
            self.end_completion(key, &pending, false).await?;
            self.ring_for_received(upload).await.map_err(RuntimeStoreError::Other)?;
            return Err(RuntimeStoreError::Lost(format!(
                "the run receiving '{upload}' into '{key}' stopped before it did; its copy was removed, and the upload \
                 waits for a run to receive it"
            )));
        }
        // The parts cannot change now: reserving and reporting refuse a
        // 'completing' row. A row with no parts is an empty file, or one an
        // in-bucket copy fills to its declared size.
        let parts = Self::completion_parts(&self.pool, key).await?;
        let total: u64 = match parts.is_empty() {
            true => pending.declared_size.unwrap_or(0) as u64,
            false => parts.iter().map(|(_, _, s)| *s as u64).sum(),
        };
        match self.bucket_completion(key, &pending, &parts, total, recovering).await? {
            Bucket::Assembled(actual) => {
                let landed = match self.held_to_its_address(key, &pending, actual).await {
                    Ok(()) => self.landed_mime(key, &pending, actual).await,
                    Err(e) => Err(e),
                };
                match landed {
                    Ok(mime) => self.finish_completion(key, &pending, total, actual, &mime).await,
                    Err(refused @ (RuntimeStoreError::Invalid(_) | RuntimeStoreError::NotItsContent(_))) => {
                        self.end_completion(key, &pending, false).await?;
                        Err(refused)
                    }
                    Err(e) => Err(e),
                }
            }
            Bucket::Lost { why, overwritten } => self.lose_completion(key, &pending, &why, overwritten).await,
        }
    }

    /// Hold an assembled ASSET upload to its key: an asset's id is the
    /// sha256 of its bytes, which every reader and every later publish of
    /// the same content trusts without looking, so bytes that hash to
    /// anything else are refused ([`RuntimeStoreError::NotItsContent`]) and
    /// the caller ends the upload, freeing the key for a correct one.
    /// Uploads into any other scope have no content address to hold.
    async fn held_to_its_address(&self, key: &str, pending: &PendingUpload, size: u64) -> StoreResult<()> {
        let parsed = weft_core::storage::key::parse_key(key)
            .map_err(|e| RuntimeStoreError::Other(anyhow::anyhow!("a completing upload's key '{key}': {e}")))?;
        if parsed.scope != weft_core::storage::key::KeyScope::Asset {
            return Ok(());
        }
        let actual = self
            .object_sha256(key, &pending.object_key(key), size)
            .await
            .context("runtime complete_upload: hash an assembled asset")
            .map_err(RuntimeStoreError::Other)?;
        if actual != parsed.id {
            return Err(RuntimeStoreError::NotItsContent(format!(
                "the bytes uploaded as '{}' (asset '{key}') hash to {actual}, not to {}, the content \
                 its key names; the upload was ended, so upload the file again",
                pending.filename, parsed.id
            )));
        }
        Ok(())
    }

    /// The type a landed upload is stored as: the one it was begun as, or
    /// for a page's upload through a slot, read off its first bytes
    /// within the type its slot accepts ([`weft_core::storage::received_file`]),
    /// which fills in a type the page did not state. Every landing of a
    /// slot's upload runs it before the row waits ('uploaded'), so none
    /// waits unchecked. Refused, the answer is [`RuntimeStoreError::Invalid`]
    /// in words a page can show, and the caller ends the upload. An empty
    /// file has no first bytes to read, and is held to the type as such (a
    /// place that takes any file takes it).
    async fn landed_mime(&self, key: &str, pending: &PendingUpload, size: u64) -> StoreResult<String> {
        let Some(accepts) = &pending.slot_accepts else { return Ok(pending.mime_type.clone()) };
        let accepts = weft_core::storage::slot::accepted_type(accepts).map_err(|why| {
            RuntimeStoreError::Other(anyhow::anyhow!("the slot of the upload '{key}' accepts no file type: {why}"))
        })?;
        let head = match size {
            0 => bytes::Bytes::new(),
            _ => self
                .bucket
                .get_range(&pending.object_key(key), 0, SNIFF_BYTES)
                .await
                .context("runtime complete_upload: read a slot upload's first bytes")
                .map_err(RuntimeStoreError::Other)?
                .ok_or_else(|| RuntimeStoreError::Other(anyhow::anyhow!("the upload '{key}' landed and has no object")))?,
        };
        weft_core::storage::received_file(&head, Some(&pending.mime_type), &accepts)
            .map(|(fitted, _)| fitted)
            .map_err(|why| RuntimeStoreError::Invalid(format!("the file uploaded is not what this upload takes ({accepts}): {why}")))
    }

    /// Step 2: what the bucket made of the upload.
    ///
    /// The bucket is asked first whether the multipart is still open
    /// (`multipart_exists`), so a completion is never sent for an upload
    /// already completed. Open: complete it. Gone: since nothing aborts an
    /// upload marked 'completing', a drive completed it, and the object's
    /// current size is read; an object of any other size means it was
    /// aborted outside weft (the bucket's incomplete-upload lifecycle
    /// rule), or completed wrong, and that is a [`Bucket::Lost`] verdict.
    /// A completion the bucket refuses while the upload stays open is an
    /// error (the row stays 'completing') unless `recovering`, where it is
    /// final. An error from the bucket itself is never a verdict.
    async fn bucket_completion(
        &self,
        key: &str,
        pending: &PendingUpload,
        parts: &[(i32, Option<String>, i64)],
        total: u64,
        recovering: bool,
    ) -> StoreResult<Bucket> {
        let object = pending.object_key(key);
        if parts.is_empty() && total > 0 {
            // Filled by an in-bucket copy ([`Self::fill_by_copy`]), which
            // lands its object whole or not at all: whole, it is the file;
            // otherwise the copy is still running, unless the claim is past
            // its lease, when the copy is taken to have died.
            return match self.bucket.size(&object).await.context("runtime complete_upload: read the copy's size").map_err(RuntimeStoreError::Other)? {
                Some(size) if size == total => Ok(Bucket::Assembled(size)),
                _ if recovering => Ok(Bucket::Lost {
                    why: "the in-bucket copy filling it never landed".into(),
                    overwritten: false,
                }),
                _ => Err(RuntimeStoreError::Other(anyhow::anyhow!(
                    "runtime complete_upload: an in-bucket copy is still filling '{key}'"
                ))),
            };
        }
        if parts.is_empty() {
            // Empty object: multipart cannot make a zero-byte object (a
            // part is never empty), so the (empty) multipart is dropped and
            // the object written directly. Both steps are safe to repeat.
            if let Some(upload_id) = &pending.upload_id {
                self.bucket
                    .abort_multipart(&object, upload_id)
                    .await
                    .context("runtime complete_upload: abort empty multipart")
                    .map_err(RuntimeStoreError::Other)?;
            }
            self.bucket
                .put(&object, bytes::Bytes::new())
                .await
                .context("runtime complete_upload: write empty object")
                .map_err(RuntimeStoreError::Other)?;
            return Ok(Bucket::Assembled(0));
        }
        let Some(upload_id) = pending.upload_id.clone() else {
            // Parts with no multipart handle: nothing can assemble them, and
            // nothing was ever written to the object.
            return Ok(Bucket::Lost {
                why: "its row carries parts but no multipart upload id".into(),
                overwritten: false,
            });
        };
        let open = self
            .bucket
            .multipart_exists(&object, &upload_id)
            .await
            .context("runtime complete_upload: ask the bucket whether the upload is open")
            .map_err(RuntimeStoreError::Other)?;
        if open {
            let etags: Vec<(i32, String)> = parts
                .iter()
                .map(|(n, etag, _)| (*n, etag.clone().expect("a claimed upload has every etag")))
                .collect();
            match self.bucket.complete_multipart(&object, &upload_id, &etags).await {
                Ok(size) => return Ok(Bucket::Assembled(size)),
                Err(refused) => {
                    let still_open = self
                        .bucket
                        .multipart_exists(&object, &upload_id)
                        .await
                        .context("runtime complete_upload: ask the bucket whether a failed completion left the upload open")
                        .map_err(RuntimeStoreError::Other)?;
                    if still_open {
                        if recovering {
                            // Refused again past the lease, still open: the
                            // multipart never completed, so the object was
                            // never touched.
                            return Ok(Bucket::Lost {
                                why: format!("the bucket refuses to complete it: {refused:#}"),
                                overwritten: false,
                            });
                        }
                        return Err(RuntimeStoreError::Other(refused.context(format!(
                            "runtime complete_upload: the bucket refused to complete '{key}'; it \
                             stays 'completing' and the expiry loop retries it once the claim lapsed"
                        ))));
                    }
                    // Gone after a failure: a concurrent drive completed it.
                }
            }
        }
        let size = self
            .bucket
            .size(&object)
            .await
            .context("runtime complete_upload: read the completed object's size")
            .map_err(RuntimeStoreError::Other)?;
        match size {
            Some(size) if size == total => Ok(Bucket::Assembled(size)),
            other => {
                // For a replacement the object is the replaced file's: it
                // still holds that file's bytes when its size is the one the
                // file's row records, else something rewrote it.
                let overwritten = match &pending.replaces {
                    None => true,
                    Some(replaced) => {
                        let recorded: Option<i64> = self
                            .pool
                            .scalar_opt("SELECT size_bytes FROM runtime_file WHERE key = $1", &[replaced])
                            .await
                            .context("runtime complete_upload: read the replaced file's size")
                        .map_err(RuntimeStoreError::Other)?;
                        recorded.map(|r| r as u64) != other
                    }
                };
                Ok(Bucket::Lost {
                    why: format!(
                        "the multipart is gone from the bucket but the object is {other:?} bytes, \
                         not the {total} it was completing to (aborted outside weft, likely by the \
                         bucket's incomplete-upload lifecycle rule, or completed wrong)"
                    ),
                    overwritten,
                })
            }
        }
    }

    /// The final verdict on a completion the bucket cannot account for:
    /// logged as an error naming the key and why, and the upload ended
    /// ([`Self::end_completion`]). A replacement's file keeps its row when its object was never
    /// overwritten (the multipart never completed, so the bucket never
    /// swapped the object); when it may have been (`overwritten`), the file
    /// goes too, so no row states a size its object does not have.
    async fn lose_completion(
        &self,
        key: &str,
        pending: &PendingUpload,
        why: &str,
        overwritten: bool,
    ) -> StoreResult<StoredFileMeta> {
        tracing::error!(
            target: "weft_broker::runtime_store",
            key = %key, replaces = ?pending.replaces, why = %why,
            "a completion cannot be accounted for; the upload is ended"
        );
        self.end_completion(key, pending, overwritten).await?;
        let lost = match (&pending.replaces, overwritten) {
            (Some(replaced), true) => format!("; the file it replaced, '{replaced}', is gone with it"),
            (Some(replaced), false) => format!("; '{replaced}' keeps its old content"),
            (None, _) => String::new(),
        };
        Err(RuntimeStoreError::Lost(format!("upload '{key}' could not be completed ({why}){lost}; upload the file again")))
    }

    /// End a claimed completion that will not land: the upload fenced to
    /// 'reaping' and reaped like an abort, freeing its reservation, and
    /// with it, when `overwritten`, the file it replaces. An upload no
    /// longer 'completing' was landed or ended by another call meanwhile,
    /// and is left to it.
    async fn end_completion(&self, key: &str, pending: &PendingUpload, overwritten: bool) -> StoreResult<()> {
        let mut conn = self
            .pool
            .get()
            .await
            .context("runtime complete_upload: begin the end of a completion")
            .map_err(RuntimeStoreError::Other)?;
        let tx = conn
            .transaction()
            .await
            .context("runtime complete_upload: begin the end of a completion")
            .map_err(RuntimeStoreError::Other)?;
        let now = self.clock.now_unix();
        let fenced = tx
            .execute(&fence_sql("AND status = 'completing'"), &[&key, &now])
            .await
            .context("runtime complete_upload: fence an ended completion")
            .map_err(RuntimeStoreError::Other)?
            == 1;
        if !fenced {
            return Ok(());
        }
        let mut doomed = vec![pending.sweep_entry(key)];
        if let (true, Some(replaced)) = (overwritten, &pending.replaces) {
            tx.execute(&fence_sql(""), &[replaced, &now])
                .await
                .context("runtime complete_upload: fence the file an ended completion overwrote")
                .map_err(RuntimeStoreError::Other)?;
            doomed.push(SweepEntry::file(replaced));
        }
        tx.commit()
            .await
            .context("runtime complete_upload: commit the end of a completion")
            .map_err(RuntimeStoreError::Other)?;
        drop(conn);
        // The upload's own row first (it ends the multipart), then the
        // replaced file's (it deletes the object both share).
        for entry in &doomed {
            if let Err(e) = self.reap_fenced(entry).await {
                tracing::error!(
                    target: "weft_broker::runtime_store",
                    key = %entry.key, error = %e,
                    "failed to reap an ended completion; its row stays marked ('reaping') until a wipe, its run's end or its \
                     tenant running short of space tries it again"
                );
            }
        }
        Ok(())
    }

    /// Step 3: fold an assembled object into the rows, in one short
    /// transaction locking the upload's row and, for a replacement, the
    /// file it replaces. A new upload flips active ([`Self::flip_active`]);
    /// a replacement folds into its file ([`Self::fold_replacement`]).
    ///
    /// A row no longer 'completing' means a concurrent drive folded it
    /// first, and its result is the answer.
    ///
    /// An object whose size differs from the reservation (signed into
    /// every part's URL, so only a bucket anomaly) was never charged: it
    /// gets the [`Self::lose_completion`] verdict, which fences the rows
    /// before removing the object, so no row ever points at a deleted one.
    async fn finish_completion(
        &self,
        key: &str,
        pending: &PendingUpload,
        total: u64,
        actual: u64,
        mime: &str,
    ) -> StoreResult<StoredFileMeta> {
        let mut conn = self.pool.get().await.context("runtime complete_upload: begin fold").map_err(RuntimeStoreError::Other)?;
        let tx = conn.transaction().await.context("runtime complete_upload: begin fold").map_err(RuntimeStoreError::Other)?;
        let row = Self::pending_row_locked(&tx, key).await.map_err(RuntimeStoreError::Other)?;
        if row.is_none_or(|r| r.status != "completing") {
            let done = Self::completed_meta(&tx, key).await?;
            return done.ok_or_else(|| {
                RuntimeStoreError::Other(anyhow::anyhow!(
                    "the bucket completed '{key}' but its row is gone and no file records it; \
                     the object at '{}' has no row",
                    pending.object_key(key)
                ))
            });
        }
        if let Some(replaced) = &pending.replaces {
            tx.execute("SELECT 1 FROM runtime_file WHERE key = $1 FOR UPDATE", &[replaced])
                .await
                .context("runtime complete_upload: lock the replaced file")
                .map_err(RuntimeStoreError::Other)?;
        }
        if actual != total {
            drop(tx);
            drop(conn);
            return self
                .lose_completion(
                    key,
                    pending,
                    &format!("the assembled object is {actual} bytes but {total} were reserved"),
                    true,
                )
                .await;
        }
        let now = self.clock.now_unix();
        // A replaced file's expiry is renewed, never made sooner. It is
        // rung all the same: the expiry loop leaves a file out of its next
        // look while its replacement completes, so no look may be booked
        // for it.
        let meta = match &pending.replaces {
            Some(replaced) => Self::fold_replacement(&tx, key, replaced, actual, now).await?,
            None => Self::flip_active(&tx, key, pending, actual, mime, now).await?,
        };
        tx.commit()
            .await
            .context("runtime complete_upload: commit fold")
            .map_err(RuntimeStoreError::Other)?;
        drop(conn);
        self.ring_for(meta.expires_at_unix).await;
        Ok(meta)
    }

    /// Fold a completed replacement into the file it replaced: the bucket
    /// already swapped that file's object for the new bytes, so its row
    /// takes the new size (and the charge that goes with it), its expiry is
    /// renewed (a replace is an access), it records this upload as its last
    /// replacement (what a retried complete finds), and the replacement's
    /// own row goes, its charge and part rows with it. The replaced file is
    /// active: nothing removes a file whose replacement is completing.
    async fn fold_replacement(
        tx: &Transaction<'_>,
        key: &str,
        replaced: &str,
        actual: u64,
        now: i64,
    ) -> StoreResult<StoredFileMeta> {
        tx.execute("DELETE FROM runtime_file WHERE key = $1 AND status = 'completing'", &[&key])
            .await
            .context("runtime replace: drop the replacement's row")
            .map_err(RuntimeStoreError::Other)?;
        let folded: FileRow = tx
            .opt(
                &format!(
                    "UPDATE runtime_file SET size_bytes = $2, reserved_bytes = $2, version = version + 1, \
                         last_replacement = $4, \
                         expires_at_unix = CASE WHEN keep_ttl_secs IS NULL THEN expires_at_unix ELSE {} END \
                     WHERE key = $1 AND status = 'active' \
                     RETURNING {FILE_ROW_COLUMNS}",
                    expiry_honoring_links("$3 + keep_ttl_secs")
                ),
                &[&replaced, &(actual as i64), &now, &key],
            )
            .await
            .context("runtime replace: fold the new size into the replaced file")
            .map_err(RuntimeStoreError::Other)?
            .ok_or_else(|| {
                RuntimeStoreError::Other(anyhow::anyhow!(
                    "'{replaced}' is no longer active although its replacement '{key}' was \
                     completing, which every removal refuses; its object now holds the new bytes"
                ))
            })?;
        Ok(folded.to_meta())
    }

    /// Flip a completed upload's row to active with its final size and drop
    /// its part rows. The expiry is stamped NOW (completion is when a kept
    /// file starts existing), unless the row already carries one: the
    /// run sweep's linger stamp on an upload still completing when
    /// its run ended ([`Self::sweep_run`]), or a slot's end. An upload
    /// through a slot lands 'uploaded' instead, waiting for the run that
    /// receives it ([`Self::claim_uploads`]), as the type `mime` its
    /// landing found ([`Self::landed_mime`]). `reserved_bytes` is set to the
    /// final size so the tenant's charged sum is identical before and after
    /// the flip.
    async fn flip_active(
        tx: &Transaction<'_>,
        key: &str,
        pending: &PendingUpload,
        actual: u64,
        mime: &str,
        now: i64,
    ) -> StoreResult<StoredFileMeta> {
        let expires_at = pending.keep_ttl_secs.map(|s| now + s);
        let flipped: FileRow = tx
            .one(
                &format!(
                    "UPDATE runtime_file SET \
                       size_bytes = $2, status = CASE WHEN upload_slot IS NULL THEN 'active' ELSE 'uploaded' END, \
                       expires_at_unix = COALESCE(expires_at_unix, $3), mime_type = $4, \
                       upload_id = NULL, part_size = NULL, declared_size = NULL, reserved_bytes = $2 \
                     WHERE key = $1 AND status = 'completing' \
                     RETURNING {FILE_ROW_COLUMNS}"
                ),
                &[&key, &(actual as i64), &expires_at, &mime],
            )
            .await
        .context("runtime complete_upload: flip active (the row is locked by this transaction)")
        .map_err(RuntimeStoreError::Other)?;
        tx.execute("DELETE FROM runtime_file_part WHERE key = $1", &[&key])
            .await
            .context("runtime complete_upload: drop part rows")
            .map_err(RuntimeStoreError::Other)?;
        Ok(flipped.to_meta())
    }

    /// Where each of this upload's parts starts in the file.
    ///
    /// Known-size uploads allow reserving parts out of order. Their offsets
    /// come from the declared layout, never from which rows arrived first.
    /// Streams reserve a contiguous prefix and only their final part is short.
    #[allow(clippy::type_complexity)]
    async fn part_offsets(
        &self,
        key: &str,
        pending: &PendingUpload,
    ) -> anyhow::Result<(std::collections::BTreeMap<i32, u64>, std::collections::BTreeMap<i32, u64>, Vec<(i32, i64)>)> {
        let rows: Vec<(i32, i64, bool)> = self
            .pool
            .all(
                "SELECT part_number, size_bytes, etag IS NULL FROM runtime_file_part \
                 WHERE key = $1 ORDER BY part_number",
                &[&key],
            )
            .await
            .context("runtime part_offsets: read part sizes")?;
        let mut at = 0u64;
        let mut offsets = std::collections::BTreeMap::new();
        let mut sizes = std::collections::BTreeMap::new();
        let mut missing = Vec::new();
        let part_size = pending.part_size(key)?;
        for (number, size, unfinished) in rows {
            offsets.insert(number, if pending.declared_size.is_some() {
                (number as u64 - 1) * part_size
            } else { at });
            sizes.insert(number, size as u64);
            if unfinished { missing.push((number, size)); }
            at += size as u64;
        }
        Ok((offsets, sizes, missing))
    }

    /// Resume an interrupted upload: re-presign exactly the reserved parts
    /// that were never reported done. Returns the upload's part size + those
    /// parts. (A part that was uploaded but whose done-report was lost is
    /// simply re-uploaded: its etag is only trusted from our own records.)
    pub async fn resume_upload(
        &self,
        caller: &CallerAuth,
        key: &str,
        entitlements: &dyn EntitlementSource,
        audience: &PresignAudience,
    ) -> StoreResult<(u64, Vec<PresignedPart>, u64)> {
        Self::wall_key(caller, key)?;
        let conn = self.pool.get().await.context("runtime resume_upload: begin read").map_err(RuntimeStoreError::BeforeWrites)?;
        let pending = Self::pending_row(&conn, key)
            .await
            .map_err(RuntimeStoreError::Other)?;
        drop(conn);
        if let Some(pending) = &pending {
            pending.refuse_if_completing(key)?;
        }
        let Some(pending) = pending else {
            return match self.row(key).await.map_err(RuntimeStoreError::Other)? {
                Some(_) => Err(RuntimeStoreError::Invalid(format!(
                    "upload '{key}' already completed; nothing to resume"
                ))),
                None => Err(RuntimeStoreError::NotFound(format!(
                    "no upload in flight for key '{key}'"
                ))),
            };
        };
        let upload_id = pending.upload_id.clone().ok_or_else(|| {
            RuntimeStoreError::Other(anyhow::anyhow!(
                "pending row for '{key}' unexpectedly has no upload id (a committed \
                 pending row always carries one); abort this upload and begin again"
            ))
        })?;
        let part_size = pending.part_size(key).map_err(RuntimeStoreError::Other)?;
        // Signing part links is progress: a PUT may start until they die, so
        // the abandoned-pending clock starts from now ([`abandoned_at`]).
        let alive = self
            .pool
            .execute("UPDATE runtime_file SET progressed_at_unix = $2 WHERE key = $1 AND status = 'pending'", &[&key, &self.clock.now_unix()])
            .await
            .context("runtime resume_upload: bump progress clock")
            .map_err(RuntimeStoreError::BeforeWrites)?;
        if alive == 0 {
            return Err(RuntimeStoreError::Invalid(format!(
                "upload '{key}' is no longer pending (a completion claimed it, or it was swept); call complete, \
                 or begin the upload again"
            )));
        }
        // Missing parts and the resume position must describe the same rows.
        // A reservation arriving between separate reads could otherwise move
        // the reader past bytes that neither publisher has uploaded.
        let (offsets, sizes, missing) = self
            .part_offsets(key, &pending)
            .await
            .map_err(RuntimeStoreError::Other)?;
        let mut parts = Vec::with_capacity(missing.len());
        if let Some(declared) = pending.declared_size {
            // Reservation order can leave holes before the highest part. Fill
            // them through normal reservation before returning the resume
            // prefix, so a forward-only reader never skips unreserved bytes.
            let highest = sizes.keys().next_back().copied().unwrap_or(0);
            let gaps: Vec<_> = (1..=highest).filter(|number| !sizes.contains_key(number)).map(|number| {
                let offset = (number as u64 - 1) * part_size;
                PartAsk { part_number: number, size_bytes: part_size.min(declared as u64 - offset) }
            }).collect();
            if !gaps.is_empty() {
                // After the progress clock this resume moved above.
                parts.extend(self.reserve_parts(caller, key, &gaps, entitlements, audience).await.map_err(RuntimeStoreError::after_writes)?);
            }
        }
        for (part_number, size) in missing {
            let offset_bytes = *offsets.get(&part_number).ok_or_else(|| {
                RuntimeStoreError::Other(anyhow::anyhow!(
                    "part {part_number} of '{key}' is missing its own row, so where its bytes \
                     belong in the file cannot be stated; abort this upload and begin again"
                ))
            })?;
            let url = self
                .bucket
                .presign_part(
                    &pending.object_key(key),
                    &upload_id,
                    part_number,
                    size as u64,
                    audience,
                    PART_LINK_SECS,
                )
                .await
                .context("runtime resume_upload: presign missing part")
                .map_err(RuntimeStoreError::Other)?;
            parts.push(PresignedPart { part_number, size_bytes: size as u64, offset_bytes, url });
        }
        // Everything already carved into parts, which is where new ones
        // begin. `offsets` holds every part row, so the end of the last
        // one plus its size is that length.
        let reserved_bytes = offsets
            .iter()
            .next_back()
            .map(|(number, at)| at + sizes.get(number).copied().unwrap_or(0))
            .unwrap_or(0);
        parts.sort_by_key(|part| part.part_number);
        Ok((part_size, parts, reserved_bytes))
    }

    /// Cancel an in-flight upload. Idempotent: a key with no in-flight
    /// upload and no file is already in the aborted state. A COMPLETED file
    /// is not abortable (delete it instead), and neither is an upload a
    /// completion has claimed: the bucket may already hold its object.
    ///
    /// The row is fenced to 'reaping' first, under its lock, so no
    /// completion can claim it afterwards; then the bucket's multipart is
    /// aborted and the row deleted (freeing the reservation; the part rows
    /// cascade), exactly like a sweep's reap. A crash in between leaves a
    /// 'reaping' row the expiry loop finishes once its claim lapsed; a
    /// bucket that refuses leaves it marked ([`MARKED`]).
    pub async fn abort_upload(&self, caller: &CallerAuth, key: &str) -> StoreResult<()> {
        Self::wall_key(caller, key)?;
        let mut conn = self.pool.get().await.context("runtime abort_upload: begin tx").map_err(RuntimeStoreError::BeforeWrites)?;
        let tx = conn.transaction().await.context("runtime abort_upload: begin tx").map_err(RuntimeStoreError::BeforeWrites)?;
        let pending = Self::pending_row_locked(&tx, key)
            .await
            .map_err(RuntimeStoreError::Other)?;
        let Some(pending) = pending else {
            drop(tx);
            drop(conn);
            return match self.row(key).await.map_err(RuntimeStoreError::Other)? {
                Some(_) => Err(RuntimeStoreError::Invalid(format!(
                    "'{key}' already completed; delete the file instead of aborting"
                ))),
                None => Ok(()),
            };
        };
        pending.refuse_if_completing(key)?;
        tx.execute(&fence_sql(""), &[&key, &self.clock.now_unix()])
            .await
            .context("runtime abort_upload: fence the row")
            .map_err(RuntimeStoreError::Other)?;
        tx.commit()
            .await
            .context("runtime abort_upload: commit fence")
            .map_err(RuntimeStoreError::Other)?;
        drop(conn);
        self.reap_fenced(&pending.sweep_entry(key))
            .await
            .context("runtime abort_upload (the row stays marked in `weft status`; a wipe or the tenant running short of space \
                 tries it again)")
            .map_err(RuntimeStoreError::Other)
    }

    /// Fill the upload just begun under `key` with a copy of the stored
    /// object at `from`, made inside the bucket (no byte leaves it), and
    /// land it as a completed upload does ([`Self::flip_active`]), a page's
    /// upload through a slot held to its slot's type first
    /// ([`Self::landed_mime`]).
    ///
    /// The row is claimed for the copy first, as a completion claims it:
    /// 'completing', its multipart handle cleared (the multipart the begin
    /// opened is aborted next), so no begin resumes it, no part is sent to
    /// it and no sweep reaps it while the bucket copies ([`Self::copy_into`]).
    /// A drive of it meanwhile (a complete, or the expiry loop's drive of a
    /// claim past its lease) lands the copy once it is whole ([`Self::bucket_completion`]).
    /// Any failure ends the upload ([`Self::end_failed_copy`]), so nothing
    /// stays reserved for a copy that never happened.
    async fn fill_by_copy(&self, caller: &CallerAuth, key: &str, from: &str) -> StoreResult<StoredFileMeta> {
        let pending = match self.claim_for_copy(key, from).await {
            Ok(pending) => pending,
            Err(e) => {
                if let Err(abort) = self.abort_upload(caller, key).await {
                    tracing::error!(
                        target: "weft_broker::runtime_store", key = %key, error = %abort,
                        "a copy that could not claim its upload could not end it; the sweep of abandoned uploads removes it"
                    );
                }
                return Err(e);
            }
        };
        let filled = async {
            if let Some(upload_id) = &pending.upload_id {
                self.bucket
                    .abort_multipart(&object_key(key), upload_id)
                    .await
                    .context("runtime copy: drop the upload's multipart")
                    .map_err(RuntimeStoreError::Other)?;
            }
            let size = self.copy_into(from, key, pending.declared_size.unwrap_or(0) as u64, &[key]).await?;
            let mime = self.landed_mime(key, &pending, size).await?;
            let mut conn = self.pool.get().await.context("runtime copy: begin land").map_err(RuntimeStoreError::Other)?;
            let tx = conn.transaction().await.context("runtime copy: begin land").map_err(RuntimeStoreError::Other)?;
            let row = Self::pending_row_locked(&tx, key).await.map_err(RuntimeStoreError::Other)?;
            if row.is_none_or(|r| r.status != "completing") {
                drop(tx);
                drop(conn);
                // A drive landed the copy first, and its file is the answer.
                // Otherwise the row went while the bucket copied, which the
                // end of the failed copy cleans up after.
                return Self::completed_meta(&self.pool, key).await?.ok_or_else(|| {
                    RuntimeStoreError::Other(anyhow::anyhow!("the upload '{key}' a copy fills was ended while it copied"))
                });
            }
            let meta = Self::flip_active(&tx, key, &pending, size, &mime, self.clock.now_unix()).await?;
            tx.commit().await.context("runtime copy: commit land").map_err(RuntimeStoreError::Other)?;
            drop(conn);
            self.ring_for(meta.expires_at_unix).await;
            Ok(meta)
        }
        .await;
        if filled.is_err() {
            // The begin's multipart is aborted again (in case the copy
            // failed before it was).
            if let Err(end) = self.end_failed_copy(&pending.sweep_entry(key)).await {
                tracing::error!(
                    target: "weft_broker::runtime_store", key = %key, error = %end,
                    "a copy that failed could not end its upload; the expiry loop removes it once abandoned"
                );
            }
        }
        filled
    }

    /// Copy the stored object at `from` over the 'completing' row at `key`,
    /// inside the bucket, and answer the copy's size, refused unless it is
    /// `size`, what the row reserved. Each piece the bucket finishes renews
    /// the row's claim ([`CopyClaim`]), so a copy of any size runs with no
    /// transaction open and is never taken for dead while it moves. A copy
    /// that fails leaves its row for [`Self::end_failed_copy`]. `held` is
    /// every row the calling claim holds, renewed with it ([`CopyClaim`]).
    async fn copy_into(&self, from: &str, key: &str, size: u64, held: &[&str]) -> StoreResult<u64> {
        let claim = CopyClaim { pool: &self.pool, clock: self.clock.as_ref(), key, held };
        let copied = self
            .bucket
            .copy(&object_key(from), &object_key(key), &claim)
            .await
            .context("runtime copy: copy inside the bucket")
            .map_err(RuntimeStoreError::Other)?;
        if copied != size {
            return Err(RuntimeStoreError::Other(anyhow::anyhow!(
                "the copy of '{from}' is {copied} bytes, and {size} were reserved for it"
            )));
        }
        Ok(copied)
    }

    /// End the 'completing' row `entry` names after the copy filling it
    /// ([`Self::copy_into`]) or the copy's landing failed: fenced and
    /// reaped while it is still the copy's. A drive may have landed it
    /// meanwhile, and that file stays; or the row went while the bucket
    /// copied (the copy stopped when its claim found it gone), and an
    /// object the copy made is one no row points at
    /// ([`Self::drop_unrecorded_copy`]).
    async fn end_failed_copy(&self, entry: &SweepEntry) -> Result<()> {
        let fenced = self
            .pool
            .execute(&fence_sql("AND status = 'completing'"), &[&entry.key, &self.clock.now_unix()])
            .await
            .context("runtime copy: fence the failed copy")?
            == 1;
        match fenced {
            true => self.reap_fenced(entry).await,
            false => self.drop_unrecorded_copy(&entry.key).await,
        }
    }

    /// Claim the 'pending' upload under `key` for an in-bucket copy
    /// ([`Self::fill_by_copy`]) of the file at `from`: 'completing' with
    /// the claim time, as a completion marks it, no multipart handle, and
    /// the source's content hash (the same bytes), so whichever path lands
    /// the copy, the hash is on it. Answers the row as it was, its handle
    /// the multipart the copy aborts.
    async fn claim_for_copy(&self, key: &str, from: &str) -> StoreResult<PendingUpload> {
        let mut conn = self.pool.get().await.context("runtime copy: begin claim").map_err(RuntimeStoreError::Other)?;
        let tx = conn.transaction().await.context("runtime copy: begin claim").map_err(RuntimeStoreError::Other)?;
        let pending = Self::pending_row_locked(&tx, key)
            .await
            .map_err(RuntimeStoreError::Other)?
            .filter(|row| row.status == "pending")
            .ok_or_else(|| RuntimeStoreError::Other(anyhow::anyhow!("the upload '{key}' a copy fills is gone")))?;
        tx.execute(
            "UPDATE runtime_file SET status = 'completing', progressed_at_unix = $2, upload_id = NULL, \
             content_sha256 = (SELECT content_sha256 FROM runtime_file WHERE key = $3) WHERE key = $1",
            &[&key, &self.clock.now_unix(), &from],
        )
        .await
        .context("runtime copy: claim the upload")
        .map_err(RuntimeStoreError::Other)?;
        tx.commit().await.context("runtime copy: commit claim").map_err(RuntimeStoreError::Other)?;
        Ok(pending)
    }

    /// Copy the stored file at `from` into `scope` as a new file, inside
    /// the bucket: what `StorageHandle::copy` asks. The copy is charged and
    /// counted like any new file, keeps the source's type and name, and
    /// takes `keep` and `identity` as a put does (an identity the scope
    /// already holds answers that file and copies nothing).
    pub async fn copy_file(
        &self,
        caller: &CallerAuth,
        from: &ParsedKey,
        scope: &StorageScope,
        identity: Option<&str>,
        keep: Option<KeepTtl>,
        entitlements: &dyn EntitlementSource,
    ) -> StoreResult<StoredFileMeta> {
        if matches!(scope, StorageScope::Asset) {
            return Err(RuntimeStoreError::Denied(
                "the asset scope is managed by the pre-build asset sync; node code copies into run, project, shared or \
                 instance scope"
                    .into(),
            ));
        }
        let source = self.meta(from).await?;
        let spec = UploadSpec {
            scope,
            mime: &source.mime_type,
            filename: &source.filename,
            keep,
            declared_size: Some(source.size_bytes),
            content_hash: None,
            identity,
            slot: None,
            imported: None,
        };
        match self.begin_upload(caller, &spec, entitlements).await? {
            BeginUpload::Ready { key, .. } => self.fill_by_copy(caller, &key, &from.to_key()).await,
            BeginUpload::AlreadyStored { key } => Self::completed_meta(&self.pool, &key)
                .await?
                .ok_or_else(|| RuntimeStoreError::NotFound(key)),
            BeginUpload::Resume { key, .. } => Err(RuntimeStoreError::Conflict(format!(
                "a file with this identity is still uploading in this scope ('{key}'); retry once it lands"
            ))),
        }
    }

    /// Begin the one upload of a page's slot: `begin_upload` with the
    /// slot's grant, and, when the page states the content's sha256 and
    /// the tenant holds a file of that content weft hashed itself
    /// ([`Self::hash_upload`]), the upload made from that file inside the
    /// bucket and answered as stored: the page sends no byte. Only a hash
    /// weft computed is matched, so naming one takes holding its bytes.
    /// Only an active file is a source: another slot's upload still
    /// waiting to be received goes once a run receives it (or refuses it),
    /// so it could vanish under the copy.
    pub async fn begin_slot_upload(
        &self,
        caller: &CallerAuth,
        spec: &UploadSpec<'_>,
        entitlements: &dyn EntitlementSource,
    ) -> StoreResult<BeginUpload> {
        let Some(SlotUpload { sha256, .. }) = spec.slot else {
            return Err(RuntimeStoreError::Invalid("a slot's upload names its slot".into()));
        };
        if let Some(hash) = sha256 {
            if !weft_core::storage::is_content_hash(hash) {
                return Err(RuntimeStoreError::Invalid(format!("'{hash}' is not a sha256, 64 lowercase hex characters")));
            }
        }
        let tenant = match caller {
            CallerAuth::Worker { tenant, .. } => tenant.clone(),
            _ => return Err(RuntimeStoreError::Denied("a slot's upload is made for its project".into())),
        };
        let same: Option<String> = match sha256 {
            Some(hash) => self
                .pool
                .scalar_opt(
                    &format!(
                        "SELECT key FROM runtime_file WHERE tenant_id = $1 AND content_sha256 = $2 AND size_bytes = $3 \
                           AND status = 'active' AND {live} LIMIT 1",
                        live = unexpired("$4"),
                    ),
                    &[&tenant, &hash, &(spec.declared_size.unwrap_or(0) as i64), &self.clock.now_unix()],
                )
                .await
                .context("runtime begin_slot_upload: look for the same content")
                .map_err(RuntimeStoreError::BeforeWrites)?,
            None => None,
        };
        let begun = self.begin_upload(caller, spec, entitlements).await?;
        let (BeginUpload::Ready { key, .. }, Some(same)) = (&begun, same) else { return Ok(begun) };
        self.fill_by_copy(caller, key, &same).await?;
        Ok(BeginUpload::AlreadyStored { key: key.clone() })
    }

    /// Whether `key` is the upload of the slot `sid`: what every verb a
    /// page sends through its slot is held to, so a slot reaches its own
    /// upload and nothing else of its project.
    pub async fn is_slot_upload(&self, key: &str, sid: &str) -> StoreResult<bool> {
        self.pool
            .scalar("SELECT EXISTS (SELECT 1 FROM runtime_file WHERE key = $1 AND upload_slot = $2)", &[&key, &sid])
            .await
            .context("runtime slot upload check")
            .map_err(RuntimeStoreError::BeforeWrites)
    }

    /// Read a landed slot upload back from the bucket and record its
    /// sha256 ([`Self::object_sha256`]): what a later page sending the
    /// same content is matched against ([`Self::begin_slot_upload`]).
    pub async fn hash_upload(&self, key: &str) -> Result<()> {
        let size: Option<i64> = self
            .pool
            .scalar_opt("SELECT size_bytes FROM runtime_file WHERE key = $1 AND status = 'uploaded'", &[&key])
            .await
            .context("hash_upload: read the upload")?;
        let Some(size) = size else { return Ok(()) };
        let size = size as u64;
        let hash = self.object_sha256(key, &object_key(key), size).await.context("hash_upload")?;
        self.pool
            .execute("UPDATE runtime_file SET content_sha256 = $2 WHERE key = $1 AND size_bytes = $3", &[&key, &hash, &(size as i64)])
            .await
            .context("hash_upload: record the hash")?;
        Ok(())
    }

    /// The sha256 (hex) of the `size` bytes of `object`, the bucket object
    /// of the file `key`. Read in pieces, so a file of any size costs a
    /// piece of memory.
    async fn object_sha256(&self, key: &str, object: &str, size: u64) -> Result<String> {
        use sha2::Digest;
        let mut hasher = sha2::Sha256::new();
        let mut at = 0;
        while at < size {
            let end = (at + HASH_PIECE_BYTES).min(size);
            let piece = self
                .bucket
                .get_range(object, at, end)
                .await
                .with_context(|| format!("read bytes {at}..{end} of '{key}'"))?
                .ok_or_else(|| anyhow::anyhow!("'{key}' is gone from the bucket"))?;
            hasher.update(&piece);
            at = end;
        }
        Ok(weft_core::access::hex_of(&hasher.finalize()))
    }

    /// Receive the uploads of `slots`, pages' uploads, into the run
    /// `caller` drives: each file, now this run's to use, active in its
    /// slot's scope with its slot's lifetime, stored as the type its place
    /// takes it ([`weft_core::storage::fit_mime`]), in the order of
    /// `slots`. Every slot is received or none is: one transaction takes
    /// them all, so a refusal of any (named as the call sent it) leaves
    /// every slot open for a corrected call, and a copy made for a
    /// run-scoped one is deleted. A slot is received once: the same run
    /// asking again (a body run twice, or one call naming it twice) gets
    /// the same file when it fits there as the type it was received as,
    /// any other run is refused. Every upload waiting to be received was
    /// held to its slot's type when it landed ([`Self::landed_mime`]).
    ///
    /// A run-scoped slot's upload becomes a file of the run, copied inside
    /// the bucket into the run's own space before the transaction, with no
    /// lock held (a big copy takes a while), under a row of this call's
    /// copy that the copy's progress keeps claimed ([`Self::copy_into`]).
    /// While that row is there, nothing removes the upload it copies, so a
    /// slot ending meanwhile waits for the claim. The transaction then
    /// locks every row it changes, re-checks them, and flips them; the
    /// upload the copy came from goes once it commits, and so does every
    /// copy this call made that it did not receive (a same-run call
    /// received the slot first).
    pub async fn claim_uploads(&self, caller: &CallerAuth, slots: &[ClaimedSlot<'_>]) -> StoreResult<Vec<StoredFileMeta>> {
        let CallerAuth::Worker { tenant, project_id, run_id: Some(run), .. } = caller else {
            return Err(RuntimeStoreError::Denied("an upload is received by a run".into()));
        };
        let mut accepted = Vec::with_capacity(slots.len());
        for slot in slots {
            let named = |e: RuntimeStoreError| of_slot(slot.slot, e);
            // The slot came from the run's caller, so another project's is
            // theirs to fix, never this worker's credentials refused.
            if &slot.claims.tenant != tenant || &slot.claims.project != project_id {
                return Err(named(RuntimeStoreError::Conflict("this upload slot belongs to another project".into())));
            }
            let accepts = weft_core::storage::slot::accepted_type(&slot.claims.accepts)
                .map_err(|why| named(RuntimeStoreError::Invalid(why)))?;
            accepted.push(accepts);
        }
        let mut copies = Vec::new();
        let (files, moved) = match self.take_slots(caller, run, slots, &accepted, &mut copies).await {
            Ok(taken) => taken,
            Err(e) => {
                // Nothing this call did is recorded, so every copy it made
                // is ended. Once they are, the call has written nothing
                // that stays, so a `BeforeWrites` of a later step (a second
                // slot's look for its upload, the receiving transaction's
                // begin) is true of the whole call and keeps its kind; a
                // copy that cannot be ended makes it `Other`.
                for copy in &copies {
                    if let Err(drop) = self.end_run_copy(copy).await {
                        return Err(RuntimeStoreError::Other(anyhow::anyhow!(
                            "{e}, and the copy at '{}' it made could not be removed ({drop:#})",
                            copy.target
                        )));
                    }
                }
                return Err(e);
            }
        };
        for key in &moved {
            if let Err(e) = self.reap_fenced(&SweepEntry::file(key)).await {
                tracing::error!(
                    target: "weft_broker::runtime_store", key = %key, error = %e,
                    "a received upload's first copy could not be removed; its row stays marked ('reaping') until a wipe or \
                     its tenant running short of space tries it again"
                );
            }
        }
        for copy in copies.iter().filter(|copy| !moved.contains(&copy.upload)) {
            if let Err(e) = self.end_run_copy(copy).await {
                tracing::error!(
                    target: "weft_broker::runtime_store", key = %copy.target, error = %e,
                    "a run's copy another call of the run received first could not be removed; the expiry loop ends it \
                     once its claim lapses"
                );
            }
        }
        for file in &files {
            self.ring_for(file.expires_at_unix).await;
        }
        Ok(files)
    }

    /// End a run's copy this call made and did not receive
    /// ([`Self::claim_uploads`]), and ring for the upload it held.
    async fn end_run_copy(&self, copy: &RunCopy) -> Result<()> {
        self.end_failed_copy(&SweepEntry::file(&copy.target)).await?;
        self.ring_for_received(&copy.upload).await
    }

    /// The work of [`Self::claim_uploads`]: the run's copies first, each
    /// written on `copies` before it starts, then the one transaction
    /// receiving every slot. Answers the files and the uploads it fenced.
    /// An upload that landed after the copies were made has none yet: the
    /// transaction is given up, a second round copies it, and the
    /// transaction runs again. An upload once waiting waits until it is
    /// received or ends, so the second round finds every copy it needs.
    async fn take_slots(
        &self,
        caller: &CallerAuth,
        run: &str,
        slots: &[ClaimedSlot<'_>],
        accepted: &[weft_core::weft_type::WeftType],
        copies: &mut Vec<RunCopy>,
    ) -> StoreResult<(Vec<StoredFileMeta>, Vec<String>)> {
        self.make_run_copies(caller, run, slots, copies).await?;
        if let Some(taken) = self.receive_slots(run, slots, accepted, copies).await? {
            return Ok(taken);
        }
        self.make_run_copies(caller, run, slots, copies).await?;
        self.receive_slots(run, slots, accepted, copies).await?.ok_or_else(|| {
            RuntimeStoreError::Other(anyhow::anyhow!("runtime claim: a slot's upload still has no copy after a second round"))
        })
    }

    /// Copy every run-scoped slot's waiting upload that has no copy on
    /// `copies` yet into the run ([`Self::begin_run_copy`]). A slot named
    /// twice is one file, copied once. A copy the bucket fails is kept on
    /// `copies` with why ([`RunCopy`]), for the transaction to answer.
    async fn make_run_copies(
        &self,
        caller: &CallerAuth,
        run: &str,
        slots: &[ClaimedSlot<'_>],
        copies: &mut Vec<RunCopy>,
    ) -> StoreResult<()> {
        for slot in slots.iter().filter(|slot| matches!(slot.claims.scope, StorageScope::Run)) {
            if copies.iter().any(|copy| copy.sid == slot.claims.sid) {
                continue;
            }
            let named = |e: RuntimeStoreError| of_slot(slot.slot, e);
            let Some((copy, size)) = self.begin_run_copy(caller, run, &slot.claims.sid).await.map_err(named)? else {
                continue;
            };
            let (from, to) = (copy.upload.clone(), copy.target.clone());
            copies.push(copy);
            // Every copy this call made is held while this one runs.
            let held: Vec<&str> = copies.iter().map(|copy| copy.target.as_str()).collect();
            if let Err(e) = self.copy_into(&from, &to, size, &held).await {
                copies.last_mut().expect("the copy just written").failed = Some(format!("{e:#}"));
            }
        }
        Ok(())
    }

    /// The one transaction receiving every slot ([`Self::take_slots`]),
    /// committed with the files and the uploads it fenced, or, when a
    /// run-scoped slot's upload has no copy on `copies`, given up with
    /// nothing changed (`None`).
    async fn receive_slots(
        &self,
        run: &str,
        slots: &[ClaimedSlot<'_>],
        accepted: &[weft_core::weft_type::WeftType],
        copies: &[RunCopy],
    ) -> StoreResult<Option<(Vec<StoredFileMeta>, Vec<String>)>> {
        // Ahead of the commit, a failure leaves nothing of this transaction
        // ([`Self::claim_uploads`] ends the copies made before it).
        let mut conn = self.pool.get().await.context("runtime claim: begin").map_err(RuntimeStoreError::BeforeWrites)?;
        let tx = conn.transaction().await.context("runtime claim: begin").map_err(RuntimeStoreError::BeforeWrites)?;
        // Every row the call changes locked up front, in one order, so two
        // calls naming the same slots never wait on each other in a circle.
        let sids: Vec<&str> = slots.iter().map(|slot| slot.claims.sid.as_str()).collect();
        let targets: Vec<&str> = copies.iter().map(|copy| copy.target.as_str()).collect();
        tx.execute(
            "SELECT 1 FROM runtime_file WHERE (upload_slot = ANY($1) AND status <> 'reaping') OR key = ANY($2) \
             ORDER BY key FOR UPDATE",
            &[&sids, &targets],
        )
        .await
        .context("runtime claim: lock the slots' uploads and the run's copies")
        .map_err(RuntimeStoreError::BeforeWrites)?;
        let mut files = Vec::with_capacity(slots.len());
        let mut moved = Vec::new();
        for (slot, accepts) in slots.iter().zip(accepted) {
            let received =
                self.receive_slot(&tx, run, slot, accepts, copies, &mut moved).await.map_err(|e| of_slot(slot.slot, e))?;
            // Dropped, the transaction rolls back.
            let Some(file) = received else { return Ok(None) };
            files.push(file);
        }
        tx.commit().await.context("runtime claim: commit").map_err(RuntimeStoreError::Other)?;
        Ok(Some((files, moved)))
    }

    /// Write the row of this call's copy of the upload waiting in slot
    /// `sid`, under a key of its own in the run's space, 'completing' and
    /// claimed now, with the upload's name, size and hash: what
    /// [`Self::copy_into`] fills and the claim's transaction receives.
    /// Answers the copy and the size it copies, or nothing when the slot
    /// holds no upload waiting (the transaction answers that slot). The
    /// expiry loop is rung for the claim's lapse, the time it ends the copy
    /// if this call dies before it does ([`Self::expire`]).
    async fn begin_run_copy(&self, caller: &CallerAuth, run: &str, sid: &str) -> StoreResult<Option<(RunCopy, u64)>> {
        let waiting: Option<String> = self
            .pool
            .scalar_opt("SELECT key FROM runtime_file WHERE upload_slot = $1 AND status = 'uploaded'", &[&sid])
            .await
            .context("runtime claim: look for the slot's upload")
            .map_err(RuntimeStoreError::BeforeWrites)?;
        let Some(upload) = waiting else { return Ok(None) };
        let target = weft_core::storage::key::key_for_put(caller, &StorageScope::Run, &uuid::Uuid::new_v4().to_string())
            .map_err(RuntimeStoreError::Denied)?
            .to_key();
        let now = self.clock.now_unix();
        // The parameters an INSERT ... SELECT lists take no type from the
        // columns they fill, so they say theirs.
        let size: Option<i64> = self
            .pool
            .scalar_opt(
                "INSERT INTO runtime_file \
                 (key, tenant_id, mime_type, filename, size_bytes, status, created_at_unix, declared_size, reserved_bytes, \
                  progressed_at_unix, slot_received_by, receives, content_sha256) \
                 SELECT $2::TEXT, tenant_id, mime_type, filename, 0, 'completing', $3::BIGINT, size_bytes, size_bytes, $3::BIGINT, \
                        $4::TEXT, key, content_sha256 \
                 FROM runtime_file WHERE key = $1 AND status = 'uploaded' \
                 RETURNING declared_size",
                &[&upload, &target, &now, &run],
            )
            .await
        .context("runtime claim: write the run's copy")
        .map_err(RuntimeStoreError::Other)?;
        let Some(size) = size else { return Ok(None) };
        self.ring_for_abandon(CLAIM_LEASE_SECS).await;
        Ok(Some((RunCopy { sid: sid.to_string(), upload, target, failed: None }, size as u64)))
    }

    /// Receive one slot's upload on `tx`, which holds its row locked
    /// ([`Self::receive_slots`]). A run-scoped one becomes this call's copy
    /// of it, made in `copies`, and its upload is fenced, written on
    /// `moved`; with no copy of it there, the answer is `None`.
    async fn receive_slot(
        &self,
        tx: &Transaction<'_>,
        run: &str,
        slot: &ClaimedSlot<'_>,
        accepts: &weft_core::weft_type::WeftType,
        copies: &[RunCopy],
        moved: &mut Vec<String>,
    ) -> StoreResult<Option<StoredFileMeta>> {
        let ClaimedSlot { claims, fits, .. } = *slot;
        let found: Option<(String, String, Option<String>, String)> = tx
            .opt(
                "SELECT key, status, slot_received_by, mime_type FROM runtime_file WHERE upload_slot = $1 AND status <> 'reaping'",
                &[&claims.sid],
            )
            .await
        .context("runtime claim: read the slot's upload")
        .map_err(RuntimeStoreError::Other)?;
        let Some((key, status, received_by, mime)) = found else { return Err(nothing_uploaded()) };
        if received_by.as_ref().is_some_and(|by| by != run) {
            return Err(RuntimeStoreError::Conflict("this slot's upload was already received by another run".into()));
        }
        let not_finished = || {
            RuntimeStoreError::Invalid(
                "the upload through this slot has not finished; the page calls the route once uploadFile answered".into(),
            )
        };
        match status.as_str() {
            // A file this run already received is one type: it is handed
            // out again only where it fits as that type.
            "active" => {
                let meta = Self::completed_meta(tx, &key).await?.ok_or_else(nothing_uploaded)?;
                return weft_core::storage::fits_as_handed_over(&meta.mime_type, fits).map(|()| Some(meta)).map_err(|why| {
                    RuntimeStoreError::Invalid(format!(
                        "the file uploaded for {accepts} this run received does not fit the {fits} it is sent to: {why}"
                    ))
                });
            }
            "uploaded" => {}
            _ => return Err(not_finished()),
        }
        let mime = weft_core::storage::fit_mime(&mime, fits).map_err(|why| {
            RuntimeStoreError::Invalid(format!("the file uploaded for {accepts} does not fit the {fits} it is sent to: {why}"))
        })?;
        let now = self.clock.now_unix();
        let ttl = claims.keep.and_then(KeepTtl::secs).map(|s| s as i64);
        if !matches!(claims.scope, StorageScope::Run) {
            let row: FileRow = tx
                .one(
                    &format!(
                        "UPDATE runtime_file SET status = 'active', slot_received_by = $2, mime_type = $5, keep_ttl_secs = $3, \
                         expires_at_unix = $4 WHERE key = $1 RETURNING {FILE_ROW_COLUMNS}"
                    ),
                    &[&key, &run, &ttl, &ttl.map(|t| now + t), &mime],
                )
                .await
            .context("runtime claim: receive the upload")
            .map_err(RuntimeStoreError::Other)?;
            return Ok(Some(row.to_meta()));
        }
        // A run's own space: the run's copy becomes the file, and the
        // upload it came from goes (fenced first, so the slot passes to the
        // copy). An upload that landed after the copies were made has none
        // yet.
        let Some(copy) = copies.iter().find(|copy| copy.upload == key) else { return Ok(None) };
        if let Some(failed) = &copy.failed {
            return Err(RuntimeStoreError::Other(anyhow::anyhow!("runtime claim: copy the upload into the run: {failed}")));
        }
        tx.execute(&fence_sql(""), &[&key, &now])
            .await
            .context("runtime claim: fence the upload")
            .map_err(RuntimeStoreError::Other)?;
        let row: Option<FileRow> = tx
            .opt(
                &format!(
                    "UPDATE runtime_file SET status = 'active', size_bytes = declared_size, declared_size = NULL, receives = NULL, \
                       upload_slot = $3, mime_type = $4, keep = $5, expires_at_unix = $6, keep_ttl_secs = $7, created_at_unix = $8 \
                     WHERE key = $1 AND status = 'completing' AND receives = $2 RETURNING {FILE_ROW_COLUMNS}"
                ),
                &[&copy.target, &key, &claims.sid, &mime, &claims.keep.is_some(), &ttl.map(|t| now + t), &ttl, &now],
            )
            .await
        .context("runtime claim: receive the run's copy")
        .map_err(RuntimeStoreError::Other)?;
        let row = row.ok_or_else(|| {
            RuntimeStoreError::Other(anyhow::anyhow!(
                "the run's copy '{}' of the upload '{key}' was ended while it was made",
                copy.target
            ))
        })?;
        moved.push(key);
        Ok(Some(row.to_meta()))
    }

    /// Ring the expiry loop for the upload a run's copy of it held
    /// ([`Self::claim_uploads`]), once that copy ended unreceived: the
    /// loop books no look for an upload while a copy of it is made, so
    /// one whose slot ended meanwhile waits for this ring.
    async fn ring_for_received(&self, upload: &str) -> Result<()> {
        let at: Option<Option<i64>> = self
            .pool
            .scalar_opt("SELECT expires_at_unix FROM runtime_file WHERE key = $1 AND status = 'uploaded'", &[&upload])
            .await
            .context("runtime claim: read when the copied upload ends")?;
        self.ring_for(at.flatten()).await;
        Ok(())
    }

    /// Delete the object at `key` when no row records it: a copy made
    /// ahead of a row that never came, or whose row went while it copied.
    /// A row being reaped records nothing: its object may be deleted
    /// already, before the copy landed.
    async fn drop_unrecorded_copy(&self, key: &str) -> Result<()> {
        let recorded: bool = self
            .pool
            .scalar("SELECT EXISTS (SELECT 1 FROM runtime_file WHERE key = $1 AND status <> 'reaping')", &[&key])
            .await
            .context("runtime copy: look for the copy's row")?;
        if !recorded {
            self.bucket
                .delete(&object_key(key))
                .await
                .with_context(|| format!("runtime copy: delete the copy '{key}' no row records"))?;
        }
        Ok(())
    }

    /// The in-flight upload row ('pending' or 'completing') for a key.
    /// None when the key has none (active, being reaped, or unknown).
    async fn pending_row(db: &impl Client, key: &str) -> Result<Option<PendingUpload>> {
        db.opt(&format!("{PENDING_ROW_SELECT_ANY} AND status IN ('pending', 'completing')"), &[&key])
            .await
            .context("read pending upload row")
    }

    /// [`Self::pending_row`], holding the row locked until `tx` ends. Every
    /// transaction that changes an upload's status or parts takes this
    /// lock first, so they apply one after another.
    async fn pending_row_locked(tx: &Transaction<'_>, key: &str) -> Result<Option<PendingUpload>> {
        tx.opt(&format!("{PENDING_ROW_SELECT_ANY} AND status IN ('pending', 'completing') FOR UPDATE"), &[&key])
            .await
            .context("lock pending upload row")
    }

    /// The file an upload key completed into, for a complete that finds its
    /// pending row gone (a retry, or the loser of two concurrent completes):
    /// the key's own ACTIVE row for a new file, or for a replacement the
    /// active file it last folded into. None when the key never completed
    /// (unknown, swept, aborted) or a later replacement of the same file
    /// has folded since.
    async fn completed_meta(db: &impl Client, key: &str) -> StoreResult<Option<StoredFileMeta>> {
        let row: Option<FileRow> = db
            .opt(
                &format!(
                    "SELECT {FILE_ROW_COLUMNS} FROM runtime_file \
                     WHERE (key = $1 OR last_replacement = $1) AND status IN ('active', 'uploaded')"
                ),
                &[&key],
            )
            .await
        .context("runtime complete_upload: read existing row")
        .map_err(RuntimeStoreError::Other)?;
        Ok(row.as_ref().map(FileRow::to_meta))
    }

    /// The metadata row for an ACTIVE (finalized) file, or None if absent, still
    /// pending, or expired. Every user-facing read (meta/get/download/keep/link)
    /// goes through here, so a half-uploaded 'pending' file reads as not-found
    /// until it finalizes. Access does NOT bump a kept file's expiry here (a
    /// metadata peek is not an access); the byte `get`/`download_url` bumps it.
    async fn row(&self, key: &str) -> Result<Option<FileRow>> {
        self.pool
            .opt(
                &format!(
                    "SELECT {FILE_ROW_COLUMNS} FROM runtime_file WHERE key = $1 AND status = 'active' AND {live}",
                    live = unexpired("$2"),
                ),
                &[&key, &self.clock.now_unix()],
            )
            .await
            .context("runtime row")
    }

    /// The ACTIVE file stored under `identity` in the caller's `scope`,
    /// if any: the read half of an identified put, so a caller can ask
    /// before it fetches anything. The scope is the same prefix the
    /// begin's collision check and the identity index use.
    pub async fn find_identity(
        &self,
        caller: &CallerAuth,
        scope: &StorageScope,
        identity: &str,
    ) -> StoreResult<Option<StoredFileMeta>> {
        let prefix = weft_core::storage::key::prefix_for_list(caller, scope)
            .map_err(RuntimeStoreError::Denied)?;
        let row: Option<FileRow> = self
            .pool
            .opt(
                &format!(
                    "SELECT {FILE_ROW_COLUMNS} FROM runtime_file \
                     WHERE regexp_replace(key, '/[^/]+$', '') = $1 AND identity = $2 AND status = 'active' AND {live}",
                    live = unexpired("$3"),
                ),
                &[&prefix.trim_end_matches('/'), &identity, &self.clock.now_unix()],
            )
            .await
        .context("runtime find_identity")
        .map_err(RuntimeStoreError::BeforeWrites)?;
        Ok(row.as_ref().map(FileRow::to_meta))
    }

    /// Metadata only (no access bump). The caller has already passed the wall.
    pub async fn meta(&self, parsed: &ParsedKey) -> StoreResult<StoredFileMeta> {
        let key = parsed.to_key();
        self.row(&key)
            .await
            .map_err(RuntimeStoreError::BeforeWrites)?
            .map(|r| r.to_meta())
            .ok_or_else(|| RuntimeStoreError::NotFound(key))
    }

    /// Read an active, unexpired file and renew its expiry from its current
    /// TTL, then push an expiry that is still short of `cover_until` to it,
    /// and record `cover_until` as a link's end ([`expiry_honoring_links`]):
    /// a link handed out for the file lives that long, and the bytes must
    /// outlive it. `0` hands out nothing (a read made now). Leaves no
    /// expiry on a file that has none. Never shortens below a live link's
    /// end. An expired file is never brought back to life: it reads as
    /// gone. With `visitors_of`, the same statement reads that project's
    /// visitor address, so a link for its outside callers costs no other
    /// round trip.
    async fn bump_expiry(&self, key: &str, cover_until: i64, visitors_of: Option<uuid::Uuid>) -> Result<Option<LinkedRow>> {
        // Read the TTL from the locked row, not an earlier metadata read:
        // source publication may have protected or retired this asset since
        // that read. A reaper that already claimed the row wins over access.
        let now = self.clock.now_unix();
        // `$4` only when it is asked about: a statement takes exactly the
        // parameters it names.
        let mut params: Vec<db::Param<'_>> = vec![&now, &key, &cover_until];
        let visitor_address = match &visitors_of {
            Some(project) => {
                params.push(project);
                "(SELECT visitor_address FROM project_frontend WHERE project_id = $4 AND visitor_address IS NOT NULL)"
            }
            None => "NULL::TEXT",
        };
        self.pool
            .opt(
                &format!(
                    "UPDATE runtime_file SET expires_at_unix = CASE \
                         WHEN expires_at_unix IS NULL THEN NULL \
                         WHEN keep_ttl_secs IS NULL THEN GREATEST(expires_at_unix, $3) \
                         ELSE GREATEST({}, $3) END, \
                       links_until_unix = CASE WHEN $3 > 0 THEN GREATEST(COALESCE(links_until_unix, 0), $3) ELSE links_until_unix END \
                     WHERE key = $2 AND status = 'active' AND {live} \
                     RETURNING {FILE_ROW_COLUMNS}, {visitor_address} AS visitor_address",
                    expiry_honoring_links("$1 + keep_ttl_secs"),
                    live = unexpired("$1"),
                ),
                &params,
            )
            .await
            .context("bump expiry")
    }

    /// Delete a file (object + row). A missing row is a not-found so the caller
    /// learns the key was already gone.
    ///
    /// Delete a finished file: fence its row to 'reaping' (under its lock),
    /// then remove the object, then the row, the same three steps as every
    /// reap, so a crash in between leaves a 'reaping' row the expiry loop
    /// finishes and never a row pointing at a deleted object. A file whose
    /// reap the bucket refused before (marked, [`MARKED`]) is tried again. A file whose
    /// replacement is completing is refused: the bucket may be writing its
    /// object.
    pub async fn delete(&self, parsed: &ParsedKey) -> StoreResult<()> {
        let key = parsed.to_key();
        let mut conn = self.pool.get().await.context("runtime delete: begin tx").map_err(RuntimeStoreError::BeforeWrites)?;
        let tx = conn.transaction().await.context("runtime delete: begin tx").map_err(RuntimeStoreError::BeforeWrites)?;
        if let Removal::Blocked(blocker) = Self::lock_for_removal(&tx, &key).await.map_err(RuntimeStoreError::Other)? {
            return Err(RuntimeStoreError::Completing(format!(
                "'{key}' cannot be deleted while {blocker}; delete it once that lands"
            )));
        }
        // A marked row (a reap the bucket refused) is this file being
        // removed already: deleting it again tries its reap again.
        let fenced = tx
            .execute(&fence_sql(&format!("AND (status = 'active' OR {MARKED})")), &[&key, &self.clock.now_unix()])
            .await
            .context("runtime delete: fence the row")
            .map_err(RuntimeStoreError::Other)?;
        if fenced == 0 {
            return Err(RuntimeStoreError::NotFound(key));
        }
        tx.commit()
            .await
            .context("runtime delete: commit fence")
            .map_err(RuntimeStoreError::Other)?;
        drop(conn);
        // A worker reaches this too, so the bucket's own words stay in the
        // log (`mark_reap_failed`) and the answer says what happened.
        let reaped = self.reap_fenced_once(&SweepEntry::file(&key)).await?;
        if reaped.changes_record {
            self.record_reap_failures().await?;
        }
        match reaped.refused {
            None => Ok(()),
            Some(_) => Err(RuntimeStoreError::RemovalIncomplete(format!(
                "the storage bucket refused to remove '{key}'; the file is gone from listings and stays marked in \
                 `weft status`, and deleting it again tries again"
            ))),
        }
    }

    /// Lock a row before fencing it for removal, and say whether it is
    /// there and what forbids the removal: the row is an upload a
    /// completion has claimed, a file whose replacement a completion has
    /// claimed (the bucket may be writing its object), or a slot's upload a
    /// run's copy is being made of ([`Self::claim_uploads`]). The check runs
    /// as its own statement after the lock, so it sees a claim that
    /// committed while this waited; a claim arriving later locks the same
    /// file and finds it fenced.
    async fn lock_for_removal(tx: &Transaction<'_>, key: &str) -> Result<Removal> {
        let status: Option<String> = tx
            .scalar_opt("SELECT status FROM runtime_file WHERE key = $1 FOR UPDATE", &[&key])
            .await
            .context("lock a row for removal")?;
        match status.as_deref() {
            None => return Ok(Removal::Gone),
            Some("completing") => return Ok(Removal::Blocked(format!("its upload '{key}' is completing"))),
            Some(_) => {}
        }
        let landing: Option<(String, bool)> = tx
            .opt(
                "SELECT key, replaces IS NOT NULL FROM runtime_file WHERE (replaces = $1 OR receives = $1) AND status = 'completing'",
                &[&key],
            )
            .await
            .context("look for a completing replacement or copy")?;
        Ok(match landing {
            Some((r, true)) => Removal::Blocked(format!("its replacement '{r}' is completing")),
            Some((copy, false)) => Removal::Blocked(format!("a run is receiving it as '{copy}'")),
            None => Removal::Free,
        })
    }

    /// Fence one row for a sweep or wipe: lock it, skip it when it is gone
    /// or a completion forbids the removal ([`Self::lock_for_removal`]),
    /// else flip it to 'reaping', claimed at `now`, where `condition`
    /// ([`fence_sql`]'s, `$2` being `now`) still holds.
    async fn fence_for_reap(&self, key: &str, condition: &str, now: i64) -> Result<Fence> {
        let mut conn = self.pool.get().await.context("fence: begin tx")?;
        let tx = conn.transaction().await.context("fence: begin tx")?;
        match Self::lock_for_removal(&tx, key).await? {
            Removal::Gone => return Ok(Fence::Gone),
            Removal::Blocked(blocker) => {
                tracing::info!(target: "weft_broker::runtime_store", key = %key, "not reaped while {blocker}");
                return Ok(Fence::Completing);
            }
            Removal::Free => {}
        }
        let fenced = tx.execute(&fence_sql(condition), &[&key, &now]).await.context("fence")?;
        tx.commit().await.context("fence: commit")?;
        Ok(if fenced > 0 { Fence::Fenced } else { Fence::Spared })
    }

    /// Replace the set of the tenant's assets `project` references. An asset
    /// any project of the tenant references has no expiry; one whose LAST
    /// reference this publish removes starts the default access-renewed TTL
    /// once (repeated publishes never restart it), and referencing it again
    /// clears that countdown. Every project's publish runs under the tenant
    /// lock, so two projects publishing at once agree on who references what.
    ///
    /// `keys` must all be present (a missing one fails the update and
    /// changes nothing); `kept` are referenced when present and handed back
    /// when not, so a version whose file is gone never blocks a build.
    pub async fn set_asset_references(
        &self,
        tenant: &str,
        project: &str,
        keys: &[String],
        kept: &[String],
    ) -> StoreResult<Vec<String>> {
        if !weft_core::storage::key::valid_segment(project) {
            return Err(RuntimeStoreError::Invalid(format!("'{project}' is not a valid project id")));
        }
        for key in keys.iter().chain(kept) {
            let parsed = weft_core::storage::key::parse_key(key).map_err(RuntimeStoreError::Invalid)?;
            if parsed.tenant != tenant || parsed.scope != weft_core::storage::key::KeyScope::Asset {
                return Err(RuntimeStoreError::Denied(
                    "asset references must name the acting tenant's assets".into(),
                ));
            }
        }
        let mut conn = self.pool.get().await.context("asset references transaction").map_err(RuntimeStoreError::BeforeWrites)?;
        let tx = conn.transaction().await.context("asset references transaction").map_err(RuntimeStoreError::BeforeWrites)?;
        lock_tenant_storage(&tx, tenant).await?;
        let before: Vec<String> = tx
            .scalars("SELECT key FROM asset_reference WHERE tenant_id = $1 AND project_id = $2", &[&tenant, &project])
            .await
            .context("read the project's asset references")?;
        // Lock every file this publish touches before checking presence, so
        // expiry cannot remove a referenced file between validation and
        // protection. A reaper that won first makes this update fail without
        // changing any reference.
        let touched: Vec<&String> = keys.iter().chain(kept).chain(&before).collect();
        let available: Vec<String> = tx
            .scalars("SELECT key FROM runtime_file WHERE key = ANY($1) AND status = 'active' ORDER BY key FOR UPDATE", &[&touched])
            .await
            .context("lock the referenced assets")?;
        let available: std::collections::HashSet<&str> = available.iter().map(String::as_str).collect();
        if let Some(missing) = keys.iter().find(|key| !available.contains(key.as_str())) {
            return Err(RuntimeStoreError::NotFound(missing.clone()));
        }
        let (kept_present, missing): (Vec<String>, Vec<String>) = kept.iter().cloned()
            .partition(|key| available.contains(key.as_str()));
        let referenced: Vec<String> = keys.iter().cloned().chain(kept_present)
            .collect::<std::collections::BTreeSet<_>>().into_iter().collect();
        tx.execute("DELETE FROM asset_reference WHERE tenant_id = $1 AND project_id = $2", &[&tenant, &project])
            .await
            .context("clear the project's asset references")?;
        tx.execute(
            "INSERT INTO asset_reference (tenant_id, project_id, key) SELECT $1::TEXT, $2::TEXT, unnest($3::text[])",
            &[&tenant, &project, &referenced],
        )
        .await
        .context("record the project's asset references")?;
        tx.execute(
            "UPDATE runtime_file SET expires_at_unix = NULL, keep_ttl_secs = NULL \
             WHERE key = ANY($1) AND status = 'active'",
            &[&referenced],
        )
        .await
        .context("keep the referenced assets")?;
        // What this project stopped referencing, and no other project
        // references either, starts its countdown.
        let countdown = self.clock.now_unix() + DEFAULT_KEEP_TTL_SECS as i64;
        let retired = tx
            .execute(
                &format!(
                    "UPDATE runtime_file SET \
                         expires_at_unix = COALESCE(expires_at_unix, {}), \
                         keep_ttl_secs = COALESCE(keep_ttl_secs, $3) \
                     WHERE key = ANY($1) AND status = 'active' \
                       AND NOT EXISTS (SELECT 1 FROM asset_reference r WHERE r.key = runtime_file.key)",
                    expiry_honoring_links("$2")
                ),
                &[&before, &countdown, &(DEFAULT_KEEP_TTL_SECS as i64)],
            )
            .await
            .context("retire the unreferenced assets")?;
        tx.commit().await.context("commit the project's asset references")?;
        drop(conn);
        if retired > 0 {
            self.ring_for(Some(countdown)).await;
        }
        Ok(missing)
    }

    /// Which of `hashes` the tenant stores whole: `content hash -> key`. An
    /// upload still in flight is not held (its begin answers how to resume
    /// it). Every hash must be a content hash.
    pub async fn held_assets(
        &self,
        tenant: &str,
        hashes: &[String],
    ) -> StoreResult<std::collections::BTreeMap<String, String>> {
        let mut wanted = std::collections::BTreeMap::new();
        for hash in hashes {
            let key = ParsedKey::asset(tenant, hash).map_err(RuntimeStoreError::Invalid)?.to_key();
            wanted.insert(key, hash.clone());
        }
        let keys: Vec<&String> = wanted.keys().collect();
        let held: Vec<String> = self
            .pool
            .scalars("SELECT key FROM runtime_file WHERE key = ANY($1) AND status = 'active'", &[&keys])
            .await
        .context("look up held assets")
        .map_err(RuntimeStoreError::BeforeWrites)?;
        Ok(held.into_iter().map(|key| (wanted[&key].clone(), key)).collect())
    }

    /// List every ACTIVE file under a key prefix (a scope, or a whole tenant). The
    /// prefix is produced by the wall (`prefix_for_list` / `tenant_prefix`), so it
    /// is always tenant-anchored. Pending (half-uploaded) files are excluded: they
    /// are not real files a user can see yet; so are expired ones.
    pub async fn list(&self, prefix: &str) -> StoreResult<Vec<StoredFileMeta>> {
        let pattern = like_prefix(prefix);
        let rows: Vec<FileRow> = self
            .pool
            .all(
                &format!(
                    "SELECT {FILE_ROW_COLUMNS} FROM runtime_file \
                     WHERE key LIKE $1 ESCAPE '\\' AND status = 'active' AND {live} ORDER BY key",
                    live = unexpired("$2"),
                ),
                &[&pattern, &self.clock.now_unix()],
            )
            .await
        .context("runtime list")
        .map_err(RuntimeStoreError::BeforeWrites)?;
        Ok(rows.iter().map(FileRow::to_meta).collect())
    }


    /// Every key under a prefix REGARDLESS of status (active + pending), with
    /// what a sweep needs: is it a kept ACTIVE file (spared by the terminate
    /// sweep), and the in-flight upload id to abort (pending rows). Sweeps use
    /// this (not `list`) so a half-uploaded file's bucket state is cleaned
    /// too; the bucket never keeps state whose row a sweep removed.
    async fn keys_under(&self, prefix: &str) -> Result<Vec<SweepEntry>> {
        let pattern = like_prefix(prefix);
        self.pool
            .all(
                "SELECT key, (keep AND status = 'active') AS kept_active, status, upload_id, replaces \
                 FROM runtime_file WHERE key LIKE $1 ESCAPE '\\' ORDER BY key",
                &[&pattern],
            )
            .await
            .context("runtime keys_under")
    }

    /// Steps 2+3 of a fenced reap (the caller has already flipped the row to
    /// 'reaping'): remove the bucket state, then the row. If the bucket reap
    /// fails, the row is marked ([`Self::mark_reap_failed`]), the error
    /// propagates, and the row STAYS in 'reaping', taken over by what needs
    /// it once marked ([`MARKED`]), by the expiry loop once its claim lapsed
    /// after a crash (rung here for that lapse, [`Self::ring_for_abandon`]).
    /// So a crash between the steps never strands an object without a row
    /// (nothing would ever revisit it: every cleanup is row-driven) nor a
    /// charged row pointing at deleted bytes. The record `weft status`
    /// shows is written here; a pass over many rows reaps with
    /// [`Self::reap_fenced_once`] and writes it once at its end.
    async fn reap_fenced(&self, entry: &SweepEntry) -> Result<()> {
        let reaped = self.reap_fenced_once(entry).await?;
        if reaped.changes_record {
            self.record_reap_failures().await?;
        }
        reaped.refused.map_or(Ok(()), Err)
    }

    /// [`Self::reap_fenced`], one try, with the record of failed reaps left
    /// to the caller ([`Reaped`]).
    async fn reap_fenced_once(&self, entry: &SweepEntry) -> Result<Reaped> {
        self.ring_for_abandon(CLAIM_LEASE_SECS).await;
        self.reap_unrecorded(entry, &[]).await
    }

    /// Reap a fenced row, trying again after each of `pauses`, still under
    /// this call's claim. A bucket that refused every try leaves the row
    /// marked and answers why ([`Reaped::refused`]); a failure of the
    /// database is the error. The record `weft status` shows is the
    /// caller's to write ([`Reaped::changes_record`]), once per pass.
    async fn reap_unrecorded(&self, entry: &SweepEntry, pauses: &[std::time::Duration]) -> Result<Reaped> {
        let mut pauses = pauses.iter();
        while let Err(e) = self.reap_bucket_state(entry).await {
            match pauses.next() {
                Some(pause) => tokio::time::sleep(*pause).await,
                None => {
                    self.mark_reap_failed(&entry.key, &e).await?;
                    return Ok(Reaped { refused: Some(e), changes_record: true });
                }
            }
        }
        let was_marked: bool = self.pool.scalar(&delete_upload_row(""), &[&entry.key]).await.context("delete reaped row")?;
        Ok(Reaped { refused: None, changes_record: was_marked })
    }

    /// Remove a doomed key's bucket state: abort its in-flight multipart
    /// upload (if any; idempotent) and delete its object (idempotent). Every
    /// sweep/wipe path funnels through here so no path can forget the abort.
    /// A replacement upload's multipart lives on the object of the file it
    /// replaces, which that file still owns: only the upload is aborted.
    async fn reap_bucket_state(&self, entry: &SweepEntry) -> Result<()> {
        let key = &entry.key;
        let object = object_key(entry.replaces.as_deref().unwrap_or(key));
        if let Some(id) = &entry.upload_id {
            self.bucket
                .abort_multipart(&object, id)
                .await
                .with_context(|| format!("abort in-flight upload for {key}"))?;
        }
        if entry.replaces.is_none() {
            self.bucket
                .delete(&object)
                .await
                .with_context(|| format!("delete object {key}"))?;
        }
        Ok(())
    }

    /// Set how long a file lives from now on, access-renewed. On a
    /// run file this also flags it to survive the terminate sweep;
    /// in the scopes that outlive runs already it is only the lifetime
    /// (`KeepTtl::Never` there clears one). An asset's lifetime follows
    /// the source, so it is refused.
    pub async fn keep(&self, parsed: &ParsedKey, ttl: KeepTtl) -> StoreResult<StoredFileMeta> {
        if matches!(parsed.scope, weft_core::storage::key::KeyScope::Asset) {
            return Err(RuntimeStoreError::Invalid(
                "an asset's lifetime follows the source that references it; it takes no keep".into(),
            ));
        }
        let run_scoped = matches!(parsed.scope, weft_core::storage::key::KeyScope::Run { .. });
        let key = parsed.to_key();
        let ttl_secs = ttl.secs();
        let now = self.clock.now_unix();
        let expires_at = ttl_secs.map(|s| now + s as i64);
        // KeepTtl::Never binds NULL (never expires, covers links
        // trivially); a finite deadline never undercuts a live link. A file
        // that already expired stays expired.
        let row: Option<FileRow> = self
            .pool
            .opt(
                &format!(
                    "UPDATE runtime_file SET keep = keep OR $4, keep_ttl_secs = $1, \
                         expires_at_unix = CASE WHEN $2::bigint IS NULL THEN NULL ELSE {} END \
                     WHERE key = $3 AND status <> 'reaping' AND {live} \
                     RETURNING {FILE_ROW_COLUMNS}",
                    expiry_honoring_links("$2"),
                    live = unexpired("$5"),
                ),
                &[&ttl_secs.map(|s| s as i64), &expires_at, &key, &run_scoped, &now],
            )
            .await
        .context("runtime keep")
        .map_err(RuntimeStoreError::BeforeWrites)?;
        let row = row.ok_or_else(|| RuntimeStoreError::NotFound(key))?;
        self.ring_for(row.expires_at_unix).await;
        Ok(row.to_meta())
    }

    /// A link to a file for `audience`, living at least `ttl_secs` (the
    /// value's own lifetime, else [`DEFAULT_LINK_SECS`]), plus the file's
    /// metadata and the moment the link stops working: [`Self::grant_link`]
    /// then [`Self::sign_link`], for an asker who knows the audience
    /// before the file is read.
    pub async fn link(
        &self,
        parsed: &ParsedKey,
        audience: PresignAudience,
        ttl_secs: Option<u64>,
        hands_out: bool,
    ) -> StoreResult<(StoredFileMeta, String, i64)> {
        let granted = self.grant_link(parsed, ttl_secs, hands_out, None).await?;
        let url = self.sign_link(&granted, audience).await?;
        Ok((granted.file, url, granted.window.until()))
    }

    /// The first half of a link: the file read, its life pushed to cover
    /// the link, and (with `visitors_of`) that project's visitor address,
    /// all in one statement, so the asker can pick where the link goes
    /// before [`Self::sign_link`] signs it.
    ///
    /// The link is signed at the start of its window ([`link_window`]), so
    /// the same file asked for again within the window gets the same
    /// link, for everyone, with no row kept: a page that refreshes keeps
    /// its video playing. Asking counts as access (renews a kept file's
    /// expiry), and with `hands_out` (a link somebody fetches later) the
    /// file's expiry is pushed past the link's end, so the bytes outlive
    /// every link handed out; a read made now (`hands_out` false) needs no
    /// such promise. A missing or expired file fails the ask rather than
    /// handing out a dead link.
    pub async fn grant_link(
        &self,
        parsed: &ParsedKey,
        ttl_secs: Option<u64>,
        hands_out: bool,
        visitors_of: Option<uuid::Uuid>,
    ) -> StoreResult<GrantedLink> {
        let key = parsed.to_key();
        let ttl = weft_core::storage::checked_link_secs(ttl_secs.unwrap_or(DEFAULT_LINK_SECS)).map_err(RuntimeStoreError::Invalid)?;
        let window = link_window(self.clock.now_unix(), ttl);
        let row = self
            .bump_expiry(&key, if hands_out { window.until() } else { 0 }, visitors_of)
            .await
            .map_err(RuntimeStoreError::BeforeWrites)?
            .ok_or_else(|| RuntimeStoreError::NotFound(key.clone()))?;
        Ok(GrantedLink { file: row.file.to_meta(), window, visitor_address: row.visitor_address })
    }

    /// Sign a granted link for `audience`. It answers with the file's
    /// stored type, as a download under its name
    /// ([`weft_core::storage::download_disposition`]).
    pub async fn sign_link(&self, granted: &GrantedLink, audience: PresignAudience) -> StoreResult<String> {
        let link = GetLink {
            audience,
            signed_at: std::time::UNIX_EPOCH + std::time::Duration::from_secs(granted.window.signed_at as u64),
            ttl_secs: granted.window.works_for,
            content_type: Some(granted.file.mime_type.clone()),
            disposition: Some(weft_core::storage::download_disposition(&granted.file.filename)),
        };
        self.bucket
            .presign_get(&object_key(&granted.file.key), &link)
            .await
            .context("runtime link: sign")
            .map_err(RuntimeStoreError::Other)
    }

    /// Wipe every file under a wall-validated prefix (a `weft files rm` of a
    /// scope, or a tenant-delete). Deletes rows + objects; returns the count.
    /// The prefix MUST have passed `validate_wipe_prefix` at the edge. Rows
    /// it leaves are named by kind: any the bucket refused make the answer
    /// [`RuntimeStoreError::RemovalIncomplete`]; only rows a live claim holds
    /// make it [`RuntimeStoreError::Completing`] (ask again shortly).
    pub async fn wipe_prefix(&self, prefix: &str) -> StoreResult<u64> {
        // ALL keys under the prefix (active + pending), so a half-uploaded
        // file's bucket state (in-flight upload + any object) is wiped too.
        // Same fenced three-step reap as the sweeps (fence the row to
        // 'reaping', reap bucket, delete row): a crash mid-wipe leaves only
        // 'reaping' rows the expiry loop finishes, never a live
        // row pointing at deleted bytes. The wipe is unconditional (the whole
        // prefix dies), so the fence has no doom re-check; it exists to
        // lock out writers/readers and make the residue self-healing. A
        // marked row (a reap the bucket refused) is taken at once, and so is
        // a row whose reap crashed, once its claim lapsed.
        //
        // What a wipe leaves: a completion in flight (the bucket may be
        // writing that object), a row another call is reaping right now
        // (its claim is live), and a row whose reap the bucket refuses (it
        // stays marked). The wipe goes through every row, then fails naming
        // each kind it left, so the person knows what is still there and
        // why. The answer reaches the tenant (`weft files rm`, `weft rm`),
        // so a refused row is named by its key only: the bucket's own words
        // stay in the log and on the row (`reap_error`).
        let now = self.clock.now_unix();
        let (mut completing, mut claimed) = (0, 0);
        let mut failed = Vec::new();
        let mut record = false;
        let entries = self.keys_under(prefix).await.map_err(RuntimeStoreError::BeforeWrites)?;
        let pass: Result<u64> = async {
            let mut removed = 0;
            for entry in entries {
                match self
                    .fence_for_reap(&entry.key, &format!("AND (status <> 'reaping' OR {} OR {MARKED})", reap_lapsed("$2")), now)
                    .await?
                {
                    Fence::Fenced => {}
                    Fence::Gone => continue,
                    Fence::Completing => {
                        completing += 1;
                        continue;
                    }
                    Fence::Spared => {
                        claimed += 1;
                        continue;
                    }
                }
                let reaped = self.reap_fenced_once(&entry).await?;
                record |= reaped.changes_record;
                match reaped.refused {
                    None => removed += 1,
                    Some(_) => failed.push(format!("'{}'", entry.key)),
                }
            }
            Ok(removed)
        }
        .await;
        let removed = self.finish_pass(record, pass).await?;
        // A whole-tenant wipe takes the tenant's asset references with its
        // assets; every narrower prefix names no asset, so this matches none.
        self.pool
            .execute("DELETE FROM asset_reference WHERE key LIKE $1 ESCAPE '\\'", &[&like_prefix(prefix)])
            .await
            .context("drop the wiped assets' references")?;
        let mut left = Vec::new();
        if !failed.is_empty() {
            left.push(format!(
                "{} the storage bucket refused to remove, marked in `weft status` ({})",
                failed.len(),
                failed.join(", ")
            ));
        }
        if completing > 0 {
            left.push(format!("{completing} a completing upload holds"));
        }
        if claimed > 0 {
            left.push(format!("{claimed} another removal in progress holds"));
        }
        let left = format!("wiped {removed} files under '{prefix}' and left {}", left.join(", "));
        // A row a live claim holds clears by itself, so that alone is the
        // "ask again" answer; a refusal does not clear by waiting, so any
        // refused row makes the whole answer a refusal.
        match (failed.is_empty(), completing + claimed) {
            (true, 0) => Ok(removed),
            (true, _) => Err(RuntimeStoreError::Completing(format!("{left}; wipe again shortly"))),
            (false, _) => Err(RuntimeStoreError::RemovalIncomplete(format!(
                "{left}; wiping again tries the refused ones again"
            ))),
        }
    }

    /// Terminate sweep: close out a run's un-kept run files (the
    /// `<tenant>/run/<run_id>/` prefix, kept files excepted).
    ///
    /// - A COMPLETED un-kept file is not deleted here: it gets
    ///   `expires_at_unix = now + RUN_LINGER_TTL_SECS` stamped, so the user
    ///   can still list/download a run's output right after the run ends; the
    ///   expiry loop removes it once the linger passes (the stamp rings it,
    ///   [`Self::ring_for`]). The stamped deadline
    ///   is what the file lists surface as the remaining lifetime.
    /// - A PENDING row (a crashed/abandoned upload) is reaped immediately,
    ///   in-flight multipart aborted: a half-uploaded file has nothing worth
    ///   downloading (even a kept-flagged one: keep only spares a COMPLETED
    ///   file). A leftover 'reaping' row whose claim lapsed, or a marked one,
    ///   is retried.
    ///
    /// A row the bucket refuses is no failure of the sweep: it stays marked
    /// and is answered in [`RunSweep::refused`], so the run's end is done
    /// with and `weft status` shows the one record of the cause.
    pub async fn sweep_run(&self, tenant: &str, run_id: &str) -> StoreResult<RunSweep> {
        // Rendered through the key grammar (never hand-built): validates both
        // segments and keeps the scope tag single-sourced.
        let prefix = weft_core::storage::key::run_prefix(tenant, run_id)
            .map_err(|e| RuntimeStoreError::Invalid(format!("sweep_run: {e}")))?;
        let mut reaped = 0;
        let mut lingering = 0;
        let mut refused = Vec::new();
        let mut record = false;
        let entries = self.keys_under(&prefix).await.map_err(RuntimeStoreError::BeforeWrites)?;
        let pass: Result<()> = async {
            for entry in entries {
                if entry.kept_active {
                    continue;
                }
                // A completion in flight lands first (driving it here is safe
                // even if its own caller is still driving it), so the file it
                // becomes gets the linger stamp below like any finished file.
                if entry.status == "completing" {
                    if let Err(e) = self.drive_completion(&entry.key, false).await {
                        tracing::error!(
                            target: "weft_broker::runtime_store",
                            key = %entry.key, error = %e,
                            "a run's completing upload did not land; it gets the linger \
                             stamp now and the expiry loop lands it once its claim lapsed"
                        );
                    }
                }
                if entry.status == "active" || entry.status == "completing" {
                    // Completed un-kept file: stamp the linger deadline. A
                    // completion still landing is stamped too, and its fold keeps
                    // the stamp ([`Self::flip_active`]), so a file that lands after
                    // its run ended lingers like the run's other files. The guard
                    // re-checks the row is still un-kept (a keep that
                    // landed since the scan wins and the file survives untouched),
                    // and only stamps a NULL expiry so a re-delivered terminate
                    // sweep (the queue is idempotent) can't keep pushing the
                    // deadline out. The deadline honors every link handed out
                    // for the file (`links_until_unix`: a link is a promise the
                    // bytes stay fetchable for its stated lifetime).
                    let stamped: Option<Option<i64>> = self
                        .pool
                        .scalar_opt(
                            &format!(
                                "UPDATE runtime_file SET expires_at_unix = {} \
                                 WHERE key = $1 AND status IN ('active', 'completing') AND NOT keep \
                                   AND expires_at_unix IS NULL \
                                 RETURNING expires_at_unix",
                                expiry_honoring_links("$2")
                            ),
                            &[&entry.key, &(self.clock.now_unix() + RUN_LINGER_TTL_SECS)],
                        )
                        .await
                        .context("linger stamp")?;
                    if let Some(at) = stamped {
                        lingering += 1;
                        // The expiry loop must look by the linger's end; one
                        // ring covers the run's other files, stamped later.
                        self.ring_for(at).await;
                    }
                    continue;
                }
                // FENCE first: flip the row to 'reaping', atomically re-checking it
                // is still a pending row or a reap whose claim lapsed (a completion
                // that landed since the scan wins: the row is 'active' now and the
                // clause spares it; so does another call's reap in progress).
                // The flip locks out every writer and reader (record_part/reserve
                // gate on 'pending'; keep/get/download gate on 'active'), so the
                // bucket reap below can never race an in-flight upload, and a
                // crash at any point leaves a 'reaping' row this sweep's retry
                // (and the expiry loop) re-finds. Neither delete-object-first (a
                // charged row pointing at gone bytes on crash) nor
                // delete-row-first (an orphan object nothing row-driven ever
                // revisits) has that property.
                if self
                    .fence_for_reap(
                        &entry.key,
                        &format!("AND (status = 'pending' OR {} OR {MARKED})", reap_lapsed("$2")),
                        self.clock.now_unix(),
                    )
                    .await?
                    != Fence::Fenced
                {
                    continue;
                }
                // A row the bucket refuses stays marked, shown in `weft status`
                // as the one record of it; the sweep goes on with the run's
                // other rows and answers it among the refused.
                let outcome = self.reap_fenced_once(&entry).await?;
                record |= outcome.changes_record;
                match outcome.refused {
                    None => reaped += 1,
                    Some(e) => refused.push(format!("'{}': {e:#}", entry.key)),
                }
            }
            Ok(())
        }
        .await;
        self.finish_pass(record, pass).await?;
        Ok(RunSweep { reaped, lingering, refused })
    }

    /// The expiry loop's pass: remove every file whose expiry passed (the
    /// store's clock) and finish every reap whose call crashed, then say when it next has a row to
    /// look at. Each row is fenced first (flipped to 'reaping' under its
    /// lock, re-checking it is still doomed: an access that renewed it
    /// since the scan spares it, and so does another call's reap in
    /// progress), so the bucket reap never races a writer, a reader or
    /// another reap. A row whose reap keeps failing is marked
    /// ([`Self::mark_reap_failed`]) and does not hold up the others.
    ///
    /// The pass also ends every row in flight whose call is taken to have
    /// died ([`abandoned`]): a pending upload with no progress for
    /// [`PENDING_RESERVE_GRACE_SECS`] is reaped (its multipart aborted, its
    /// reservation freed), a reap whose claim lapsed is finished, and a
    /// completion whose claim lapsed is driven ([`Self::drive_completion`],
    /// recovering): it lands, or gets its verdict and ends. A run's copy of
    /// a slot's upload is such a completion, and it goes first: while its
    /// row is there nothing removes the upload it copies, so that upload,
    /// once free, goes in this same pass when its time is up. A drive that
    /// fails with no verdict (the bucket unreachable) takes the claim
    /// itself, so the next try is a lease later.
    ///
    /// The next look is booked at the soonest of two times: the expiry of
    /// an active file, leaving out a file whose replacement is completing
    /// (it cannot go until the replacement lands, and landing rings,
    /// [`Self::finish_completion`]) and a slot's upload a run's copy is
    /// being made of (received, it goes; ended, the copy rings,
    /// [`Self::ring_for_received`]), and the time a row in flight is
    /// abandoned ([`abandoned_at`]; a row's birth rings for it,
    /// [`Self::ring_for_abandon`]), a marked row a call took over included.
    /// A marked row nobody holds books nothing ([`MARKED`]).
    pub async fn expire(&self) -> Result<weft_task_store::drain::DrainStep> {
        use weft_task_store::drain::DrainStep;
        let now = self.clock.now_unix();
        let lapsed_completions: Vec<String> = self
            .pool
            .scalars(
                &format!("SELECT key FROM runtime_file WHERE status = 'completing' AND {} ORDER BY receives IS NULL", abandoned("$1")),
                &[&now],
            )
            .await
            .context("lapsed completion scan")?;
        for key in &lapsed_completions {
            // Any answer but a failure with no verdict is the end of it:
            // landed, ended with its verdict, or taken by another call
            // since the scan.
            let Err(RuntimeStoreError::Other(e)) = self.drive_completion(key, true).await else { continue };
            tracing::error!(
                target: "weft_broker::runtime_store", key = %key, error = format!("{e:#}"),
                "a completion whose claim lapsed did not land; the expiry loop tries it again a lease from now"
            );
            self.pool
                .execute("UPDATE runtime_file SET progressed_at_unix = $2 WHERE key = $1 AND status = 'completing'", &[key, &now])
                .await
                .context("take the claim of a completion that did not land")?;
        }
        // `$1` in the scan and `$2` in the fence are the store's clock. A
        // completing row is never fenced: it lands or ends by its drive.
        let doomed = |now: &str| {
            format!(
                "((status IN ('active', 'uploaded') AND expires_at_unix < {now}) OR (status <> 'completing' AND {}))",
                abandoned(now)
            )
        };
        let entries: Vec<SweepEntry> = self
            .pool
            .all(
                &format!("SELECT key, FALSE AS kept_active, status, upload_id, replaces FROM runtime_file WHERE {}", doomed("$1")),
                &[&now],
            )
            .await
            .context("expired file scan")?;
        let mut record = false;
        let pass: Result<u64> = async {
            let mut removed = 0;
            for entry in &entries {
                if self.fence_for_reap(&entry.key, &format!("AND {}", doomed("$2")), now).await? != Fence::Fenced {
                    continue;
                }
                let reaped = self.reap_unrecorded(entry, &REAP_RETRY_PAUSES).await?;
                record |= reaped.changes_record;
                if reaped.refused.is_none() {
                    removed += 1;
                }
            }
            Ok(removed)
        }
        .await;
        let removed = self.finish_pass(record, pass).await?;
        if removed > 0 {
            tracing::info!(target: "weft_broker::runtime_store", removed, "expired stored files removed");
        }
        let next: Option<i64> = self
            .pool
            .scalar(
                &format!(
                    "SELECT MIN(at) FROM ( \
                         SELECT f.expires_at_unix AS at FROM runtime_file f \
                          WHERE f.status IN ('active', 'uploaded') AND f.expires_at_unix IS NOT NULL \
                            AND NOT EXISTS (SELECT 1 FROM runtime_file r \
                                            WHERE (r.replaces = f.key OR r.receives = f.key) AND r.status = 'completing') \
                       UNION ALL \
                         SELECT {} FROM runtime_file \
                          WHERE {HELD} \
                     ) due",
                    abandoned_at()
                ),
                &[],
            )
            .await
            .context("next file expiry")?;
        Ok(match next {
            // An expiry and a claim's lapse are a second's mark: the row
            // is doomed once it passed.
            Some(at) => DrainStep::RetryIn(std::time::Duration::from_secs(u64::try_from(at + 1 - now).unwrap_or(0))),
            None => DrainStep::Done,
        })
    }

    /// Mark a fenced row whose bucket reap failed with `error`, its claim
    /// given back ([`MARKED`]: nothing retries it on a timer, so a bucket
    /// that refuses for good wakes nothing). The caller writes the failure
    /// down ([`Self::record_reap_failures`]), once per pass.
    async fn mark_reap_failed(&self, key: &str, error: &anyhow::Error) -> Result<()> {
        tracing::error!(
            target: "weft_broker::runtime_store", key = %key, error = %format!("{error:#}"),
            "a stored file could not be removed from the bucket; it stays marked until a removal, a wipe, its run's \
             end or its tenant running short of space tries it again"
        );
        let reap_error = format!("{error:#}");
        self.pool
            .execute("UPDATE runtime_file SET reap_error = $2, progressed_at_unix = 0 WHERE key = $1 AND status = 'reaping'", &[&key, &reap_error])
            .await
            .context("mark a row whose reap failed")?;
        Ok(())
    }

    /// Write down, as the failure `weft status` shows (`Callee::Reaps`, a
    /// record of its own: the expiry loop writes and clears its own passes'
    /// failures), every row a reap failed on, or clear it once none is
    /// left. Run when a row is marked and when a marked row is removed,
    /// after that change committed. Two of these run one after the other
    /// (a transaction lock), so the last one reads every change made
    /// before it and what it writes stands.
    async fn record_reap_failures(&self) -> Result<()> {
        let mut conn = self.pool.get().await.context("record failed reaps: begin tx")?;
        let tx = conn.transaction().await.context("record failed reaps: begin tx")?;
        tx.execute("SELECT pg_advisory_xact_lock(hashtextextended('weft:runtime-file-reaps', 0))", &[])
            .await
            .context("wait for another record of failed reaps")?;
        let (failing, shown): (i64, Option<String>) = tx
            .one(
                "SELECT COUNT(*), string_agg(key || ': ' || reap_error, '; ' ORDER BY key) FILTER (WHERE n <= 5) \
                 FROM (SELECT key, reap_error, row_number() OVER (ORDER BY key) AS n FROM runtime_file \
                       WHERE status = 'reaping' AND reap_error IS NOT NULL) marked",
                &[],
            )
            .await
            .context("read the rows a reap failed on")?;
        let callee = weft_task_store::unanswered::Callee::Reaps;
        let alerts = if failing == 0 {
            weft_task_store::unanswered::answered(&tx, callee).await?;
            weft_task_store::unanswered::Alerts::default()
        } else {
            let error = format!(
                "{failing} stored file(s) could not be removed from the bucket ({}{}); they hold their tenant's \
                 quota until removed",
                shown.unwrap_or_default(),
                if failing > 5 { "; ..." } else { "" },
            );
            weft_task_store::unanswered::failed(&tx, callee, None, &error).await?
        };
        tx.commit().await.context("record failed reaps: commit")?;
        alerts.log();
        Ok(())
    }

    /// Remove the active file stored under `identity` in `key`'s scope when
    /// it expired, so an upload of the same thing takes its place. A
    /// file whose replacement is completing is left (the begin then says
    /// it is expired and to retry). Its first read is ahead of every
    /// write of the upload ([`RuntimeStoreError::BeforeWrites`]); the fence
    /// and the reap after it are not.
    async fn reap_expired_identity(&self, key: &str, identity: &str) -> StoreResult<()> {
        let now = self.clock.now_unix();
        let expired: Option<SweepEntry> = self
            .pool
            .opt(
                "SELECT key, FALSE AS kept_active, status, upload_id, replaces FROM runtime_file \
                 WHERE regexp_replace(key, '/[^/]+$', '') = $1 AND identity = $2 \
                   AND status = 'active' AND expires_at_unix < $3",
                &[&scope_prefix(key), &identity, &now],
            )
            .await
        .context("look for an expired file under this identity")
        .map_err(RuntimeStoreError::BeforeWrites)?;
        let Some(entry) = expired else { return Ok(()) };
        // Tried once, on the caller's upload: a bucket that refuses fails it.
        let fence = self.fence_for_reap(&entry.key, "AND status = 'active' AND expires_at_unix < $2", now).await?;
        if fence == Fence::Fenced {
            self.reap_fenced(&entry).await?;
        }
        Ok(())
    }
}

/// The ONE definition of a tenant's charged bytes in the runtime-file plane:
/// an active file counts by its size (nothing once it expired by `now`, the
/// store's clock), any other row by its quota-charged reservation. Every
/// quota check and usage view that needs the runtime-file footprint reads
/// through here so the numbers can never disagree.
pub async fn charged_bytes_for(db: &impl Client, tenant: &str, now: i64) -> Result<u64> {
    let bytes: Option<i64> = db
        .scalar(
            &format!(
                "SELECT COALESCE(SUM(CASE WHEN status <> 'active' THEN reserved_bytes \
                                          WHEN {live} THEN size_bytes ELSE 0 END), 0)::BIGINT \
                 FROM runtime_file WHERE tenant_id = $1",
                live = unexpired("$2"),
            ),
            &[&tenant, &now],
        )
        .await
        .context("read tenant charged bytes")?;
    Ok(bytes.unwrap_or(0) as u64)
}

/// How many files a tenant holds against its file cap: every row but an
/// active file expired by `now`.
async fn count_files(db: &impl Client, tenant: &str, now: i64) -> Result<u64> {
    let count: i64 = db
        .scalar(
            &format!(
                "SELECT COUNT(*) FROM runtime_file WHERE tenant_id = $1 AND (status <> 'active' OR {live})",
                live = unexpired("$2"),
            ),
            &[&tenant, &now],
        )
        .await
        .context("count tenant files")?;
    Ok(count as u64)
}

