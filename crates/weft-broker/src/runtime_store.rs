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
use sqlx::PgPool;

use weft_core::storage::key::{CallerAuth, ParsedKey};
use weft_core::storage::{KeepTtl, PresignedPart, StorageScope, StoredFile, StoredFileMeta};
use weft_platform_traits::{ObjectStore, PresignAudience};

use crate::entitlement::{lock_tenant_storage, EntitlementSource};

/// Default TTL of a KEPT execution-scoped file (30 days). Any access bumps
/// the expiry back to now + TTL, so an actively-used survivor never expires.
/// `KeepTtl::Default` resolves to this.
pub const DEFAULT_KEEP_TTL_SECS: u64 = 30 * 24 * 3600;

/// How long an UN-KEPT completed exec file lingers after its run terminates
/// before the expiry sweep deletes it. The terminate sweep stamps
/// `expires_at_unix = now + this` instead of deleting outright, so a user can
/// still open the files list and download a run's output right after the run
/// ends. Deletion then happens on the next expiry-sweep tick past the
/// deadline, and the stamped expiry is what the file lists surface as the
/// remaining lifetime.
pub const EXEC_LINGER_TTL_SECS: i64 = 5 * 60;

/// Default lifetime of a presigned URL when the caller doesn't choose one.
pub const DEFAULT_PRESIGN_TTL_SECS: u64 = 15 * 60;

/// Hard ceiling on a requested presign lifetime. A presign is an explicit,
/// EXPIRING artifact; a year-long one would be a durable public link.
pub const MAX_PRESIGN_TTL_SECS: u64 = 7 * 24 * 3600;

/// How long a 'pending' upload may sit with NO progress (no new part reserved)
/// before the sweep reaps it as abandoned: aborts its multipart upload, deletes
/// its row, frees its quota reservation. Long enough that a legitimately slow
/// upload (large parts on the default 15-min part-URL life, resumed after a
/// blip) is never reaped mid-flight, short enough that a crashed upload's
/// reservation doesn't hold quota hostage. Progress (a `parts` reservation)
/// refreshes the row's clock.
pub const PENDING_RESERVE_GRACE_SECS: i64 = 60 * 60;

/// Default part size for multipart uploads: 8 MiB. Every part is exactly this
/// size except the final one (which may be smaller). Above the 5 MiB floor
/// buckets impose on non-final parts, a 256 KiB multiple, and small enough
/// that a retry re-sends little. For a KNOWN total size the part size scales
/// up so the plan stays under the 10,000-part ceiling.
pub const DEFAULT_PART_SIZE_BYTES: u64 = 8 * 1024 * 1024;

/// Hard ceiling on part numbers (the S3 multipart limit).
const MAX_PARTS: u64 = 10_000;

/// The part size for an upload: the default, scaled up (in whole MiB) when a
/// known total would otherwise exceed the part-count ceiling.
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
fn object_key(key: &str) -> String {
    format!("{RUNTIME_PREFIX}{key}")
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
    #[error(transparent)]
    Other(#[from] anyhow::Error),
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
}

/// What a begin answered: a fresh reservation to upload into, or (asset
/// scope only) the content already stored ACTIVE under its hash, in which
/// case there is nothing to transfer and `key` is the existing file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BeginUpload {
    Ready { key: String, part_size: u64 },
    AlreadyStored { key: String },
}

/// Internal outcome of the begin's reservation transaction.
enum Reserved {
    Fresh,
    AlreadyActive,
}

type StoreResult<T> = Result<T, RuntimeStoreError>;

/// Create the `runtime_file` table. The broker owns this schema (it is the
/// only reader/writer) and runs this at its own boot. Per the
/// no-migration-cruft rule the canonical CREATE lives here and is edited in
/// place (fresh DB on rebuild). The DDL runs in one transaction behind an
/// advisory lock: `IF NOT EXISTS` is idempotent but NOT concurrency-safe in
/// Postgres (replicas racing the same CREATE on a fresh DB both pass the
/// existence check, then one fails on a duplicate catalog key), so concurrent
/// boots serialize and the losers no-op.
pub async fn migrate(pool: &PgPool) -> Result<()> {
    let mut tx = pool.begin().await.context("runtime_file migrate: begin")?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('weft:schema-migrate', 0))")
        .execute(&mut *tx)
        .await
        .context("runtime_file migrate: schema lock")?;
    sqlx::raw_sql(
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
            -- multipart upload). 'reaping': a sweep fenced the row for removal
            -- (writers and reads are locked out; the bucket state goes next,
            -- then the row; a crash mid-reap leaves the row in 'reaping' and
            -- every sweep scan re-finds and retries it). Only 'active' rows
            -- appear in user listings / gets; ALL statuses count toward the
            -- byte quota ('active' by size_bytes, others by reserved_bytes),
            -- which is what makes an in-flight upload unable to blow past the
            -- cap.
            status             TEXT NOT NULL DEFAULT 'active',
            -- True iff this exec-scoped file is flagged to survive the
            -- terminate sweep. Always false for project/shared files (they
            -- are persistent without a flag). Set at begin; a PENDING kept
            -- row is still sweepable (only kept ACTIVE files are spared).
            keep               BOOLEAN NOT NULL DEFAULT FALSE,
            -- Unix seconds at which a kept file expires (access-bumped).
            -- NULL = no expiry (project/shared files, KeepTtl::Never).
            -- Set at complete, never on a pending row.
            expires_at_unix    BIGINT,
            -- The kept file's TTL so an access can recompute expiry. NULL when
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
            -- Progress clock for the abandoned-pending reap: bumped whenever a
            -- part is reserved, so a long multi-part upload that is still
            -- moving is never reaped mid-flight.
            progressed_at_unix BIGINT NOT NULL DEFAULT 0
        );
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
        -- The expiry sweep ranges kept files by their expiry.
        CREATE INDEX IF NOT EXISTS idx_runtime_file_expiry
            ON runtime_file(expires_at_unix) WHERE expires_at_unix IS NOT NULL;
        "#,
    )
    .execute(&mut *tx)
    .await
    .context("runtime_file migrate")?;
    tx.commit().await.context("runtime_file migrate: commit")?;
    Ok(())
}

/// A clock seam so the expiry math is testable without wall-clock. The broker
/// already carries a `weft_platform_traits::Clock`; the store takes one.
pub use weft_platform_traits::Clock;

/// The runtime-file store: PG metadata + bucket bytes, one gatekeeper.
pub struct RuntimeStore {
    pool: PgPool,
    bucket: Arc<dyn ObjectStore>,
    clock: Arc<dyn Clock>,
}

/// One stored-file metadata row, the in-Rust shape of a `runtime_file` row.
#[derive(Debug, Clone, sqlx::FromRow)]
struct FileRow {
    key: String,
    mime_type: String,
    filename: String,
    size_bytes: i64,
    keep: bool,
    expires_at_unix: Option<i64>,
    keep_ttl_secs: Option<i64>,
    created_at_unix: i64,
}

impl FileRow {
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
        }
    }
}

/// What a sweep needs to know per row: spare it (kept ACTIVE file) or reap
/// its bucket state (aborting the in-flight upload of a pending row).
#[derive(Debug, sqlx::FromRow)]
struct SweepEntry {
    key: String,
    kept_active: bool,
    status: String,
    upload_id: Option<String>,
}

/// One in-flight upload's row: the metadata captured at begin plus the
/// multipart resume state. The in-Rust shape of a 'pending' `runtime_file` row.
#[derive(Debug, Clone, sqlx::FromRow)]
struct PendingUpload {
    tenant_id: String,
    mime_type: String,
    filename: String,
    keep: bool,
    keep_ttl_secs: Option<i64>,
    created_at_unix: i64,
    upload_id: Option<String>,
    part_size: i64,
    declared_size: Option<i64>,
}

/// Resolve a `KeepTtl` to its seconds, `None` for `Never`.
fn keep_ttl_secs(ttl: KeepTtl) -> Option<u64> {
    match ttl {
        KeepTtl::Never => None,
        KeepTtl::Default => Some(DEFAULT_KEEP_TTL_SECS),
        KeepTtl::Secs { secs } => Some(secs),
    }
}

impl RuntimeStore {
    pub fn new(pool: PgPool, bucket: Arc<dyn ObjectStore>, clock: Arc<dyn Clock>) -> Self {
        Self { pool, bucket, clock }
    }

    /// One tenant's live footprint (file_count, charged_bytes). Bytes count an
    /// ACTIVE file by its size and a PENDING upload by its reserved (quota-
    /// charged) bytes, so an in-flight upload is visible in usage the moment
    /// it reserves, exactly as the quota check sees it. The quota check + the
    /// usage view read this, so the two can never disagree.
    pub async fn tenant_usage(&self, tenant: &str) -> Result<(u64, u64)> {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM runtime_file WHERE tenant_id = $1")
                .bind(tenant)
                .fetch_one(&self.pool)
                .await
                .context("tenant_usage count")?;
        let bytes = charged_bytes_for(&self.pool, tenant).await?;
        Ok((count as u64, bytes))
    }

    /// Would storing `incoming` more bytes push the tenant over their disk cap,
    /// counting their WHOLE account (every storage plane), read on `tx` under
    /// the caller's lock so the total is one fresh number. This is the single
    /// place the account-wide byte check is assembled; both upload entry points
    /// call it. `account_used_bytes` already includes this plane's charged
    /// bytes, so the check is just `total + incoming > cap`.
    async fn account_would_exceed(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        entitlements: &dyn EntitlementSource,
        tenant: &str,
        incoming: u64,
    ) -> Result<bool> {
        let total = entitlements.account_used_bytes(tx, tenant).await?;
        Ok(entitlements.caps(tenant).await?.disk_bytes_would_exceed(total, incoming))
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
        let UploadSpec { scope, mime, filename, keep, declared_size, content_hash } = *spec;
        // keep only applies to execution scope (project/shared/asset are
        // persistent without a flag); reject loud rather than silently
        // dropping the flag.
        if keep.is_some() && !matches!(scope, StorageScope::Execution) {
            return Err(RuntimeStoreError::Invalid(
                "keep only applies to execution-scoped files; project/shared/asset files are \
                 persistent without a flag"
                    .into(),
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
        let parsed = weft_core::storage::key::key_for_put(caller, scope, &id)
            .map_err(RuntimeStoreError::Denied)?;
        let key = parsed.to_key();
        let tenant = parsed.tenant.clone();
        let part_size = part_size_for(declared_size);
        let ttl = keep.and_then(keep_ttl_secs);

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
            .create_multipart(&object_key(&key))
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
        let reserve = async {
            let mut tx = self
                .pool
                .begin()
                .await
                .context("runtime begin_upload: begin reserve tx")
                .map_err(RuntimeStoreError::Other)?;
            lock_tenant_storage(&mut tx, &tenant)
                .await
                .map_err(RuntimeStoreError::Other)?;
            // A content-addressed (asset) key can legitimately collide: the
            // same content uploaded twice IS the same file. Check under the
            // lock: an ACTIVE row is this begin's idempotent success (nothing
            // to upload), a PENDING row is another upload of the same content
            // mid-flight (a loud conflict; rerun once it settles). Answered
            // structurally instead of via a raw PK violation.
            if content_hash.is_some() {
                let existing: Option<String> = sqlx::query_scalar(
                    "SELECT status FROM runtime_file WHERE key = $1",
                )
                .bind(&key)
                .fetch_optional(&mut *tx)
                .await
                .context("runtime begin_upload: check content-addressed key")
                .map_err(RuntimeStoreError::Other)?;
                match existing.as_deref() {
                    Some("active") => return Ok(Reserved::AlreadyActive),
                    Some(status) => {
                        return Err(RuntimeStoreError::Conflict(format!(
                            "asset '{key}' already exists ({status})"
                        )));
                    }
                    None => {}
                }
            }
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM runtime_file WHERE tenant_id = $1")
                    .bind(&tenant)
                    .fetch_one(&mut *tx)
                    .await
                    .context("runtime begin_upload: count tenant files")
                    .map_err(RuntimeStoreError::Other)?;
            let caps = entitlements
                .caps(&tenant)
                .await
                .context("runtime begin_upload: resolve tenant caps")
                .map_err(RuntimeStoreError::Other)?;
            if caps.file_count_would_exceed(count as u64) {
                return Err(RuntimeStoreError::QuotaExceeded(format!(
                    "tenant '{tenant}' is at its file cap ({} files); delete files or raise the cap",
                    caps.file_cap
                )));
            }
            if let Some(declared) = declared_size {
                if Self::account_would_exceed(&mut tx, entitlements, &tenant, declared)
                    .await
                    .map_err(RuntimeStoreError::Other)?
                {
                    return Err(RuntimeStoreError::QuotaExceeded(format!(
                        "tenant '{tenant}' would exceed its storage quota ({} bytes) by storing \
                         {declared} more",
                        caps.disk_bytes_cap
                    )));
                }
            }
            sqlx::query(
                "INSERT INTO runtime_file \
                 (key, tenant_id, mime_type, filename, size_bytes, status, keep, expires_at_unix, \
                  keep_ttl_secs, created_at_unix, upload_id, part_size, declared_size, \
                  reserved_bytes, progressed_at_unix) \
                 VALUES ($1, $2, $3, $4, 0, 'pending', $5, NULL, $6, $7, $8, $9, $10, $11, $7)",
            )
            .bind(&key)
            .bind(&tenant)
            .bind(mime)
            .bind(filename)
            .bind(keep.is_some())
            .bind(ttl.map(|s| s as i64))
            .bind(now)
            .bind(&upload_id)
            .bind(part_size as i64)
            .bind(declared_size.map(|s| s as i64))
            .bind(declared_size.unwrap_or(0) as i64)
            .execute(&mut *tx)
            .await
            .context("runtime begin_upload: reserve pending row")
            .map_err(RuntimeStoreError::Other)?;
            tx.commit()
                .await
                .context("runtime begin_upload: commit reservation")
                .map_err(RuntimeStoreError::Other)?;
            Ok(Reserved::Fresh)
        }
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
            Ok(Reserved::Fresh) => Ok(BeginUpload::Ready { key, part_size }),
            Ok(Reserved::AlreadyActive) => {
                abort_opened_multipart().await;
                Ok(BeginUpload::AlreadyStored { key })
            }
            Err(e) => {
                abort_opened_multipart().await;
                Err(e)
            }
        }
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
        };
        let assembled = async {
            // The multipart handle, committed with the pending row by begin.
            let mut tx = self
                .pool
                .begin()
                .await
                .context("assemble: read pending row")
                .map_err(RuntimeStoreError::Other)?;
            let upload_id = Self::pending_row(&mut tx, &key)
                .await
                .map_err(RuntimeStoreError::Other)?
                .and_then(|p| p.upload_id)
                .ok_or_else(|| {
                    RuntimeStoreError::Other(anyhow::anyhow!(
                        "assemble: begin committed no upload handle for '{key}'"
                    ))
                })?;
            drop(tx);

            // Concatenate sources into exactly part_size-d parts (only the
            // final one may be smaller; `reserve_parts` validates the
            // slicing), each reserved in the ledger and uploaded directly.
            // Each source is size-checked up front and then read in
            // part-sized ranges, so memory stays bounded at ~one part
            // regardless of source-object and asset size.
            let mut buf: Vec<u8> = Vec::with_capacity(part_size as usize);
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
                        self.assemble_part(caller, &key, &upload_id, chunk, entitlements)
                            .await?;
                    }
                }
            }
            if !buf.is_empty() {
                let chunk = std::mem::take(&mut buf);
                self.assemble_part(caller, &key, &upload_id, chunk, entitlements).await?;
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
                        "failed to abort assembly after error; the sweep will reap it"
                    );
                }
                Err(e)
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
        chunk: Vec<u8>,
        entitlements: &dyn EntitlementSource,
    ) -> StoreResult<()> {
        let size = chunk.len() as u64;
        let reserved = self
            .reserve_parts(caller, key, &[size], entitlements, PresignAudience::Internal)
            .await?;
        let part_number = reserved[0].part_number;
        let etag = self
            .bucket
            .upload_part(&object_key(key), upload_id, part_number, bytes::Bytes::from(chunk))
            .await
            .context("assemble: direct part upload")
            .map_err(RuntimeStoreError::Other)?;
        self.record_part(caller, key, part_number, &etag).await
    }

    /// Reserve + presign the caller's NEXT parts, in order. Every size must be
    /// exactly the upload's part size except the final one (any smaller size
    /// marks the final part; nothing can be reserved after it). Each URL is
    /// signed with its part's EXACT size, so the bucket enforces the
    /// reservation byte-for-byte.
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
        sizes: &[u64],
        entitlements: &dyn EntitlementSource,
        audience: PresignAudience,
    ) -> StoreResult<Vec<PresignedPart>> {
        Self::wall_key(caller, key)?;
        if sizes.is_empty() {
            return Err(RuntimeStoreError::Invalid("no part sizes requested".into()));
        }
        let now = self.clock.now_unix();
        let mut tx = self
            .pool
            .begin()
            .await
            .context("runtime reserve_parts: begin tx")
            .map_err(RuntimeStoreError::Other)?;
        let pending = Self::pending_row(&mut tx, key)
            .await
            .map_err(RuntimeStoreError::Other)?
            .ok_or_else(|| {
                RuntimeStoreError::Invalid(format!(
                    "no in-flight upload for key '{key}'; begin an upload first"
                ))
            })?;
        lock_tenant_storage(&mut tx, &pending.tenant_id)
            .await
            .map_err(RuntimeStoreError::Other)?;
        let upload_id = pending.upload_id.clone().ok_or_else(|| {
            RuntimeStoreError::Other(anyhow::anyhow!(
                "pending row for '{key}' unexpectedly has no upload id (a committed \
                 pending row always carries one); abort this upload and begin again"
            ))
        })?;
        let part_size = pending.part_size as u64;

        // Where the existing reservations stand: highest part number, bytes
        // reserved so far, and whether the FINAL (short) part is already in.
        let (max_part, reserved_sum, has_final): (i32, i64, bool) = sqlx::query_as(
            "SELECT COALESCE(MAX(part_number), 0)::INT, COALESCE(SUM(size_bytes), 0)::BIGINT, \
             EXISTS(SELECT 1 FROM runtime_file_part WHERE key = $1 AND size_bytes < $2) \
             FROM runtime_file_part WHERE key = $1",
        )
        .bind(key)
        .bind(pending.part_size)
        .fetch_one(&mut *tx)
        .await
        .context("runtime reserve_parts: read existing parts")
        .map_err(RuntimeStoreError::Other)?;
        if has_final {
            return Err(RuntimeStoreError::Invalid(format!(
                "upload '{key}' already reserved its final part; complete or abort it"
            )));
        }
        // Size validation: every part is exactly part_size except the last
        // (which may be smaller), and every part is non-empty. A multipart
        // part can never be zero bytes (S3 rejects it); an empty object
        // uploads ZERO parts and is written directly at complete.
        for (i, &size) in sizes.iter().enumerate() {
            let is_last = i == sizes.len() - 1;
            if size == 0 {
                return Err(RuntimeStoreError::Invalid(
                    "a part must be at least 1 byte; an empty object uploads zero parts".into(),
                ));
            }
            if size > part_size || (!is_last && size != part_size) {
                return Err(RuntimeStoreError::Invalid(format!(
                    "part size {size} invalid: every part must be exactly {part_size} bytes \
                     except the final one (which may be smaller)"
                )));
            }
        }
        if max_part as u64 + sizes.len() as u64 > MAX_PARTS {
            return Err(RuntimeStoreError::Invalid(format!(
                "upload '{key}' would exceed {MAX_PARTS} parts; declare the total size at \
                 begin so the part size scales"
            )));
        }
        let incoming: u64 = sizes.iter().sum();
        if let Some(declared) = pending.declared_size {
            // Known size: the parts must be the canonical slices of the
            // declared total, in order. Anything else is a caller bug.
            let mut offset = reserved_sum as u64;
            for &size in sizes {
                let expected = part_size.min((declared as u64).saturating_sub(offset));
                if size != expected {
                    return Err(RuntimeStoreError::Invalid(format!(
                        "part at offset {offset} must be {expected} bytes to slice the \
                         declared {declared}-byte total; got {size}"
                    )));
                }
                offset += size;
            }
        } else {
            // Stream: charge these parts now, under the lock. The account check
            // sums THIS plane's charged bytes (already including this upload's
            // reserved_bytes) plus the other plane's, both read on this tx.
            if Self::account_would_exceed(&mut tx, entitlements, &pending.tenant_id, incoming)
                .await
                .map_err(RuntimeStoreError::Other)?
            {
                // The stream cannot fit: abort the WHOLE upload now (delete the
                // row in-tx so the freed charge is serialized; cascade drops the
                // parts), then abort the bucket's multipart upload. The caller
                // gets a loud quota error; nothing is left to clean.
                sqlx::query("DELETE FROM runtime_file WHERE key = $1 AND status = 'pending'")
                    .bind(key)
                    .execute(&mut *tx)
                    .await
                    .context("runtime reserve_parts: delete over-quota upload")
                    .map_err(RuntimeStoreError::Other)?;
                tx.commit()
                    .await
                    .context("runtime reserve_parts: commit over-quota abort")
                    .map_err(RuntimeStoreError::Other)?;
                if let Err(abort) =
                    self.bucket.abort_multipart(&object_key(key), &upload_id).await
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
            sqlx::query(
                "UPDATE runtime_file SET reserved_bytes = reserved_bytes + $2 \
                 WHERE key = $1 AND status = 'pending'",
            )
            .bind(key)
            .bind(incoming as i64)
            .execute(&mut *tx)
            .await
            .context("runtime reserve_parts: charge stream parts")
            .map_err(RuntimeStoreError::Other)?;
        }
        let mut reserved = Vec::with_capacity(sizes.len());
        for (i, &size) in sizes.iter().enumerate() {
            let part_number = max_part + 1 + i as i32;
            sqlx::query(
                "INSERT INTO runtime_file_part (key, part_number, size_bytes, etag) \
                 VALUES ($1, $2, $3, NULL)",
            )
            .bind(key)
            .bind(part_number)
            .bind(size as i64)
            .execute(&mut *tx)
            .await
            .context("runtime reserve_parts: insert part row")
            .map_err(RuntimeStoreError::Other)?;
            reserved.push((part_number, size));
        }
        // A reservation is progress: refresh the abandoned-pending clock. This
        // is also the LIVENESS ASSERTION for the whole transaction: it gates on
        // status='pending', so if a sweep fenced the row to 'reaping' after our
        // initial read, this hits 0 rows and we fail loud BEFORE commit (the
        // whole reservation rolls back) instead of handing the caller URLs for
        // a multipart the sweep is about to abort. If the fence loses the race
        // instead, its WHERE re-check sees the bumped clock and spares the row.
        let alive = sqlx::query("UPDATE runtime_file SET progressed_at_unix = $2 WHERE key = $1 AND status = 'pending'")
            .bind(key)
            .bind(now)
            .execute(&mut *tx)
            .await
            .context("runtime reserve_parts: bump progress clock")
            .map_err(RuntimeStoreError::Other)?;
        if alive.rows_affected() == 0 {
            return Err(RuntimeStoreError::Invalid(format!(
                "upload '{key}' was swept mid-flight (idle past the reserve grace); \
                 begin the upload again"
            )));
        }
        tx.commit()
            .await
            .context("runtime reserve_parts: commit reservations")
            .map_err(RuntimeStoreError::Other)?;

        // Presign AFTER the commit (no bucket I/O under the tenant lock). If a
        // presign fails here the reservations stand: the caller resumes (which
        // re-presigns exactly these parts), so nothing is stranded.
        let mut parts = Vec::with_capacity(reserved.len());
        for (part_number, size) in reserved {
            let url = self
                .bucket
                .presign_part(
                    &object_key(key),
                    &upload_id,
                    part_number,
                    size,
                    audience,
                    DEFAULT_PRESIGN_TTL_SECS,
                )
                .await
                .context("runtime reserve_parts: presign part (the reservation stands; resume the upload to re-presign)")
                .map_err(RuntimeStoreError::Other)?;
            parts.push(PresignedPart { part_number, size_bytes: size, url });
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
        let updated = sqlx::query(
            "UPDATE runtime_file_part SET etag = $3 WHERE key = $1 AND part_number = $2",
        )
        .bind(key)
        .bind(part_number)
        .bind(etag)
        .execute(&self.pool)
        .await
        .context("runtime record_part")
        .map_err(RuntimeStoreError::Other)?;
        if updated.rows_affected() == 0 {
            return Err(RuntimeStoreError::Invalid(format!(
                "part {part_number} of '{key}' was never reserved (or the upload is no \
                 longer in flight)"
            )));
        }
        // A landed part is progress: refresh the abandoned-pending clock. Also
        // the LIVENESS ASSERTION: 0 rows means a sweep fenced the row to
        // 'reaping' (the part row above still existed, so that update passed)
        // and the multipart is being aborted underneath the caller; fail loud
        // so the client re-begins instead of believing the part counted.
        let alive = sqlx::query("UPDATE runtime_file SET progressed_at_unix = $2 WHERE key = $1 AND status = 'pending'")
            .bind(key)
            .bind(self.clock.now_unix())
            .execute(&self.pool)
            .await
            .context("runtime record_part: bump progress clock")
            .map_err(RuntimeStoreError::Other)?;
        if alive.rows_affected() == 0 {
            return Err(RuntimeStoreError::Invalid(format!(
                "upload '{key}' was swept mid-flight (idle past the reserve grace); \
                 begin the upload again"
            )));
        }
        Ok(())
    }

    /// Finalize an upload: every reserved part must have been reported done
    /// (and, for a known size, the parts must sum to the declared total).
    /// Completes the bucket's multipart upload from OUR stored etag list,
    /// verifies the assembled size equals the charged reservation, flips the
    /// row active, stamps the keep expiry, and drops the part rows. Idempotent
    /// on retry: a key that is already active returns its metadata.
    pub async fn complete_upload(
        &self,
        caller: &CallerAuth,
        key: &str,
    ) -> StoreResult<StoredFileMeta> {
        Self::wall_key(caller, key)?;
        let mut tx = self
            .pool
            .begin()
            .await
            .context("runtime complete_upload: begin read tx")
            .map_err(RuntimeStoreError::Other)?;
        let Some(pending) = Self::pending_row(&mut tx, key)
            .await
            .map_err(RuntimeStoreError::Other)?
        else {
            // No pending row: an ACTIVE row is a completed retry (idempotent),
            // no row at all is an unknown key.
            let existing = self.query_active_meta(&mut tx, key).await?;
            tx.commit()
                .await
                .context("runtime complete_upload: commit idempotent read")
                .map_err(RuntimeStoreError::Other)?;
            return existing.ok_or_else(|| {
                RuntimeStoreError::NotFound(format!("no upload in flight for key '{key}'"))
            });
        };
        let parts: Vec<(i32, Option<String>, i64)> = sqlx::query_as(
            "SELECT part_number, etag, size_bytes FROM runtime_file_part \
             WHERE key = $1 ORDER BY part_number",
        )
        .bind(key)
        .fetch_all(&mut *tx)
        .await
        .context("runtime complete_upload: read parts")
        .map_err(RuntimeStoreError::Other)?;
        tx.commit()
            .await
            .context("runtime complete_upload: commit read")
            .map_err(RuntimeStoreError::Other)?;

        let upload_id = pending.upload_id.clone().ok_or_else(|| {
            RuntimeStoreError::Other(anyhow::anyhow!(
                "pending row for '{key}' unexpectedly has no upload id (a committed \
                 pending row always carries one); abort this upload and begin again"
            ))
        })?;
        // Empty object: no parts. S3 multipart cannot represent a zero-byte
        // object (a part is never empty), so abort the (empty) multipart and
        // write the object directly. A declared NON-zero size with no parts is
        // an incomplete upload, not an empty file.
        if parts.is_empty() {
            if let Some(declared) = pending.declared_size {
                if declared > 0 {
                    return Err(RuntimeStoreError::Invalid(format!(
                        "upload '{key}' declared {declared} bytes but no parts were uploaded; \
                         resume the upload to finish it, or abort it"
                    )));
                }
            }
            self.bucket
                .abort_multipart(&object_key(key), &upload_id)
                .await
                .context("runtime complete_upload: abort empty multipart")
                .map_err(RuntimeStoreError::Other)?;
            self.bucket
                .put(&object_key(key), bytes::Bytes::new())
                .await
                .context("runtime complete_upload: write empty object")
                .map_err(RuntimeStoreError::Other)?;
            return self.flip_active(key, &pending, 0).await;
        }
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
        let etag_list: Vec<(i32, String)> = parts
            .iter()
            .map(|(n, etag, _)| (*n, etag.clone().expect("missing etags rejected above")))
            .collect();

        // Assemble in the bucket (no DB lock held across bucket I/O). If a
        // concurrent retry completed first, the multipart is gone: answer from
        // the now-active row instead of failing.
        let actual = match self
            .bucket
            .complete_multipart(&object_key(key), &upload_id, &etag_list)
            .await
        {
            Ok(size) => size,
            Err(e) => {
                if let Some(meta) =
                    self.row(key).await.map_err(RuntimeStoreError::Other)?.map(|r| r.to_meta())
                {
                    return Ok(meta);
                }
                return Err(RuntimeStoreError::Other(e.context(
                    "runtime complete_upload: bucket completion failed (the upload is \
                     still in flight; retry complete, or resume/abort the upload)",
                )));
            }
        };
        // Every part's size was signed into its URL, so the assembled size can
        // only diverge from the reservation on a bucket anomaly. That object
        // was never charged as active: remove it and free the reservation
        // rather than activate a file whose size the ledger never approved.
        if actual != total {
            if let Err(del) = self.bucket.delete(&object_key(key)).await {
                tracing::error!(
                    target: "weft_broker::runtime_store",
                    key = %key, error = %del,
                    "failed to delete size-mismatched assembled object; \
                     an admin delete or wipe can remove it"
                );
            }
            if let Err(del) = sqlx::query("DELETE FROM runtime_file WHERE key = $1 AND status = 'pending'")
                .bind(key)
                .execute(&self.pool)
                .await
            {
                tracing::error!(
                    target: "weft_broker::runtime_store",
                    key = %key, error = %del,
                    "failed to delete size-mismatched upload row; the pending sweep will reap it"
                );
            }
            return Err(RuntimeStoreError::Other(anyhow::anyhow!(
                "assembled object for '{key}' is {actual} bytes but {total} were reserved; \
                 the upload was discarded, re-upload the file"
            )));
        }

        self.flip_active(key, &pending, actual).await
    }

    /// Flip a completed upload's pending row to active with its final size, drop
    /// its part rows, and return the stored-file metadata. Shared by the normal
    /// (multipart-assembled) and empty-object completion paths, since both end
    /// the same way. The expiry is stamped NOW (completion is when a kept file
    /// starts existing); `reserved_bytes` is set to the final size so the
    /// tenant's charged sum is identical before and after the flip. A lost race
    /// (a concurrent retry flipped it first) answers from the now-active row.
    async fn flip_active(
        &self,
        key: &str,
        pending: &PendingUpload,
        actual: u64,
    ) -> StoreResult<StoredFileMeta> {
        let now = self.clock.now_unix();
        let expires_at = pending.keep_ttl_secs.map(|s| now + s);
        let mut tx = self
            .pool
            .begin()
            .await
            .context("runtime complete_upload: begin finalize tx")
            .map_err(RuntimeStoreError::Other)?;
        lock_tenant_storage(&mut tx, &pending.tenant_id)
            .await
            .map_err(RuntimeStoreError::Other)?;
        let updated = sqlx::query(
            "UPDATE runtime_file SET \
               size_bytes = $2, status = 'active', expires_at_unix = $3, \
               upload_id = NULL, part_size = NULL, declared_size = NULL, reserved_bytes = $2 \
             WHERE key = $1 AND status = 'pending'",
        )
        .bind(key)
        .bind(actual as i64)
        .bind(expires_at)
        .execute(&mut *tx)
        .await
        .context("runtime complete_upload: flip active")
        .map_err(RuntimeStoreError::Other)?;
        if updated.rows_affected() == 0 {
            // A concurrent retry flipped it first: answer from the active row.
            let existing = self.query_active_meta(&mut tx, key).await?;
            tx.commit()
                .await
                .context("runtime complete_upload: commit lost-race read")
                .map_err(RuntimeStoreError::Other)?;
            return existing.ok_or_else(|| {
                RuntimeStoreError::Other(anyhow::anyhow!(
                    "upload '{key}' vanished while completing (swept mid-flight?); \
                     re-upload the file"
                ))
            });
        }
        sqlx::query("DELETE FROM runtime_file_part WHERE key = $1")
            .bind(key)
            .execute(&mut *tx)
            .await
            .context("runtime complete_upload: drop part rows")
            .map_err(RuntimeStoreError::Other)?;
        tx.commit()
            .await
            .context("runtime complete_upload: commit finalize")
            .map_err(RuntimeStoreError::Other)?;
        Ok(StoredFileMeta {
            key: key.to_string(),
            mime_type: pending.mime_type.clone(),
            size_bytes: actual,
            filename: pending.filename.clone(),
            keep: pending.keep,
            expires_at_unix: expires_at,
            keep_ttl_secs: pending.keep_ttl_secs.map(|s| s as u64),
            created_at_unix: pending.created_at_unix,
        })
    }

    /// Resume an interrupted upload: re-presign exactly the reserved parts
    /// that were never reported done. Returns the upload's part size + those
    /// parts. (A part that was uploaded but whose done-report was lost is
    /// simply re-uploaded: its etag is only trusted from our own records.)
    pub async fn resume_upload(
        &self,
        caller: &CallerAuth,
        key: &str,
        audience: PresignAudience,
    ) -> StoreResult<(u64, Vec<PresignedPart>)> {
        Self::wall_key(caller, key)?;
        let mut tx = self
            .pool
            .begin()
            .await
            .context("runtime resume_upload: begin read tx")
            .map_err(RuntimeStoreError::Other)?;
        let pending = Self::pending_row(&mut tx, key)
            .await
            .map_err(RuntimeStoreError::Other)?;
        tx.commit()
            .await
            .context("runtime resume_upload: commit read")
            .map_err(RuntimeStoreError::Other)?;
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
        let missing: Vec<(i32, i64)> = sqlx::query_as(
            "SELECT part_number, size_bytes FROM runtime_file_part \
             WHERE key = $1 AND etag IS NULL ORDER BY part_number",
        )
        .bind(key)
        .fetch_all(&self.pool)
        .await
        .context("runtime resume_upload: read missing parts")
        .map_err(RuntimeStoreError::Other)?;
        let mut parts = Vec::with_capacity(missing.len());
        for (part_number, size) in missing {
            let url = self
                .bucket
                .presign_part(
                    &object_key(key),
                    &upload_id,
                    part_number,
                    size as u64,
                    audience,
                    DEFAULT_PRESIGN_TTL_SECS,
                )
                .await
                .context("runtime resume_upload: presign missing part")
                .map_err(RuntimeStoreError::Other)?;
            parts.push(PresignedPart { part_number, size_bytes: size as u64, url });
        }
        Ok((pending.part_size as u64, parts))
    }

    /// Cancel an in-flight upload: abort the bucket's multipart upload, then
    /// delete the pending row (freeing the quota reservation; the part rows
    /// cascade). Idempotent: a key with no in-flight upload and no file is
    /// already in the aborted state. A COMPLETED file is not abortable
    /// (delete it instead).
    pub async fn abort_upload(&self, caller: &CallerAuth, key: &str) -> StoreResult<()> {
        Self::wall_key(caller, key)?;
        let mut tx = self
            .pool
            .begin()
            .await
            .context("runtime abort_upload: begin read tx")
            .map_err(RuntimeStoreError::Other)?;
        let pending = Self::pending_row(&mut tx, key)
            .await
            .map_err(RuntimeStoreError::Other)?;
        tx.commit()
            .await
            .context("runtime abort_upload: commit read")
            .map_err(RuntimeStoreError::Other)?;
        let Some(pending) = pending else {
            return match self.row(key).await.map_err(RuntimeStoreError::Other)? {
                Some(_) => Err(RuntimeStoreError::Invalid(format!(
                    "'{key}' already completed; delete the file instead of aborting"
                ))),
                None => Ok(()),
            };
        };
        // Bucket first, row second: if the bucket abort fails the row (and its
        // upload_id) survives, so a retry or the sweep can re-abort. The
        // reverse order would strand an incomplete multipart upload with no
        // handle anywhere (only the bucket lifecycle rule would reap it).
        if let Some(upload_id) = &pending.upload_id {
            self.bucket
                .abort_multipart(&object_key(key), upload_id)
                .await
                .context("runtime abort_upload: abort multipart")
                .map_err(RuntimeStoreError::Other)?;
        }
        sqlx::query("DELETE FROM runtime_file WHERE key = $1 AND status = 'pending'")
            .bind(key)
            .execute(&self.pool)
            .await
            .context("runtime abort_upload: delete pending row")
            .map_err(RuntimeStoreError::Other)?;
        Ok(())
    }

    /// The pending (in-flight upload) row for a key, read inside an open
    /// transaction. None when the key has no pending row (active or unknown).
    async fn pending_row(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        key: &str,
    ) -> Result<Option<PendingUpload>> {
        Ok(sqlx::query_as::<_, PendingUpload>(
            "SELECT tenant_id, mime_type, filename, keep, keep_ttl_secs, created_at_unix, \
             upload_id, part_size, declared_size \
             FROM runtime_file WHERE key = $1 AND status = 'pending'",
        )
        .bind(key)
        .fetch_optional(&mut **tx)
        .await
        .context("read pending upload row")?)
    }

    /// Read a key's ACTIVE row inside an open transaction (the complete-upload
    /// disambiguation path: no pending row means the key is either already
    /// active, a retried complete, or was never reserved). None if no active row.
    async fn query_active_meta(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        key: &str,
    ) -> StoreResult<Option<StoredFileMeta>> {
        let row: Option<(String, String, i64, bool, Option<i64>, Option<i64>, i64)> =
            sqlx::query_as(
                "SELECT mime_type, filename, size_bytes, keep, expires_at_unix, keep_ttl_secs, \
                 created_at_unix FROM runtime_file WHERE key = $1 AND status = 'active'",
            )
            .bind(key)
            .fetch_optional(&mut **tx)
            .await
            .context("runtime complete_upload: read existing row")
            .map_err(RuntimeStoreError::Other)?;
        Ok(row.map(|(mime_type, filename, size_bytes, keep, expires_at_unix, keep_ttl_secs, created_at_unix)| {
            StoredFileMeta {
                key: key.to_string(),
                mime_type,
                filename,
                size_bytes: size_bytes as u64,
                keep,
                expires_at_unix,
                keep_ttl_secs: keep_ttl_secs.map(|s| s as u64),
                created_at_unix,
            }
        }))
    }

    /// The metadata row for an ACTIVE (finalized) file, or None if absent or still
    /// pending. Every user-facing read (meta/get/download/keep/presign) goes through
    /// here, so a half-uploaded 'pending' file reads as not-found until it finalizes.
    /// Access does NOT bump a kept file's expiry here (a metadata peek is not an
    /// access); the byte `get`/`download_url` bumps it.
    async fn row(&self, key: &str) -> Result<Option<FileRow>> {
        Ok(sqlx::query_as::<_, FileRow>(
            "SELECT key, mime_type, filename, size_bytes, keep, expires_at_unix, keep_ttl_secs, created_at_unix \
             FROM runtime_file WHERE key = $1 AND status = 'active'",
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .context("runtime row")?)
    }

    /// Metadata only (no access bump). The caller has already passed the wall.
    pub async fn meta(&self, parsed: &ParsedKey) -> StoreResult<StoredFileMeta> {
        let key = parsed.to_key();
        self.row(&key)
            .await
            .map_err(RuntimeStoreError::Other)?
            .map(|r| r.to_meta())
            .ok_or_else(|| RuntimeStoreError::NotFound(key))
    }

    /// Bump a kept file's expiry to now + its TTL (an access keeps it alive).
    /// No-op for files with no expiry (project/shared, KeepTtl::Never).
    async fn bump_expiry(&self, row: &FileRow) -> Result<()> {
        let Some(ttl) = row.keep_ttl_secs else {
            return Ok(());
        };
        let new_expiry = self.clock.now_unix() + ttl;
        sqlx::query("UPDATE runtime_file SET expires_at_unix = $1 WHERE key = $2")
            .bind(new_expiry)
            .bind(&row.key)
            .execute(&self.pool)
            .await
            .context("bump expiry")?;
        Ok(())
    }

    /// Delete a file (object + row). A missing row is a not-found so the caller
    /// learns the key was already gone.
    ///
    /// Order matters: delete the OBJECT first (idempotent: absent = ok), then
    /// the row. If the row delete then fails, the object is gone but the row
    /// lingers, and a later `get` hits the loud "row but no object (torn write)"
    /// error which a retried delete cleans up. The reverse order (row first)
    /// would, on an object-delete failure, leave a SILENT orphan object with no
    /// row, which nothing can ever reach or reclaim, exactly the untouchable
    /// junk the put path's orphan-cleanup is designed to prevent.
    pub async fn delete(&self, parsed: &ParsedKey) -> StoreResult<()> {
        let key = parsed.to_key();
        // Confirm the row exists first (so a delete of an unknown key is a clean
        // 404), but do the destructive object delete before removing the row.
        if self.row(&key).await.map_err(RuntimeStoreError::Other)?.is_none() {
            return Err(RuntimeStoreError::NotFound(key));
        }
        self.bucket
            .delete(&object_key(&key))
            .await
            .context("runtime delete object")
            .map_err(RuntimeStoreError::Other)?;
        sqlx::query("DELETE FROM runtime_file WHERE key = $1")
            .bind(&key)
            .execute(&self.pool)
            .await
            .context("runtime delete row")
            .map_err(RuntimeStoreError::Other)?;
        Ok(())
    }

    /// List every ACTIVE file under a key prefix (a scope, or a whole tenant). The
    /// prefix is produced by the wall (`prefix_for_list` / `tenant_prefix`), so it
    /// is always tenant-anchored. Pending (half-uploaded) files are excluded: they
    /// are not real files a user can see yet.
    pub async fn list(&self, prefix: &str) -> Result<Vec<StoredFileMeta>> {
        let pattern = like_prefix(prefix);
        let rows = sqlx::query_as::<_, FileRow>(
            "SELECT key, mime_type, filename, size_bytes, keep, expires_at_unix, keep_ttl_secs, created_at_unix \
             FROM runtime_file WHERE key LIKE $1 ESCAPE '\\' AND status = 'active' ORDER BY key",
        )
        .bind(&pattern)
        .fetch_all(&self.pool)
        .await
        .context("runtime list")?;
        Ok(rows.iter().map(FileRow::to_meta).collect())
    }


    /// Every key under a prefix REGARDLESS of status (active + pending), with
    /// what a sweep needs: is it a kept ACTIVE file (spared by the terminate
    /// sweep), and the in-flight upload id to abort (pending rows). Sweeps use
    /// this (not `list`) so a half-uploaded file's bucket state is cleaned
    /// too; the bucket never keeps state whose row a sweep removed.
    async fn keys_under(&self, prefix: &str) -> Result<Vec<SweepEntry>> {
        let pattern = like_prefix(prefix);
        Ok(sqlx::query_as::<_, SweepEntry>(
            "SELECT key, (keep AND status = 'active') AS kept_active, status, upload_id \
             FROM runtime_file WHERE key LIKE $1 ESCAPE '\\'",
        )
        .bind(&pattern)
        .fetch_all(&self.pool)
        .await
        .context("runtime keys_under")?)
    }

    /// Steps 2+3 of a fenced reap (the caller has already flipped the row to
    /// 'reaping'): remove the bucket state, then the row. If the bucket reap
    /// fails, the error propagates and the row STAYS in 'reaping', which every
    /// sweep scan re-finds and retries. So a crash between the steps never
    /// strands an object without a row (nothing would ever revisit it: all
    /// reclaim paths are row-driven) nor a charged row pointing at deleted
    /// bytes; the only residue is a 'reaping' row that self-heals next tick.
    async fn reap_fenced(&self, key: &str, upload_id: Option<&str>) -> Result<()> {
        self.reap_bucket_state(key, upload_id).await?;
        sqlx::query("DELETE FROM runtime_file WHERE key = $1")
            .bind(key)
            .execute(&self.pool)
            .await
            .context("delete reaped row")?;
        Ok(())
    }

    /// Remove a doomed key's bucket state: abort its in-flight multipart
    /// upload (if any; idempotent) and delete its object (idempotent). Every
    /// sweep/wipe path funnels through here so no path can forget the abort.
    async fn reap_bucket_state(&self, key: &str, upload_id: Option<&str>) -> Result<()> {
        if let Some(id) = upload_id {
            self.bucket
                .abort_multipart(&object_key(key), id)
                .await
                .with_context(|| format!("abort in-flight upload for {key}"))?;
        }
        self.bucket
            .delete(&object_key(key))
            .await
            .with_context(|| format!("delete object {key}"))?;
        Ok(())
    }

    /// Flag an exec-scoped file to survive the terminate sweep, with a TTL.
    /// Project/shared files are persistent without a flag (rejected loud).
    pub async fn keep(&self, parsed: &ParsedKey, ttl: KeepTtl) -> StoreResult<StoredFileMeta> {
        if !matches!(parsed.scope, weft_core::storage::key::KeyScope::Exec { .. }) {
            return Err(RuntimeStoreError::Invalid(
                "keep only applies to execution-scoped files; project/shared files are \
                 persistent without a flag"
                    .into(),
            ));
        }
        let key = parsed.to_key();
        let ttl_secs = keep_ttl_secs(ttl);
        let expires_at = ttl_secs.map(|s| self.clock.now_unix() + s as i64);
        let row: Option<FileRow> = sqlx::query_as::<_, FileRow>(
            "UPDATE runtime_file SET keep = TRUE, keep_ttl_secs = $1, expires_at_unix = $2 \
             WHERE key = $3 AND status <> 'reaping' \
             RETURNING key, mime_type, filename, size_bytes, keep, expires_at_unix, keep_ttl_secs, created_at_unix",
        )
        .bind(ttl_secs.map(|s| s as i64))
        .bind(expires_at)
        .bind(&key)
        .fetch_optional(&self.pool)
        .await
        .context("runtime keep")
        .map_err(RuntimeStoreError::Other)?;
        row.map(|r| r.to_meta()).ok_or_else(|| RuntimeStoreError::NotFound(key))
    }

    /// Mint a presigned GET URL for a file, valid for a clamped TTL. The
    /// browser/external caller streams the bytes directly from the bucket; the
    /// broker never proxies them. Minting counts as access (bumps the expiry),
    /// and a missing file fails the mint rather than handing out a 404 URL.
    pub async fn presign(&self, parsed: &ParsedKey, ttl_secs: Option<u64>) -> StoreResult<String> {
        // Handed to an EXTERNAL URL-accepting API (the node's `ctx.storage.presign`),
        // which streams from the bucket over the public network -> External audience.
        Ok(self.presign_get(parsed, PresignAudience::External, ttl_secs).await?.1)
    }

    /// Mint a presigned GET URL a WORKER uses to read a runtime file's bytes
    /// DIRECTLY from the bucket, plus its metadata. Bytes never transit the broker.
    /// Signed for the INTERNAL endpoint (the worker is in-cluster). Counts as access
    /// (bumps a kept file's expiry), like the old streaming get did.
    pub async fn download_url(
        &self,
        parsed: &ParsedKey,
        audience: PresignAudience,
        ttl_secs: Option<u64>,
    ) -> StoreResult<(StoredFileMeta, String)> {
        self.presign_get(parsed, audience, ttl_secs).await
    }

    /// Shared body of the presigned-GET mints: load the row (404 if absent), bump a
    /// kept file's expiry (minting IS an access), clamp the TTL, sign for `audience`.
    /// Returns the file's metadata + the URL.
    async fn presign_get(
        &self,
        parsed: &ParsedKey,
        audience: PresignAudience,
        ttl_secs: Option<u64>,
    ) -> StoreResult<(StoredFileMeta, String)> {
        let key = parsed.to_key();
        let row = self
            .row(&key)
            .await
            .map_err(RuntimeStoreError::Other)?
            .ok_or_else(|| RuntimeStoreError::NotFound(key.clone()))?;
        self.bump_expiry(&row).await.map_err(RuntimeStoreError::Other)?;
        let ttl = ttl_secs.unwrap_or(DEFAULT_PRESIGN_TTL_SECS).min(MAX_PRESIGN_TTL_SECS).max(1);
        let url = self
            .bucket
            .presign_get(&object_key(&key), audience, ttl)
            .await
            .context("runtime presign GET")
            .map_err(RuntimeStoreError::Other)?;
        Ok((row.to_meta(), url))
    }

    /// Wipe every file under a wall-validated prefix (a `weft files rm` of a
    /// scope, or a tenant-delete). Deletes rows + objects; returns the count.
    /// The prefix MUST have passed `validate_wipe_prefix` at the edge.
    pub async fn wipe_prefix(&self, prefix: &str) -> Result<u64> {
        // ALL keys under the prefix (active + pending), so a half-uploaded
        // file's bucket state (in-flight upload + any object) is wiped too.
        // Same fenced three-step reap as the sweeps (fence the row to
        // 'reaping', reap bucket, delete row): a crash mid-wipe leaves only
        // 'reaping' rows the expiry sweep re-finds and finishes, never a live
        // row pointing at deleted bytes. The wipe is unconditional (the whole
        // prefix dies), so the fence has no doom re-check; it exists purely to
        // lock out writers/readers and make the residue self-healing.
        let mut removed = 0;
        for entry in self.keys_under(prefix).await? {
            let fenced = sqlx::query("UPDATE runtime_file SET status = 'reaping' WHERE key = $1")
                .bind(&entry.key)
                .execute(&self.pool)
                .await
                .context("wipe fence")?
                .rows_affected();
            if fenced == 0 {
                continue;
            }
            self.reap_fenced(&entry.key, entry.upload_id.as_deref()).await?;
            removed += 1;
        }
        Ok(removed)
    }

    /// Terminate sweep: close out a color's un-kept exec files (the
    /// `<tenant>/exec/<color>/` prefix, kept files excepted).
    ///
    /// - A COMPLETED un-kept file is not deleted here: it gets
    ///   `expires_at_unix = now + EXEC_LINGER_TTL_SECS` stamped, so the user
    ///   can still list/download a run's output right after the run ends; the
    ///   expiry sweep deletes it once the linger passes. The stamped deadline
    ///   is what the file lists surface as the remaining lifetime.
    /// - A PENDING row (a crashed/abandoned upload) is reaped immediately,
    ///   in-flight multipart aborted: a half-uploaded file has nothing worth
    ///   downloading (even a kept-flagged one: keep only spares a COMPLETED
    ///   file). A leftover 'reaping' row from a crashed reap is retried.
    ///
    /// Returns `(reaped, lingering)`: rows removed now vs stamped to expire.
    pub async fn sweep_exec(&self, tenant: &str, color: &str) -> Result<(u64, u64)> {
        // Rendered through the key grammar (never hand-built): validates both
        // segments and keeps the scope tag single-sourced.
        let prefix = weft_core::storage::key::exec_prefix(tenant, color)
            .map_err(|e| anyhow::anyhow!("sweep_exec: {e}"))?;
        let mut reaped = 0;
        let mut lingering = 0;
        for entry in self.keys_under(&prefix).await? {
            if entry.kept_active {
                continue;
            }
            if entry.status == "active" {
                // Completed un-kept file: stamp the linger deadline. The guard
                // re-checks the row is still an un-kept active (a keep that
                // landed since the scan wins and the file survives untouched),
                // and only stamps a NULL expiry so a re-delivered terminate
                // sweep (the queue is idempotent) can't keep pushing the
                // deadline out.
                let stamped = sqlx::query(
                    "UPDATE runtime_file SET expires_at_unix = $2 \
                     WHERE key = $1 AND status = 'active' AND NOT keep \
                       AND expires_at_unix IS NULL",
                )
                .bind(&entry.key)
                .bind(self.clock.now_unix() + EXEC_LINGER_TTL_SECS)
                .execute(&self.pool)
                .await
                .context("linger stamp")?
                .rows_affected();
                lingering += stamped;
                continue;
            }
            // FENCE first: flip the row to 'reaping', atomically re-checking it
            // is still a pending/reaping row (a completion that landed since
            // the scan wins: the row is 'active' now and the clause spares it).
            // The flip locks out every writer and reader (record_part/complete
            // gate on 'pending'; keep/get/download gate on 'active'), so the
            // bucket reap below can never race an in-flight upload, and a
            // crash at any point leaves a 'reaping' row this sweep's retry
            // (and the expiry sweep) re-finds. Neither delete-object-first (a
            // charged row pointing at gone bytes on crash) nor
            // delete-row-first (an orphan object nothing row-driven ever
            // revisits) has that property.
            let fenced = sqlx::query(
                "UPDATE runtime_file SET status = 'reaping' \
                 WHERE key = $1 AND status IN ('pending', 'reaping')",
            )
            .bind(&entry.key)
            .execute(&self.pool)
            .await
            .context("sweep fence")?
            .rows_affected();
            if fenced == 0 {
                continue;
            }
            self.reap_fenced(&entry.key, entry.upload_id.as_deref()).await?;
            reaped += 1;
        }
        Ok((reaped, lingering))
    }

    /// Expiry sweep: delete kept files whose `expires_at_unix` has passed AND reap
    /// abandoned uploads (pending rows older than the reserve grace). Runs
    /// periodically (an actively-used kept file's expiry is bumped on every access,
    /// so only genuinely-idle survivors are reclaimed).
    ///
    /// The pending reap is what closes the abandoned-upload window for scopes an
    /// exec sweep never touches (project/shared): a row is reserved at begin, so a
    /// crash mid-upload leaves a 'pending' row holding a quota reservation and an
    /// in-flight multipart upload. Once the row has made NO progress (no part
    /// reserved or landed) for longer than the grace, it is abandoned: abort the
    /// multipart, delete the row (freeing the reservation). The bucket's
    /// `AbortIncompleteMultipartUpload` lifecycle rule is the belt-and-suspenders
    /// floor for upload state this reap can never see (and for a part PUT that
    /// raced an abort and landed after it).
    pub async fn sweep_expired(&self) -> Result<u64> {
        let now = self.clock.now_unix();
        let pending_cutoff = now - PENDING_RESERVE_GRACE_SECS;
        // One scan for THREE conditions: an expired kept file, an abandoned
        // pending upload, or a 'reaping' row a crashed reap left behind (from
        // ANY sweep; this scan is the global retry net for those).
        let doomed: Vec<SweepEntry> = sqlx::query_as(
            "SELECT key, (keep AND status = 'active') AS kept_active, status, upload_id \
             FROM runtime_file \
             WHERE (expires_at_unix IS NOT NULL AND expires_at_unix < $1) \
                OR (status = 'pending' AND progressed_at_unix < $2) \
                OR status = 'reaping'",
        )
        .bind(now)
        .bind(pending_cutoff)
        .fetch_all(&self.pool)
        .await
        .context("expiry/pending scan")?;
        let mut removed = 0;
        for entry in &doomed {
            // FENCE first: flip the row to 'reaping', atomically re-checking
            // the doom clause. A pending row that PROGRESSED between scan and
            // fence is no longer abandoned: the clause spares it and its
            // in-flight multipart survives. The flip locks out every writer
            // and reader (record_part/complete gate on 'pending'; keep/get/
            // download gate on 'active'), so the bucket reap can never race an
            // in-flight upload, and a crash at any point leaves a 'reaping'
            // row the next tick's scan re-finds. Neither delete-object-first
            // (a charged row pointing at gone bytes on crash) nor
            // delete-row-first (an orphan object nothing row-driven ever
            // revisits) has that property.
            let fenced = sqlx::query(
                "UPDATE runtime_file SET status = 'reaping' \
                 WHERE key = $1 \
                   AND ((expires_at_unix IS NOT NULL AND expires_at_unix < $2) \
                        OR (status = 'pending' AND progressed_at_unix < $3) \
                        OR status = 'reaping')",
            )
            .bind(&entry.key)
            .bind(now)
            .bind(pending_cutoff)
            .execute(&self.pool)
            .await
            .context("expiry fence")?
            .rows_affected();
            if fenced == 0 {
                continue;
            }
            self.reap_fenced(&entry.key, entry.upload_id.as_deref()).await?;
            removed += 1;
        }
        Ok(removed)
    }
}

/// The ONE definition of a tenant's charged bytes in the runtime-file plane:
/// an active file counts by its size, a pending upload by its quota-charged
/// reservation. Every quota check and usage view that needs the runtime-file
/// footprint reads through here so the numbers can never disagree.
pub async fn charged_bytes_for<'e, E>(executor: E, tenant: &str) -> Result<u64>
where
    E: sqlx::PgExecutor<'e>,
{
    let bytes: Option<i64> = sqlx::query_scalar(
        "SELECT COALESCE(SUM(CASE WHEN status = 'active' THEN size_bytes ELSE reserved_bytes END), 0)::BIGINT \
         FROM runtime_file WHERE tenant_id = $1",
    )
    .bind(tenant)
    .fetch_one(executor)
    .await
    .context("read tenant charged bytes")?;
    Ok(bytes.unwrap_or(0) as u64)
}

/// Build the concrete stored-file value (the `__weft_<kind>__` marker) from a
/// metadata row, for the put response the worker re-emits onto edges.
pub fn meta_to_stored_file(meta: &StoredFileMeta) -> StoredFile {
    StoredFile {
        key: meta.key.clone(),
        mime_type: meta.mime_type.clone(),
        size_bytes: meta.size_bytes,
        filename: meta.filename.clone(),
    }
}

