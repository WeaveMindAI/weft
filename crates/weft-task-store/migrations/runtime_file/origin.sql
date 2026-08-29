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
        -- One row per minted PUBLIC RELAY link: the public
        -- `/public/files/{token}` route resolves the token here and
        -- streams the file. `fetch_url` is a presigned in-cluster GET
        -- the relay reads the bytes from, signed for the same lifetime
        -- as the token. Rows expire with the link; every mint deletes
        -- the expired ones, so the table stays the size of the live
        -- link set. ON DELETE CASCADE ties a link to its file row, so a
        -- deleted/swept file takes its links with it and a live token
        -- can never point at bytes that are gone (a dead token is a
        -- clean 404, never a broken stream).
        CREATE TABLE IF NOT EXISTS public_file_link (
            token           TEXT PRIMARY KEY,
            key             TEXT NOT NULL REFERENCES runtime_file(key) ON DELETE CASCADE,
            mime_type       TEXT NOT NULL,
            filename        TEXT NOT NULL,
            fetch_url       TEXT NOT NULL,
            expires_at_unix BIGINT NOT NULL
        );
