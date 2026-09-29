CREATE TABLE IF NOT EXISTS trigger_setup (
            project_id UUID NOT NULL,
            execution_id TEXT PRIMARY KEY
        );
CREATE INDEX IF NOT EXISTS idx_trigger_setup_project ON trigger_setup(project_id);
CREATE TABLE IF NOT EXISTS trigger_bake (
            project_id UUID NOT NULL,
            member_id TEXT,
            program_hash TEXT NOT NULL,
            bake_json TEXT NOT NULL
        );
CREATE UNIQUE INDEX IF NOT EXISTS idx_trigger_bake_key
             ON trigger_bake(project_id, member_id, program_hash) NULLS NOT DISTINCT;
CREATE TABLE IF NOT EXISTS exec_event (
            id BIGSERIAL PRIMARY KEY,
            execution_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at BIGINT NOT NULL,
            instance TEXT,
            dedup_key TEXT,
            -- The transaction that wrote the row, the order the
            -- dispatcher's cursor reads in (`crate::settled`).
            writer_xid XID8 NOT NULL DEFAULT pg_current_xact_id()
        );
CREATE INDEX IF NOT EXISTS idx_exec_event_execution_id ON exec_event(execution_id, id);
CREATE INDEX IF NOT EXISTS idx_exec_event_settled ON exec_event(writer_xid, id);
CREATE INDEX IF NOT EXISTS idx_exec_event_kind ON exec_event(kind, id DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_exec_event_dedup
           ON exec_event(dedup_key) WHERE dedup_key IS NOT NULL;
CREATE OR REPLACE FUNCTION exec_event_notify() RETURNS trigger AS $$
            BEGIN
                PERFORM pg_notify('weft_exec_event', NEW.execution_id);
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS exec_event_notify_on_insert ON exec_event;
CREATE TRIGGER exec_event_notify_on_insert
            AFTER INSERT ON exec_event
            FOR EACH ROW
            EXECUTE FUNCTION exec_event_notify();
CREATE TABLE IF NOT EXISTS signal_token (
            id UUID PRIMARY KEY,
            -- sha256 hex of the full token value. The raw value is NEVER
            -- stored (show-once): lookups hash the presented credential.
            token_hash TEXT NOT NULL UNIQUE,
            -- Display prefix ("wft-<word>-…") so lists can tell tokens apart.
            recognizer TEXT NOT NULL,
            tenant_id TEXT NOT NULL,
            name TEXT,
            allowed_projects UUID[] NOT NULL DEFAULT '{}',
            allowed_tags TEXT[] NOT NULL DEFAULT '{}',
            -- The display dimension, which does NOT follow the
            -- empty-means-wildcard rule above: a display can be a
            -- credential, so a token says nothing about displays and
            -- reads none. `all_displays` is the wildcard, set by
            -- `weft token mint --displays`.
            allowed_displays TEXT[] NOT NULL DEFAULT '{}',
            all_displays BOOLEAN NOT NULL DEFAULT FALSE,
            created_at BIGINT NOT NULL,
            -- A member token: the member it acts as, in its one project
            -- (`allowed_projects` holds exactly that one). NULL for a
            -- token that acts as nobody in particular.
            member_id TEXT,
            -- When it stops working (unix seconds); NULL never. A
            -- member token always has one: it lives in a browser.
            expires_at BIGINT,
            -- What the token may do: `caller` (the scoped outside
            -- credential above, a member's included) or `operator` (the
            -- tenant's admin key, which carries no scope and acts as no
            -- member). Checked at every door.
            -- SYNC: kind values <-> journal::TokenKind::as_str
            kind TEXT NOT NULL DEFAULT 'caller' CHECK (kind IN ('caller', 'operator')),
            CONSTRAINT signal_token_member_has_one_project
                CHECK (member_id IS NULL OR cardinality(allowed_projects) = 1),
            CONSTRAINT signal_token_member_expires
                CHECK (member_id IS NULL OR expires_at IS NOT NULL),
            CONSTRAINT signal_token_operator_is_nobody
                CHECK (kind = 'caller' OR member_id IS NULL)
        );
CREATE INDEX IF NOT EXISTS idx_signal_token_tenant ON signal_token(tenant_id);
CREATE TABLE IF NOT EXISTS signal (
            token TEXT PRIMARY KEY,
            program_json JSONB,
            setup_execution_id UUID,
            source_version TEXT,
            tenant_id TEXT NOT NULL,
            project_id UUID NOT NULL,
            execution_id TEXT,
            node_id TEXT NOT NULL,
            is_resume BOOLEAN NOT NULL,
            spec_json TEXT NOT NULL,
            -- The connection this signal acts as (`spec.access.id`),
            -- denormalized at register time. NULL for kinds without
            -- one. Inbound provider pushes route account-to-signal
            -- on this column, so the match is one indexed filter
            -- instead of a spec-parsing scan.
            access_id TEXT,
            created_at BIGINT NOT NULL,
            -- Opaque per-kind state persisted at register time and
            -- read back at rehydrate time. Empty for most kinds.
            -- Timer uses it to persist absolute next_fire_at_unix_ms
            -- for After-style schedules so a listener restart
            -- doesn't reset the clock. Future stateful kinds (e.g.
            -- SSE last-event-id, socket reconnect token) use the
            -- same column.
            kind_state JSONB NOT NULL DEFAULT '{}'::jsonb,
            -- The kind_state write fence: a strictly-increasing
            -- per-signal version stamped by the holder's durable
            -- cursor writes. An update whose seq is not above the
            -- stored one is dropped (the newer state stands), and a
            -- register that carries prior state forward loses to any
            -- newer in-flight write the same way. A REAL column, so
            -- the version can never be lost by handling the state
            -- blob (the blob stays purely the kind's own state).
            kind_state_seq BIGINT NOT NULL DEFAULT 0,
            -- FIFO queue of fires that landed while the project was
            -- not Active (Activating / park / hibernate-in-grace /
            -- Deactivating). Each element is { "payload": <json>,
            -- "received_at_unix": <int> }. Entry signals append on
            -- every fire; resume signals append iff the queue is
            -- empty (first submission wins; subsequent ones for the
            -- same suspension are dropped). Drained on reactivate by
            -- replaying every element through dispatch_listener_outcome,
            -- then clearing the array.
            parked_fires JSONB NOT NULL DEFAULT '[]'::jsonb,
            -- Claim guard for the drain loop: set when a dispatcher
            -- instance claims this row's queue for replay, cleared on
            -- either success (alongside parked_fires=[]) or failure
            -- (release). A sweeper releases stale claims older than
            -- the claim-stale threshold so a dispatcher crash
            -- mid-step doesn't leave the row uncloseable.
            drain_claimed_at_unix BIGINT,
            -- Per-claim owner nonce. Set when a drain claims the row;
            -- every pop + the release is fenced on it. If a stale-claim
            -- sweep hands the row to a sibling instance mid-drain, the
            -- original drainer's fenced pop matches 0 rows and it aborts
            -- instead of popping an element the new owner already
            -- dispatched (which would silently drop an undispatched fire).
            drain_claimed_by TEXT,
            consumer_kind TEXT,
            tags TEXT[] NOT NULL DEFAULT '{}',
            -- The trigger's delivered port values at registration time
            -- (entry signals only). Replayed onto the trigger's ports at
            -- every fire: a trigger's inputs are whatever they were at
            -- trigger setup.
            port_snapshot JSONB,
            consumer_payload TEXT,
            surface_kind TEXT NOT NULL DEFAULT 'task_callback',
            -- A public entry's route pattern under its tenant
            -- (`/<tenant>/chat/{room}`); the dispatcher matches a call
            -- against every pattern of the tenant in Rust, so this is
            -- never an equality lookup key.
            mount_path TEXT,
            -- The HTTP methods the public entry serves, uppercase; empty
            -- = any. Two entries of one tenant may not overlap in both
            -- pattern and method (checked at register time).
            mount_methods TEXT[] NOT NULL DEFAULT '{}',
            -- Whose signal: NULL for the program's shared ones, else the
            -- member whose copy of a per-member trigger this is, or whose
            -- run is waiting on it.
            member_id TEXT,
            -- The trigger whose activation gates this signal (with
            -- `member_id`, the `trigger_activation` row the fire gate
            -- reads): an entry signal's own trigger, or the trigger that
            -- fired the run a wait belongs to. NULL for a wait of a run
            -- started by hand, which no activation governs.
            activation_trigger TEXT,
            auth_kind TEXT NOT NULL DEFAULT 'none',
            auth_config JSONB
        );
CREATE INDEX IF NOT EXISTS idx_signal_tenant ON signal(tenant_id);
CREATE INDEX IF NOT EXISTS idx_signal_project ON signal(project_id);
CREATE INDEX IF NOT EXISTS idx_signal_execution_id ON signal(execution_id);
CREATE INDEX IF NOT EXISTS idx_signal_consumer_kind ON signal(consumer_kind);
CREATE INDEX IF NOT EXISTS idx_signal_access_id ON signal(access_id)
           WHERE access_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_signal_mount_path
             ON signal(mount_path, mount_methods) WHERE mount_path IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_signal_entry_node
             ON signal(project_id, node_id, member_id) NULLS NOT DISTINCT WHERE is_resume = FALSE;
CREATE INDEX IF NOT EXISTS idx_signal_activation
             ON signal(project_id, activation_trigger, member_id) WHERE activation_trigger IS NOT NULL;
CREATE OR REPLACE FUNCTION signal_parked_fire_notify() RETURNS trigger AS $$
            BEGIN
                PERFORM pg_notify('weft_parked_fire', NEW.project_id::text);
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS signal_parked_fire_notify_on_grow ON signal;
CREATE TRIGGER signal_parked_fire_notify_on_grow
            AFTER UPDATE OF parked_fires ON signal
            FOR EACH ROW
            WHEN (jsonb_array_length(NEW.parked_fires) > jsonb_array_length(OLD.parked_fires))
            EXECUTE FUNCTION signal_parked_fire_notify();
CREATE TABLE IF NOT EXISTS execution (
            execution_id TEXT PRIMARY KEY,
            project_id UUID NOT NULL,
            tenant_id TEXT NOT NULL,
            started_at_unix BIGINT NOT NULL,
            phase TEXT NOT NULL,
            -- Worker instance that owns this execution's writes. NULL until the
            -- first worker claims an execution-bearing task (the broker
            -- stamps it in task_claim_one); thereafter it is the instance of
            -- the LATEST claimer. The broker rejects any journal_record
            -- whose caller.instance doesn't match, so a compromised
            -- worker can only journal under its own bound instance, not
            -- cross-write sibling executions in the same tenant.
            --
            -- "Latest claimer wins" is how a resume hands ownership to a
            -- new instance when the original is gone: the resume task is
            -- pinned to the original owner if it is still alive (so only
            -- it reclaims and ownership stays stable), and spawns + pins
            -- to a fresh instance only when the owner is dead (so the handoff
            -- is the ONLY time ownership moves). Without that pinning a
            -- fresh worker could steal a live owner's execution mid-flight
            -- now that a project can run more than one worker; see
            -- `task_kinds::execute::enqueue_resume`.
            -- NULL also covers dispatcher-orchestrated writes (no instance).
            owner_instance TEXT,
            -- What this execution IS (`weft_core::exec::RunKind`):
            -- 'execution' (a project run; the project-lifecycle sweeps,
            -- cancel, wipe, drain counting and the listings operate on
            -- these), 'node_test' (a node self-test's identity: real
            -- cost attribution and broker scoping, but its lifecycle is
            -- owned by its task, so the project sweeps must never cancel
            -- it or wait on it), or 'unrecorded' (a run whose journal
            -- lives in its worker's memory: never listed, dropped when
            -- it ends unless its costs keep it, and turned into an
            -- 'execution' with its whole record written if it fails).
            kind TEXT NOT NULL DEFAULT 'execution',
            -- Who the run is for: the member its `ExecutionStarted`
            -- names, copied here in the same transaction so every
            -- member filter (clean, costs, a member token's reads) is
            -- a column read. NULL for a run for nobody in particular.
            member_id TEXT,
            -- The trigger whose firing started the run (its
            -- `ExecutionStarted.fired_trigger`), NULL for a run started
            -- by hand and every setup run. With `member_id` it names the
            -- activation the run belongs to: a wait the run registers is
            -- gated by that activation, and taking it down reaches it.
            fired_by TEXT,
            -- When an unrecorded run ended, for the one whose row its
            -- costs keep after it is over (`weft_journal::unrecorded`):
            -- NULL while it runs, so the live rule stops counting it the
            -- moment it ends, before its execute task is closed. NULL
            -- for every other kind, whose ending is its journal row.
            ended_at_unix BIGINT
        );
CREATE INDEX IF NOT EXISTS idx_execution_tenant ON execution(tenant_id);
CREATE INDEX IF NOT EXISTS idx_execution_project ON execution(project_id);
CREATE INDEX IF NOT EXISTS idx_execution_member
             ON execution(project_id, member_id) WHERE member_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_execution_listing
             ON execution(tenant_id, started_at_unix DESC, execution_id DESC)
             WHERE kind = 'execution';
CREATE TABLE IF NOT EXISTS execution_tag (
            seq BIGSERIAL PRIMARY KEY,
            execution_id TEXT NOT NULL,
            tag TEXT NOT NULL,
            tagged_at_unix BIGINT NOT NULL,
            UNIQUE (execution_id, tag)
        );
CREATE INDEX IF NOT EXISTS idx_execution_tag_tag ON execution_tag(tag);
