CREATE TABLE IF NOT EXISTS exec_event (
            id BIGSERIAL PRIMARY KEY,
            color TEXT NOT NULL,
            kind TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at BIGINT NOT NULL,
            pod_name TEXT,
            dedup_key TEXT
        );
CREATE INDEX IF NOT EXISTS idx_exec_event_color ON exec_event(color, id);
CREATE INDEX IF NOT EXISTS idx_exec_event_kind ON exec_event(kind, id DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_exec_event_dedup
           ON exec_event(dedup_key) WHERE dedup_key IS NOT NULL;
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
            created_at BIGINT NOT NULL
        );
CREATE INDEX IF NOT EXISTS idx_signal_token_tenant ON signal_token(tenant_id);
CREATE TABLE IF NOT EXISTS signal (
            token TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            project_id TEXT NOT NULL,
            color TEXT,
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
            -- pod claims this row's queue for replay, cleared on
            -- either success (alongside parked_fires=[]) or failure
            -- (release). A sweeper releases stale claims older than
            -- the claim-stale threshold so a dispatcher crash
            -- mid-step doesn't leave the row uncloseable.
            drain_claimed_at_unix BIGINT,
            -- Per-claim owner nonce. Set when a drain claims the row;
            -- every pop + the release is fenced on it. If a stale-claim
            -- sweep hands the row to a sibling pod mid-drain, the
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
            mount_path TEXT,
            auth_kind TEXT NOT NULL DEFAULT 'none',
            auth_config JSONB,
            -- Placement: which pooled listener pod currently holds this
            -- signal's live in-RAM registry entry (its Timer/SSE loop).
            -- NULL when no listener holds it yet (freshly registered
            -- before placement, or the holding pod died and it awaits
            -- re-placement). The fire path resolves token -> this pod's
            -- admin URL; boot/rehydrate lists `WHERE listener_pod = me`
            -- to rebuild a restarted pod's registry. A pooled listener
            -- holds many tenants' signals, so placement is per-signal,
            -- not per-tenant.
            listener_pod TEXT,
            -- Monotonic placement generation, bumped on EVERY (re)placement
            -- (set_placement). The holding pod is told its generation at
            -- register time and stamps it on every held-event fire it
            -- enqueues. A move registers the signal on the new pod under
            -- gen+1 BEFORE unregistering the old pod, so during the brief
            -- both-armed overlap the old pod still fires under the OLD gen.
            -- The broker rejects any FireSignal whose generation is below
            -- the row's current generation: the stale (old-pod) fire from
            -- the overlap is dropped, the new pod's fire passes. This is
            -- the fence that prevents a self-firing kind (Timer/SSE) from
            -- double-firing across a scale-down move.
            placement_generation BIGINT NOT NULL DEFAULT 0
        );
CREATE INDEX IF NOT EXISTS idx_signal_tenant ON signal(tenant_id);
CREATE INDEX IF NOT EXISTS idx_signal_listener_pod
             ON signal(listener_pod) WHERE listener_pod IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_signal_project ON signal(project_id);
CREATE INDEX IF NOT EXISTS idx_signal_color ON signal(color);
CREATE INDEX IF NOT EXISTS idx_signal_consumer_kind ON signal(consumer_kind);
CREATE INDEX IF NOT EXISTS idx_signal_access_id ON signal(access_id)
           WHERE access_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_signal_mount_path
             ON signal(mount_path) WHERE mount_path IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_signal_entry_node
             ON signal(project_id, node_id) WHERE is_resume = FALSE;
CREATE TABLE IF NOT EXISTS execution_color (
            color TEXT PRIMARY KEY,
            project_id TEXT NOT NULL,
            tenant_id TEXT NOT NULL,
            started_at_unix BIGINT NOT NULL,
            phase TEXT NOT NULL,
            -- Worker pod that owns this color's writes. NULL until the
            -- first worker claims a color-bearing task (the broker
            -- stamps it in task_claim_one); thereafter it is the pod of
            -- the LATEST claimer. The broker rejects any journal_record
            -- whose caller.pod_name doesn't match, so a compromised
            -- worker can only journal under its own bound pod, not
            -- cross-write sibling executions in the same tenant.
            --
            -- "Latest claimer wins" is how a resume hands ownership to a
            -- new pod when the original is gone: the resume task is
            -- pinned to the original owner if it is still alive (so only
            -- it reclaims and ownership stays stable), and spawns + pins
            -- to a fresh pod only when the owner is dead (so the handoff
            -- is the ONLY time ownership moves). Without that pinning a
            -- fresh worker could steal a live owner's color mid-flight
            -- now that a project can run more than one worker; see
            -- `task_kinds::execute::enqueue_resume`.
            -- NULL also covers dispatcher-orchestrated writes (no pod).
            owner_pod_name TEXT,
            -- What this color IS: 'execution' (a project run; the
            -- project-lifecycle sweeps, cancel, wipe, and drain
            -- counting operate on these) or 'node_test' (a node
            -- self-test's identity: real cost attribution and broker
            -- scoping, but its lifecycle is owned by its task, so the
            -- project sweeps must never cancel it or wait on it).
            kind TEXT NOT NULL DEFAULT 'execution'
        );
CREATE INDEX IF NOT EXISTS idx_execution_color_tenant ON execution_color(tenant_id);
CREATE INDEX IF NOT EXISTS idx_execution_color_project ON execution_color(project_id);
