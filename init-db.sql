-- Weft Local Database Initialization
-- This script sets up the PostgreSQL database for standalone local development.
-- Owned by weft-api (Rust backend). The dashboard's `db.ts::initDb()`
-- creates the same `projects` / `executions` / `project_versions` /
-- `test_configs` tables on startup with equivalent schemas, so the
-- two are interchangeable sources-of-truth. Whichever runs first wins;
-- the other finds the table already present and skips the CREATE.
-- We duplicate here so `./dev.sh server` works on a fresh DB without
-- requiring the dashboard to have initialized first (otherwise the
-- ALTER TABLE statements below that extend `projects` would hit a
-- missing table and silently skip, leaving the schema incomplete).

-- Projects table (builder projects + deployment clones)
CREATE TABLE IF NOT EXISTS projects (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id TEXT NOT NULL DEFAULT 'local',
    name TEXT NOT NULL,
    description TEXT,
    weft_code TEXT,
    loom_code TEXT,
    layout_code TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_projects_user_id ON projects(user_id);

-- Executions table (run history, referenced by triggers and usage_events)
CREATE TABLE IF NOT EXISTS executions (
    id TEXT PRIMARY KEY,
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL,
    trigger_id TEXT,
    node_type TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    node_statuses JSONB NOT NULL DEFAULT '{}',
    node_outputs JSONB NOT NULL DEFAULT '{}',
    node_executions JSONB NOT NULL DEFAULT '{}',
    error TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_executions_project_id ON executions(project_id);
CREATE INDEX IF NOT EXISTS idx_executions_user_id ON executions(user_id);

-- Triggers table (for trigger persistence)
CREATE TABLE IF NOT EXISTS triggers (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    trigger_node_id TEXT NOT NULL,
    trigger_category TEXT NOT NULL,
    node_type TEXT NOT NULL,
    user_id TEXT NOT NULL,
    config JSONB NOT NULL DEFAULT '{}',
    credentials JSONB,
    project_definition JSONB,
    status TEXT NOT NULL DEFAULT 'pending',
    instance_id TEXT,
    last_heartbeat TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_triggers_project_id ON triggers(project_id);
CREATE INDEX IF NOT EXISTS idx_triggers_user_id ON triggers(user_id);
CREATE INDEX IF NOT EXISTS idx_triggers_status ON triggers(status);
CREATE INDEX IF NOT EXISTS idx_triggers_instance_id ON triggers(instance_id);

-- Extension tokens table
CREATE TABLE IF NOT EXISTS extension_tokens (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    name TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    last_used_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_extension_tokens_user_id ON extension_tokens(user_id);

-- Pricing tiers table. `execution_base_cost` is the flat fee charged
-- per project execution, and `margin` is the multiplier applied to
-- raw provider costs before billing. Both columns are read by
-- weft-api's usage_store on every execution and every LLM event;
-- missing columns cause `get_execution_base_cost` to panic via
-- `.expect(...)` on the query result. Seeded with a `default` row
-- so OSS local has a working free tier out of the box.
CREATE TABLE IF NOT EXISTS pricing_tiers (
    tier TEXT PRIMARY KEY,
    margin DOUBLE PRECISION NOT NULL DEFAULT 1.2,
    execution_base_cost DOUBLE PRECISION NOT NULL DEFAULT 0.01,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- Back-compat migration: older installs may have the table without
-- execution_base_cost. ALTER is idempotent and safe to re-run.
ALTER TABLE pricing_tiers ADD COLUMN IF NOT EXISTS execution_base_cost DOUBLE PRECISION NOT NULL DEFAULT 0.01;
INSERT INTO pricing_tiers (tier, margin, execution_base_cost, description)
    VALUES ('default', 1.2, 0.01, 'Default pricing tier')
    ON CONFLICT (tier) DO NOTHING;

-- Subscriptions table. Empty in OSS local — there are no paying
-- subscribers in single-user dev — but `usage_store::get_user_margin`
-- does a `SELECT ... FROM subscriptions` on every billable event and
-- panics on a missing table. Creating an empty table lets the query
-- return `None` and fall through to the free-tier default margin.
-- Cloud deployments populate this from Stripe webhooks.
CREATE TABLE IF NOT EXISTS subscriptions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id TEXT NOT NULL UNIQUE,
    tier TEXT NOT NULL REFERENCES pricing_tiers(tier),
    status TEXT NOT NULL DEFAULT 'active',
    stripe_customer_id TEXT,
    stripe_subscription_id TEXT UNIQUE,
    current_period_start TIMESTAMPTZ,
    current_period_end TIMESTAMPTZ,
    trial_end TIMESTAMPTZ,
    custom_margin DOUBLE PRECISION,
    custom_credits_per_cycle DOUBLE PRECISION,
    custom_execution_base_cost DOUBLE PRECISION,
    custom_monthly_price DOUBLE PRECISION,
    features JSONB NOT NULL DEFAULT '{}',
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_subscriptions_user_id ON subscriptions(user_id);
CREATE INDEX IF NOT EXISTS idx_subscriptions_status ON subscriptions(status);

-- Usage events table (append-only log of all billable events)
CREATE TABLE IF NOT EXISTS usage_events (
    id BIGSERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    event_type TEXT NOT NULL,  -- 'service', 'tangle', 'execution', 'infra_daily'
    subtype TEXT,              -- e.g. 'llm', 'speech_to_text', 'transcription'
    project_id TEXT,
    execution_id TEXT,
    node_id TEXT,
    model TEXT,                -- model used (e.g. 'scribe_v2', 'anthropic/claude-3.5-sonnet')
    prompt_tokens INTEGER,
    completion_tokens INTEGER,
    cost_usd DOUBLE PRECISION, -- actual cost from provider
    billed_usd DOUBLE PRECISION NOT NULL DEFAULT 0, -- cost charged to user (cost_usd * margin, 0 for BYOK)
    is_byok BOOLEAN NOT NULL DEFAULT false,
    metadata JSONB,            -- extra data (generation_id, provider, etc.)
    event_date DATE NOT NULL DEFAULT CURRENT_DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_usage_events_user_id ON usage_events(user_id);
CREATE INDEX IF NOT EXISTS idx_usage_events_event_date ON usage_events(event_date);
CREATE INDEX IF NOT EXISTS idx_usage_events_user_date ON usage_events(user_id, event_date);
CREATE INDEX IF NOT EXISTS idx_usage_events_event_type ON usage_events(event_type);
CREATE INDEX IF NOT EXISTS idx_usage_events_execution_id ON usage_events(execution_id) WHERE execution_id IS NOT NULL;
-- Idempotency for the per-execution flat fee: at most one 'execution' row per execution_id.
-- The orchestrator retries start-execution on transport failure; this guards against double-charging.
CREATE UNIQUE INDEX IF NOT EXISTS uniq_usage_events_execution_once ON usage_events(execution_id) WHERE event_type = 'execution';

-- Usage daily table (aggregated per user per day, materialized by cron)
CREATE TABLE IF NOT EXISTS usage_daily (
    id BIGSERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    date DATE NOT NULL,
    service_cost_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    service_billed_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    service_requests INTEGER NOT NULL DEFAULT 0,
    tangle_cost_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    tangle_billed_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    tangle_requests INTEGER NOT NULL DEFAULT 0,
    execution_count INTEGER NOT NULL DEFAULT 0,
    infra_cost_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    infra_billed_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    execution_billed_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    last_aggregated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, date)
);
CREATE INDEX IF NOT EXISTS idx_usage_daily_user_id ON usage_daily(user_id);
CREATE INDEX IF NOT EXISTS idx_usage_daily_date ON usage_daily(date);

-- User credits table (current balance per user)
CREATE TABLE IF NOT EXISTS user_credits (
    user_id TEXT PRIMARY KEY,
    balance_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    tier TEXT NOT NULL DEFAULT 'default' REFERENCES pricing_tiers(tier),
    has_paid BOOLEAN NOT NULL DEFAULT false,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Credit transactions (append-only audit log)
CREATE TABLE IF NOT EXISTS credit_transactions (
    id BIGSERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    amount_usd DOUBLE PRECISION NOT NULL,  -- positive = add, negative = deduct
    reason TEXT NOT NULL,                   -- 'admin_grant', 'ai_completion', 'stripe_topup', etc.
    reference_id TEXT,                      -- execution_id, stripe_payment_id, etc.
    balance_after DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_credit_transactions_user_id ON credit_transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_credit_transactions_created_at ON credit_transactions(created_at);

-- Infrastructure pending actions (transitional flag for stop/terminate)
-- Used by weft-api to immediately report transitional status
-- before the Restate exclusive handler processes the request.
CREATE TABLE IF NOT EXISTS infra_pending_action (
    project_id TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Trigger pending actions (transitional flag for activating/deactivating)
-- Same pattern as infra_pending_action: immediate transitional status
-- before async work completes.
CREATE TABLE IF NOT EXISTS trigger_pending_action (
    trigger_id TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Incremental migrations for trigger lifecycle (idempotent)
ALTER TABLE triggers ADD COLUMN IF NOT EXISTS setup_run_counter INTEGER NOT NULL DEFAULT 0;
ALTER TABLE triggers ADD COLUMN IF NOT EXISTS setup_execution_id TEXT;
ALTER TABLE triggers ADD COLUMN IF NOT EXISTS project_hash TEXT;

-- ═══ Published Projects (deploy-as-a-page, base table) ═══
--
-- A snapshot of a Weft project exposed at a public URL /p/<slug>. Same table
-- is used by the OSS weft-api (local) and the closed-source cloud-api (cloud),
-- so publish works both offline and hosted. Cloud-specific features (credit
-- gating, rate limits, visitor sessions) live in cloud-api's init-db extension.
-- Deployments are keyed on (username, slug). Username is denormalized from
-- the auth-side user record at publish time.
CREATE TABLE IF NOT EXISTS published_projects (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    slug TEXT NOT NULL,
    username TEXT NOT NULL,
    user_id TEXT NOT NULL,                        -- deployer who owns the snapshot
    project_id UUID,                              -- references projects(id), nullable after unlink
    project_name TEXT NOT NULL,
    description TEXT,
    -- Legacy ghost columns. The publish flow now reads weft/loom/layout
    -- from the deployment's `projects` row via project_id and never
    -- writes these. Kept as nullable for backwards compat with older
    -- installs; removed in a later migration once all live deployments
    -- have been re-published.
    weft_code TEXT,
    loom_code TEXT,
    layout_code TEXT,
    is_live BOOLEAN NOT NULL DEFAULT true,
    view_count BIGINT NOT NULL DEFAULT 0,
    run_count BIGINT NOT NULL DEFAULT 0,
    rate_limit_per_minute INTEGER,                -- deployer-configured per-slug rate limit (null = default)
    published_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (username, slug)
);
-- Relax the legacy NOT NULL constraints on older installs.
ALTER TABLE published_projects ALTER COLUMN weft_code DROP NOT NULL;
ALTER TABLE published_projects ALTER COLUMN loom_code DROP NOT NULL;
-- Back-compat migration for older installs that predate rate_limit_per_minute.
ALTER TABLE published_projects ADD COLUMN IF NOT EXISTS rate_limit_per_minute INTEGER;
CREATE INDEX IF NOT EXISTS idx_published_projects_user_id ON published_projects(user_id);
CREATE INDEX IF NOT EXISTS idx_published_projects_username ON published_projects(username);
CREATE INDEX IF NOT EXISTS idx_published_projects_project_id ON published_projects(project_id);

-- One-shot cleanup: before the username-drift fix shipped (H-1 ext),
-- re-publishing under a renamed username could leave multiple mapping
-- rows for the same (user_id, slug) pointing at different deployment
-- project rows. The drift sweep in publish_project now prevents this
-- going forward, but any pre-fix data is still orphaned: mapping row
-- under the old username plus a deployment project it was pointing
-- at that no handler can reach.
--
-- This block finds every (user_id, slug) group with more than one
-- mapping row, keeps the most recently updated one, and hard-deletes
-- the deployment project + triggers for each older row so nothing is
-- left dangling. Idempotent: after the first run every group has at
-- most one row so the subquery is empty.
DO $$
DECLARE
    stale RECORD;
BEGIN
    FOR stale IN
        SELECT pp.project_id
        FROM published_projects pp
        JOIN (
            SELECT user_id, slug, MAX(updated_at) AS latest
            FROM published_projects
            GROUP BY user_id, slug
            HAVING COUNT(*) > 1
        ) winners
          ON winners.user_id = pp.user_id
         AND winners.slug = pp.slug
         AND winners.latest <> pp.updated_at
    LOOP
        IF stale.project_id IS NOT NULL THEN
            DELETE FROM triggers WHERE project_id = stale.project_id::text;
            DELETE FROM projects WHERE id = stale.project_id AND is_deployment = true;
        END IF;
    END LOOP;
    DELETE FROM published_projects pp
    USING (
        SELECT user_id, slug, MAX(updated_at) AS latest
        FROM published_projects
        GROUP BY user_id, slug
        HAVING COUNT(*) > 1
    ) winners
    WHERE pp.user_id = winners.user_id
      AND pp.slug = winners.slug
      AND pp.updated_at <> winners.latest;
END $$;

-- ═══ Deployment-as-project model ═══
--
-- A deployment is just another `projects` row. Publishing clones the
-- builder project into a new row with is_deployment=true, copies the
-- builder's triggers into `triggers` keyed on the new project id, and
-- records a thin `published_projects` mapping so the slug points at the
-- clone. The runtime (trigger dispatcher, executor, infra lifecycle) is
-- unified: everything keys on `project_id` and never needs to know if a
-- row is a builder or a deployment.
ALTER TABLE projects ADD COLUMN IF NOT EXISTS is_deployment BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE projects ADD COLUMN IF NOT EXISTS origin_project_id UUID REFERENCES projects(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_projects_is_deployment ON projects(is_deployment) WHERE is_deployment = true;
CREATE INDEX IF NOT EXISTS idx_projects_origin_project_id ON projects(origin_project_id) WHERE origin_project_id IS NOT NULL;

-- Allowlist of which node configs / output ports are visitor-accessible
-- on a deployment. Shape:
--   { "inputs":  { "<nodeId>": ["fieldKey1", ...], ... },
--     "outputs": { "<nodeId>": ["portName1", ...], ... } }
-- Populated at publish time from the loom's input/output directives. The
-- visitor-run path merges ONLY the named `inputs` into node.config, and
-- the trigger-broadcast path returns ONLY the named `outputs`. Null on
-- non-deployment projects.
ALTER TABLE projects ADD COLUMN IF NOT EXISTS visitor_access JSONB;
