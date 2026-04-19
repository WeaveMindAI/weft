import pg from 'pg';
import { env } from '$env/dynamic/private';
import { building } from '$app/environment';
import { getApiUrl } from './api-url';

let _pool: pg.Pool | null = null;

function getPool(): pg.Pool {
	if (!_pool) {
		_pool = new pg.Pool({
			connectionString: env.DATABASE_URL || 'postgresql://postgres:postgres@localhost:5433/weft_local'
		});
	}
	return _pool;
}

const pool = building ? null! : getPool();

export interface DbProject {
	id: string;
	userId: string;
	name: string;
	description: string | null;
	weftCode: string | null;
	loomCode: string | null;
	layoutCode: string | null;
	createdAt: Date;
	updatedAt: Date;
	lastOpenedAt: Date;
	/** True when this row is a publication snapshot, not a buildable project.
	 *  Deployment projects are hidden from the builder list and the builder
	 *  API rejects structural mutations against them. The runner admin path
	 *  can still edit field config values which flow through update_project
	 *  to rewrite weft_code in place. */
	isDeployment?: boolean;
	/** For deployment rows: the builder project this one was cloned from.
	 *  Null for builder projects. */
	originProjectId?: string | null;
}

export async function initDb(): Promise<void> {
	try {
		await pool.query(`
			CREATE TABLE IF NOT EXISTS projects (
				id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
				user_id TEXT NOT NULL DEFAULT 'local',
				name TEXT NOT NULL,
				description TEXT,
				weft_code TEXT,
				loom_code TEXT,
				created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
				updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
				last_opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
			)
		`);
	} catch (e: unknown) {
		const err = e as { code?: string };
		if (err.code !== '23505' && err.code !== '42P07') throw e;
	}
	try {
		await pool.query(`
			CREATE INDEX IF NOT EXISTS idx_projects_user_id ON projects(user_id)
		`);
	} catch (e: unknown) {
		const err = e as { code?: string };
		if (err.code !== '23505' && err.code !== '42P07') throw e;
	}
	// Migration: add loom_code column if upgrading from old schema
	try {
		await pool.query(`ALTER TABLE projects ADD COLUMN IF NOT EXISTS loom_code TEXT`);
	} catch {
		// Ignore
	}
	// Migration: add weft_code column if upgrading from old schema
	try {
		await pool.query(`ALTER TABLE projects ADD COLUMN IF NOT EXISTS weft_code TEXT`);
	} catch {
		// Ignore
	}
	// Migration: add last_opened_at if upgrading from old schema
	try {
		await pool.query(`ALTER TABLE projects ADD COLUMN IF NOT EXISTS last_opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`);
	} catch {
		// Ignore
	}
	// Migration: add layout_code column
	try {
		await pool.query(`ALTER TABLE projects ADD COLUMN IF NOT EXISTS layout_code TEXT`);
	} catch {
		// Ignore
	}
	// Migration: deployment-as-project columns. Added when the publish
	// flow started cloning builder projects into deployment rows.
	// Both columns are referenced by PROJECT_SELECT and every query
	// filter in this file, so the dashboard fails closed if they're
	// missing. `initDb()` is the dashboard's own source of schema
	// truth when running OSS standalone (i.e. without the Rust
	// backend's `init-db.sh` running first), so we replicate the same
	// ALTERs here for parity with weft/init-db.sql.
	try {
		await pool.query(`ALTER TABLE projects ADD COLUMN IF NOT EXISTS is_deployment BOOLEAN NOT NULL DEFAULT false`);
	} catch {
		// Ignore
	}
	try {
		await pool.query(`ALTER TABLE projects ADD COLUMN IF NOT EXISTS origin_project_id UUID REFERENCES projects(id) ON DELETE SET NULL`);
	} catch {
		// Ignore
	}
	try {
		await pool.query(`CREATE INDEX IF NOT EXISTS idx_projects_is_deployment ON projects(is_deployment) WHERE is_deployment = true`);
	} catch {
		// Ignore
	}
	try {
		await pool.query(`CREATE INDEX IF NOT EXISTS idx_projects_origin_project_id ON projects(origin_project_id) WHERE origin_project_id IS NOT NULL`);
	} catch {
		// Ignore
	}
	await initExecutionsTable();
	await initVersionsTable();
	await initTestConfigsTable();
	// Migration: add layout_code to versions table
	try {
		await pool.query(`ALTER TABLE project_versions ADD COLUMN IF NOT EXISTS layout_code TEXT`);
	} catch {
		// Ignore
	}
}

const PROJECT_SELECT = `id, user_id AS "userId", name, description, weft_code AS "weftCode", loom_code AS "loomCode", layout_code AS "layoutCode", created_at AS "createdAt", updated_at AS "updatedAt", last_opened_at AS "lastOpenedAt", is_deployment AS "isDeployment", origin_project_id AS "originProjectId"`;

export async function listProjects(
	userId: string = 'local',
	limit: number = 100,
	offset: number = 0,
	options: { includeDeployments?: boolean } = {},
): Promise<DbProject[]> {
	// Deployment projects are hidden from the builder list by default.
	// They're only reachable through the Manage Deployments modal where we
	// fetch them explicitly by id.
	const deploymentFilter = options.includeDeployments
		? ''
		: ' AND is_deployment = false';
	const result = await pool.query<DbProject>(
		`SELECT ${PROJECT_SELECT} FROM projects
		 WHERE user_id = $1${deploymentFilter}
		 ORDER BY last_opened_at DESC LIMIT $2 OFFSET $3`,
		[userId, limit, offset]
	);
	return result.rows;
}

export async function getProject(id: string, userId: string): Promise<DbProject | null> {
	const result = await pool.query<DbProject>(
		`SELECT ${PROJECT_SELECT} FROM projects WHERE id = $1 AND user_id = $2`,
		[id, userId]
	);
	return result.rows[0] || null;
}

export async function createProject(
	userId: string,
	name: string,
	description: string | null,
	weftCode?: string,
	loomCode?: string,
	layoutCode?: string,
): Promise<DbProject> {
	const cols = ['user_id', 'name', 'description'];
	const vals: unknown[] = [userId, name, description];
	if (weftCode !== undefined) {
		cols.push('weft_code');
		vals.push(weftCode);
	}
	if (loomCode !== undefined) {
		cols.push('loom_code');
		vals.push(loomCode);
	}
	if (layoutCode !== undefined) {
		cols.push('layout_code');
		vals.push(layoutCode);
	}
	const placeholders = vals.map((_, i) => `$${i + 1}`).join(', ');
	const result = await pool.query<DbProject>(
		`INSERT INTO projects (${cols.join(', ')}) VALUES (${placeholders}) RETURNING ${PROJECT_SELECT}`,
		vals
	);
	return result.rows[0];
}

export async function updateProject(
	id: string,
	userId: string,
	data: { name?: string; description?: string; weftCode?: string; loomCode?: string; layoutCode?: string }
): Promise<DbProject | null> {
	const sets: string[] = ['updated_at = NOW()'];
	const values: unknown[] = [];
	let paramIndex = 1;

	if (data.name !== undefined) {
		sets.push(`name = $${paramIndex++}`);
		values.push(data.name);
	}
	if (data.description !== undefined) {
		sets.push(`description = $${paramIndex++}`);
		values.push(data.description);
	}
	if (data.weftCode !== undefined) {
		sets.push(`weft_code = $${paramIndex++}`);
		values.push(data.weftCode);
	}
	if (data.loomCode !== undefined) {
		sets.push(`loom_code = $${paramIndex++}`);
		values.push(data.loomCode);
	}
	if (data.layoutCode !== undefined) {
		sets.push(`layout_code = $${paramIndex++}`);
		values.push(data.layoutCode);
	}

	values.push(id);
	const idParam = paramIndex++;
	values.push(userId);
	const userParam = paramIndex;
	const result = await pool.query<DbProject>(
		`UPDATE projects SET ${sets.join(', ')}
		 WHERE id = $${idParam} AND user_id = $${userParam}
		 RETURNING ${PROJECT_SELECT}`,
		values
	);
	return result.rows[0] || null;
}

export async function touchProjectOpened(id: string, userId: string): Promise<void> {
	await pool.query(
		`UPDATE projects SET last_opened_at = NOW() WHERE id = $1 AND user_id = $2`,
		[id, userId]
	);
}


/** Deletion outcome so the caller can render a useful error. */
export type DeleteProjectResult =
	| { ok: true }
	| { ok: false; reason: 'not_found' }
	| { ok: false; reason: 'has_deployments'; deploymentCount: number }
	| { ok: false; reason: 'cleanup_failed'; detail: string };

/**
 * Delete a builder project and cascade-clean its runtime state.
 *
 * Historically this function was a plain `DELETE FROM projects` which
 * left any triggers attached to the project alive in the DB AND in
 * the TriggerService's in-memory dispatcher. The dispatcher kept
 * firing them forever (crons every N seconds), burning credits
 * against a deleted project. That's the "stuck weather_cron" bug from
 * 2026-04-14.
 *
 * Cleanup contract:
 *
 *   1. Open a transaction and re-check every validation condition
 *      inside it: row existence, `is_deployment = false`, and the
 *      deployment-descendant count. Doing the check inside the tx
 *      closes the race where a concurrent publish creates a
 *      descendant between a precheck and the actual DELETE. Postgres
 *      MVCC guarantees we either see the concurrent publish's rows
 *      (and abort with `has_deployments`) or don't (and our DELETE
 *      wins, causing the concurrent publish's subsequent work to
 *      fail naturally when it can't find the builder row).
 *
 *   2. Delete trigger rows and the project row atomically. `triggers.project_id`
 *      has no FK cascade to `projects(id)` (M-9), so the DELETE must
 *      be explicit. Executions and project_versions DO cascade.
 *
 *   3. AFTER the transaction commits, call weft-api's
 *      `DELETE /api/v1/triggers/project/{id}` to tear down the live
 *      dispatcher handles. Doing this post-commit means a failed
 *      transaction leaves the dispatcher untouched (no drift), and
 *      a successful commit still guarantees the handles are cleaned
 *      up. Best-effort: if weft-api is unreachable, the orphan
 *      sweep in `start_trigger_maintenance_task` will catch the
 *      dangling handles within 30 seconds because their `project_id`
 *      no longer references a real project row.
 *
 * Deployment projects (`is_deployment=true`) cannot be deleted through
 * this path at all. They must go through the unpublish flow which
 * owns the publication lifecycle.
 */
export async function deleteProject(id: string, userId: string): Promise<DeleteProjectResult> {
	const client = await pool.connect();
	let commitOk = false;
	try {
		await client.query('BEGIN');

		// Single query locks the builder row with `FOR UPDATE`,
		// scoped to the caller's user_id so a foreign user cannot
		// delete another user's project. The precheck/cleanup race
		// is closed by the FK interaction on
		// `projects.origin_project_id REFERENCES projects(id)`: when
		// a concurrent publish tries to INSERT a deployment row with
		// `origin_project_id = this_builder_id`, Postgres takes a
		// `FOR KEY SHARE` row lock on the parent (the builder row)
		// to validate the FK. `FOR KEY SHARE` is incompatible with
		// our `FOR UPDATE`, so the publish INSERT blocks until our
		// transaction ends. The outcomes are:
		//
		//   a) We commit first: the publish wakes up, tries to
		//      re-validate the FK, finds the parent gone, and the
		//      INSERT fails with a foreign_key_violation.
		//   b) Publish commits first (not possible here because it
		//      blocked on our lock, but hypothetically under a
		//      different isolation level): our descendant count
		//      query below would see the new row and abort.
		//
		// So the `FOR UPDATE` + descendant count combination is
		// race-free. If the FK is ever dropped or changed to
		// `DEFERRABLE INITIALLY DEFERRED`, this analysis no longer
		// holds and the descendant check would need stronger
		// serialization.
		const check = await client.query(
			`SELECT id, is_deployment AS "isDeployment"
			 FROM projects
			 WHERE id = $1 AND user_id = $2
			 FOR UPDATE`,
			[id, userId]
		);
		if (check.rowCount === 0) {
			await client.query('ROLLBACK');
			return { ok: false, reason: 'not_found' };
		}
		if (check.rows[0].isDeployment) {
			// Deployment deletion goes through the unpublish flow.
			// The dashboard builder view never offers this option;
			// surfacing "not_found" makes a stray API call behave
			// the same way.
			await client.query('ROLLBACK');
			return { ok: false, reason: 'not_found' };
		}

		// Descendant count is a defensive second line of defense.
		// With the FK lock interaction above, concurrent publishes
		// are blocked on our row, so this query only finds rows
		// that were committed BEFORE our tx began. Scoped to the
		// same user_id so we don't leak the existence of another
		// user's deployments through the count.
		const desc = await client.query(
			`SELECT COUNT(*)::int AS count
			 FROM projects
			 WHERE origin_project_id = $1 AND is_deployment = true AND user_id = $2`,
			[id, userId]
		);
		const deploymentCount: number = desc.rows[0].count;
		if (deploymentCount > 0) {
			await client.query('ROLLBACK');
			return { ok: false, reason: 'has_deployments', deploymentCount };
		}

		// Cleanup: triggers first, then the project row. triggers has
		// no FK cascade to projects so the explicit DELETE is required.
		// The `triggers` table is owned by weft-api's schema (not the
		// dashboard's `initDb`), so a bare dashboard install that
		// never ran the Rust backend's init-db.sh may not have it.
		// Probe existence first with `to_regclass` to skip the DELETE
		// if the table is absent, instead of aborting the whole
		// transaction with "relation does not exist". This keeps
		// OSS dashboard-only installs functional even without the
		// full backend schema.
		const triggersTable = await client.query(
			`SELECT to_regclass('public.triggers') AS oid`
		);
		if (triggersTable.rows[0]?.oid !== null) {
			await client.query('DELETE FROM triggers WHERE project_id = $1', [id]);
		}
		const result = await client.query(
			'DELETE FROM projects WHERE id = $1 AND user_id = $2 AND is_deployment = false',
			[id, userId]
		);
		await client.query('COMMIT');
		commitOk = true;

		if ((result.rowCount ?? 0) === 0) {
			// Shouldn't be reachable because we re-checked under
			// FOR UPDATE above, but guard anyway.
			return { ok: false, reason: 'not_found' };
		}
	} catch (e) {
		try {
			await client.query('ROLLBACK');
		} catch (rollbackErr) {
			console.error(`[deleteProject] ROLLBACK failed for ${id}:`, rollbackErr);
		}
		const detail = e instanceof Error ? e.message : String(e);
		console.error(`[deleteProject] DB cleanup failed for ${id}:`, e);
		return { ok: false, reason: 'cleanup_failed', detail };
	} finally {
		client.release();
	}

	// Post-commit: tell weft-api to drop the live dispatcher handles.
	// Best-effort. A failure here means the dispatcher still holds
	// stale handles; the orphan sweep in
	// `start_trigger_maintenance_task` will remove them within 30s
	// because their `project_id` now points at a deleted projects row.
	if (commitOk) {
		const apiUrl = getApiUrl();
		try {
			const res = await fetch(`${apiUrl}/api/v1/triggers/project/${encodeURIComponent(id)}`, {
				method: 'DELETE',
			});
			if (!res.ok && res.status !== 404) {
				// 404 is fine (no triggers). Anything else is logged
				// but not fatal because the orphan sweep will clean
				// up any leftover in-memory handles on its next tick.
				console.warn(
					`[deleteProject] weft-api unregister returned ${res.status} for project ${id}; orphan sweep will retry`,
				);
			}
		} catch (e) {
			console.warn(
				`[deleteProject] Failed to reach weft-api for trigger cleanup of ${id}; orphan sweep will retry:`,
				e,
			);
		}
	}

	return { ok: true };
}

// Project version history
export interface DbProjectVersion {
	id: string;
	projectId: string;
	weftCode: string | null;
	loomCode: string | null;
	layoutCode: string | null;
	label: string | null;
	versionType: 'auto' | 'manual';
	createdAt: Date;
}

const VERSION_SELECT = `id, project_id AS "projectId", weft_code AS "weftCode", loom_code AS "loomCode", layout_code AS "layoutCode", label, version_type AS "versionType", created_at AS "createdAt"`;

async function initVersionsTable(): Promise<void> {
	try {
		await pool.query(`
			CREATE TABLE IF NOT EXISTS project_versions (
				id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
				project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
				weft_code TEXT,
				loom_code TEXT,
				label TEXT,
				version_type TEXT NOT NULL DEFAULT 'auto',
				created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
			)
		`);
	} catch (e: unknown) {
		const err = e as { code?: string };
		if (err.code !== '23505' && err.code !== '42P07') throw e;
	}
	try {
		await pool.query(`CREATE INDEX IF NOT EXISTS idx_project_versions_project_id ON project_versions(project_id)`);
	} catch (e: unknown) {
		const err = e as { code?: string };
		if (err.code !== '23505' && err.code !== '42P07') throw e;
	}
}

/**
 * Project version helpers. `project_versions` has no `user_id`
 * column of its own: ownership is inherited via
 * `project_id → projects(id) → projects.user_id`. Every function
 * here takes a `userId` and validates it via a subquery on the
 * parent `projects` row so a foreign caller can't read or mutate
 * another user's version history by guessing version ids.
 */
export async function createProjectVersion(
	projectId: string,
	userId: string,
	weftCode: string | null,
	loomCode: string | null,
	label: string | null,
	versionType: 'auto' | 'manual',
	layoutCode?: string | null,
): Promise<DbProjectVersion | null> {
	// Verify the parent project belongs to the caller before
	// inserting. Returns null if not (handler maps to 404).
	const owner = await pool.query(
		`SELECT 1 FROM projects WHERE id = $1 AND user_id = $2`,
		[projectId, userId]
	);
	if (owner.rowCount === 0) return null;

	const result = await pool.query<DbProjectVersion>(
		`INSERT INTO project_versions (project_id, weft_code, loom_code, layout_code, label, version_type)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 RETURNING ${VERSION_SELECT}`,
		[projectId, weftCode, loomCode, layoutCode ?? null, label, versionType]
	);
	// Trim auto-saves to 20 slots
	if (versionType === 'auto') {
		await pool.query(
			`DELETE FROM project_versions WHERE id IN (
				SELECT id FROM project_versions
				WHERE project_id = $1 AND version_type = 'auto'
				ORDER BY created_at DESC
				OFFSET 20
			)`,
			[projectId]
		);
	}
	return result.rows[0];
}

export async function listProjectVersions(projectId: string, userId: string): Promise<DbProjectVersion[]> {
	const result = await pool.query<DbProjectVersion>(
		`SELECT ${VERSION_SELECT} FROM project_versions
		 WHERE project_id = $1
		   AND project_id IN (SELECT id FROM projects WHERE user_id = $2)
		 ORDER BY created_at DESC`,
		[projectId, userId]
	);
	return result.rows;
}

export async function getProjectVersion(id: string, userId: string): Promise<DbProjectVersion | null> {
	const result = await pool.query<DbProjectVersion>(
		`SELECT ${VERSION_SELECT} FROM project_versions
		 WHERE id = $1
		   AND project_id IN (SELECT id FROM projects WHERE user_id = $2)`,
		[id, userId]
	);
	return result.rows[0] || null;
}

export async function updateProjectVersionLabel(id: string, userId: string, label: string | null): Promise<DbProjectVersion | null> {
	const result = await pool.query<DbProjectVersion>(
		`UPDATE project_versions SET label = $1
		 WHERE id = $2
		   AND project_id IN (SELECT id FROM projects WHERE user_id = $3)
		 RETURNING ${VERSION_SELECT}`,
		[label, id, userId]
	);
	return result.rows[0] || null;
}

export async function deleteProjectVersion(id: string, userId: string): Promise<boolean> {
	const result = await pool.query(
		`DELETE FROM project_versions
		 WHERE id = $1
		   AND project_id IN (SELECT id FROM projects WHERE user_id = $2)`,
		[id, userId]
	);
	return (result.rowCount ?? 0) > 0;
}

// Execution tracking
export interface DbExecution {
	id: string;
	projectId: string;
	userId: string;
	triggerId: string | null;
	nodeType: string | null;
	status: 'pending' | 'running' | 'completed' | 'failed' | 'waiting_for_input' | 'paused' | 'cancelled';
	nodeStatuses: Record<string, unknown>;
	nodeOutputs: Record<string, unknown>;
	nodeExecutions: Record<string, unknown>;
	error: string | null;
	startedAt: Date;
	completedAt: Date | null;
}

const EXECUTION_SELECT = `
	id,
	project_id AS "projectId",
	user_id AS "userId",
	trigger_id AS "triggerId",
	node_type AS "nodeType",
	status,
	node_statuses AS "nodeStatuses",
	node_outputs AS "nodeOutputs",
	COALESCE(node_executions, '{}') AS "nodeExecutions",
	error,
	started_at AS "startedAt",
	completed_at AS "completedAt"
`;

export async function initExecutionsTable(): Promise<void> {
	try {
		await pool.query(`
			CREATE TABLE IF NOT EXISTS executions (
				id TEXT PRIMARY KEY,
				project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
				user_id TEXT NOT NULL,
				trigger_id TEXT,
				node_type TEXT,
				status TEXT NOT NULL DEFAULT 'pending',
				node_statuses JSONB NOT NULL DEFAULT '{}',
				node_outputs JSONB NOT NULL DEFAULT '{}',
				error TEXT,
				started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
				completed_at TIMESTAMPTZ
			)
		`);
	} catch (e: unknown) {
		const err = e as { code?: string };
		if (err.code !== '23505' && err.code !== '42P07') throw e;
	}
	// Migration: add node_executions column
	try {
		await pool.query(`ALTER TABLE executions ADD COLUMN IF NOT EXISTS node_executions JSONB NOT NULL DEFAULT '{}'`);
	} catch (e: unknown) {
		const err = e as { code?: string };
		if (err.code !== '42701') throw e; // 42701 = column already exists
	}
	try {
		await pool.query(`
			CREATE INDEX IF NOT EXISTS idx_executions_project_id ON executions(project_id)
		`);
	} catch (e: unknown) {
		const err = e as { code?: string };
		if (err.code !== '23505' && err.code !== '42P07') throw e;
	}
	try {
		await pool.query(`
			CREATE INDEX IF NOT EXISTS idx_executions_user_id ON executions(user_id)
		`);
	} catch (e: unknown) {
		const err = e as { code?: string };
		if (err.code !== '23505' && err.code !== '42P07') throw e;
	}
}

export async function updateExecution(
	id: string,
	userId: string,
	data: {
		status?: string;
		nodeStatuses?: Record<string, unknown>;
		nodeOutputs?: Record<string, unknown>;
		nodeExecutions?: Record<string, unknown>;
		error?: string;
	}
): Promise<DbExecution | null> {
	const sets: string[] = [];
	const values: unknown[] = [];
	let paramIndex = 1;

	if (data.status !== undefined) {
		const s = data.status.toLowerCase();
		sets.push(`status = $${paramIndex++}`);
		values.push(s);
		if (s === 'completed' || s === 'failed' || s === 'cancelled') {
			sets.push(`completed_at = NOW()`);
		}
	}
	if (data.nodeStatuses !== undefined) {
		sets.push(`node_statuses = $${paramIndex++}`);
		values.push(JSON.stringify(data.nodeStatuses));
	}
	if (data.nodeOutputs !== undefined) {
		sets.push(`node_outputs = $${paramIndex++}`);
		values.push(JSON.stringify(data.nodeOutputs));
	}
	if (data.nodeExecutions !== undefined) {
		sets.push(`node_executions = $${paramIndex++}`);
		values.push(JSON.stringify(data.nodeExecutions));
	}
	if (data.error !== undefined) {
		sets.push(`error = $${paramIndex++}`);
		values.push(data.error);
	}

	if (sets.length === 0) return null;

	values.push(id);
	const idParam = paramIndex++;
	values.push(userId);
	const userParam = paramIndex;
	const result = await pool.query<DbExecution>(
		`UPDATE executions SET ${sets.join(', ')}
		 WHERE id = $${idParam} AND user_id = $${userParam}
		 RETURNING ${EXECUTION_SELECT}`,
		values
	);
	return result.rows[0] || null;
}

export async function getExecution(id: string, userId: string): Promise<DbExecution | null> {
	const result = await pool.query<DbExecution>(
		`SELECT ${EXECUTION_SELECT} FROM executions WHERE id = $1 AND user_id = $2`,
		[id, userId]
	);
	return result.rows[0] || null;
}

/** Look up the owning user_id for an execution without a per-user filter.
 *  `_INTERNAL` suffix is a NAMING FLAG: this must ONLY be called from the
 *  internal-API-key auth path in hooks.server.ts, never from a user-facing
 *  handler, or the per-user isolation guarantee collapses. Adding a new
 *  call site in a user route is a bug. */
export async function getExecutionOwnerInternal(id: string): Promise<string | null> {
	const result = await pool.query<{ user_id: string }>(
		`SELECT user_id FROM executions WHERE id = $1`,
		[id]
	);
	return result.rows[0]?.user_id ?? null;
}

export async function listExecutions(
	projectId?: string,
	userId?: string,
	limit: number = 50,
	offset: number = 0,
): Promise<DbExecution[]> {
	let query = `SELECT ${EXECUTION_SELECT} FROM executions`;
	const conditions: string[] = [];
	const values: unknown[] = [];
	let paramIndex = 1;

	if (projectId) {
		conditions.push(`project_id = $${paramIndex++}`);
		values.push(projectId);
	}
	if (userId) {
		conditions.push(`user_id = $${paramIndex++}`);
		values.push(userId);
	}

	if (conditions.length > 0) {
		query += ' WHERE ' + conditions.join(' AND ');
	}
	query += ` ORDER BY started_at DESC LIMIT $${paramIndex++} OFFSET $${paramIndex}`;
	values.push(limit, offset);

	const result = await pool.query<DbExecution>(query, values);
	return result.rows;
}

// =============================================================================
// TEST CONFIGS
// =============================================================================

export interface DbTestConfig {
	id: string;
	projectId: string;
	name: string;
	description: string;
	mocks: Record<string, Record<string, unknown>>;
	createdAt: Date;
	updatedAt: Date;
}

async function initTestConfigsTable(): Promise<void> {
	try {
		await pool.query(`
			CREATE TABLE IF NOT EXISTS test_configs (
				id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
				project_id UUID NOT NULL,
				name TEXT NOT NULL,
				description TEXT NOT NULL DEFAULT '',
				mocks JSONB NOT NULL DEFAULT '{}',
				created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
				updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
			)
		`);
		await pool.query(`CREATE INDEX IF NOT EXISTS idx_test_configs_project_id ON test_configs(project_id)`);
	} catch (e: unknown) {
		const err = e as { code?: string };
		if (err.code !== '23505' && err.code !== '42P07') throw e;
	}
}

const TEST_CONFIG_SELECT = `id, project_id AS "projectId", name, description, mocks, created_at AS "createdAt", updated_at AS "updatedAt"`;

/**
 * Test config helpers. Like project_versions, `test_configs` has no
 * `user_id` column of its own, so ownership is inherited via
 * `project_id → projects.user_id`. Every function takes a `userId`
 * and validates it through the parent project so a foreign caller
 * can't read or mutate another user's mocks by guessing config ids.
 */
export async function listTestConfigs(projectId: string, userId: string): Promise<DbTestConfig[]> {
	const result = await pool.query<DbTestConfig>(
		`SELECT ${TEST_CONFIG_SELECT} FROM test_configs
		 WHERE project_id = $1
		   AND project_id IN (SELECT id FROM projects WHERE user_id = $2)
		 ORDER BY created_at ASC`,
		[projectId, userId]
	);
	return result.rows;
}

export async function getTestConfig(id: string, userId: string): Promise<DbTestConfig | null> {
	const result = await pool.query<DbTestConfig>(
		`SELECT ${TEST_CONFIG_SELECT} FROM test_configs
		 WHERE id = $1
		   AND project_id IN (SELECT id FROM projects WHERE user_id = $2)`,
		[id, userId]
	);
	return result.rows[0] ?? null;
}

export async function createTestConfig(
	projectId: string,
	userId: string,
	data: { name: string; description?: string; mocks?: Record<string, Record<string, unknown>> },
): Promise<DbTestConfig | null> {
	// Verify the parent project belongs to the caller.
	const owner = await pool.query(
		`SELECT 1 FROM projects WHERE id = $1 AND user_id = $2`,
		[projectId, userId]
	);
	if (owner.rowCount === 0) return null;

	const result = await pool.query<DbTestConfig>(
		`INSERT INTO test_configs (project_id, name, description, mocks) VALUES ($1, $2, $3, $4) RETURNING ${TEST_CONFIG_SELECT}`,
		[projectId, data.name, data.description ?? '', JSON.stringify(data.mocks ?? {})]
	);
	return result.rows[0];
}

export async function updateTestConfig(
	id: string,
	userId: string,
	data: { name?: string; description?: string; mocks?: Record<string, Record<string, unknown>> },
): Promise<DbTestConfig | null> {
	const sets: string[] = [];
	const values: unknown[] = [];
	let paramIndex = 1;

	if (data.name !== undefined) {
		sets.push(`name = $${paramIndex++}`);
		values.push(data.name);
	}
	if (data.description !== undefined) {
		sets.push(`description = $${paramIndex++}`);
		values.push(data.description);
	}
	if (data.mocks !== undefined) {
		sets.push(`mocks = $${paramIndex++}`);
		values.push(JSON.stringify(data.mocks));
	}

	if (sets.length === 0) return getTestConfig(id, userId);

	sets.push(`updated_at = NOW()`);
	values.push(id);
	const idParam = paramIndex++;
	values.push(userId);
	const userParam = paramIndex;

	const result = await pool.query<DbTestConfig>(
		`UPDATE test_configs SET ${sets.join(', ')}
		 WHERE id = $${idParam}
		   AND project_id IN (SELECT id FROM projects WHERE user_id = $${userParam})
		 RETURNING ${TEST_CONFIG_SELECT}`,
		values
	);
	return result.rows[0] ?? null;
}

export async function deleteTestConfig(id: string, userId: string): Promise<boolean> {
	const result = await pool.query(
		`DELETE FROM test_configs
		 WHERE id = $1
		   AND project_id IN (SELECT id FROM projects WHERE user_id = $2)`,
		[id, userId]
	);
	return (result.rowCount ?? 0) > 0;
}
