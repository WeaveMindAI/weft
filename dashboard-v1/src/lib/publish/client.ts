/**
 * Dashboard publish client.
 *
 * Thin wrapper around the publish API. The dashboard talks to whichever
 * backend is configured (weft-api locally, cloud-api in cloud mode) via the
 * `api.*` URL builders in $lib/config. The backend contract is identical for
 * the CRUD surface.
 *
 * Publications are keyed on (username, slug). The public URL takes the
 * form `/p/<username>/<slug>`, so two different users can each own
 * `weather-monitor` without colliding. Owner-scoped mutation endpoints
 * stay slug-only because the authenticated user_id already uniquely
 * scopes the caller's slug namespace.
 *
 * Auth: picked up automatically from `getAuthHeaders()` which reads the
 * sessionStorage JWT (set by the website iframe auth handshake in cloud
 * mode) or falls back to nothing in local mode. A `x-user-id` header is
 * also sent so local weft-api and the cloud-api's local-mode path can
 * attribute ownership without a JWT.
 */

import { api, getAuthHeaders } from '$lib/config';
import { browser } from '$app/environment';
import { STORAGE_KEYS } from '$lib/utils';

export interface PublishedProject {
	id: string;
	slug: string;
	username: string;
	user_id: string;
	project_id: string | null;
	project_name: string;
	description: string | null;
	/** Legacy ghost columns, nullable. New publishes leave these null;
	 *  the dashboard never reads them — the deployment row is the
	 *  source of truth. Kept on the type so older rows still deserialize. */
	weft_code: string | null;
	loom_code: string | null;
	layout_code: string | null;
	is_live: boolean;
	view_count: number;
	run_count: number;
	published_at: string;
	updated_at: string;
	/** Deployer-chosen per-slug rate limit (req/min). Null means default
	 *  (see backend `PUBLISH_RATE_LIMIT_DEFAULT_PER_MINUTE`). Hard-capped
	 *  at `PUBLISH_RATE_LIMIT_MAX_PER_MINUTE`. */
	rate_limit_per_minute: number | null;
	/** Builder project this deployment was cloned from, joined from
	 *  `projects.origin_project_id` server-side. Used by the dashboard
	 *  project list to light the purple thunder on the builder when
	 *  any of its deployment descendants has an active trigger. Null
	 *  for orphan mapping rows whose project row has been deleted. */
	origin_project_id: string | null;
}

/**
 * Visitor access allowlist derived from a deployment's loom manifest.
 * Computed client-side at publish time and sent with the publish
 * request so the backend can enforce it on every visitor run.
 *
 * Shape:
 *   {
 *     inputs:  { "<nodeId>": ["field1", "field2", ...], ... },
 *     outputs: { "<nodeId>": ["port1", "port2", ...], ... },
 *   }
 */
export interface VisitorAccessAllowlist {
	inputs: Record<string, string[]>;
	outputs: Record<string, string[]>;
}

export interface PublicSnapshot {
	slug: string;
	username: string;
	projectName: string;
	description: string | null;
	weftCode: string;
	loomCode: string;
	layoutCode: string | null;
	available: boolean;
	/** True when the visitor page must render a "Built with WeaveMind"
	 *  footer. Computed server-side from the deployer's subscription tier.
	 *  Builder tier and above get `false` (unbranded publishing is a paid
	 *  feature); everything else gets `true`. OSS local mode returns
	 *  `false` because there's no hosted brand to promote. */
	showBuiltWithFooter: boolean;
}

export interface LatestTriggerRun {
	executionId: string;
	startedAt: string;
	outputs: Record<string, unknown>;
}

function publishHeaders(): HeadersInit {
	const h: Record<string, string> = { 'content-type': 'application/json' };
	Object.assign(h, getAuthHeaders());
	if (browser) {
		const userId = sessionStorage.getItem(STORAGE_KEYS.userId);
		if (userId) h['x-user-id'] = userId;
	}
	return h;
}

// ── Owner-scoped CRUD ────────────────────────────────────────────────────────

export async function listPublications(): Promise<PublishedProject[]> {
	const res = await fetch(api.listPublications(), {
		credentials: 'include',
		headers: publishHeaders(),
	});
	if (!res.ok) throw new Error(`Failed to list publications: ${res.status}`);
	return res.json();
}

export async function publishProject(
	input: {
		projectId: string;
		slug: string;
		description?: string | null;
		/** Already-sanitized weft source. The caller is responsible for
		 *  stripping sensitive fields via `$lib/ai/sanitize` when the
		 *  deployer asked to (default). Omitting this field tells the
		 *  backend to fall back to the builder row's weft_code — only
		 *  used by CLI/test harnesses that can't sanitize client-side. */
		weftCode?: string;
		loomCode?: string;
		layoutCode?: string | null;
		/** Precomputed allowlist derived from the loom manifest so the
		 *  backend can gate visitor input merges and output broadcasts
		 *  without parsing loom in Rust. */
		visitorAccess?: VisitorAccessAllowlist;
		/** Deployer-chosen rate limit; null/undefined keeps the backend
		 *  default. Rejected with 400 if above the hard cap. */
		rateLimitPerMinute?: number | null;
	},
): Promise<PublishedProject> {
	const res = await fetch(api.publishProject(), {
		method: 'POST',
		credentials: 'include',
		headers: publishHeaders(),
		body: JSON.stringify({
			project_id: input.projectId,
			slug: input.slug,
			description: input.description ?? null,
			weft_code: input.weftCode ?? null,
			loom_code: input.loomCode ?? null,
			layout_code: input.layoutCode ?? null,
			visitor_access: input.visitorAccess ?? null,
			rate_limit_per_minute: input.rateLimitPerMinute ?? null,
		}),
	});
	if (!res.ok) {
		const err = await res.json().catch(() => ({}));
		throw new Error(err.error ?? `Publish failed (${res.status})`);
	}
	return res.json();
}

export async function updatePublication(
	slug: string,
	patch: {
		is_live?: boolean;
		description?: string;
		/** Deployer-chosen rate limit; omit to leave unchanged. */
		rate_limit_per_minute?: number;
	},
): Promise<void> {
	const res = await fetch(api.updatePublication(slug), {
		method: 'PATCH',
		credentials: 'include',
		headers: publishHeaders(),
		body: JSON.stringify(patch),
	});
	if (!res.ok) {
		const err = await res.json().catch(() => ({}));
		throw new Error(err.error ?? `Update failed: ${res.status}`);
	}
}

export async function deletePublication(slug: string): Promise<void> {
	const res = await fetch(api.deletePublication(slug), {
		method: 'DELETE',
		credentials: 'include',
		headers: publishHeaders(),
	});
	if (!res.ok) throw new Error(`Delete failed: ${res.status}`);
}

// ── Public endpoints (keyed on (username, slug), no auth) ───────────────────

/** Used by the /p/<username>/<slug> route to render the page. */
export async function getPublication(username: string, slug: string): Promise<PublicSnapshot> {
	const res = await fetch(api.getPublicationByUserSlug(username, slug), {
		credentials: 'include',
	});
	if (!res.ok) throw new Error(`Not found: ${res.status}`);
	return res.json();
}

/** Mint visitor cookie + return any prior state. Cloud-only; local 404s. */
export async function getPublicationSession(username: string, slug: string): Promise<{
	visitorId: string;
	session: { lastInputs: Record<string, unknown> | null; lastOutputs: Record<string, unknown> | null } | null;
}> {
	const res = await fetch(api.publicationSession(username, slug), { credentials: 'include' });
	if (!res.ok) throw new Error(`Session fetch failed: ${res.status}`);
	return res.json();
}

/** Run a visitor request. Cloud-only. */
export async function runPublication(
	username: string,
	slug: string,
	inputs: Record<string, Record<string, unknown>>,
): Promise<{ ok: boolean; result: unknown }> {
	const res = await fetch(api.publicationRun(username, slug), {
		method: 'POST',
		credentials: 'include',
		headers: { 'content-type': 'application/json' },
		body: JSON.stringify({ inputs }),
	});
	if (!res.ok) {
		const err = await res.json().catch(() => ({}));
		throw new Error(err.error ?? `Run failed (${res.status})`);
	}
	return res.json();
}

/**
 * Fetch the latest trigger-fired execution's outputs for this deployment.
 * Used by visitor polling so everyone on the page sees the same shared
 * trigger result. Returns null if no trigger has fired yet.
 */
export async function getLatestTriggerRun(
	username: string,
	slug: string,
): Promise<LatestTriggerRun | null> {
	const res = await fetch(api.publicationLatestTriggerRun(username, slug), {
		credentials: 'include',
	});
	if (!res.ok) return null;
	const body = await res.json();
	return body as LatestTriggerRun | null;
}
