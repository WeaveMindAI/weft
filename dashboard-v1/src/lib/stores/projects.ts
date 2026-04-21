import { writable } from "svelte/store";
import { browser } from "$app/environment";
import type { ProjectDefinition } from "$lib/types";
import { authFetch } from "$lib/config";
import { hydrateProject } from "$lib/project-hydration";

// All requests go through `authFetch`, which attaches the dashboard
// JWT from sessionStorage as `Authorization: Bearer <token>`. The
// server's auth middleware decodes the JWT, populates
// `event.locals.user`, and scopes every db.ts query by user_id. We
// no longer pass `userId` in the query string or body: the server
// ignores those and uses the authenticated identity instead.
function getProjectsApiUrl(): string {
	return '/api/projects';
}

function createProjectStore() {
	const { subscribe, set, update } = writable<ProjectDefinition[]>([]);
	const loading = writable(false);
	const error = writable<string | null>(null);
	const baseUrl = getProjectsApiUrl();

	async function fetchProjects(): Promise<void> {
		if (!browser) return;
		loading.set(true);
		error.set(null);
		try {
			const response = await authFetch(baseUrl, { credentials: 'include' });
			if (!response.ok) {
				throw new Error(`Failed to fetch projects: ${response.statusText}`);
			}
			const data = await response.json();
			set(data.map((raw: Record<string, unknown>) => hydrateProject(raw as Parameters<typeof hydrateProject>[0])));
		} catch (e) {
			console.error("Failed to fetch projects:", e);
			error.set(e instanceof Error ? e.message : "Unknown error");
		} finally {
			loading.set(false);
		}
	}

	async function addProject(data: { name: string; description?: string | null; weftCode?: string; loomCode?: string; layoutCode?: string }): Promise<ProjectDefinition | null> {
		if (!browser) return null;
		try {
			const response = await authFetch(baseUrl, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					name: data.name,
					description: data.description || null,
					weftCode: data.weftCode,
					loomCode: data.loomCode,
					layoutCode: data.layoutCode,
				}),
				credentials: 'include',
			});
			if (!response.ok) {
				throw new Error(`Failed to create project: ${response.statusText}`);
			}
			const raw = await response.json();
			const created = hydrateProject(raw);
			update((items) => [...items, created]);
			return created;
		} catch (e) {
			console.error("Failed to create project:", e);
			error.set(e instanceof Error ? e.message : "Unknown error");
			return null;
		}
	}

	async function removeProject(id: string): Promise<{ ok: true } | { ok: false; status: number; message: string }> {
		if (!browser) return { ok: false, status: 0, message: 'Not in browser' };
		try {
			const response = await authFetch(`${baseUrl}/${id}`, {
				method: 'DELETE',
				credentials: 'include',
			});
			if (!response.ok) {
				let message = response.statusText;
				try {
					const body = await response.json();
					if (body?.error) message = body.error;
				} catch { /* response had no JSON body */ }
				return { ok: false, status: response.status, message };
			}
			update((items) => items.filter((w) => w.id !== id));
			return { ok: true };
		} catch (e) {
			console.error("Failed to delete project:", e);
			const message = e instanceof Error ? e.message : "Unknown error";
			error.set(message);
			return { ok: false, status: 0, message };
		}
	}

	async function updateProject(id: string, data: { name?: string; description?: string; weftCode?: string; loomCode?: string; layoutCode?: string }): Promise<ProjectDefinition | null> {
		if (!browser) return null;
		try {
			const response = await authFetch(`${baseUrl}/${id}`, {
				method: 'PUT',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify(data),
				credentials: 'include',
			});
			if (!response.ok) {
				throw new Error(`Failed to update project: ${response.statusText}`);
			}
			const raw = await response.json();
			const updated = hydrateProject(raw);
			update((items) => items.map((w) => (w.id === id ? updated : w)));
			return updated;
		} catch (e) {
			console.error("Failed to update project:", e);
			error.set(e instanceof Error ? e.message : "Unknown error");
			return null;
		}
	}

	/**
	 * Fetch a single project by id and add it to the store. Works for both
	 * builder and deployment projects; used when navigating directly to a
	 * deployment's page via the Manage Deployments modal (deployments are
	 * excluded from the default list fetch).
	 */
	async function fetchProjectById(id: string): Promise<ProjectDefinition | null> {
		if (!browser) return null;
		try {
			const response = await authFetch(`${baseUrl}/${id}`, { credentials: 'include' });
			if (!response.ok) {
				console.warn(`[projects] fetchById(${id}) failed: ${response.status} ${response.statusText}`);
				return null;
			}
			const raw = await response.json();
			if (!raw || typeof raw !== 'object' || !('id' in raw)) {
				console.warn(`[projects] fetchById(${id}) returned malformed row:`, raw);
				return null;
			}
			const hydrated = hydrateProject(raw);
			update((items) => {
				const without = items.filter(p => p.id !== id);
				return [hydrated, ...without];
			});
			return hydrated;
		} catch (e) {
			console.error(`[projects] fetchById(${id}) threw:`, e);
			return null;
		}
	}

	return {
		subscribe,
		loading: { subscribe: loading.subscribe },
		error: { subscribe: error.subscribe },
		add: addProject,
		remove: removeProject,
		update: updateProject,
		fetchById: fetchProjectById,
		set,
		reload: fetchProjects,
		init: fetchProjects,
	};
}

export const projects = createProjectStore();
