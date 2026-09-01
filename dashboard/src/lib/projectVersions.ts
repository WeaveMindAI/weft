import { authFetch } from '$lib/config';

/** One saved snapshot of a project's code. */
export interface ProjectVersion {
	id: string;
	projectId: string;
	weftCode: string | null;
	loomCode: string | null;
	layoutCode: string | null;
	label: string | null;
	versionType: 'auto' | 'manual';
	createdAt: string;
}

export interface VersionSnapshot {
	weftCode: string;
	loomCode: string | null;
	layoutCode: string | null;
}

export async function listProjectVersions(projectId: string): Promise<ProjectVersion[]> {
	const res = await authFetch(`/api/projects/${projectId}/versions`);
	if (!res.ok) throw new Error(`Could not load the version history (${res.status})`);
	return res.json();
}

/** Save a snapshot. Throws on failure: a snapshot the caller believes was
 *  written but was not is how a project becomes unrecoverable. */
export async function saveProjectVersion(
	projectId: string,
	snapshot: VersionSnapshot,
	label: string | null,
	versionType: 'auto' | 'manual',
): Promise<ProjectVersion> {
	const res = await authFetch(`/api/projects/${projectId}/versions`, {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify({ ...snapshot, label, versionType }),
	});
	if (!res.ok) throw new Error(`Could not save a version (${res.status})`);
	return res.json();
}
