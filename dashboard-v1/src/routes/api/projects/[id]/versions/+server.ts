import { json } from '@sveltejs/kit';
import type { RequestEvent } from '@sveltejs/kit';
import * as db from '$lib/server/db';
import { encryptWeftCode, decryptWeftCode } from '$lib/server/crypto';
import { requireUserId } from '$lib/server/auth-locals';

export const GET = async (event: RequestEvent) => {
	const userId = requireUserId(event);
	try {
		await db.initDb();
		const versions = await db.listProjectVersions(event.params.id!, userId);
		for (const v of versions) {
			if (typeof v.weftCode === 'string') {
				v.weftCode = decryptWeftCode(v.weftCode);
			}
		}
		return json(versions);
	} catch (error) {
		console.error('Failed to list project versions:', error);
		return json({ error: 'Failed to list versions' }, { status: 500 });
	}
};

export const POST = async (event: RequestEvent) => {
	const userId = requireUserId(event);
	try {
		await db.initDb();
		const body = await event.request.json();
		const encryptedWeft = typeof body.weftCode === 'string' ? encryptWeftCode(body.weftCode) : body.weftCode;
		const version = await db.createProjectVersion(
			event.params.id!,
			userId,
			encryptedWeft ?? null,
			body.loomCode ?? null,
			body.label ?? null,
			body.versionType ?? 'auto',
			body.layoutCode ?? null,
		);
		if (!version) {
			// createProjectVersion returns null when the parent
			// project doesn't belong to the caller.
			return json({ error: 'Project not found' }, { status: 404 });
		}
		if (typeof version.weftCode === 'string') {
			version.weftCode = decryptWeftCode(version.weftCode);
		}
		return json(version, { status: 201 });
	} catch (error) {
		console.error('Failed to create project version:', error);
		return json({ error: 'Failed to create version' }, { status: 500 });
	}
};
