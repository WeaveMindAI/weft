import { json } from '@sveltejs/kit';
import type { RequestEvent } from '@sveltejs/kit';
import * as db from '$lib/server/db';
import { requireUserId } from '$lib/server/auth-locals';

export const PATCH = async (event: RequestEvent) => {
	const userId = requireUserId(event);
	try {
		await db.initDb();
		const body = await event.request.json();
		const version = await db.updateProjectVersionLabel(event.params.vid!, userId, body.label ?? null);
		if (!version) {
			return json({ error: 'Version not found' }, { status: 404 });
		}
		return json(version);
	} catch (error) {
		console.error('Failed to update version:', error);
		return json({ error: 'Failed to update version' }, { status: 500 });
	}
};

export const DELETE = async (event: RequestEvent) => {
	const userId = requireUserId(event);
	try {
		await db.initDb();
		const deleted = await db.deleteProjectVersion(event.params.vid!, userId);
		if (!deleted) {
			return json({ error: 'Version not found' }, { status: 404 });
		}
		return json({ ok: true });
	} catch (error) {
		console.error('Failed to delete version:', error);
		return json({ error: 'Failed to delete version' }, { status: 500 });
	}
};
