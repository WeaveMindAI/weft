import { json } from '@sveltejs/kit';
import type { RequestEvent } from '@sveltejs/kit';
import * as db from '$lib/server/db';
import { requireUserId } from '$lib/server/auth-locals';

export const GET = async (event: RequestEvent) => {
	const userId = requireUserId(event);
	try {
		await db.initDb();
		const config = await db.getTestConfig(event.params.configId!, userId);
		if (!config) {
			return json({ error: 'Test config not found' }, { status: 404 });
		}
		return json(config);
	} catch (error) {
		console.error('Failed to get test config:', error);
		return json({ error: 'Failed to get test config' }, { status: 500 });
	}
};

export const PUT = async (event: RequestEvent) => {
	const userId = requireUserId(event);
	try {
		await db.initDb();
		const body = await event.request.json();
		const config = await db.updateTestConfig(event.params.configId!, userId, {
			name: body.name,
			description: body.description,
			mocks: body.mocks,
		});
		if (!config) {
			return json({ error: 'Test config not found' }, { status: 404 });
		}
		return json(config);
	} catch (error) {
		console.error('Failed to update test config:', error);
		return json({ error: 'Failed to update test config' }, { status: 500 });
	}
};

export const DELETE = async (event: RequestEvent) => {
	const userId = requireUserId(event);
	try {
		await db.initDb();
		const deleted = await db.deleteTestConfig(event.params.configId!, userId);
		if (!deleted) {
			return json({ error: 'Test config not found' }, { status: 404 });
		}
		return json({ ok: true });
	} catch (error) {
		console.error('Failed to delete test config:', error);
		return json({ error: 'Failed to delete test config' }, { status: 500 });
	}
};
