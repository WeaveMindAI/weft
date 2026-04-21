import { json } from '@sveltejs/kit';
import type { RequestEvent } from '@sveltejs/kit';
import * as db from '$lib/server/db';
import { requireUserId } from '$lib/server/auth-locals';

export const GET = async (event: RequestEvent) => {
	const userId = requireUserId(event);
	try {
		await db.initDb();
		const configs = await db.listTestConfigs(event.params.id!, userId);
		return json(configs);
	} catch (error) {
		console.error('Failed to list test configs:', error);
		return json({ error: 'Failed to list test configs' }, { status: 500 });
	}
};

export const POST = async (event: RequestEvent) => {
	const userId = requireUserId(event);
	try {
		await db.initDb();
		const body = await event.request.json();
		if (!body.name) {
			return json({ error: 'name is required' }, { status: 400 });
		}
		const config = await db.createTestConfig(event.params.id!, userId, {
			name: body.name,
			description: body.description,
			mocks: body.mocks,
		});
		if (!config) {
			// Parent project doesn't belong to the caller.
			return json({ error: 'Project not found' }, { status: 404 });
		}
		return json(config, { status: 201 });
	} catch (error) {
		console.error('Failed to create test config:', error);
		return json({ error: 'Failed to create test config' }, { status: 500 });
	}
};
