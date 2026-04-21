import { json } from '@sveltejs/kit';
import type { RequestEvent } from '@sveltejs/kit';
import * as db from '$lib/server/db';
import { requireUserId } from '$lib/server/auth-locals';

export const GET = async (event: RequestEvent) => {
	const userId = requireUserId(event);
	try {
		await db.initDb();
		const execution = await db.getExecution(event.params.id!, userId);
		if (!execution) {
			return json({ error: 'Execution not found' }, { status: 404 });
		}
		return json(execution);
	} catch (error) {
		console.error('Failed to get execution:', error);
		return json({ error: 'Failed to get execution' }, { status: 500 });
	}
};

async function handleUpdate(event: RequestEvent) {
	const userId = requireUserId(event);
	try {
		await db.initDb();
		const body = await event.request.json();
		const execution = await db.updateExecution(event.params.id!, userId, body);
		if (!execution) {
			return json({ error: 'Execution not found' }, { status: 404 });
		}
		return json(execution);
	} catch (error) {
		console.error('Failed to update execution:', error);
		return json({ error: 'Failed to update execution' }, { status: 500 });
	}
}

export const PUT = handleUpdate;
export const POST = handleUpdate;
