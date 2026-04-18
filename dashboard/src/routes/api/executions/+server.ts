import { json } from '@sveltejs/kit';
import type { RequestEvent } from '@sveltejs/kit';
import * as db from '$lib/server/db';
import { requireUserId } from '$lib/server/auth-locals';

export const GET = async (event: RequestEvent) => {
	// Always scope to the authenticated caller. Previously this read
	// userId from a query param, which let any client pass any
	// userId and read another user's execution history.
	const userId = requireUserId(event);
	try {
		await db.initDb();
		const projectId = event.url.searchParams.get('projectId') || undefined;
		const limit = parseInt(event.url.searchParams.get('limit') || '50', 10);
		const offset = parseInt(event.url.searchParams.get('offset') || '0', 10);

		const executions = await db.listExecutions(projectId, userId, limit, offset);
		return json(executions);
	} catch (error) {
		console.error('Failed to list executions:', error);
		return json({ error: 'Failed to list executions' }, { status: 500 });
	}
};
