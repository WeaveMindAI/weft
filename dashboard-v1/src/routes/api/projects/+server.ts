import { json } from '@sveltejs/kit';
import type { RequestEvent } from '@sveltejs/kit';
import * as db from '$lib/server/db';
import { encryptWeftCode, decryptWeftCode } from '$lib/server/crypto';
import { requireUserId } from '$lib/server/auth-locals';

export const GET = async (event: RequestEvent) => {
	const userId = requireUserId(event);
	try {
		await db.initDb();
		const limit = parseInt(event.url.searchParams.get('limit') || '100', 10);
		const offset = parseInt(event.url.searchParams.get('offset') || '0', 10);
		const projects = await db.listProjects(userId, limit, offset);
		for (const wf of projects) {
			if (typeof wf.weftCode === 'string') {
				wf.weftCode = decryptWeftCode(wf.weftCode);
			}
		}
		return json(projects);
	} catch (error) {
		console.error('Failed to list projects:', error);
		return json({ error: 'Failed to list projects' }, { status: 500 });
	}
};

export const POST = async (event: RequestEvent) => {
	// Owner is the authenticated caller from locals.user. The
	// previous version trusted `body.userId` (and a `?userId=` query
	// param) which let any client claim to be any user. Now the
	// owner is fixed at the server side.
	const userId = requireUserId(event);
	try {
		await db.initDb();
		const body = await event.request.json();
		const { name, description, weftCode, loomCode, layoutCode } = body;

		if (!name) {
			return json({ error: 'Name is required' }, { status: 400 });
		}

		const encryptedWeft = typeof weftCode === 'string' ? encryptWeftCode(weftCode) : undefined;
		const project = await db.createProject(
			userId,
			name,
			description || null,
			encryptedWeft,
			loomCode,
			layoutCode,
		);
		return json(project, { status: 201 });
	} catch (error) {
		console.error('Failed to create project:', error);
		return json({ error: 'Failed to create project' }, { status: 500 });
	}
};
