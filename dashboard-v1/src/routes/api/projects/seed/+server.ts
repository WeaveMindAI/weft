import { json } from '@sveltejs/kit';
import type { RequestEvent } from '@sveltejs/kit';
import * as db from '$lib/server/db';
import { encryptWeftCode } from '$lib/server/crypto';
import { requireUserId } from '$lib/server/auth-locals';

import w1 from '../../../../../static/tutorials/01-hello-weavemind.json';
import w2 from '../../../../../static/tutorials/02-scheduled-fetch.json';
import w3 from '../../../../../static/tutorials/03-human-in-the-loop.json';
import w4 from '../../../../../static/tutorials/04-persistent-memory.json';
import w5 from '../../../../../static/tutorials/05-parallel-processing.json';

const TUTORIALS = [w1, w2, w3, w4, w5];

export const POST = async (event: RequestEvent) => {
	const userId = requireUserId(event);
	try {
		await db.initDb();

		const existing = await db.listProjects(userId);
		const existingNames = new Set(existing.map((w) => w.name));

		const created = [];
		for (const tutorial of TUTORIALS) {
			if (existingNames.has(tutorial.name)) continue;
			const weftCode = encryptWeftCode(tutorial.weftCode as string);
			const project = await db.createProject(
				userId,
				tutorial.name,
				tutorial.description ?? null,
				weftCode,
				undefined,
				(tutorial as { layoutCode?: string }).layoutCode,
			);
			created.push(project.id);
		}

		return json({ seeded: created.length });
	} catch (error) {
		console.error('Failed to seed tutorial projects:', error);
		return json({ error: 'Failed to seed tutorial projects' }, { status: 500 });
	}
};
