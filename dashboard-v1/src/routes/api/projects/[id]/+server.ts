import { json } from '@sveltejs/kit';
import type { RequestEvent } from '@sveltejs/kit';
import * as db from '$lib/server/db';
import { encryptWeftCode, decryptWeftCode } from '$lib/server/crypto';
import { requireUserId } from '$lib/server/auth-locals';

export const GET = async (event: RequestEvent) => {
	const userId = requireUserId(event);
	try {
		await db.initDb();
		const project = await db.getProject(event.params.id!, userId);
		if (!project) {
			return json({ error: 'Project not found' }, { status: 404 });
		}

		// Decrypt sensitive fields in weft code (passwords, tokens)
		if (typeof project.weftCode === 'string') {
			project.weftCode = decryptWeftCode(project.weftCode);
		}

		return json(project);
	} catch (error) {
		console.error('Failed to get project:', error);
		return json({ error: 'Failed to get project' }, { status: 500 });
	}
};

export const PUT = async (event: RequestEvent) => {
	const userId = requireUserId(event);
	try {
		await db.initDb();
		const body = await event.request.json();
		const encryptedWeft = typeof body.weftCode === 'string' ? encryptWeftCode(body.weftCode) : body.weftCode;
		const project = await db.updateProject(event.params.id!, userId, {
			name: body.name,
			description: body.description,
			weftCode: encryptedWeft,
			loomCode: body.loomCode,
			layoutCode: body.layoutCode,
		});
		if (!project) {
			return json({ error: 'Project not found' }, { status: 404 });
		}
		// Decrypt for response
		if (typeof project.weftCode === 'string') {
			project.weftCode = decryptWeftCode(project.weftCode);
		}
		return json(project);
	} catch (error) {
		console.error('Failed to update project:', error);
		return json({ error: 'Failed to update project' }, { status: 500 });
	}
};

export const PATCH = async (event: RequestEvent) => {
	const userId = requireUserId(event);
	try {
		await db.initDb();
		await db.touchProjectOpened(event.params.id!, userId);
		return json({ ok: true });
	} catch (error) {
		console.error('Failed to touch project opened:', error);
		return json({ error: 'Failed to touch project' }, { status: 500 });
	}
};

export const DELETE = async (event: RequestEvent) => {
	const userId = requireUserId(event);
	try {
		await db.initDb();
		const result = await db.deleteProject(event.params.id!, userId);
		if (result.ok) {
			return json({ deleted: true });
		}
		switch (result.reason) {
			case 'not_found':
				return json({ error: 'Project not found' }, { status: 404 });
			case 'has_deployments':
				return json(
					{
						error: `This project has ${result.deploymentCount} active deployment${result.deploymentCount === 1 ? '' : 's'}. Unpublish them first.`,
					},
					{ status: 409 },
				);
			case 'cleanup_failed':
				return json(
					{ error: 'Failed to delete project', detail: result.detail },
					{ status: 500 },
				);
			default: {
				// Exhaustiveness guard: if `DeleteProjectResult` ever
				// grows a new `reason` variant, this assignment will
				// fail to compile and remind us to handle it here.
				// Without it, TypeScript's `switch` would silently
				// fall through to `undefined` and SvelteKit would 500
				// with no useful error message.
				const _exhaustive: never = result;
				console.error('Unhandled deleteProject result variant:', _exhaustive);
				return json({ error: 'Unhandled deletion result' }, { status: 500 });
			}
		}
	} catch (error) {
		console.error('Failed to delete project:', error);
		return json({ error: 'Failed to delete project' }, { status: 500 });
	}
};
