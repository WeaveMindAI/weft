import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { env } from '$env/dynamic/private';

function getBackendUrl(): string {
	return env.TRIGGER_API_URL || env.API_URL || 'http://localhost:3000';
}

export const POST: RequestHandler = async ({ params, request, fetch }) => {
	const { token, triggerTaskId } = params;
	const backendUrl = getBackendUrl();

	try {
		const body = await request.json();
		const response = await fetch(`${backendUrl}/ext/${token}/triggers/${triggerTaskId}/submit`, {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify(body),
		});
		if (!response.ok) {
			const text = await response.text().catch(() => response.statusText);
			return json({ error: text }, { status: response.status });
		}
		const data = await response.json();
		return json(data);
	} catch (error) {
		console.error('Failed to proxy trigger submission:', error);
		return json({ error: 'Failed to submit trigger' }, { status: 502 });
	}
};
