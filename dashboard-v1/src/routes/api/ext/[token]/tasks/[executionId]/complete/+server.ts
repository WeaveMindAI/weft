import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { env } from '$env/dynamic/private';

// Proxy task completion to the backend

function getBackendUrl(): string {
	return env.TRIGGER_API_URL || env.API_URL || 'http://localhost:3000';
}

export const POST: RequestHandler = async ({ params, request, fetch }) => {
	const { token, executionId } = params;
	const backendUrl = getBackendUrl();
	
	try {
		const body = await request.json();
		const response = await fetch(`${backendUrl}/ext/${token}/tasks/${executionId}/complete`, {
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
		console.error('Failed to proxy task completion:', error);
		return json({ error: 'Failed to complete task' }, { status: 502 });
	}
};
