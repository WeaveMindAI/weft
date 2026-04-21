import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { env } from '$env/dynamic/private';

// Proxy extension API calls to the backend
// This allows the extension to call the dashboard URL instead of the backend directly

function getBackendUrl(): string {
	return env.TRIGGER_API_URL || env.API_URL || 'http://localhost:3000';
}

export const GET: RequestHandler = async ({ params, fetch }) => {
	const { token } = params;
	const backendUrl = getBackendUrl();
	
	try {
		const response = await fetch(`${backendUrl}/ext/${token}/tasks`);
		if (!response.ok) {
			const text = await response.text().catch(() => response.statusText);
			return json({ error: text }, { status: response.status });
		}
		const data = await response.json();
		return json(data);
	} catch (error) {
		console.error('Failed to proxy tasks request:', error);
		return json({ error: 'Failed to fetch tasks' }, { status: 502 });
	}
};
