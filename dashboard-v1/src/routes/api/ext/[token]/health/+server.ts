import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { env } from '$env/dynamic/private';

// Proxy health check to the backend

function getBackendUrl(): string {
	return env.TRIGGER_API_URL || env.API_URL || 'http://localhost:3000';
}

export const GET: RequestHandler = async ({ params, fetch }) => {
	const { token } = params;
	const backendUrl = getBackendUrl();
	
	try {
		const response = await fetch(`${backendUrl}/ext/${token}/health`);
		if (!response.ok) {
			const text = await response.text().catch(() => response.statusText);
			return json({ error: text }, { status: response.status });
		}
		const data = await response.json();
		return json(data);
	} catch (error) {
		console.error('Failed to proxy health check:', error);
		return json({ error: 'Failed to check health' }, { status: 502 });
	}
};
