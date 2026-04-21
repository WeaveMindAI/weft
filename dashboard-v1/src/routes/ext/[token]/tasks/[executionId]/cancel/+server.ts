import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { getApiUrl } from '$lib/server/api-url';

// Extension task cancel endpoint
// Dashboard proxies to the API (weft-api in local mode, cloud-api in cloud mode)
export const POST: RequestHandler = async ({ params, fetch }) => {
	const token = params.token;
	const executionId = params.executionId;
	const apiUrl = getApiUrl();
	
	try {
		const response = await fetch(`${apiUrl}/ext/${token}/tasks/${executionId}/cancel`, {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
		});
		if (!response.ok) {
			const text = await response.text().catch(() => response.statusText);
			return json({ error: text }, { status: response.status });
		}
		const data = await response.json();
		return json(data);
	} catch (e) {
		console.error('Failed to cancel extension task:', e);
		return json({ error: 'Failed to connect to API' }, { status: 502 });
	}
};
