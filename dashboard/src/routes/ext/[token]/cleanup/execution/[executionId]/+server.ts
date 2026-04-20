import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { getApiUrl } from '$lib/server/api-url';

// Dashboard proxies to the API (weft-api in local mode, cloud-api in cloud mode).
// Production chain: extension -> website /api/ext/* -> dashboard /ext/* -> weft-api.
export const POST: RequestHandler = async ({ params, fetch }) => {
	const { token, executionId } = params;
	const apiUrl = getApiUrl();

	try {
		const response = await fetch(
			`${apiUrl}/ext/${token}/cleanup/execution/${encodeURIComponent(executionId!)}`,
			{ method: 'POST' },
		);
		if (!response.ok) {
			const text = await response.text().catch(() => response.statusText);
			return json({ error: text }, { status: response.status });
		}
		const data = await response.json();
		return json(data);
	} catch (e) {
		console.error('Failed to clean up tasks for execution:', e);
		return json({ error: 'Failed to connect to API' }, { status: 502 });
	}
};
