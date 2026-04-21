import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { getApiUrl } from '$lib/server/api-url';

export const POST: RequestHandler = async ({ params, request }) => {
	const { token, actionId } = params;
	const backendUrl = getApiUrl();
	
	try {
		const response = await fetch(`${backendUrl}/ext/${token}/actions/${actionId}/dismiss`, {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
		});
		if (!response.ok) {
			const text = await response.text().catch(() => response.statusText);
			return json({ error: text }, { status: response.status });
		}
		const data = await response.json();
		return json(data);
	} catch (error) {
		console.error('Failed to proxy action dismissal:', error);
		return json({ error: 'Failed to dismiss action' }, { status: 502 });
	}
};
