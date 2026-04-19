import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { env } from '$env/dynamic/private';

function getBackendUrl(): string {
	return env.TRIGGER_API_URL || env.API_URL || 'http://localhost:3000';
}

export const POST: RequestHandler = async ({ params, fetch }) => {
	const { token } = params;
	const backendUrl = getBackendUrl();

	try {
		const response = await fetch(`${backendUrl}/ext/${token}/cleanup/all`, { method: 'POST' });
		const text = await response.text();
		return new Response(text, {
			status: response.status,
			headers: { 'content-type': response.headers.get('content-type') ?? 'application/json' },
		});
	} catch (error) {
		console.error('Failed to proxy cleanup-all request:', error);
		return json({ error: 'Failed to clean up tasks' }, { status: 502 });
	}
};
