import { json, error } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { verifyDashboardToken } from '$lib/server/verify-token';

export const POST: RequestHandler = async ({ request }) => {
	const { token } = await request.json();
	
	if (!token) {
		throw error(400, { message: 'Token required' });
	}
	
	const result = await verifyDashboardToken(token);
	
	if (!result.valid || !result.payload) {
		throw error(403, { message: result.error || 'Invalid token' });
	}
	
	return json({
		userId: result.payload.user_id,
		username: result.payload.username,
		email: result.payload.email,
		name: result.payload.name,
	});
};
