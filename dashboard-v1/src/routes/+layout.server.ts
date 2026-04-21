import { env } from '$env/dynamic/private';
import type { ServerLoad } from '@sveltejs/kit';

// DEPLOYMENT_MODE: "local" (default) or "cloud"
// In cloud mode, token validation happens client-side via postMessage
// In local mode, we accept user_id/username directly (no auth required)
const isCloudMode = env.DEPLOYMENT_MODE === 'cloud';

export const load: ServerLoad = async ({ url }) => {
	const userId = url.searchParams.get('user_id');
	const username = url.searchParams.get('username');
	
	// LOCAL MODE: Accept user_id and username directly
	if (!isCloudMode) {
		return {
			userId: userId || null,
			username: username || null,
			isCloudMode: false,
		};
	}
	
	// CLOUD MODE: Token validation happens client-side via postMessage
	// Server just tells the client we're in cloud mode
	return {
		userId: null,
		username: null,
		isCloudMode: true,
	};
};
