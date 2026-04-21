import type { LayoutLoad } from './$types';

export const load: LayoutLoad = ({ url, data }) => {
	// User info comes from server-side validation
	// In cloud mode: from validated JWT token via postMessage
	// In local mode: from URL params (passed through by server)
	const apiUrl = url.searchParams.get('api_url');
	const websiteUrl = url.searchParams.get('website_url');
	const initialPath = url.searchParams.get('path');
	
	// Fallback to URL params if server didn't provide (backward compat for local mode)
	const userId = (data as any)?.userId || url.searchParams.get('user_id');
	const username = (data as any)?.username || url.searchParams.get('username');
	const isCloudMode = (data as any)?.isCloudMode ?? false;
	
	return {
		userId,
		username,
		apiUrl,
		websiteUrl,
		initialPath,
		isCloudMode,
	};
};
