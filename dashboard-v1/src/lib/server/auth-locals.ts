import { error } from '@sveltejs/kit';
import type { RequestEvent } from '@sveltejs/kit';

/**
 * Pull the authenticated user id off `event.locals.user` or throw a
 * 401 error. The auth middleware in `hooks.server.ts` populates
 * `locals.user` for every route under `/api/*` that's not in the
 * public allowlist (validate-token, ext, dashboard-token), so this
 * helper should always succeed when called from a protected route.
 *
 * If the route is somehow exposed without going through the
 * middleware (e.g. a future contributor adds a new route and forgets
 * to add it to `requiresJwtAuth`), this throws 401 instead of
 * silently letting the request through. Defense in depth.
 */
export function requireUserId(event: RequestEvent): string {
	const user = event.locals.user;
	if (!user?.id) {
		throw error(401, 'Authentication required');
	}
	return user.id;
}
