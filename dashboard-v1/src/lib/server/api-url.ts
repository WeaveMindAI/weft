/**
 * Resolves the weft-api base URL for server-side HTTP calls.
 *
 * The dashboard's SvelteKit server has several handlers that forward
 * to weft-api (project deletion, extension tasks, etc.). Each one
 * historically read `env.API_URL` directly and fell back to
 * `http://localhost:3000`. That's correct in local dev but silently
 * wrong in any deployed environment without `API_URL` set: the
 * handler points at localhost, fails to reach weft-api, and the
 * caller sees "success" because the best-effort call swallowed the
 * error.
 *
 * This helper centralizes the lookup. In production mode
 * (`NODE_ENV === 'production'` or `DEPLOYMENT_MODE === 'cloud'`),
 * a missing `API_URL` is a hard failure and the caller should
 * surface a 500. In local/dev mode we fall back to localhost as
 * before. Routing every caller through this helper means a
 * misconfiguration is caught at the first request instead of
 * silently mis-routing trigger cleanups into the void.
 */

import { env } from '$env/dynamic/private';

const DEFAULT_LOCAL_API_URL = 'http://localhost:3000';

/** Return the configured weft-api base URL. Throws in production
 *  when the env var is missing so a misconfigured deploy fails loud
 *  at the first call site.
 *
 *  Production detection: `DEPLOYMENT_MODE === 'cloud'` is the
 *  authoritative signal. It's an application env var we control and
 *  set in every cloud deploy. `NODE_ENV === 'production'` is kept as
 *  a convenience fallback because Node sets it by default in most
 *  production bundlers, but SvelteKit's dynamic env adapter may or
 *  may not forward it depending on the host, so we don't rely on it
 *  alone. If neither is set we treat the environment as dev and
 *  fall back to localhost.
 */
export function getApiUrl(): string {
	const configured = env.API_URL?.trim();
	if (configured) return configured;

	const isProduction =
		env.DEPLOYMENT_MODE === 'cloud' || env.NODE_ENV === 'production';
	if (isProduction) {
		throw new Error(
			'API_URL is not set. The dashboard cannot reach weft-api without it. ' +
				'Set API_URL in your environment (e.g. https://api.weavemind.ai) ' +
				'or configure DEPLOYMENT_MODE=local for dev.',
		);
	}
	return DEFAULT_LOCAL_API_URL;
}
