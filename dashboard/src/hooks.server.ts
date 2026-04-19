import type { Handle } from "@sveltejs/kit";
import { env } from "$env/dynamic/private";
import { json } from "@sveltejs/kit";
import { timingSafeEqual } from "node:crypto";
import { verifyDashboardToken } from "$lib/server/verify-token";
import { getExecutionOwnerInternal } from "$lib/server/db";

/// Constant-time compare to avoid leaking the internal API key via response-time
/// oracles. Plain string === is observably timing-different at every byte.
function safeKeyEqual(a: string, b: string): boolean {
	const ab = Buffer.from(a);
	const bb = Buffer.from(b);
	if (ab.length !== bb.length) return false;
	return timingSafeEqual(ab, bb);
}

/** Local mode sentinel identity. The dashboard runs single-user in
 *  OSS standalone, so when no JWT infrastructure is available we
 *  attribute every request to a fixed `local` user. The `users` table
 *  doesn't need a real row; every db.ts function just filters by
 *  this id and the local install's data is created with the same id
 *  by `createProject` callers. Matches the convention used by
 *  weft-api and cloud-api. */
const LOCAL_USER = { id: "local", username: "local" } as const;

/** Path prefixes under `/api/` that bypass JWT auth. These are either
 *  the auth handshake itself or have their own token-based auth that
 *  weft-api validates downstream. */
const PUBLIC_API_PREFIXES = [
	"/api/validate-token",
	"/api/ext/",
	"/api/dashboard-token",
] as const;

function requiresJwtAuth(pathname: string): boolean {
	if (!pathname.startsWith("/api/")) return false;
	for (const prefix of PUBLIC_API_PREFIXES) {
		if (pathname.startsWith(prefix)) return false;
	}
	return true;
}

const MAINTENANCE_PAGE = `<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="utf-8" />
	<meta name="viewport" content="width=device-width, initial-scale=1" />
	<title>WeaveMind - Maintenance</title>
	<style>
		* { margin: 0; padding: 0; box-sizing: border-box; }
		body {
			font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
			background: #0a0a0a;
			color: #e5e5e5;
			min-height: 100vh;
			display: flex;
			align-items: center;
			justify-content: center;
		}
		.container {
			text-align: center;
			max-width: 480px;
			padding: 2rem;
		}
		h1 {
			font-size: 1.5rem;
			font-weight: 600;
			margin-bottom: 1rem;
			color: #fff;
		}
		p {
			font-size: 1rem;
			line-height: 1.6;
			color: #a3a3a3;
		}
		.dot {
			display: inline-block;
			width: 8px;
			height: 8px;
			background: #f59e0b;
			border-radius: 50%;
			margin-bottom: 1.5rem;
			animation: pulse 2s ease-in-out infinite;
		}
		@keyframes pulse {
			0%, 100% { opacity: 1; }
			50% { opacity: 0.3; }
		}
	</style>
</head>
<body>
	<div class="container">
		<div class="dot"></div>
		<h1>We're updating WeaveMind</h1>
		<p>This should only take a few minutes. Please check back shortly.</p>
	</div>
</body>
</html>`;

// Initialize database on startup (only if DATABASE_URL is set - local mode)
let dbInitialized = false;

// Allowed origins for embedding the dashboard
// In production, only the website can embed the dashboard
// Locally, allow direct access
const getAllowedOrigins = (): string[] => {
	const origins: string[] = [];
	
	// Local development origins (only in dev mode)
	if (!isProductionMode()) {
		origins.push("http://localhost:5173");
		origins.push("http://localhost:5174");
	}
	
	// Add production website origin if configured
	if (env.ALLOWED_ORIGINS) {
		origins.push(...env.ALLOWED_ORIGINS.split(",").map(o => o.trim()));
	}
	
	return origins;
};

// Check if we're in production mode (ALLOWED_ORIGINS is set and ALLOW_DIRECT_ACCESS is not set)
const isProductionMode = (): boolean => {
	return !!env.ALLOWED_ORIGINS && env.ALLOW_DIRECT_ACCESS !== "true";
};

export const handle: Handle = async ({ event, resolve }) => {
	// Maintenance mode: return static page for all requests
	if (env.MAINTENANCE_MODE === 'true') {
		return new Response(MAINTENANCE_PAGE, {
			status: 503,
			headers: {
				'Content-Type': 'text/html; charset=utf-8',
				'Retry-After': '300',
			}
		});
	}

	// ── CORS for browser extension API routes ──
	//
	// The browser extension (Chrome/Firefox/etc) runs from its own
	// origin (`chrome-extension://<id>`) and fetches the dashboard's
	// `/api/ext/*` proxy routes over the network. Without
	// `Access-Control-Allow-Origin` on the response, the browser
	// blocks the response body even though the server returned 200,
	// and the extension sees a CORS error.
	//
	// In the cloud deployment, the website (on the main app origin)
	// owns the `/api/ext/*` surface and sets CORS headers in its own
	// hooks.server.ts. But in OSS local mode, the extension hits the
	// dashboard directly at `http://localhost:5174/api/ext/*`, so
	// the dashboard has to set the same CORS headers itself.
	//
	// Extension API routes allow `*` (anonymous; auth is via the
	// opaque token in the URL path, not cookies) and don't need
	// credentials. We also handle the preflight `OPTIONS` request
	// here so the browser lets the real request through.
	const isExtensionApi = event.url.pathname.startsWith('/api/ext/');
	if (isExtensionApi && event.request.method === 'OPTIONS') {
		return new Response(null, {
			status: 204,
			headers: {
				'Access-Control-Allow-Origin': '*',
				'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
				'Access-Control-Allow-Headers': 'Content-Type',
				'Access-Control-Allow-Credentials': 'false',
			},
		});
	}

	if (!dbInitialized && env.DATABASE_URL) {
		try {
			const { initDb } = await import("$lib/server/db");
			await initDb();
			dbInitialized = true;
			console.log("Database initialized successfully");
		} catch (error) {
			console.error("Failed to initialize database:", error);
		}
	}
	
	// In production mode, block direct access to the dashboard
	// Only allow access from allowed origins (via iframe or with proper referer)
	if (isProductionMode()) {
		const allowedOrigins = getAllowedOrigins();
		const referer = event.request.headers.get("referer");
		const origin = event.request.headers.get("origin");
		const secFetchDest = event.request.headers.get("sec-fetch-dest");
		
		// Allow API requests, extension routes, assets, playground, and iframe requests
		const isApiRequest = event.url.pathname.startsWith("/api/");
		const isExtensionRequest = event.url.pathname.startsWith("/ext/");
		const isPlaygroundRequest = event.url.pathname.startsWith("/playground");
		const isAssetRequest = event.url.pathname.startsWith("/_app/") ||
			event.url.pathname.startsWith("/favicon") ||
			event.url.pathname.endsWith(".js") ||
			event.url.pathname.endsWith(".css") ||
			event.url.pathname.endsWith(".png") ||
			event.url.pathname.endsWith(".svg");
		const isIframeRequest = secFetchDest === "iframe";
		
		// For page requests (not API/assets/extension), check if it's from an allowed origin
		if (!isApiRequest && !isExtensionRequest && !isAssetRequest && !isPlaygroundRequest) {
			let isAllowed = false;
			
			// Check if request is from an iframe embedded in allowed origin
			if (isIframeRequest) {
				// For iframe requests, check the origin or referer
				const requestOrigin = origin || (referer ? new URL(referer).origin : null);
				if (requestOrigin && allowedOrigins.includes(requestOrigin)) {
					isAllowed = true;
				}
			}
			
			// Check referer for navigation within the dashboard
			if (referer) {
				const refererOrigin = new URL(referer).origin;
				// Allow if referer is from the dashboard itself or an allowed origin
				if (refererOrigin === event.url.origin || allowedOrigins.includes(refererOrigin)) {
					isAllowed = true;
				}
			}
			
			// Direct access in production: redirect to the website with the correct path
			// This handles "right-click > open in new tab" from inside the iframe
			if (!isAllowed && !referer) {
				const websiteOrigin = env.ALLOWED_ORIGINS?.split(",")[0]?.trim();
				if (websiteOrigin) {
					const dashboardPath = event.url.pathname;
					return new Response(null, {
						status: 302,
						headers: { Location: `${websiteOrigin}/app#${dashboardPath}` },
					});
				}
				return new Response("Dashboard must be accessed through the WeaveMind website", {
					status: 403,
					headers: { "Content-Type": "text/plain" }
				});
			}
		}
	}
	
	// ── Auth: populate event.locals.user for protected /api/* routes ──
	//
	// Historically the dashboard's SvelteKit `/api/projects/*` routes
	// took the project id from the URL and trusted it without any
	// per-user filtering. Combined with the iframe origin check above
	// being EXPLICITLY skipped for `/api/*`, that exposed every
	// project to anonymous cross-user privilege escalation in cloud
	// production: anyone could `curl https://app.weavemind.ai/api/projects/<uuid>`
	// to read, write, or delete an arbitrary project.
	//
	// Fix: every `/api/*` route except the small public allowlist
	// requires a valid dashboard JWT in production. The JWT is the
	// same one the website mints and postMessages into the iframe
	// (see `weavemind/website/src/lib/server/dashboard-token.ts`).
	// The client-side `authFetch` in `lib/config.ts` attaches it as
	// `Authorization: Bearer <token>` from sessionStorage.
	//
	// In local OSS mode (no `ALLOWED_ORIGINS`) we inject a fixed
	// `local` user so single-user dev keeps working without a JWT
	// infrastructure. Db functions filter by this id, matching the
	// data created by local-mode handlers.
	if (requiresJwtAuth(event.url.pathname)) {
		// Internal service-to-service calls (currently: orchestrator
		// status callbacks POSTing to /api/executions/{id}) authenticate
		// with x-internal-api-key instead of a JWT. When the header is
		// present and valid, we attribute the request to the execution's
		// owner so the db.updateExecution WHERE clause resolves. Scoped
		// to /api/executions/* on purpose: every other /api/* route is
		// genuinely user-driven and has a session.
		const internalKey = event.request.headers.get("x-internal-api-key");
		const executionPathMatch = event.url.pathname.match(/^\/api\/executions\/([^/]+)(?:\/|$)/);
		// Scope the internal-key bypass to writes only (PUT/POST). The
		// orchestrator's status callback is always POST; allowing GET would
		// turn the internal key into a read-any-execution credential that
		// skips the per-user filter in getExecution.
		const isWriteMethod = event.request.method === "POST" || event.request.method === "PUT";
		if (
			isWriteMethod
			&& internalKey
			&& env.INTERNAL_API_KEY
			&& env.INTERNAL_API_KEY.length >= 16
			&& safeKeyEqual(internalKey, env.INTERNAL_API_KEY)
			&& executionPathMatch
		) {
			const executionId = executionPathMatch[1];
			const ownerId = await getExecutionOwnerInternal(executionId);
			if (!ownerId) {
				return json({ error: "Execution not found" }, { status: 404 });
			}
			event.locals.user = { id: ownerId, username: "internal" };
			return await resolve(event);
		}

		// Try to decode an Authorization Bearer JWT first, regardless
		// of mode. This means a local dev environment that happens to
		// be running the website + dashboard together (so the iframe
		// receives a real JWT via postMessage) gets the real user id
		// from the token and operates against the same data the
		// website's session sees. The local-mode `local` sentinel is
		// only used as a fallback when no JWT is present at all.
		const authHeader = event.request.headers.get("authorization");
		const token = authHeader?.startsWith("Bearer ")
			? authHeader.slice("Bearer ".length).trim()
			: "";

		if (token) {
			const result = await verifyDashboardToken(token);
			if (!result.valid || !result.payload) {
				return json(
					{ error: result.error || "Invalid token" },
					{ status: 401 },
				);
			}
			event.locals.user = {
				id: result.payload.user_id,
				username: result.payload.username,
			};
		} else if (!isProductionMode()) {
			// No JWT and we're in local mode: inject the OSS
			// standalone sentinel so single-user dev keeps working
			// without any auth infrastructure.
			event.locals.user = { ...LOCAL_USER };
		} else {
			// Production mode requires a JWT.
			return json(
				{ error: "Authentication required" },
				{ status: 401 },
			);
		}
	}

	const response = await resolve(event);

	// CORS response header for the non-preflight extension API calls.
	// The OPTIONS preflight was already handled at the top of this
	// function; here we tag the actual GET/POST/DELETE response so
	// the browser allows the extension to read the body.
	if (isExtensionApi) {
		response.headers.set('Access-Control-Allow-Origin', '*');
		response.headers.set('Access-Control-Allow-Credentials', 'false');
	}

	// Add security headers for embedding control
	if (env.ALLOWED_ORIGINS) {
		const allowedOrigins = getAllowedOrigins();
		const frameAncestors = allowedOrigins.join(" ");
		// Playground routes can be embedded from the landing page too
		if (event.url.pathname.startsWith("/playground")) {
			const isDev = !isProductionMode();
			const localDev = isDev ? ' http://localhost:*' : '';
			response.headers.set("Content-Security-Policy", `frame-ancestors 'self' ${frameAncestors} https://weavemind.ai${localDev}`);
		} else {
			response.headers.set("Content-Security-Policy", `frame-ancestors 'self' ${frameAncestors}`);
		}
	}
	
	return response;
};
