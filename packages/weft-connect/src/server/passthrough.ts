// The website's side of every weft call its pages make. A page calls its
// own server at `/weft/...` (`WEFT_PASS_THROUGH_PATH`), and this passes the
// call on to the dispatcher, whose address lives only in the server's
// environment. The browser therefore needs to reach nothing but the site:
// a dispatcher on a private address, or on a loopback port another machine
// cannot see (a Windows browser in front of a WSL install), still works.
//
// It forwards only the doors a visitor's browser uses, and only what that
// browser may say there: its own token (`Authorization`, or
// `Weft-Instance-Token` on a live call), the body and its type. The site's
// cookies stay behind, and so does `Weft-Instance`, the header only the
// site's server may send to name an instance.

import { trimTrailingSlashes } from '../core/url';

/** The first path segment of each door a page may reach through here: the
 *  instance door, a signal's fire / skip / cancel door, the instance or
 *  api token's listings (signals, displays, files), the program's own live
 *  routes (`connect/<tenant>/<path>`), which a browser calls with its
 *  instance token, a field's provider chooser (`access/picker/<state>` and the
 *  `/result` it posts; nothing else under `access` passes), and a stored
 *  file's link (`public/files/<token>`: a route's answer names a picture by
 *  a link built on the site's address, so the picture comes back through
 *  here; nothing else under `public` passes).
 *  SYNC: PASSED_DOORS <-> crates/weft-dispatcher/src/api/mod.rs outside_caller_routes, crates/weft-dispatcher/src/api/instance_door.rs */
export const PASSED_DOORS = ['instance', 'signal', 'signal-token', 'connect', 'access', 'public'] as const;

/** The door of the program's live routes. The dispatcher answers a call
 *  there with a redirect to the worker serving the run, an address the
 *  browser may not reach either, so this server follows it itself; the
 *  body is read up front because a redirect sends it again. */
const ROUTE_DOOR = 'connect';

// The request headers that travel on. Anything else (the site's cookies,
// its own auth, `Weft-Instance`) stays on the site.
// SYNC: 'weft-instance-token' <-> src/core/transport.ts INSTANCE_TOKEN_HEADER
const PASSED_REQUEST_HEADERS = ['authorization', 'content-type', 'accept', 'weft-instance-token'];

// The response headers that travel back: what the body is, how long it may
// be kept, and where a redirect points (the dispatcher builds that address
// from the forwarded headers below, so it is the site's own).
const PASSED_RESPONSE_HEADERS = ['content-type', 'content-length', 'cache-control', 'content-disposition', 'location'];

// The headers that tell the dispatcher which address the BROWSER used, so a
// link it hands back (an instance's file chooser page) points at the site, not
// at the site server's own view of the dispatcher.
// The scheme goes twice: Cloud Run's front end, between this server and a
// dispatcher on Cloud Run, replaces `x-forwarded-proto` with its own https,
// and weft's own name for it is the one that survives.
// SYNC: the forwarded headers <-> crates/weft-core/src/net.rs request_base_url, WEFT_FORWARDED_PROTO
const FORWARDED_HOST = 'x-forwarded-host';
const FORWARDED_PROTO = 'x-forwarded-proto';
const WEFT_FORWARDED_PROTO = 'x-weft-forwarded-proto';
const FORWARDED_PREFIX = 'x-forwarded-prefix';

export interface PassThroughOptions {
	/** The dispatcher's address as the site's SERVER reaches it (for a local
	 *  install `http://127.0.0.1:14111`), read on every call, so a function
	 *  reading the environment is fine. */
	dispatcher: string | (() => string | undefined);
	/** Defaults to the global `fetch`. */
	fetcher?: typeof fetch;
}

/** Pass one call on to the dispatcher. `path` is what follows the mount
 *  point (`instance/fields` for `/weft/instance/fields`), the query string is
 *  read off `request`. A path outside the weft doors answers 404 without
 *  reaching the dispatcher; a dispatcher that cannot be reached answers
 *  502 naming the address tried. */
export function weftPassThrough(options: PassThroughOptions): (request: Request, path: string) => Promise<Response> {
	const fetcher = options.fetcher ?? ((...args) => fetch(...args));
	return async (request, path) => {
		const base = (typeof options.dispatcher === 'function' ? options.dispatcher() : options.dispatcher)?.trim();
		if (!base) {
			return new Response(
				"weft pass-through: the dispatcher's address is not set in the server environment (WEFT_DISPATCHER_URL)",
				{ status: 500 },
			);
		}
		const segments = path.split('/').filter((s) => s !== '');
		const refused = refusal(segments);
		if (refused) return new Response(`weft pass-through: ${refused}`, { status: 404 });

		const url = new URL(request.url);
		const target = `${trimTrailingSlashes(base)}/${segments.map(encodeURIComponent).join('/')}${url.search}`;
		const headers = new Headers();
		for (const name of PASSED_REQUEST_HEADERS) {
			const value = request.headers.get(name);
			if (value !== null) headers.set(name, value);
		}
		// Built from the site's own request, never passed through: a
		// browser's `X-Forwarded-*` would let it choose the links it is given.
		headers.set(FORWARDED_HOST, url.host);
		headers.set(FORWARDED_PROTO, url.protocol.replace(/:$/, ''));
		headers.set(WEFT_FORWARDED_PROTO, url.protocol.replace(/:$/, ''));
		headers.set(FORWARDED_PREFIX, mountOf(url.pathname, segments.length));
		const hasBody = request.method !== 'GET' && request.method !== 'HEAD';
		const route = segments[0] === ROUTE_DOOR;
		let answer: Response;
		try {
			answer = await fetcher(target, {
				method: request.method,
				headers,
				...(route
					? { body: hasBody ? await request.arrayBuffer() : undefined, redirect: 'follow' }
					: {
							body: hasBody ? request.body : undefined,
							redirect: 'manual',
							// A streamed request body needs this in Node's fetch.
							...(hasBody ? { duplex: 'half' } : {}),
						}),
			} as RequestInit);
		} catch (e) {
			return new Response(
				`weft pass-through: the dispatcher at ${base} did not answer (${failureOf(e)})`,
				{ status: 502 },
			);
		}
		const out = new Headers();
		for (const name of PASSED_RESPONSE_HEADERS) {
			const value = answer.headers.get(name);
			if (value !== null) out.set(name, value);
		}
		return new Response(answer.body, { status: answer.status, statusText: answer.statusText, headers: out });
	};
}

/** What a failed fetch says, with every cause beneath it: Node's fetch
 *  says only "fetch failed", and the reason (a refused connection, a
 *  certificate, a name that does not resolve) is on its `cause`. */
export function failureOf(e: unknown): string {
	const parts: string[] = [];
	let at: unknown = e;
	for (let depth = 0; at !== undefined && at !== null && depth < 5; depth++) {
		if (at instanceof Error) {
			const code = (at as { code?: unknown }).code;
			parts.push(typeof code === 'string' && !at.message.includes(code) ? `${at.message} (${code})` : at.message);
			at = (at as { cause?: unknown }).cause;
		} else {
			parts.push(String(at));
			break;
		}
	}
	return parts.join(': ');
}

/** Where the pass-through is mounted (`/weft`), read off the request's
 *  path by dropping the segments that follow the mount. `''` at the root. */
function mountOf(pathname: string, after: number): string {
	const all = pathname.split('/').filter((s) => s !== '');
	return all.length > after ? `/${all.slice(0, all.length - after).join('/')}` : '';
}

/** Why a path may not pass, or `null` when it may. */
function refusal(segments: string[]): string | null {
	if (segments.length === 0 || !(PASSED_DOORS as readonly string[]).includes(segments[0])) {
		return `only ${PASSED_DOORS.map((d) => `/${d}/`).join(', ')} pass through here, not /${segments.join('/')}`;
	}
	if (segments.some((s) => s === '.' || s === '..')) {
		return `a path climbing out of its door is refused: /${segments.join('/')}`;
	}
	if (segments[0] === 'access' && !isPickerDoor(segments)) {
		return `under /access/ only a chooser page (/access/picker/<state>) and its /result pass, not /${segments.join('/')}`;
	}
	if (segments[0] === 'public' && !(segments.length === 3 && segments[1] === 'files')) {
		return `under /public/ only a file link (/public/files/<token>) passes, not /${segments.join('/')}`;
	}
	return null;
}

/** `access/picker/<state>` or `access/picker/<state>/result`. `begin` is the
 *  author's door that opens a chooser, not a chooser. */
function isPickerDoor(segments: string[]): boolean {
	const [, picker, state, result] = segments;
	if (picker !== 'picker' || !state || state === 'begin') return false;
	return segments.length === 3 || (segments.length === 4 && result === 'result');
}
