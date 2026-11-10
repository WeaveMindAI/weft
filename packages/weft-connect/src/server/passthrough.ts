// The website's side of every weft call its pages make. A page calls its
// own server at `/weft/...` (`WEFT_PASS_THROUGH_PATH`), and this passes the
// call on to the dispatcher, whose address lives only in the server's
// environment. The browser therefore needs to reach nothing but the site:
// a dispatcher on a private address, or on a loopback port another machine
// cannot see (a Windows browser in front of a WSL install), still works.
//
// It forwards only the doors a visitor's browser uses, and only what that
// browser may say there: its own token (`Authorization`, or
// `Weft-Instance-Token` on a live call), the body and its type, and what a
// read or write of a stored file asks (a range, a version it holds). The
// site's cookies stay behind, and so does `Weft-Instance`, the header only
// the site's server may send to name an instance.
//
// Apart from it, `weftProgramPassThrough` passes a site's ADMIN page on to
// the program door, adding the connections key the server holds. It is
// mounted on its own route (`WEFT_PROGRAM_PASS_THROUGH_PATH`), and every call
// carries the answer of the site's own admin check, which it reads first:
// whoever reaches that route can replace the program's keys, and weft cannot
// tell a site's admin from its visitors.

import { COMPLETING_HEADER } from '../core/upload';
import { trimTrailingSlashes } from '../core/url';

/** The first path segment of each door a page may reach through here: the
 *  instance door, a signal's fire / skip / cancel door, the instance or
 *  api token's listings (signals, displays, files), the program's own live
 *  routes (`connect/<tenant>/<project id>/<path>`), which a browser calls with its
 *  instance token, a field's provider chooser (`access/picker/<state>` and the
 *  `/result` it posts; nothing else under `access` passes), and the files
 *  door: a stored file's link (`files/runtime/<key>?<signature>`, read with
 *  GET or HEAD, or a part of an upload sent with PUT) and an upload's steps
 *  (`files/upload/<verb>`, POST). A project whose visitor address is the
 *  site's own signs its file links for the site's host, so they come back
 *  through here; nothing else under `files` passes.
 *  SYNC: PASSED_DOORS <-> crates/weft-dispatcher/src/api/mod.rs outside_caller_routes, crates/weft-dispatcher/src/api/instance_door.rs, crates/weft-platform-traits/src/object_store.rs FILES_DOOR */
export const PASSED_DOORS = ['instance', 'signal', 'signal-token', 'connect', 'access', 'files'] as const;

// The request headers that travel on: the caller's token, what the body is,
// and what a stored file's read or write asks for (a range of it, only if it
// is still the version the browser holds, nothing if that version has not
// changed, or a write only over the version named). Anything else (the
// site's cookies, its own auth, `Weft-Instance`) stays on the site.
// SYNC: 'weft-instance-token' <-> src/core/transport.ts INSTANCE_TOKEN_HEADER
// SYNC: the stored file's headers (content-type, range, if-range, if-none-match, if-modified-since, if-match) <-> crates/weft-dispatcher/src/files_door.rs PASSED_REQUEST_HEADERS.
//   The intended differences: the files door also passes `content-length` as it came, where here the length
//   is the one fetch writes for the buffered part (`partBody`); `authorization`, `accept` and
//   `weft-instance-token` are for the dispatcher's other doors, and the files door passes none of them on.
const PASSED_REQUEST_HEADERS = [
	'authorization',
	'content-type',
	'accept',
	'weft-instance-token',
	'range',
	'if-range',
	'if-none-match',
	'if-modified-since',
	'if-match',
];

/** The largest part of an upload weft signs a link for, which is the most
 *  a part PUT through here may carry: a part is held whole before it goes
 *  on, so a bigger one is refused before its bytes are read. Parts are
 *  8 MiB, growing in whole MiB for a file that would otherwise take more
 *  than 10,000 of them, up to this for the largest object a store keeps
 *  (5 TiB). */
// SYNC: LARGEST_PART_BYTES <-> crates/weft-broker/src/runtime_store.rs part_size_for, MAX_PARTS
export const LARGEST_PART_BYTES = 525 * 1024 * 1024;

/** The header every answer of a route's run carries, success or failure:
 *  the run's id, which the site can log or show beside a failure so the
 *  program's owner can open the run.
 *  SYNC: RUN_ID_HEADER <-> crates/weft-core/src/caller.rs RUN_ID_HEADER */
export const RUN_ID_HEADER = 'Weft-Run-Id';

/** What a route answers when its run failed without answering: the plain
 *  words and the run's id, never the failure itself (the owner reads that
 *  on the run). The body before anything was sent, and the item of a
 *  stream's error marker after.
 *  SYNC: PlainFailure <-> crates/weft-core/src/caller.rs PlainFailure */
export interface PlainFailure {
	error: string;
	runId: string;
}

// The response headers that travel back: what the body is, how long it may
// be kept, where a redirect points (the dispatcher builds that address
// from the forwarded headers below, so it is the site's own), which run
// answered, and what a stored file's read needs: which bytes of it came,
// that ranges may be asked, its version (`etag` is also how an uploaded
// part is reported done), and that a browser never guesses another type
// for it (`x-content-type-options`).
const PASSED_RESPONSE_HEADERS = [
	'content-type',
	'content-length',
	'cache-control',
	'content-disposition',
	'location',
	RUN_ID_HEADER.toLowerCase(),
	'content-range',
	'accept-ranges',
	'etag',
	'last-modified',
	'x-content-type-options',
	COMPLETING_HEADER,
];

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

/** The program door, which `weftProgramPassThrough` alone passes.
 *  SYNC: PROGRAM_DOOR <-> crates/weft-dispatcher/src/api/program_door.rs, crates/weft-dispatcher/src/api/mod.rs outside_caller_routes */
const PROGRAM_DOOR = 'program';

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
		const base = dispatcherOf(options);
		if (base instanceof Response) return base;
		const segments = path.split('/').filter((s) => s !== '');
		const refused = refusal(segments, request.method);
		if (refused) return new Response(`weft pass-through: ${refused}`, { status: 404 });
		return passOn(fetcher, base, request, segments, request.headers.get('authorization'));
	};
}

export interface ProgramPassThroughOptions extends PassThroughOptions {
	/** The connections key (`weft token mint --connections`), read on every
	 *  call like `dispatcher`. It lives in the server's environment only. */
	connectionsKey: string | (() => string | undefined);
}

/** Who is calling the program pass-through, as the site's own admin check
 *  answered for this request. It is asked per call because that check
 *  usually reads what the site attached to the request (SvelteKit's
 *  `locals.user`), which the bare `Request` does not carry. */
export interface ProgramCaller {
	admin: boolean;
}

/** Pass one call of a site's admin page on to the program door, presenting
 *  the connections key in place of anything the page sent. Mount it on its
 *  own route (`WEFT_PROGRAM_PASS_THROUGH_PATH`, `/weft-admin`) and hand every
 *  call the site's own admin check (`caller.admin`): weft cannot tell a
 *  site's admin from its visitors, and this route stores the program's keys.
 *  Anything but `admin: true` answers 403 and reaches nothing. `path` is
 *  what follows the mount point (`program/connections`). Only the program
 *  door passes (anything else answers 404); a key that is not set answers
 *  500. */
export function weftProgramPassThrough(
	options: ProgramPassThroughOptions,
): (request: Request, path: string, caller: ProgramCaller) => Promise<Response> {
	const fetcher = options.fetcher ?? ((...args) => fetch(...args));
	return async (request, path, caller) => {
		// A plain JavaScript site can still leave `caller` out; that is a refusal too.
		if (caller?.admin !== true) {
			return new Response(
				"weft pass-through: the site's admin check refused this call, so it was not passed on (only the site's admin may change the program's connections)",
				{ status: 403 },
			);
		}
		const base = dispatcherOf(options);
		if (base instanceof Response) return base;
		const key = (typeof options.connectionsKey === 'function' ? options.connectionsKey() : options.connectionsKey)?.trim();
		if (!key) {
			return new Response(
				'weft pass-through: the connections key is not set in the server environment (WEFT_CONNECTIONS_KEY)',
				{ status: 500 },
			);
		}
		const segments = path.split('/').filter((s) => s !== '');
		if (segments[0] !== PROGRAM_DOOR || segments.some((s) => s === '.' || s === '..')) {
			return new Response(`weft pass-through: only /${PROGRAM_DOOR}/ passes through here, not /${segments.join('/')}`, {
				status: 404,
			});
		}
		return passOn(fetcher, base, request, segments, `Bearer ${key}`);
	};
}

/** The dispatcher's address, or the answer saying it is not set. */
function dispatcherOf(options: PassThroughOptions): string | Response {
	const base = (typeof options.dispatcher === 'function' ? options.dispatcher() : options.dispatcher)?.trim();
	if (base) return base;
	return new Response(
		"weft pass-through: the dispatcher's address is not set in the server environment (WEFT_DISPATCHER_URL)",
		{ status: 500 },
	);
}

/** Send `request` on to `segments` at the dispatcher `base`, presenting
 *  `authorization` (none when `null`), and hand its answer back. */
async function passOn(
	fetcher: typeof fetch,
	base: string,
	request: Request,
	segments: string[],
	authorization: string | null,
): Promise<Response> {
	const url = new URL(request.url);
	const target = `${trimTrailingSlashes(base)}/${segments.map(encodeSegment).join('/')}${url.search}`;
	const headers = new Headers();
	for (const name of PASSED_REQUEST_HEADERS) {
		const value = request.headers.get(name);
		if (value !== null) headers.set(name, value);
	}
	if (authorization === null) headers.delete('authorization');
	else headers.set('authorization', authorization);
	// Built from the site's own request, never passed through: a
	// browser's `X-Forwarded-*` would let it choose the links it is given.
	headers.set(FORWARDED_HOST, url.host);
	headers.set(FORWARDED_PROTO, url.protocol.replace(/:$/, ''));
	headers.set(WEFT_FORWARDED_PROTO, url.protocol.replace(/:$/, ''));
	headers.set(FORWARDED_PREFIX, mountOf(url.pathname, segments.length));
	const hasBody = request.method !== 'GET' && request.method !== 'HEAD';
	// A part of an upload goes with its exact length: the length is
	// signed into its link, and a streamed body goes out chunked, with
	// none, which the store refuses. Every other body streams.
	let body: ArrayBuffer | ReadableStream<Uint8Array> | null | undefined;
	if (!hasBody) body = undefined;
	else if (isFilePut(request.method, segments)) {
		const part = await partBody(request);
		if (part instanceof Response) return part;
		body = part;
	} else body = request.body;
	let answer: Response;
	try {
		answer = await fetcher(target, {
			method: request.method,
			headers,
			body,
			redirect: 'manual',
			// A streamed request body needs this in Node's fetch.
			...(body instanceof ReadableStream ? { duplex: 'half' } : {}),
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

/** A part of an upload, held whole, or the refusal: a part states its
 *  length, at most `LARGEST_PART_BYTES`, and reading stops the moment the
 *  body runs past it. */
async function partBody(request: Request): Promise<ArrayBuffer | Response> {
	const stated = request.headers.get('content-length');
	if (stated === null || !/^\d+$/.test(stated)) {
		return new Response('weft pass-through: a part of an upload states its length (Content-Length)', { status: 411 });
	}
	const length = Number(stated);
	if (length > LARGEST_PART_BYTES) {
		return new Response(
			`weft pass-through: a part of an upload carries at most ${LARGEST_PART_BYTES} bytes, and this one says ${length}`,
			{ status: 413 },
		);
	}
	const held = new Uint8Array(length);
	let at = 0;
	const reader = request.body?.getReader();
	for (;;) {
		const read = await reader?.read();
		if (!read || read.done) break;
		if (at + read.value.byteLength > length) {
			await reader?.cancel();
			return new Response(`weft pass-through: a part of an upload said ${length} bytes and carried more`, { status: 413 });
		}
		held.set(read.value, at);
		at += read.value.byteLength;
	}
	return held.buffer.slice(0, at);
}

/** One path segment as a URL writes it (RFC 3986): every byte but
 *  `A-Z a-z 0-9 - _ . ~` escaped. A stored file's link is signed over its
 *  key written this way, so a key reaches the install byte for byte as it
 *  was signed; `encodeURIComponent` alone leaves `!'()*` bare. */
// SYNC: encodeSegment <-> crates/weft-core/src/access/spec.rs percent_encode
function encodeSegment(segment: string): string {
	return encodeURIComponent(segment).replace(/[!'()*]/g, (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
}

/** Where the pass-through is mounted (`/weft`), read off the request's
 *  path by dropping the segments that follow the mount. `''` at the root. */
function mountOf(pathname: string, after: number): string {
	const all = pathname.split('/').filter((s) => s !== '');
	return all.length > after ? `/${all.slice(0, all.length - after).join('/')}` : '';
}

/** Why a path may not pass, or `null` when it may. */
function refusal(segments: string[], method: string): string | null {
	if (segments.length === 0 || !(PASSED_DOORS as readonly string[]).includes(segments[0])) {
		return `only ${PASSED_DOORS.map((d) => `/${d}/`).join(', ')} pass through here, not /${segments.join('/')}`;
	}
	if (segments.some((s) => s === '.' || s === '..')) {
		return `a path climbing out of its door is refused: /${segments.join('/')}`;
	}
	if (segments[0] === 'access' && !isPickerDoor(segments)) {
		return `under /access/ only a chooser page (/access/picker/<state>) and its /result pass, not /${segments.join('/')}`;
	}
	if (segments[0] === 'files' && !isFilesDoor(segments, method)) {
		return `under /files/ only a stored file's link (GET, HEAD or PUT /files/runtime/...) and an upload's steps (POST /files/upload/<step>) pass, not ${method} /${segments.join('/')}`;
	}
	return null;
}

/** GET, HEAD or PUT `files/runtime/<key...>`, or POST `files/upload/<verb>`.
 *  Which verbs exist is the install's to answer. */
function isFilesDoor(segments: string[], method: string): boolean {
	const [, under] = segments;
	if (under === 'runtime') return segments.length > 2 && ['GET', 'HEAD', 'PUT'].includes(method);
	return under === 'upload' && segments.length === 3 && method === 'POST';
}

/** A part of an upload: a PUT to a stored file's link. */
function isFilePut(method: string, segments: string[]): boolean {
	return method === 'PUT' && segments[0] === 'files' && segments[1] === 'runtime';
}

/** `access/picker/<state>` or `access/picker/<state>/result`. `begin` is the
 *  author's door that opens a chooser, not a chooser. */
function isPickerDoor(segments: string[]): boolean {
	const [, picker, state, result] = segments;
	if (picker !== 'picker' || !state || state === 'begin') return false;
	return segments.length === 3 || (segments.length === 4 && result === 'result');
}
