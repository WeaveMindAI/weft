// A socket to one of a program's `Socket` routes that comes back on its own.
//
// A connection does not last: on a cloud install Google closes any request
// to weft after an hour, and a phone changing networks drops one sooner.
// Each connection is its own run, so what a conversation needs to survive
// lives in the program's storage, keyed by a session id the client sends on
// every connection. This keeps that id and reconnects whenever the socket
// closes, until the page closes it, the program ends the conversation, or
// the route refuses this caller.

/** The query parameter the session id rides on every connection. The
 *  program reads it off the trigger's `query` port. */
export const SESSION_PARAM = 'session';

/** The close code a program sends to say the conversation is over, not
 *  just this connection: no reconnect follows it. One of the codes the
 *  WebSocket standard leaves to applications (4000 to 4999), because a
 *  run that simply ends closes with 1000, and that ends only its own
 *  connection. */
export const CONVERSATION_OVER = 4000;

/** How long a connection has to stay open before the next drop waits the
 *  shortest time again. A program that accepts and at once fails would
 *  otherwise be called again at the shortest wait for ever. */
export const SETTLED_MS = 10_000;

export interface LiveSocketOptions {
	/** The route's address, `http(s)://.../connect/<tenant>/<project id>/<path>`. */
	url: string;
	/** The conversation this is, kept across reconnects. A fresh one is
	 *  made when absent; pass a stored one to resume a conversation after
	 *  the page reloads. */
	session?: string;
	/** Headers proving who calls (`Weft-Instance-Token`, `X-Api-Key`). A
	 *  browser cannot put headers on a socket's opening request, so with
	 *  these the socket opens in two steps: a plain `GET` carrying them
	 *  answers the address to open the socket at. Without them it opens at
	 *  `url` itself. */
	headers?: Record<string, string>;
	onMessage(data: string): void;
	/** A connection opened (the first, or a reconnect). */
	onOpen?(): void;
	/** A connection closed and another is coming in `retryInMs`. */
	onDrop?(drop: { code: number; reason: string; retryInMs: number }): void;
	/** The conversation ended: the program closed it with
	 *  [`CONVERSATION_OVER`], the route refused this caller (`status` is
	 *  its answer), or `close()` was called. Nothing reconnects after this. */
	onEnd?(end: { code: number; reason: string; status?: number }): void;
	/** Waits between reconnects, doubling from the first to the longest. */
	retry?: { firstMs: number; longestMs: number };
	/** Defaults to the global `fetch` and `WebSocket`. */
	fetcher?: typeof fetch;
	WebSocketImpl?: typeof WebSocket;
}

export interface LiveSocket {
	/** Sent now when connected, or as soon as the next connection opens.
	 *  Throws once the conversation has ended. */
	send(data: string): void;
	/** End the conversation from this side. */
	close(): void;
	readonly session: string;
}

/** The route refused the caller itself (a revoked token, a route that is
 *  gone): asking again gets the same answer. */
class Refused extends Error {
	constructor(
		readonly status: number,
		message: string,
	) {
		super(message);
	}
}

export function openLiveSocket(options: LiveSocketOptions): LiveSocket {
	const fetcher = options.fetcher ?? ((...args) => fetch(...args));
	const Ws = options.WebSocketImpl ?? WebSocket;
	const retry = options.retry ?? { firstMs: 500, longestMs: 15_000 };
	const session = options.session ?? crypto.randomUUID();
	const pending: string[] = [];
	let socket: WebSocket | null = null;
	let wait = retry.firstMs;
	let ended = false;
	let timer: ReturnType<typeof setTimeout> | null = null;

	const end = (code: number, reason: string, status?: number) => {
		if (ended) return;
		ended = true;
		if (timer !== null) clearTimeout(timer);
		pending.length = 0;
		options.onEnd?.({ code, reason, status });
	};

	const again = (code: number, reason: string) => {
		if (ended) return;
		const retryInMs = wait;
		wait = Math.min(wait * 2, retry.longestMs);
		options.onDrop?.({ code, reason, retryInMs });
		timer = setTimeout(connect, retryInMs);
	};

	async function connect(): Promise<void> {
		timer = null;
		let address: string;
		try {
			address = await socketAddress(options.url, session, options.headers, fetcher);
		} catch (e) {
			if (e instanceof Refused) end(0, e.message, e.status);
			else again(0, e instanceof Error ? e.message : String(e));
			return;
		}
		if (ended) return;
		const opened = new Ws(address);
		socket = opened;
		let openedAt = 0;
		opened.onopen = () => {
			openedAt = Date.now();
			options.onOpen?.();
			for (const data of pending.splice(0)) opened.send(data);
		};
		opened.onmessage = (event) => options.onMessage(String(event.data));
		opened.onclose = (event) => {
			socket = null;
			if (event.code === CONVERSATION_OVER) {
				end(event.code, event.reason);
				return;
			}
			if (openedAt > 0 && Date.now() - openedAt >= SETTLED_MS) wait = retry.firstMs;
			again(event.code, event.reason);
		};
	}

	void connect();
	return {
		session,
		send(data) {
			if (ended) throw new Error('this conversation has ended; open a new socket to start another');
			if (socket !== null && socket.readyState === Ws.OPEN) socket.send(data);
			else pending.push(data);
		},
		close() {
			const open = socket;
			end(CONVERSATION_OVER, 'closed by this side');
			open?.close(CONVERSATION_OVER);
		},
	};
}

/** Where to open one connection: `url` itself with the session on it, or,
 *  with `headers`, the address a plain `GET` carrying them answers. A 4xx
 *  answer is the route refusing this caller ([`Refused`]), but for the two
 *  that mean "not now"; anything else that fails is worth asking again.
 *  SYNC: the ticket answer's shape <-> crates/weft-engine/src/door/mod.rs (the ticket answer), crates/weft-e2e/src/live.rs (ticket) */
async function socketAddress(
	url: string,
	session: string,
	headers: Record<string, string> | undefined,
	fetcher: typeof fetch,
): Promise<string> {
	const withSession = new URL(url);
	withSession.searchParams.set(SESSION_PARAM, session);
	if (!headers || Object.keys(headers).length === 0) return asSocketUrl(withSession.toString());
	const answer = await fetcher(withSession.toString(), { method: 'GET', headers });
	// 408 and 429 say "not now" (a slow request, a rate limit), not "not you".
	if (answer.status >= 400 && answer.status < 500 && answer.status !== 408 && answer.status !== 429) {
		throw new Refused(answer.status, `the route refused this caller (${answer.status}): ${await answer.text()}`);
	}
	if (!answer.ok) throw new Error(`the route answered ${answer.status}: ${await answer.text()}`);
	const ticket = (await answer.json()) as { url?: unknown };
	if (typeof ticket.url !== 'string') throw new Error('the route answered no socket address');
	return asSocketUrl(ticket.url);
}

function asSocketUrl(url: string): string {
	return url.replace(/^http(s?):\/\//, 'ws$1://');
}
