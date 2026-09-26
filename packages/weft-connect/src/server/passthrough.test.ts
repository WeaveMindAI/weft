import { describe, expect, it } from 'vitest';
import { weftPassThrough } from './passthrough';

/** A dispatcher that records what reached it and echoes the body back. */
function fakeDispatcher(answer: (req: Request) => Response | Promise<Response> = () => new Response('ok')) {
	const seen: Request[] = [];
	const fetcher = (async (url: string, init: RequestInit) => {
		const req = new Request(url, init);
		seen.push(req.clone());
		return answer(req);
	}) as unknown as typeof fetch;
	return { fetcher, seen };
}

const site = 'https://site.example/weft';

describe('the pass-through', () => {
	it("forwards a member door call with the member's token and the query", async () => {
		const { fetcher, seen } = fakeDispatcher(() => new Response('[]', { headers: { 'content-type': 'application/json' } }));
		const pass = weftPassThrough({ dispatcher: 'http://127.0.0.1:9999/', fetcher });
		const res = await pass(
			new Request(`${site}/member/connections?service=slack`, {
				headers: { Authorization: 'Bearer wft-m', Cookie: 'session=secret', 'Weft-Member': 'someone-else' },
			}),
			'member/connections',
		);
		expect(res.status).toBe(200);
		expect(res.headers.get('content-type')).toBe('application/json');
		expect(await res.text()).toBe('[]');
		expect(seen[0].url).toBe('http://127.0.0.1:9999/member/connections?service=slack');
		expect(seen[0].headers.get('authorization')).toBe('Bearer wft-m');
		// The site's cookies and the server-only member header stay behind.
		expect(seen[0].headers.get('cookie')).toBeNull();
		expect(seen[0].headers.get('weft-member')).toBeNull();
		// The dispatcher learns the address the browser used, mount included.
		expect(seen[0].headers.get('x-forwarded-host')).toBe('site.example');
		expect(seen[0].headers.get('x-forwarded-proto')).toBe('https');
		expect(seen[0].headers.get('x-forwarded-prefix')).toBe('/weft');
	});

	it("builds the forwarded address from the site's request, never from the browser's headers", async () => {
		const { fetcher, seen } = fakeDispatcher();
		const pass = weftPassThrough({ dispatcher: 'http://d', fetcher });
		await pass(
			new Request('http://127.0.0.1:5173/member/fields', {
				headers: { 'X-Forwarded-Host': 'evil.example', 'X-Forwarded-Prefix': '/elsewhere' },
			}),
			'member/fields',
		);
		expect(seen[0].headers.get('x-forwarded-host')).toBe('127.0.0.1:5173');
		expect(seen[0].headers.get('x-forwarded-proto')).toBe('http');
		// Mounted at the root: no prefix.
		expect(seen[0].headers.get('x-forwarded-prefix')).toBe('');
	});

	it("hands a door's redirect back with where it points", async () => {
		const { fetcher } = fakeDispatcher(
			() => new Response(null, { status: 303, headers: { location: 'https://site.example/weft/access/picker/st' } }),
		);
		const res = await weftPassThrough({ dispatcher: 'http://d', fetcher })(
			new Request(`${site}/signal/tok`),
			'signal/tok',
		);
		expect(res.status).toBe(303);
		expect(res.headers.get('location')).toBe('https://site.example/weft/access/picker/st');
	});

	it("reaches a field's chooser page and the result it posts, and nothing else under access", async () => {
		const { fetcher, seen } = fakeDispatcher();
		const pass = weftPassThrough({ dispatcher: 'http://d', fetcher });
		expect((await pass(new Request(`${site}/access/picker/st`), 'access/picker/st')).status).toBe(200);
		expect(
			(await pass(new Request(`${site}/access/picker/st/result`, { method: 'POST', body: '{}' }), 'access/picker/st/result')).status,
		).toBe(200);
		expect(seen.map((r) => r.url)).toEqual(['http://d/access/picker/st', 'http://d/access/picker/st/result']);
		for (const path of ['access/picker/begin', 'access/grants', 'access/picker', 'access/picker/st/other', 'access/picker/st/result/x']) {
			expect((await pass(new Request(`${site}/${path}`, { method: 'POST' }), path)).status).toBe(404);
		}
		expect(seen).toHaveLength(2);
	});

	it('streams a body through and keeps the status the dispatcher answered', async () => {
		const { fetcher, seen } = fakeDispatcher(async (req) => new Response(await req.text(), { status: 409 }));
		const pass = weftPassThrough({ dispatcher: () => 'http://d', fetcher });
		const res = await pass(
			new Request(`${site}/signal/tok/skip`, { method: 'POST', body: '{"a":1}', headers: { 'content-type': 'application/json' } }),
			'signal/tok/skip',
		);
		expect(res.status).toBe(409);
		expect(await res.text()).toBe('{"a":1}');
		expect(seen[0].method).toBe('POST');
		expect(seen[0].headers.get('content-type')).toBe('application/json');
	});

	it("calls a program's live route with the member's token, following the dispatcher to the worker", async () => {
		let init: RequestInit | undefined;
		const fetcher = (async (url: string, given: RequestInit) => {
			init = given;
			return new Response('{"reply":"hi"}', { headers: { 'content-type': 'application/json' } });
		}) as unknown as typeof fetch;
		const pass = weftPassThrough({ dispatcher: 'http://d', fetcher });
		const res = await pass(
			new Request(`${site}/connect/local/bot/ask`, {
				method: 'POST',
				body: '{"text":"hello"}',
				headers: { 'Weft-Member-Token': 'wft-m', 'content-type': 'application/json' },
			}),
			'connect/local/bot/ask',
		);
		expect(await res.text()).toBe('{"reply":"hi"}');
		// The redirect to the worker is followed here, not handed to the
		// browser, and the body is whole so it can be sent again.
		expect(init?.redirect).toBe('follow');
		expect(new TextDecoder().decode(init?.body as ArrayBuffer)).toBe('{"text":"hello"}');
		expect(new Headers(init?.headers).get('weft-member-token')).toBe('wft-m');
	});

	it('reaches the display doors, keeping each segment one segment', async () => {
		const { fetcher, seen } = fakeDispatcher();
		const pass = weftPassThrough({ dispatcher: 'http://d', fetcher });
		await pass(new Request(`${site}/signal-token/displays/p/test.whatsapp`), 'signal-token/displays/p/test.whatsapp');
		expect(seen[0].url).toBe('http://d/signal-token/displays/p/test.whatsapp');
	});

	it('refuses anything outside the weft doors without calling the dispatcher', async () => {
		const { fetcher, seen } = fakeDispatcher();
		const pass = weftPassThrough({ dispatcher: 'http://d', fetcher });
		for (const path of ['projects/1/activate', '', 'member/../projects', 'signal/./x']) {
			expect((await pass(new Request(`${site}/${path}`), path)).status).toBe(404);
		}
		expect(seen).toHaveLength(0);
	});

	it('says which setting is missing when the address is not set', async () => {
		const pass = weftPassThrough({ dispatcher: () => undefined, fetcher: fakeDispatcher().fetcher });
		const res = await pass(new Request(`${site}/member/fields`), 'member/fields');
		expect(res.status).toBe(500);
		expect(await res.text()).toContain('WEFT_DISPATCHER_URL');
	});

	it('answers 502 naming the address when the dispatcher does not answer', async () => {
		const fetcher = (async () => {
			throw new TypeError('fetch failed');
		}) as unknown as typeof fetch;
		const res = await weftPassThrough({ dispatcher: 'http://127.0.0.1:9999', fetcher })(
			new Request(`${site}/member/fields`),
			'member/fields',
		);
		expect(res.status).toBe(502);
		expect(await res.text()).toContain('http://127.0.0.1:9999');
	});
});
