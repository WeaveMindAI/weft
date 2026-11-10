import { describe, expect, it } from 'vitest';
import { LARGEST_PART_BYTES, RUN_ID_HEADER, weftPassThrough, weftProgramPassThrough, type PlainFailure } from './passthrough';

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
	it("forwards an instance door call with the instance's token and the query", async () => {
		const { fetcher, seen } = fakeDispatcher(() => new Response('[]', { headers: { 'content-type': 'application/json' } }));
		const pass = weftPassThrough({ dispatcher: 'http://127.0.0.1:14111/', fetcher });
		const res = await pass(
			new Request(`${site}/instance/connections?service=slack`, {
				headers: { Authorization: 'Bearer wft-m', Cookie: 'session=secret', 'Weft-Instance': 'someone-else' },
			}),
			'instance/connections',
		);
		expect(res.status).toBe(200);
		expect(res.headers.get('content-type')).toBe('application/json');
		expect(await res.text()).toBe('[]');
		expect(seen[0].url).toBe('http://127.0.0.1:14111/instance/connections?service=slack');
		expect(seen[0].headers.get('authorization')).toBe('Bearer wft-m');
		// The site's cookies and the server-only instance header stay behind.
		expect(seen[0].headers.get('cookie')).toBeNull();
		expect(seen[0].headers.get('weft-instance')).toBeNull();
		// The dispatcher learns the address the browser used, mount included.
		expect(seen[0].headers.get('x-forwarded-host')).toBe('site.example');
		expect(seen[0].headers.get('x-forwarded-proto')).toBe('https');
		expect(seen[0].headers.get('x-weft-forwarded-proto')).toBe('https');
		expect(seen[0].headers.get('x-forwarded-prefix')).toBe('/weft');
	});

	it("builds the forwarded address from the site's request, never from the browser's headers", async () => {
		const { fetcher, seen } = fakeDispatcher();
		const pass = weftPassThrough({ dispatcher: 'http://d', fetcher });
		await pass(
			new Request('http://127.0.0.1:5173/instance/fields', {
				headers: { 'X-Forwarded-Host': 'evil.example', 'X-Forwarded-Prefix': '/elsewhere' },
			}),
			'instance/fields',
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

	it("calls a program's live route with the instance's token, streaming its body", async () => {
		let init: RequestInit | undefined;
		const fetcher = (async (url: string, given: RequestInit) => {
			init = given;
			return new Response('{"reply":"hi"}', { headers: { 'content-type': 'application/json' } });
		}) as unknown as typeof fetch;
		const pass = weftPassThrough({ dispatcher: 'http://d', fetcher });
		const res = await pass(
			new Request(`${site}/connect/local/2c65676f-ac18-496c-8e61-2d4a170d2f00/bot/ask`, {
				method: 'POST',
				body: '{"text":"hello"}',
				headers: { 'Weft-Instance-Token': 'wft-m', 'content-type': 'application/json' },
			}),
			'connect/local/2c65676f-ac18-496c-8e61-2d4a170d2f00/bot/ask',
		);
		expect(await res.text()).toBe('{"reply":"hi"}');
		// The dispatcher answers a route call in the one request, so its
		// body streams on as every door's does.
		expect(init?.redirect).toBe('manual');
		expect(await new Response(init?.body as ReadableStream).text()).toBe('{"text":"hello"}');
		expect(new Headers(init?.headers).get('weft-instance-token')).toBe('wft-m');
	});

	it("hands back which run answered, on a failure and on a success", async () => {
		const runId = '7f3c0d2e-0000-4000-8000-000000000001';
		const failure: PlainFailure = { error: 'Something went wrong', runId };
		for (const [status, body] of [
			[500, JSON.stringify(failure)],
			[200, '{"reply":"hi"}'],
		] as const) {
			const { fetcher } = fakeDispatcher(
				() => new Response(body, { status, headers: { 'content-type': 'application/json', [RUN_ID_HEADER]: runId } }),
			);
			const pass = weftPassThrough({ dispatcher: 'http://d', fetcher });
			const res = await pass(new Request(`${site}/connect/local/p/bot/ask`, { method: 'POST', body: '{}' }), 'connect/local/p/bot/ask');
			expect(res.status).toBe(status);
			expect(res.headers.get(RUN_ID_HEADER)).toBe(runId);
			expect(await res.text()).toBe(body);
		}
	});

	it('reaches the display doors, keeping each segment one segment', async () => {
		const { fetcher, seen } = fakeDispatcher();
		const pass = weftPassThrough({ dispatcher: 'http://d', fetcher });
		await pass(new Request(`${site}/signal-token/displays/p/test.whatsapp`), 'signal-token/displays/p/test.whatsapp');
		expect(seen[0].url).toBe('http://d/signal-token/displays/p/test.whatsapp');
	});

	it("passes a stored file's link byte for byte, so its signature still matches", async () => {
		const { fetcher, seen } = fakeDispatcher();
		const pass = weftPassThrough({ dispatcher: 'http://d', fetcher });
		// A weft key is segments of [A-Za-z0-9._-] joined by '/', which
		// re-encoding each segment leaves as it was; the query carries the
		// signature with its escapes, and must reach the store unchanged.
		const key = 'runtime/local/project/0b6e8c1a-2f3d-4c5e-9a7b-1c2d3e4f5a6b/7f3e2a10-1b2c-4d5e-8f9a-0b1c2d3e4f5a';
		const query =
			'?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=weft%2F20261019%2Fus-east-1%2Fs3%2Faws4_request' +
			'&X-Amz-Date=20261019T101500Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host' +
			'&X-Amz-Signature=3f1c%2B0a&response-content-disposition=inline%3B%20filename%3D%22a%20b.png%22&x-id=GetObject';
		for (const name of ['a.B_c-9', 'with.many.dots', '_', '-']) {
			const path = `files/${key}/${name}`;
			await pass(new Request(`${site}/${path}${query}`), path);
		}
		expect(seen.map((r) => r.url)).toEqual(
			['a.B_c-9', 'with.many.dots', '_', '-'].map((name) => `http://d/files/${key}/${name}${query}`),
		);
	});

	it("asks the store for what a file read needs, and hands back what it answered", async () => {
		const answered = {
			'content-range': 'bytes 0-99/1000',
			'accept-ranges': 'bytes',
			etag: '"abc"',
			'last-modified': 'Mon, 19 Oct 2026 10:15:00 GMT',
			'x-content-type-options': 'nosniff',
		};
		const { fetcher, seen } = fakeDispatcher(() => new Response('x'.repeat(100), { status: 206, headers: answered }));
		const pass = weftPassThrough({ dispatcher: 'http://d', fetcher });
		const asked = {
			range: 'bytes=0-99',
			'if-range': '"abc"',
			'if-none-match': '"old"',
			'if-modified-since': 'Sun, 18 Oct 2026 10:15:00 GMT',
		};
		const res = await pass(new Request(`${site}/files/runtime/t/f?sig=1`, { headers: asked }), 'files/runtime/t/f');
		expect(res.status).toBe(206);
		for (const [name, value] of Object.entries(asked)) expect(seen[0].headers.get(name)).toBe(value);
		for (const [name, value] of Object.entries(answered)) expect(res.headers.get(name)).toBe(value);
	});

	it("sends a part of an upload with its exact length, never chunked", async () => {
		let init: RequestInit | undefined;
		const fetcher = (async (_url: string, given: RequestInit) => {
			init = given;
			return new Response(null, { status: 200, headers: { etag: '"part-1"' } });
		}) as unknown as typeof fetch;
		const pass = weftPassThrough({ dispatcher: 'http://d', fetcher });
		const part = new Uint8Array(1234).fill(7);
		const res = await pass(
			new Request(`${site}/files/runtime/t/upload?partNumber=1&sig=1`, {
				method: 'PUT',
				headers: { 'content-length': '1234' },
				body: new ReadableStream({
					start(c) {
						c.enqueue(part.subarray(0, 1000));
						c.enqueue(part.subarray(1000));
						c.close();
					},
				}),
				duplex: 'half',
			} as RequestInit),
			'files/runtime/t/upload',
		);
		expect(res.headers.get('etag')).toBe('"part-1"');
		// A whole body (not a stream), so fetch sends `Content-Length: 1234`.
		expect(init?.body).toBeInstanceOf(ArrayBuffer);
		expect((init?.body as ArrayBuffer).byteLength).toBe(1234);
		expect((init as { duplex?: string }).duplex).toBeUndefined();
	});

	it('refuses a part with no length, or a length past the largest part, before reading it', async () => {
		const { fetcher, seen } = fakeDispatcher();
		const pass = weftPassThrough({ dispatcher: 'http://d', fetcher });
		let pulled = false;
		const untouched = () =>
			// No high-water mark: the stream is pulled only when read.
			new ReadableStream(
				{
					pull() {
						pulled = true;
					},
				},
				{ highWaterMark: 0 },
			);
		const put = (headers: Record<string, string>) =>
			pass(
				new Request(`${site}/files/runtime/t/upload?partNumber=1`, { method: 'PUT', headers, body: untouched(), duplex: 'half' } as RequestInit),
				'files/runtime/t/upload',
			);
		expect((await put({})).status).toBe(411);
		const tooBig = await put({ 'content-length': String(LARGEST_PART_BYTES + 1) });
		expect(tooBig.status).toBe(413);
		expect(await tooBig.text()).toContain(`at most ${LARGEST_PART_BYTES} bytes`);
		expect(pulled).toBe(false);
		expect(seen).toHaveLength(0);
	});

	it('stops reading a part that carries more than it said', async () => {
		const { fetcher, seen } = fakeDispatcher();
		const pass = weftPassThrough({ dispatcher: 'http://d', fetcher });
		let sent = 0;
		const res = await pass(
			new Request(`${site}/files/runtime/t/upload?partNumber=1`, {
				method: 'PUT',
				headers: { 'content-length': '10' },
				// Endless: only stopping reading ends this test.
				body: new ReadableStream({
					pull(c) {
						sent++;
						c.enqueue(new Uint8Array(4));
					},
				}),
				duplex: 'half',
			} as RequestInit),
			'files/runtime/t/upload',
		);
		expect(res.status).toBe(413);
		expect(sent).toBeLessThan(5);
		expect(seen).toHaveLength(0);
	});

	it("passes a write's version check on to the store", async () => {
		const { fetcher, seen } = fakeDispatcher();
		const pass = weftPassThrough({ dispatcher: 'http://d', fetcher });
		await pass(
			new Request(`${site}/files/runtime/t/f?sig=1`, { method: 'PUT', headers: { 'content-length': '1', 'if-match': '"v1"' }, body: 'x' }),
			'files/runtime/t/f',
		);
		expect(seen[0].headers.get('if-match')).toBe('"v1"');
	});

	it('writes a key the way its link was signed, whatever it holds', async () => {
		const { fetcher, seen } = fakeDispatcher();
		const pass = weftPassThrough({ dispatcher: 'http://d', fetcher });
		// What the signer wrote (RFC 3986), and the decoded path a site's
		// router hands over for it.
		const signed = 'files/runtime/t/a%20b%2Bc%25d%C3%A9%E2%82%AC%28x%29%21%2A%27';
		const decoded = "files/runtime/t/a b+c%dé€(x)!*'";
		await pass(new Request(`${site}/${signed}?X-Amz-Signature=ab%2F`), decoded);
		expect(seen[0].url).toBe(`http://d/${signed}?X-Amz-Signature=ab%2F`);
	});

	it("passes an upload's steps, and nothing else under files", async () => {
		const { fetcher, seen } = fakeDispatcher(
			() => new Response('being completed', { status: 409, headers: { 'x-weft-completing': '1' } }),
		);
		const pass = weftPassThrough({ dispatcher: 'http://d', fetcher });
		const res = await pass(
			new Request(`${site}/files/upload/begin`, { method: 'POST', body: '{"slot":"weft-upload:t"}', headers: { 'content-type': 'application/json' } }),
			'files/upload/begin',
		);
		// The "being completed, ask again" mark comes back with the answer.
		expect(res.headers.get('x-weft-completing')).toBe('1');
		expect(seen.map((r) => r.url)).toEqual(['http://d/files/upload/begin']);
		expect(await seen[0].text()).toBe('{"slot":"weft-upload:t"}');
		for (const [method, path] of [
			['GET', 'files/other'],
			['GET', 'files/upload/begin'],
			['POST', 'files/upload'],
			['POST', 'files/upload/begin/x'],
			['POST', 'files/runtime/t/f'],
			['DELETE', 'files/runtime/t/f'],
			['GET', 'files/runtime'],
		]) {
			const refused = await pass(new Request(`${site}/${path}`, { method, ...(method === 'POST' ? { body: '{}' } : {}) }), path);
			expect(refused.status, `${method} ${path}`).toBe(404);
			expect(await refused.text()).toContain('under /files/ only');
		}
		expect(seen).toHaveLength(1);
	});

	it('refuses anything outside the weft doors without calling the dispatcher', async () => {
		const { fetcher, seen } = fakeDispatcher();
		const pass = weftPassThrough({ dispatcher: 'http://d', fetcher });
		for (const path of ['projects/1/activate', '', 'instance/../projects', 'signal/./x', 'public/files/tok', 'files', 'files/runtime/../chunks/x']) {
			expect((await pass(new Request(`${site}/${path}`), path)).status).toBe(404);
		}
		expect(seen).toHaveLength(0);
	});

	it('says which setting is missing when the address is not set', async () => {
		const pass = weftPassThrough({ dispatcher: () => undefined, fetcher: fakeDispatcher().fetcher });
		const res = await pass(new Request(`${site}/instance/fields`), 'instance/fields');
		expect(res.status).toBe(500);
		expect(await res.text()).toContain('WEFT_DISPATCHER_URL');
	});

	it('answers 502 naming the address when the dispatcher does not answer', async () => {
		const fetcher = (async () => {
			throw new TypeError('fetch failed');
		}) as unknown as typeof fetch;
		const res = await weftPassThrough({ dispatcher: 'http://127.0.0.1:14111', fetcher })(
			new Request(`${site}/instance/fields`),
			'instance/fields',
		);
		expect(res.status).toBe(502);
		expect(await res.text()).toContain('http://127.0.0.1:14111');
	});

	it('names the cause beneath a failed fetch', async () => {
		const fetcher = (async () => {
			const refused = Object.assign(new Error('connect ECONNREFUSED 10.10.0.2:14113'), { code: 'ECONNREFUSED' });
			throw new TypeError('fetch failed', { cause: refused });
		}) as unknown as typeof fetch;
		const res = await weftPassThrough({ dispatcher: 'http://10.10.0.2:14113', fetcher })(
			new Request(`${site}/instance/fields`),
			'instance/fields',
		);
		expect(await res.text()).toContain('fetch failed: connect ECONNREFUSED 10.10.0.2:14113');
	});
});

describe('the program pass-through', () => {
	it('presents the connections key the server holds, never what the page sent', async () => {
		const { fetcher, seen } = fakeDispatcher(() => new Response('[]', { headers: { 'content-type': 'application/json' } }));
		const pass = weftProgramPassThrough({ dispatcher: 'http://d', connectionsKey: () => 'wft-key', fetcher });
		const res = await pass(
			new Request('https://site.example/weft-admin/program/connections', {
				headers: { Authorization: 'Bearer from-the-page', Cookie: 'admin=1' },
			}),
			'program/connections',
			{ admin: true },
		);
		expect(res.status).toBe(200);
		expect(seen[0].url).toBe('http://d/program/connections');
		expect(seen[0].headers.get('authorization')).toBe('Bearer wft-key');
		expect(seen[0].headers.get('cookie')).toBeNull();
		expect(seen[0].headers.get('x-forwarded-prefix')).toBe('/weft-admin');
	});

	it('passes the program door alone', async () => {
		const { fetcher, seen } = fakeDispatcher();
		const pass = weftProgramPassThrough({ dispatcher: 'http://d', connectionsKey: 'wft-key', fetcher });
		for (const path of ['instance/fields', 'signal-token/signals', 'program/../instance/fields']) {
			const res = await pass(new Request(`https://site.example/weft-admin/${path}`), path, { admin: true });
			expect(res.status).toBe(404);
		}
		expect(seen).toHaveLength(0);
	});

	it("passes nothing on when the site's admin check refuses the caller", async () => {
		const { fetcher, seen } = fakeDispatcher();
		const pass = weftProgramPassThrough({ dispatcher: 'http://d', connectionsKey: 'wft-key', fetcher });
		const call = () => new Request('https://site.example/weft-admin/program/connections', { method: 'POST', body: '{}' });
		const refused = await pass(call(), 'program/connections', { admin: false });
		expect(refused.status).toBe(403);
		expect(await refused.text()).toContain("the site's admin check refused this call");
		// A plain JavaScript site that leaves the check out is refused the same way.
		const unchecked = pass as unknown as (request: Request, path: string) => Promise<Response>;
		expect((await unchecked(call(), 'program/connections')).status).toBe(403);
		expect(seen).toHaveLength(0);
	});

	it("passes the call on once the site's admin check lets it through", async () => {
		const { fetcher, seen } = fakeDispatcher();
		const pass = weftProgramPassThrough({ dispatcher: 'http://d', connectionsKey: 'wft-key', fetcher });
		const res = await pass(new Request('https://site.example/weft-admin/program/connections'), 'program/connections', { admin: true });
		expect(res.status).toBe(200);
		expect(seen).toHaveLength(1);
		expect(seen[0].url).toBe('http://d/program/connections');
	});

	it('says so when the key is not set', async () => {
		const { fetcher, seen } = fakeDispatcher();
		const pass = weftProgramPassThrough({ dispatcher: 'http://d', connectionsKey: () => undefined, fetcher });
		const res = await pass(new Request('https://site.example/weft-admin/program/connections'), 'program/connections', { admin: true });
		expect(res.status).toBe(500);
		expect(await res.text()).toContain('WEFT_CONNECTIONS_KEY');
		expect(seen).toHaveLength(0);
	});

	it("never passes the program door on a visitor's route", async () => {
		const { fetcher, seen } = fakeDispatcher();
		const pass = weftPassThrough({ dispatcher: 'http://d', fetcher });
		const res = await pass(new Request(`${site}/program/connections`), 'program/connections');
		expect(res.status).toBe(404);
		expect(seen).toHaveLength(0);
	});
});
