import { describe, expect, it } from 'vitest';
import { InstanceDoor, InstanceDoorError } from './transport';

/** A fetch that records what it was asked and answers from a table. */
function fakeFetch(answers: Record<string, unknown>) {
	const calls: { url: string; method: string; auth: string | null; body: unknown }[] = [];
	const fetcher = (async (url: string, init: RequestInit) => {
		const headers = init.headers as Record<string, string>;
		calls.push({
			url,
			method: init.method ?? 'GET',
			auth: headers.Authorization ?? null,
			body: init.body ? JSON.parse(init.body as string) : undefined,
		});
		const key = `${init.method} ${url.replace('https://weft.example', '')}`;
		if (!(key in answers)) return new Response('no such route', { status: 404 });
		const answer = answers[key];
		return answer === null ? new Response(null, { status: 204 }) : new Response(JSON.stringify(answer), { status: 200 });
	}) as unknown as typeof fetch;
	return { fetcher, calls };
}

describe('the instance door', () => {
	it('asks every question inside the instance, under /instance', async () => {
		const { fetcher, calls } = fakeFetch({
			'GET /instance/connections?service=slack': [],
			'PUT /instance/values': { rearmed: ['digest'] },
			'GET /instance/fields': [{ step: 'post', field: 'account', nodeType: 'SlackAccess', input: { name: 'account' }, needed: true, connection: 'none' }],
			'POST /instance/lookup': { items: [{ id: 's-1', label: 'Budget' }], next_cursor: null },
			'POST /instance/picker': { state: 'st', url: 'https://weft.example/access/picker/st' },
		});
		const door = new InstanceDoor('wft-a-b', { base: 'https://weft.example/', fetcher, opener: () => {} });
		expect(await door.connections('slack')).toEqual([]);
		const changed = await door.setValues([{ step: 'post', field: 'account', value: { id: 'c1' } }], [{ step: 'read', field: 'tab' }]);
		expect(changed.rearmed).toEqual(['digest']);
		expect((await door.fields())[0].field).toBe('account');
		const sheets = door.resources('one.read', 'spreadsheet');
		const listSource = { kind: 'list' as const, get: 'https://x/{query}', items: 'files', label: 'name', value: 'id' };
		expect((await sheets.list(1, listSource, 'bud', { a: 'b' }, null)).items[0].id).toBe('s-1');
		expect((await sheets.beginPicker(2, { kind: 'picker', script: 'https://s', code: 'x' })).state).toBe('st');
		expect(calls.every((c) => c.auth === 'Bearer wft-a-b')).toBe(true);
		expect(calls[1].body).toEqual({
			set: [{ step: 'post', field: 'account', value: { id: 'c1' } }],
			clear: [{ step: 'read', field: 'tab' }],
		});
		// Only the field and the source's position travel: the door reads
		// the source itself off the program.
		expect(calls[3].body).toEqual({ step: 'one.read', field: 'spreadsheet', source: 1, query: 'bud', parents: { a: 'b' }, cursor: null });
		expect(calls[4].body).toEqual({ step: 'one.read', field: 'spreadsheet', source: 2 });
	});

	it('never offers the shared key or a mint to an instance', () => {
		const door = new InstanceDoor('t', { base: 'https://weft.example' });
		expect(door.allowsSharedKey).toBe(false);
		expect('mintApp' in door).toBe(false);
	});

	it("says what the door answered when it refuses", async () => {
		const { fetcher } = fakeFetch({});
		const door = new InstanceDoor('t', { base: 'https://weft.example', fetcher });
		await expect(door.fields()).rejects.toThrow('no such route');
	});

	it("calls the site's own server by default", async () => {
		const urls: string[] = [];
		const fetcher = (async (url: string) => {
			urls.push(url);
			return new Response('[]', { status: 200 });
		}) as unknown as typeof fetch;
		await new InstanceDoor('t', { fetcher }).fields();
		expect(urls).toEqual(['/weft/instance/fields']);
	});

	it('tells a token that is not an instance token from a door that failed', async () => {
		const answering = (status: number) =>
			(async () => new Response('refused', { status })) as unknown as typeof fetch;
		const refusal = (status: number) =>
			new InstanceDoor('t', { base: 'https://weft.example', fetcher: answering(status) }).fields().catch((e: unknown) => e);
		const notInstance = await refusal(403);
		expect(notInstance).toBeInstanceOf(InstanceDoorError);
		expect((notInstance as InstanceDoorError).notAnInstanceToken).toBe(true);
		expect(((await refusal(502)) as InstanceDoorError).notAnInstanceToken).toBe(false);
	});
});
