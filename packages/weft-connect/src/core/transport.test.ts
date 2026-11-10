import { describe, expect, it } from 'vitest';
import { InstanceDoor, InstanceDoorError, ProgramDoor, ProgramDoorError, pickConnection, type FieldsDoor } from './transport';

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
			'GET /instance/fields': [{ node: 'post', field: 'account', nodeType: 'SlackAccess', input: { name: 'account' }, needed: true, connection: 'none' }],
			'POST /instance/lookup': { items: [{ id: 's-1', label: 'Budget' }], nextCursor: null },
			'POST /instance/picker': { state: 'st', url: 'https://weft.example/access/picker/st' },
		});
		const door = new InstanceDoor('wft-a-b', { base: 'https://weft.example/', fetcher, opener: () => {} });
		expect(await door.connections('slack')).toEqual([]);
		const changed = await door.setValues([{ node: 'post', field: 'account', value: { id: 'c1' } }], [{ node: 'read', field: 'tab' }]);
		expect(changed.rearmed).toEqual(['digest']);
		expect((await door.fields())[0].field).toBe('account');
		const sheets = door.resources('one.read', 'spreadsheet');
		const listSource = { kind: 'list' as const, get: 'https://x/{query}', items: 'files', label: 'name', value: 'id' };
		expect((await sheets.list(1, listSource, 'bud', { a: 'b' }, null)).items[0].id).toBe('s-1');
		expect((await sheets.beginPicker(2, { kind: 'picker', script: 'https://s', code: 'x' })).state).toBe('st');
		expect(calls.every((c) => c.auth === 'Bearer wft-a-b')).toBe(true);
		expect(calls[1].body).toEqual({
			set: [{ node: 'post', field: 'account', value: { id: 'c1' } }],
			clear: [{ node: 'read', field: 'tab' }],
		});
		// Only the field and the source's position travel: the door reads
		// the source itself off the program.
		expect(calls[3].body).toEqual({ node: 'one.read', field: 'spreadsheet', source: 1, query: 'bud', parents: { a: 'b' }, cursor: null });
		expect(calls[4].body).toEqual({ node: 'one.read', field: 'spreadsheet', source: 2 });
	});

	it('picks a connection it just made by saving it, and clears a field on its own', async () => {
		const field = { node: 'post', field: 'account' } as unknown as import('./wire').FilledField;
		const { fetcher, calls } = fakeFetch({
			'POST /instance/connections/direct': { grant: { id: 'c1', service: 'slack' } },
			'PUT /instance/values': { rearmed: ['digest'] },
		});
		const door = new InstanceDoor('t', { base: 'https://weft.example', fetcher });
		const done = await door.connectTransport().connectDirect({} as Parameters<InstanceDoor['connectDirect']>[0]);
		// Its connect picks nothing, so the page stores the pick.
		expect(done.picked).toBeUndefined();
		expect(await pickConnection(door, field, { id: done.grant.id }, null)).toEqual({ rearmed: ['digest'] });
		expect(calls[1].body).toEqual({ set: [{ node: 'post', field: 'account', value: { id: 'c1' } }], clear: [] });
		await pickConnection(door, field, null, null);
		expect(calls[2].body).toEqual({ set: [], clear: [{ node: 'post', field: 'account' }] });
	});

	it('names the way a pasted connect was filled for, and hears which ways the install offers', async () => {
		const spec = { service: 'anthropic', acquisition: { kind: 'static' as const, ways: [{ id: 'apiKey', label: 'API key', fields: [{ name: 'key' }] }] } };
		const { fetcher, calls } = fakeFetch({
			'POST /instance/doors': { sharedApps: [], sharedCredential: false, redirectUri: null, offeredWays: ['apiKey'] },
			'POST /instance/connections/direct': { grant: { id: 'g', service: 'anthropic', way: 'apiKey' } },
		});
		const door = new InstanceDoor('t', { base: 'https://weft.example', fetcher });
		expect((await door.doors(spec)).offeredWays).toEqual(['apiKey']);
		const request = {
			spec,
			door: 'own' as const,
			paste: false,
			values: { key: 'sk-1' },
			way: 'apiKey',
			label: null,
			permissions: [],
			sharedApp: null,
			registration: null,
		};
		expect((await door.connectDirect(request)).grant.way).toBe('apiKey');
		expect(calls[1].body).toEqual(request);
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

describe('the program door', () => {
	const field = {
		node: 'one.vapi',
		field: 'account',
		nodeType: 'VapiAccess',
		input: { name: 'account' },
		needed: true,
		connection: 'none' as const,
	} as unknown as import('./wire').FilledField;
	const spec = { service: 'vapi', acquisition: { kind: 'static' as const, ways: [{ id: 'apiKey', fields: [{ name: 'key' }] }] } };
	const request = {
		spec,
		door: 'own' as const,
		paste: false,
		values: { key: 'sk-1' },
		way: 'apiKey',
		label: null,
		permissions: [],
		sharedApp: null,
		registration: null,
	};

	it("calls the site's admin route with no key of its own", async () => {
		const urls: string[] = [];
		const auth: (string | null)[] = [];
		const fetcher = (async (url: string, init: RequestInit) => {
			urls.push(url);
			auth.push((init.headers as Record<string, string>).Authorization ?? null);
			return new Response('[]', { status: 200 });
		}) as unknown as typeof fetch;
		await new ProgramDoor({ fetcher }).fields();
		expect(urls).toEqual(['/weft-admin/program/connections']);
		expect(auth).toEqual([null]);
	});

	it('picks a pasted key for its field in the connect itself, and says what that pick changed', async () => {
		const { fetcher, calls } = fakeFetch({
			'POST /program/connections/direct': { grant: { id: 'g1', service: 'vapi' }, rearmed: ['call'], forgotten: ['g0'] },
			'DELETE /program/connections/one.vapi/account': { rearmed: [], forgotten: ['g1'] },
		});
		const door = new ProgramDoor({ base: 'https://weft.example', key: 'wft-k', fetcher });
		const connect = door.connectTransport(field);
		expect(connect.allowsSharedKey).toBe(false);
		expect(await connect.connections('vapi')).toEqual([]);
		const done = await connect.connectDirect(request);
		expect(done).toEqual({ grant: { id: 'g1', service: 'vapi' }, picked: { rearmed: ['call'], forgotten: ['g0'] } });
		expect(calls[0].body).toEqual({ ...request, node: 'one.vapi', field: 'account' });
		expect(calls[0].auth).toBe('Bearer wft-k');
		// The page stores nothing after it: the connect's pick is the answer.
		expect(await pickConnection(door, field, { id: 'g1' }, done.picked ?? null)).toEqual(done.picked);
		expect(calls).toHaveLength(1);
		expect(await door.clear(field)).toEqual({ rearmed: [], forgotten: ['g1'] });
		expect(calls[1].method).toBe('DELETE');
		expect(await pickConnection(door, field, null, null)).toEqual({ rearmed: [], forgotten: ['g1'] });
		expect(calls[2].method).toBe('DELETE');
	});

	it('has no save: a connection is picked here only by the connect that stores its key', async () => {
		const { fetcher, calls } = fakeFetch({});
		const door: FieldsDoor = new ProgramDoor({ base: 'https://weft.example', fetcher });
		// @ts-expect-error the program door stores no value of its own
		void new ProgramDoor().save;
		expect(door.save).toBeUndefined();
		await expect(pickConnection(door, field, { id: 'g1' }, null)).rejects.toThrow('only picks a connection by connecting it');
		expect(calls).toHaveLength(0);
	});

	it('offers no browser sign-in', async () => {
		const connect = new ProgramDoor({ base: 'https://weft.example' }).connectTransport(field);
		await expect(connect.beginConsent({ spec, door: 'own', permissions: [], sharedApp: null, upgradeGrantId: null, registration: null })).rejects.toThrow(
			'weft connect',
		);
	});

	it('tells a token that is not a connections key from a door that failed', async () => {
		const answering = (status: number) => (async () => new Response('refused', { status })) as unknown as typeof fetch;
		const refusal = (status: number) =>
			new ProgramDoor({ base: 'https://weft.example', fetcher: answering(status) }).fields().catch((e: unknown) => e);
		expect(((await refusal(403)) as ProgramDoorError).notAConnectionsKey).toBe(true);
		expect(((await refusal(502)) as ProgramDoorError).notAConnectionsKey).toBe(false);
	});
});
