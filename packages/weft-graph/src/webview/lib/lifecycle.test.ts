// What the contextual bar offers for a selection, and what the badges
// on the nodes say. Every state comes from the status answer; these
// check that the bar and the badges read it the same way.

import { describe, it, expect } from 'vitest';
import type { ActionAvailability, InfraPlacementStatus, TriggerEntry, TriggerState } from '../../protocol';
import { emptyActionAvailability } from '../../status';
import { contextActions, describeHeld, infraBadge, optionsFor, runUnavailable, takenDownWith, triggerBadge, type SelectedNode } from './lifecycle';

function status(over: Partial<ActionAvailability> = {}): ActionAvailability {
	return { ...emptyActionAvailability(), ...over };
}

const trigger = (place: string, perInstance = false): SelectedNode => ({ place, isTrigger: true, isInfra: false, perInstance });
const infra = (place: string, perInstance = false): SelectedNode => ({ place, isTrigger: false, isInfra: true, perInstance });
const plain = (place: string): SelectedNode => ({ place, isTrigger: false, isInfra: false, perInstance: false });

const shared = (node: string, state: TriggerState): TriggerEntry => ({
	node,
	perInstance: false,
	shared: { state, mode: state === 'off' ? 'take' : 'active' },
});

const place = (node: string, status: string, over: Partial<InfraPlacementStatus> = {}): InfraPlacementStatus => ({
	node,
	nodeType: 'Postgres',
	status,
	...over,
});

describe('the contextual bar counts', () => {
	it('lists each action once, with how many selected nodes it applies to', () => {
		const s = status({
			triggers: [shared('a', 'on'), shared('b', 'behind'), shared('c', 'off')],
		});
		const actions = contextActions([trigger('a'), trigger('b'), trigger('c'), plain('mid')], s, undefined);
		expect(actions.triggers).toEqual([
			{ verb: 'deactivate', places: ['a', 'b'] },
			{ verb: 'activate', places: ['c'] },
			{ verb: 'resync', places: ['b'] },
		]);
		expect(actions.infra).toEqual([]);
	});

	it('reads a trigger the install never turned on as off', () => {
		const actions = contextActions([trigger('fresh')], status(), undefined);
		expect(actions.triggers).toEqual([{ verb: 'activate', places: ['fresh'] }]);
	});

	it('offers nothing on a trigger whose setup is running', () => {
		const actions = contextActions([trigger('a')], status({ triggers: [shared('a', 'turning_on')] }), undefined);
		expect(actions.triggers).toEqual([]);
	});

	it('groups infra by what each copy takes', () => {
		const s = status({
			infraNodes: [
				place('db', 'running', { behind: true }),
				place('cache', 'stopped'),
				place('queue', 'not_started'),
				place('bridge', 'provisioning'),
			],
		});
		const actions = contextActions([infra('db'), infra('cache'), infra('queue'), infra('bridge')], s, undefined);
		expect(actions.infra).toEqual([
			{ verb: 'infra_start', places: ['cache', 'queue'] },
			{ verb: 'infra_stop', places: ['db'] },
			{ verb: 'infra_terminate', places: ['db', 'cache'] },
			{ verb: 'infra_upgrade', places: ['db'] },
		]);
	});

	it('asks for an instance before acting on a per-instance node', () => {
		const s = status({
			triggers: [{ node: 'inbox', perInstance: true, copies: [{ instance: 'ada', state: 'on', mode: 'active' }] }],
			infraNodes: [place('box', 'per_instance', { instanceCopyCount: 1 })],
		});
		const selection = [trigger('inbox', true), infra('box', true), trigger('door')];
		const onShared = contextActions(selection, s, undefined);
		expect(onShared.needInstance).toEqual(['inbox', 'box']);
		expect(onShared.anyPerInstance).toBe(true);
		expect(onShared.triggers).toEqual([{ verb: 'activate', places: ['door'] }]);
		// One instance picked: the per-instance nodes count, the shared one
		// is not reached.
		const onAda = contextActions(selection, s, ['ada']);
		expect(onAda.needInstance).toEqual([]);
		expect(onAda.sharedLeftOut).toEqual(['door']);
		expect(onAda.triggers).toEqual([{ verb: 'deactivate', places: ['inbox'] }]);
		expect(onAda.infra).toEqual([{ verb: 'infra_start', places: ['box'] }]);
	});

	it('acts on every instance weft holds anything for with all_instances', () => {
		const s = status({
			triggers: [{ node: 'inbox', perInstance: true, copies: [{ instance: 'ada', state: 'on', mode: 'active' }] }],
			instances: [
				{ instance: 'ada', values: 0, connections: 0, tokens: 0, copies: [], triggers: [] },
				{ instance: 'bob', values: 0, connections: 0, tokens: 0, copies: [], triggers: [] },
			],
		});
		const actions = contextActions([trigger('inbox', true), trigger('door')], s, 'all_instances');
		// Ada's is on, Bob's is off: both actions apply to the one node.
		expect(actions.triggers).toEqual([
			{ verb: 'deactivate', places: ['inbox'] },
			{ verb: 'activate', places: ['inbox'] },
		]);
		// The shared trigger is not reached.
		expect(actions.sharedLeftOut).toEqual(['door']);
	});

	it('acts on the shared ones and every instance with all', () => {
		const s = status({
			triggers: [
				{ node: 'inbox', perInstance: true, copies: [{ instance: 'ada', state: 'on', mode: 'active' }] },
				shared('door', 'behind'),
			],
			infraNodes: [place('box', 'per_instance', { instanceCopyCount: 0 })],
			instances: [{ instance: 'ada', values: 0, connections: 0, tokens: 0, copies: [], triggers: [] }],
		});
		const actions = contextActions([trigger('inbox', true), trigger('door'), infra('box', true)], s, 'all');
		expect(actions.needInstance).toEqual([]);
		expect(actions.sharedLeftOut).toEqual([]);
		// Ada's inbox is on, the shared door is behind.
		expect(actions.triggers).toEqual([
			{ verb: 'deactivate', places: ['inbox', 'door'] },
			{ verb: 'resync', places: ['door'] },
		]);
		expect(actions.infra).toEqual([{ verb: 'infra_start', places: ['box'] }]);
	});

	it('names what taking infra down takes down with it', () => {
		const s = status({
			infraNodes: [
				place('db', 'running', {
					takesDown: [
						{ owner: { kind: 'shared' }, triggers: ['door', 'feed'] },
						{ owner: { kind: 'instance', instance: 'ada' }, triggers: ['inbox'] },
					],
				}),
				place('cache', 'running', {
					takesDown: [
						{ owner: { kind: 'shared' }, triggers: ['feed', 'clock'] },
						{ owner: { kind: 'instance', instance: 'ada' }, triggers: ['inbox'] },
					],
				}),
			],
		});
		// Each owner once, each of its triggers once.
		expect(takenDownWith(['db', 'cache'], s)).toEqual([
			{ owner: { kind: 'shared' }, triggers: ['door', 'feed', 'clock'] },
			{ owner: { kind: 'instance', instance: 'ada' }, triggers: ['inbox'] },
		]);
	});
});

describe('the badges', () => {
	it('say what a shared trigger is doing', () => {
		expect(triggerBadge(shared('a', 'on')).label).toBe('on');
		expect(triggerBadge(shared('a', 'behind')).label).toBe('behind');
		expect(triggerBadge(shared('a', 'turning_on')).label).toBe('turning on...');
		expect(triggerBadge(shared('a', 'off')).label).toBe('off');
		const holding = triggerBadge({
			node: 'a',
			perInstance: false,
			shared: { state: 'off', mode: 'take', held: { arrivals: 2, paused: 1 } },
		});
		expect(holding.label).toBe('held 3');
		expect(holding.detail).toContain('2 arrivals and 1 paused run');
		const heldBack = triggerBadge({
			node: 'a',
			perInstance: false,
			shared: { state: 'off', mode: 'take', downWith: 'health', heldBack: 'other triggers are on another build. `weft resync --all` moves them' },
		});
		expect(heldBack.label).toBe('held back');
		expect(heldBack.detail).toContain('Held back: other triggers are on another build. `weft resync --all` moves them');
	});

	it('warn while a trigger that is on drops what arrives, saying how many and why', () => {
		const dropping = triggerBadge({
			node: 'a',
			perInstance: false,
			shared: { state: 'on', mode: 'active', dropped: { count: 3, lastReason: "package 'relay' didn't compile" } },
		});
		expect(dropping).toMatchObject({ label: 'dropping', tone: 'warn' });
		expect(dropping.detail).toContain("3 fires or calls dropped in the last minute or two (refused before any run started), the last because: package 'relay' didn't compile");
		const perInstance = triggerBadge({
			node: 'inbox',
			perInstance: true,
			copies: [
				{ instance: 'ada', state: 'on', mode: 'active', dropped: { count: 1, lastReason: 'the trigger is gone' } },
				{ instance: 'bob', state: 'on', mode: 'active' },
			],
		});
		expect(perInstance).toMatchObject({ label: '2 of 2 on', tone: 'warn', detail: 'ada: dropping\nbob: on' });
	});

	it('count a per-instance trigger\'s instances that are on', () => {
		const badge = triggerBadge({
			node: 'inbox',
			perInstance: true,
			copies: [
				{ instance: 'ada', state: 'on', mode: 'active' },
				{ instance: 'bob', state: 'behind', mode: 'active' },
				{ instance: 'cy', state: 'off', mode: 'take' },
			],
		});
		expect(badge.label).toBe('2 of 3 on');
		expect(badge.detail).toBe('ada: on\nbob: behind\ncy: off');
	});

	it('mark infra behind and starting', () => {
		expect(infraBadge(place('db', 'running', { behind: true }), [], 0).label).toBe('behind');
		expect(infraBadge(place('db', 'provisioning'), [], 0).label).toBe('starting...');
		expect(infraBadge(place('db', 'not_started'), [], 0).label).toBe('stopped');
		expect(
			infraBadge(
				place('db', 'running', {
					takesDown: [
						{ owner: { kind: 'shared' }, triggers: ['door'] },
						{ owner: { kind: 'instance', instance: 'ada' }, triggers: ['inbox'] },
					],
				}),
				[],
				0,
			).detail,
		).toBe("Taking it down takes down: 'door'; 'inbox' of instance 'ada'.");
	});

	it('tell an infra node\'s last restart on its pill', () => {
		const lastRestart = { atUnix: 1_793_109_800, cause: { kind: 'out_of_memory' as const } };
		expect(infraBadge(place('db', 'running', { lastRestart }), [], 0).detail).toBe(
			'db ran out of memory at 2026-10-27 14:03 UTC: a process in it was killed because it needs more memory than its container may use; give it more.',
		);
		expect(
			infraBadge(place('box', 'per_instance'), [{ node: 'box', instance: 'ada', status: 'running', lastRestart }], 0).detail,
		).toBe('ada: running\nbox (instance ada) ran out of memory at 2026-10-27 14:03 UTC: a process in it was killed because it needs more memory than its container may use; give it more.');
	});

	it('count a per-instance infra node\'s copies that run', () => {
		const badge = infraBadge(place('box', 'per_instance'), [
			{ node: 'box', instance: 'ada', status: 'running' },
			{ node: 'box', instance: 'bob', status: 'stopped' },
			{ node: 'other', instance: 'ada', status: 'running' },
		], 0);
		expect(badge.label).toBe('1 of 2 running');
		expect(badge.detail).toBe('ada: running\nbob: stopped');
	});

	it('refuse an infra status they cannot read', () => {
		expect(() => infraBadge(place('db', 'mystery'), [], 0)).toThrow(/cannot read/);
	});

	it('word held work the way weft does', () => {
		expect(describeHeld({ arrivals: 1, paused: 0 })).toBe('1 arrival');
		expect(describeHeld({ arrivals: 0, paused: 2 })).toBe('2 paused runs');
	});
});

describe('the contextual bar\'s Run', () => {
	const open = { blocked: false, offered: true, runBlocked: null, status: status() };

	it('is disabled with the check\'s refusal as its text', () => {
		const refusal = "needs `door`, which only fires from outside; use the CLI with --fire or --from";
		expect(runUnavailable({ ...open, check: { ok: false, refusal } })).toBe(refusal);
	});

	it('goes once the check resolves and reads no infra', () => {
		expect(runUnavailable({ ...open, check: { ok: true } })).toBeUndefined();
	});

	it('waits for the check\'s answer', () => {
		expect(runUnavailable({ ...open, check: undefined })).toMatch(/Checking/);
	});

	it('names the infra the check says the run reads that is not running', () => {
		const check = { ok: true as const, infraRead: [{ node: 'one.db' }, { node: 'cache' }, { node: 'bridge', instance: 'acme' }] };
		const shown = status({
			infraNodes: [place('one.db', 'stopped'), place('cache', 'running')],
			instanceInfra: [{ node: 'bridge', instance: 'acme', status: 'running' }],
		});
		expect(runUnavailable({ ...open, status: shown, check })).toBe('Start one.db first: this run reads it, and it is not running.');
		const allUp = status({
			infraNodes: [place('one.db', 'running'), place('cache', 'running')],
			instanceInfra: [{ node: 'bridge', instance: 'acme', status: 'stopped' }],
		});
		expect(runUnavailable({ ...open, status: allUp, check })).toBe("Start bridge (instance 'acme') first: this run reads it, and it is not running.");
		expect(runUnavailable({ ...open, status: status(), check: { ok: true, infraRead: [{ node: 'a' }, { node: 'b' }] } }))
			.toBe('Start a, b first: this run reads them, and they are not running.');
	});

	it('counts infra with no status entry (never created) as not running, and running infra as running', () => {
		const check = { ok: true as const, infraRead: [{ node: 'postgres_database_1' }] };
		expect(runUnavailable({ ...open, status: status(), check }))
			.toBe('Start postgres_database_1 first: this run reads it, and it is not running.');
		expect(runUnavailable({ ...open, status: status({ infraNodes: [place('postgres_database_1', 'not_started')] }), check }))
			.toMatch(/^Start postgres_database_1 first/);
		expect(runUnavailable({ ...open, status: status({ infraNodes: [place('postgres_database_1', 'running')] }), check })).toBeUndefined();
	});

	it('names each blocked target of a mixed selection with its own reason, and the ones that could run alone', () => {
		const targets = [
			{ target: 'debug', infraRead: [{ node: 'pg' }] },
			{ target: 'reply', unfiredTriggers: ['door'] },
			{ target: 'lone' },
		];
		const refused = { ok: false as const, refusal: 'the run cannot start:\nreply.p gets nothing in this run', targets };
		expect(runUnavailable({ ...open, check: refused })).toBe(
			'These run together as one run, which cannot start: debug needs pg started; '
				+ 'reply only gets its input from the trigger door, and Run does not fire triggers. '
				+ 'lone could run on its own: select it without the others.',
		);
		// pg running: only the trigger's side stops the run.
		const pgUp = status({ infraNodes: [place('pg', 'running')] });
		expect(runUnavailable({ ...open, status: pgUp, check: refused })).toBe(
			'These run together as one run, which cannot start: '
				+ 'reply only gets its input from the trigger door, and Run does not fire triggers. '
				+ 'debug, lone could each run on their own: select them without the others.',
		);
	});

	it('names the infra a mixed selection reads when weft takes the run but the infra is down', () => {
		const check = {
			ok: true as const,
			infraRead: [{ node: 's.db' }],
			targets: [{ target: 's.show', infraRead: [{ node: 's.db' }] }, { target: 'lone' }],
		};
		expect(runUnavailable({ ...open, check })).toBe(
			'These run together as one run, which cannot start: s.show needs s.db started. lone could run on its own: select it without the others.',
		);
		expect(runUnavailable({ ...open, status: status({ infraNodes: [place('s.db', 'running')] }), check })).toBeUndefined();
	});

	it('names every reason one target has, and keeps the run\'s own refusal when no target is blocked alone', () => {
		const targets = [
			{ target: 'a', infraRead: [{ node: 'pg', instance: 'acme' }], unfiredTriggers: ['door', 'tick'], refusal: 'a.x gets nothing' },
			{ target: 'b', refusal: 'b.y gets nothing' },
		];
		expect(runUnavailable({ ...open, check: { ok: false, refusal: 'whole', targets } })).toBe(
			"These run together as one run, which cannot start: a needs pg (instance 'acme') started, and only gets its input from the triggers door, tick, "
				+ 'and Run does not fire triggers, and cannot start: a.x gets nothing; b cannot start: b.y gets nothing.',
		);
		expect(runUnavailable({ ...open, check: { ok: false, refusal: 'whole', targets: [{ target: 'a' }, { target: 'b' }] } })).toBe('whole');
	});

	it('says what stops it before asking the check', () => {
		expect(runUnavailable({ ...open, runBlocked: 'This file was opened on its own.', check: { ok: true } })).toBe('This file was opened on its own.');
		expect(runUnavailable({ ...open, blocked: true, check: { ok: true } })).toMatch(/under way/);
	});
});

describe('the contextual bar\'s options', () => {
	it('has none for infra that can only start, so no Options button', () => {
		const actions = contextActions([infra('pg')], status(), undefined);
		expect(actions.infra.map((a) => a.verb)).toEqual(['infra_start']);
		expect(optionsFor(actions).any).toBe(false);
	});

	it('has the infra choice once infra can go down, and the trigger choices by verb', () => {
		const running = optionsFor(contextActions([infra('pg')], status({ infraNodes: [place('pg', 'running')] }), undefined));
		expect(running.any).toBe(true);
		expect(running.infraGoingDown.map((a) => a.verb)).toEqual(['infra_stop', 'infra_terminate']);
		const off = optionsFor(contextActions([trigger('door')], status(), undefined));
		expect(off).toMatchObject({ any: true, turnsTriggersOn: true, takesTriggersDown: false });
		const on = optionsFor(contextActions([trigger('door')], status({ triggers: [shared('door', 'on')] }), undefined));
		expect(on).toMatchObject({ any: true, turnsTriggersOn: false, takesTriggersDown: true });
	});

	it('has none for plain nodes', () => {
		expect(optionsFor(contextActions([plain('a')], status(), undefined)).any).toBe(false);
	});
});
