import { describe, it, expect } from 'vitest';
import { scopeFirings, copyFrames, type ScopeBox } from './scope-firings';
import type { Frame, ScopeDecidedEvent, ValueRef } from '../../../protocol';
import { readOnTheSpot } from './value-ref';
import type { Edge, NodeFiring } from '../types';

function row(nodeId: string, frames: Frame[], over: Partial<NodeFiring> = {}): NodeFiring {
	return {
		id: `${nodeId}-${JSON.stringify(frames)}`,
		nodeId,
		status: 'completed',
		pulseId: '',
		startedAt: 1000,
		completedAt: 2000,
		costUsd: 0,
		logs: [],
		runId: '',
		frames,
		framesKey: JSON.stringify(frames),
		...over,
	};
}

/** Each port's value, written out in its reference. */
function written(values: Record<string, unknown>): Record<string, ValueRef> {
	return Object.fromEntries(Object.entries(values).map(([port, value]) => [port, { from: 'written', value }]));
}

/** Each port's value its reference holds. */
function read(ports: Record<string, ValueRef> | undefined): Record<string, unknown> {
	return Object.fromEntries(Object.entries(ports ?? {}).map(([port, at]) => [port, readOnTheSpot(at)?.value]));
}

function leg(source: string, sourceHandle: string, target: string, targetHandle: string, path?: string[]): Edge {
	return { id: `e-${source}-${sourceHandle}-${target}-${targetHandle}`, source, sourceHandle, target, targetHandle, ...(path ? { path } : {}) };
}

const open = (scope: string, frames: Frame[], atUnix = 1): ScopeDecidedEvent => ({ scope, frames, state: 'open', atUnix });

/** A box `g` with input `x` read by `g.m` and output `y` written by `g.m`
 *  through the key `text`, fed by `a` and read by `b` outside. */
function groupBox(over: Partial<ScopeBox>, rows: Record<string, NodeFiring[]>): ScopeBox {
	return {
		id: 'g',
		callPath: [],
		decisions: [],
		inside: rows['g.m'] ?? [],
		copyOf: (r) => r.frames,
		legs: [
			leg('a', 'out', 'g', 'x'),
			leg('g', 'x__inner', 'g.m', 'in'),
			leg('g.m', 'out', 'g', 'y__inner', ['text']),
			leg('g', 'y', 'b', 'in'),
		],
		rowsOf: (id) => rows[id] ?? [],
		inPorts: ['x'],
		outPorts: ['y'],
		terminal: undefined,
		...over,
	};
}

describe('scopeFirings: a group box painted from its scope', () => {
	it('an open copy reads its members: status, and the values at its ports', () => {
		const rows = {
			'g.m': [row('g.m', [], { input: written({ in: 'hello' }), output: written({ out: { text: 'HELLO', n: 1 } }), costUsd: 0.5 })],
		};
		const [firing] = scopeFirings(groupBox({ decisions: [open('g', [], 3)] }, rows));
		expect(firing.status).toBe('completed');
		expect(firing.startedAt).toBe(3000);
		expect(read(firing.input)).toEqual({ x: 'hello' });
		// The leg into `self.y` takes `text` off the member's output.
		expect(read(firing.output)).toEqual({ y: 'HELLO' });
		expect(firing.costUsd).toBe(0.5);
		expect(firing.skipReason).toBeUndefined();
	});

	it('an open copy whose members have not finished is running, and the end of the run ends it', () => {
		const rows = { 'g.m': [row('g.m', [], { status: 'running', completedAt: undefined })] };
		expect(scopeFirings(groupBox({ decisions: [open('g', [])] }, rows))[0].status).toBe('running');
		const nothingInside = groupBox({ decisions: [open('g', [])], terminal: { state: 'cancelled' } }, {});
		expect(scopeFirings(nothingInside)[0].status).toBe('cancelled');
	});

	it('a skipped copy says skipped, and why', () => {
		const skipped: ScopeDecidedEvent = { scope: 'g', frames: [], state: 'skipped', reason: { kind: 'did_not_flow' }, atUnix: 2 };
		const rows = { 'g.m': [row('g.m', [], { status: 'skipped', skipReason: { kind: 'scope_skipped', scope: 'g' } })] };
		const [firing] = scopeFirings(groupBox({ decisions: [skipped] }, rows));
		expect(firing.status).toBe('skipped');
		expect(firing.skipReason).toEqual({ kind: 'did_not_flow' });
	});

	it('one card per copy, each with its own rows, and only the copies of the call on screen', () => {
		const rows = {
			'g.m': [
				row('g.m', [{ index: 0 }], { input: written({ in: 'zero' }), costUsd: 1 }),
				row('g.m', [{ index: 1 }], { input: written({ in: 'one' }), costUsd: 2 }),
			],
		};
		const decisions = [
			open('g', [{ index: 0 }]),
			open('g', [{ index: 1 }]),
			// The same file reached through another call is not this view's.
			open('g', [{ site: 'other' }]),
		];
		const firings = scopeFirings(groupBox({ decisions }, rows));
		expect(firings.map((f) => f.framesKey)).toEqual([JSON.stringify([{ index: 0 }]), JSON.stringify([{ index: 1 }])]);
		expect(firings.map((f) => read(f.input))).toEqual([{ x: 'zero' }, { x: 'one' }]);
		expect(firings.map((f) => f.costUsd)).toEqual([1, 2]);
	});

	it('a group the run decided nothing about paints as its members say, per copy', () => {
		// Dissolved in the run's cut: no scope ran, the members did.
		const rows = {
			'g.m': [
				row('g.m', [{ index: 0 }]),
				row('g.m', [{ index: 1 }], { status: 'failed' }),
			],
		};
		const firings = scopeFirings(groupBox({}, rows));
		expect(firings.map((f) => [f.framesKey, f.status])).toEqual([
			[JSON.stringify([{ index: 0 }]), 'completed'],
			[JSON.stringify([{ index: 1 }]), 'failed'],
		]);
	});

	it('a closed input reads closed, and a handed one keeps how it got there', () => {
		const rows = {
			'g.m': [row('g.m', [], { input: {}, closedPorts: ['in'] })],
			'g.n': [row('g.n', [], { input: written({ in: 5 }), providedPorts: ['in'] })],
		};
		const box = groupBox({
			decisions: [open('g', [])],
			inPorts: ['x', 'z'],
			legs: [leg('g', 'x__inner', 'g.m', 'in'), leg('g', 'z__inner', 'g.n', 'in')],
			inside: [...rows['g.m'], ...rows['g.n']],
		}, rows);
		const [firing] = scopeFirings(box);
		expect(firing.closedPorts).toEqual(['x']);
		expect(read(firing.input)).toEqual({ z: 5 });
		expect(firing.providedPorts).toEqual(['z']);
	});

	it('an include box reads its ports off the wires outside it', () => {
		// The call site `inc` at the top: its body runs one call frame deeper.
		const rows: Record<string, NodeFiring[]> = {
			a: [row('a', [], { output: written({ out: { name: 'ada' } }) })],
			b: [row('b', [], { input: written({ in: 'hi ada' }) })],
		};
		const body = row('@lib:inc.strip', [{ site: 'inc' }]);
		const box: ScopeBox = {
			id: 'inc',
			callPath: [],
			decisions: [open('inc', [])],
			inside: [body],
			copyOf: (r) => copyFrames(r.frames, 0, 0),
			legs: [leg('a', 'out', 'inc', 'who', ['name']), leg('inc', 'greeting', 'b', 'in')],
			rowsOf: (id) => rows[id] ?? [],
			inPorts: ['who'],
			outPorts: ['greeting'],
			terminal: undefined,
		};
		const [firing] = scopeFirings(box);
		expect(firing.status).toBe('completed');
		expect(read(firing.input)).toEqual({ who: 'ada' });
		expect(read(firing.output)).toEqual({ greeting: 'hi ada' });
	});
});

describe('copyFrames', () => {
	it('a member of a group at the copy itself', () => {
		expect(copyFrames([{ index: 2 }], 0, 0)).toEqual([{ index: 2 }]);
	});

	it("strips the iterations of the loops between the box and the row", () => {
		expect(copyFrames([{ index: 2 }, { index: 5 }], 0, 1)).toEqual([{ index: 2 }]);
	});

	it("cuts a body's row at the call its site makes", () => {
		expect(copyFrames([{ index: 1 }, { site: 'inc' }, { index: 3 }], 0, 0)).toEqual([{ index: 1 }]);
		// Inside a call already, the site below is the second call frame.
		expect(copyFrames([{ site: 'top' }, { site: 'Top.inc' }], 1, 0)).toEqual([{ site: 'top' }]);
	});
});
