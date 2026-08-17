// Model-based stress tests for the projection engine: the real engine wired
// against a MODEL host that behaves like the production stack (it applies op
// batches to its own server-side project with the SAME pure applier the
// projection uses, refuses invalid batches with the server's vocabulary,
// serves resyncs from its current state, and replays text-edit inverses as
// project snapshots). On top of it:
//
// - Targeted scenario tests reproducing real user incident shapes (delete
//   while a config burst is still debouncing, a rejected add cascading into
//   its dependent connect, create/delete/recreate churn).
// - Seeded random walks: hundreds of interleaved user actions (typing,
//   structural edits, drags, undo/redo, held replies, injected rejections,
//   external truth advances, lock windows) per seed, then a quiescence check:
//   the engine's truth must equal the model server's project exactly, with an
//   empty queue, an empty layout log, and base == visible layout. Seeds are
//   fixed, so a failure names its seed and replays deterministically.
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import type { ProjectDefinition } from '../types';
import type { EditOp, TextEdit } from '../../../protocol';
import { ProjectionEngine, type EngineHost } from './engine.svelte';
import type { EditRpcResult } from './types';
import { applyOpsToProject } from './apply';
import { addEdgeOp, addNodeOp, baseProject, catalog, setConfigOp } from './engine.test-rig';

/** A host whose replies are COMPUTED, not scripted: `server` is the
 *  authoritative project, batches apply to it atomically via the shared pure
 *  applier, and inverses are snapshot indices (undo/redo restores). Faults
 *  are injected per-call (`rejectNext`) and replies can be held open
 *  (`holdNext`) to stretch the in-flight window a walk acts inside. */
class ModelHost implements EngineHost {
	server: ProjectDefinition;
	version = 0;
	notifications: Array<{ title: string; description: string }> = [];
	layoutSaves: string[] = [];
	snapBacks = 0;
	flashes = 0;
	nowMs = 1_000_000;
	/** Force the next applyEdits/applyTextEdit to refuse without applying. */
	rejectNext = 0;
	/** Gate the next N replies on an explicit release. */
	holdNext = 0;
	held: Array<() => void> = [];
	private snapshots: ProjectDefinition[] = [];

	constructor(initial: ProjectDefinition) {
		this.server = structuredClone(initial);
	}

	private reply(result: () => EditRpcResult | Error): Promise<EditRpcResult> {
		const settle = (): Promise<EditRpcResult> => {
			const r = result();
			return r instanceof Error ? Promise.reject(r) : Promise.resolve(r);
		};
		if (this.holdNext <= 0) return settle();
		this.holdNext--;
		return new Promise((resolve, reject) => {
			this.held.push(() => void settle().then(resolve, reject));
		});
	}

	/** Apply `fn` to the server as one transaction; the inverse restores the
	 *  pre-state snapshot. Shared by op batches and text-edit replays. */
	private transact(fn: (current: ProjectDefinition) => ProjectDefinition): EditRpcResult | Error {
		if (this.rejectNext > 0) {
			this.rejectNext--;
			return new Error('injected server rejection');
		}
		let next: ProjectDefinition;
		try {
			next = fn(this.server);
		} catch (e) {
			return e instanceof Error ? e : new Error(String(e));
		}
		this.snapshots.push(structuredClone(this.server));
		const inverse: TextEdit = { start: this.snapshots.length - 1, end: 0, text: '' };
		this.server = next;
		this.version++;
		return { inverse, project: structuredClone(this.server), weftCode: `v${this.version}` };
	}

	applyEdits(ops: EditOp[]): Promise<EditRpcResult> {
		return this.reply(() => this.transact((current) => applyOpsToProject(current, ops, catalog)));
	}

	applyTextEdit(edit: TextEdit): Promise<EditRpcResult> {
		return this.reply(() => this.transact(() => structuredClone(this.snapshots[edit.start])));
	}

	resyncSource(): Promise<{ project: ProjectDefinition; weftCode: string } | null> {
		return Promise.resolve({ project: structuredClone(this.server), weftCode: `v${this.version}` });
	}

	releaseAllHeld(): void {
		const held = this.held;
		this.held = [];
		for (const release of held) release();
	}

	persistLayout(layoutCode: string): Promise<void> {
		this.layoutSaves.push(layoutCode);
		return Promise.resolve();
	}
	notify(title: string, description: string): void {
		this.notifications.push({ title, description });
	}
	snapBack(): void {
		this.snapBacks++;
	}
	flashSave(): void {
		this.flashes++;
	}
	now(): number {
		return this.nowMs;
	}
}

/** Normalize a project for convergence comparison: order-independent, only
 *  the fields the walk mutates. */
function shape(p: ProjectDefinition): string {
	return JSON.stringify({
		nodes: [...p.nodes]
			.sort((x, y) => x.id.localeCompare(y.id))
			.map(n => ({ id: n.id, nodeType: n.nodeType, config: n.config })),
		edges: [...p.edges]
			.map(e => `${e.source}.${e.sourceHandle}->${e.target}.${e.targetHandle}`)
			.sort(),
	});
}

/** Deterministic PRNG (mulberry32): a failing seed replays exactly. */
function prng(seed: number): () => number {
	let a = seed >>> 0;
	return () => {
		a = (a + 0x6d2b79f5) >>> 0;
		let t = a;
		t = Math.imul(t ^ (t >>> 15), t | 1);
		t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
}

/** Update-or-append one node's line in the layout text. */
function upsertLayout(layout: string, id: string, x: number, y: number): string {
	const line = `${id} @layout ${x} ${y}`;
	const lines = layout.split('\n').filter(l => l.trim() !== '');
	const idx = lines.findIndex(l => l.startsWith(id + ' '));
	if (idx >= 0) lines[idx] = line;
	else lines.push(line);
	return lines.join('\n');
}

let host: ModelHost;
let engine: ProjectionEngine;

beforeEach(() => {
	vi.useFakeTimers();
	host = new ModelHost(baseProject());
	engine = new ProjectionEngine(
		{
			applyEdits: (ops) => host.applyEdits(ops),
			applyTextEdit: (e) => host.applyTextEdit(e),
			resyncSource: () => host.resyncSource(),
			persistLayout: (l) => host.persistLayout(l),
			notify: (t, d) => host.notify(t, d),
			snapBack: () => host.snapBack(),
			flashSave: () => host.flashSave(),
			now: () => host.now(),
		},
		catalog,
		{ project: baseProject(), weftCode: 'v0' },
		'',
	);
});
afterEach(() => {
	vi.useRealTimers();
});

/** Drain everything: release held replies (repeatedly: a release can enqueue
 *  work that holds again), flush typing, run out timers, settle the chain. */
async function drain(): Promise<void> {
	for (let i = 0; i < 200; i++) {
		// Release BEFORE settling: the chain may be parked on a held reply
		// that only got registered after the previous round's release ran,
		// so `settled()` against a blocked chain would never resolve. The
		// timer advance runs microtasks too, letting a just-dispatched RPC
		// register its hold for the next round's release.
		host.releaseAllHeld();
		engine.flushTypingOps();
		await vi.advanceTimersByTimeAsync(1000);
		host.nowMs += 1000;
		if (host.held.length === 0 && engine.pendingOps.length === 0) {
			await engine.settled();
			if (host.held.length === 0 && engine.pendingOps.length === 0) return;
		}
	}
	throw new Error('drain did not quiesce in 200 rounds');
}

/** The quiescence invariants every scenario and walk must land on. */
function expectConverged(): void {
	expect(engine.pendingOps).toHaveLength(0);
	expect(shape(engine.truth.project)).toBe(shape(host.server));
	expect(shape(engine.visibleProject())).toBe(shape(engine.truth.project));
	expect(engine.layoutLog).toHaveLength(0);
	expect(engine.layoutCode).toBe(engine.layoutBase);
}

describe('model host scenarios (real incident shapes)', () => {
	it('typing into a node, then deleting that node: config flushes first, both land, no toast', async () => {
		engine.recordEdit([setConfigOp('a', '"half-typed"')], (l) => l, 'cfg:a');
		engine.recordEdit([{ op: 'removeNode', node: 'a' }]);
		await drain();
		expectConverged();
		expect(host.server.nodes.some(n => n.id === 'a')).toBe(false);
		expect(host.notifications).toHaveLength(0);
	});

	it('typing into one node, deleting ANOTHER, then more typing: all land in order', async () => {
		engine.recordEdit([setConfigOp('a', '"one"')], (l) => l, 'cfg:a');
		engine.recordEdit([{ op: 'removeNode', node: 'b' }]);
		engine.recordEdit([setConfigOp('a', '"two"')], (l) => l, 'cfg:a');
		await drain();
		expectConverged();
		expect(host.server.nodes.find(n => n.id === 'a')!.config.text).toBe('two');
		expect(host.notifications).toHaveLength(0);
	});

	it('a rejected add cascades: the dependent connect is dropped, nothing wedges', async () => {
		// The debug-node incident shape: add fails server-side, so the edge
		// recorded against the optimistic node must be dropped (its target
		// never existed on the server), both roll back visibly, and the
		// engine converges back to the server's truth.
		host.rejectNext = 1;
		engine.recordEdit([addNodeOp('debug_1')]);
		engine.recordEdit([addEdgeOp('debug_1', 'b')]);
		await drain();
		expectConverged();
		expect(host.server.nodes.some(n => n.id === 'debug_1')).toBe(false);
		expect(host.notifications.length).toBeGreaterThanOrEqual(1);
		expect(host.notifications.some(n => n.title === 'Edit failed')).toBe(true);
	});

	it('create, delete, recreate the same id back-to-back', async () => {
		engine.recordEdit([addNodeOp('text_2')]);
		engine.recordEdit([{ op: 'removeNode', node: 'text_2' }]);
		engine.recordEdit([addNodeOp('text_2')]);
		await drain();
		expectConverged();
		expect(host.server.nodes.filter(n => n.id === 'text_2')).toHaveLength(1);
		expect(host.notifications).toHaveLength(0);
	});

	it('add + connect + type + delete, all inside one held round-trip window', async () => {
		host.holdNext = 1;
		engine.recordEdit([addNodeOp('text_2')]);
		engine.recordEdit([addEdgeOp('text_2', 'b')]);
		engine.recordEdit([setConfigOp('text_2', '"burst"')], (l) => l, 'cfg:text_2');
		engine.recordEdit([{ op: 'removeNode', node: 'a' }]);
		await drain();
		expectConverged();
		expect(host.server.nodes.some(n => n.id === 'text_2')).toBe(true);
		expect(host.server.edges).toHaveLength(1);
		expect(host.server.nodes.some(n => n.id === 'a')).toBe(false);
		expect(host.notifications).toHaveLength(0);
	});

	it('undo/redo ping-pong across mixed history converges', async () => {
		engine.recordEdit([addNodeOp('text_2')]);
		engine.recordEdit([], (l) => upsertLayout(l, 'text_2', 10, 20));
		engine.recordEdit([setConfigOp('text_2', '"v"')], (l) => l, 'cfg:text_2');
		await drain();
		engine.undo();
		engine.undo();
		engine.redo();
		engine.undo();
		engine.redo();
		engine.redo();
		await drain();
		expectConverged();
		expect(host.server.nodes.find(n => n.id === 'text_2')!.config.text).toBe('v');
	});

	it('an external truth advance echoing an in-flight op does not double or wedge', async () => {
		// The echo race: the server applied the add and something external
		// delivers that truth BEFORE the reply lands. The refold drops the
		// pending op as a duplicate; the held reply must then not regress
		// truth or resurrect the op.
		host.holdNext = 1;
		engine.recordEdit([addNodeOp('text_2')]);
		await vi.advanceTimersByTimeAsync(1);
		// The op is on the server even though the reply is held.
		expect(host.held).toHaveLength(1);
		engine.applyExternalSource(structuredClone(host.server), `v${host.version}`, engine.layoutBase);
		await drain();
		expectConverged();
		expect(host.server.nodes.filter(n => n.id === 'text_2')).toHaveLength(1);
	});
});

describe('seeded random walks', () => {
	const SEEDS = Array.from({ length: 60 }, (_, i) => i + 1);

	it.each(SEEDS)('walk with seed %i converges to the model server', async (seed) => {
		const rand = prng(seed);
		const pick = <T>(xs: T[]): T => xs[Math.floor(rand() * xs.length)];
		let minted = 0;
		let externals = 0;

		const visibleTextNodes = (): string[] =>
			engine.visibleProject().nodes.filter(n => n.nodeType === 'Text').map(n => n.id);
		const visibleNodes = (): string[] => engine.visibleProject().nodes.map(n => n.id);

		const actions: Array<() => void | Promise<void>> = [
			() => engine.recordEdit([addNodeOp(`n${++minted}`)], (l) => upsertLayout(l, `n${minted}`, minted, minted)),
			() => {
				const ids = visibleNodes();
				if (ids.length > 0) engine.recordEdit([{ op: 'removeNode', node: pick(ids) }]);
			},
			() => {
				const sources = visibleTextNodes();
				const hasDebug = visibleNodes().includes('b');
				if (sources.length > 0 && hasDebug) engine.recordEdit([addEdgeOp(pick(sources), 'b')]);
			},
			() => {
				const ids = visibleNodes();
				if (ids.length === 0) return;
				const id = pick(ids);
				engine.recordEdit([setConfigOp(id, `"${Math.floor(rand() * 100)}"`)], (l) => l, `cfg:${id}`);
			},
			() => {
				const ids = visibleNodes();
				if (ids.length > 0) {
					engine.recordEdit([], (l) => upsertLayout(l, pick(ids), Math.floor(rand() * 500), Math.floor(rand() * 500)));
				}
			},
			() => {
				// A reflow that DROPS a random node's line (auto-organize can
				// emit a smaller set): exercises removeEntry ops through the
				// ownership/peel machinery, which upserts alone never reach.
				const ids = visibleNodes();
				if (ids.length === 0) return;
				const dropped = pick(ids);
				engine.persistLayoutEdit((l) =>
					l.split('\n').filter(line => !line.startsWith(dropped + ' ')).join('\n'));
			},
			() => engine.undo(),
			() => engine.redo(),
			() => {
				// A rapid undo/redo burst in one breath: the tight
				// press-against-press shapes (Z Z Y, Z Y Z Y) are where the
				// ordering bugs hid, and uniform single presses rarely
				// produce them.
				for (let i = 0, n = 2 + Math.floor(rand() * 3); i < n; i++) {
					if (rand() < 0.5) engine.undo();
					else engine.redo();
				}
			},
			() => engine.flushTypingOps(),
			() => {
				host.rejectNext++;
			},
			() => {
				host.holdNext++;
			},
			() => {
				if (host.held.length > 0) host.held.shift()!();
			},
			() => {
				// An out-of-band edit landing as an external truth advance (the
				// text tab / an AI edit): mutate the server directly, then echo.
				host.server = applyOpsToProject(host.server, [addNodeOp(`ext${++externals}`)], catalog);
				host.version++;
				engine.applyExternalSource(structuredClone(host.server), `v${host.version}`, engine.layoutBase);
			},
			() => {
				// The 1s code-edit auto-lock engages; preflight refuses source
				// gestures until the walk's timer advances move past it.
				engine.setCodeEditTouched();
			},
			async () => {
				await vi.advanceTimersByTimeAsync(Math.floor(rand() * 400));
			},
			async () => {
				host.nowMs += 1100; // release the auto-lock window
				await Promise.resolve();
			},
		];

		for (let step = 0; step < 120; step++) {
			await actions[Math.floor(rand() * actions.length)]()!;
		}
		// Injected-but-unconsumed faults would poison drain's flush sends
		// nondeterministically; consume the counters, keep real held replies.
		host.rejectNext = 0;
		host.holdNext = 0;
		await drain();
		expectConverged();
	});
});
