// Layer-3 contract tests: the real ProjectionEngine wired against a fake
// host. The fake is dumb (scripted replies + append-only call log, no
// business logic); each test enqueues exactly the server responses it
// expects the engine to consume, so an unexpected RPC fails loudly.
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import type { ProjectDefinition, NodeInstance } from '../types';
import type { EditOp, TextEdit } from '../../../shared/protocol';
import { ProjectionEngine, type EngineHost } from './engine.svelte';
import type { EditRpcResult } from './types';
import type { ProjectionCatalog } from './apply';

const catalog: ProjectionCatalog = {
	Text: { defaultInputs: [], defaultOutputs: [{ name: 'value', portType: 'String', required: true, configurable: false }] },
	Debug: { defaultInputs: [{ name: 'data', portType: 'T', required: true, configurable: false }], defaultOutputs: [] },
};

function node(partial: Partial<NodeInstance> & { id: string; nodeType: string }): NodeInstance {
	return {
		label: null, config: {}, position: { x: 0, y: 0 },
		inputs: [], outputs: [], features: {}, scope: [],
		...partial,
	};
}

function project(nodes: NodeInstance[], edges: ProjectDefinition['edges'] = []): ProjectDefinition {
	return { id: 'p1', nodes, edges, createdAt: '', updatedAt: '' };
}

function baseProject(): ProjectDefinition {
	return project([
		node({ id: 'a', nodeType: 'Text', outputs: [{ name: 'value', portType: 'String', required: true, configurable: false }] }),
		node({ id: 'b', nodeType: 'Debug', inputs: [{ name: 'data', portType: 'T', required: true, configurable: false }] }),
	]);
}

type EditScriptEntry =
	| { kind: 'ok'; result: EditRpcResult; hold?: boolean }
	| { kind: 'reject'; reason: string; hold?: boolean };

class FakeHost implements EngineHost {
	calls: Array<{ kind: string; payload?: unknown }> = [];
	notifications: Array<{ title: string; description: string }> = [];
	layoutSaves: string[] = [];
	snapBacks = 0;
	flashes = 0;
	nowMs = 1_000_000;
	editScript: EditScriptEntry[] = [];
	resyncScript: Array<{ project: ProjectDefinition; weftCode: string } | null> = [];
	/** Resolvers for `hold: true` entries, released by the test. */
	held: Array<() => void> = [];

	private run(entry: EditScriptEntry): Promise<EditRpcResult> {
		const settle = (): Promise<EditRpcResult> =>
			entry.kind === 'ok' ? Promise.resolve(entry.result) : Promise.reject(new Error(entry.reason));
		if (!entry.hold) return settle();
		return new Promise((resolve, reject) => {
			this.held.push(() => {
				if (entry.kind === 'ok') resolve(entry.result);
				else reject(new Error(entry.reason));
			});
		});
	}

	applyEdits(ops: EditOp[]): Promise<EditRpcResult> {
		this.calls.push({ kind: 'applyEdits', payload: ops });
		const entry = this.editScript.shift();
		if (!entry) return Promise.reject(new Error('FakeHost: unexpected applyEdits call'));
		return this.run(entry);
	}

	applyTextEdit(edit: TextEdit): Promise<EditRpcResult> {
		this.calls.push({ kind: 'applyTextEdit', payload: edit });
		const entry = this.editScript.shift();
		if (!entry) return Promise.reject(new Error('FakeHost: unexpected applyTextEdit call'));
		return this.run(entry);
	}

	resyncSource(): Promise<{ project: ProjectDefinition; weftCode: string } | null> {
		this.calls.push({ kind: 'resyncSource' });
		if (this.resyncScript.length === 0) return Promise.reject(new Error('FakeHost: unexpected resyncSource call'));
		return Promise.resolve(this.resyncScript.shift()!);
	}

	persistLayout(layoutCode: string): void {
		this.layoutSaves.push(layoutCode);
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

const inverse: TextEdit = { start: 0, end: 5, text: 'old' };

function ok(p: ProjectDefinition, weftCode = 'code', hold = false): EditScriptEntry {
	return { kind: 'ok', result: { inverse, project: p, weftCode }, hold };
}

const addNodeOp = (id: string): EditOp => ({ op: 'addNode', id, nodeType: 'Text', parentGroup: null });
const addEdgeOp = (source: string, target: string): EditOp =>
	({ op: 'addEdge', source, sourcePort: 'value', target, targetPort: 'data', scopeGroup: null });
const setConfigOp = (nodeId: string, value: string): EditOp =>
	({ op: 'setConfig', node: nodeId, key: 'text', value });

let host: FakeHost;
let engine: ProjectionEngine;

beforeEach(() => {
	vi.useFakeTimers();
	host = new FakeHost();
	engine = new ProjectionEngine(host, catalog, { project: baseProject(), weftCode: 'v0' }, '');
});
afterEach(() => {
	vi.useRealTimers();
});

describe('gesture to confirmation', () => {
	it('an op projects immediately, sends, and truth advances on confirm', async () => {
		const after = project([...baseProject().nodes, node({ id: 'text_2', nodeType: 'Text' })]);
		host.editScript.push(ok(after, 'v1'));
		engine.recordEdit([addNodeOp('text_2')]);
		// Optimistic: the projection shows the node before any reply.
		expect(engine.visibleProject().nodes.some(n => n.id === 'text_2')).toBe(true);
		expect(engine.pendingOps).toHaveLength(1);
		await engine.settled();
		expect(engine.pendingOps).toHaveLength(0);
		expect(engine.truth.weftCode).toBe('v1');
		expect(engine.undoStack).toHaveLength(1);
		expect(engine.undoStack[0].kind).toBe('confirmed');
	});

	it('case A: typing + add + connect burst inside the round-trip window all land', async () => {
		// Three gestures stack while the first round-trip is in flight. The
		// host applies them in order (chain-serialized); each reply carries
		// the cumulative truth.
		const p1 = project([...baseProject().nodes, node({ id: 'text_2', nodeType: 'Text', outputs: [{ name: 'value', portType: 'String', required: true, configurable: false }] })]);
		const p2 = project(p1.nodes, [{ id: 'e1', source: 'text_2', target: 'b', sourceHandle: 'value', targetHandle: 'data' }]);
		const p3 = project(p2.nodes.map(n => (n.id === 'a' ? { ...n, config: { text: 'hello' } } : n)), p2.edges);
		host.editScript.push(ok(p1, 'v1'), ok(p2, 'v2'), ok(p3, 'v3'));

		engine.recordEdit([addNodeOp('text_2')]);
		engine.recordEdit([addEdgeOp('text_2', 'b')]);
		engine.recordEdit([setConfigOp('a', '"hello"')], (l) => l, 'cfg:a');
		// All three visible instantly.
		const visible = engine.visibleProject();
		expect(visible.nodes.some(n => n.id === 'text_2')).toBe(true);
		expect(visible.edges.some(e => e.source === 'text_2' && e.target === 'b')).toBe(true);
		expect(visible.nodes.find(n => n.id === 'a')!.config.text).toBe('hello');

		vi.advanceTimersByTime(300); // typing debounce fires
		await engine.settled();
		expect(engine.pendingOps).toHaveLength(0);
		expect(engine.truth.weftCode).toBe('v3');
		expect(host.calls.filter(c => c.kind === 'applyEdits')).toHaveLength(3);
		// No phantom edges, no lost typing: truth IS the final state.
		expect(engine.truth.project.nodes.find(n => n.id === 'a')!.config.text).toBe('hello');
	});

	it('case B: a server rejection rolls back the op, resyncs, keeps independents', async () => {
		const withNode = project([...baseProject().nodes, node({ id: 'text_2', nodeType: 'Text', outputs: [{ name: 'value', portType: 'String', required: true, configurable: false }] })]);
		host.editScript.push({ kind: 'reject', reason: 'node not found: ghost' });
		host.editScript.push(ok(withNode, 'v2'));
		host.resyncScript.push({ project: baseProject(), weftCode: 'v0' });

		// Op 1 will be rejected; op 2 is independent and must survive.
		engine.recordEdit([{ op: 'removeConfig', node: 'a', key: 'text' }]);
		engine.recordEdit([addNodeOp('text_2')]);
		await engine.settled();

		expect(engine.pendingOps).toHaveLength(0);
		expect(engine.truth.weftCode).toBe('v2');
		expect(host.notifications.some(n => n.title === 'Edit failed' && n.description.includes('node not found'))).toBe(true);
		// The rejected op left no history entry; the surviving op did.
		expect(engine.undoStack).toHaveLength(1);
		// Resync happened exactly once.
		expect(host.calls.filter(c => c.kind === 'resyncSource')).toHaveLength(1);
	});

	it('case C: an external truth advance drops a typing op whose node vanished', () => {
		engine.recordEdit([setConfigOp('a', '"typing"')], (l) => l, 'cfg:a');
		expect(engine.pendingOps).toHaveLength(1);
		// The text tab removes node `a` and the parse lands.
		const without = project(baseProject().nodes.filter(n => n.id !== 'a'));
		engine.applyExternalSource(without, 'v9', '');
		expect(engine.pendingOps).toHaveLength(0);
		expect(engine.truth.weftCode).toBe('v9');
		expect(host.notifications.some(n => n.title === 'Edit failed' && n.description.includes('not found'))).toBe(true);
		// The dropped typing op's history entry is gone too.
		expect(engine.undoStack).toHaveLength(0);
	});

	it('an external truth advance mid-flight, then a held OK reply, does not regress truth', async () => {
		// Op X is in flight (held). An external parseResult drops X while the
		// RPC awaits. When X's OK reply finally lands, the post-await membership
		// re-check must bail: truth stays the newer external one, no stale stash.
		const afterX = project(baseProject().nodes.map(n => (n.id === 'a' ? { ...n, config: {} } : n)));
		host.editScript.push(ok(afterX, 'vX', true)); // held
		// Op X targets node `a`; the external truth will remove `a`, so X can
		// no longer apply and drops.
		engine.recordEdit([{ op: 'removeConfig', node: 'a', key: 'text' }], () => 'a @layout 5 5');
		// Wait for the send task to reach the held await.
		for (let i = 0; i < 50 && host.held.length === 0; i++) await Promise.resolve();
		expect(host.held).toHaveLength(1);
		// External parse removes the op's target node; X drops through failPendingOp.
		const without = project(baseProject().nodes.filter(n => n.id !== 'a'));
		engine.applyExternalSource(without, 'vEXT', '');
		expect(engine.truth.weftCode).toBe('vEXT');
		expect(engine.pendingOps).toHaveLength(0);
		const layoutAfterDrop = engine.layoutCode;
		// Release X's OK reply: the post-await guard must bail.
		host.held.shift()!();
		await engine.settled();
		expect(engine.truth.weftCode).toBe('vEXT'); // NOT regressed to vX
		expect(engine.layoutCode).toBe(layoutAfterDrop); // inverse not re-applied
	});

	it('an external truth advance mid-flight, then a held REJECT reply, is idempotent', async () => {
		host.editScript.push({ kind: 'reject', reason: 'node not found: ghost', hold: true });
		engine.recordEdit([{ op: 'removeConfig', node: 'a', key: 'text' }], () => 'a @layout 5 5');
		for (let i = 0; i < 50 && host.held.length === 0; i++) await Promise.resolve();
		const without = project(baseProject().nodes.filter(n => n.id !== 'a'));
		engine.applyExternalSource(without, 'vEXT', '');
		const notifyCount = host.notifications.length;
		const resyncCount = host.calls.filter(c => c.kind === 'resyncSource').length;
		const layoutAfterDrop = engine.layoutCode;
		host.held.shift()!(); // release the rejection
		await engine.settled();
		// The stale rejection must NOT double-toast, double-resync, or
		// double-roll-back the layout.
		expect(host.notifications.length).toBe(notifyCount);
		expect(host.calls.filter(c => c.kind === 'resyncSource').length).toBe(resyncCount);
		expect(engine.layoutCode).toBe(layoutAfterDrop);
		expect(engine.truth.weftCode).toBe('vEXT');
	});

	it('a dependent queued op drops when its producer is rejected', async () => {
		// Op 1 (addNode) rejected by the host; op 2 (edge from that node)
		// becomes unapplyable after the resync and drops with its own toast.
		host.editScript.push({ kind: 'reject', reason: 'id already exists in scope: text_2' });
		host.resyncScript.push({ project: baseProject(), weftCode: 'v0' });
		engine.recordEdit([addNodeOp('text_2')]);
		engine.recordEdit([addEdgeOp('text_2', 'b')]);
		await engine.settled();
		expect(engine.pendingOps).toHaveLength(0);
		expect(engine.truth.weftCode).toBe('v0');
		// Two failure toasts: the rejected op and its invalidated dependent.
		expect(host.notifications.filter(n => n.title === 'Edit failed')).toHaveLength(2);
		// The dependent's send task saw the op gone and never called the host again.
		expect(host.calls.filter(c => c.kind === 'applyEdits')).toHaveLength(1);
	});
});

describe('preflight', () => {
	it('rejects a doomed gesture locally: toast + snap-back, nothing sent', () => {
		engine.recordEdit([addNodeOp('a')]); // duplicate id
		expect(host.notifications[0].title).toBe('Edit rejected');
		expect(host.snapBacks).toBe(1);
		expect(engine.pendingOps).toHaveLength(0);
		expect(host.calls).toHaveLength(0);
	});
});

describe('the graph-logic lock', () => {
	it('case D: auto-lock rejects logical edits within 1s; layout gestures pass', () => {
		engine.setCodeEditTouched();
		engine.recordEdit([addNodeOp('text_2')]);
		expect(host.notifications[0].description).toMatch(/Weft code is being edited/);
		expect(host.calls).toHaveLength(0);
		// Layout-only gesture bypasses the lock entirely.
		engine.recordEdit([], () => 'a @layout 10 10');
		expect(host.layoutSaves).toHaveLength(1);
	});

	it('case E: the auto-lock releases on its own after 1s', async () => {
		engine.setCodeEditTouched();
		host.nowMs += 1001;
		const after = project([...baseProject().nodes, node({ id: 'text_2', nodeType: 'Text' })]);
		host.editScript.push(ok(after, 'v1'));
		engine.recordEdit([addNodeOp('text_2')]);
		await engine.settled();
		expect(engine.truth.weftCode).toBe('v1');
	});

	it('case F: a keystroke burst keeps the lock engaged, then releases 1s after the last', async () => {
		for (let i = 0; i < 20; i++) {
			host.nowMs += 500; // AI streaming every 500ms
			engine.setCodeEditTouched();
			engine.recordEdit([addNodeOp(`burst_${i}`)]);
		}
		expect(host.notifications).toHaveLength(20);
		expect(host.calls).toHaveLength(0);
		host.nowMs += 1001;
		const after = project([...baseProject().nodes, node({ id: 'text_2', nodeType: 'Text' })]);
		host.editScript.push(ok(after, 'v1'));
		engine.recordEdit([addNodeOp('text_2')]);
		// Past the deadline: the gesture enters the queue and confirms.
		expect(engine.pendingOps).toHaveLength(1);
		await engine.settled();
		expect(engine.truth.weftCode).toBe('v1');
	});

	it('case G: the explicit lock rejects with its reason and releases on demand', () => {
		engine.setGraphLogicLock(true, 'AI is editing');
		engine.recordEdit([addNodeOp('text_2')]);
		expect(host.notifications[0].description).toBe('Graph logic locked (AI is editing)');
		engine.setGraphLogicLock(false);
		expect(engine.lockGraphLogic).toBe(false);
		expect(engine.lockReason).toBeUndefined();
	});

	it('case H: a doc-version race rejection reads as code-was-edited', async () => {
		host.editScript.push({ kind: 'reject', reason: 'code-was-edited' });
		host.resyncScript.push({ project: baseProject(), weftCode: 'v1' });
		engine.recordEdit([addNodeOp('text_2')]);
		await engine.settled();
		expect(host.notifications[0].description).toMatch(/Weft code was edited during the round-trip/);
		expect(engine.truth.weftCode).toBe('v1');
	});

	it('case I: truth advances from code edits WHILE locked; lock stays engaged', () => {
		engine.setGraphLogicLock(true, 'AI is editing');
		const after = project([...baseProject().nodes, node({ id: 'from_code', nodeType: 'Text' })]);
		engine.applyExternalSource(after, 'v5', '');
		expect(engine.truth.weftCode).toBe('v5');
		expect(engine.visibleProject().nodes.some(n => n.id === 'from_code')).toBe(true);
		expect(engine.lockGraphLogic).toBe(true);
	});
});

describe('undo / redo', () => {
	it('undo of a confirmed op replays the inverse; redo mirrors', async () => {
		const after = project([...baseProject().nodes, node({ id: 'text_2', nodeType: 'Text' })]);
		host.editScript.push(ok(after, 'v1'));
		engine.recordEdit([addNodeOp('text_2')]);
		await engine.settled();

		// Undo: the inverse text edit replays; the host answers with v0 truth.
		host.editScript.push(ok(baseProject(), 'v0'));
		engine.undo();
		await engine.settled();
		expect(engine.truth.weftCode).toBe('v0');
		expect(engine.undoStack).toHaveLength(0);
		expect(engine.redoStack).toHaveLength(1);
		const replay = host.calls.filter(c => c.kind === 'applyTextEdit');
		expect(replay).toHaveLength(1);

		// Redo: mirrors back to v1.
		host.editScript.push(ok(after, 'v1'));
		engine.redo();
		await engine.settled();
		expect(engine.truth.weftCode).toBe('v1');
		expect(engine.undoStack).toHaveLength(1);
		expect(engine.redoStack).toHaveLength(0);
	});

	it('undo of an unsent typing op peels it locally; redo re-records and sends', async () => {
		engine.recordEdit([setConfigOp('a', '"draft"')], (l) => l, 'cfg:a');
		expect(engine.pendingOps).toHaveLength(1);
		engine.undo();
		await engine.settled();
		expect(engine.pendingOps).toHaveLength(0);
		expect(host.calls).toHaveLength(0); // never sent
		expect(engine.visibleProject().nodes.find(n => n.id === 'a')!.config.text).toBeUndefined();
		expect(engine.redoStack).toHaveLength(1);

		// Redo re-records the op as a fresh gesture and sends it.
		const after = project(baseProject().nodes.map(n => (n.id === 'a' ? { ...n, config: { text: 'draft' } } : n)));
		host.editScript.push(ok(after, 'v1'));
		engine.redo();
		await engine.settled();
		expect(engine.truth.weftCode).toBe('v1');
		expect(engine.undoStack).toHaveLength(1);
		expect(engine.undoStack[0].kind).toBe('confirmed');
	});

	it('undo pressed while the op is in flight consumes the stashed confirmation', async () => {
		const after = project([...baseProject().nodes, node({ id: 'text_2', nodeType: 'Text' })]);
		host.editScript.push(ok(after, 'v1', true)); // held: in flight
		engine.recordEdit([addNodeOp('text_2')]);
		engine.undo(); // queued behind the send
		host.editScript.push(ok(baseProject(), 'v0')); // the undo's inverse replay
		// The send task starts on a microtask; wait for it to reach the host.
		for (let i = 0; i < 50 && host.held.length === 0; i++) await Promise.resolve();
		expect(host.held).toHaveLength(1);
		host.held.shift()!(); // release the confirmation
		await engine.settled();
		// The op confirmed, then the queued undo replayed its inverse.
		expect(engine.truth.weftCode).toBe('v0');
		expect(engine.pendingOps).toHaveLength(0);
		expect(engine.redoStack).toHaveLength(1);
	});

	it('a redo rejected by a transient lock keeps the redo entry replayable', async () => {
		// Record + undo an unsent typing op, so a 'reapply' redo entry exists.
		engine.recordEdit([setConfigOp('a', '"draft"')], (l) => l, 'cfg:a');
		engine.undo();
		await engine.settled();
		expect(engine.redoStack).toHaveLength(1);
		// Engage the 1s code-edit lock, then redo: preflight rejects it.
		engine.setCodeEditTouched();
		engine.redo();
		await engine.settled();
		expect(host.notifications.some(n => n.title === 'Redo failed')).toBe(true);
		// The entry must SURVIVE (throw -> caller restores), not vanish.
		expect(engine.redoStack).toHaveLength(1);
		// Let the lock expire; redo now succeeds.
		host.nowMs += 1001;
		const afterDraft = project(baseProject().nodes.map(n => (n.id === 'a' ? { ...n, config: { text: 'draft' } } : n)));
		host.editScript.push(ok(afterDraft, 'v1'));
		engine.redo();
		await engine.settled();
		expect(engine.redoStack).toHaveLength(0);
		expect(engine.undoStack).toHaveLength(1);
	});

	it('a confirmation with no truth (project null) records the undo but does not advance truth', async () => {
		host.editScript.push({ kind: 'ok', result: { inverse, project: null, weftCode: '' } });
		engine.recordEdit([addNodeOp('text_2')], () => 'text_2 @layout 1 1');
		await engine.settled();
		expect(engine.pendingOps).toHaveLength(0);
		expect(engine.truth.weftCode).toBe('v0'); // truth unchanged
		expect(engine.undoStack).toHaveLength(1);
		expect(engine.undoStack[0].kind).toBe('confirmed');
	});

	it('a layout-only gesture is one confirmed undo unit; undo restores the layout', async () => {
		engine.recordEdit([], () => 'a @layout 50 60');
		expect(engine.undoStack).toHaveLength(1);
		engine.undo();
		await engine.settled();
		expect(engine.layoutCode).toBe('');
		expect(engine.redoStack).toHaveLength(1);
		engine.redo();
		await engine.settled();
		expect(engine.layoutCode.trim()).toBe('a @layout 50 60');
	});

	it('a new forward edit clears the redo branch', async () => {
		engine.recordEdit([], () => 'a @layout 50 60');
		engine.undo();
		await engine.settled();
		expect(engine.redoStack).toHaveLength(1);
		engine.recordEdit([], () => 'a @layout 1 2');
		expect(engine.redoStack).toHaveLength(0);
	});

	it('a rejected edit does not branch history: the redo stack is restored', async () => {
		engine.recordEdit([], () => 'a @layout 50 60');
		engine.undo();
		await engine.settled();
		expect(engine.redoStack).toHaveLength(1);
		// A forward edit that the server rejects must give the redo branch back.
		host.editScript.push({ kind: 'reject', reason: 'invalid edit argument: nope' });
		host.resyncScript.push({ project: baseProject(), weftCode: 'v0' });
		engine.recordEdit([addNodeOp('text_2')]);
		expect(engine.redoStack).toHaveLength(0); // cleared synchronously at record
		await engine.settled();
		expect(engine.redoStack).toHaveLength(1); // restored after the rejection
	});

	it('a SERVER-rejected redo keeps the redo entry replayable (not just preflight)', async () => {
		// Record + undo an unsent typing op so a 'reapply' redo entry exists.
		engine.recordEdit([setConfigOp('a', '"draft"')], (l) => l, 'cfg:a');
		engine.undo();
		await engine.settled();
		expect(engine.redoStack).toHaveLength(1);
		// Redo: it re-records + sends, but the SERVER refuses it (a transient
		// race, not the preflight lock). The redo entry must come back.
		host.editScript.push({ kind: 'reject', reason: 'code-was-edited' });
		host.resyncScript.push({ project: baseProject(), weftCode: 'v0' });
		engine.redo();
		await engine.settled();
		expect(engine.redoStack).toHaveLength(1); // NOT destroyed
		// And it's genuinely replayable: a clean retry confirms.
		const after = project(baseProject().nodes.map(n => (n.id === 'a' ? { ...n, config: { text: 'draft' } } : n)));
		host.editScript.push(ok(after, 'v1'));
		engine.redo();
		await engine.settled();
		expect(engine.redoStack).toHaveLength(0);
		expect(engine.undoStack).toHaveLength(1);
	});
});

describe('config typing', () => {
	it('keystrokes coalesce into ONE pending op, sent once after the debounce', async () => {
		engine.recordEdit([setConfigOp('a', '"h"')], (l) => l, 'cfg:a');
		engine.recordEdit([setConfigOp('a', '"he"')], (l) => l, 'cfg:a');
		engine.recordEdit([setConfigOp('a', '"hello"')], (l) => l, 'cfg:a');
		expect(engine.pendingOps).toHaveLength(1);
		expect(engine.visibleProject().nodes.find(n => n.id === 'a')!.config.text).toBe('hello');
		const after = project(baseProject().nodes.map(n => (n.id === 'a' ? { ...n, config: { text: 'hello' } } : n)));
		host.editScript.push(ok(after, 'v1'));
		vi.advanceTimersByTime(300);
		await engine.settled();
		expect(host.calls.filter(c => c.kind === 'applyEdits')).toHaveLength(1);
		expect(host.flashes).toBe(1);
		expect(engine.truth.weftCode).toBe('v1');
	});

	it('a REFUSED typing op restores the redo branch its record cleared', async () => {
		// Set up a redo branch: record + undo a layout-only gesture.
		engine.recordEdit([], () => 'a @layout 1 1');
		engine.undo();
		await engine.settled();
		expect(engine.redoStack).toHaveLength(1);
		// Type into a field (first keystroke clears redo), then flush; the host
		// refuses it. The refused edit branched nothing, so redo must come back.
		engine.recordEdit([setConfigOp('a', '"x"')], (l) => l, 'cfg:a');
		expect(engine.redoStack).toHaveLength(0); // cleared by the typing op's record
		host.editScript.push({ kind: 'reject', reason: 'refused' });
		host.resyncScript.push({ project: baseProject(), weftCode: 'v0' });
		engine.flushTypingOps();
		await engine.settled();
		// The redo branch is restored (the refused typing op didn't really branch).
		expect(engine.redoStack).toHaveLength(1);
	});

	it('a structural edit mid-typing does not disturb the typing op', async () => {
		engine.recordEdit([setConfigOp('a', '"hel"')], (l) => l, 'cfg:a');
		const p1 = project([...baseProject().nodes, node({ id: 'text_2', nodeType: 'Text' })]);
		host.editScript.push(ok(p1, 'v1'));
		engine.recordEdit([addNodeOp('text_2')]); // sent immediately
		await engine.settled();
		// The structural op confirmed; the typing op re-applied on the new truth.
		expect(engine.pendingOps).toHaveLength(1);
		expect(engine.visibleProject().nodes.find(n => n.id === 'a')!.config.text).toBe('hel');
		const p2 = project(p1.nodes.map(n => (n.id === 'a' ? { ...n, config: { text: 'hel' } } : n)));
		host.editScript.push(ok(p2, 'v2'));
		vi.advanceTimersByTime(300);
		await engine.settled();
		expect(engine.pendingOps).toHaveLength(0);
		expect(engine.truth.weftCode).toBe('v2');
	});
});

describe('layout fold (base + log)', () => {
	const posOf = (layout: string, id: string): string | undefined =>
		layout.split('\n').find(l => l.startsWith(id + ' '));

	it('a layout-only drag of a key a PENDING source op also moved survives that op being rejected', async () => {
		// The reshape's core guarantee. Source op A moves `a` to (5,5) [pending,
		// held]. Then a layout-only drag moves `a` to (9,9). A is rejected. The
		// LATER drag must win, not be clobbered by an absolute inverse.
		host.editScript.push({ kind: 'reject', reason: 'server says no', hold: true });
		host.resyncScript.push({ project: baseProject(), weftCode: 'v0' });
		engine.recordEdit([{ op: 'removeConfig', node: 'a', key: 'text' }], () => 'a @layout 5 5');
		for (let i = 0; i < 50 && host.held.length === 0; i++) await Promise.resolve();
		expect(posOf(engine.layoutCode, 'a')).toBe('a @layout 5 5');
		// Layout-only drag of the same key to (9,9), AFTER the pending op.
		engine.recordEdit([], () => 'a @layout 9 9');
		expect(posOf(engine.layoutCode, 'a')).toBe('a @layout 9 9');
		// A rejects: its layer drops, the drag (later layer) stays.
		host.held.shift()!();
		await engine.settled();
		expect(posOf(engine.layoutCode, 'a')).toBe('a @layout 9 9');
		// The surviving drag is now DURABLE (rebased into the base + persisted).
		expect(engine.layoutLog).toHaveLength(0);
		expect(posOf(engine.layoutBase, 'a')).toBe('a @layout 9 9');
		expect(host.layoutSaves.at(-1)).toContain('a @layout 9 9');
	});

	it('latest write wins: a drag after a pending add survives the add CONFIRMING', async () => {
		const after = project([...baseProject().nodes, node({ id: 'text_2', nodeType: 'Text' })]);
		host.editScript.push(ok(after, 'v1', true)); // held add
		engine.recordEdit([addNodeOp('text_2')], () => 'text_2 @layout 1 1');
		for (let i = 0; i < 50 && host.held.length === 0; i++) await Promise.resolve();
		// Drag the still-pending node to (7,7).
		engine.recordEdit([], () => 'text_2 @layout 7 7');
		expect(posOf(engine.layoutCode, 'text_2')).toBe('text_2 @layout 7 7');
		// The add confirms: the drag (later) must still win, not snap to (1,1).
		host.held.shift()!();
		await engine.settled();
		expect(posOf(engine.layoutCode, 'text_2')).toBe('text_2 @layout 7 7');
		// At rest the log rebased into the base (nothing pending).
		expect(engine.layoutLog).toHaveLength(0);
		expect(posOf(engine.layoutBase, 'text_2')).toBe('text_2 @layout 7 7');
	});

	it('an unconfirmed source op never persists its optimistic layout to disk', async () => {
		// Only DURABLE layout reaches disk; a pending op's position is persisted
		// only when it rebases (confirm). A rejected op must never have written.
		host.editScript.push({ kind: 'reject', reason: 'no', hold: true });
		host.resyncScript.push({ project: baseProject(), weftCode: 'v0' });
		engine.recordEdit([{ op: 'removeConfig', node: 'a', key: 'text' }], () => 'a @layout 5 5');
		for (let i = 0; i < 50 && host.held.length === 0; i++) await Promise.resolve();
		// While pending+held: the optimistic position is visible but NOT on disk.
		expect(posOf(engine.layoutCode, 'a')).toBe('a @layout 5 5');
		expect(host.layoutSaves.some(s => s.includes('a @layout 5 5'))).toBe(false);
		host.held.shift()!();
		await engine.settled();
		// After rejection, still never persisted.
		expect(host.layoutSaves.some(s => s.includes('a @layout 5 5'))).toBe(false);
	});

	it('a confirmed source op folds its layout into the durable base on quiescence', async () => {
		const after = project([...baseProject().nodes, node({ id: 'text_2', nodeType: 'Text' })]);
		host.editScript.push(ok(after, 'v1'));
		engine.recordEdit([addNodeOp('text_2')], () => 'text_2 @layout 3 3');
		await engine.settled();
		expect(engine.layoutLog).toHaveLength(0);
		expect(posOf(engine.layoutBase, 'text_2')).toBe('text_2 @layout 3 3');
		// Undo reverts the layout (and replays the source inverse).
		host.editScript.push(ok(baseProject(), 'v0'));
		engine.undo();
		await engine.settled();
		expect(posOf(engine.layoutBase, 'text_2')).toBeUndefined();
	});
});

describe('burst with periodic rejections', () => {
	it('50 ops, every 10th rejected: resyncs land, survivors confirm, queue drains', async () => {
		// The fake server applies adds cumulatively; rejected ops contribute
		// nothing. Replies are scripted in order.
		let serverNodes = baseProject().nodes;
		let v = 0;
		const expectFail = (i: number) => (i + 1) % 10 === 0;
		for (let i = 0; i < 50; i++) {
			if (expectFail(i)) {
				host.editScript.push({ kind: 'reject', reason: `invalid edit argument: op ${i}` });
				host.resyncScript.push({ project: project(serverNodes), weftCode: `v${v}` });
			} else {
				serverNodes = [...serverNodes, node({ id: `n_${i}`, nodeType: 'Text' })];
				v++;
				host.editScript.push(ok(project(serverNodes), `v${v}`));
			}
		}
		for (let i = 0; i < 50; i++) {
			engine.recordEdit([addNodeOp(`n_${i}`)]);
		}
		await engine.settled();
		expect(engine.pendingOps).toHaveLength(0);
		expect(engine.truth.weftCode).toBe(`v${v}`);
		expect(engine.truth.project.nodes).toHaveLength(2 + 45);
		expect(host.notifications.filter(n => n.title === 'Edit failed')).toHaveLength(5);
		// History holds exactly the 45 confirmed ops.
		expect(engine.undoStack).toHaveLength(45);
	});
});
