// Layer-3 contract tests: the real ProjectionEngine wired against a fake
// host. The fake is dumb (scripted replies + append-only call log, no
// business logic); each test enqueues exactly the server responses it
// expects the engine to consume, so an unexpected RPC fails loudly.
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import type { ProjectDefinition } from '../types';
import type { EditOp } from '../../../protocol';
import { ProjectionEngine } from './engine.svelte';
import {
	FakeHost, addEdgeOp, addNodeOp, baseProject, catalog, inverse, node, ok, project, setConfigOp,
} from './engine.test-rig';

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

	it('a structural gesture flushes an earlier typing burst ahead of itself', async () => {
		// The typing op is still on its debounce when the removal records.
		// Sends must leave in QUEUE order (the config edit first, then the
		// delete); a structural op jumping the queue would make the host
		// apply the config edit to an already-deleted node ("node not
		// found") and roll back a gesture the user already saw succeed.
		const withText = project(baseProject().nodes.map(n => (n.id === 'a' ? { ...n, config: { text: 'hi' } } : n)));
		const afterDelete = project(withText.nodes.filter(n => n.id !== 'a'));
		host.editScript.push(ok(withText, 'v1'), ok(afterDelete, 'v2'));
		engine.recordEdit([setConfigOp('a', '"hi"')], (l) => l, 'cfg:a');
		engine.recordEdit([{ op: 'removeNode', node: 'a' }]);
		await engine.settled();
		const editCalls = host.calls.filter(c => c.kind === 'applyEdits');
		expect(editCalls).toHaveLength(2);
		expect((editCalls[0].payload as EditOp[])[0]).toMatchObject({ op: 'setConfig', node: 'a' });
		expect((editCalls[1].payload as EditOp[])[0]).toMatchObject({ op: 'removeNode', node: 'a' });
		expect(engine.pendingOps).toHaveLength(0);
		expect(host.notifications).toHaveLength(0);
	});

	it('case A: typing + add + connect burst inside the round-trip window all land', async () => {
		// Three gestures stack while the first round-trip is in flight. The
		// host applies them in order (chain-serialized); each reply carries
		// the cumulative truth.
		const p1 = project([...baseProject().nodes, node({ id: 'text_2', nodeType: 'Text', outputs: [{ name: 'value', portType: 'String', required: true }] })]);
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
		const withNode = project([...baseProject().nodes, node({ id: 'text_2', nodeType: 'Text', outputs: [{ name: 'value', portType: 'String', required: true }] })]);
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

	it('redo pressed in the same breath as undo still redoes (pop-at-run)', async () => {
		// The press outruns the undo's round-trip: at redo() press time the
		// redo stack is still empty (the undo task pushes its entry only on
		// completion). Popping at RUN time inside the chain makes the rapid
		// undo+redo pair land as the user meant it.
		const after = project([...baseProject().nodes, node({ id: 'text_2', nodeType: 'Text' })]);
		host.editScript.push(ok(after, 'v1'));
		engine.recordEdit([addNodeOp('text_2')]);
		await engine.settled();
		host.editScript.push(ok(baseProject(), 'v2')); // the undo's inverse replay
		host.editScript.push(ok(after, 'v3')); // the redo's replay
		engine.undo();
		engine.redo();
		await engine.settled();
		expect(engine.truth.weftCode).toBe('v3');
		expect(engine.undoStack).toHaveLength(1);
		expect(engine.redoStack).toHaveLength(0);
	});

	it('undo pressed twice rapidly undoes two entries', async () => {
		const p1 = project([...baseProject().nodes, node({ id: 'text_2', nodeType: 'Text' })]);
		const p2 = project([...p1.nodes, node({ id: 'text_3', nodeType: 'Text' })]);
		host.editScript.push(ok(p1, 'v1'), ok(p2, 'v2'));
		engine.recordEdit([addNodeOp('text_2')]);
		engine.recordEdit([addNodeOp('text_3')]);
		await engine.settled();
		host.editScript.push(ok(p1, 'v3'), ok(baseProject(), 'v4'));
		engine.undo();
		engine.undo();
		await engine.settled();
		expect(engine.truth.weftCode).toBe('v4');
		expect(engine.undoStack).toHaveLength(0);
		expect(engine.redoStack).toHaveLength(2);
	});

	it('a SOURCE edit between an undo press and its run refuses the undo LOUDLY', async () => {
		// The press targets a CONFIRMED entry (an in-flight or unsent one
		// revokes at press time instead); a new source gesture recorded
		// before the undo task ran makes the target's inverse text edit
		// stale, so the undo must refuse with a toast, never silently
		// reorder or vanish. The chain is kept busy with a HELD undo of a
		// newer entry so the press genuinely queues.
		const p1 = project([...baseProject().nodes, node({ id: 'text_2', nodeType: 'Text' })]);
		const p2 = project([...p1.nodes, node({ id: 'text_3', nodeType: 'Text' })]);
		host.editScript.push(ok(p1, 'v1'), ok(p2, 'v2'));
		engine.recordEdit([addNodeOp('text_2')]);
		engine.recordEdit([addNodeOp('text_3')]);
		await engine.settled(); // both CONFIRMED
		host.editScript.push(ok(p1, 'v3', true)); // undo of text_3: HELD
		engine.undo(); // its task pops text_3 and goes in flight
		for (let i = 0; i < 50 && host.held.length === 0; i++) await Promise.resolve();
		engine.undo(); // queued press, targets text_2's confirmed entry
		const p3 = project([...p1.nodes, node({ id: 'text_4', nodeType: 'Text' })]);
		host.editScript.push(ok(p3, 'v4'));
		engine.recordEdit([addNodeOp('text_4')]); // source gesture above the target
		host.held.shift()!();
		await engine.settled();
		// The first undo landed and the new edit stands; the second undo
		// refused loudly (a pending source entry sat above its target when
		// it ran) and changed nothing.
		expect(engine.truth.weftCode).toBe('v4');
		expect(engine.truth.project.nodes.some(n => n.id === 'text_2')).toBe(true);
		expect(host.notifications.some(n => n.title === 'Undo failed')).toBe(true);
	});

	it('a LAYOUT drag between an undo press and its run commutes: the undo still lands', async () => {
		// The user pressed undo of a CONFIRMED edit, then nudged a node
		// before the undo's task ran. A drag touches no source text, so it
		// cannot stale the target's inverse: the undo must still take
		// effect, and the drag must survive it.
		const p1 = project([...baseProject().nodes, node({ id: 'text_2', nodeType: 'Text' })]);
		host.editScript.push(ok(p1, 'v1'));
		engine.recordEdit([addNodeOp('text_2')]);
		await engine.settled(); // confirmed
		engine.undo(); // task queued on a microtask
		engine.recordEdit([], () => 'a @layout 9 9'); // sync nudge, entry above the target
		host.editScript.push(ok(baseProject(), 'v2')); // the undo's inverse replay
		await engine.settled();
		expect(engine.truth.weftCode).toBe('v2'); // the add was undone
		expect(engine.layoutCode.includes('a @layout 9 9')).toBe(true); // the nudge stands
		expect(host.notifications).toHaveLength(0);
	});

	it('undo of an IN-FLIGHT op takes effect at press time; the inverse lands behind the confirmation', async () => {
		const after = project([...baseProject().nodes, node({ id: 'text_2', nodeType: 'Text' })]);
		host.editScript.push(ok(after, 'v1', true)); // held: in flight
		// The send task starts on a microtask; wait for it to reach the host
		// so the op is genuinely IN FLIGHT (state 'sending') at press time.
		engine.recordEdit([addNodeOp('text_2')]);
		for (let i = 0; i < 50 && host.held.length === 0; i++) await Promise.resolve();
		expect(host.held).toHaveLength(1);
		engine.undo();
		// OPTIMISTIC: the projection drops the node at press time, before
		// any reply, so a long edit/undo chain reads coherently while the
		// server lags.
		expect(engine.visibleProject().nodes.some(n => n.id === 'text_2')).toBe(false);
		expect(engine.undoStack).toHaveLength(0);
		expect(engine.redoStack).toHaveLength(1);
		host.editScript.push(ok(baseProject(), 'v0')); // the inverse replay
		host.held.shift()!(); // release the confirmation
		await engine.settled();
		// The op confirmed, then its inverse applied immediately after.
		expect(engine.truth.weftCode).toBe('v0');
		expect(engine.pendingOps).toHaveLength(0);
		expect(engine.redoStack).toHaveLength(1);
	});

	it('an edit recorded AFTER an in-flight undo press lands after the revert', async () => {
		// The ordering guarantee behind the optimistic revoke: the user saw
		// text_2 disappear at press time, then added text_3; the server must
		// end at (base + text_3), applying the revert BEFORE the new add.
		const withT2 = project([...baseProject().nodes, node({ id: 'text_2', nodeType: 'Text' })]);
		host.editScript.push(ok(withT2, 'v1', true)); // held add, in flight
		engine.recordEdit([addNodeOp('text_2')]);
		for (let i = 0; i < 50 && host.held.length === 0; i++) await Promise.resolve();
		engine.undo(); // revokes text_2 optimistically
		const withT3 = project([...baseProject().nodes, node({ id: 'text_3', nodeType: 'Text' })]);
		host.editScript.push(ok(baseProject(), 'v2')); // the revert of text_2
		host.editScript.push(ok(withT3, 'v3')); // then text_3
		engine.recordEdit([addNodeOp('text_3')]);
		host.held.shift()!();
		await engine.settled();
		expect(engine.truth.weftCode).toBe('v3');
		expect(engine.truth.project.nodes.some(n => n.id === 'text_2')).toBe(false);
		expect(engine.truth.project.nodes.some(n => n.id === 'text_3')).toBe(true);
		expect(host.notifications).toHaveLength(0);
	});

	it('a redo pressed AFTER a queued undo does not defeat that undo', async () => {
		// Press order: undo(revoke the in-flight n2), undo(n1's older
		// CONFIRMED entry, which queues because the chain is busy),
		// redo. The redo must NOT act early (its sync path would plant a
		// fresh pending entry above the queued undo's target, making it
		// refuse "a newer edit landed" for a press the user made LATER):
		// with an undo press queued, the redo queues too, runs after it,
		// and redoes the LAST undone entry (n1, standard LIFO), leaving
		// the first undo's product (n2) on the stack for the next press.
		const withN1 = project([...baseProject().nodes, node({ id: 'n1', nodeType: 'Text' })]);
		const withBoth = project([...withN1.nodes, node({ id: 'n2', nodeType: 'Text' })]);
		host.editScript.push(ok(withN1, 'v1'));
		engine.recordEdit([addNodeOp('n1')]);
		await engine.settled(); // n1 CONFIRMED: a 'confirmed' entry on the stack

		host.editScript.push(ok(withBoth, 'v2', true)); // n2's add: HELD in flight
		engine.recordEdit([addNodeOp('n2')]);
		for (let i = 0; i < 50 && host.held.length === 0; i++) await Promise.resolve();
		engine.undo(); // press 1: revokes n2 at press time (sync)
		engine.undo(); // press 2: targets n1's confirmed entry, QUEUES
		engine.redo(); // press 3: sync re-apply of n2, pushes a pending entry

		host.editScript.push(ok(withN1, 'v3'));   // n2's revert (the revoke)
		host.editScript.push(ok(baseProject(), 'v4')); // press 2's inverse replay
		host.editScript.push(ok(withN1, 'v5'));   // the redo's replay of n1
		host.held.shift()!();
		await engine.settled();
		// No press was silently eaten or refused; the undo of n1 LANDED
		// (then the redo, running after it, brought n1 back: LIFO).
		expect(host.notifications).toHaveLength(0);
		expect(engine.truth.project.nodes.some(n => n.id === 'n1')).toBe(true);
		expect(engine.truth.project.nodes.some(n => n.id === 'n2')).toBe(false);
		// n2's reapply is still on the redo stack for the next press.
		expect(engine.redoStack.some(e => e.kind === 'reapply')).toBe(true);
	});

	it('a revoked op never leaves its mark behind when its send task never runs', async () => {
		// An op revoked while state==='sending' but whose task had not yet
		// reached the host (the chain was blocked on an earlier held reply):
		// the task's initial "still queued?" guard returns early, so the
		// consume sites in the try/catch never run. The revoke mark must not
		// survive that path, or the set grows without bound for the life of
		// the view.
		const withN1 = project([...baseProject().nodes, node({ id: 'n1', nodeType: 'Text' })]);
		host.editScript.push(ok(withN1, 'v1', true)); // op-1 held: blocks the chain
		engine.recordEdit([addNodeOp('n1')]);
		for (let i = 0; i < 50 && host.held.length === 0; i++) await Promise.resolve();
		// op-2 is recorded: sendPendingOp flips it to 'sending' synchronously
		// and enqueues, but the task cannot run behind the held op-1.
		engine.recordEdit([addNodeOp('n2')]);
		engine.undo(); // revokes op-2 while 'sending' -> marked
		host.editScript.push(ok(baseProject(), 'v2')); // op-1's own reply path
		host.held.shift()!();
		await engine.settled();
		expect(engine.revokedOpCount).toBe(0);
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

	it('a structural edit mid-typing flushes the burst ahead of itself, in order', async () => {
		// Sends leave in QUEUE order: the half-typed burst goes out first
		// (its debounce is cut short), then the structural op. The burst is
		// split (typing after the structural edit starts a NEW op), which is
		// the accepted cost of the ordering invariant.
		engine.recordEdit([setConfigOp('a', '"hel"')], (l) => l, 'cfg:a');
		const p1 = project(baseProject().nodes.map(n => (n.id === 'a' ? { ...n, config: { text: 'hel' } } : n)));
		const p2 = project([...p1.nodes, node({ id: 'text_2', nodeType: 'Text' })]);
		host.editScript.push(ok(p1, 'v1'), ok(p2, 'v2'));
		engine.recordEdit([addNodeOp('text_2')]);
		await engine.settled();
		const editCalls = host.calls.filter(c => c.kind === 'applyEdits');
		expect((editCalls[0].payload as EditOp[])[0]).toMatchObject({ op: 'setConfig', node: 'a' });
		expect((editCalls[1].payload as EditOp[])[0]).toMatchObject({ op: 'addNode', id: 'text_2' });
		expect(engine.pendingOps).toHaveLength(0);
		expect(engine.visibleProject().nodes.find(n => n.id === 'a')!.config.text).toBe('hel');
		expect(engine.truth.weftCode).toBe('v2');
		// Typing again after the flush starts a fresh burst (fresh undo unit).
		const p3 = project(p2.nodes.map(n => (n.id === 'a' ? { ...n, config: { text: 'hello' } } : n)));
		host.editScript.push(ok(p3, 'v3'));
		engine.recordEdit([setConfigOp('a', '"hello"')], (l) => l, 'cfg:a');
		vi.advanceTimersByTime(300);
		await engine.settled();
		expect(engine.truth.weftCode).toBe('v3');
		expect(engine.undoStack).toHaveLength(3);
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

	it('a reflow touching an optimistic node leaves no orphan when that node is rejected', async () => {
		// The add is in flight; an auto-organize (persistLayoutEdit) moves
		// EVERY visible node, including the optimistic one. The optimistic
		// node's entry must ride the add's own layer: when the add is
		// rejected, the node never existed, so no layout line for it may
		// survive to disk. The real node's reflowed position DOES survive.
		host.editScript.push({ kind: 'reject', reason: 'server says no', hold: true });
		host.resyncScript.push({ project: baseProject(), weftCode: 'v0' });
		engine.recordEdit([addNodeOp('text_2')], () => 'text_2 @layout 1 1');
		for (let i = 0; i < 50 && host.held.length === 0; i++) await Promise.resolve();
		engine.persistLayoutEdit(() => 'text_2 @layout 4 4\na @layout 7 7');
		host.held.shift()!();
		await engine.settled();
		expect(engine.layoutLog).toHaveLength(0);
		expect(posOf(engine.layoutBase, 'text_2')).toBeUndefined();
		expect(posOf(engine.layoutBase, 'a')).toBe('a @layout 7 7');
		expect(host.layoutSaves.every(l => !l.includes('text_2'))).toBe(true);
	});

	it('a cancelled edit stands down quietly: no confirmation, no persist, no toast', async () => {
		// The view switched away mid-round-trip and the request was swept:
		// the engine must neither confirm (a rebase would persist this dying
		// view's layout onto the newly watched file) nor roll back (nothing
		// to toast at a user who navigated away).
		host.editScript.push({ kind: 'ok', result: { inverse: null, project: null, weftCode: '', cancelled: true } });
		engine.recordEdit([addNodeOp('text_2')], () => 'text_2 @layout 1 1');
		await engine.settled();
		expect(engine.pendingOps).toHaveLength(0);
		expect(engine.undoStack).toHaveLength(0);
		expect(engine.layoutLog).toHaveLength(0);
		expect(host.layoutSaves).toHaveLength(0);
		expect(host.notifications).toHaveLength(0);
		expect(engine.truth.weftCode).toBe('v0');
	});

	it('an add with NO layout half of its own still owns its node key: rejection leaves no orphan', async () => {
		// Ownership comes from which pending op's fold INTRODUCES the node,
		// not from which layers happened to touch the key: an op recorded
		// with no layout mutation must still absorb a later drag of its
		// optimistic node, or the rejection leaves the drag's line on disk.
		host.editScript.push({ kind: 'reject', reason: 'server says no', hold: true });
		host.resyncScript.push({ project: baseProject(), weftCode: 'v0' });
		engine.recordEdit([addNodeOp('text_2')]); // deliberately no mutateLayout
		for (let i = 0; i < 50 && host.held.length === 0; i++) await Promise.resolve();
		engine.persistLayoutEdit((l) => l + (l ? '\n' : '') + 'text_2 @layout 7 7');
		host.held.shift()!();
		await engine.settled();
		expect(engine.layoutLog).toHaveLength(0);
		expect(posOf(engine.layoutBase, 'text_2')).toBeUndefined();
		expect(host.layoutSaves.every(l => !l.includes('text_2'))).toBe(true);
	});

	it('undo after a rejection cannot resurrect the dead node key', async () => {
		// A drag of the optimistic node produced a CONFIRMED history entry
		// whose inverse names the key. The rejection must scrub those layout
		// ops out of history, or one undo press writes the dead node's line
		// durably into the saved file.
		host.editScript.push({ kind: 'reject', reason: 'server says no', hold: true });
		host.resyncScript.push({ project: baseProject(), weftCode: 'v0' });
		engine.recordEdit([addNodeOp('text_2')], () => 'text_2 @layout 1 1');
		for (let i = 0; i < 50 && host.held.length === 0; i++) await Promise.resolve();
		engine.recordEdit([], (l) => l.replace('text_2 @layout 1 1', 'text_2 @layout 9 9')); // undoable drag
		host.held.shift()!();
		await engine.settled();
		expect(posOf(engine.layoutBase, 'text_2')).toBeUndefined();
		engine.undo();
		await engine.settled();
		expect(posOf(engine.layoutBase, 'text_2')).toBeUndefined();
		expect(engine.layoutCode.includes('text_2')).toBe(false);
		expect(host.layoutSaves.every(l => !l.includes('text_2'))).toBe(true);
	});

	it('a rejected delete does not make a reflow-dropped key-REMOVAL durable', async () => {
		// The symmetric half of ownership: a key DISAPPEARING because of a
		// pending op (here a delete of 'a') is owned by that op, so when the
		// op is rejected the removal drops with it and the node's saved
		// position survives. Judging only appearing keys made the removal
		// commit and erased the position forever.
		engine.recordEdit([], () => 'a @layout 1 1'); // a's position, durable
		await engine.settled();
		host.editScript.push({ kind: 'reject', reason: 'server says no', hold: true });
		host.resyncScript.push({ project: baseProject(), weftCode: 'v0' });
		engine.recordEdit([{ op: 'removeNode', node: 'a' }]);
		for (let i = 0; i < 50 && host.held.length === 0; i++) await Promise.resolve();
		// A reflow while the delete is in flight: 'a' is not visible, so the
		// reflow's output has no line for it (a removeEntry in the diff).
		engine.persistLayoutEdit(() => 'b @layout 7 7');
		host.held.shift()!();
		await engine.settled();
		expect(posOf(engine.layoutBase, 'a')).toBe('a @layout 1 1'); // survived
		expect(posOf(engine.layoutBase, 'b')).toBe('b @layout 7 7'); // committed
	});

	it('a scrubbed mixed-key drag entry still undoes its surviving half', async () => {
		// One drag moved BOTH the optimistic node and a real one. The
		// rejection scrubs the dead key out of the entry; the undo press,
		// matched by the entry's stable seq (the scrub cloned the object),
		// still reverts the real node's half.
		host.editScript.push({ kind: 'reject', reason: 'server says no', hold: true });
		host.resyncScript.push({ project: baseProject(), weftCode: 'v0' });
		engine.recordEdit([addNodeOp('text_2')], () => 'text_2 @layout 1 1');
		for (let i = 0; i < 50 && host.held.length === 0; i++) await Promise.resolve();
		engine.recordEdit([], () => 'text_2 @layout 9 9\na @layout 5 5'); // undoable mixed drag
		host.held.shift()!();
		await engine.settled();
		expect(posOf(engine.layoutBase, 'a')).toBe('a @layout 5 5');
		engine.undo();
		await engine.settled();
		expect(posOf(engine.layoutBase, 'a')).toBeUndefined(); // drag undone
		expect(engine.layoutCode.includes('text_2')).toBe(false); // dead key stays dead
	});

	it('a deep optimistic chain with the server at step 0 converges in press order', async () => {
		// The user's exact scenario: do, undo, do, undo, redo, do, undo,
		// redo, all while the server has confirmed NOTHING. Every press
		// must read instantly in the projection, and the server, catching
		// up, must land on exactly what the projection showed.
		const t = (id: string) => node({ id, nodeType: 'Text' });
		host.editScript.push(ok(project([...baseProject().nodes, t('n1')]), 'v1', true)); // n1: held
		engine.recordEdit([addNodeOp('n1')]);
		for (let i = 0; i < 50 && host.held.length === 0; i++) await Promise.resolve();
		engine.undo(); // revoke n1 instantly
		expect(engine.visibleProject().nodes.some(n => n.id === 'n1')).toBe(false);
		engine.recordEdit([addNodeOp('n2')]); // queued behind the held n1
		expect(engine.visibleProject().nodes.some(n => n.id === 'n2')).toBe(true);
		engine.undo(); // revoke n2 instantly (unsent: peeled, never sent)
		expect(engine.visibleProject().nodes.some(n => n.id === 'n2')).toBe(false);
		engine.redo(); // re-record n2
		engine.recordEdit([addNodeOp('n3')]);
		engine.undo(); // revoke n3
		expect(engine.visibleProject().nodes.some(n => n.id === 'n3')).toBe(false);
		// Server catches up. Replies in chain order: n1's revert, then the
		// redone n2, (n3 was revoked before its send, never reaches the host).
		host.editScript.push(ok(baseProject(), 'v2')); // revert n1
		host.editScript.push(ok(project([...baseProject().nodes, t('n2')]), 'v3')); // n2
		host.held.shift()!();
		await engine.settled();
		expect(engine.truth.weftCode).toBe('v3');
		expect(engine.truth.project.nodes.some(n => n.id === 'n1')).toBe(false);
		expect(engine.truth.project.nodes.some(n => n.id === 'n2')).toBe(true);
		expect(engine.truth.project.nodes.some(n => n.id === 'n3')).toBe(false);
		expect(engine.pendingOps).toHaveLength(0);
		expect(host.notifications).toHaveLength(0);
		// And the redo branch still holds n3 for one more press.
		host.editScript.push(ok(project([...baseProject().nodes, t('n2'), t('n3')]), 'v4'));
		engine.redo();
		await engine.settled();
		expect(engine.truth.project.nodes.some(n => n.id === 'n3')).toBe(true);
	});

	it('a key whose existence flips TWICE is owned by the op that decides its FINAL state', async () => {
		// op-1 removes 'a'; op-2 re-adds it. Both pending, so 'a' IS visible.
		// A reflow then positions it. Ownership must follow the op that
		// decides whether 'a' ends up existing (op-2, the LAST flip), not the
		// first one to touch it: routing the position to op-1 means a
		// rejection of op-1 drops the position of a node that still exists,
		// erasing it from the saved file with no way back.
		host.editScript.push({ kind: 'reject', reason: 'server says no', hold: true }); // op-1
		host.resyncScript.push({ project: baseProject(), weftCode: 'v0' });
		engine.recordEdit([{ op: 'removeNode', node: 'a' }]);
		for (let i = 0; i < 50 && host.held.length === 0; i++) await Promise.resolve();
		engine.recordEdit([addNodeOp('a')]); // op-2, queued behind op-1
		expect(engine.visibleProject().nodes.some(n => n.id === 'a')).toBe(true);
		engine.persistLayoutEdit(() => 'a @layout 7 7\nb @layout 1 1');
		host.editScript.push(ok(baseProject(), 'v2'));
		host.held.shift()!();
		await engine.settled();
		// 'a' survives in truth (the removal was refused), so its reflowed
		// position must survive with it.
		expect(engine.truth.project.nodes.some(n => n.id === 'a')).toBe(true);
		expect(posOf(engine.layoutCode, 'a')).toBe('a @layout 7 7');
	});

	it('a reflow touching an optimistic node turns durable with it on confirmation', async () => {
		const after = project([...baseProject().nodes, node({ id: 'text_2', nodeType: 'Text' })]);
		host.editScript.push(ok(after, 'v1', true));
		engine.recordEdit([addNodeOp('text_2')], () => 'text_2 @layout 1 1');
		for (let i = 0; i < 50 && host.held.length === 0; i++) await Promise.resolve();
		engine.persistLayoutEdit(() => 'text_2 @layout 4 4\na @layout 7 7');
		host.held.shift()!();
		await engine.settled();
		expect(engine.layoutLog).toHaveLength(0);
		expect(posOf(engine.layoutBase, 'text_2')).toBe('text_2 @layout 4 4');
		expect(posOf(engine.layoutBase, 'a')).toBe('a @layout 7 7');
	});

	it('a stale layout echo during an un-acked persist does not clobber the saved positions', async () => {
		// The user's "new node snaps to x=0" bug: the engine persisted the
		// node's position, but a parse fired before that write reached disk
		// and echoed the OLD file back; adopting it erased the entry, the
		// fallback placement kicked in, and the anchor pass made the wrong
		// spot durable. While any persist is un-acked, the echo predates our
		// base and must be refused.
		const after = project([...baseProject().nodes, node({ id: 'text_2', nodeType: 'Text' })]);
		host.editScript.push(ok(after, 'v1'));
		host.holdPersists = true;
		engine.recordEdit([addNodeOp('text_2')], () => 'text_2 @layout 120 80');
		await engine.settled(); // confirmed; rebase persisted (ack HELD)
		expect(posOf(engine.layoutBase, 'text_2')).toBe('text_2 @layout 120 80');
		// The echo: same truth, but layout read from PRE-WRITE disk (empty).
		engine.applyExternalSource(after, 'v1', '');
		expect(posOf(engine.layoutBase, 'text_2')).toBe('text_2 @layout 120 80'); // refused
		// Ack lands; a later echo carrying the CURRENT disk adopts normally.
		host.heldPersists.shift()!();
		await vi.advanceTimersByTimeAsync(0);
		engine.applyExternalSource(after, 'v1', 'text_2 @layout 300 40');
		expect(posOf(engine.layoutBase, 'text_2')).toBe('text_2 @layout 300 40');
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

	it('undoing an op does NOT erase a reflow position routed onto it', async () => {
		// A reflow (persistLayoutEdit, no undo entry of its own) that touches a
		// still-optimistic node rides a ROUTED layer owned by that node's op.
		// A REJECTION re-judges routed ops in place. An UNDO of the same op must
		// too: the reflow's position is a REAL position the user saw, authored
		// by a different gesture, and undoing the add is not consent to discard
		// it. dropLayoutLayers drops by owner alone, so the routed reflow of the
		// SURVIVING node dies with the op it never belonged to.
		host.editScript.push(ok(project([...baseProject().nodes, node({ id: 'text_2', nodeType: 'Text' })]), 'v1', true));
		engine.recordEdit([addNodeOp('text_2')], () => 'text_2 @layout 1 1');
		for (let i = 0; i < 50 && host.held.length === 0; i++) await Promise.resolve();
		// A reflow places BOTH the optimistic node and the pre-existing `a`.
		engine.persistLayoutEdit(() => 'text_2 @layout 50 60\na @layout 9 9');
		expect(posOf(engine.layoutCode, 'a')).toBe('a @layout 9 9');
		expect(posOf(engine.layoutCode, 'text_2')).toBe('text_2 @layout 50 60');
		// Undo the add at press time (the op is still pending: sync revoke).
		engine.undo();
		host.held.shift()!();
		await engine.settled();
		// `a` is not the undone op's node; its reflow position must survive.
		expect(posOf(engine.layoutBase, 'a')).toBe('a @layout 9 9');
		// text_2's node is genuinely gone with the undo, so its key goes too.
		expect(posOf(engine.layoutBase, 'text_2')).toBeUndefined();
		// The redo entry must carry the position the user LAST saw (50 60), not
		// the stale forward layout of the original gesture (1 1).
		const redoTop = engine.redoStack.at(-1)!;
		expect(redoTop.kind).toBe('reapply');
		expect(redoTop.kind === 'reapply' ? redoTop.layout : undefined)
			.toEqual([{ op: 'setEntry', id: 'text_2', entry: { x: 50, y: 60 } }]);
	});

	it('a reflow that DROPS the optimistic node line leaves no removeEntry in the redo', async () => {
		// The sibling above covers a reflow that MOVES the optimistic node.
		// This is the other half: a reflow whose output has no line for it at
		// all (the node is off-canvas, or the organizer emitted a smaller set),
		// which diffs to a removeEntry routed onto the op. That removeEntry is
		// the LAST op for the key, so a redo layout built as "last op per dead
		// key" would carry a REMOVAL as the gesture's forward layout. Replaying
		// it on redo deletes whatever line the key has then, and since the
		// re-attach diff comes back empty the op gets no layoutUndo either, so
		// the next undo cannot restore it. A removal is never a position: the
		// key died with the op, so the redo must simply carry nothing for it.
		engine.persistLayoutEdit(() => 'text_2 @layout 3 3\na @layout 0 0');
		await engine.settled();
		host.editScript.push(ok(project([...baseProject().nodes, node({ id: 'text_2', nodeType: 'Text' })]), 'v1', true));
		engine.recordEdit([addNodeOp('text_2')], (l) => l);
		for (let i = 0; i < 50 && host.held.length === 0; i++) await Promise.resolve();
		engine.persistLayoutEdit(() => 'a @layout 0 0'); // drops text_2's line
		engine.undo();
		const redoTop = engine.redoStack.at(-1)!;
		expect(redoTop.kind).toBe('reapply');
		const redoLayout = redoTop.kind === 'reapply' ? redoTop.layout : undefined;
		expect(redoLayout?.some(o => o.op === 'removeEntry')).not.toBe(true);
		host.held.shift()!();
		await engine.settled();
		// The pre-existing durable position survived the undo.
		expect(posOf(engine.layoutBase, 'text_2')).toBe('text_2 @layout 3 3');
		// Redo re-adds the node and must NOT delete that saved position.
		host.editScript.push(ok(project([...baseProject().nodes, node({ id: 'text_2', nodeType: 'Text' })]), 'v2'));
		engine.redo();
		await engine.settled();
		expect(posOf(engine.layoutBase, 'text_2')).toBe('text_2 @layout 3 3');
	});
});

describe('undo/redo press ordering', () => {
	it('redo consumes the LATEST undo product first, even when a sync revoke races a queued press', async () => {
		// Press 1 targets a CONFIRMED entry: it queues, and pushes its redo
		// entry only when the chain runs. Press 2 targets a PENDING op below it:
		// that path revokes SYNCHRONOUSLY and pushes its redo entry at once.
		// The stack therefore ends [press2, press1]: the later press sits BELOW
		// the earlier one, so the next redo (LIFO) replays press 1's product and
		// undoes the OLDER undo first. Redo must mirror undo order exactly.
		host.editScript.push(ok(baseProject(), 'v1', true));
		engine.recordEdit([addNodeOp('n2')]);
		for (let i = 0; i < 50 && host.held.length === 0; i++) await Promise.resolve();
		engine.recordEdit([], () => 'a @layout 7 7'); // confirmed layout entry on top
		engine.undo(); // press 1: queued task, target = the confirmed entry
		engine.undo(); // press 2: sync revoke of the pending op below it
		host.held.shift()!();
		await engine.settled();
		const stamps = engine.redoStack.map(e => e.pressStamp);
		// Press order must be ascending up the stack, so the top is the newest.
		expect(stamps).toEqual([...stamps].sort((x, y) => (x ?? 0) - (y ?? 0)));
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
