// Shared rig for the projection-engine contract tests: a dumb scripted
// FakeHost (append-only call log, test-enqueued replies, no business logic)
// plus the small project/op builders. Used by engine.test.ts (targeted
// scenarios) and engine.stress.test.ts (model-based random walks).
import type { ProjectDefinition, NodeInstance } from '../types';
import type { EditOp, TextEdit } from '../../../protocol';
import type { EngineHost } from './engine.svelte';
import type { EditRpcResult } from './types';
import type { ProjectionCatalog } from './apply';

export const catalog: ProjectionCatalog = {
	Text: { defaultInputs: [], defaultOutputs: [{ name: 'value', portType: 'String', required: true }] },
	Debug: { defaultInputs: [{ name: 'data', portType: 'T', required: true }], defaultOutputs: [] },
};

export function node(partial: Partial<NodeInstance> & { id: string; nodeType: string }): NodeInstance {
	return {
		label: null, config: {}, position: { x: 0, y: 0 },
		inputs: [], outputs: [], features: {}, scope: [],
		...partial,
	};
}

export function project(nodes: NodeInstance[], edges: ProjectDefinition['edges'] = []): ProjectDefinition {
	return { id: 'p1', nodes, edges, createdAt: '', updatedAt: '' };
}

export function baseProject(): ProjectDefinition {
	return project([
		node({ id: 'a', nodeType: 'Text', outputs: [{ name: 'value', portType: 'String', required: true }] }),
		node({ id: 'b', nodeType: 'Debug', inputs: [{ name: 'data', portType: 'T', required: true }] }),
	]);
}

export type EditScriptEntry =
	| { kind: 'ok'; result: EditRpcResult; hold?: boolean }
	| { kind: 'reject'; reason: string; hold?: boolean };

export class FakeHost implements EngineHost {
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

	/** With `holdPersists`, saves record but their ack is held until the
	 *  test releases it (models the disk write still in transit). */
	holdPersists = false;
	heldPersists: Array<() => void> = [];
	persistLayout(layoutCode: string): Promise<void> {
		this.layoutSaves.push(layoutCode);
		if (this.holdPersists) {
			return new Promise((resolve) => {
				this.heldPersists.push(resolve);
			});
		}
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

export const inverse: TextEdit = { start: 0, end: 5, text: 'old' };

export function ok(p: ProjectDefinition, weftCode = 'code', hold = false): EditScriptEntry {
	return { kind: 'ok', result: { inverse, project: p, weftCode }, hold };
}

export const addNodeOp = (id: string): EditOp => ({ op: 'addNode', id, nodeType: 'Text', parentGroup: null });
export const addEdgeOp = (source: string, target: string): EditOp =>
	({ op: 'addEdge', source, sourcePort: 'value', target, targetPort: 'data', scopeGroup: null });
export const setConfigOp = (nodeId: string, value: string): EditOp =>
	({ op: 'setConfig', node: nodeId, key: 'text', value });
