import { describe, it, expect } from 'vitest';
import type { ProjectDefinition, NodeInstance } from '../types';
import { runPreflight } from './preflight';
import { isLogicLocked, lockReasonText, type LockState } from './types';
import type { ProjectionCatalog } from './apply';
import type { EditOp } from '../../../shared/protocol';

const catalog: ProjectionCatalog = {
  Text: { defaultInputs: [], defaultOutputs: [{ name: 'value', portType: 'String', required: true }] },
  Debug: { defaultInputs: [{ name: 'data', portType: 'T', required: true }], defaultOutputs: [] },
};

function node(partial: Partial<NodeInstance> & { id: string; nodeType: string }): NodeInstance {
  return {
    label: null, config: {}, position: { x: 0, y: 0 },
    inputs: [], outputs: [], features: {}, scope: [],
    ...partial,
  };
}

function fixture(): ProjectDefinition {
  return {
    id: 'p1',
    nodes: [
      node({ id: 'G', nodeType: 'Group', label: 'G' }),
      node({ id: 'G.a', nodeType: 'Text', parentId: 'G', config: { parentId: 'G' } }),
      node({ id: 'a', nodeType: 'Text' }),
      node({ id: 'b', nodeType: 'Debug' }),
      node({ id: 'c', nodeType: 'Debug' }),
    ],
    edges: [
      { id: 'e1', source: 'a', target: 'b', sourceHandle: 'value', targetHandle: 'data' },
    ],
    createdAt: '', updatedAt: '',
  };
}

const unlocked: LockState = { codeEditLockUntil: null, lockGraphLogic: false };
const NOW = 1_000_000;

const addEdge = (source: string, target: string, scopeGroup: string | null = null): EditOp =>
  ({ op: 'addEdge', source, sourcePort: 'value', target, targetPort: 'data', scopeGroup });

describe('lock state', () => {
  it('auto-lock engages until the deadline and expires on its own', () => {
    const lock: LockState = { codeEditLockUntil: NOW + 1000, lockGraphLogic: false };
    expect(isLogicLocked(lock, NOW)).toBe(true);
    expect(isLogicLocked(lock, NOW + 1001)).toBe(false);
    expect(lockReasonText(lock, NOW)).toMatch(/Weft code is being edited/);
  });

  it('explicit lock carries its reason', () => {
    const lock: LockState = { codeEditLockUntil: null, lockGraphLogic: true, lockReason: 'AI is editing' };
    expect(isLogicLocked(lock, NOW)).toBe(true);
    expect(lockReasonText(lock, NOW)).toBe('Graph logic locked (AI is editing)');
  });

  it('rejects any source op while locked, with the lock reason', () => {
    const lock: LockState = { codeEditLockUntil: NOW + 1000, lockGraphLogic: false };
    const r = runPreflight([{ op: 'removeNode', node: 'a' }], fixture(), lock, catalog, NOW);
    expect(r).toEqual({ ok: false, reason: 'Graph logic locked (Weft code is being edited)' });
  });

  it('an empty batch (layout-only gesture) bypasses the lock', () => {
    const lock: LockState = { codeEditLockUntil: NOW + 1000, lockGraphLogic: false };
    expect(runPreflight([], fixture(), lock, catalog, NOW).ok).toBe(true);
  });
});

describe('cycle rule', () => {
  it('rejects a connection that closes a cycle', () => {
    const p = fixture();
    p.nodes.push(node({ id: 'd', nodeType: 'Text' }));
    p.edges.push({ id: 'e2', source: 'b', target: 'c', sourceHandle: 'x', targetHandle: 'data' });
    const r = runPreflight([addEdge('c', 'a')], p, unlocked, catalog, NOW);
    // c -> a would close a (a -> b -> c -> a) cycle... only if a feeds the
    // chain; a -> b exists, b -> c exists, so c -> a closes it.
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.reason).toMatch(/cycle/);
  });

  it('accepts a straight-line connection', () => {
    expect(runPreflight([addEdge('a', 'c')], fixture(), unlocked, catalog, NOW).ok).toBe(true);
  });

  it('ignores __inner pass-through edges when hunting cycles', () => {
    const p = fixture();
    p.edges.push({ id: 'e3', source: 'G', target: 'G.a', sourceHandle: 'in__inner', targetHandle: 'data' });
    expect(runPreflight([addEdge('a', 'c')], p, unlocked, catalog, NOW).ok).toBe(true);
  });

  it('rejects a cyclic BATCH where each edge alone is acyclic', () => {
    // a->b and b->a: neither edge alone closes a cycle against the original
    // graph, but together they do. The batch must be folded op-by-op so the
    // second edge is checked against the first already applied.
    const p = fixture();
    p.edges = []; // start clean so only the batch's edges matter
    const r = runPreflight([addEdge('a', 'b'), addEdge('b', 'a')], p, unlocked, catalog, NOW);
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.reason).toMatch(/cycle/);
  });
});

describe('scope rule', () => {
  it('rejects a connection across scopes', () => {
    const r = runPreflight(
      [{ op: 'addEdge', source: 'G.a', sourcePort: 'value', target: 'b', targetPort: 'data', scopeGroup: null }],
      fixture(), unlocked, catalog, NOW,
    );
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.reason).toMatch(/scope/);
  });

  it('accepts self wiring from a child inside the scope group, rejects it from outside', () => {
    const inside = runPreflight(
      [{ op: 'addEdge', source: 'a', sourcePort: 'value', target: 'self', targetPort: 'out', scopeGroup: 'G' }],
      fixture(), unlocked, catalog, NOW,
    );
    // `a` resolves to G.a inside scopeGroup G; self is G's body: same scope.
    expect(inside.ok).toBe(true);
    const outside = runPreflight(
      [{ op: 'addEdge', source: 'b', sourcePort: 'value', target: 'self', targetPort: 'out', scopeGroup: 'G' }],
      fixture(), unlocked, catalog, NOW,
    );
    // `b` only exists at top level: inside G it doesn't resolve, and the
    // dry-run apply reports the missing ref.
    expect(outside.ok).toBe(false);
  });
});

describe('dry-run apply (the semantic backstop)', () => {
  it('rejects a move that would orphan connections, with the server wording', () => {
    const r = runPreflight([{ op: 'moveNodeScope', node: 'a', targetGroup: 'G' }], fixture(), unlocked, catalog, NOW);
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.reason).toMatch(/connections/);
  });

  it('rejects a duplicate id before the round-trip', () => {
    const r = runPreflight([{ op: 'addNode', id: 'a', nodeType: 'Text', parentGroup: null }], fixture(), unlocked, catalog, NOW);
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.reason).toMatch(/already exists/);
  });

  it('passes a clean batch through', () => {
    const r = runPreflight([
      { op: 'addNode', id: 'd', nodeType: 'Text', parentGroup: null },
      addEdge('d', 'c'),
    ], fixture(), unlocked, catalog, NOW);
    expect(r.ok).toBe(true);
  });
});
