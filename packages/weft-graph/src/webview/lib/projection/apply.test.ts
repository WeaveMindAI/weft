import { describe, it, expect } from 'vitest';
import type { ProjectDefinition, NodeInstance } from '../types';
import { applyOpsToProject, foldOps, type ProjectionCatalog } from './apply';
import type { PendingOp } from './types';
import type { EditOp } from '../../../shared/protocol';

const catalog: ProjectionCatalog = {
  Text: {
    defaultInputs: [],
    defaultOutputs: [{ name: 'value', portType: 'String', required: true }],
  },
  Debug: {
    defaultInputs: [{ name: 'data', portType: 'T', required: true }],
    defaultOutputs: [],
  },
};

function node(partial: Partial<NodeInstance> & { id: string; nodeType: string }): NodeInstance {
  return {
    label: null,
    config: {},
    position: { x: 0, y: 0 },
    inputs: [],
    outputs: [],
    features: {},
    scope: [],
    ...partial,
  };
}

/** text_1 -> debug_1 at top level, plus group G containing G.inner_text wired
 *  to G's `out` port (self.out = inner_text.value). */
function fixture(): ProjectDefinition {
  return {
    id: 'p1',
    nodes: [
      node({
        id: 'G', nodeType: 'Group', label: 'G',
        inputs: [],
        outputs: [{ name: 'out', portType: 'String', required: true }],
      }),
      node({
        id: 'G.inner_text', nodeType: 'Text', label: null, parentId: 'G',
        config: { parentId: 'G' }, scope: ['G'],
        outputs: [{ name: 'value', portType: 'String', required: true }],
      }),
      node({
        id: 'text_1', nodeType: 'Text',
        outputs: [{ name: 'value', portType: 'String', required: true }],
        config: { text: 'hello' },
      }),
      node({
        id: 'debug_1', nodeType: 'Debug',
        inputs: [{ name: 'data', portType: 'T', required: true }],
      }),
    ],
    edges: [
      { id: 'e1', source: 'text_1', target: 'debug_1', sourceHandle: 'value', targetHandle: 'data' },
      { id: 'e2', source: 'G.inner_text', target: 'G', sourceHandle: 'value', targetHandle: 'out__inner' },
    ],
    createdAt: '', updatedAt: '',
  };
}

const ops = (...list: EditOp[]): EditOp[] => list;

describe('applyOpsToProject: config and label', () => {
  it('setConfig parses the formatted token back to a value', () => {
    const p = applyOpsToProject(fixture(), ops({ op: 'setConfig', node: 'text_1', key: 'text', value: '"world"' }), catalog);
    expect(p.nodes.find((n) => n.id === 'text_1')!.config.text).toBe('world');
  });

  it('setConfig parses numbers, booleans, JSON, heredocs and @file markers', () => {
    const apply = (value: string) =>
      applyOpsToProject(fixture(), ops({ op: 'setConfig', node: 'text_1', key: 'k', value }), catalog)
        .nodes.find((n) => n.id === 'text_1')!.config.k;
    expect(apply('42')).toBe(42);
    expect(apply('true')).toBe(true);
    expect(apply('{\n  "a": 1\n}')).toEqual({ a: 1 });
    expect(apply('```\nline1\nline2\n```')).toBe('line1\nline2');
    expect(apply('@file("prompt.md")')).toEqual({ __weftFileRef: { path: 'prompt.md', type: 'String', marker: 'file' } });
    expect(apply('@asset("assets/pic.png", Image)')).toEqual({ __weftFileRef: { path: 'assets/pic.png', type: 'Image', marker: 'asset' } });
  });

  it('removeConfig deletes the key', () => {
    const p = applyOpsToProject(fixture(), ops({ op: 'removeConfig', node: 'text_1', key: 'text' }), catalog);
    expect('text' in p.nodes.find((n) => n.id === 'text_1')!.config).toBe(false);
  });

  it('setConfig on a container is a kind mismatch', () => {
    expect(() => applyOpsToProject(fixture(), ops({ op: 'setConfig', node: 'G', key: 'k', value: '1' }), catalog))
      .toThrow(/not a Node/);
  });

  it('setLabel sets and clears a node label', () => {
    let p = applyOpsToProject(fixture(), ops({ op: 'setLabel', node: 'text_1', label: 'My Text' }), catalog);
    expect(p.nodes.find((n) => n.id === 'text_1')!.label).toBe('My Text');
    p = applyOpsToProject(p, ops({ op: 'setLabel', node: 'text_1', label: null }), catalog);
    expect(p.nodes.find((n) => n.id === 'text_1')!.label).toBeNull();
  });

  it('resolves by unique local id and rejects ambiguous refs', () => {
    const p = fixture();
    expect(applyOpsToProject(p, ops({ op: 'setConfig', node: 'inner_text', key: 'k', value: '1' }), catalog)
      .nodes.find((n) => n.id === 'G.inner_text')!.config.k).toBe(1);
    p.nodes.push(node({ id: 'inner_text', nodeType: 'Text' }));
    expect(() => applyOpsToProject(p, ops({ op: 'setConfig', node: 'G.inner_text', key: 'k', value: '1' }), catalog))
      .not.toThrow();
    // Two decls share the local id but neither matches exactly: ambiguous.
    p.nodes.push(node({ id: 'G2', nodeType: 'Group', label: 'G2' }));
    p.nodes.push(node({ id: 'G2.inner_text', nodeType: 'Text', parentId: 'G2', config: { parentId: 'G2' } }));
    p.nodes = p.nodes.filter((n) => n.id !== 'inner_text');
    expect(() => applyOpsToProject(p, ops({ op: 'setConfig', node: 'inner_text', key: 'k', value: '1' }), catalog))
      .toThrow(/ambiguous/);
  });
});

describe('applyOpsToProject: nodes', () => {
  it('addNode seeds catalog ports and scopes into the parent group', () => {
    const p = applyOpsToProject(fixture(), ops({ op: 'addNode', id: 'debug_2', nodeType: 'Debug', parentGroup: 'G' }), catalog);
    const added = p.nodes.find((n) => n.id === 'G.debug_2')!;
    expect(added.parentId).toBe('G');
    expect(added.config.parentId).toBe('G');
    expect(added.inputs.map((i) => i.name)).toEqual(['data']);
    expect(added.scope).toEqual(['G']);
  });

  it('addNode rejects duplicates and unknown types', () => {
    expect(() => applyOpsToProject(fixture(), ops({ op: 'addNode', id: 'text_1', nodeType: 'Text', parentGroup: null }), catalog))
      .toThrow(/already exists/);
    expect(() => applyOpsToProject(fixture(), ops({ op: 'addNode', id: 'x', nodeType: 'Nope', parentGroup: null }), catalog))
      .toThrow(/unknown node type/);
  });

  it('removeNode drops the node and its edges', () => {
    const p = applyOpsToProject(fixture(), ops({ op: 'removeNode', node: 'text_1' }), catalog);
    expect(p.nodes.some((n) => n.id === 'text_1')).toBe(false);
    expect(p.edges.some((e) => e.source === 'text_1')).toBe(false);
    expect(p.edges).toHaveLength(1);
  });
});

describe('applyOpsToProject: edges', () => {
  it('addEdge resolves scoped endpoints and replaces an existing driver', () => {
    let p = applyOpsToProject(fixture(), ops(
      { op: 'addNode', id: 'text_2', nodeType: 'Text', parentGroup: null },
      { op: 'addEdge', source: 'text_2', sourcePort: 'value', target: 'debug_1', targetPort: 'data', scopeGroup: null },
    ), catalog);
    // The old text_1 -> debug_1.data driver is replaced, not duplicated.
    const drivers = p.edges.filter((e) => e.target === 'debug_1' && e.targetHandle === 'data');
    expect(drivers).toHaveLength(1);
    expect(drivers[0].source).toBe('text_2');
  });

  it('addEdge with self maps to the group inner handle', () => {
    const base = fixture();
    base.edges = base.edges.filter((e) => e.id !== 'e2');
    const p = applyOpsToProject(base, ops(
      { op: 'addEdge', source: 'inner_text', sourcePort: 'value', target: 'self', targetPort: 'out', scopeGroup: 'G' },
    ), catalog);
    expect(p.edges.some((e) => e.source === 'G.inner_text' && e.target === 'G' && e.targetHandle === 'out__inner')).toBe(true);
  });

  it('removeEdge removes the exact connection and fails on a missing one', () => {
    const p = applyOpsToProject(fixture(), ops(
      { op: 'removeEdge', source: 'text_1', sourcePort: 'value', target: 'debug_1', targetPort: 'data', scopeGroup: null },
    ), catalog);
    expect(p.edges).toHaveLength(1);
    expect(() => applyOpsToProject(p, ops(
      { op: 'removeEdge', source: 'text_1', sourcePort: 'value', target: 'debug_1', targetPort: 'data', scopeGroup: null },
    ), catalog)).toThrow(/connection not found/);
  });
});

describe('applyOpsToProject: containers', () => {
  it('addGroup / addLoop create containers, nested ids are scoped', () => {
    const p = applyOpsToProject(fixture(), ops(
      { op: 'addGroup', label: 'H', parentGroup: 'G' },
      { op: 'addLoop', label: 'L', parentGroup: null },
    ), catalog);
    expect(p.nodes.find((n) => n.id === 'G.H')!.nodeType).toBe('Group');
    expect(p.nodes.find((n) => n.id === 'L')!.nodeType).toBe('Loop');
  });

  it('removeGroup UNGROUPS: children climb a scope, boundary edges die', () => {
    const p = applyOpsToProject(fixture(), ops({ op: 'removeGroup', group: 'G' }), catalog);
    expect(p.nodes.some((n) => n.id === 'G')).toBe(false);
    const climbed = p.nodes.find((n) => n.id === 'inner_text')!;
    expect(climbed.parentId).toBeUndefined();
    expect('parentId' in climbed.config).toBe(false);
    // The self.out boundary wiring died with the group.
    expect(p.edges.some((e) => e.source === 'inner_text' || e.target === 'G')).toBe(false);
  });

  it('removeGroup on a Loop is a kind mismatch (and vice versa)', () => {
    const p = applyOpsToProject(fixture(), ops({ op: 'addLoop', label: 'L', parentGroup: null }), catalog);
    expect(() => applyOpsToProject(p, ops({ op: 'removeGroup', group: 'L' }), catalog)).toThrow(/Loop/);
    expect(() => applyOpsToProject(p, ops({ op: 'removeLoop', loopId: 'G' }), catalog)).toThrow(/Group/);
  });

  it('renameGroup re-keys the subtree: child ids, parent pointers, edges', () => {
    const p = applyOpsToProject(fixture(), ops({ op: 'renameGroup', group: 'G', newLabel: 'Renamed' }), catalog);
    expect(p.nodes.some((n) => n.id === 'Renamed')).toBe(true);
    const child = p.nodes.find((n) => n.id === 'Renamed.inner_text')!;
    expect(child.parentId).toBe('Renamed');
    expect(child.config.parentId).toBe('Renamed');
    expect(p.edges.some((e) => e.source === 'Renamed.inner_text' && e.target === 'Renamed')).toBe(true);
  });

  it('renameGroup rejects a duplicate id', () => {
    const p = fixture();
    p.nodes.push(node({ id: 'H', nodeType: 'Group', label: 'H' }));
    expect(() => applyOpsToProject(p, ops({ op: 'renameGroup', group: 'G', newLabel: 'H' }), catalog))
      .toThrow(/already exists/);
  });
});

describe('applyOpsToProject: nested ungroup ordering', () => {
  /** outer > outer.inner > outer.inner.leaf (two nested groups + a leaf). */
  function nested(): ProjectDefinition {
    return {
      id: 'p1',
      nodes: [
        node({ id: 'outer', nodeType: 'Group', label: 'outer' }),
        node({ id: 'outer.inner', nodeType: 'Group', label: 'inner', parentId: 'outer', config: { parentId: 'outer' }, scope: ['outer'] }),
        node({ id: 'outer.inner.leaf', nodeType: 'Text', parentId: 'outer.inner', config: { parentId: 'outer.inner' }, scope: ['outer', 'outer.inner'] }),
      ],
      edges: [],
      createdAt: '', updatedAt: '',
    };
  }

  it('deepest-first removal climbs the leaf to root and every op resolves', () => {
    const p = applyOpsToProject(nested(), ops(
      { op: 'removeGroup', group: 'outer.inner' },
      { op: 'removeGroup', group: 'outer' },
    ), catalog);
    expect(p.nodes.map(n => n.id).sort()).toEqual(['leaf']);
    expect(p.nodes[0].parentId).toBeUndefined();
  });

  it('parent-first removal fails: the nested ref is gone after the parent ungroups', () => {
    // Demonstrates WHY deleteNodes must order removals deepest-first.
    expect(() => applyOpsToProject(nested(), ops(
      { op: 'removeGroup', group: 'outer' },
      { op: 'removeGroup', group: 'outer.inner' },
    ), catalog)).toThrow(/not found/);
  });
});

describe('applyOpsToProject: moves', () => {
  it('moveNodeScope re-ids into the target scope', () => {
    const base = applyOpsToProject(fixture(), ops(
      { op: 'removeEdge', source: 'text_1', sourcePort: 'value', target: 'debug_1', targetPort: 'data', scopeGroup: null },
    ), catalog);
    const p = applyOpsToProject(base, ops({ op: 'moveNodeScope', node: 'text_1', targetGroup: 'G' }), catalog);
    const moved = p.nodes.find((n) => n.id === 'G.text_1')!;
    expect(moved.parentId).toBe('G');
    expect(moved.scope).toEqual(['G']);
  });

  it('moveNodeScope rejects when the node still has connections', () => {
    expect(() => applyOpsToProject(fixture(), ops({ op: 'moveNodeScope', node: 'text_1', targetGroup: 'G' }), catalog))
      .toThrow(/connections/);
  });

  it('moveGroupScope carries internal wiring, rejects external legs', () => {
    let p = applyOpsToProject(fixture(), ops({ op: 'addGroup', label: 'H', parentGroup: null }), catalog);
    p = applyOpsToProject(p, ops({ op: 'moveGroupScope', group: 'G', targetGroup: 'H' }), catalog);
    const moved = p.nodes.find((n) => n.id === 'H.G')!;
    expect(moved.parentId).toBe('H');
    expect(p.nodes.some((n) => n.id === 'H.G.inner_text')).toBe(true);
    expect(p.edges.some((e) => e.source === 'H.G.inner_text' && e.target === 'H.G' && e.targetHandle === 'out__inner')).toBe(true);
    // Wire G's out into the parent scope: now it has an external leg and can't move back out.
    p.nodes.push(node({ id: 'H.debug_9', nodeType: 'Debug', parentId: 'H', config: { parentId: 'H' }, inputs: [{ name: 'data', portType: 'T', required: true }] }));
    p.edges.push({ id: 'x', source: 'H.G', target: 'H.debug_9', sourceHandle: 'out', targetHandle: 'data' });
    expect(() => applyOpsToProject(p, ops({ op: 'moveGroupScope', group: 'H.G', targetGroup: null }), catalog))
      .toThrow(/connections/);
  });

  it('cannot move a container into its own subtree', () => {
    let p = applyOpsToProject(fixture(), ops({ op: 'addGroup', label: 'H', parentGroup: 'G' }), catalog);
    expect(() => applyOpsToProject(p, ops({ op: 'moveGroupScope', group: 'G', targetGroup: 'G.H' }), catalog))
      .toThrow(/own subtree/);
  });
});

describe('applyOpsToProject: ports', () => {
  it('updateNodePorts merges metadata by name and drops dangling edges', () => {
    const p = applyOpsToProject(fixture(), ops({
      op: 'updateNodePorts', node: 'debug_1',
      inputs: [{ name: 'payload', required: true }],
      outputs: [],
    }), catalog);
    const updated = p.nodes.find((n) => n.id === 'debug_1')!;
    expect(updated.inputs.map((i) => i.name)).toEqual(['payload']);
    // The text_1 -> debug_1.data edge dangles and is dropped.
    expect(p.edges.some((e) => e.target === 'debug_1')).toBe(false);
  });

  it('updateGroupPorts keeps inner-handle edges for surviving ports only', () => {
    const p = applyOpsToProject(fixture(), ops({
      op: 'updateGroupPorts', group: 'G',
      inputs: [],
      outputs: [{ name: 'renamed_out', required: true, portType: 'String' }],
    }), catalog);
    // The self.out wiring referenced the old port name: dropped.
    expect(p.edges.some((e) => e.target === 'G')).toBe(false);
  });
});

describe('loop carry ghost inputs', () => {
	/** A loop with carry "acc": the input side is DERIVED (synthesizedFromCarry),
	 *  never part of the signature; a seed wire feeds it from outside. */
	function loopFixture(): ProjectDefinition {
		return {
			id: 'p1',
			nodes: [
				node({
					id: 'seed', nodeType: 'Text',
					outputs: [{ name: 'value', portType: 'Number', required: true }],
				}),
				node({
					id: 'L', nodeType: 'Loop', label: 'L',
					config: { carry: ['acc'] },
					inputs: [
						{ name: 'items', portType: 'List[Number]', required: true },
						{ name: 'acc', portType: 'Number', required: true, synthesizedFromCarry: true },
					],
					outputs: [{ name: 'acc', portType: 'Number', required: true }],
				}),
			],
			edges: [
				{ id: 'e1', source: 'seed', target: 'L', sourceHandle: 'value', targetHandle: 'acc' },
			],
			createdAt: '', updatedAt: '',
		};
	}

	it('dissolving a carry drops the ghost input and its seed wire', () => {
		const p = applyOpsToProject(loopFixture(), ops(
			{ op: 'setLoopConfig', loopId: 'L', key: 'carry', value: '[]' },
		), catalog);
		const loop = p.nodes.find((n) => n.id === 'L')!;
		expect(loop.inputs.some((i) => i.name === 'acc')).toBe(false);
		expect(p.edges).toHaveLength(0);
	});

	it('adding a carry synthesizes the ghost input from the paired output', () => {
		const base = loopFixture();
		base.nodes.find((n) => n.id === 'L')!.config.carry = [];
		base.nodes.find((n) => n.id === 'L')!.inputs = [
			{ name: 'items', portType: 'List[Number]', required: true },
		];
		base.edges = [];
		const p = applyOpsToProject(base, ops(
			{ op: 'setLoopConfig', loopId: 'L', key: 'carry', value: '[\n  "acc"\n]' },
		), catalog);
		const ghost = p.nodes.find((n) => n.id === 'L')!.inputs.find((i) => i.name === 'acc');
		expect(ghost?.synthesizedFromCarry).toBe(true);
		expect(ghost?.portType).toBe('Number');
	});

	it('a ports update on the loop re-derives the ghost and keeps its seed wire', () => {
		// Signatures never carry ghosts: the update arrives without `acc`, but
		// the carry list still pairs it with the output, so the ghost (and the
		// wire feeding it) survives.
		const p = applyOpsToProject(loopFixture(), ops({
			op: 'updateLoopPorts', loopId: 'L',
			inputs: [{ name: 'items', required: true, portType: 'List[Number]' }],
			outputs: [{ name: 'acc', required: true, portType: 'Number' }],
		}), catalog);
		const loop = p.nodes.find((n) => n.id === 'L')!;
		expect(loop.inputs.some((i) => i.name === 'acc' && i.synthesizedFromCarry)).toBe(true);
		expect(p.edges).toHaveLength(1);
	});

	it('removing the paired output drops the ghost even while the carry entry remains', () => {
		const p = applyOpsToProject(loopFixture(), ops({
			op: 'updateLoopPorts', loopId: 'L',
			inputs: [{ name: 'items', required: true, portType: 'List[Number]' }],
			outputs: [],
		}), catalog);
		const loop = p.nodes.find((n) => n.id === 'L')!;
		expect(loop.inputs.some((i) => i.name === 'acc')).toBe(false);
		expect(p.edges).toHaveLength(0);
	});

	it('a name re-arriving as a REAL signature input loses the ghost flag (survives a later carry clear)', () => {
		// The carry ghost `acc` is promoted to a genuine declared input by a
		// ports update that includes it. Merging must NOT inherit the prior
		// port's synthesizedFromCarry:true, else a subsequent carry clear would
		// wrongly sweep this real input.
		const afterPorts = applyOpsToProject(loopFixture(), ops({
			op: 'updateLoopPorts', loopId: 'L',
			inputs: [
				{ name: 'items', required: true, portType: 'List[Number]' },
				{ name: 'acc', required: true, portType: 'Number' },
			],
			outputs: [{ name: 'acc', required: true, portType: 'Number' }],
		}), catalog);
		const promoted = afterPorts.nodes.find((n) => n.id === 'L')!.inputs.find((i) => i.name === 'acc');
		expect(promoted?.synthesizedFromCarry).toBe(false);
		// Now clear the carry list: the real input must remain.
		const afterClear = applyOpsToProject(afterPorts, ops(
			{ op: 'setLoopConfig', loopId: 'L', key: 'carry', value: '[]' },
		), catalog);
		expect(afterClear.nodes.find((n) => n.id === 'L')!.inputs.some((i) => i.name === 'acc')).toBe(true);
	});
});

describe('rename validation', () => {
	it('rejects a rename to an empty label (mirrors the server)', () => {
		expect(() => applyOpsToProject(fixture(), ops(
			{ op: 'renameGroup', group: 'G', newLabel: '' },
		), catalog)).toThrow(/empty label/);
	});
});

describe('duplicate-Loop op sequence (port signature + carry)', () => {
	// The exact op order duplicateNodes emits for a Loop with over+carry:
	// addLoop -> updateLoopPorts(signature, ghost-stripped) -> setLoopConfig.
	// The signature must exist BEFORE the carry config so the projection (and
	// the server) can pair carry outputs and re-synthesize the ghost input.
	it('builds a Loop shell whose copied carry re-synthesizes its ghost input', () => {
		const start: ProjectDefinition = { id: 'p1', nodes: [], edges: [], createdAt: '', updatedAt: '' };
		const p = applyOpsToProject(start, ops(
			{ op: 'addLoop', label: 'L2', parentGroup: null },
			{ op: 'updateLoopPorts', loopId: 'L2',
				inputs: [{ name: 'items', required: true, portType: 'List[Number]' }],
				outputs: [{ name: 'acc', required: true, portType: 'Number' }] },
			{ op: 'setLoopConfig', loopId: 'L2', key: 'over', value: '[\n  "items"\n]' },
			{ op: 'setLoopConfig', loopId: 'L2', key: 'carry', value: '[\n  "acc"\n]' },
		), catalog);
		const loop = p.nodes.find(n => n.id === 'L2')!;
		// The declared signature.
		expect(loop.inputs.some(i => i.name === 'items')).toBe(true);
		expect(loop.outputs.some(o => o.name === 'acc')).toBe(true);
		// The carry ghost input, re-derived from the copied carry list.
		const ghost = loop.inputs.find(i => i.name === 'acc');
		expect(ghost?.synthesizedFromCarry).toBe(true);
		expect(ghost?.portType).toBe('Number');
	});
});

describe('foldOps', () => {
  const pending = (id: string, ...list: EditOp[]): PendingOp => ({ id, ops: list, state: 'pending' });

  it('applies the queue in order and partitions failures', () => {
    const result = foldOps(fixture(), [
      pending('1', { op: 'addNode', id: 'text_9', nodeType: 'Text', parentGroup: null }),
      pending('2', { op: 'addEdge', source: 'text_9', sourcePort: 'value', target: 'debug_1', targetPort: 'data', scopeGroup: null }),
    ], catalog);
    expect(result.dropped).toHaveLength(0);
    expect(result.kept).toHaveLength(2);
    expect(result.project.edges.some((e) => e.source === 'text_9')).toBe(true);
  });

  it('drops a dependent op when its producer is gone (no fixpoint loops)', () => {
    // Truth never had text_9: op 2 consumed op 1's product; with op 1 absent
    // (rejected by the host and removed), op 2 fails against the new truth.
    const result = foldOps(fixture(), [
      pending('2', { op: 'addEdge', source: 'text_9', sourcePort: 'value', target: 'debug_1', targetPort: 'data', scopeGroup: null }),
    ], catalog);
    expect(result.kept).toHaveLength(0);
    expect(result.dropped[0].reason).toMatch(/not found/);
  });

  it('a failed batch is atomic: no partial application leaks into the projection', () => {
    const result = foldOps(fixture(), [
      pending('1',
        { op: 'addNode', id: 'text_9', nodeType: 'Text', parentGroup: null },
        { op: 'addNode', id: 'text_1', nodeType: 'Text', parentGroup: null }, // duplicate: fails
      ),
    ], catalog);
    expect(result.dropped).toHaveLength(1);
    expect(result.project.nodes.some((n) => n.id === 'text_9')).toBe(false);
  });

  it('a rename mid-queue rewrites refs for later ops captured post-rename', () => {
    const result = foldOps(fixture(), [
      pending('1', { op: 'renameGroup', group: 'G', newLabel: 'R' }),
      // Captured AFTER the rename landed visually, so it references R.
      pending('2', { op: 'addNode', id: 'n1', nodeType: 'Debug', parentGroup: 'R' }),
    ], catalog);
    expect(result.dropped).toHaveLength(0);
    expect(result.project.nodes.some((n) => n.id === 'R.n1')).toBe(true);
  });

  it('does not mutate the truth project', () => {
    const truth = fixture();
    const snapshot = JSON.stringify(truth);
    foldOps(truth, [pending('1', { op: 'removeNode', node: 'text_1' })], catalog);
    expect(JSON.stringify(truth)).toBe(snapshot);
  });

  it('handles a Proxy-wrapped truth project (Svelte $state wraps state in proxies)', () => {
    // structuredClone throws on ANY Proxy, and in production the truth
    // project arrives wrapped in Svelte's $state proxy: the clone must read
    // through it instead.
    const deepProxy = <T extends object>(obj: T): T =>
      new Proxy(obj, {
        get(target, prop, receiver) {
          const v = Reflect.get(target, prop, receiver);
          return v !== null && typeof v === 'object' ? deepProxy(v as object) : v;
        },
      }) as T;
    const result = foldOps(deepProxy(fixture()), [
      pending('1', { op: 'addNode', id: 'text_9', nodeType: 'Text', parentGroup: null }),
    ], catalog);
    expect(result.dropped).toHaveLength(0);
    expect(result.project.nodes.some((n) => n.id === 'text_9')).toBe(true);
  });
});

describe('portLiterals projection', () => {
  it('a setConfig(form inline) on an anywhere-placement port homes in portLiterals and survives folding', () => {
    const project: ProjectDefinition = {
      id: 'p', createdAt: '', updatedAt: '',
      nodes: [node({
        id: 'orc', nodeType: 'OpenRouterConfig',
        inputs: [{ name: 'systemPrompt', portType: 'String', required: false, literal: 'anywhere' }],
      })],
      edges: [],
    } as unknown as ProjectDefinition;
    const pending: PendingOp[] = [{
      opId: '1',
      ops: [{ op: 'setConfig', node: 'orc', key: 'systemPrompt', value: '"test"', form: 'inline' } as EditOp],
    } as unknown as PendingOp];
    const folded = foldOps(project, pending, catalog);
    expect(folded.dropped).toEqual([]);
    const n = folded.project.nodes.find((x) => x.id === 'orc')!;
    expect(n.portLiterals).toEqual({ systemPrompt: 'test' });
    expect(n.config).toEqual({});
  });

  it('truth-side portLiterals survive a fold with no pending ops', () => {
    const project: ProjectDefinition = {
      id: 'p', createdAt: '', updatedAt: '',
      nodes: [node({
        id: 'orc', nodeType: 'OpenRouterConfig',
        inputs: [{ name: 'systemPrompt', portType: 'String', required: false, literal: 'anywhere' }],
        portLiterals: { systemPrompt: 'test' },
        portLiteralSpans: { systemPrompt: { span: { startLine: 7, startColumn: 2, endLine: 7, endColumn: 44 }, origin: 'inline' } },
      })],
      edges: [],
    } as unknown as ProjectDefinition;
    const folded = foldOps(project, [], catalog);
    const n = folded.project.nodes.find((x) => x.id === 'orc')!;
    expect(n.portLiterals).toEqual({ systemPrompt: 'test' });
  });
});
