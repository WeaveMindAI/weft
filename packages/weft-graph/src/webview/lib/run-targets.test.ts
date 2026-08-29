import { describe, it, expect } from 'vitest';
import { isOutputNode, runLabel, pruneTargets, runTargetFacts } from './run-targets';

describe('isOutputNode', () => {
  it('reads the node type default', () => {
    expect(isOutputNode({}, { isOutputDefault: true })).toBe(true);
    expect(isOutputNode({}, { isOutputDefault: false })).toBe(false);
    expect(isOutputNode({}, {})).toBe(false);
    expect(isOutputNode(undefined, undefined)).toBe(false);
  });

  it('lets the project override it either way', () => {
    expect(isOutputNode({ _is_output: true }, { isOutputDefault: false })).toBe(true);
    expect(isOutputNode({ _is_output: false }, { isOutputDefault: true })).toBe(false);
  });

  it('ignores a non-boolean override rather than guessing', () => {
    expect(isOutputNode({ _is_output: 'yes' }, { isOutputDefault: true })).toBe(true);
    expect(isOutputNode({ _is_output: 'yes' }, { isOutputDefault: false })).toBe(false);
  });
});

describe('runLabel', () => {
  it('stays plain until the user aims the run', () => {
    expect(runLabel(0)).toBe('Run Project');
  });

  it('counts what it will run', () => {
    expect(runLabel(1)).toBe('Run 1 target');
    expect(runLabel(3)).toBe('Run 3 targets');
  });
});

describe('pruneTargets', () => {
  const nodes = [
    { id: 'out', data: { config: {}, features: { isOutputDefault: true } } },
    { id: 'mid', data: { config: {}, features: { isOutputDefault: false } } },
    { id: 'promoted', data: { config: { _is_output: true }, features: {} } },
  ];

  it('keeps output nodes', () => {
    expect(pruneTargets(['out', 'promoted'], nodes)).toEqual(new Set(['out', 'promoted']));
  });

  it('drops a node that was deleted', () => {
    expect(pruneTargets(['out', 'gone'], nodes)).toEqual(new Set(['out']));
  });

  it('drops a node that stopped being an output', () => {
    expect(pruneTargets(['out', 'mid'], nodes)).toEqual(new Set(['out']));
  });
});

describe('runTargetFacts', () => {
  // trigger -> mid -> out ; bridge(infra) -> door ; side -> door
  const nodes = [
    { id: 'trigger', isTrigger: true, isInfra: false },
    { id: 'mid', isTrigger: false, isInfra: false },
    { id: 'out', isTrigger: false, isInfra: false },
    { id: 'bridge', isTrigger: false, isInfra: true },
    { id: 'side', isTrigger: false, isInfra: false },
    { id: 'door', isTrigger: false, isInfra: false },
  ];
  const edges = [
    { source: 'trigger', target: 'mid' },
    { source: 'mid', target: 'out' },
    { source: 'bridge', target: 'door' },
    { source: 'side', target: 'door' },
  ];

  it('a target fed only by non-triggers avoids them', () => {
    expect(runTargetFacts(['door'], nodes, edges).avoidsTriggers).toBe(true);
  });

  it('a target downstream of a trigger does not', () => {
    expect(runTargetFacts(['out'], nodes, edges).avoidsTriggers).toBe(false);
  });

  it('one triggered target poisons the joined set', () => {
    expect(runTargetFacts(['door', 'out'], nodes, edges).avoidsTriggers).toBe(false);
  });

  it('no targets means no bypass', () => {
    expect(runTargetFacts([], nodes, edges).avoidsTriggers).toBe(false);
  });

  it('reports the infra inside the joined subgraph and nothing else', () => {
    expect(runTargetFacts(['door'], nodes, edges).infraIds).toEqual(['bridge']);
    expect(runTargetFacts(['out'], nodes, edges).infraIds).toEqual([]);
  });

  it('does not walk through a trigger to infra above it', () => {
    const edges2 = [...edges, { source: 'bridge', target: 'trigger' }];
    expect(runTargetFacts(['out'], nodes, edges2).infraIds).toEqual([]);
  });
});
