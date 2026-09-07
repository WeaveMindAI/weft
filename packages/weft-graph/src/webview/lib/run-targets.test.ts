import { describe, it, expect } from 'vitest';
import { runLabel, pruneTargets, runTargetFacts } from './run-targets';

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
  const nodes = [{ id: 'out' }, { id: 'mid' }];

  it('keeps any node the graph still holds', () => {
    expect(pruneTargets(['out', 'mid'], nodes)).toEqual(new Set(['out', 'mid']));
  });

  it('drops a node that was deleted', () => {
    expect(pruneTargets(['out', 'gone'], nodes)).toEqual(new Set(['out']));
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

  it('a target inside a group brings the group, its inputs, and its siblings', () => {
    // feed -> grp ; grp contains inner (the target) and db (infra) ; trigger -> feed
    const scoped = [
      { id: 'trigger', isTrigger: true, isInfra: false },
      { id: 'feed', isTrigger: false, isInfra: false },
      { id: 'grp', isTrigger: false, isInfra: false },
      { id: 'inner', isTrigger: false, isInfra: false, parentId: 'grp' },
      { id: 'db', isTrigger: false, isInfra: true, parentId: 'grp' },
    ];
    const scopedEdges = [
      { source: 'trigger', target: 'feed' },
      { source: 'feed', target: 'grp' },
    ];
    const facts = runTargetFacts(['inner'], scoped, scopedEdges);
    expect(facts.infraIds).toEqual(['db']);
    expect(facts.avoidsTriggers).toBe(false);
  });

  it('reaches infra nested two containers deep', () => {
    // grp contains inner (the target) and sub ; sub contains db (infra)
    const scoped = [
      { id: 'grp', isTrigger: false, isInfra: false },
      { id: 'inner', isTrigger: false, isInfra: false, parentId: 'grp' },
      { id: 'sub', isTrigger: false, isInfra: false, parentId: 'grp' },
      { id: 'db', isTrigger: false, isInfra: true, parentId: 'sub' },
    ];
    expect(runTargetFacts(['inner'], scoped, []).infraIds).toEqual(['db']);
  });

  it('a container target brings what is inside it', () => {
    const scoped = [
      { id: 'grp', isTrigger: false, isInfra: false },
      { id: 'db', isTrigger: false, isInfra: true, parentId: 'grp' },
    ];
    expect(runTargetFacts(['grp'], scoped, []).infraIds).toEqual(['db']);
  });
});
