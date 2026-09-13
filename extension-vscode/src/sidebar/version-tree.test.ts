// The version tree the Executions view draws in "by version" mode is
// folded from `weft tree --json` by pure functions, so the nesting, the
// marks and the labels are pinned here without an editor process.

import { describe, expect, it } from 'vitest';
import {
  buildVersionTree,
  runDescription,
  runScopedTo,
  versionLabel,
  versionMarks,
  type RunSummary,
  type TreeJson,
  type VersionSummary,
} from './version-tree';

const version = (id: string, parent: string | null, at: number, changed: string[] = [], label: string | null = null): VersionSummary => ({
  id: `${id}00000000`,
  parent_id: parent ? `${parent}00000000` : null,
  label,
  created_at: at,
  diff: { added: [], removed: [], changed },
  manifest: {},
});

const run = (color: string, version: string, at: number, extra: Partial<RunSummary> = {}): RunSummary => ({
  color: `${color}00000000`,
  version_id: `${version}00000000`,
  seed_color: null,
  stale: [],
  spec: null,
  example: null,
  status: 'completed',
  started_at: at,
  completed_at: null,
  ...extra,
});

const id = (n: string) => `${n}00000000`;
const tree: TreeJson = {
  head: { head_version: id('v2'), head_run: id('c2'), activation_version: id('v1') },
  disk_version: id('v3'),
  // In the dispatcher's order: the order they were recorded.
  versions: [version('v1', null, 1, [], 'base'), version('v2', 'v1', 2, ['main.weft']), version('v3', 'v1', 3, ['prompts/p.txt'])],
  runs: [run('c1', 'v1', 4), run('c2', 'v2', 5, { seed_color: id('c1'), stale: ['b'] })],
};

describe('version tree', () => {
  it('nests children under their parent in recorded order with their runs', () => {
    const roots = buildVersionTree(tree);
    expect(roots.map((r) => r.version.id)).toEqual([id('v1')]);
    expect(roots[0].children.map((c) => c.version.id)).toEqual([id('v2'), id('v3')]);
    expect(roots[0].runs.map((r) => r.color)).toEqual([id('c1')]);
    expect(roots[0].children[0].runs.map((r) => r.color)).toEqual([id('c2')]);
  });

  it('draws every version once, even in a cycle', () => {
    // A parent link carries no foreign key, so two rows pointing at each
    // other is something the database permits. Recursing over it blew the
    // stack and took the view down with it.
    const cyclic: TreeJson = {
      ...tree,
      versions: [version('a1', 'b1', 1), version('b1', 'a1', 2)],
      runs: [],
    };
    const roots = buildVersionTree(cyclic);
    const drawn: string[] = [];
    const walk = (n: ReturnType<typeof buildVersionTree>[number]): void => {
      drawn.push(n.version.id);
      n.children.forEach(walk);
    };
    roots.forEach(walk);
    expect(drawn.sort()).toEqual([id('a1'), id('b1')]);
  });

  it('draws a cycle before what hangs off it, and each version once', () => {
    // A parent link carries no foreign key, so a cycle is something the
    // database permits, and a version can hang off a member of one. The
    // cycle's members are drawn first: starting at the child would put
    // it at top level and then again under its parent.
    const cyclic: TreeJson = {
      ...tree,
      versions: [version('c1', 'b1', 1), version('a1', 'b1', 2), version('b1', 'a1', 3)],
      runs: [],
    };
    const roots = buildVersionTree(cyclic);
    const drawn: string[] = [];
    const walk = (n: ReturnType<typeof buildVersionTree>[number]): void => {
      drawn.push(n.version.id);
      n.children.forEach(walk);
    };
    roots.forEach(walk);
    expect(drawn.sort()).toEqual([id('a1'), id('b1'), id('c1')]);
    // `c1` hangs off `b1`, so it is drawn under it and not as a root.
    expect(roots.map((r) => r.version.id)).toEqual([id('a1')]);
  });

  it('draws a version that is its own parent once, as a root', () => {
    const selfish: TreeJson = { ...tree, versions: [version('s1', 's1', 1)], runs: [] };
    const roots = buildVersionTree(selfish);
    expect(roots.map((r) => r.version.id)).toEqual([id('s1')]);
    expect(roots[0].children).toEqual([]);
  });

  it('draws a history far too deep to recurse over', () => {
    // A version is recorded per run, so a working project's history is
    // long and almost entirely linear: its depth IS its length. Building
    // it by recursion overflowed the stack and took the view down for the
    // same reason a cycle did, just later.
    const deep = Array.from({ length: 20000 }, (_, i) =>
      version(`v${i}`, i === 0 ? null : `v${i - 1}`, i + 1),
    );
    const roots = buildVersionTree({ ...tree, versions: deep, runs: [] });
    expect(roots.map((r) => r.version.id)).toEqual([id('v0')]);
    let depth = 1;
    let at = roots[0];
    while (at.children.length > 0) {
      at = at.children[0];
      depth += 1;
    }
    expect(depth).toBe(20000);
  });

  it('marks head, the version on disk, and the activated one', () => {
    const [root] = buildVersionTree(tree);
    expect(versionMarks(root)).toEqual(['activated']);
    expect(versionMarks(root.children[0])).toEqual(['HEAD']);
    expect(versionMarks(root.children[1])).toEqual(['on disk']);
  });

  it('a version whose parent was pruned is a root', () => {
    const orphaned: TreeJson = { ...tree, versions: [version('v9', 'gone', 9)], runs: [] };
    expect(buildVersionTree(orphaned).map((r) => r.version.id)).toEqual([id('v9')]);
  });

  it('labels a version by what changed and a run by how it ran', () => {
    const [root] = buildVersionTree(tree);
    expect(versionLabel(root)).toBe('v1000000 (base) root');
    expect(versionLabel(root.children[0])).toBe('v2000000 ~main.weft');
    expect(runDescription(root.children[0].runs[0], id('c2'))).toBe('completed  ·  seed c1000000 (1 stale)  ·  HEAD run');
    const saved = run('c5', 'v2', 6, { spec: { name: 'angry' }, example: 'angry' });
    expect(runDescription(saved, null)).toBe('completed  ·  spec angry  ·  example angry');
  });

  it('knows which runs are scoped to the group the editor stands in', () => {
    const scoped = run('c6', 'v2', 7, { spec: { name: 'g', group: ['triage', {}] } });
    const inner = run('c7', 'v2', 8, { spec: { name: 'f', from: { 'triage.parse': {} } } });
    expect(runScopedTo(scoped, 'triage')).toBe(true);
    expect(runScopedTo(inner, 'triage')).toBe(true);
    expect(runScopedTo(scoped, 'billing')).toBe(false);
    expect(runScopedTo(run('c8', 'v2', 9), 'triage')).toBe(false);
  });
});
