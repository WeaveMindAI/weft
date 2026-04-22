// Per-node visibility + z-index resolution. Matches
// ProjectEditorInner.svelte:864-905 behaviour exactly.
//
// Rules:
// • Walk the rawParentId chain. If any ancestor is collapsed, the
//   node is hidden (display: none, parentId cleared so xyflow
//   doesn't compute a parent-relative position we won't render).
// • Otherwise parentId = rawParentId when the direct parent is
//   expanded; undefined when it's collapsed (so the node renders
//   but isn't clipped by xyflow's parent sub-frame).
// • Z-index:
//     annotations       -1
//     expanded groups   -1 + nestingDepth
//     collapsed groups   4
//     regular nodes      4
//   Clicked nodes get raised by a separate nextNodeZ counter in
//   Graph.svelte; that's NOT our concern here.
//
// Edges are NOT touched here — callers filter edges by the
// `hiddenNodeIds` set we return.

import type { ViewNode } from './types';

export interface VisibilityResult {
  parentIdById: Map<string, string | undefined>;
  styleById: Map<string, string>;
  zIndexById: Map<string, number>;
  hiddenNodeIds: Set<string>;
}

export function computeVisibility(
  viewNodes: readonly ViewNode[],
): VisibilityResult {
  const byId = new Map(viewNodes.map((n) => [n.id, n]));
  const parentIdById = new Map<string, string | undefined>();
  const styleById = new Map<string, string>();
  const zIndexById = new Map<string, number>();
  const hiddenNodeIds = new Set<string>();

  function isGroupExpanded(id: string): boolean {
    const g = byId.get(id);
    if (!g) return false;
    return g.kind === 'group' && Boolean(g.config.expanded);
  }

  function nestingDepth(id: string): number {
    let depth = 0;
    let pid: string | null = id;
    while (pid) {
      const p = byId.get(pid);
      if (!p || p.kind !== 'group') break;
      pid = p.rawParentId;
      if (pid) depth++;
    }
    return depth;
  }

  for (const n of viewNodes) {
    let hidden = false;
    let pid: string | null = n.rawParentId;
    while (pid) {
      const ancestor = byId.get(pid);
      if (!ancestor) break;
      if (ancestor.kind === 'group' && !ancestor.config.expanded) {
        hidden = true;
        break;
      }
      pid = ancestor.rawParentId;
    }

    const directParent = n.rawParentId ? byId.get(n.rawParentId) ?? null : null;
    const directExpanded = directParent
      ? directParent.kind === 'group' && Boolean(directParent.config.expanded)
      : false;
    const parentId =
      n.rawParentId && directExpanded && !hidden ? n.rawParentId : undefined;

    parentIdById.set(n.id, parentId);
    styleById.set(n.id, hidden ? 'display: none;' : '');
    if (hidden) hiddenNodeIds.add(n.id);

    let z: number;
    if (n.kind === 'annotation') {
      z = -1;
    } else if (n.kind === 'group' && n.config.expanded) {
      z = -1 + nestingDepth(n.id);
    } else {
      z = 4;
    }
    zIndexById.set(n.id, z);
  }

  return { parentIdById, styleById, zIndexById, hiddenNodeIds };
}

// Topological sort: xyflow requires parent entries before children.
// Groups go first (sorted by nesting depth ascending); then regular
// nodes.
export function topoSortForXyflow(
  viewNodes: readonly ViewNode[],
): ViewNode[] {
  const byId = new Map(viewNodes.map((n) => [n.id, n]));
  const depthOf = new Map<string, number>();

  function depth(n: ViewNode): number {
    const cached = depthOf.get(n.id);
    if (cached != null) return cached;
    let d = 0;
    let pid: string | null = n.rawParentId;
    while (pid) {
      const p = byId.get(pid);
      if (!p) break;
      d++;
      pid = p.rawParentId;
    }
    depthOf.set(n.id, d);
    return d;
  }

  const groups = viewNodes.filter((n) => n.kind === 'group');
  const annots = viewNodes.filter((n) => n.kind === 'annotation');
  const rest = viewNodes.filter((n) => n.kind === 'regular');
  groups.sort((a, b) => depth(a) - depth(b));
  return [...groups, ...annots, ...rest];
}
