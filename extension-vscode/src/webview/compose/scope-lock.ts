// Scope-lock pipeline on drag stop. A node cannot change scope
// (enter/leave a group) while it has any edge to another node in
// its current scope. See dashboard-v1/src/lib/components/project/
// ProjectEditorInner.svelte:2385-2629 and docs parity/scope-lock.md.
//
// The checks work on xyflow state (positions + parentId + edges) so
// the shape of our ViewNode / view-edge objects doesn't matter, as
// long as they expose `{id, parentId?, position, measured?}` +
// `{source, target}`.

import type { Edge as FlowEdge, Node as FlowNode } from '@xyflow/svelte';

export interface ScopeEdge {
  source: string;
  target: string;
}

export function nodeHasConnectionsInScope(
  nodeId: string,
  scopeParentId: string | null | undefined,
  nodes: readonly FlowNode[],
  edges: readonly ScopeEdge[],
): boolean {
  const sameScope = new Set(
    nodes
      .filter((n) => n.id !== nodeId && (n.parentId ?? null) === (scopeParentId ?? null))
      .map((n) => n.id),
  );
  if (scopeParentId) sameScope.add(scopeParentId);
  for (const e of edges) {
    if (e.source === nodeId && sameScope.has(e.target)) return true;
    if (e.target === nodeId && sameScope.has(e.source)) return true;
  }
  return false;
}

export interface NodeRect {
  x: number;
  y: number;
  w: number;
  h: number;
}

export function nodeRect(n: FlowNode): NodeRect {
  const measured = (n as FlowNode & { measured?: { width?: number; height?: number } }).measured;
  const w = measured?.width ?? (n.width as number | undefined) ?? 200;
  const h = measured?.height ?? (n.height as number | undefined) ?? 120;
  return { x: n.position.x, y: n.position.y, w, h };
}

export function absolutePosition(
  n: FlowNode,
  nodesById: Map<string, FlowNode>,
): { x: number; y: number } {
  let x = n.position.x;
  let y = n.position.y;
  let pid = n.parentId;
  while (pid) {
    const parent = nodesById.get(pid);
    if (!parent) break;
    x += parent.position.x;
    y += parent.position.y;
    pid = parent.parentId;
  }
  return { x, y };
}

// Find the deepest-nested + smallest group whose rect contains the
// absolute position `pos`. Exclude `excludeIds` (self + descendants).
export function deepestGroupContaining(
  pos: { x: number; y: number },
  nodes: readonly FlowNode[],
  excludeIds: ReadonlySet<string>,
): FlowNode | null {
  const nodesById = new Map(nodes.map((n) => [n.id, n]));
  let best: { node: FlowNode; area: number; depth: number } | null = null;

  for (const n of nodes) {
    if (excludeIds.has(n.id)) continue;
    if (n.type !== 'weftGroup') continue; // only expanded groups
    if ((n.style as string | undefined)?.includes('display: none')) continue;

    const abs = absolutePosition(n, nodesById);
    const r = nodeRect(n);
    if (
      pos.x < abs.x ||
      pos.x > abs.x + r.w ||
      pos.y < abs.y ||
      pos.y > abs.y + r.h
    ) {
      continue;
    }
    const depth = depthOf(n, nodesById);
    const area = r.w * r.h;
    if (
      !best ||
      depth > best.depth ||
      (depth === best.depth && area < best.area)
    ) {
      best = { node: n, area, depth };
    }
  }
  return best?.node ?? null;
}

function depthOf(n: FlowNode, nodesById: Map<string, FlowNode>): number {
  let d = 0;
  let pid = n.parentId;
  while (pid) {
    const p = nodesById.get(pid);
    if (!p) break;
    d++;
    pid = p.parentId;
  }
  return d;
}

export function descendantIds(
  rootId: string,
  nodes: readonly FlowNode[],
): Set<string> {
  const out = new Set<string>([rootId]);
  let changed = true;
  while (changed) {
    changed = false;
    for (const n of nodes) {
      if (n.parentId && out.has(n.parentId) && !out.has(n.id)) {
        out.add(n.id);
        changed = true;
      }
    }
  }
  return out;
}

// Toast with debounce — v1 rate-limits the scope-lock message to
// once per 3 seconds so bulk drags don't spam.
export class DebouncedToast {
  private lastAt = 0;
  private timer: ReturnType<typeof setTimeout> | null = null;
  constructor(
    private readonly show: (msg: string) => void,
    private readonly minGapMs: number = 3000,
  ) {}
  fire(msg: string): void {
    const now = Date.now();
    if (now - this.lastAt < this.minGapMs) return;
    this.lastAt = now;
    if (this.timer) clearTimeout(this.timer);
    this.timer = setTimeout(() => this.show(msg), 0);
  }
}

// Xyflow edges we query during scope-lock don't need handles.
export function toScopeEdges(edges: readonly FlowEdge[]): ScopeEdge[] {
  return edges.map((e) => ({ source: e.source, target: e.target }));
}
