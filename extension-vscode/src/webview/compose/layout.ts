// Bottom-up per-scope ELK layout. Ported from
// dashboard-v1/src/lib/ai/weft-parser.ts:4714-5442 (`autoOrganize`).
//
// Strategy:
//   1. Compute each group's depth.
//   2. Deepest-first: for each group's children, find connected
//      components, layout each independently, side-by-side arrange
//      disconnected components, update the group's size to the bounding
//      box + padding.
//   3. Layout root last with the groups' final sizes baked in.
//
// Port Y positions match the CSS constants the components use so
// edge endpoints align with the handle DOM positions.

import ELK, { type ElkNode } from 'elkjs/lib/elk.bundled.js';
import type { Edge as FlowEdge, Node as FlowNode } from '@xyflow/svelte';

const elk = new ELK();

const LAYOUT_OPTIONS = {
  'elk.algorithm': 'layered',
  'elk.direction': 'RIGHT',
  'elk.layered.spacing.nodeNodeBetweenLayers': '50',
  'elk.spacing.nodeNode': '25',
  'elk.layered.spacing.edgeNodeBetweenLayers': '15',
  'elk.layered.nodePlacement.strategy': 'NETWORK_SIMPLEX',
  'elk.layered.crossingMinimization.strategy': 'LAYER_SWEEP',
  'elk.layered.crossingMinimization.greedySwitch.type': 'TWO_SIDED',
  'elk.layered.crossingMinimization.thoroughness': '100',
  'elk.layered.considerModelOrder.strategy': 'NODES_AND_EDGES',
  'elk.layered.considerModelOrder.crossingCounterNodeInfluence': '0.5',
  'elk.layered.considerModelOrder.crossingCounterPortInfluence': '0.5',
  'elk.layered.crossingMinimization.forceNodeModelOrder': 'true',
  'elk.layered.nodePromotion.strategy': 'DUMMYNODE_PERCENTAGE',
  'elk.separateConnectedComponents': 'true',
};

const GROUP_PADDING = 40;
const GROUP_TOP_PADDING = 80;
const GROUP_SIDE_PADDING = 60;
const GROUP_BOTTOM_PADDING = 40;
const COMPONENT_GAP = 60;

export interface LayoutNode {
  id: string;
  parentId: string | null;
  width: number;
  height: number;
  kind: 'group' | 'regular' | 'annotation';
  expanded?: boolean;
}

export interface LayoutEdge {
  id: string;
  source: string;
  target: string;
}

export interface LayoutResult {
  positions: Map<string, { x: number; y: number }>;
  groupSizes: Map<string, { w: number; h: number }>;
}

export async function autoOrganize(
  nodes: readonly LayoutNode[],
  edges: readonly LayoutEdge[],
): Promise<LayoutResult> {
  const positions = new Map<string, { x: number; y: number }>();
  const groupSizes = new Map<string, { w: number; h: number }>();

  const byParent = new Map<string | null, LayoutNode[]>();
  for (const n of nodes) {
    const key = n.parentId;
    if (!byParent.has(key)) byParent.set(key, []);
    byParent.get(key)!.push(n);
  }

  const depthOf = new Map<string, number>();
  const nodeById = new Map(nodes.map((n) => [n.id, n]));
  function depth(id: string): number {
    const cached = depthOf.get(id);
    if (cached != null) return cached;
    const n = nodeById.get(id);
    if (!n || !n.parentId) {
      depthOf.set(id, 0);
      return 0;
    }
    const d = depth(n.parentId) + 1;
    depthOf.set(id, d);
    return d;
  }

  // Deepest first so each scope's size is known before its parent lays out.
  const expandedGroups = nodes
    .filter((n) => n.kind === 'group' && n.expanded !== false)
    .sort((a, b) => depth(b.id) - depth(a.id));

  for (const g of expandedGroups) {
    const children = byParent.get(g.id) ?? [];
    if (children.length === 0) continue;
    const childEdges = edges.filter(
      (e) =>
        children.some((c) => c.id === e.source) &&
        children.some((c) => c.id === e.target),
    );
    const { boundingBox, pts } = await layoutScope(children, childEdges);
    for (const [id, p] of pts) {
      positions.set(id, p);
    }
    groupSizes.set(g.id, {
      w: boundingBox.w + GROUP_SIDE_PADDING * 2,
      h: boundingBox.h + GROUP_TOP_PADDING + GROUP_BOTTOM_PADDING,
    });
  }

  // Root scope last, with each group sized per the result above.
  const rootChildren = byParent.get(null) ?? [];
  const rootChildrenSized: LayoutNode[] = rootChildren.map((n) => {
    if (n.kind === 'group' && n.expanded !== false && groupSizes.has(n.id)) {
      const s = groupSizes.get(n.id)!;
      return { ...n, width: s.w, height: s.h };
    }
    return n;
  });
  const rootEdges = edges.filter(
    (e) =>
      rootChildren.some((c) => c.id === e.source) &&
      rootChildren.some((c) => c.id === e.target),
  );
  const { pts: rootPts } = await layoutScope(rootChildrenSized, rootEdges);
  for (const [id, p] of rootPts) positions.set(id, p);

  return { positions, groupSizes };
}

interface ScopeResult {
  boundingBox: { w: number; h: number };
  pts: Map<string, { x: number; y: number }>;
}

async function layoutScope(
  children: readonly LayoutNode[],
  edges: readonly LayoutEdge[],
): Promise<ScopeResult> {
  const components = findConnectedComponents(children, edges);
  const pts = new Map<string, { x: number; y: number }>();
  let cursorX = 0;
  let maxY = 0;

  for (const comp of components) {
    const compChildren = children.filter((c) => comp.has(c.id));
    const compEdges = edges.filter(
      (e) => comp.has(e.source) && comp.has(e.target),
    );
    const graph: ElkNode = {
      id: 'root',
      layoutOptions: LAYOUT_OPTIONS,
      children: compChildren.map((c, i) => ({
        id: c.id,
        width: c.width,
        height: c.height,
        layoutOptions: {
          'elk.position': `(0, ${i})`,
        },
      })),
      edges: compEdges.map((e) => ({
        id: e.id,
        sources: [e.source],
        targets: [e.target],
      })) as unknown as ElkNode['edges'],
    };
    const laid = await elk.layout(graph);
    let compW = 0;
    let compH = 0;
    for (const c of laid.children ?? []) {
      if (c.x != null && c.y != null) {
        pts.set(c.id, { x: c.x + cursorX, y: c.y });
        compW = Math.max(compW, c.x + (c.width ?? 0));
        compH = Math.max(compH, c.y + (c.height ?? 0));
      }
    }
    cursorX += compW + COMPONENT_GAP;
    maxY = Math.max(maxY, compH);
  }

  return {
    boundingBox: { w: Math.max(0, cursorX - COMPONENT_GAP), h: maxY },
    pts,
  };
}

function findConnectedComponents(
  children: readonly LayoutNode[],
  edges: readonly LayoutEdge[],
): Set<string>[] {
  const parent = new Map<string, string>();
  for (const n of children) parent.set(n.id, n.id);
  function find(x: string): string {
    let p = parent.get(x) ?? x;
    while (p !== (parent.get(p) ?? p)) {
      const next = parent.get(p) ?? p;
      parent.set(p, next);
      p = next;
    }
    parent.set(x, p);
    return p;
  }
  function union(a: string, b: string): void {
    const ra = find(a);
    const rb = find(b);
    if (ra !== rb) parent.set(ra, rb);
  }
  for (const e of edges) {
    if (parent.has(e.source) && parent.has(e.target)) union(e.source, e.target);
  }
  const byRoot = new Map<string, Set<string>>();
  for (const n of children) {
    const r = find(n.id);
    if (!byRoot.has(r)) byRoot.set(r, new Set());
    byRoot.get(r)!.add(n.id);
  }
  return Array.from(byRoot.values());
}

// Convert xyflow nodes + edges into LayoutInput shape. Width/height
// use measured sizes when available, else fall back to defaults.
export function buildLayoutInput(
  nodes: readonly FlowNode[],
  edges: readonly FlowEdge[],
): { nodes: LayoutNode[]; edges: LayoutEdge[] } {
  const layoutNodes: LayoutNode[] = nodes.map((n) => {
    const measured = (n as FlowNode & { measured?: { width?: number; height?: number } }).measured;
    const width =
      measured?.width ?? (typeof n.width === 'number' ? n.width : 260);
    const height =
      measured?.height ?? (typeof n.height === 'number' ? n.height : 120);
    let kind: LayoutNode['kind'];
    if (n.type === 'weftGroup' || n.type === 'weftGroupCollapsed') kind = 'group';
    else if (n.type === 'annotation') kind = 'annotation';
    else kind = 'regular';
    return {
      id: n.id,
      parentId: (n.parentId as string | undefined) ?? null,
      width,
      height,
      kind,
      expanded: n.type === 'weftGroup',
    };
  });
  // Edges with __inner handles stay for routing but don't influence
  // connected-components if they cross a group boundary — filter them
  // so scope layout ignores boundary edges (which would pull root
  // nodes into a group component).
  const layoutEdges: LayoutEdge[] = edges
    .filter(
      (e) =>
        !(e.sourceHandle ?? '').endsWith('__inner') &&
        !(e.targetHandle ?? '').endsWith('__inner'),
    )
    .map((e) => ({ id: e.id, source: e.source, target: e.target }));
  return { nodes: layoutNodes, edges: layoutEdges };
}
