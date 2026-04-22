// Would-create-cycle DFS. Edges with `__inner` on either end are
// skipped: they represent data flowing through a group boundary,
// not a true dependency (v1 line 1817-1848, parity/edges.md).

import type { Edge as FlowEdge } from '@xyflow/svelte';

export function wouldCreateCycle(
  source: string,
  target: string,
  edges: readonly FlowEdge[],
): boolean {
  if (source === target) return true;
  const adj = new Map<string, string[]>();
  for (const e of edges) {
    if (
      (e.sourceHandle ?? '').endsWith('__inner') ||
      (e.targetHandle ?? '').endsWith('__inner')
    ) {
      continue;
    }
    const list = adj.get(e.source) ?? [];
    list.push(e.target);
    adj.set(e.source, list);
  }
  const seedList = adj.get(target) ?? [];
  const stack = [...seedList];
  const seen = new Set<string>();
  while (stack.length) {
    const cur = stack.pop()!;
    if (cur === source) return true;
    if (seen.has(cur)) continue;
    seen.add(cur);
    const next = adj.get(cur);
    if (next) stack.push(...next);
  }
  return false;
}

export function getHandleScope(
  nodeId: string,
  handleId: string | null,
  nodes: readonly { id: string; type?: string; parentId?: string }[],
): string {
  const node = nodes.find((n) => n.id === nodeId);
  if (!node) return '__root__';
  const isGroup = node.type === 'weftGroup' || node.type === 'weftGroupCollapsed';
  if (isGroup && handleId && handleId.endsWith('__inner')) {
    return nodeId;
  }
  return node.parentId ?? '__root__';
}

export function isValidConnection(
  src: { nodeId: string; handleId: string | null },
  tgt: { nodeId: string; handleId: string | null },
  nodes: readonly { id: string; type?: string; parentId?: string }[],
): boolean {
  return (
    getHandleScope(src.nodeId, src.handleId, nodes) ===
    getHandleScope(tgt.nodeId, tgt.handleId, nodes)
  );
}
