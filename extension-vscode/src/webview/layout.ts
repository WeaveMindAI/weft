// ELK-based auto-layout for the graph. Runs in the webview whenever
// the structural signature of the graph changes (new nodes or
// edges) and the node doesn't already have saved positions.
//
// Ported from v1's runAutoOrganize(). Uses layered direction =
// LEFT-RIGHT, which matches weft's "data flows left to right" visual.

import ELK, { type ElkNode } from 'elkjs/lib/elk.bundled.js';
import type { Edge, Node } from '@xyflow/svelte';

const elk = new ELK();

interface LayoutInput {
  nodes: Node[];
  edges: Edge[];
  nodeSize?: { width: number; height: number };
}

export async function autoLayout(
  input: LayoutInput,
): Promise<Record<string, { x: number; y: number }>> {
  const { nodes, edges } = input;
  const size = input.nodeSize ?? { width: 200, height: 80 };

  const children: ElkNode[] = nodes.map((n) => ({
    id: n.id,
    width: size.width,
    height: size.height,
  }));
  const elkEdges = edges.map((e) => ({
    id: e.id,
    sources: [e.source],
    targets: [e.target],
  }));

  const graph: ElkNode = {
    id: 'root',
    layoutOptions: {
      'elk.algorithm': 'layered',
      'elk.direction': 'RIGHT',
      'elk.layered.spacing.nodeNodeBetweenLayers': '80',
      'elk.spacing.nodeNode': '40',
    },
    children,
    edges: elkEdges as any,
  };

  const laid = await elk.layout(graph);
  const positions: Record<string, { x: number; y: number }> = {};
  for (const c of laid.children ?? []) {
    if (c.x !== undefined && c.y !== undefined) {
      positions[c.id] = { x: c.x, y: c.y };
    }
  }
  return positions;
}
