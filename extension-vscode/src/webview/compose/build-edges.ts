// Build xyflow edges from ProjectDefinition + active-edge set +
// node visibility. Dedups by (target, targetHandle) — one driver
// per input (v1 line 971-1011).

import type { Edge as FlowEdge } from '@xyflow/svelte';
import { MarkerType } from '@xyflow/svelte';
import type {
  Edge,
  NodeDefinition,
  PortDefinition,
  ProjectDefinition,
} from '../../shared/protocol';
import { getPortTypeColor } from '../utils/colors';
import { rewriteEdges, stripInner } from './edge-rewrite';
import type { ViewNode } from './types';

export interface BuildEdgesContext {
  project: ProjectDefinition;
  viewNodes: readonly ViewNode[];
  hiddenNodeIds: ReadonlySet<string>;
  activeEdges: ReadonlySet<string>;
  edgeZBoost: Record<string, number>;
}

export function buildEdges(ctx: BuildEdgesContext): FlowEdge[] {
  const groupIds = new Set(ctx.viewNodes.filter((v) => v.kind === 'group').map((v) => v.id));
  const rewritten = rewriteEdges(ctx.project.edges, groupIds);

  // Dedup by (target, targetHandle); last-wins matches v1.
  const seen = new Map<string, (typeof rewritten)[number]>();
  for (const e of rewritten) {
    seen.set(`${e.target}:${e.targetHandle ?? 'default'}`, e);
  }

  const outputLookup = buildOutputLookup(ctx.viewNodes, ctx.project);

  const out: FlowEdge[] = [];
  for (const e of seen.values()) {
    const color = edgeColor(e.source, e.sourceHandle, outputLookup);
    const active = ctx.activeEdges.has(e.id);
    const hidden = ctx.hiddenNodeIds.has(e.source) || ctx.hiddenNodeIds.has(e.target);
    out.push({
      id: e.id,
      source: e.source,
      target: e.target,
      sourceHandle: e.sourceHandle ?? undefined,
      targetHandle: e.targetHandle ?? undefined,
      type: 'weft',
      animated: active,
      hidden,
      zIndex: 5 + (ctx.edgeZBoost[e.id] ?? 0),
      style: `stroke-width: ${active ? 3 : 2}px; stroke: ${color};`,
      markerEnd: {
        type: MarkerType.ArrowClosed,
        width: 20,
        height: 20,
        color,
      },
      class: active ? 'edge-active' : '',
    });
  }
  return out;
}

// Builds nodeId → Map<portName, PortDefinition> so edge-color lookup
// is O(1). Groups are included with their inPorts + outPorts so
// __inner edges resolve the right port type.
function buildOutputLookup(
  viewNodes: readonly ViewNode[],
  project: ProjectDefinition,
): Map<string, Map<string, PortDefinition>> {
  const result = new Map<string, Map<string, PortDefinition>>();
  for (const v of viewNodes) {
    const m = new Map<string, PortDefinition>();
    for (const p of v.outputs) m.set(p.name, p);
    // Groups also source from inPorts via __inner (the internal side
    // of an in-port is a `source` Handle).
    if (v.kind === 'group') {
      for (const p of v.inputs) m.set(p.name, p);
    }
    result.set(v.id, m);
  }
  // Also index raw passthroughs in case the caller rewrote incorrectly.
  for (const n of project.nodes) {
    if (!result.has(n.id)) {
      const m = new Map<string, PortDefinition>();
      for (const p of n.outputs) m.set(p.name, p);
      result.set(n.id, m);
    }
  }
  return result;
}

function edgeColor(
  source: string,
  sourceHandle: string | null,
  outputLookup: Map<string, Map<string, PortDefinition>>,
): string {
  const ports = outputLookup.get(source);
  if (!ports) return getPortTypeColor('');
  const port = ports.get(stripInner(sourceHandle));
  if (!port) return getPortTypeColor('');
  return getPortTypeColor(port.portType);
}

// Edge refs used for delete/reconnect surgical mutations. Handles
// the `__inner` → `self.port` convention.
export interface WeftEdgeRef {
  source: string;
  sourcePort: string;
  target: string;
  targetPort: string;
  scopeGroupLabel: string | null;
}

export function toWeftEdgeRef(
  sourceNodeId: string,
  sourceHandle: string | null,
  targetNodeId: string,
  targetHandle: string | null,
  nodesById: Map<string, NodeDefinition | ViewNode>,
): WeftEdgeRef {
  const srcInner = sourceHandle?.endsWith('__inner') ?? false;
  const tgtInner = targetHandle?.endsWith('__inner') ?? false;
  const srcPort = stripInner(sourceHandle) || 'value';
  const tgtPort = stripInner(targetHandle) || 'value';

  let source = sourceNodeId;
  let target = targetNodeId;
  let scopeGroupLabel: string | null = null;

  if (srcInner) {
    scopeGroupLabel = sourceNodeId;
    source = 'self';
  } else if (tgtInner) {
    scopeGroupLabel = targetNodeId;
    target = 'self';
  }
  // Convert scoped ids ("Outer.child") back to local names when
  // we're emitting a group-scoped connection.
  if (scopeGroupLabel) {
    const gPrefix = `${scopeGroupLabel}.`;
    if (source.startsWith(gPrefix)) source = source.slice(gPrefix.length);
    if (target.startsWith(gPrefix)) target = target.slice(gPrefix.length);
  }
  return { source, sourcePort: srcPort, target, targetPort: tgtPort, scopeGroupLabel };
}
