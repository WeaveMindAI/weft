// Single entry point for the composition layer. `composeGraph` takes
// a ProjectDefinition + overlays (catalog, layout, exec, activeEdges)
// and returns the xyflow `{nodes, edges}` arrays ready to feed into
// SvelteFlow. Graph.svelte wires reactive state to this function and
// that's it.

import type { Edge as FlowEdge, Node } from '@xyflow/svelte';
import type {
  CatalogEntry,
  PortDefinition,
  ProjectDefinition,
} from '../../shared/protocol';
import { buildNodes } from './build-nodes';
import { buildEdges } from './build-edges';
import type { ExecMap, LayoutMap } from './types';

export interface ComposeInput {
  project: ProjectDefinition;
  catalog: Record<string, CatalogEntry>;
  layout: LayoutMap;
  exec: ExecMap;
  activeEdges: ReadonlySet<string>;
  previous: readonly Node[];
  nodeZBoost: Record<string, number>;
  edgeZBoost: Record<string, number>;
  onConfigChange: (nodeId: string, key: string, value: unknown) => void;
  onLabelChange: (nodeId: string, label: string | null) => void;
  onPortsChange: (
    nodeId: string,
    changes: { inputs?: PortDefinition[]; outputs?: PortDefinition[] },
  ) => void;
}

export interface ComposeOutput {
  nodes: Node[];
  edges: FlowEdge[];
  hiddenNodeIds: Set<string>;
}

export function composeGraph(input: ComposeInput): ComposeOutput {
  const wiredByTarget: Record<string, Set<string>> = {};
  for (const e of input.project.edges) {
    if (!e.targetHandle) continue;
    (wiredByTarget[e.target] ??= new Set()).add(e.targetHandle);
  }

  const nodes = buildNodes({
    project: input.project,
    catalog: input.catalog,
    layout: input.layout,
    exec: input.exec,
    wiredByTarget,
    onConfigChange: input.onConfigChange,
    onLabelChange: input.onLabelChange,
    onPortsChange: input.onPortsChange as unknown as (
      nodeId: string,
      changes: { inputs?: unknown; outputs?: unknown },
    ) => void,
    previous: input.previous,
    zIndexBoost: input.nodeZBoost,
  });

  const hiddenNodeIds = new Set<string>();
  for (const n of nodes) {
    const s = (n.style as string | undefined) ?? '';
    if (s.includes('display: none')) hiddenNodeIds.add(n.id);
  }

  // Group kinds recovered from the xyflow node list for edge-builder.
  const viewNodes = nodes.map((n) => ({
    id: n.id,
    kind:
      n.type === 'weftGroup' || n.type === 'weftGroupCollapsed'
        ? ('group' as const)
        : n.type === 'annotation'
        ? ('annotation' as const)
        : ('regular' as const),
    label: null,
    nodeType: '',
    rawParentId: (n.parentId ?? null) as string | null,
    inputs: ((n.data as { node?: { inputs?: PortDefinition[] } }).node?.inputs ?? []) as PortDefinition[],
    outputs: ((n.data as { node?: { outputs?: PortDefinition[] } }).node?.outputs ?? []) as PortDefinition[],
    config: {},
    features: {
      oneOfRequired: [],
      correlatedPorts: [],
      canAddInputPorts: false,
      canAddOutputPorts: false,
      hasFormSchema: false,
    },
    groupDef: null,
    source: null,
  }));

  const edges = buildEdges({
    project: input.project,
    viewNodes,
    hiddenNodeIds,
    activeEdges: input.activeEdges,
    edgeZBoost: input.edgeZBoost,
  });

  return { nodes, edges, hiddenNodeIds };
}
