// Assemble xyflow Node[] from a ProjectDefinition + catalog +
// layout + execution state. Pure: given identical inputs it returns
// identical output. Callers reuse existing nodes (by id) to preserve
// xyflow's in-place mutation semantics.

import type { Node } from '@xyflow/svelte';
import type {
  CatalogEntry,
  NodeDefinition,
  ProjectDefinition,
} from '../../shared/protocol';
import { synthesizeGroupNodes } from './group-synthesis';
import { computeVisibility, topoSortForXyflow } from './visibility';
import {
  executionsByViewNode,
  glowClassForLatest,
} from './exec-overlay';
import {
  computeMinNodeWidth,
  computeGroupMinHeight,
} from '../utils/node-geometry';
import type { ExecMap, LayoutMap, ViewNode } from './types';
import type { NodeViewData } from '../components/node-view-data';
import type { NodeExecution } from '../components/exec-types';

export interface BuildNodesContext {
  project: ProjectDefinition;
  catalog: Record<string, CatalogEntry>;
  layout: LayoutMap;
  exec: ExecMap;
  // Ids and wired sets precomputed by the caller (shared with edge
  // builder to avoid re-scanning project.edges twice).
  wiredByTarget: Record<string, Set<string>>;
  // Callbacks wired through to each component.
  onConfigChange: (nodeId: string, key: string, value: unknown) => void;
  onLabelChange: (nodeId: string, label: string | null) => void;
  onPortsChange: (
    nodeId: string,
    changes: { inputs?: unknown; outputs?: unknown },
  ) => void;
  // Previously-composed nodes (for position preservation).
  previous: readonly Node[];
  // Click-raise counter; callers increment externally.
  zIndexBoost: Record<string, number>;
}

export interface BuildNodesResult {
  nodes: Node[];
  // Ids that were rendered without any position (no saved layout,
  // no previous xyflow node). Graph.svelte uses this to decide
  // whether to auto-run ELK after composition.
  pendingLayoutIds: string[];
}

export function buildNodes(ctx: BuildNodesContext): BuildNodesResult {
  const synthesis = synthesizeGroupNodes(ctx.project, ctx.layout);
  const viewNodes: ViewNode[] = [
    ...synthesis.groupNodes,
    ...synthesis.regularNodes,
  ];
  const ordered = topoSortForXyflow(viewNodes);
  const visibility = computeVisibility(ordered);
  const execsById = executionsByViewNode(ordered, ctx.exec, ctx.project);

  const previousById = new Map(ctx.previous.map((n) => [n.id, n]));

  const out: Node[] = [];
  const pendingLayoutIds: string[] = [];
  for (const v of ordered) {
    const saved = ctx.layout[v.id] ?? {};
    const existing = previousById.get(v.id);
    const execs = execsById.get(v.id) ?? [];
    const glow = glowClassForLatest(execs);
    const visibilityStyle = visibility.styleById.get(v.id) ?? '';
    const zIndex =
      (visibility.zIndexById.get(v.id) ?? 4) + (ctx.zIndexBoost[v.id] ?? 0);

    const data = buildData(v, ctx, execs);
    const { type, sizeStyle, width, height } = shapeFor(v, saved, execs);

    const style = [visibilityStyle, sizeStyle].filter(Boolean).join(' ');
    const parentId = visibility.parentIdById.get(v.id) ?? undefined;
    const hasPos = existing?.position != null || saved.x != null;
    const position = existing?.position ?? {
      x: saved.x ?? 0,
      y: saved.y ?? 0,
    };
    if (!hasPos) pendingLayoutIds.push(v.id);

    const node: Node = {
      id: v.id,
      type,
      position,
      data,
      parentId,
      style,
      zIndex,
      class: [
        glow,
        visibility.hiddenNodeIds.has(v.id) ? 'node-hidden' : '',
        !hasPos ? 'node-pending-layout' : '',
      ].filter(Boolean).join(' '),
      selected: existing?.selected ?? false,
      width,
      height,
    } as Node;
    out.push(node);
  }
  return { nodes: out, pendingLayoutIds };
}

function buildData(
  v: ViewNode,
  ctx: BuildNodesContext,
  executions: NodeExecution[],
): NodeViewData {
  const catalog = v.source ? ctx.catalog[v.nodeType] ?? null : null;
  const wiredInputs = ctx.wiredByTarget[v.id] ?? new Set<string>();

  const sourceProxy: NodeDefinition = v.source ?? synthesizeGroupSource(v);

  return {
    node: sourceProxy,
    catalog,
    wiredInputs,
    executions,
    onConfigChange: ctx.onConfigChange,
    onLabelChange: ctx.onLabelChange,
    onPortsChange: ctx.onPortsChange,
  };
}

// For group virtual nodes, the components still expect a NodeDefinition-
// shaped object. Build a proxy that matches its key fields.
function synthesizeGroupSource(v: ViewNode): NodeDefinition {
  return {
    id: v.id,
    nodeType: 'Group',
    label: v.label,
    config: v.config,
    position: { x: 0, y: 0 },
    scope: v.rawParentId ? [v.rawParentId] : [],
    groupBoundary: null,
    inputs: v.inputs,
    outputs: v.outputs,
    features: v.features,
    entry: [],
  };
}

interface ShapeInfo {
  type: string;
  sizeStyle: string;
  width?: number;
  height?: number;
}

function shapeFor(
  v: ViewNode,
  saved: LayoutMap[string] | Record<string, never>,
  _execs: NodeExecution[],
): ShapeInfo {
  if (v.kind === 'annotation') {
    const w = saved.w ?? (v.config.width as number | undefined) ?? 250;
    const h = saved.h ?? (v.config.height as number | undefined) ?? 120;
    return {
      type: 'annotation',
      sizeStyle: `width: ${w}px; height: ${h}px;`,
      width: w,
      height: h,
    };
  }
  if (v.kind === 'group') {
    if (v.config.expanded) {
      const w = saved.w ?? (v.config.width as number | undefined) ?? 400;
      const hMin = computeGroupMinHeight(v.inputs.length, v.outputs.length);
      const h = Math.max(
        hMin,
        saved.h ?? (v.config.height as number | undefined) ?? 300,
      );
      return {
        type: 'weftGroup',
        sizeStyle: `width: ${w}px; height: ${h}px;`,
        width: w,
        height: h,
      };
    }
    const w = computeMinNodeWidth(v.inputs, v.outputs);
    return {
      type: 'weftGroupCollapsed',
      sizeStyle: `width: ${w}px;`,
      width: w,
    };
  }
  // Regular: respect explicit width/height when expanded, else min.
  const expanded = Boolean(v.config.expanded);
  const minW = computeMinNodeWidth(v.inputs, v.outputs);
  if (expanded) {
    const savedW = saved.w ?? (v.config.width as number | undefined);
    const w = savedW ? Math.max(minW, savedW) : Math.max(320, minW);
    const h = saved.h ?? (v.config.height as number | undefined);
    if (h) {
      return {
        type: 'weft',
        sizeStyle: `width: ${w}px; height: ${h}px;`,
        width: w,
        height: h,
      };
    }
    return { type: 'weft', sizeStyle: `width: ${w}px;`, width: w };
  }
  return { type: 'weft', sizeStyle: `width: ${minW}px;`, width: minW };
}
