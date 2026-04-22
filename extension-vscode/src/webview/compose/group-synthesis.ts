// Synthesize virtual group view nodes from GroupDefinition[]. v1's
// frontend parser kept `nodeType: 'Group'` NodeInstance entries in
// the public output. Our v2 compiler flattens to Passthrough In/Out
// + child nodes. This function rebuilds the v1-equivalent structured
// view for rendering; the dispatcher runtime continues to see the
// flat shape.

import type {
  GroupDefinition,
  NodeDefinition,
  ProjectDefinition,
} from '../../shared/protocol';
import type { LayoutMap, ViewNode } from './types';

export interface GroupSynthesisResult {
  groupNodes: ViewNode[];
  regularNodes: ViewNode[];
  // Passthrough ids the caller should filter out (they live in
  // `project.nodes` but must not render).
  passthroughIds: Set<string>;
}

export function synthesizeGroupNodes(
  project: ProjectDefinition,
  layout: LayoutMap,
): GroupSynthesisResult {
  const passthroughIds = new Set<string>();
  for (const g of project.groups) {
    passthroughIds.add(`${g.id}__in`);
    passthroughIds.add(`${g.id}__out`);
  }

  const groupNodes: ViewNode[] = project.groups.map((g) => {
    const saved = layout[g.id] ?? {};
    const config: Record<string, unknown> = {
      // Groups default to EXPANDED (v1: `isExpanded = (config.expanded as boolean) ?? true`).
      expanded: saved.expanded ?? true,
      width: saved.w ?? 400,
      height: saved.h ?? 300,
      parentId: g.parentGroupId ?? undefined,
    };
    return {
      id: g.id,
      kind: 'group',
      nodeType: 'Group',
      label: groupLocalName(g),
      rawParentId: g.parentGroupId,
      inputs: g.inPorts,
      outputs: g.outPorts,
      config,
      features: {
        oneOfRequired: g.oneOfRequired,
        correlatedPorts: [],
        canAddInputPorts: true,
        canAddOutputPorts: true,
        hasFormSchema: false,
      },
      groupDef: g,
      source: null,
    };
  });

  const regularNodes: ViewNode[] = [];
  for (const n of project.nodes) {
    if (passthroughIds.has(n.id)) continue;
    if (n.groupBoundary) continue;
    // Merge the layout sidecar's expanded / width / height on top of
    // the compiler config. Layout-only keys never round-trip through
    // the .weft source; they live in .layout.json.
    const saved = layout[n.id] ?? {};
    const mergedConfig: Record<string, unknown> = { ...(n.config ?? {}) };
    if (saved.expanded !== undefined) mergedConfig.expanded = saved.expanded;
    if (saved.w !== undefined) mergedConfig.width = saved.w;
    if (saved.h !== undefined) mergedConfig.height = saved.h;
    regularNodes.push({
      id: n.id,
      kind: n.nodeType === 'Annotation' ? 'annotation' : 'regular',
      nodeType: n.nodeType,
      label: n.label,
      rawParentId: resolveParentGroup(n, project),
      inputs: n.inputs,
      outputs: n.outputs,
      config: mergedConfig,
      features: n.features,
      groupDef: null,
      source: n,
    });
  }

  return { groupNodes, regularNodes, passthroughIds };
}

// v1 strips the scope prefix to recover the locally-written name
// ("Outer.Inner" → "Inner"). The group header is edited by local
// name, not scoped id. Identical behaviour to v1 parser line 4389.
export function groupLocalName(g: GroupDefinition): string {
  if (g.label) return stripScopePrefix(g.label, g.parentGroupId);
  return stripScopePrefix(g.id, g.parentGroupId);
}

function stripScopePrefix(id: string, parentId: string | null): string {
  if (!parentId) return id;
  const prefix = parentId + '.';
  return id.startsWith(prefix) ? id.slice(prefix.length) : id;
}

// Parent group for a regular node = the deepest group whose id is
// in the node's scope chain. Scope is stored as `scope: string[]`.
function resolveParentGroup(
  node: NodeDefinition,
  project: ProjectDefinition,
): string | null {
  const groupIds = new Set(project.groups.map((g) => g.id));
  // Walk deepest-first so the direct parent wins over ancestors.
  for (let i = node.scope.length - 1; i >= 0; i--) {
    const s = node.scope[i];
    if (groupIds.has(s)) return s;
  }
  return null;
}
