// Bridge between the host parse and the graph editor.
//
// Responsibility: translate the Rust compiler's ProjectDefinition (flat
// passthroughs + separate groups array, scoped ids) into the editor's
// ProjectDefinition shape (groups folded back into `nodes` as
// nodeType:"Group" NodeInstances with __inner handle routing). The Rust
// parse is the single source of the graph; this only reshapes it for xyflow.

import type {
  ProjectDefinition as HostProject,
  NodeDefinition as HostNode,
  Edge as HostEdge,
  GroupDefinition as HostGroup,
  PortDefinition as HostPort,
} from '../shared/protocol';
import type {
  ProjectDefinition as V1Project,
  NodeInstance,
  Edge as V1Edge,
  PortDefinition as V1Port,
} from './lib/types';

function toV1Port(p: HostPort): V1Port {
  return {
    name: p.name,
    portType: p.portType,
    required: p.required,
    description: p.description ?? undefined,
    configurable: p.configurable,
    synthesizedFromCarry: p.synthesizedFromCarry,
  };
}

function toV1Edge(e: HostEdge, groupIds: Set<string>): V1Edge {
  // Rewrite boundary edges to v1's shape: endpoints collapse onto
  // the group id + `__inner` suffix marks the internal side of a
  // boundary port. Matches v1 parser:4543-4554.
  let source = e.source;
  let target = e.target;
  let sourceHandle = e.sourceHandle ?? null;
  let targetHandle = e.targetHandle ?? null;

  const srcBound = parseBoundary(e.source, groupIds);
  const tgtBound = parseBoundary(e.target, groupIds);

  if (srcBound?.role === 'In') {
    source = srcBound.groupId;
    sourceHandle = sourceHandle ? `${sourceHandle}__inner` : sourceHandle;
  } else if (srcBound?.role === 'Out') {
    source = srcBound.groupId;
  }
  if (tgtBound?.role === 'Out') {
    target = tgtBound.groupId;
    targetHandle = targetHandle ? `${targetHandle}__inner` : targetHandle;
  } else if (tgtBound?.role === 'In') {
    target = tgtBound.groupId;
  }

  return {
    id: e.id,
    source,
    target,
    sourceHandle,
    targetHandle,
  };
}

function parseBoundary(
  id: string,
  groupIds: Set<string>,
): { groupId: string; role: 'In' | 'Out' } | null {
  if (id.endsWith('__in')) {
    const gid = id.slice(0, -4);
    if (groupIds.has(gid)) return { groupId: gid, role: 'In' };
  }
  if (id.endsWith('__out')) {
    const gid = id.slice(0, -5);
    if (groupIds.has(gid)) return { groupId: gid, role: 'Out' };
  }
  return null;
}

function resolveParentGroup(node: HostNode, groupIds: Set<string>): string | undefined {
  for (let i = node.scope.length - 1; i >= 0; i--) {
    const s = node.scope[i];
    if (groupIds.has(s)) return s;
  }
  return undefined;
}

// Strip the scope prefix so nested groups show their local label in
// editors (v1 parser:4389 sets `label = group.originalName || id`).
function localName(id: string, parentId: string | undefined): string {
  if (!parentId) return id;
  const prefix = parentId + '.';
  return id.startsWith(prefix) ? id.slice(prefix.length) : id;
}

function groupToNodeInstance(g: HostGroup): NodeInstance {
  // `kind` is required on the wire; if it lands as anything other
  // than the two known values, the host/webview view of the project
  // has drifted (version skew, corruption). Silently treating an
  // unknown kind as Group would render a Loop as a regular group and
  // hide the divergence; surface it instead.
  if (g.kind !== 'group' && g.kind !== 'loop') {
    throw new Error(`host-bridge: unknown container kind '${String(g.kind)}' for id '${g.id}'`);
  }
  const isLoop = g.kind === 'loop';
  const parentId = g.parentGroupId ?? undefined;
  return {
    id: g.id,
    // Distinct nodeType per container so the renderer can pick a
    // loop-flavored xyflow type vs a group-flavored one. The lowering
    // (LoopIn/LoopOut boundary nodes) is unaffected by this.
    nodeType: isLoop ? 'Loop' : 'Group',
    label: localName(g.id, parentId),
    // The webview reads the parent group from `config.parentId` (buildNodes,
    // getLayoutKey, edge scoping, the ancestor-collapse walk all read it there),
    // so the structural parent MUST be mirrored into config on every parse. Before
    // this it lived only on the top-level `parentId` field, so a re-parse left
    // `config.parentId` undefined: the node's scoped `id` said it was nested but
    // the graph thought it was top-level, and the next move op read the wrong
    // scope (the "every other move is a no-op" desync). A loop carries its loop
    // config too; both a group and a loop need parentId mirrored for nesting.
    config: {
      ...(isLoop && g.loopConfig ? (g.loopConfig as Record<string, unknown>) : {}),
      ...(parentId ? { parentId } : {}),
      // GroupNode renders the description from `config.description`.
      ...(g.description ? { description: g.description } : {}),
    },
    position: { x: 0, y: 0 },
    parentId,
    inputs: g.inPorts.map(toV1Port),
    outputs: g.outPorts.map(toV1Port),
    features: {
      oneOfRequired: g.oneOfRequired,
    },
    scope: parentId ? [parentId] : [],
  };
}

function toV1Node(n: HostNode, groupIds: Set<string>): NodeInstance {
  // `label` is a first-class field on NodeDefinition; the compiler
  // extracts `label: "..."` lines from a node's config block and
  // promotes them. Defensively strip any leftover 'label' entry
  // from config so it doesn't show up as a synthetic text field in
  // the inline config form.
  const rawConfig = (n.config ?? {}) as Record<string, unknown>;
  const { label: _stripped, ...cleanConfig } = rawConfig;
  // The Rust parse resolves `@file` to the file's content in config. The
  // editor's invariant is that config holds the structural `@file` MARKER
  // ({path, type}), never resolved content, so it serializes safely and
  // renders file-backed. Put the structural ref back from `fileRefs`; the
  // resolved content is shown separately via the host's fileContents map.
  if (n.fileRefs) {
    for (const [key, ref] of Object.entries(n.fileRefs)) {
      cleanConfig[key] = { __weftFileRef: { path: ref.path, type: ref.type } };
    }
  }
  const parentId = resolveParentGroup(n, groupIds);
  // Mirror the structural parent into config.parentId, which is where the webview
  // reads it (see groupToNodeInstance for why). Without this a re-parse resets the
  // node's perceived scope to top-level while its scoped id stays nested.
  if (parentId) cleanConfig.parentId = parentId; else delete cleanConfig.parentId;
  return {
    id: n.id,
    nodeType: n.nodeType,
    label: n.label,
    config: cleanConfig,
    position: n.position,
    parentId,
    inputs: n.inputs.map(toV1Port),
    outputs: n.outputs.map(toV1Port),
    features: n.features,
    scope: n.scope,
    groupBoundary: n.groupBoundary ?? undefined,
    includePath: n.includePath,
  };
}

export function translateProject(
  host: HostProject,
  weftCode: string,
  layoutCode: string,
): V1Project {
  const groupIds = new Set(host.groups.map((g) => g.id));
  const passthroughIds = new Set<string>();
  for (const gid of groupIds) {
    passthroughIds.add(`${gid}__in`);
    passthroughIds.add(`${gid}__out`);
  }
  const structuralNodes: NodeInstance[] = [];
  // Groups first (v1 requires parent-before-child order).
  for (const g of host.groups) structuralNodes.push(groupToNodeInstance(g));
  for (const n of host.nodes) {
    if (passthroughIds.has(n.id)) continue;
    if (n.groupBoundary) continue;
    structuralNodes.push(toV1Node(n, groupIds));
  }
  const edges = host.edges.map((e) => toV1Edge(e, groupIds));

  // Position/size/expanded are NOT applied here. The Rust ProjectDefinition emits
  // position=(0,0) and carries no view-state; the companion `.layout` file owns it.
  // The merge of (structural nodes, layout) is done in ONE place, `buildNodes` in
  // ProjectEditorInner, so the initial render and every post-edit re-render share
  // the same pure merge and cannot drift.
  return {
    id: host.id,
    weftCode,
    layoutCode,
    nodes: structuralNodes,
    edges,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };
}
