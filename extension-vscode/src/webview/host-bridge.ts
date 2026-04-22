// Bridge between the VS Code host and v1's ProjectEditor.
//
// Responsibility: translate the dispatcher's ProjectDefinition
// (flat passthroughs + separate groups array) into v1's
// ProjectDefinition shape (groups folded back into `nodes` as
// nodeType:"Group" NodeInstances with __inner handle routing).
// Also caches the latest parse response so the v1 parser shim
// (`parseWeft`) can return it synchronously.

import type {
  ParseResponse,
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
  LaneMode,
} from './lib/types';

function toV1Port(p: HostPort): V1Port {
  return {
    name: p.name,
    portType: p.portType,
    required: p.required,
    description: p.description ?? undefined,
    laneMode: (p.laneMode ?? undefined) as LaneMode | undefined,
    laneDepth: p.laneDepth ?? undefined,
    configurable: p.configurable,
  };
}

function toV1Edge(e: HostEdge, groupIds: Set<string>): V1Edge {
  // Rewrite boundary edges to v1's shape: endpoints collapse onto
  // the group id + `__inner` suffix marks the internal side of a
  // boundary port. Matches v1 parser:4543-4554.
  let source = e.source;
  let target = e.target;
  let sourceHandle = e.sourceHandle ?? undefined;
  let targetHandle = e.targetHandle ?? undefined;

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
  return {
    id: g.id,
    nodeType: 'Group',
    label: localName(g.id, g.parentGroupId ?? undefined),
    config: {},
    position: { x: 0, y: 0 },
    parentId: g.parentGroupId ?? undefined,
    inputs: g.inPorts.map(toV1Port),
    outputs: g.outPorts.map(toV1Port),
    features: {
      oneOfRequired: g.oneOfRequired,
    },
    scope: g.parentGroupId ? [g.parentGroupId] : [],
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
  return {
    id: n.id,
    nodeType: n.nodeType,
    label: n.label,
    config: cleanConfig,
    position: n.position,
    parentId: resolveParentGroup(n, groupIds),
    inputs: n.inputs.map(toV1Port),
    outputs: n.outputs.map(toV1Port),
    features: n.features as unknown as NodeInstance['features'],
    scope: n.scope,
    groupBoundary: n.groupBoundary ?? undefined,
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
  return {
    id: host.id,
    name: host.name,
    description: host.description,
    weftCode,
    layoutCode,
    nodes: structuralNodes,
    edges,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };
}

// Cache of the latest parse response — v1's parseWeft shim reads
// from here. Declared in its own module so weft-parser's shim can
// import without App.svelte circular dep.
let latestResponse: ParseResponse | null = null;
let latestSource: string = '';
let latestLayoutCode: string = '';

export function setLatestParseResponse(
  response: ParseResponse,
  source: string,
  layoutCode: string,
): void {
  latestResponse = response;
  latestSource = source;
  latestLayoutCode = layoutCode;
}

export function getLatestParseResponse(): {
  response: ParseResponse;
  source: string;
  layoutCode: string;
} | null {
  if (!latestResponse) return null;
  return { response: latestResponse, source: latestSource, layoutCode: latestLayoutCode };
}
