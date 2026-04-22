// Rewrite edges so group boundary passthroughs disappear and their
// endpoints collapse onto the group node. The source/target becomes
// the group's id; handles get `__inner` appended when they sit on
// the inside of the boundary (the side children wire into).
//
// v1 scheme — `weft-parser.ts:4543-4554`, `weft-parser.ts:3285-3403`:
//   • source = gid__in   ⇒ source = gid, sourceHandle += __inner
//     (internal source side of an in-port: a child reads this)
//   • target = gid__out  ⇒ target = gid, targetHandle += __inner
//     (internal target side of an out-port: a child writes here)
//   • target = gid__in   ⇒ target = gid (bare; external side)
//   • source = gid__out  ⇒ source = gid (bare; external side)
//
// For cycle detection and scope validation downstream, callers
// distinguish internal vs external sides by the `__inner` suffix.

import type { Edge } from '../../shared/protocol';

export interface RewrittenEdge extends Edge {
  sourceHandleRaw: string | null;
  targetHandleRaw: string | null;
}

export function rewriteEdges(
  edges: readonly Edge[],
  groupIds: Set<string>,
): RewrittenEdge[] {
  const out: RewrittenEdge[] = [];
  for (const e of edges) {
    out.push(rewriteOne(e, groupIds));
  }
  return out;
}

function rewriteOne(e: Edge, groupIds: Set<string>): RewrittenEdge {
  let source = e.source;
  let target = e.target;
  let sourceHandle = e.sourceHandle;
  let targetHandle = e.targetHandle;

  const srcBoundary = parseBoundary(e.source, groupIds);
  const tgtBoundary = parseBoundary(e.target, groupIds);

  if (srcBoundary?.role === 'In') {
    // __in.output drives a child — internal side of an in-port.
    source = srcBoundary.groupId;
    sourceHandle = withInner(sourceHandle);
  } else if (srcBoundary?.role === 'Out') {
    // __out drives an external consumer — bare external side.
    source = srcBoundary.groupId;
  }

  if (tgtBoundary?.role === 'Out') {
    // A child's output lands on __out.input — internal side of an
    // out-port.
    target = tgtBoundary.groupId;
    targetHandle = withInner(targetHandle);
  } else if (tgtBoundary?.role === 'In') {
    // An external node feeds __in.input — bare external side.
    target = tgtBoundary.groupId;
  }

  return {
    ...e,
    source,
    target,
    sourceHandle,
    targetHandle,
    sourceHandleRaw: e.sourceHandle,
    targetHandleRaw: e.targetHandle,
  };
}

interface BoundaryMatch {
  groupId: string;
  role: 'In' | 'Out';
}

function parseBoundary(nodeId: string, groupIds: Set<string>): BoundaryMatch | null {
  if (nodeId.endsWith('__in')) {
    const gid = nodeId.slice(0, -4);
    if (groupIds.has(gid)) return { groupId: gid, role: 'In' };
  }
  if (nodeId.endsWith('__out')) {
    const gid = nodeId.slice(0, -5);
    if (groupIds.has(gid)) return { groupId: gid, role: 'Out' };
  }
  return null;
}

function withInner(handle: string | null): string | null {
  if (handle == null) return null;
  return handle.endsWith('__inner') ? handle : `${handle}__inner`;
}

// Strip `__inner` for color/handle lookups (v1 line 397-407 also
// strips before `outputs.find`).
export function stripInner(handle: string | null): string {
  if (!handle) return '';
  return handle.endsWith('__inner') ? handle.slice(0, -7) : handle;
}
