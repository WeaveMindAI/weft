// Preflight: reject a gesture's op batch BEFORE it enters the pending queue,
// producing the same `{ok:false, reason}` shape a server rejection does so
// both flow through one rollback path. Three webview-only rules (lock, cycle,
// scope) plus a dry-run apply against the current visible project, which
// catches everything the server would reject semantically (missing refs,
// duplicate ids, kind mismatches, orphaning moves) without a round-trip.
// The server stays the authority: anything preflight misses comes back as an
// `editApplied {ok:false}` and rolls back through the same handler.

import type { ProjectDefinition } from '$lib/types';
import { isContainerNodeType } from '$lib/types';
import type { EditOp } from '../../../shared/protocol';
import { applyOpsToProject, type ProjectionCatalog } from './apply';
import { isLogicLocked, lockReasonText, type LockState } from './types';

export type PreflightResult = { ok: true } | { ok: false; reason: string };

/** Check one gesture's op batch against the current visible project. The
 *  batch is atomic (the server applies it as one transaction), so any failing
 *  op rejects the whole batch. */
export function runPreflight(
  ops: EditOp[],
  visible: ProjectDefinition,
  lock: LockState,
  catalog: ProjectionCatalog,
  now: number,
): PreflightResult {
  if (ops.length === 0) return { ok: true };
  if (isLogicLocked(lock, now)) {
    return { ok: false, reason: lockReasonText(lock, now) };
  }
  // Fold the batch op-by-op: each scope/cycle check runs against the state the
  // op will ACTUALLY apply to (the prior ops already applied), and the fold
  // doubles as the dry-run apply (a semantic failure reads exactly like the
  // server rejection it predicts). Checking every addEdge against the original
  // `visible` instead missed cyclic batches like `[A->B, B->A]` (each edge
  // alone is acyclic) and skipped checks on an edge to a node added earlier in
  // the same batch.
  let working = visible;
  for (const op of ops) {
    if (op.op === 'addEdge') {
      const scopeCheck = checkSameScope(op, working);
      if (scopeCheck) return { ok: false, reason: scopeCheck };
      if (createsCycle(op, working)) {
        return { ok: false, reason: 'This connection would create a cycle' };
      }
    }
    try {
      working = applyOpsToProject(working, [op], catalog);
    } catch (err) {
      return { ok: false, reason: err instanceof Error ? err.message : String(err) };
    }
  }
  return { ok: true };
}

/** Resolve an addEdge endpoint to (scoped node id, isInner). Returns null when
 *  the ref doesn't resolve; the dry-run apply reports that case with the
 *  server's wording, so scope/cycle checks just skip it. */
function endpointId(
  ref: string,
  visible: ProjectDefinition,
  scopeGroup: string | null,
): { id: string; inner: boolean } | null {
  const scope = scopeGroup == null
    ? undefined
    : visible.nodes.find((n) => n.id === scopeGroup)
      ?? visible.nodes.find((n) => isContainerNodeType(n.nodeType) && localOf(n.id) === scopeGroup);
  if (scopeGroup != null && !scope) return null;
  if (ref === 'self') {
    return scope ? { id: scope.id, inner: true } : null;
  }
  const id = scope ? `${scope.id}.${ref}` : ref;
  return visible.nodes.some((n) => n.id === id) ? { id, inner: false } : null;
}

function localOf(id: string): string {
  const i = id.lastIndexOf('.');
  return i < 0 ? id : id.slice(i + 1);
}

/** Both endpoints of a connection must live in the same scope: a node's scope
 *  is its parent container; a container's `self` (inner) side IS the
 *  container's body scope. */
function checkSameScope(
  op: Extract<EditOp, { op: 'addEdge' }>,
  visible: ProjectDefinition,
): string | null {
  const src = endpointId(op.source, visible, op.scopeGroup);
  const tgt = endpointId(op.target, visible, op.scopeGroup);
  if (!src || !tgt) return null; // unresolvable ref: the dry-run apply reports it
  const scopeOf = (ep: { id: string; inner: boolean }): string => {
    if (ep.inner) return ep.id; // the container's own body
    const node = visible.nodes.find((n) => n.id === ep.id);
    return node?.parentId ?? '__root__';
  };
  const a = scopeOf(src);
  const b = scopeOf(tgt);
  return a === b ? null : 'Cannot connect across scopes: both ends must live in the same group';
}

/** Node-level cycle detection over the visible edges plus the new edge.
 *  `__inner`-handled edges are group interface pass-throughs (data flowing
 *  through the group), not dependencies, so they don't participate. */
function createsCycle(
  op: Extract<EditOp, { op: 'addEdge' }>,
  visible: ProjectDefinition,
): boolean {
  const src = endpointId(op.source, visible, op.scopeGroup);
  const tgt = endpointId(op.target, visible, op.scopeGroup);
  if (!src || !tgt || src.inner || tgt.inner) return false;
  const adjacency = new Map<string, string[]>();
  const push = (from: string, to: string) => {
    const list = adjacency.get(from);
    if (list) list.push(to);
    else adjacency.set(from, [to]);
  };
  for (const e of visible.edges) {
    if (e.sourceHandle?.endsWith('__inner') || e.targetHandle?.endsWith('__inner')) continue;
    push(e.source, e.target);
  }
  push(src.id, tgt.id);
  const visited = new Set<string>();
  const stack = new Set<string>();
  const dfs = (node: string): boolean => {
    if (stack.has(node)) return true;
    if (visited.has(node)) return false;
    visited.add(node);
    stack.add(node);
    for (const next of adjacency.get(node) ?? []) {
      if (dfs(next)) return true;
    }
    stack.delete(node);
    return false;
  };
  return dfs(src.id);
}
