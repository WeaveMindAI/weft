// Aiming a run at nodes.
//
// A run normally kicks every root of the graph and pulses run whatever
// they reach. Aiming it at some nodes kicks only the roots those nodes
// need, so any node can be a target; the dispatcher refuses only a
// target that names no node.

/// The Run button's label. Plain until the user has aimed the run somewhere.
export function runLabel(targetCount: number): string {
  if (targetCount === 0) return 'Run Project';
  if (targetCount === 1) return 'Run 1 target';
  return `Run ${targetCount} targets`;
}

/// What an aimed run would actually execute, read off the graph: the
/// upstream walk from every target, with triggers as terminators (a
/// trigger in the walk is included but not walked through), closed
/// over scopes (a node inside a group or loop brings the whole
/// container, whose own inputs are then walked), the same subgraph the
/// dispatcher computes for `run` with targets.
///
/// - `avoidsTriggers`: at least one target, and the joined subgraph
///   holds no trigger. Such a run is an ordinary one-shot even in a
///   project that HAS triggers, so the action bar offers Run next to
///   the trigger lifecycle (a maintenance branch fired by hand).
/// - `infraIds`: the infra nodes inside the joined subgraph. Only
///   these gate the aimed run's readiness; infra outside it never
///   runs, so it has no say.
///
/// SYNC: runTargetFacts <-> crates/weft-dispatcher/src/api/project.rs RunSubgraph::of
export function runTargetFacts(
  targets: Iterable<string>,
  nodes: Array<{ id: string; isTrigger: boolean; isInfra: boolean; parentId?: string }>,
  edges: Array<{ source: string; target: string }>,
): { avoidsTriggers: boolean; infraIds: string[] } {
  const byId = new Map(nodes.map((n) => [n.id, n]));
  const stack = [...targets];
  const empty = stack.length === 0;
  const seen = new Set<string>();
  const infraIds: string[] = [];
  let sawTrigger = false;
  while (stack.length > 0) {
    const id = stack.pop()!;
    if (seen.has(id)) continue;
    seen.add(id);
    const node = byId.get(id);
    if (node?.isInfra) infraIds.push(id);
    // A node inside a container brings the container (its inputs are
    // what the scope needs), and a container brings everything inside
    // it. Both directions, so the walk reaches all the way down a
    // nesting: the parent pushes its children, and a child that is
    // itself a container pushes its own. Walking only siblings stopped
    // one level in, and an infra node or trigger two groups deep was
    // missed here while the dispatcher counted it.
    if (node?.parentId) stack.push(node.parentId);
    for (const other of nodes) {
      if (other.parentId === id) stack.push(other.id);
    }
    if (node?.isTrigger) {
      sawTrigger = true;
      continue; // terminator: included, not walked through
    }
    for (const e of edges) {
      if (e.target === id) stack.push(e.source);
    }
  }
  return { avoidsTriggers: !empty && !sawTrigger, infraIds };
}

/// Targets survive edits, but a node that no longer exists cannot be
/// one. Drop those rather than sending the dispatcher something it
/// will refuse.
export function pruneTargets(targets: Iterable<string>, nodes: Array<{ id: string }>): Set<string> {
  const kept = new Set<string>();
  for (const id of targets) {
    if (nodes.some((n) => n.id === id)) kept.add(id);
  }
  return kept;
}
