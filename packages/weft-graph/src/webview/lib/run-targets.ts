// Which nodes a run can be aimed at.
//
// A run normally starts from every output node and walks upstream, so what
// executes is exactly what some output needs. Aiming it at a subset is the
// same walk with a smaller starting set, and the dispatcher refuses a target
// that is not an output node, so the graph must only ever offer outputs.
//
// SYNC: isOutputNode <-> crates/weft-core/src/project.rs NodeDefinition::is_output

/// A node's `_is_output` config overrides its type's `isOutputDefault`, which
/// is how a project turns any node into a deliverable.
export function isOutputNode(config: unknown, features: unknown): boolean {
  const override =
    config && typeof config === 'object'
      ? (config as Record<string, unknown>)['_is_output']
      : undefined;
  if (typeof override === 'boolean') return override;
  const declared =
    features && typeof features === 'object'
      ? (features as Record<string, unknown>)['isOutputDefault']
      : undefined;
  return declared === true;
}

/// The Run button's label. Plain until the user has aimed the run somewhere.
export function runLabel(targetCount: number): string {
  if (targetCount === 0) return 'Run Project';
  if (targetCount === 1) return 'Run 1 target';
  return `Run ${targetCount} targets`;
}

/// What an aimed run would actually execute, read off the graph: the
/// upstream walk from every target, with triggers as terminators (a
/// trigger in the walk is included but not walked through), the same
/// subgraph the dispatcher computes for `run` with targets.
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
  nodes: Array<{ id: string; isTrigger: boolean; isInfra: boolean }>,
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

/// Targets survive edits, but a node that no longer exists, or that stopped
/// being an output, cannot be one. Drop those rather than sending the
/// dispatcher something it will refuse.
export function pruneTargets(
  targets: Iterable<string>,
  nodes: Array<{ id: string; data?: { config?: unknown; features?: unknown } }>,
): Set<string> {
  const kept = new Set<string>();
  for (const id of targets) {
    const node = nodes.find((n) => n.id === id);
    if (node && isOutputNode(node.data?.config, node.data?.features)) kept.add(id);
  }
  return kept;
}
