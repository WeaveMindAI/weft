// Running the selected nodes.
//
// A run of targets kicks only the roots those nodes need, so any node
// can be a target. The graph never runs the whole program: the
// contextual bar's Run runs what is selected, and the CLI keeps every
// other way to start one.

import type { NodeFiringStatus } from '../../protocol';

/// The selected nodes Run runs: every one but the triggers and the
/// infra. A trigger starts a run when it fires, and run by hand it would
/// run without the event it exists to bring; an infra node is started,
/// never run. Each has its own buttons instead, so a selection of them
/// alone has no Run, and in a mixed selection Run runs the rest.
export function runTargetsOf<T extends { isTrigger: boolean; isInfra: boolean }>(selected: readonly T[]): T[] {
  return selected.filter((node) => !node.isTrigger && !node.isInfra);
}

/// What a run gesture says when the selection holds nothing Run runs
/// (`runTargetsOf` is empty).
export function whyNothingToRun(selected: readonly { isTrigger: boolean; isInfra: boolean }[]): string {
  if (selected.length === 0) return 'Select the nodes to run first: the run takes them and what they need.';
  const triggers = selected.some((node) => node.isTrigger);
  const infra = selected.some((node) => node.isInfra);
  if (triggers && infra) {
    return 'Run leaves out triggers and infra: a trigger runs when it fires, and infra is started, not run. Use their own buttons, or select the nodes after them to run those.';
  }
  if (infra) return 'Infra is started, not run, so Run leaves it out: start it with its own button, or select the nodes that read it to run them.';
  return 'A trigger runs when it fires, so Run leaves it out: turn it on, or select the nodes after it to run them.';
}

/// What a run check's answer depends on, by value: the selected places
/// and the program's source (`weft run --check` reads the files on
/// disk). The same places give the same key whatever array (or order)
/// they arrive in, so a graph redrawn around an unchanged selection and
/// program never asks the walk again, and any change of the source (the
/// text tab, a canvas edit once saved, an outside writer) asks it anew.
export function runCheckKey(targets: Iterable<string>, source: string): string {
  return JSON.stringify([[...new Set(targets)].sort(), source]);
}

/// How far the followed run got: the places it reached that are done
/// (their latest firing ended, whichever way), out of the places its
/// program runs (`null` when the run's program could not be read).
export function runProgress(
  firings: Record<string, Array<{ status: NodeFiringStatus }>>,
  scope: string[] | null | undefined,
): { done: number; total: number | null } {
  let done = 0;
  for (const rows of Object.values(firings)) {
    const last = rows[rows.length - 1];
    if (last && last.status !== 'running' && last.status !== 'waiting' && last.status !== 'paused') done += 1;
  }
  return { done, total: Array.isArray(scope) ? scope.length : null };
}
