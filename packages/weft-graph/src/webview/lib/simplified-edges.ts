// The simplified view's wires: every connection between two nodes collapses
// onto their single in/out dots, and every line landing on the same pair of
// dots merges into ONE drawn edge. Kept pure (no xyflow, no colors) so the
// merge is tested on its own; `buildEdges` in ProjectEditorInner turns each
// merge into an edge.

import type { Edge } from './types';
import {
  SIMPLIFIED_IN_HANDLE,
  SIMPLIFIED_OUT_HANDLE,
  SIMPLIFIED_INNER_SOURCE_HANDLE,
  SIMPLIFIED_INNER_TARGET_HANDLE,
  SIMPLIFIED_LOOP_INDEX_HANDLE,
  SIMPLIFIED_LOOP_DONE_HANDLE,
} from './constants/simplified-view';

/** One drawn simplified edge: the dots it joins, and the type of every
 *  connection merged into it. */
export interface SimplifiedMerge {
  id: string;
  source: string;
  target: string;
  sourceHandle: string;
  targetHandle: string;
  types: Set<string>;
}

/** What the merge needs to know about the graph around a wire. */
export interface SimplifiedLookup {
  /** `index`/`done` are reserved implicit ports ONLY on loops; a plain
   *  Group may declare a port of either name, so the loop dots are gated
   *  on the node really being a loop, never on the handle string alone
   *  (a Group's `index__inner` edge would otherwise point at a
   *  `__simp_index` dot a GroupNode never renders, and vanish). */
  isLoop(id: string): boolean;
  /** The type of the port a wire leaves, to color the merged edge. */
  sourceType(e: Edge): string;
}

/** The dot a wire leaves from. A wire the parser marked `__inner` (an
 *  expanded container feeding one of its own inputs to a child) leaves
 *  the container's inner source dot; anything else the out dot. */
function sourceDot(e: Edge, lookup: SimplifiedLookup): string {
  if (e.sourceHandle === 'index__inner' && lookup.isLoop(e.source)) return SIMPLIFIED_LOOP_INDEX_HANDLE;
  return e.sourceHandle?.endsWith('__inner') ? SIMPLIFIED_INNER_SOURCE_HANDLE : SIMPLIFIED_OUT_HANDLE;
}

/** The dot a wire lands on: symmetric to `sourceDot`, a child writing the
 *  container's output lands on its inner target dot. */
function targetDot(e: Edge, lookup: SimplifiedLookup): string {
  if (e.targetHandle === 'done__inner' && lookup.isLoop(e.target)) return SIMPLIFIED_LOOP_DONE_HANDLE;
  return e.targetHandle?.endsWith('__inner') ? SIMPLIFIED_INNER_TARGET_HANDLE : SIMPLIFIED_IN_HANDLE;
}

/** Merge every wire landing on the SAME pair of dots of the SAME two nodes
 *  into one. The key is the resolved dots, not just the node ids, so an
 *  inner and an outer edge between two nodes stay apart. Handle ids are
 *  bare (xyflow pairs a handle with its node), so the node ids are in the
 *  key too. */
export function mergeSimplifiedEdges(edges: readonly Edge[], lookup: SimplifiedLookup): SimplifiedMerge[] {
  const byPair = new Map<string, SimplifiedMerge>();
  for (const e of edges) {
    const sourceHandle = sourceDot(e, lookup);
    const targetHandle = targetDot(e, lookup);
    const id = `simplified:${e.source}.${sourceHandle}->${e.target}.${targetHandle}`;
    let merged = byPair.get(id);
    if (!merged) {
      merged = { id, source: e.source, target: e.target, sourceHandle, targetHandle, types: new Set() };
      byPair.set(id, merged);
    }
    merged.types.add(lookup.sourceType(e));
  }
  return [...byPair.values()];
}
