// Turn a node-update's config payload into the MINIMAL set of source ops.
//
// Update senders spread the node's FULL config (`{...data.config, key: v}`),
// so most keys arrive unchanged. Emitting an op per present key would
// round-trip no-op source edits on every interaction, and worse: a pure
// expand/collapse toggle (a LAYOUT change) would emit phantom source ops
// whose projection round-trip races the layout persist and wipes the toggle.
// Diffing against the node's current (projected) config keeps source ops to
// exactly what changed.

import type { EditOp } from '../../../shared/protocol';
import { formatConfigValue } from '$lib/value-format';

/// Keys that are view-state (layout file) or webview plumbing, never source.
/// The ONE definition: the dispatch ladder and the duplicate-node path import
/// this instead of re-listing the keys (a drifted third copy already emitted
/// `configCollapsed` into source). `parentId` is non-source but is owned by
/// the layout file's scope re-key, not a live node merge, so VIEW_KEYS (the
/// keys live-merged into the rendered node) excludes it.
export const NON_SOURCE_KEYS = new Set(['parentId', 'textareaHeights', 'width', 'height', 'expanded', 'configCollapsed']);

/// View-state keys merged LIVE into the rendered node for instant feedback
/// (resize/collapse/textarea), never sent as source. Everything in
/// NON_SOURCE_KEYS except `parentId` (owned by the layout re-key, not a merge).
export const VIEW_KEYS = [...NON_SOURCE_KEYS].filter(k => k !== 'parentId');

function sameConfigValue(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  const aUnset = a === undefined || a === null;
  const bUnset = b === undefined || b === null;
  if (aUnset || bUnset) return aUnset && bUnset;
  // Compare by canonical source token: object/array values (form schemas,
  // JSON configs) are recreated per render, so identity can't be trusted.
  return formatConfigValue(a) === formatConfigValue(b);
}

/** Source ops for the keys in `updated` whose value differs from `current`
 *  (the node's projected config). `isLoopConfig` routes to the Loop op
 *  family (the Rust dispatch rejects a generic SetConfig on a Loop decl).
 *  Unset (null/undefined) on a previously-set key emits a remove. */
export function diffConfigOps(
  nodeId: string,
  updated: Record<string, unknown>,
  current: Record<string, unknown>,
  isLoopConfig: boolean,
): EditOp[] {
  const ops: EditOp[] = [];
  for (const [key, value] of Object.entries(updated)) {
    if (NON_SOURCE_KEYS.has(key)) continue;
    if (sameConfigValue(value, current[key])) continue;
    if (value === undefined || value === null) {
      ops.push(isLoopConfig ? { op: 'removeLoopConfig', loopId: nodeId, key } : { op: 'removeConfig', node: nodeId, key });
    } else {
      ops.push(isLoopConfig
        ? { op: 'setLoopConfig', loopId: nodeId, key, value: formatConfigValue(value) }
        : { op: 'setConfig', node: nodeId, key, value: formatConfigValue(value) });
    }
  }
  return ops;
}
