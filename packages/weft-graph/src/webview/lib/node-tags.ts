// The one place that knows how a node's tags live in its config. Tags are
// stored under the reserved `_tags` config key as an array of strings (the same
// key the compiler reads for signal-scoping). Hosts read + write tags through
// here so the shape never forks.
//
// SYNC: TAGS_CONFIG_KEY <-> crates/weft-core/src/tag.rs (TAGS_CONFIG_KEY)

import type { NodeInstance } from './types';

// The reserved config key that holds a node's tags. Must equal weft-core's
// `tag::TAGS_CONFIG_KEY` (see the SYNC marker above); they are one concept split
// across the language boundary.
export const TAGS_CONFIG_KEY = '_tags';

/// A node's tags, normalized to a string[] (empty when unset or malformed).
export function nodeTags(node: Pick<NodeInstance, 'config'>): string[] {
  const raw = node.config?.[TAGS_CONFIG_KEY];
  if (!Array.isArray(raw)) return [];
  return raw.filter((t): t is string => typeof t === 'string');
}
