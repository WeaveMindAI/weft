// Single source of truth for "does this node want a body-panel
// feed, and which one." Every place that needs to know whether a
// node is infra-backed or trigger-backed uses these helpers; do
// NOT inline the lookup elsewhere or the catalog/instance dual-
// path will drift again.
//
// The flags can come from EITHER the parsed NodeInstance or the
// catalog template (some are populated only on one side; the
// compiler enrich pass mirrors metadata onto the instance, but
// the webview also registers catalog entries from the dispatcher's
// describe-nodes endpoint with the raw metadata shape). Always
// check both.

import { NODE_TYPE_CONFIG } from '../nodes';
import type { IncludedContents } from '../../../protocol';

/// Minimal NodeInstance shape this module reads. Avoids importing
/// the full type from `../types` so this file stays focused.
interface RoleNodeShape {
  nodeType: string;
  features?: { isTrigger?: boolean } | undefined;
}


/// True iff the node is infra-backed (`/live` poller applies).
export function nodeRequiresInfra(node: RoleNodeShape & { requiresInfra?: boolean }): boolean {
  if (node.requiresInfra) return true;
  return !!NODE_TYPE_CONFIG[node.nodeType]?.requiresInfra;
}

/// True iff the node is a trigger (`/display` poller applies).
export function nodeIsTrigger(node: RoleNodeShape): boolean {
  if (node.features?.isTrigger) return true;
  const catalog = NODE_TYPE_CONFIG[node.nodeType];
  return !!catalog?.features?.isTrigger;
}

/// The two PROJECT-level questions, which are not the same as the
/// per-node ones above. An opaque `@include` node is neither infra nor a
/// trigger (it gets no poller, no infra slot, no mount URL), but the file
/// behind it can hold both, and in the interface parse that body is not
/// in the graph at all. So "can this project be activated" and "does this
/// project need its infra up" have to count through includes, which is
/// what `includeContents` is for. Get this wrong and a project whose only
/// trigger sits in an included file shows no Activate button.
type ProjectRoleNode = RoleNodeShape & {
  requiresInfra?: boolean;
  includeContents?: IncludedContents | undefined;
};

/// True iff the project declares any infra, an included file's included.
export function projectHasInfra(nodes: readonly ProjectRoleNode[]): boolean {
  return nodes.some((n) => nodeRequiresInfra(n) || !!n.includeContents?.requiresInfra);
}

/// True iff the project declares any trigger, an included file's included.
export function projectHasTriggers(nodes: readonly ProjectRoleNode[]): boolean {
  return nodes.some((n) => nodeIsTrigger(n) || !!n.includeContents?.hasTrigger);
}

/// Which body-panel feed a node consumes, or undefined if none.
/// Mutually exclusive: a node is infra OR trigger OR neither, never
/// both (current catalog enforces this; the helper picks infra
/// first so a future overlap stays deterministic).
export type NodeBodyFeedKind = 'infra' | 'signal';

export function nodeBodyFeedKind(
  node: RoleNodeShape & { requiresInfra?: boolean },
): NodeBodyFeedKind | undefined {
  if (nodeRequiresInfra(node)) return 'infra';
  if (nodeIsTrigger(node)) return 'signal';
  return undefined;
}
