// Single source of truth for "is this node infra-backed or a
// trigger". Every place that needs to know uses these helpers; do
// NOT inline the lookup elsewhere or the catalog/instance dual-
// path will drift again.
//
// Every flag has two possible sources, and ONE order of precedence:
// the parsed instance first (the compiler mirrors the catalog's
// metadata onto every node it parses), and the catalog template only
// when the instance says nothing (a node added in the editor and not
// yet parsed). The host reads the parse the same way before it starts
// a poller (`graphView.ts syncDisplayPollers`), so the two cannot
// disagree about which nodes get a display.
// SYNC: role precedence <-> extension-vscode/src/graphView.ts syncDisplayPollers

import { NODE_TYPE_CONFIG } from '../nodes';
import type { IncludedContents, PerInstance } from '../../../protocol';

/// Minimal NodeInstance shape this module reads. Avoids importing
/// the full type from `../types` so this file stays focused.
interface RoleNodeShape {
  nodeType: string;
  requiresInfra?: boolean;
  features?: { isTrigger?: boolean; liveEndpoint?: string } | undefined;
}

/// True iff the node is infra-backed (its container is what serves
/// the node's display, and its infra verbs apply).
export function nodeRequiresInfra(node: RoleNodeShape): boolean {
  return node.requiresInfra ?? !!NODE_TYPE_CONFIG[node.nodeType]?.requiresInfra;
}

/// True iff the node is a trigger (its listener kind is what serves
/// the node's display).
export function nodeIsTrigger(node: RoleNodeShape): boolean {
  return node.features?.isTrigger ?? !!NODE_TYPE_CONFIG[node.nodeType]?.features?.isTrigger;
}

/// May this node carry `@per_instance`? It must run a container of its
/// own, which each instance then gets. A value each instance has its own
/// of (a connection, a sheet) is a field written `@instance_filled` instead.
// SYNC: canBePerInstance <-> crates/weft-core/src/node.rs NodeMetadata::per_instance_eligible
export function canBePerInstance(node: RoleNodeShape): boolean {
  return nodeRequiresInfra(node);
}

/// The two PROJECT-level questions, which are not the same as the
/// per-node ones above. An opaque `@include` node is neither infra nor a
/// trigger (it gets no poller, no infra slot, no mount URL), but the file
/// behind it can hold both, and in the interface parse that body is not
/// in the graph at all. So "can this project be activated" and "does this
/// project need its infra up" have to count through includes, which is
/// what `includeContents` is for. Get this wrong and a project whose only
/// trigger sits in an included file shows no Activate button.
///
/// Only SHARED nodes count: a `@per_instance` node's copies each belong
/// to one instance, armed and started by the program for it, so the
/// action bar's Activate and Start infra have nothing to act on for it
/// (an included file's contents already leave such nodes out).
type ProjectRoleNode = RoleNodeShape & {
  requiresInfra?: boolean;
  perInstance?: PerInstance;
  includeContents?: IncludedContents | undefined;
};

/// True iff the project declares any shared infra, an included file's included.
// SYNC: shared roles <-> crates/weft-dispatcher/src/api/project.rs gather_action_snapshot (source_infra)
export function projectHasInfra(nodes: readonly ProjectRoleNode[]): boolean {
  return nodes.some((n) => (!n.perInstance && nodeRequiresInfra(n)) || !!n.includeContents?.requiresInfra);
}

/// True iff the project declares any shared trigger, an included file's included.
// SYNC: shared roles <-> crates/weft-dispatcher/src/api/project.rs gather_action_snapshot (shared_triggers)
export function projectHasTriggers(nodes: readonly ProjectRoleNode[]): boolean {
  return nodes.some((n) => (!n.perInstance && nodeIsTrigger(n)) || !!n.includeContents?.hasTrigger);
}

/// Does this node show a display on its body?
///
/// The same question the host asks before it starts a poller, so the
/// two cannot disagree about which nodes get one. A trigger always
/// does (the listener kind holding its signal serves it). An infra
/// node does only when its metadata NAMES the endpoint serving it: a
/// node that speaks only TCP has nothing to serve one on, and one that
/// answered true here with nothing ever polling it would sit forever
/// under an empty panel.
///
/// Both kinds answer in the same shape through the same channel, so
/// the caller needs the question answered, not which of the two it was.
export function nodeHasDisplay(node: RoleNodeShape): boolean {
  if (nodeRequiresInfra(node)) return nodeServesLive(node);
  return nodeIsTrigger(node);
}

/// Does this node's container serve `/live`, per its metadata?
function nodeServesLive(node: RoleNodeShape): boolean {
  return (node.features?.liveEndpoint ?? NODE_TYPE_CONFIG[node.nodeType]?.features?.liveEndpoint) != null;
}
