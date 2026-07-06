import type { NodeInstance, Edge } from '../types';
import { extractSubgraph, type SubgraphResult } from './subgraph';
import { nodeIsTrigger, nodeRequiresInfra } from './node-roles';

/**
 * Extract the infrastructure subgraph from a project.
 *
 * Walks backwards from every infrastructure node, collecting all upstream
 * dependencies. Returns the set of node IDs in the subgraph plus any
 * validation errors (e.g. triggers found in the subgraph).
 */
export function extractInfraSubgraph(
	nodes: NodeInstance[],
	edges: Edge[],
): SubgraphResult {
	return extractSubgraph(nodes, edges, {
		// NodeInstance carries no `requiresInfra` flag (it lives on the catalog
		// template), so nodeRequiresInfra resolves it from NODE_TYPE_CONFIG by
		// nodeType. Passing only nodeType is exact, not lossy.
		seedFilter: (n) => nodeRequiresInfra({ nodeType: n.nodeType }),
		validateNode: (n) => {
			if (nodeIsTrigger(n)) {
				return (
					`Trigger node "${n.label || n.id}" (${n.nodeType}) cannot be in the infrastructure subgraph. ` +
					`Infrastructure nodes and their dependencies must not include triggers.`
				);
			}
			return null;
		},
	});
}
