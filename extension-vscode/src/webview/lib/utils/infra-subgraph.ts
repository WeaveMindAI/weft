import type { NodeInstance, Edge } from '$lib/types';
import { NODE_TYPE_CONFIG } from '$lib/nodes';
import { extractSubgraph, type SubgraphResult } from './subgraph';

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
		seedFilter: (n) => !!n.features?.isInfrastructure,
		validateNode: (n) => {
			const isTrigger = n.features?.isTrigger
				|| NODE_TYPE_CONFIG[n.nodeType]?.features?.isTrigger;
			if (isTrigger) {
				return (
					`Trigger node "${n.label || n.id}" (${n.nodeType}) cannot be in the infrastructure subgraph. ` +
					`Infrastructure nodes and their dependencies must not include triggers.`
				);
			}
			return null;
		},
	});
}
