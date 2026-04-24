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
		// v2 renamed the metadata flag from `isInfrastructure` to
		// `requires_infra` (mirrored onto NodeDefinition as
		// `requiresInfra` by the compiler's enrich pass). Check
		// both on the node instance and on the catalog entry so
		// projects parsed before the rename still work.
		seedFilter: (n) => {
			const catalog = NODE_TYPE_CONFIG[n.nodeType];
			const catalogFlag = !!(
				(catalog as { requires_infra?: boolean } | undefined)?.requires_infra
				?? (catalog?.features as { requiresInfra?: boolean; isInfrastructure?: boolean } | undefined)?.requiresInfra
				?? (catalog?.features as { isInfrastructure?: boolean } | undefined)?.isInfrastructure
			);
			const nodeFlag = !!(
				(n as unknown as { requiresInfra?: boolean }).requiresInfra
				?? (n.features as { requiresInfra?: boolean; isInfrastructure?: boolean } | undefined)?.requiresInfra
				?? (n.features as { isInfrastructure?: boolean } | undefined)?.isInfrastructure
			);
			return nodeFlag || catalogFlag;
		},
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
