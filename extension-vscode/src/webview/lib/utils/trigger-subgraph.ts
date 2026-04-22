import type { NodeInstance, Edge } from '$lib/types';
import { NODE_TYPE_CONFIG } from '$lib/nodes';
import { extractSubgraph, type SubgraphResult } from './subgraph';

/**
 * Extract the trigger setup subgraph from a project.
 *
 * Walks backwards from every trigger node, collecting all upstream
 * dependencies (including the trigger node itself). Returns the set
 * of node IDs in the subgraph plus any validation errors.
 *
 * Infrastructure nodes upstream of triggers are allowed (e.g. WhatsApp
 * bridge feeding a WhatsApp trigger).
 */
export function extractTriggerSubgraph(
	nodes: NodeInstance[],
	edges: Edge[],
): SubgraphResult {
	return extractSubgraph(nodes, edges, {
		seedFilter: (n) =>
			!!n.features?.isTrigger
			|| !!NODE_TYPE_CONFIG[n.nodeType]?.features?.isTrigger,
	});
}
