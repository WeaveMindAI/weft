// SYNC NOTE: Backend has equivalent logic in weft-core project.rs (extract_subgraph,
// extract_infra_subgraph, extract_trigger_setup_subgraph). This frontend copy is
// used for UI highlighting and divergence detection. Keep both in sync.
import type { NodeInstance, Edge } from '$lib/types';

export interface SubgraphResult {
	nodeIds: Set<string>;
	edges: Edge[];
	errors: string[];
}

export interface SubgraphOptions {
	/** Predicate to select the seed nodes (e.g. infra nodes, trigger nodes). */
	seedFilter: (node: NodeInstance) => boolean;
	/** Optional validator run on every node inside the extracted subgraph. Return an error string to reject, or null to accept. */
	validateNode?: (node: NodeInstance) => string | null;
}

/**
 * Generic subgraph extraction via backward BFS.
 *
 * Starting from every node that matches `seedFilter`, walks backwards along
 * incoming edges to collect all upstream dependencies. Optionally validates
 * each collected node via `validateNode`.
 */
export function extractSubgraph(
	nodes: NodeInstance[],
	edges: Edge[],
	options: SubgraphOptions,
): SubgraphResult {
	const errors: string[] = [];

	const seedIds = new Set(
		nodes.filter(options.seedFilter).map(n => n.id)
	);

	if (seedIds.size === 0) {
		return { nodeIds: new Set(), edges: [], errors };
	}

	// Build incoming-edges index: nodeId -> edges targeting it
	const incomingEdges = new Map<string, Edge[]>();
	for (const edge of edges) {
		if (!incomingEdges.has(edge.target)) {
			incomingEdges.set(edge.target, []);
		}
		incomingEdges.get(edge.target)!.push(edge);
	}

	// BFS backwards from seed nodes
	const requiredNodeIds = new Set<string>();
	const queue = [...seedIds];

	while (queue.length > 0) {
		const nodeId = queue.shift()!;
		if (requiredNodeIds.has(nodeId)) continue;
		requiredNodeIds.add(nodeId);

		for (const edge of incomingEdges.get(nodeId) ?? []) {
			queue.push(edge.source);
		}
	}

	// Validate nodes in the subgraph
	if (options.validateNode) {
		for (const node of nodes) {
			if (!requiredNodeIds.has(node.id)) continue;
			const err = options.validateNode(node);
			if (err) errors.push(err);
		}
	}

	// Collect edges within the subgraph
	const subEdges = edges.filter(
		e => requiredNodeIds.has(e.source) && requiredNodeIds.has(e.target)
	);

	return { nodeIds: requiredNodeIds, edges: subEdges, errors };
}
