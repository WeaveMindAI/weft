/**
 * Node Registry - Auto-discovers all node definitions
 * 
 * This module uses Vite's glob import to automatically discover all node
 * definition files in this folder. To add a new node, simply create a new
 * .ts file that exports a NodeTemplate object - it will be auto-discovered.
 */

import type { NodeTemplate } from '$lib/types';

export type { NodeTemplate, NodeCategory } from '$lib/types';

// Auto-discover all node modules using Vite's glob import
const nodeModules = import.meta.glob<{ [key: string]: NodeTemplate }>(['./*.ts', '!./index.ts'], { eager: true });

// Collect all exported NodeTemplate objects
const allNodes: NodeTemplate[] = [];

for (const path in nodeModules) {
	const module = nodeModules[path];
	for (const exportName in module) {
		const exported = module[exportName];
		if (exported && typeof exported === 'object' && 'type' in exported && 'label' in exported) {
			allNodes.push(exported as NodeTemplate);
		}
	}
}

// Map of node type -> node template for quick lookup
export const NODE_TYPE_CONFIG: Record<string, NodeTemplate> = Object.fromEntries(
	allNodes.map(node => [node.type, node])
);

// All discovered nodes
export const ALL_NODES = allNodes;

// All node type strings
export const ALL_NODE_TYPES = allNodes.map(n => n.type);

// Union type of all node types
export type NodeType = typeof ALL_NODE_TYPES[number];
