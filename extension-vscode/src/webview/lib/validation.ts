/**
 * Project Validation Utilities
 *
 * Validates all nodes in a project before execution or activation.
 * Each node can define its own validate function in its NodeTemplate.
 */

import { NODE_TYPE_CONFIG } from '$lib/nodes';
import { isPortConfigurable } from '$lib/types';
import type {
	NodeInstance,
	Edge,
	ValidationContext,
	ValidationError,
	ValidationLevel,
	ProjectValidationResult
} from '$lib/types';


/**
 * Get the set of connected input port names for a node.
 */
function getConnectedInputs(nodeId: string, edges: Edge[]): Set<string> {
	const connected = new Set<string>();
	for (const edge of edges) {
		if (edge.target === nodeId && edge.targetHandle) {
			connected.add(edge.targetHandle);
		}
	}
	return connected;
}

/**
 * Validate a single node.
 */
export function validateNode(
	node: NodeInstance,
	allNodes: NodeInstance[],
	allEdges: Edge[]
): ValidationError[] {
	const template = NODE_TYPE_CONFIG[node.nodeType];
	if (!template) {
		return [{ message: `Unknown node type: ${node.nodeType}`, level: 'structural' }];
	}

	if (!template.validate) {
		return [];
	}

	const context: ValidationContext = {
		config: node.config,
		connectedInputs: getConnectedInputs(node.id, allEdges),
		allNodes,
		allEdges,
		nodeId: node.id,
	};

	return template.validate(context);
}

/**
 * Validate all nodes in a project (all levels).
 * Returns a map of nodeId -> errors for nodes that have validation errors.
 */
export function validateProject(
	nodes: NodeInstance[],
	edges: Edge[]
): ProjectValidationResult {
	const nodeErrors = new Map<string, ValidationError[]>();
	let valid = true;

	for (const node of nodes) {
		const errors = validateNode(node, nodes, edges);
		if (errors.length > 0) {
			nodeErrors.set(node.id, errors);
			valid = false;
		}
	}

	return { valid, nodeErrors };
}

/**
 * Validate only at a specific level.
 * 'structural': wiring and required config for design (used by AI builder)
 * 'runtime': credentials and data needed to execute (used before launching)
 */
export function validateProjectAtLevel(
	nodes: NodeInstance[],
	edges: Edge[],
	level: ValidationLevel
): ProjectValidationResult {
	const nodeErrors = new Map<string, ValidationError[]>();
	let valid = true;

	for (const node of nodes) {
		const allErrors = validateNode(node, nodes, edges);
		const filtered = allErrors.filter(e => e.level === level);
		if (filtered.length > 0) {
			nodeErrors.set(node.id, filtered);
			valid = false;
		}
	}

	return { valid, nodeErrors };
}

/**
 * Helper to check if a required input port is satisfied, either by being
 * wired to an edge, or by being filled from a same-named config field on the
 * node (for configurable ports). Media-type ports are wired-only by default;
 * primitives and dicts are configurable by default.
 *
 * Edge wins over config when both are present at runtime; for this validation
 * check, either is sufficient.
 */
export function isInputConnected(
	portName: string,
	context: ValidationContext
): boolean {
	if (context.connectedInputs.has(portName)) return true;

	// Check if the port is configurable and has a non-null config value.
	const node = context.allNodes.find(n => n.id === context.nodeId);
	if (!node) return false;
	const template = NODE_TYPE_CONFIG[node.nodeType];
	if (!template) return false;

	const port = template.defaultInputs?.find(p => p.name === portName);
	if (!port) return false;
	if (!isPortConfigurable(port)) return false;

	const configValue = context.config?.[portName];
	return configValue !== undefined && configValue !== null && configValue !== '';
}

/**
 * Helper to get the node type connected to a specific input port.
 * Traces through Group nodes to find the actual originating node type.
 * Returns null if not connected or if the source node doesn't exist.
 */
export function getConnectedNodeType(
	portName: string,
	context: ValidationContext
): string | null {
	const edge = context.allEdges.find(
		e => e.target === context.nodeId && e.targetHandle === portName
	);
	if (!edge) return null;

	return resolveSourceNodeType(edge.source, edge.sourceHandle, context.allNodes, context.allEdges, new Set());
}

/**
 * Recursively resolve the actual node type behind a source, tracing through
 * Group pass-through nodes.
 */
function resolveSourceNodeType(
	sourceId: string,
	sourceHandle: string | null,
	allNodes: NodeInstance[],
	allEdges: Edge[],
	visited: Set<string>,
): string | null {
	if (visited.has(sourceId)) return null;
	visited.add(sourceId);

	const sourceNode = allNodes.find(n => n.id === sourceId);
	if (!sourceNode) return null;

	if (sourceNode.nodeType === 'Group') {
		// The edge comes from a Group node. The sourceHandle may have an __inner
		// suffix (raw parser edges) or may already be stripped (editor snapshot).
		// Strip __inner to get the canonical port name, then find the edge that
		// feeds this port. It could be:
		//   - an external edge with targetHandle === portName (input forwarding inward)
		//   - an internal edge with targetHandle === portName__inner (output receiving from inside)
		const portName = sourceHandle?.endsWith('__inner') ? sourceHandle.slice(0, -7) : sourceHandle;
		const upstreamEdge = allEdges.find(
			e => e.target === sourceId
				&& (e.targetHandle === portName || e.targetHandle === `${portName}__inner`)
		);
		if (!upstreamEdge) return null;
		return resolveSourceNodeType(upstreamEdge.source, upstreamEdge.sourceHandle, allNodes, allEdges, visited);
	}

	return sourceNode.nodeType;
}

/**
 * Helper to check if an api_key field is ready to use.
 * The api_key field type has two modes:
 *   - Credits mode: value is "" (empty),ses platform tokens, no user key needed
 *   - BYOK mode: value is "__BYOK__" (selected but not entered) or an actual key string
 * Returns false only when the user switched to BYOK but hasn't entered a key yet.
 */
export function isApiKeyReady(key: string, config: Record<string, unknown>): boolean {
	const value = config[key];
	if (value === '__BYOK__') return false;
	return true;
}

/**
 * Helper to check if a config field has a non-empty value.
 */
export function hasConfigValue(key: string, config: Record<string, unknown>): boolean {
	const value = config[key];
	if (value === undefined || value === null) return false;
	if (typeof value === 'string') return value.trim().length > 0;
	if (Array.isArray(value)) return value.length > 0;
	return true;
}
