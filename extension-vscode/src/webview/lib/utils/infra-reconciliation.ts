/**
 * Infrastructure Reconciliation
 *
 * Compares the frontend project graph (desired state) against the backend
 * state (actual state) for infrastructure nodes, producing reconciliation plans.
 *
 * Trigger staleness is handled separately via project hash comparison
 * (server-side hash stored on trigger activation, compared against current hash).
 *
 * Follows a Terraform-style model:
 * - Unchanged items (same ID, same type) → keep running
 * - New items in frontend → provision on next start
 * - Removed items (in backend but not frontend) → terminate
 * - Changed items (same ID, different config) → restart
 */

// ── Infrastructure ──

export interface InfraNodeState {
	nodeId: string;
	nodeType: string;
	instanceId: string;
	status: string;
	backend?: string;
}

export interface FrontendInfraNode {
	id: string;
	nodeType: string;
	config: Record<string, unknown>;
}

// ── Triggers ──

export interface BackendTrigger {
	triggerId: string;
	nodeType: string;
	projectId: string;
	status: string;
	projectHash?: string;
}

// ── Shared ──

export type ReconciliationAction = 'keep' | 'provision' | 'terminate' | 'restart';

export interface ReconciliationEntry {
	nodeId: string;
	nodeType: string;
	action: ReconciliationAction;
	instanceId?: string;
	reason: string;
}

export interface ReconciliationPlan {
	entries: ReconciliationEntry[];
	hasChanges: boolean;
	summary: {
		keep: number;
		provision: number;
		terminate: number;
		restart: number;
	};
}

// ── Infrastructure reconciliation ──

export function buildInfraReconciliationPlan(
	frontendNodes: FrontendInfraNode[],
	backendNodes: InfraNodeState[],
): ReconciliationPlan {
	const entries: ReconciliationEntry[] = [];

	const backendByNodeId = new Map(backendNodes.map(n => [n.nodeId, n]));
	const frontendByNodeId = new Map(frontendNodes.map(n => [n.id, n]));

	for (const backend of backendNodes) {
		const frontend = frontendByNodeId.get(backend.nodeId);

		if (!frontend) {
			entries.push({
				nodeId: backend.nodeId,
				nodeType: backend.nodeType,
				action: 'terminate',
				instanceId: backend.instanceId,
				reason: 'Node removed from project',
			});
		} else if (frontend.nodeType !== backend.nodeType) {
			entries.push({
				nodeId: backend.nodeId,
				nodeType: backend.nodeType,
				action: 'restart',
				instanceId: backend.instanceId,
				reason: `Node type changed from ${backend.nodeType} to ${frontend.nodeType}`,
			});
		} else {
			entries.push({
				nodeId: backend.nodeId,
				nodeType: backend.nodeType,
				action: 'keep',
				instanceId: backend.instanceId,
				reason: 'No changes',
			});
		}
	}

	for (const frontend of frontendNodes) {
		if (!backendByNodeId.has(frontend.id)) {
			entries.push({
				nodeId: frontend.id,
				nodeType: frontend.nodeType,
				action: 'provision',
				reason: 'New node added to project',
			});
		}
	}

	return buildPlanFromEntries(entries);
}

// ── Divergence checks ──

export function hasInfraDivergence(
	frontendNodes: FrontendInfraNode[],
	backendNodes: InfraNodeState[],
): boolean {
	const backendIds = new Set(backendNodes.map(n => n.nodeId));
	const frontendIds = new Set(frontendNodes.map(n => n.id));

	for (const id of backendIds) {
		if (!frontendIds.has(id)) return true;
	}
	for (const id of frontendIds) {
		if (!backendIds.has(id)) return true;
	}

	const backendByNodeId = new Map(backendNodes.map(n => [n.nodeId, n]));
	for (const frontend of frontendNodes) {
		const backend = backendByNodeId.get(frontend.id);
		if (backend && frontend.nodeType !== backend.nodeType) return true;
	}

	return false;
}

// ── Helpers ──

function buildPlanFromEntries(entries: ReconciliationEntry[]): ReconciliationPlan {
	const summary = {
		keep: entries.filter(e => e.action === 'keep').length,
		provision: entries.filter(e => e.action === 'provision').length,
		terminate: entries.filter(e => e.action === 'terminate').length,
		restart: entries.filter(e => e.action === 'restart').length,
	};

	return {
		entries,
		hasChanges: summary.provision > 0 || summary.terminate > 0 || summary.restart > 0,
		summary,
	};
}
