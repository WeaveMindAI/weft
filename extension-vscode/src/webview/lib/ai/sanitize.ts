/**
 * Strip sensitive field values from a weft source string.
 *
 * Used at publish / export / share time to remove credentials, API keys,
 * and passwords before the code leaves the deployer's machine. Single
 * source of truth: export, community share, and the publish flow all
 * call this function so adding a new sensitive field type takes one
 * change here instead of three sites in parallel.
 *
 * The rule set: any field whose node-catalog `FieldDefinition.type`
 * matches one of `SENSITIVE_FIELD_TYPES`. Today that's `password` and
 * `api_key`; future secret-bearing field types should be added to the
 * set and nowhere else.
 *
 * The text-level edits go through the same `updateNodeConfig` helper
 * the editor uses, so the resulting weft stays syntactically valid
 * and exactly one line per removed field gets cleaned up.
 */

import type { ProjectDefinition } from '$lib/types';
import { NODE_TYPE_CONFIG } from '$lib/nodes';
import { updateNodeConfig } from '$lib/ai/weft-editor';

/** Field types whose concrete values are considered sensitive and must
 *  never leave the deployer's browser in exports, shares, or publishes.
 *  Extend here if a new field type needs to join the list. */
export const SENSITIVE_FIELD_TYPES: ReadonlySet<string> = new Set(['password', 'api_key']);

/** Count how many sensitive values are currently populated on the
 *  project's nodes. Useful for showing the user how many secrets will
 *  be stripped from an export. Counts only non-empty values so a blank
 *  api_key field doesn't inflate the number. */
export function countSensitiveValues(nodes: ProjectDefinition['nodes']): number {
	let count = 0;
	for (const node of nodes) {
		const template = NODE_TYPE_CONFIG[node.nodeType];
		if (!template) continue;
		for (const field of template.fields) {
			if (
				SENSITIVE_FIELD_TYPES.has(field.type) &&
				node.config?.[field.key] != null &&
				node.config[field.key] !== ''
			) {
				count++;
			}
		}
	}
	return count;
}

/** Return a copy of `weftCode` with every sensitive field cleared on
 *  every node. Uses the node catalog to identify which (nodeId, fieldKey)
 *  pairs are sensitive, then calls `updateNodeConfig(weft, nodeId, key,
 *  undefined)` to delete each value in the source. */
export function stripSensitiveFields(
	weftCode: string,
	nodes: ProjectDefinition['nodes'],
): string {
	let stripped = weftCode;
	for (const node of nodes) {
		const template = NODE_TYPE_CONFIG[node.nodeType];
		if (!template) continue;
		for (const field of template.fields) {
			if (!SENSITIVE_FIELD_TYPES.has(field.type)) continue;
			if (node.config?.[field.key] === undefined) continue;
			stripped = updateNodeConfig(stripped, node.id, field.key, undefined);
		}
	}
	return stripped;
}

/**
 * Visitor access allowlist. Mirrors the server-side shape consumed by
 * weft-api's publish_execute and cloud-api's latest_trigger_run: a pair
 * of maps naming which config fields a visitor can write and which
 * output ports the visitor can read. Computed once at publish time
 * from the loom's setup manifest and persisted on the deployment row.
 */
export interface VisitorAccessAllowlist {
	inputs: Record<string, string[]>;
	outputs: Record<string, string[]>;
}

/**
 * Derive the visitor access allowlist from a SetupManifest. Walks every
 * item in every phase (plus top-level blocks when present) and every
 * output declaration, and groups the visitor-visible `(nodeId, fieldKey)`
 * pairs per node. Items with `visibility: 'admin'` are excluded — the
 * deployer marked them admin-only, so they must not be writable by
 * anonymous visitors even if the visitor somehow submits them.
 *
 * Default visibility is `'both'` so an unset visibility still counts as
 * visitor-writable. That matches the runner's rendering behavior.
 */
export function computeVisitorAccess(
	manifest: ProjectDefinition['setupManifest'] | undefined,
): VisitorAccessAllowlist {
	const inputs: Record<string, Set<string>> = {};
	const outputs: Record<string, Set<string>> = {};
	if (!manifest) {
		return { inputs: {}, outputs: {} };
	}

	const addInput = (nodeId: string, fieldKey: string) => {
		(inputs[nodeId] ??= new Set()).add(fieldKey);
	};
	const addOutput = (nodeId: string, portName: string) => {
		(outputs[nodeId] ??= new Set()).add(portName);
	};

	const isVisitorVisible = (visibility: string | undefined) =>
		visibility !== 'admin'; // undefined, 'visitor', or 'both' all count

	// Walk the block tree exclusively. The parser always populates
	// `blocks` for any manifest it produces, and `phases`/`outputs`
	// on the manifest are flat projections of the same items the
	// block walker would visit. Walking both would double-work (and
	// previously risked duplicating entries in the allowlist when
	// the two projections diverged).
	function walkBlocks(blocks: NonNullable<ProjectDefinition['setupManifest']>['blocks']) {
		if (!blocks) return;
		for (const block of blocks) {
			switch (block.kind) {
				case 'phase':
					if (!isVisitorVisible(block.phase.visibility)) break;
					for (const item of block.phase.items ?? []) {
						if (isVisitorVisible(item.visibility)) {
							addInput(item.nodeId, item.fieldKey);
						}
					}
					if (block.phase.children) walkBlocks(block.phase.children);
					break;
				case 'output':
					if (isVisitorVisible(block.output.visibility)) {
						addOutput(block.output.nodeId, block.output.portName);
					}
					break;
				default:
					break;
			}
		}
	}

	if (manifest.blocks) {
		walkBlocks(manifest.blocks);
	} else {
		// Legacy fallback: a manifest constructed without a block
		// list (old test fixtures, programmatic callers). Walk the
		// flat indexes directly.
		for (const phase of manifest.phases ?? []) {
			if (!isVisitorVisible(phase.visibility)) continue;
			for (const item of phase.items ?? []) {
				if (!isVisitorVisible(item.visibility)) continue;
				addInput(item.nodeId, item.fieldKey);
			}
		}
		for (const output of manifest.outputs ?? []) {
			if (!isVisitorVisible(output.visibility)) continue;
			addOutput(output.nodeId, output.portName);
		}
	}

	// Materialize sets → arrays for JSON serialization.
	const materialize = (m: Record<string, Set<string>>): Record<string, string[]> => {
		const out: Record<string, string[]> = {};
		for (const [k, v] of Object.entries(m)) out[k] = [...v];
		return out;
	};
	return { inputs: materialize(inputs), outputs: materialize(outputs) };
}
