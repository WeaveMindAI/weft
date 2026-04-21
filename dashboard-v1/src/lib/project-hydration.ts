import type { ProjectDefinition } from '$lib/types';
import { parseWeft } from '$lib/ai/weft-parser';
import { parseLoom } from '$lib/ai/loom-parser';
import { parseLayoutCode } from '$lib/ai/weft-editor';

/**
 * Hydrate a project from its stored code representation.
 * Parses weftCode → nodes/edges, loomCode → setupManifest, layoutCode → positions.
 */
export function hydrateProject(raw: {
	id: string;
	name: string;
	description: string | null;
	weftCode?: string | null;
	loomCode?: string | null;
	layoutCode?: string | null;
	createdAt: string;
	updatedAt: string;
	isDeployment?: boolean;
	originProjectId?: string | null;
}): ProjectDefinition {
	let nodes: ProjectDefinition['nodes'] = [];
	let edges: ProjectDefinition['edges'] = [];
	let setupManifest: ProjectDefinition['setupManifest'] = undefined;

	if (raw.weftCode) {
		const result = parseWeft('````weft\n' + raw.weftCode + '\n````');
		if (result.projects.length > 0) {
			const w = result.projects[0].project;
			// The parser must always produce arrays for nodes/edges.
			// If it ever returns something else, we want the store
			// update to throw loudly here rather than poison
			// `project.nodes` with a non-iterable that crashes
			// somewhere deeper (validateProject, runProject, ELK
			// layout). No silent fallbacks.
			nodes = w.nodes;
			edges = w.edges;
		}
	}

	// Apply positions from layoutCode to parsed nodes
	if (raw.layoutCode) {
		const layoutMap = parseLayoutCode(raw.layoutCode);
		for (const n of nodes) {
			const entry = layoutMap[n.id];
			if (entry) {
				n.position = { x: entry.x, y: entry.y };
				if (entry.w !== undefined) (n.config as Record<string, unknown>).width = entry.w;
				if (entry.h !== undefined) (n.config as Record<string, unknown>).height = entry.h;
				if (entry.expanded !== undefined) (n.config as Record<string, unknown>).expanded = entry.expanded;
			}
		}
	}

	if (raw.loomCode) {
		const result = parseLoom('````loom\n' + raw.loomCode + '\n````');
		if (result.manifest) {
			setupManifest = result.manifest;
		}
	}

	return {
		id: raw.id,
		name: raw.name,
		description: raw.description,
		weftCode: raw.weftCode,
		loomCode: raw.loomCode,
		layoutCode: raw.layoutCode,
		nodes,
		edges,
		setupManifest,
		createdAt: raw.createdAt,
		updatedAt: raw.updatedAt,
		isDeployment: raw.isDeployment ?? false,
		originProjectId: raw.originProjectId ?? null,
	};
}
