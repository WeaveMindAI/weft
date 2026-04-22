import type { RunnerMode, Visibility, SetupItem } from '$lib/types';
import { NODE_TYPE_CONFIG } from '$lib/nodes';
import type { ProjectDefinition } from '$lib/types';

/**
 * Decide whether an element with a given visibility tag should render in the
 * current mode. Unset visibility defaults to 'both'. In visitor mode, admin
 * items are hidden; in admin mode, visitor-only items are hidden.
 */
export function visibleInMode(visibility: Visibility | undefined, mode: RunnerMode): boolean {
	const v = visibility ?? 'both';
	if (v === 'both') return true;
	return v === mode;
}

/**
 * Re-clamp visibility for a setup item based on the underlying field type.
 * Sensitive fields (password, api_key) are forced to 'admin' regardless of
 * what the DSL declared. Safety is not opt-in. Returns the effective visibility.
 */
export function effectiveItemVisibility(
	item: SetupItem,
	project: ProjectDefinition,
): Visibility {
	const node = project.nodes.find(n => n.id === item.nodeId);
	if (node) {
		const template = NODE_TYPE_CONFIG[node.nodeType];
		const field = template?.fields.find(f => f.key === item.fieldKey);
		if (field && (field.type === 'password' || field.type === 'api_key')) {
			return 'admin';
		}
	}
	return item.visibility ?? 'both';
}
