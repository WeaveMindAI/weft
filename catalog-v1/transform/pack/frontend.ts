import type { NodeTemplate, PortDefinition } from '$lib/types';
import { Package } from '@lucide/svelte';

export const PackNode: NodeTemplate = {
	type: 'Pack',
	label: 'Pack',
	description: 'Combines multiple inputs into a single Dict. Add input ports for each value to include.',
	icon: Package,
	isBase: true,
	color: '#6366f1',
	category: 'Utility',
	tags: ['pack', 'combine', 'bundle', 'dict', 'object'],
	fields: [],
	defaultInputs: [],
	defaultOutputs: [
		{ name: 'out', portType: 'Dict[String, T]', required: false, description: 'Combined Dict of all inputs' },
	],
	features: {
		canAddInputPorts: true,
	},
	resolveTypes: (inputs: PortDefinition[]) => {
		if (inputs.length === 0) return {};
		const valueTypes = [...new Set(inputs.map(p => p.portType))];
		const valueType = valueTypes.length === 1 ? valueTypes[0] : valueTypes.join(' | ');
		return { outputs: { out: `Dict[String, ${valueType}]` } };
	},
};
