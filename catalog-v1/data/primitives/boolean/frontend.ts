import type { NodeTemplate } from '$lib/types';
import { ToggleLeft } from '@lucide/svelte';

export const BooleanNode: NodeTemplate = {
	type: 'Boolean',
	label: 'Boolean',
	description: 'True/false value',
	isBase: true,
	icon: ToggleLeft,
	color: '#8b5cf6',
	category: 'Data',
	tags: ['data', 'boolean', 'flag', 'toggle', 'true', 'false', 'input', 'constant'],
	fields: [
		{ key: 'value', label: 'Value', type: 'checkbox', description: 'True or false' },
	],
	defaultInputs: [],
	defaultOutputs: [
		{ name: 'value', portType: 'Boolean', required: false, description: 'Boolean value' },
	],
	features: {
	},
};
