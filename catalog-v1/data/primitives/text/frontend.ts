import type { NodeTemplate } from '$lib/types';
import { Type } from '@lucide/svelte';

export const TextNode: NodeTemplate = {
	type: 'Text',
	label: 'Text',
	description: 'Text input value',
	isBase: true,
	icon: Type,
	color: '#6b7280',
	category: 'Data',
	tags: ['data', 'string', 'input', 'constant'],
	fields: [
		{ key: 'value', label: 'Text Value', type: 'textarea', placeholder: 'Enter text here...' },
	],
	defaultInputs: [],
	defaultOutputs: [
		{ name: 'value', portType: 'String', required: false, description: 'Text value' },
	],
	features: {
	},
};
