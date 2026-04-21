import type { NodeTemplate } from '$lib/types';
import { Hash } from '@lucide/svelte';

export const NumberNode: NodeTemplate = {
	type: 'Number',
	label: 'Number',
	description: 'Numeric input value',
	isBase: true,
	icon: Hash,
	color: '#5a9eb8',
	category: 'Data',
	tags: ['data', 'numeric', 'input', 'constant', 'integer'],
	fields: [
		{ key: 'value', label: 'Number Value', type: 'text', placeholder: '0' },
	],
	defaultInputs: [],
	defaultOutputs: [
		{ name: 'value', portType: 'Number', required: false, description: 'Numeric value' },
	],
	features: {
	},
};
