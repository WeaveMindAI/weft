import type { NodeTemplate } from '$lib/types';
import { List } from '@lucide/svelte';

export const ListNode: NodeTemplate = {
	type: 'List',
	label: 'List',
	description: 'Array/list input',
	isBase: true,
	icon: List,
	color: '#5a8a8a',
	category: 'Data',
	tags: ['data', 'array', 'collection', 'input'],
	fields: [
		{ key: 'value', label: 'JSON Array', type: 'textarea', placeholder: '[\n  "item1",\n  "item2"\n]' },
	],
	defaultInputs: [],
	defaultOutputs: [
		{ name: 'value', portType: 'MustOverride', required: false, description: 'List value (type must be declared in Weft)' },
	],
	features: {
	},
};
