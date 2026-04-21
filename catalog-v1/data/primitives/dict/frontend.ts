import type { NodeTemplate } from '$lib/types';
import { Braces } from '@lucide/svelte';

export const DictNode: NodeTemplate = {
	type: 'Dict',
	label: 'Dict',
	description: 'JSON dictionary/object input',
	isBase: true,
	icon: Braces,
	color: '#7c6f9f',
	category: 'Data',
	tags: ['data', 'json', 'object', 'map', 'input'],
	fields: [
		{ key: 'value', label: 'JSON Object', type: 'textarea', placeholder: '{\n  "key": "value"\n}' },
	],
	defaultInputs: [],
	defaultOutputs: [
		{ name: 'value', portType: 'MustOverride', required: false, description: 'Dictionary value (type must be declared in Weft)' },
	],
	features: {
	},
};
