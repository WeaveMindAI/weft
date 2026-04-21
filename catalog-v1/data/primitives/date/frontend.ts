import type { NodeTemplate } from '$lib/types';
import { CalendarDays } from '@lucide/svelte';

export const DateNode: NodeTemplate = {
	type: 'Date',
	label: 'Date',
	description: 'Date/time input value',
	icon: CalendarDays,
	isBase: true,
	color: '#8a7a6b',
	category: 'Data',
	tags: ['data', 'time', 'datetime', 'input', 'constant'],
	fields: [
		{ key: 'value', label: 'Date Value', type: 'text', placeholder: '2024-01-01' },
		{ key: 'format', label: 'Format', type: 'select', options: ['ISO', 'Unix', 'Custom'] },
	],
	defaultInputs: [],
	defaultOutputs: [
		{ name: 'value', portType: 'String', required: false, description: 'Date value' },
	],
	features: {
	},
};
