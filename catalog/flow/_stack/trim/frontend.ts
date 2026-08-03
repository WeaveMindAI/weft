import type { NodeTemplate } from '$lib/types';
import { Scissors } from '@lucide/svelte';

export const StackTrimNode: NodeTemplate = {
	type: 'StackTrim',
	label: 'Stack Trim',
	description: 'Trims a parallel stack to a specified number of lanes. Keeps the first N lanes and drops the rest. Useful for limiting parallelism or taking a subset of results.',
	icon: Scissors,
	color: '#0ea5e9',
	category: 'Flow',
	tags: ['flow', 'stack', 'trim', 'lanes', 'parallel', 'limit', 'slice'],
	fields: [],
	defaultInputs: [
		{ name: 'value', portType: 'List[T]', required: true, description: 'Values from the parallel stack to trim', laneMode: 'Gather' },
		{ name: 'count', portType: 'Number', required: true, description: 'Number of lanes to keep (from the start)' },
	],
	defaultOutputs: [
		{ name: 'value', portType: 'T', required: false, description: 'Trimmed values (one per remaining lane)', laneMode: 'Expand' },
	],
	features: {
	},
	setupGuide: [
		'Connect a parallel branch to the value input',
		'Provide the number of lanes to keep via the count input',
		'Downstream nodes will run with the trimmed number of lanes',
		'Example: ForEach over 10 items, StackTrim with count=3, downstream runs 3 times',
	],
};
