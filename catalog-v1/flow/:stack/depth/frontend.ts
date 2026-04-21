import type { NodeTemplate } from '$lib/types';
import { Layers } from '@lucide/svelte';

export const StackDepthNode: NodeTemplate = {
	type: 'StackDepth',
	label: 'Stack Depth',
	description: 'Outputs the current lane/stack depth as a number. Inside a ForEach with 5 items, outputs 5. Outside any parallel context, outputs 1.',
	icon: Layers,
	color: '#0ea5e9',
	category: 'Flow',
	tags: ['flow', 'stack', 'depth', 'lanes', 'parallel', 'count'],
	fields: [],
	defaultInputs: [
		{ name: 'value', portType: 'List[T]', required: true, description: 'Any value from the stack to measure', laneMode: 'Gather' },
	],
	defaultOutputs: [
		{ name: 'depth', portType: 'Number', required: false, description: 'Number of lanes in the stack (1 if not in a parallel context)' },
	],
	features: {
	},
	setupGuide: [
		'Connect any port from a parallel branch to the input',
		'Outputs the number of parallel lanes (items in the ForEach list)',
		'Useful for conditional logic based on batch size',
	],
};
