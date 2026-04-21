import type { NodeTemplate } from '$lib/types';
import { Group } from '@lucide/svelte';

export const GroupNode: NodeTemplate = {
	type: 'Group',
	label: 'Group',
	description: 'Functional group with interface ports. Inner nodes connect to the group boundary via bare names in Weft.',
	icon: Group,
	isBase: true,
	color: '#52525b',
	category: 'Utility',
	tags: ['structure', 'organize', 'container', 'folder', 'group', 'subgraph'],
	fields: [],
	defaultInputs: [],
	defaultOutputs: [],
	features: {
		canAddInputPorts: true,
		canAddOutputPorts: true,
	},
};
