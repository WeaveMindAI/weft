import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { GitBranch } from '@lucide/svelte';
import { isInputConnected } from '$lib/validation';

export const GateNode: NodeTemplate = {
	type: 'Gate',
	label: 'Gate',
	description: 'Forwards value when pass is true, outputs null when pass is null or false (cuts downstream flow via null propagation).',
	isBase: true,
	icon: GitBranch,
	color: '#6366f1',
	category: 'Flow',
	tags: ['flow', 'gate', 'route', 'conditional', 'pass'],
	fields: [],
	defaultInputs: [
		{ name: 'pass', portType: 'Boolean', required: true, description: 'true to forward value, null or false to cut flow' },
		{ name: 'value', portType: 'T', required: true, description: 'Value to forward when pass is true' },
	],
	defaultOutputs: [
		{ name: 'value', portType: 'T', required: false, description: 'The forwarded value, or null if pass was null or false' },
	],
	features: {},
	setupGuide: [
		'In parallel lanes, use Gate to filter items before Collect (rejected items become nulls in the gathered list)',
	],
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];
		if (!isInputConnected('pass', context)) {
			errors.push({ port: 'pass', message: 'Pass condition is required', level: 'structural' });
		}
		if (!isInputConnected('value', context)) {
			errors.push({ port: 'value', message: 'Value input is required', level: 'structural' });
		}
		return errors;
	},
};
