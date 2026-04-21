import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Bug } from '@lucide/svelte';
import { isInputConnected } from '$lib/validation';

export const DebugNode: NodeTemplate = {
	type: 'Debug',
	label: 'Debug',
	description: 'Display incoming data for debugging. Connect any output to the data port to inspect its value after execution.',
	isBase: true,
	icon: Bug,
	color: '#b05574',
	category: 'Debug',
	tags: ['debug', 'inspect', 'log', 'test'],
	fields: [],
	defaultInputs: [
		{ name: 'data', portType: 'T', required: true, description: 'Any data to display' },
	],
	defaultOutputs: [],
	features: {
		showDebugPreview: true,
	},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];
		if (!isInputConnected('data', context)) {
			errors.push({ port: 'data', message: 'Data input is required', level: 'structural' });
		}
		return errors;
	},
};
