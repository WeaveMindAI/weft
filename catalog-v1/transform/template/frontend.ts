import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { FileText } from '@lucide/svelte';
import { isInputConnected } from '$lib/validation';

export const templateNode: NodeTemplate = {
	type: 'Template',
	label: 'Template',
	description: 'String interpolation with {{variable}} syntax. Outputs the template with all placeholders replaced by input values.',
	icon: FileText,
	isBase: true,
	color: '#6366f1',
	category: 'Utility',
	tags: ['template', 'string', 'format', 'interpolate', 'text'],
	fields: [],
	defaultInputs: [
		{ name: 'template', portType: 'String', required: true, description: 'Template string with {{variable}} placeholders' },
	],
	defaultOutputs: [
		{ name: 'text', portType: 'String', required: false, description: 'Interpolated text output' },
	],
	features: {
		canAddInputPorts: true,
	},
	setupGuide: [
		'Write the template directly in the "template" config field, or wire a String source to the template port',
		'Add a custom input port (via in:) for each {{variable}} in the template',
	],
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];

		if (!isInputConnected('template', context)) {
			errors.push({ port: 'template', message: 'Template input is required', level: 'structural' });
		}

		return errors;
	},
};
