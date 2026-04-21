import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Globe } from '@lucide/svelte';
import { hasConfigValue } from '$lib/validation';

export const HttpNode: NodeTemplate = {
	type: 'Http',
	label: 'HTTP',
	description: 'Make an HTTP request',
	icon: Globe,
	color: '#5a8ab4',
	category: 'Utility',
	tags: ['api', 'request', 'fetch', 'web', 'integration'],
	fields: [
		{ key: 'url', label: 'URL', type: 'text', placeholder: 'https://api.example.com' },
		{ key: 'method', label: 'Method', type: 'select', options: ['GET', 'POST', 'PUT', 'DELETE'] },
	],
	defaultInputs: [
		{ name: 'body', portType: 'JsonDict', required: false, description: 'Request body' },
		{ name: 'headers', portType: 'Dict[String, String]', required: false, description: 'Request headers' },
	],
	defaultOutputs: [
		{ name: 'body', portType: 'String', required: false, description: 'HTTP response body' },
		{ name: 'status', portType: 'Number', required: false, description: 'HTTP status code' },
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the request succeeded (2xx status)' },
	],
	features: {
		canAddInputPorts: false,
		canAddOutputPorts: false,
	},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];
		
		if (!hasConfigValue('url', context.config)) {
			errors.push({ field: 'url', message: 'URL is required', level: 'structural' });
		}
		
		return errors;
	},
};
