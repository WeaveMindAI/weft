import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Link } from '@lucide/svelte';
import { isInputConnected } from '$lib/validation';

export const notifyNode: NodeTemplate = {
	type: 'Notify',
	label: 'Send URL',
	description: 'Send a URL to the extension for the user to open',
	icon: Link,
	color: '#f59e0b',
	category: 'Flow',
	tags: ['notify', 'url', 'extension', 'link', 'send'],
	fields: [],
	defaultInputs: [
		{ name: 'url', portType: 'String', required: true, description: 'URL to send (mailto, whatsapp, https, etc.)' },
	],
	defaultOutputs: [
		{ name: 'sent', portType: 'Boolean', required: false, description: 'Whether URL was sent' },
	],
	features: {
	},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];

		if (!isInputConnected('url', context)) {
			errors.push({ port: 'url', message: 'URL input is required', level: 'structural' });
		}

		return errors;
	},
};
