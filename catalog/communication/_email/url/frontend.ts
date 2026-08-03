import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Link } from '@lucide/svelte';
import { isInputConnected } from '$lib/validation';

export const emailUrlNode: NodeTemplate = {
	type: 'EmailUrl',
	label: 'Email URL',
	description: 'Generate a mailto: URL to compose an email. This node does NOT send the email itself; it produces a URL that must be opened via a Notify node to trigger the email client.',
	icon: Link,
	color: '#6366f1',
	category: 'Utility',
	tags: ['email', 'mailto', 'url', 'action'],
	fields: [],
	defaultInputs: [
		{ name: 'to', portType: 'String', required: true, description: 'Recipient email address' },
		{ name: 'subject', portType: 'String', required: true, description: 'Email subject line' },
		{ name: 'body', portType: 'String', required: true, description: 'Email body text' },
		{ name: 'cc', portType: 'String', required: false, description: 'CC recipients (comma-separated)' },
		{ name: 'bcc', portType: 'String', required: false, description: 'BCC recipients (comma-separated)' },
	],
	defaultOutputs: [
		{ name: 'url', portType: 'String', required: false, description: 'Generated mailto URL' },
	],
	features: {
	},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];

		if (!isInputConnected('to', context)) {
			errors.push({ port: 'to', message: 'Recipient email address is required', level: 'structural' });
		}
		if (!isInputConnected('subject', context)) {
			errors.push({ port: 'subject', message: 'Email subject is required', level: 'structural' });
		}
		if (!isInputConnected('body', context)) {
			errors.push({ port: 'body', message: 'Email body is required', level: 'structural' });
		}

		return errors;
	},
};
