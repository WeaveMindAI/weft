import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { MessageCircle } from '@lucide/svelte';
import { isInputConnected } from '$lib/validation';

export const smsUrlNode: NodeTemplate = {
	type: 'SmsUrl',
	label: 'SMS URL',
	description: 'Generate an sms: URL to send a text message. This node does NOT send the SMS itself; it produces a URL that must be opened via a Notify node to trigger the messaging app.',
	icon: MessageCircle,
	color: '#22c55e',
	category: 'Utility',
	tags: ['sms', 'text', 'message', 'url', 'action'],
	fields: [],
	defaultInputs: [
		{ name: 'phone', portType: 'String', required: true, description: 'Phone number (with country code)' },
		{ name: 'message', portType: 'String', required: true, description: 'Message text' },
	],
	defaultOutputs: [
		{ name: 'url', portType: 'String', required: false, description: 'Generated sms: URL' },
	],
	features: {
	},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];

		if (!isInputConnected('phone', context)) {
			errors.push({ port: 'phone', message: 'Phone number is required', level: 'structural' });
		}
		if (!isInputConnected('message', context)) {
			errors.push({ port: 'message', message: 'Message text is required', level: 'structural' });
		}

		return errors;
	},
};
