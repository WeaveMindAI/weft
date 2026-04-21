import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Smartphone } from '@lucide/svelte';
import { isInputConnected } from '$lib/validation';

export const whatsappUrlNode: NodeTemplate = {
	type: 'WhatsAppUrl',
	label: 'WhatsApp URL',
	description: 'Generate a WhatsApp URL to send a message. This node does NOT send the message itself; it produces a URL that must be opened via a Notify node to trigger WhatsApp.',
	icon: Smartphone,
	color: '#25d366',
	category: 'Utility',
	tags: ['whatsapp', 'message', 'chat', 'url', 'action'],
	fields: [],
	defaultInputs: [
		{ name: 'phone', portType: 'String', required: true, description: 'Phone number (with country code, no + or spaces)' },
		{ name: 'message', portType: 'String', required: true, description: 'Pre-filled message text' },
	],
	defaultOutputs: [
		{ name: 'url', portType: 'String', required: false, description: 'Generated WhatsApp URL' },
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
