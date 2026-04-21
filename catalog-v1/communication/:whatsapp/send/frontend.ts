import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Send } from '@lucide/svelte';
import { isInputConnected } from '$lib/validation';

export const WhatsAppSendNode: NodeTemplate = {
	type: 'WhatsAppSend',
	label: 'WhatsApp Send',
	description: 'Send a text message via WhatsApp. For media, use WhatsApp Send Media.',
	icon: Send,
	color: '#25D366',
	category: 'Utility',
	tags: ['whatsapp', 'messaging', 'send', 'text'],
	fields: [],
	defaultInputs: [
		{ name: 'endpointUrl', portType: 'String', required: true, description: 'Sidecar action endpoint URL (from WhatsAppBridge node endpointUrl output)' },
		{ name: 'to', portType: 'String', required: true, description: 'Recipient (e.g. 1234567890@s.whatsapp.net)' },
		{ name: 'message', portType: 'String', required: true, description: 'Text message to send' },
	],
	defaultOutputs: [
		{ name: 'messageId', portType: 'String', required: false, description: 'ID of the sent message' },
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the message was sent successfully' },
	],
	features: {
	},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];
		if (!isInputConnected('endpointUrl', context)) {
			errors.push({ port: 'endpointUrl', message: 'Connect a WhatsAppBridge node', level: 'structural' });
		}
		if (!isInputConnected('to', context)) {
			errors.push({ port: 'to', message: 'Recipient is required', level: 'structural' });
		}
		if (!isInputConnected('message', context)) {
			errors.push({ port: 'message', message: 'Message text is required', level: 'structural' });
		}
		return errors;
	},
};
