import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Trash2 } from '@lucide/svelte';
import { isInputConnected } from '$lib/validation';

export const whatsappDeleteMessageNode: NodeTemplate = {
	type: 'WhatsAppDeleteMessage',
	label: 'WhatsApp Delete Message',
	description: 'Delete a message in a WhatsApp chat',
	icon: Trash2,
	color: '#25D366',
	category: 'Utility',
	tags: ['whatsapp', 'delete', 'message', 'moderation'],
	fields: [],
	defaultInputs: [
		{ name: 'endpointUrl', portType: 'String', required: true, description: 'Bridge endpoint URL from WhatsAppBridge node' },
		{ name: 'chatId', portType: 'String', required: true, description: 'Chat JID' },
		{ name: 'messageId', portType: 'String', required: true, description: 'Message ID to delete' },
		{ name: 'fromMe', portType: 'Boolean', required: false, description: 'Whether the message was sent by you (default true)' },
	],
	defaultOutputs: [
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the deletion succeeded' },
	],
	features: {},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];
		if (!isInputConnected('endpointUrl', context)) errors.push({ port: 'endpointUrl', message: 'Connect a WhatsAppBridge node', level: 'structural' });
		if (!isInputConnected('chatId', context)) errors.push({ port: 'chatId', message: 'Chat ID is required', level: 'structural' });
		if (!isInputConnected('messageId', context)) errors.push({ port: 'messageId', message: 'Message ID is required', level: 'structural' });
		return errors;
	},
};
