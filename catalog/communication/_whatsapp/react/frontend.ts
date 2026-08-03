import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { SmilePlus } from '@lucide/svelte';
import { isInputConnected } from '$lib/validation';

export const whatsappReactNode: NodeTemplate = {
	type: 'WhatsAppReact',
	label: 'WhatsApp React',
	description: 'React to a WhatsApp message with an emoji',
	icon: SmilePlus,
	color: '#25D366',
	category: 'Utility',
	tags: ['whatsapp', 'react', 'emoji', 'reaction'],
	fields: [],
	defaultInputs: [
		{ name: 'endpointUrl', portType: 'String', required: true, description: 'Bridge endpoint URL from WhatsAppBridge node' },
		{ name: 'chatId', portType: 'String', required: true, description: 'Chat JID' },
		{ name: 'messageId', portType: 'String', required: true, description: 'Message ID to react to' },
		{ name: 'emoji', portType: 'String', required: true, description: 'Emoji to react with' },
	],
	defaultOutputs: [
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the reaction was sent' },
	],
	features: {},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];
		if (!isInputConnected('endpointUrl', context)) errors.push({ port: 'endpointUrl', message: 'Connect a WhatsAppBridge node', level: 'structural' });
		if (!isInputConnected('chatId', context)) errors.push({ port: 'chatId', message: 'Chat ID is required', level: 'structural' });
		if (!isInputConnected('messageId', context)) errors.push({ port: 'messageId', message: 'Message ID is required', level: 'structural' });
		if (!isInputConnected('emoji', context)) errors.push({ port: 'emoji', message: 'Emoji is required', level: 'structural' });
		return errors;
	},
};
