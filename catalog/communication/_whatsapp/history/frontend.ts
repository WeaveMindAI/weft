import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { History } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const WhatsAppHistoryNode: NodeTemplate = {
	type: 'WhatsAppHistory',
	label: 'WhatsApp History',
	description: 'Fetch recent messages from a WhatsApp chat. Audio messages are lazy-downloaded at query time.',
	icon: History,
	color: '#25D366',
	category: 'Utility',
	tags: ['whatsapp', 'messaging', 'history', 'chat'],
	fields: [
		{ key: 'count', type: 'number', label: 'Message count', defaultValue: 20, description: 'Number of recent messages to fetch (max 50)' },
	],
	defaultInputs: [
		{ name: 'endpointUrl', portType: 'String', required: true, description: 'Sidecar action endpoint URL (from WhatsAppBridge node endpointUrl output)' },
		{ name: 'chatId', portType: 'String', required: true, description: 'Chat/conversation ID (e.g. 1234567890@s.whatsapp.net)' },
	],
	defaultOutputs: [
		{ name: 'contents', portType: 'List[String]', required: false, description: 'Message text contents (newest last)' },
		{ name: 'senderNames', portType: 'List[String]', required: false, description: 'Display names of each sender' },
		{ name: 'timestamps', portType: 'List[String]', required: false, description: 'Unix epoch timestamps (seconds)' },
		{ name: 'fromMe', portType: 'List[Boolean]', required: false, description: 'Whether each message was sent by you (boolean)' },
		{ name: 'messageTypes', portType: 'List[String]', required: false, description: 'Message types (e.g. "text", "audio", "image")' },
	],
	features: {
	},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];

		if (!isInputConnected('endpointUrl', context)) {
			errors.push({ port: 'endpointUrl', message: 'WhatsApp Bridge is required - connect the endpointUrl output of a WhatsAppBridge node', level: 'structural' });
		} else {
			const connectedType = getConnectedNodeType('endpointUrl', context);
			if (connectedType && connectedType !== 'WhatsAppBridge') {
				errors.push({ port: 'endpointUrl', message: `endpointUrl should come from a WhatsAppBridge node, not ${connectedType}`, level: 'structural' });
			}
		}

		if (!isInputConnected('chatId', context)) {
			errors.push({ port: 'chatId', message: 'chatId is required - connect it from a WhatsAppReceive node or provide it directly', level: 'structural' });
		}

		return errors;
	},
};
