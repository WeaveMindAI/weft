import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { MessageCircle } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const WhatsAppReceiveNode: NodeTemplate = {
	type: 'WhatsAppReceive',
	label: 'WhatsApp Receive',
	description: 'Triggers when a WhatsApp message is received. Requires a running WhatsAppBridge infrastructure.',
	icon: MessageCircle,
	color: '#25D366',
	category: 'Triggers',
	tags: ['trigger', 'whatsapp', 'messaging', 'receive', 'webhook'],
	fields: [],
	defaultInputs: [
		{ name: 'endpointUrl', portType: 'String', required: true, description: 'Sidecar action endpoint URL (from WhatsAppBridge node endpointUrl output)' },
	],
	defaultOutputs: [
		{ name: 'content', portType: 'String', required: false, description: 'Message text content' },
		{ name: 'from', portType: 'String', required: false, description: 'Sender JID (e.g. 1234567890@s.whatsapp.net)' },
		{ name: 'pushName', portType: 'String', required: false, description: 'Sender display name' },
		{ name: 'messageId', portType: 'String', required: false, description: 'Unique message ID' },
		{ name: 'timestamp', portType: 'String', required: false, description: 'Message timestamp' },
		{ name: 'isGroup', portType: 'Boolean', required: false, description: 'Whether the message is from a group chat' },
		{ name: 'chatId', portType: 'String', required: false, description: 'Chat/conversation ID' },
		{ name: 'audio', portType: 'Audio', required: false, description: 'Audio media object (null for non-audio messages)' },
		{ name: 'image', portType: 'Image', required: false, description: 'Image media object (null for non-image messages)' },
		{ name: 'video', portType: 'Video', required: false, description: 'Video media object (null for non-video messages)' },
		{ name: 'document', portType: 'Document', required: false, description: 'Document media object (null for non-document messages)' },
		{ name: 'messageType', portType: 'String', required: false, description: 'Message type: text, audio, image, video, document, sticker, contact, location' },
	],
	features: {
		isTrigger: true,
		triggerCategory: 'Webhook',
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

		return errors;
	},
};
