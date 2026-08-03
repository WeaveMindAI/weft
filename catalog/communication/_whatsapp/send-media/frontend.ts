import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Smartphone } from '@lucide/svelte';
import { isInputConnected } from '$lib/validation';

export const whatsappSendMediaNode: NodeTemplate = {
	type: 'WhatsAppSendMedia',
	label: 'WhatsApp Send Media',
	description: 'Send an image, audio, video, or document via WhatsApp',
	icon: Smartphone,
	color: '#25D366',
	category: 'Utility',
	tags: ['whatsapp', 'send', 'media', 'image', 'audio', 'video'],
	fields: [],
	defaultInputs: [
		{ name: 'endpointUrl', portType: 'String', required: true, description: 'Bridge endpoint URL from WhatsAppBridge node' },
		{ name: 'to', portType: 'String', required: true, description: 'Recipient JID (e.g. 1234567890@s.whatsapp.net)' },
		{ name: 'media', portType: 'Media', required: true, description: 'Media object from Image/Video/Audio/Document nodes' },
		{ name: 'caption', portType: 'String', required: false, description: 'Caption for the media' },
	],
	defaultOutputs: [
		{ name: 'messageId', portType: 'String', required: false, description: 'ID of the sent message' },
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the media was sent' },
	],
	features: {},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];
		if (!isInputConnected('endpointUrl', context)) errors.push({ port: 'endpointUrl', message: 'Connect a WhatsAppBridge node', level: 'structural' });
		if (!isInputConnected('to', context)) errors.push({ port: 'to', message: 'Recipient is required', level: 'structural' });
		if (!isInputConnected('media', context)) errors.push({ port: 'media', message: 'Media is required. Connect an Image, Video, Audio, or Document node.', level: 'structural' });
		return errors;
	},
};
