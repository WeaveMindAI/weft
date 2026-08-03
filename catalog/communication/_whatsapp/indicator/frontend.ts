import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Activity } from '@lucide/svelte';
import { isInputConnected } from '$lib/validation';

export const whatsappIndicatorNode: NodeTemplate = {
	type: 'WhatsAppIndicator',
	label: 'WhatsApp Indicator',
	description: 'Show typing or recording indicator for a configurable duration with jitter',
	icon: Activity,
	color: '#25D366',
	category: 'Utility',
	tags: ['whatsapp', 'typing', 'recording', 'indicator', 'presence'],
	fields: [
		{ key: 'action', label: 'Action', type: 'select', options: ['composing', 'recording', 'paused'], placeholder: 'composing', description: 'composing = typing, recording = voice/video' },
	],
	defaultInputs: [
		{ name: 'endpointUrl', portType: 'String', required: true, description: 'Bridge endpoint URL from WhatsAppBridge node' },
		{ name: 'chatId', portType: 'String', required: true, description: 'Chat JID' },
		{ name: 'durationMs', portType: 'Number', required: false, description: 'Total duration in ms (default 3000, max 300000)' },
		{ name: 'intervalMs', portType: 'Number', required: false, description: 'Re-send interval in ms (default 800, capped at decay ~10s)' },
		{ name: 'jitterMs', portType: 'Number', required: false, description: 'Random ± jitter in ms (default 300)' },
	],
	defaultOutputs: [
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the indicator was shown' },
	],
	features: {},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];
		if (!isInputConnected('endpointUrl', context)) errors.push({ port: 'endpointUrl', message: 'Connect a WhatsAppBridge node', level: 'structural' });
		if (!isInputConnected('chatId', context)) errors.push({ port: 'chatId', message: 'Chat ID is required', level: 'structural' });
		return errors;
	},
};
