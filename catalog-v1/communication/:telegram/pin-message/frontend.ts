import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Pin } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const telegramPinMessageNode: NodeTemplate = {
	type: 'TelegramPinMessage',
	label: 'Telegram Pin Message',
	description: 'Pin a message in a Telegram chat',
	icon: Pin,
	color: '#0088CC',
	category: 'Utility',
	tags: ['telegram', 'pin', 'message', 'moderation'],
	fields: [],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: true, description: 'Telegram config from TelegramConfig node', configurable: false },
		{ name: 'chatId', portType: 'String', required: true, description: 'Chat ID' },
		{ name: 'messageId', portType: 'String', required: true, description: 'Message ID to pin' },
		{ name: 'disableNotification', portType: 'Boolean', required: false, description: 'Pin silently without notification' },
	],
	defaultOutputs: [
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the pin succeeded' },
	],
	features: {},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];
		if (!isInputConnected('config', context)) {
			errors.push({ port: 'config', message: 'Telegram Config is required', level: 'structural' });
		} else {
			const t = getConnectedNodeType('config', context);
			if (t && t !== 'TelegramConfig') errors.push({ port: 'config', message: `Expected TelegramConfig, got ${t}`, level: 'structural' });
		}
		if (!isInputConnected('chatId', context)) errors.push({ port: 'chatId', message: 'Chat ID is required', level: 'structural' });
		if (!isInputConnected('messageId', context)) errors.push({ port: 'messageId', message: 'Message ID is required', level: 'structural' });
		return errors;
	},
};
