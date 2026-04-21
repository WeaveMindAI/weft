import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Forward } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const telegramForwardMessageNode: NodeTemplate = {
	type: 'TelegramForwardMessage',
	label: 'Telegram Forward Message',
	description: 'Forward a message between Telegram chats',
	icon: Forward,
	color: '#0088CC',
	category: 'Utility',
	tags: ['telegram', 'forward', 'message'],
	fields: [],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: true, description: 'Telegram config from TelegramConfig node', configurable: false },
		{ name: 'chatId', portType: 'String', required: true, description: 'Target chat ID to forward to' },
		{ name: 'fromChatId', portType: 'String', required: true, description: 'Source chat ID to forward from' },
		{ name: 'messageId', portType: 'String', required: true, description: 'Message ID to forward' },
	],
	defaultOutputs: [
		{ name: 'messageId', portType: 'String', required: false, description: 'ID of the forwarded message' },
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the forward succeeded' },
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
		if (!isInputConnected('chatId', context)) errors.push({ port: 'chatId', message: 'Target Chat ID is required', level: 'structural' });
		if (!isInputConnected('fromChatId', context)) errors.push({ port: 'fromChatId', message: 'Source Chat ID is required', level: 'structural' });
		if (!isInputConnected('messageId', context)) errors.push({ port: 'messageId', message: 'Message ID is required', level: 'structural' });
		return errors;
	},
};
