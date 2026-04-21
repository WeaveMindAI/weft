import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Send } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const telegramSendNode: NodeTemplate = {
	type: 'TelegramSend',
	label: 'Telegram Send',
	description: 'Send a message or media via Telegram Bot API',
	icon: Send,
	color: '#0088cc',
	category: 'Utility',
	tags: ['telegram', 'send', 'message', 'bot', 'output'],
	fields: [],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: true, description: 'Telegram config from TelegramConfig node', configurable: false },
		{ name: 'chatId', portType: 'String', required: true, description: 'Telegram chat ID to send to' },
		{ name: 'text', portType: 'String', required: false, description: 'Message text to send (becomes caption when media provided)' },
		{ name: 'replyToMessageId', portType: 'String', required: false, description: 'Message ID to reply to (optional)' },
		{ name: 'media', portType: 'Media', required: false, description: 'Media object from Image/Video/Audio/Document node' },
	],
	defaultOutputs: [
		{ name: 'messageId', portType: 'String', required: false, description: 'ID of the sent message' },
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the message was sent successfully' },
	],
	features: {
		oneOfRequired: [['text', 'media']],
	},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];

		if (!isInputConnected('config', context)) {
			errors.push({ port: 'config', message: 'Telegram Config is required - connect a TelegramConfig node', level: 'structural' });
		} else {
			const connectedType = getConnectedNodeType('config', context);
			if (connectedType && connectedType !== 'TelegramConfig') {
				errors.push({ port: 'config', message: `Config should be connected to a TelegramConfig node, not ${connectedType}`, level: 'structural' });
			}
		}
		if (!isInputConnected('chatId', context)) {
			errors.push({ port: 'chatId', message: 'Chat ID input is required', level: 'structural' });
		}
		if (!isInputConnected('text', context) && !isInputConnected('media', context)) {
			errors.push({ port: 'text', message: 'Either text or media is required', level: 'structural' });
		}

		return errors;
	},
};
