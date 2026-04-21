import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Send } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const telegramReceiveNode: NodeTemplate = {
	type: 'TelegramReceive',
	label: 'Telegram Receive',
	description: 'Triggers on new Telegram messages to your bot',
	icon: Send,
	color: '#0088cc',
	category: 'Triggers',
	tags: ['trigger', 'telegram', 'messages', 'bot', 'polling'],
	fields: [
		{ key: 'chatId', label: 'Chat ID (optional)', type: 'text', placeholder: 'Filter to specific chat' },
	],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: true, description: 'Telegram config from TelegramConfig node', configurable: false },
	],
	defaultOutputs: [
		{ name: 'text', portType: 'String', required: false, description: 'Message text content' },
		{ name: 'chatId', portType: 'String', required: false, description: 'Chat ID where the message was sent' },
		{ name: 'chatTitle', portType: 'String', required: false, description: 'Chat title (or first name for private chats)' },
		{ name: 'chatType', portType: 'String', required: false, description: 'Chat type: private, group, supergroup, or channel' },
		{ name: 'fromUsername', portType: 'String', required: false, description: 'Username of the message sender' },
		{ name: 'fromFirstName', portType: 'String', required: false, description: 'First name of the message sender' },
		{ name: 'fromId', portType: 'String', required: false, description: 'User ID of the message sender' },
		{ name: 'messageId', portType: 'String', required: false, description: 'Unique message ID' },
		{ name: 'date', portType: 'String', required: false, description: 'Message timestamp (ISO 8601)' },
		{ name: 'isReply', portType: 'Boolean', required: false, description: 'Whether the message is a reply' },
	],
	features: {
		isTrigger: true,
		triggerCategory: 'Polling',
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

		return errors;
	},
};
