import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Shield } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const telegramBanNode: NodeTemplate = {
	type: 'TelegramBan',
	label: 'Telegram Ban',
	description: 'Ban a user from a Telegram chat',
	icon: Shield,
	color: '#0088CC',
	category: 'Utility',
	tags: ['telegram', 'ban', 'moderation', 'member'],
	fields: [],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: true, description: 'Telegram config from TelegramConfig node', configurable: false },
		{ name: 'chatId', portType: 'String', required: true, description: 'Chat ID' },
		{ name: 'userId', portType: 'String', required: true, description: 'User ID to ban' },
		{ name: 'revokeMessages', portType: 'Boolean', required: false, description: 'Delete all messages from this user (default false)' },
	],
	defaultOutputs: [
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the ban succeeded' },
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
		if (!isInputConnected('userId', context)) errors.push({ port: 'userId', message: 'User ID is required', level: 'structural' });
		return errors;
	},
};
