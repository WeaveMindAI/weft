import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Shield } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const telegramRestrictNode: NodeTemplate = {
	type: 'TelegramRestrict',
	label: 'Telegram Restrict',
	description: 'Restrict a user\'s permissions in a Telegram supergroup',
	icon: Shield,
	color: '#0088CC',
	category: 'Utility',
	tags: ['telegram', 'restrict', 'permissions', 'moderation', 'member'],
	fields: [],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: true, description: 'Telegram config from TelegramConfig node', configurable: false },
		{ name: 'chatId', portType: 'String', required: true, description: 'Supergroup chat ID' },
		{ name: 'userId', portType: 'String', required: true, description: 'User ID to restrict' },
		{ name: 'canSendMessages', portType: 'Boolean', required: false, description: 'Allow sending text messages' },
		{ name: 'canSendMedia', portType: 'Boolean', required: false, description: 'Allow sending media (photos, videos, audio, documents)' },
		{ name: 'canSendPolls', portType: 'Boolean', required: false, description: 'Allow sending polls' },
		{ name: 'canAddWebPagePreviews', portType: 'Boolean', required: false, description: 'Allow adding web page previews' },
		{ name: 'canInviteUsers', portType: 'Boolean', required: false, description: 'Allow inviting users' },
		{ name: 'canPinMessages', portType: 'Boolean', required: false, description: 'Allow pinning messages' },
	],
	defaultOutputs: [
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the restriction succeeded' },
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
