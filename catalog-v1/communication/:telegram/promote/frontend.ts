import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Shield } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const telegramPromoteNode: NodeTemplate = {
	type: 'TelegramPromote',
	label: 'Telegram Promote',
	description: 'Promote or demote a user in a Telegram chat',
	icon: Shield,
	color: '#0088CC',
	category: 'Utility',
	tags: ['telegram', 'promote', 'demote', 'admin', 'moderation'],
	fields: [],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: true, description: 'Telegram config from TelegramConfig node', configurable: false },
		{ name: 'chatId', portType: 'String', required: true, description: 'Chat ID' },
		{ name: 'userId', portType: 'String', required: true, description: 'User ID to promote/demote' },
		{ name: 'canManageChat', portType: 'Boolean', required: false, description: 'Can manage chat' },
		{ name: 'canDeleteMessages', portType: 'Boolean', required: false, description: 'Can delete messages' },
		{ name: 'canManageVideoChats', portType: 'Boolean', required: false, description: 'Can manage video chats' },
		{ name: 'canRestrictMembers', portType: 'Boolean', required: false, description: 'Can restrict members' },
		{ name: 'canPromoteMembers', portType: 'Boolean', required: false, description: 'Can promote members' },
		{ name: 'canChangeInfo', portType: 'Boolean', required: false, description: 'Can change chat info' },
		{ name: 'canInviteUsers', portType: 'Boolean', required: false, description: 'Can invite users' },
		{ name: 'canPinMessages', portType: 'Boolean', required: false, description: 'Can pin messages' },
		{ name: 'canPostMessages', portType: 'Boolean', required: false, description: 'Can post messages (channels only)' },
		{ name: 'canEditMessages', portType: 'Boolean', required: false, description: 'Can edit messages (channels only)' },
	],
	defaultOutputs: [
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the promotion succeeded' },
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
