import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { MessageSquare } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const DiscordNode: NodeTemplate = {
	type: 'DiscordReceive',
	label: 'Discord',
	description: 'Connects to Discord Gateway and triggers on messages/events',
	icon: MessageSquare,
	color: '#5865a8',
	category: 'Triggers',
	tags: ['trigger', 'discord', 'bot', 'chat', 'socket'],
	fields: [
		{ key: 'guildId', label: 'Guild ID (optional)', type: 'text', placeholder: 'Filter to specific server' },
	],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: true, description: 'Discord config from DiscordConfig node', configurable: false },
	],
	defaultOutputs: [
		{ name: 'content', portType: 'String', required: false, description: 'Message text content' },
		{ name: 'authorName', portType: 'String', required: false, description: 'Username of the message author' },
		{ name: 'authorId', portType: 'String', required: false, description: 'User ID of the message author' },
		{ name: 'channelId', portType: 'String', required: false, description: 'Channel ID where the message was sent' },
		{ name: 'guildId', portType: 'String', required: false, description: 'Server (guild) ID' },
		{ name: 'messageId', portType: 'String', required: false, description: 'Unique message ID' },
		{ name: 'timestamp', portType: 'String', required: false, description: 'Message timestamp (ISO 8601)' },
		{ name: 'isBot', portType: 'Boolean', required: false, description: 'Whether the author is a bot' },
	],
	features: {
		isTrigger: true,
		triggerCategory: 'Socket',
	},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];

		if (!isInputConnected('config', context)) {
			errors.push({ port: 'config', message: 'Discord Config is required - connect a DiscordConfig node', level: 'structural' });
		} else {
			const connectedType = getConnectedNodeType('config', context);
			if (connectedType && connectedType !== 'DiscordConfig') {
				errors.push({ port: 'config', message: `Config should be connected to a DiscordConfig node, not ${connectedType}`, level: 'structural' });
			}
		}

		return errors;
	},
};
