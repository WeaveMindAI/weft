import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { UserX } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const discordKickNode: NodeTemplate = {
	type: 'DiscordKick',
	label: 'Discord Kick',
	description: 'Kick a member from a Discord server',
	icon: UserX,
	color: '#5865F2',
	category: 'Utility',
	tags: ['discord', 'kick', 'moderation', 'member'],
	fields: [],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: true, description: 'Discord config from DiscordConfig node', configurable: false },
		{ name: 'guildId', portType: 'String', required: true, description: 'Server (guild) ID' },
		{ name: 'userId', portType: 'String', required: true, description: 'User ID to kick' },
	],
	defaultOutputs: [
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the kick succeeded' },
	],
	features: {},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];
		if (!isInputConnected('config', context)) {
			errors.push({ port: 'config', message: 'Discord Config is required', level: 'structural' });
		} else {
			const t = getConnectedNodeType('config', context);
			if (t && t !== 'DiscordConfig') errors.push({ port: 'config', message: `Expected DiscordConfig, got ${t}`, level: 'structural' });
		}
		if (!isInputConnected('guildId', context)) errors.push({ port: 'guildId', message: 'Guild ID is required', level: 'structural' });
		if (!isInputConnected('userId', context)) errors.push({ port: 'userId', message: 'User ID is required', level: 'structural' });
		return errors;
	},
};
