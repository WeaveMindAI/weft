import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Clock } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const discordTimeoutNode: NodeTemplate = {
	type: 'DiscordTimeout',
	label: 'Discord Timeout',
	description: 'Timeout (mute) a member in a Discord server',
	icon: Clock,
	color: '#5865F2',
	category: 'Utility',
	tags: ['discord', 'timeout', 'mute', 'moderation', 'member'],
	fields: [],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: true, description: 'Discord config from DiscordConfig node', configurable: false },
		{ name: 'guildId', portType: 'String', required: true, description: 'Server (guild) ID' },
		{ name: 'userId', portType: 'String', required: true, description: 'User ID to timeout' },
		{ name: 'durationSeconds', portType: 'Number', required: true, description: 'Timeout duration in seconds (0 to remove, max 2419200 = 28 days)' },
	],
	defaultOutputs: [
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the timeout succeeded' },
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
		if (!isInputConnected('durationSeconds', context)) errors.push({ port: 'durationSeconds', message: 'Duration is required', level: 'structural' });
		return errors;
	},
};
