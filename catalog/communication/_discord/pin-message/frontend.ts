import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Pin } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const discordPinMessageNode: NodeTemplate = {
	type: 'DiscordPinMessage',
	label: 'Discord Pin Message',
	description: 'Pin a message in a Discord channel',
	icon: Pin,
	color: '#5865F2',
	category: 'Utility',
	tags: ['discord', 'pin', 'message', 'moderation'],
	fields: [],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: true, description: 'Discord config from DiscordConfig node', configurable: false },
		{ name: 'channelId', portType: 'String', required: true, description: 'Channel ID containing the message' },
		{ name: 'messageId', portType: 'String', required: true, description: 'Message ID to pin' },
	],
	defaultOutputs: [
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the pin succeeded' },
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
		if (!isInputConnected('channelId', context)) errors.push({ port: 'channelId', message: 'Channel ID is required', level: 'structural' });
		if (!isInputConnected('messageId', context)) errors.push({ port: 'messageId', message: 'Message ID is required', level: 'structural' });
		return errors;
	},
};
