import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { MessageSquare } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const discordSendNode: NodeTemplate = {
	type: 'DiscordSend',
	label: 'Discord Send',
	description: 'Send a message or media to a Discord channel',
	icon: MessageSquare,
	color: '#5865f2',
	category: 'Utility',
	tags: ['discord', 'send', 'message', 'chat', 'output'],
	fields: [],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: true, description: 'Discord config from DiscordConfig node', configurable: false },
		{ name: 'message', portType: 'String', required: false, description: 'Message content to send (optional if media provided)' },
		{ name: 'channelId', portType: 'String', required: true, description: 'Discord channel ID to send to' },
		{ name: 'media', portType: 'Media', required: false, description: 'Media object from Image/Video/Audio/Document node' },
	],
	defaultOutputs: [
		{ name: 'messageId', portType: 'String', required: false, description: 'ID of the sent message' },
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the message was sent successfully' },
	],
	features: {
		oneOfRequired: [['message', 'media']],
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
		if (!isInputConnected('message', context) && !isInputConnected('media', context)) {
			errors.push({ port: 'message', message: 'Either message or media is required', level: 'structural' });
		}
		if (!isInputConnected('channelId', context)) {
			errors.push({ port: 'channelId', message: 'Channel ID input is required', level: 'structural' });
		}

		return errors;
	},
};
