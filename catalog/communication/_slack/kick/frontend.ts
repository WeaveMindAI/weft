import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { UserX } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const slackKickNode: NodeTemplate = {
	type: 'SlackKick',
	label: 'Slack Kick',
	description: 'Remove a user from a Slack channel',
	icon: UserX,
	color: '#4A154B',
	category: 'Utility',
	tags: ['slack', 'kick', 'remove', 'moderation'],
	fields: [],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: true, description: 'Slack config from SlackConfig node', configurable: false },
		{ name: 'channelId', portType: 'String', required: true, description: 'Channel ID' },
		{ name: 'userId', portType: 'String', required: true, description: 'User ID to remove' },
	],
	defaultOutputs: [
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the kick succeeded' },
	],
	features: {},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];
		if (!isInputConnected('config', context)) {
			errors.push({ port: 'config', message: 'Slack Config is required', level: 'structural' });
		} else {
			const t = getConnectedNodeType('config', context);
			if (t && t !== 'SlackConfig') errors.push({ port: 'config', message: `Expected SlackConfig, got ${t}`, level: 'structural' });
		}
		if (!isInputConnected('channelId', context)) errors.push({ port: 'channelId', message: 'Channel ID is required', level: 'structural' });
		if (!isInputConnected('userId', context)) errors.push({ port: 'userId', message: 'User ID is required', level: 'structural' });
		return errors;
	},
};
