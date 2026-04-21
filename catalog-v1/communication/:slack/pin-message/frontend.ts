import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Pin } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const slackPinMessageNode: NodeTemplate = {
	type: 'SlackPinMessage',
	label: 'Slack Pin Message',
	description: 'Pin a message in a Slack channel',
	icon: Pin,
	color: '#4A154B',
	category: 'Utility',
	tags: ['slack', 'pin', 'message'],
	fields: [],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: true, description: 'Slack config from SlackConfig node', configurable: false },
		{ name: 'channelId', portType: 'String', required: true, description: 'Channel ID' },
		{ name: 'messageTs', portType: 'String', required: true, description: 'Message timestamp to pin' },
	],
	defaultOutputs: [
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the pin succeeded' },
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
		if (!isInputConnected('messageTs', context)) errors.push({ port: 'messageTs', message: 'Message timestamp is required', level: 'structural' });
		return errors;
	},
};
