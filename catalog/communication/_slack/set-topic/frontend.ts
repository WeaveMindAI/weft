import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Hash } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const slackSetTopicNode: NodeTemplate = {
	type: 'SlackSetTopic',
	label: 'Slack Set Topic',
	description: 'Set the topic of a Slack channel',
	icon: Hash,
	color: '#4A154B',
	category: 'Utility',
	tags: ['slack', 'topic', 'channel', 'settings'],
	fields: [],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: true, description: 'Slack config from SlackConfig node', configurable: false },
		{ name: 'channelId', portType: 'String', required: true, description: 'Channel ID' },
		{ name: 'topic', portType: 'String', required: true, description: 'New topic text' },
	],
	defaultOutputs: [
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the topic was set' },
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
		if (!isInputConnected('topic', context)) errors.push({ port: 'topic', message: 'Topic text is required', level: 'structural' });
		return errors;
	},
};
