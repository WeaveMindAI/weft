import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { SmilePlus } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const slackReactNode: NodeTemplate = {
	type: 'SlackReact',
	label: 'Slack React',
	description: 'Add a reaction to a Slack message',
	icon: SmilePlus,
	color: '#4A154B',
	category: 'Utility',
	tags: ['slack', 'react', 'emoji', 'reaction'],
	fields: [],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: true, description: 'Slack config from SlackConfig node', configurable: false },
		{ name: 'channelId', portType: 'String', required: true, description: 'Channel ID' },
		{ name: 'messageTs', portType: 'String', required: true, description: 'Message timestamp to react to' },
		{ name: 'emoji', portType: 'String', required: true, description: 'Emoji name without colons (e.g. thumbsup)' },
	],
	defaultOutputs: [
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the reaction was added' },
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
		if (!isInputConnected('emoji', context)) errors.push({ port: 'emoji', message: 'Emoji name is required', level: 'structural' });
		return errors;
	},
};
