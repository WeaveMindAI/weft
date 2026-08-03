import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { MessageSquare } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const slackReceiveNode: NodeTemplate = {
	type: 'SlackReceive',
	label: 'Slack Receive',
	description: 'Triggers on new Slack messages via Socket Mode',
	icon: MessageSquare,
	color: '#4A154B',
	category: 'Triggers',
	tags: ['trigger', 'slack', 'messages', 'bot', 'socket'],
	fields: [
		{ key: 'channelId', label: 'Channel ID (optional)', type: 'text', placeholder: 'Filter to specific channel' },
	],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: true, description: 'Slack config from SlackConfig node', configurable: false },
	],
	defaultOutputs: [
		{ name: 'text', portType: 'String', required: false, description: 'Message text content' },
		{ name: 'userId', portType: 'String', required: false, description: 'User ID of the message sender' },
		{ name: 'channelId', portType: 'String', required: false, description: 'Channel ID where the message was sent' },
		{ name: 'channelType', portType: 'String', required: false, description: 'Channel type (channel, group, im)' },
		{ name: 'timestamp', portType: 'String', required: false, description: 'Message timestamp (Slack ts format)' },
		{ name: 'threadTs', portType: 'String', required: false, description: 'Thread timestamp (empty if not in a thread)' },
		{ name: 'teamId', portType: 'String', required: false, description: 'Workspace team ID' },
		{ name: 'isThread', portType: 'Boolean', required: false, description: 'Whether the message is in a thread' },
	],
	features: {
		isTrigger: true,
		triggerCategory: 'Socket',
	},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];

		if (!isInputConnected('config', context)) {
			errors.push({ port: 'config', message: 'Slack Config is required - connect a SlackConfig node', level: 'structural' });
		} else {
			const connectedType = getConnectedNodeType('config', context);
			if (connectedType && connectedType !== 'SlackConfig') {
				errors.push({ port: 'config', message: `Config should be connected to a SlackConfig node, not ${connectedType}`, level: 'structural' });
			}
		}

		return errors;
	},
};
