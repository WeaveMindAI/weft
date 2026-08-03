import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { MessageSquare } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const slackSendNode: NodeTemplate = {
	type: 'SlackSend',
	label: 'Slack Send',
	description: 'Send a message or media to a Slack channel',
	icon: MessageSquare,
	color: '#4A154B',
	category: 'Utility',
	tags: ['slack', 'send', 'message', 'bot', 'output'],
	fields: [],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: true, description: 'Slack config from SlackConfig node', configurable: false },
		{ name: 'channelId', portType: 'String', required: true, description: 'Slack channel ID to send to' },
		{ name: 'text', portType: 'String', required: false, description: 'Message text to send (optional if media provided)' },
		{ name: 'threadTs', portType: 'String', required: false, description: 'Thread timestamp to reply in (optional)' },
		{ name: 'media', portType: 'Media', required: false, description: 'Media object from Image/Video/Audio/Document node' },
	],
	defaultOutputs: [
		{ name: 'messageTs', portType: 'String', required: false, description: 'Timestamp of the sent message' },
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the message was sent successfully' },
	],
	features: {
		oneOfRequired: [['text', 'media']],
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
		if (!isInputConnected('channelId', context)) {
			errors.push({ port: 'channelId', message: 'Channel ID input is required', level: 'structural' });
		}
		if (!isInputConnected('text', context) && !isInputConnected('media', context)) {
			errors.push({ port: 'text', message: 'Either text or media is required', level: 'structural' });
		}

		return errors;
	},
};
