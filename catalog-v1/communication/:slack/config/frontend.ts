import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { MessageSquare } from '@lucide/svelte';
import { hasConfigValue } from '$lib/validation';

export const slackConfigNode: NodeTemplate = {
	type: 'SlackConfig',
	label: 'Slack Config',
	description: 'Slack API credentials for triggers and messaging',
	icon: MessageSquare,
	color: '#4A154B',
	category: 'Data',
	tags: ['slack', 'config', 'bot', 'credentials', 'token'],
	fields: [
		{ key: 'botToken', label: 'Bot Token', type: 'password', placeholder: 'xoxb-... (for sending messages)' },
		{ key: 'appToken', label: 'App-Level Token', type: 'password', placeholder: 'xapp-... (for Socket Mode / receiving)' },
	],
	defaultInputs: [],
	defaultOutputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: false, description: 'Slack configuration object' },
	],
	setupGuide: [
		'This node stores your Slack API credentials.',
		'Connect its "config" output to SlackReceive or SlackSend nodes.',
		'',
		'--- Creating a Slack App ---',
		'1. Go to api.slack.com/apps and click "Create New App"',
		'2. Choose "From scratch", name your app, and select a workspace',
		'',
		'--- Enabling Socket Mode (for receiving messages) ---',
		'3. Go to "Socket Mode" in the sidebar and toggle it on',
		'4. Generate an App-Level Token with scope "connections:write"',
		'5. Copy the token (starts with xapp-) and paste it here',
		'',
		'--- Adding Event Subscriptions ---',
		'6. Go to "Event Subscriptions" and toggle it on',
		'7. Under "Subscribe to bot events", add: message.channels, message.groups, message.im',
		'',
		'--- Bot Token ---',
		'8. Go to "OAuth & Permissions" in the sidebar',
		'9. Under "Bot Token Scopes", add: chat:write, channels:history, groups:history, im:history',
		'10. Install the app to your workspace',
		'11. Copy the Bot Token (starts with xoxb-) and paste it here',
		'',
		'--- Which tokens do you need? ---',
		'SlackReceive (trigger): App-Level Token (xapp-)',
		'SlackSend: Bot Token (xoxb-)',
		'Both: fill in both fields',
	],
	features: {
	},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];

		const hasBotToken = hasConfigValue('botToken', context.config);
		const hasAppToken = hasConfigValue('appToken', context.config);

		if (!hasBotToken && !hasAppToken) {
			errors.push({ field: 'botToken', message: 'At least a Bot Token (for sending) or App-Level Token (for receiving) is required', level: 'runtime' });
		}

		return errors;
	},
};
