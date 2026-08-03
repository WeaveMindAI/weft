import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { MessageSquare } from '@lucide/svelte';
import { hasConfigValue } from '$lib/validation';

export const discordConfigNode: NodeTemplate = {
	type: 'DiscordConfig',
	label: 'Discord Config',
	description: 'Discord bot credentials for triggers and message sending',
	icon: MessageSquare,
	color: '#5865f2',
	category: 'Data',
	tags: ['discord', 'config', 'bot', 'credentials', 'token'],
	fields: [
		{ key: 'botToken', label: 'Bot Token', type: 'password', placeholder: 'Your Discord bot token' },
	],
	defaultInputs: [],
	defaultOutputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: false, description: 'Discord configuration object' },
	],
	setupGuide: [
		'This node stores your Discord bot token. Connect its "config" output to any Discord node.',
		'',
		'--- Creating a Discord Bot ---',
		'1. Go to discord.com/developers/applications',
		'2. Click "New Application", give it a name, then click "Create"',
		'3. Go to the "Bot" tab on the left sidebar',
		'4. Click "Reset Token" to generate a new bot token',
		'5. Copy the token and paste it here',
		'',
		'--- Required Bot Permissions ---',
		'For receiving messages: Enable "Message Content Intent" under Bot > Privileged Gateway Intents',
		'For sending messages: The bot needs "Send Messages" permission in the target channel',
		'',
		'--- Inviting the Bot to Your Server ---',
		'1. Go to the "OAuth2" tab > "URL Generator"',
		'2. Select scopes: "bot"',
		'3. Select permissions: "Send Messages", "Read Message History"',
		'4. Copy the generated URL and open it in your browser to invite the bot',
	],
	features: {
	},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];

		if (!hasConfigValue('botToken', context.config)) {
			errors.push({ field: 'botToken', message: 'Bot Token is required', level: 'runtime' });
		}

		return errors;
	},
};
