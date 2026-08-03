import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Send } from '@lucide/svelte';
import { hasConfigValue } from '$lib/validation';

export const telegramConfigNode: NodeTemplate = {
	type: 'TelegramConfig',
	label: 'Telegram Config',
	description: 'Telegram Bot API credentials for triggers and messaging',
	icon: Send,
	color: '#0088cc',
	category: 'Data',
	tags: ['telegram', 'config', 'bot', 'credentials', 'token'],
	fields: [
		{ key: 'botToken', label: 'Bot Token', type: 'password', placeholder: 'Your Telegram bot token from @BotFather' },
	],
	defaultInputs: [],
	defaultOutputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: false, description: 'Telegram configuration object' },
	],
	setupGuide: [
		'This node stores your Telegram bot token.',
		'Connect its "config" output to TelegramReceive or TelegramSend nodes.',
		'',
		'--- Creating a Telegram Bot ---',
		'1. Open Telegram and search for @BotFather',
		'2. Send /newbot and follow the prompts',
		'3. Choose a name and username for your bot',
		'4. BotFather will give you a token like: 123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11',
		'5. Copy the token and paste it here',
		'',
		'--- Adding the Bot to a Group ---',
		'1. Open the group in Telegram',
		'2. Click the group name > Add Members',
		'3. Search for your bot by its username and add it',
		'4. The bot will start receiving messages from the group',
		'',
		'--- Getting a Chat ID ---',
		'Send a message to your bot, then visit:',
		'https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates',
		'Look for "chat":{"id": ...} in the response.',
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
