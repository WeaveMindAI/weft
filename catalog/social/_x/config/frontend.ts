import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { AtSign } from '@lucide/svelte';
import { hasConfigValue } from '$lib/validation';

export const xConfigNode: NodeTemplate = {
	type: 'XConfig',
	label: 'X Config',
	description: 'X (Twitter) API credentials for triggers and posting',
	icon: AtSign,
	color: '#000000',
	category: 'Data',
	tags: ['x', 'twitter', 'config', 'credentials', 'api'],
	fields: [
		{ key: 'bearerToken', label: 'Bearer Token', type: 'password', placeholder: 'For reading posts (search)' },
		{ key: 'apiKey', label: 'API Key', type: 'password', placeholder: 'For posting (OAuth 1.0a)' },
		{ key: 'apiKeySecret', label: 'API Key Secret', type: 'password', placeholder: 'For posting (OAuth 1.0a)' },
		{ key: 'accessToken', label: 'Access Token', type: 'password', placeholder: 'For posting (OAuth 1.0a)' },
		{ key: 'accessTokenSecret', label: 'Access Token Secret', type: 'password', placeholder: 'For posting (OAuth 1.0a)' },
	],
	defaultInputs: [],
	defaultOutputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: false, description: 'X API configuration object' },
	],
	setupGuide: [
		'This node stores your X (Twitter) API credentials.',
		'Connect its "config" output to XReceive (trigger) or XPost (send) nodes.',
		'',
		'--- Getting API Credentials ---',
		'1. Go to developer.x.com and sign in',
		'2. Create a new Project and App in the Developer Console',
		'3. Under "Keys and Tokens", generate your credentials:',
		'   - Bearer Token: for reading posts (used by XReceive)',
		'   - API Key + Secret: for posting (used by XPost)',
		'   - Access Token + Secret: for posting (used by XPost)',
		'',
		'--- Which credentials do you need? ---',
		'- XReceive (trigger): only Bearer Token is needed',
		'- XPost (send): API Key, API Key Secret, Access Token, Access Token Secret',
		'- Both: fill in all fields',
		'',
		'--- Pricing ---',
		'X API uses pay-per-usage pricing. No subscriptions required.',
		'Purchase credits in the Developer Console.',
	],
	features: {
	},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];

		const hasBearerToken = hasConfigValue('bearerToken', context.config);
		const hasOAuth = hasConfigValue('apiKey', context.config) &&
			hasConfigValue('apiKeySecret', context.config) &&
			hasConfigValue('accessToken', context.config) &&
			hasConfigValue('accessTokenSecret', context.config);

		if (!hasBearerToken && !hasOAuth) {
			errors.push({ field: 'bearerToken', message: 'At least a Bearer Token (for reading) or OAuth credentials (for posting) are required', level: 'runtime' });
		}

		return errors;
	},
};
