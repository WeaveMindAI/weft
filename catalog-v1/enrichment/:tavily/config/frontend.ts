import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { KeyRound } from '@lucide/svelte';
import { isApiKeyReady } from '$lib/validation';

export const TavilyConfigNode: NodeTemplate = {
	type: 'TavilyConfig',
	label: 'Tavily Config',
	description: 'Tavily web search API credentials. Connect its config output to TavilySearch.',
	icon: KeyRound,
	color: '#6366f1',
	category: 'AI',
	tags: ['tavily', 'config', 'credentials', 'api', 'search', 'web'],
	fields: [
		{ key: 'apiKey', label: 'API Key', type: 'api_key', provider: 'tavily' },
	],
	defaultInputs: [],
	defaultOutputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: false, description: 'Tavily configuration object (apiKey)' },
	],
	setupGuide: [
		'Get your Tavily API key from https://tavily.com (free tier available)',
		'Connect this node\'s config output to TavilySearch',
	],
	features: {
	},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];
		if (!isApiKeyReady('apiKey', context.config)) {
			errors.push({ field: 'apiKey', message: 'Own key selected but not entered', level: 'runtime' });
		}
		return errors;
	},
};
