import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { KeyRound } from '@lucide/svelte';
import { isApiKeyReady } from '$lib/validation';

export const ApolloConfigNode: NodeTemplate = {
	type: 'ApolloConfig',
	label: 'Apollo Config',
	description: 'Apollo.io API credentials. Connect its config output to any Apollo node.',
	icon: KeyRound,
	color: '#6366f1',
	category: 'Data',
	tags: ['apollo', 'config', 'credentials', 'api', 'prospecting', 'enrichment'],
	fields: [
		{ key: 'apiKey', label: 'API Key', type: 'api_key', provider: 'apollo' },
	],
	defaultInputs: [],
	defaultOutputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: false, description: 'Apollo configuration object (apiKey)' },
	],
	setupGuide: [
		'Get your Apollo API key from Settings > Integrations > API Keys at https://app.apollo.io',
		'A master API key is required for People Search and Enrichment',
		'Connect this node\'s config output to ApolloSearch, ApolloEnrich, ApolloOrgSearch, or ApolloOrgEnrich',
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
