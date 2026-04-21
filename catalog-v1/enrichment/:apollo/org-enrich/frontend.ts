import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Building } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const ApolloOrgEnrichNode: NodeTemplate = {
	type: 'ApolloOrgEnrich',
	label: 'Apollo Org Enrich',
	description: 'Enrich an organization by domain with full company data from Apollo.io. Returns industry, employee count, description, revenue, funding, location. Consumes credits.',
	icon: Building,
	color: '#6366f1',
	category: 'Data',
	tags: ['apollo', 'enrich', 'organization', 'company', 'industry', 'revenue'],
	fields: [],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: false, description: 'Connect ApolloConfig.config', configurable: false },
		{ name: 'domain', portType: 'String', required: true, description: 'Company domain (e.g. "apollo.io"), use ApolloOrgSearch domains output' },
	],
	defaultOutputs: [
		{ name: 'name', portType: 'String', required: false, description: 'Company name' },
		{ name: 'industry', portType: 'String', required: false, description: 'Industry (e.g. "information technology & services")' },
		{ name: 'shortDescription', portType: 'String', required: false, description: 'Company description paragraph' },
		{ name: 'estimatedEmployees', portType: 'Number', required: false, description: 'Estimated employee count' },
		{ name: 'annualRevenue', portType: 'Number', required: false, description: 'Annual revenue in USD (0 if unknown)' },
		{ name: 'city', portType: 'String', required: false, description: 'HQ city' },
		{ name: 'state', portType: 'String', required: false, description: 'HQ state/region' },
		{ name: 'country', portType: 'String', required: false, description: 'HQ country' },
		{ name: 'keywords', portType: 'String', required: false, description: 'Comma-separated keywords describing what the company does' },
		{ name: 'latestFundingStage', portType: 'String', required: false, description: 'Latest funding round type (e.g. "Series D")' },
		{ name: 'totalFunding', portType: 'Number', required: false, description: 'Total funding raised in USD (0 if unknown)' },
		{ name: 'linkedinUrl', portType: 'String', required: false, description: 'LinkedIn company page URL' },
		{ name: 'websiteUrl', portType: 'String', required: false, description: 'Full website URL' },
		{ name: 'rawOrganization', portType: 'JsonDict', required: false, description: 'Raw Apollo API response, use Unpack to access fields not exposed above' },
	],
	features: {
	},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];


		const connectedConfigType = getConnectedNodeType('config', context);
		if (connectedConfigType && connectedConfigType !== 'ApolloConfig') {
			errors.push({ port: 'config', message: `Config should be connected to a ApolloConfig node, not ${connectedConfigType}`, level: 'structural' });
		}
		if (!isInputConnected('domain', context)) {
			errors.push({ port: 'domain', message: 'Domain is required, connect from ApolloOrgSearch domains output or provide directly', level: 'structural' });
		}

		return errors;
	},
	setupGuide: [
		'Provide a company domain (e.g. "apollo.io") to get full organization details',
		'Use with ApolloOrgSearch: search → get domains → enrich each one',
		'This endpoint consumes Apollo credits (1 per call)',
	],
};
