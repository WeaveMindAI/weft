import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Building2 } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const ApolloOrgSearchNode: NodeTemplate = {
	type: 'ApolloOrgSearch',
	label: 'Apollo Org Search',
	description: 'Search Apollo.io for organizations matching filters. Returns name, domain, social links, phone, languages, founding year. Consumes credits.',
	icon: Building2,
	color: '#6366f1',
	category: 'Data',
	tags: ['apollo', 'search', 'organization', 'company', 'prospecting'],
	fields: [
		{ key: 'perPage', label: 'Max results', type: 'number', defaultValue: 10, min: 1, max: 100, description: 'Number of organizations to return (1-100)' },
		{ key: 'randomizePage', label: 'Randomize page', type: 'checkbox', defaultValue: false, description: 'Pick a random result page each run. Free if it overshoots, then retries on a valid page.' },
	],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: false, description: 'Connect ApolloConfig.config', configurable: false },
		{ name: 'organizationLocations', portType: 'List[String]', required: false, description: 'Company HQ locations (e.g. ["San Francisco, US"])' },
		{ name: 'employeeRanges', portType: 'List[String]', required: false, description: 'Employee count ranges, comma-separated bounds (e.g. ["1,10", "11,50", "51,200", "201,500", "501,1000", "1001,2000", "2001,5000", "5001,10000"])' },
		{ name: 'industries', portType: 'List[String]', required: false, description: 'Industry keyword tags (e.g. ["software", "information technology", "financial services", "mining"])' },
		{ name: 'keywords', portType: 'List[String]', required: false, description: 'Keyword tags to filter by (e.g. ["consulting", "sales strategy"])' },
		{ name: 'revenueMin', portType: 'Number', required: false, description: 'Minimum annual revenue (integer, no symbols)' },
		{ name: 'revenueMax', portType: 'Number', required: false, description: 'Maximum annual revenue (integer, no symbols)' },
		{ name: 'page', portType: 'Number', required: false, description: 'Page number (1-based). Overrides randomizePage when connected.' },
	],
	defaultOutputs: [
		{ name: 'ids', portType: 'List[String]', required: false, description: 'Apollo organization IDs (pass to ApolloSearch organizationIds to find people)' },
		{ name: 'names', portType: 'List[String]', required: false, description: 'Company names' },
		{ name: 'domains', portType: 'List[String]', required: false, description: 'Primary domains (e.g. "nikkei.com")' },
		{ name: 'websiteUrls', portType: 'List[String]', required: false, description: 'Full website URLs' },
		{ name: 'linkedinUrls', portType: 'List[String]', required: false, description: 'LinkedIn company page URLs' },
		{ name: 'twitterUrls', portType: 'List[String]', required: false, description: 'Twitter/X profile URLs' },
		{ name: 'facebookUrls', portType: 'List[String]', required: false, description: 'Facebook page URLs' },
		{ name: 'phones', portType: 'List[String]', required: false, description: 'Company phone numbers' },
		{ name: 'foundedYears', portType: 'List[String]', required: false, description: 'Year founded (0 if unknown)' },
		{ name: 'languages', portType: 'List[String]', required: false, description: 'Comma-separated languages the company operates in' },
		{ name: 'totalEntries', portType: 'Number', required: false, description: 'Total matching organizations' },
		{ name: 'rawOrganizations', portType: 'List[JsonDict]', required: false, description: 'Raw Apollo API organization objects, use Unpack to access fields not exposed above' },
	],
	features: {
		oneOfRequired: [['organizationLocations', 'employeeRanges', 'industries', 'keywords', 'revenueMin', 'revenueMax']],
	},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];


		const connectedConfigType = getConnectedNodeType('config', context);
		if (connectedConfigType && connectedConfigType !== 'ApolloConfig') {
			errors.push({ port: 'config', message: `Config should be connected to a ApolloConfig node, not ${connectedConfigType}`, level: 'structural' });
		}
		const searchPorts = ['organizationLocations', 'employeeRanges', 'industries', 'keywords', 'revenueMin', 'revenueMax'];
		const hasAnyFilter = searchPorts.some(p => isInputConnected(p, context));
		if (!hasAnyFilter) {
			errors.push({ message: 'At least one search filter must be connected', level: 'structural' });
		}

		return errors;
	},
	setupGuide: [
		'Search for target companies, then use their IDs with ApolloSearch to find people',
		'This endpoint consumes Apollo credits',
	],
};
