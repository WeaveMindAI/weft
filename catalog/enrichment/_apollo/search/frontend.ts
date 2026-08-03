import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Search } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const ApolloSearchNode: NodeTemplate = {
	type: 'ApolloSearch',
	label: 'Apollo Search',
	description: 'Search Apollo.io for people matching filters. Free (no credits). Returns lightweight records without emails.',
	icon: Search,
	color: '#6366f1',
	category: 'Data',
	tags: ['apollo', 'search', 'people', 'prospecting', 'outreach', 'leads'],
	fields: [
		{ key: 'perPage', label: 'Max results', type: 'number', defaultValue: 10, min: 1, max: 100, description: 'Number of people to return (1-100)' },
		{ key: 'requireEmail', label: 'Require email', type: 'checkbox', defaultValue: true, description: 'Only return people with a verified or likely email address' },
		{ key: 'randomizePage', label: 'Randomize page', type: 'checkbox', defaultValue: false, description: 'Pick a random result page each run. Free: people search costs no credits.' },
	],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: false, description: 'Connect ApolloConfig.config', configurable: false },
		{ name: 'personTitles', portType: 'List[String]', required: false, description: 'Job titles to search for (e.g. ["CTO", "VP Engineering"])' },
		{ name: 'personSeniorities', portType: 'List[String]', required: false, description: 'Seniority levels (e.g. ["owner", "founder", "c_suite", "vp", "director"])' },
		{ name: 'personLocations', portType: 'List[String]', required: false, description: 'Person locations (e.g. ["California, US"])' },
		{ name: 'organizationLocations', portType: 'List[String]', required: false, description: 'Company HQ locations' },
		{ name: 'employeeRanges', portType: 'List[String]', required: false, description: 'Employee count ranges, comma-separated bounds (e.g. ["1,10", "11,50", "51,200", "201,500", "501,1000", "1001,2000", "2001,5000", "5001,10000"])' },
		{ name: 'keywords', portType: 'List[String]', required: false, description: 'Keyword search terms' },
		{ name: 'industries', portType: 'List[String]', required: false, description: 'Industry keyword tags (e.g. ["software", "information technology", "financial services", "mining"])' },
		{ name: 'organizationIds', portType: 'List[String]', required: false, description: 'Specific Apollo organization IDs' },
		{ name: 'page', portType: 'Number', required: false, description: 'Page number (1-based). Overrides randomizePage when connected.' },
	],
	defaultOutputs: [
		{ name: 'ids', portType: 'List[String]', required: false, description: 'Apollo person IDs (pass to ApolloEnrich to get full profiles)' },
		{ name: 'firstNames', portType: 'List[String]', required: false, description: 'First names' },
		{ name: 'lastNames', portType: 'List[String | Null]', required: false, description: 'Last names. Obfuscated on the free endpoint (e.g. "S." for "Smith"). Null when missing.' },
		{ name: 'titles', portType: 'List[String]', required: false, description: 'Job titles (may be empty if unknown)' },
		{ name: 'companyNames', portType: 'List[String]', required: false, description: 'Current employer names' },
		{ name: 'linkedinUrls', portType: 'List[String | Null]', required: false, description: 'LinkedIn profile URLs. Often null on the free endpoint, use ApolloEnrich for reliable URLs.' },
		{ name: 'hasEmail', portType: 'List[Boolean]', required: false, description: 'Whether Apollo has a verified email (boolean)' },
		{ name: 'totalEntries', portType: 'Number', required: false, description: 'Total matching people' },
		{ name: 'rawPeople', portType: 'List[JsonDict]', required: false, description: 'Raw Apollo API person objects, use Unpack to access fields not exposed above' },
	],
	features: {
		oneOfRequired: [['personTitles', 'personSeniorities', 'personLocations', 'organizationLocations', 'employeeRanges', 'keywords', 'industries', 'organizationIds']],
	},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];


		const connectedConfigType = getConnectedNodeType('config', context);
		if (connectedConfigType && connectedConfigType !== 'ApolloConfig') {
			errors.push({ port: 'config', message: `Config should be connected to a ApolloConfig node, not ${connectedConfigType}`, level: 'structural' });
		}
		const searchPorts = ['personTitles', 'personSeniorities', 'personLocations', 'organizationLocations', 'employeeRanges', 'keywords', 'industries', 'organizationIds'];
		const hasAnyFilter = searchPorts.some(p => isInputConnected(p, context));
		if (!hasAnyFilter) {
			errors.push({ message: 'At least one search filter must be connected', level: 'structural' });
		}

		return errors;
	},
	setupGuide: [
		'Get your Apollo API key from Settings > Integrations > API Keys',
		'This endpoint requires a master API key',
		'People Search is free and does not consume credits',
		'Use ApolloEnrich to get full profiles with emails',
	],
};
