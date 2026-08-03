import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { UserSearch } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const ApolloEnrichNode: NodeTemplate = {
	type: 'ApolloEnrich',
	label: 'Apollo Enrich',
	description: 'Enrich a person with full profile data from Apollo.io. Returns email, LinkedIn, phone, employment history. Consumes credits.',
	icon: UserSearch,
	color: '#6366f1',
	category: 'Data',
	tags: ['apollo', 'enrich', 'people', 'email', 'linkedin', 'profile'],
	fields: [
		{ key: 'revealPersonalEmails', label: 'Reveal personal emails', type: 'checkbox', defaultValue: false },
		{ key: 'revealPhoneNumber', label: 'Reveal phone number', type: 'checkbox', defaultValue: false },
	],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: false, description: 'Connect ApolloConfig.config', configurable: false },
		{ name: 'id', portType: 'String', required: false, description: 'Apollo person ID (from ApolloSearch)' },
		{ name: 'email', portType: 'String', required: false, description: 'Email address to match' },
		{ name: 'firstName', portType: 'String', required: false, description: 'First name (use with domain)' },
		{ name: 'lastName', portType: 'String', required: false, description: 'Last name (use with domain)' },
		{ name: 'domain', portType: 'String', required: false, description: 'Company domain (use with name)' },
		{ name: 'linkedinUrl', portType: 'String', required: false, description: 'LinkedIn profile URL' },
	],
	defaultOutputs: [
		{ name: 'rawPerson', portType: 'JsonDict', required: false, description: 'Raw Apollo API response, use Unpack to access fields not exposed above' },
		{ name: 'name', portType: 'String', required: false, description: 'Full name' },
		{ name: 'email', portType: 'String', required: false, description: 'Work email' },
		{ name: 'title', portType: 'String', required: false, description: 'Job title' },
		{ name: 'linkedinUrl', portType: 'String', required: false, description: 'LinkedIn profile URL' },
		{ name: 'organization', portType: 'String', required: false, description: 'Company name' },
		{ name: 'city', portType: 'String', required: false, description: 'City' },
		{ name: 'state', portType: 'String', required: false, description: 'State/region' },
		{ name: 'country', portType: 'String', required: false, description: 'Country' },
		{ name: 'headline', portType: 'String', required: false, description: 'LinkedIn headline' },
	],
	features: {
		oneOfRequired: [['id', 'email', 'linkedinUrl', 'firstName', 'lastName', 'domain']],
	},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];


		const connectedConfigType = getConnectedNodeType('config', context);
		if (connectedConfigType && connectedConfigType !== 'ApolloConfig') {
			errors.push({ port: 'config', message: `Config should be connected to a ApolloConfig node, not ${connectedConfigType}`, level: 'structural' });
		}
		const identifierPorts = ['id', 'email', 'linkedinUrl', 'firstName', 'lastName', 'domain'];
		const hasIdentifier = identifierPorts.some(p => isInputConnected(p, context));
		if (!hasIdentifier) {
			errors.push({ message: 'At least one identifier must be connected (id, email, linkedinUrl, or firstName+domain)', level: 'structural' });
		}

		return errors;
	},
	setupGuide: [
		'Provide at least one identifier: id, email, firstName+domain, or linkedinUrl',
		'Using an Apollo person ID (from ApolloSearch) gives the best match accuracy',
		'This endpoint consumes Apollo credits',
		'Enable "Reveal personal emails" or "Reveal phone number" for additional data (extra credits)',
	],
};
