import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Search } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const tavilySearchNode: NodeTemplate = {
	type: 'TavilySearch',
	label: 'Tavily Search',
	description: 'Search the internet using Tavily. Returns an AI-generated answer and source results with titles, URLs, and content snippets.',
	icon: Search,
	color: '#6366f1',
	category: 'AI',
	tags: ['search', 'web', 'internet', 'tavily', 'research'],
	fields: [
		{
			key: 'maxResults',
			label: 'Max Results',
			type: 'number',
			placeholder: '5',
			description: 'Maximum number of search results to return (1-20)',
		},
		{
			key: 'searchDepth',
			label: 'Search Depth',
			type: 'select',
			options: ['basic', 'advanced'],
			description: 'Advanced returns more relevant results but costs 2 credits instead of 1',
		},
		{
			key: 'topic',
			label: 'Topic',
			type: 'select',
			options: ['general', 'news'],
			description: 'News is optimized for recent events and current affairs',
		},
	],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: false, description: 'Connect TavilyConfig.config', configurable: false },
		{ name: 'query', portType: 'String', required: true, description: 'The search query to execute' },
	],
	defaultOutputs: [
		{ name: 'answer', portType: 'String', required: false, description: 'AI-generated summary answer' },
		{ name: 'titles', portType: 'List[String]', required: false, description: 'List of result page titles (same order as urls, contents, scores)' },
		{ name: 'urls', portType: 'List[String]', required: false, description: 'List of result page URLs' },
		{ name: 'contents', portType: 'List[String]', required: false, description: 'List of content snippets from each result page' },
		{ name: 'scores', portType: 'List[Number]', required: false, description: 'List of relevance scores (0-1) for each result' },
	],
	features: {
	},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];


		const connectedConfigType = getConnectedNodeType('config', context);
		if (connectedConfigType && connectedConfigType !== 'TavilyConfig') {
			errors.push({ port: 'config', message: `Config should be connected to a TavilyConfig node, not ${connectedConfigType}`, level: 'structural' });
		}
		if (!isInputConnected('query', context)) {
			errors.push({ port: 'query', message: 'Search query input is required', level: 'structural' });
		}

		return errors;
	},
};
