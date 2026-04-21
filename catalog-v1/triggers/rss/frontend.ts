import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Rss } from '@lucide/svelte';
import { hasConfigValue } from '$lib/validation';

export const RssNode: NodeTemplate = {
	type: 'Rss',
	label: 'RSS Feed',
	description: 'Polls RSS/Atom feeds and triggers on new items',
	icon: Rss,
	isBase: true,
	color: '#c9873a',
	category: 'Triggers',
	tags: ['trigger', 'rss', 'feed', 'polling', 'news'],
	fields: [
		{ key: 'url', label: 'Feed URL', type: 'text', placeholder: 'https://example.com/feed.xml' },
		{ key: 'pollIntervalSecs', label: 'Poll Interval (seconds)', type: 'number', defaultValue: 300 },
	],
	defaultInputs: [],
	defaultOutputs: [
		{ name: 'title', portType: 'String', required: false, description: 'Entry title' },
		{ name: 'link', portType: 'String', required: false, description: 'Entry URL' },
		{ name: 'summary', portType: 'String', required: false, description: 'Entry summary or description' },
		{ name: 'content', portType: 'String', required: false, description: 'Full entry content (if available)' },
		{ name: 'published', portType: 'String', required: false, description: 'Publication date (RFC 3339)' },
		{ name: 'entryId', portType: 'String', required: false, description: 'Unique entry identifier' },
		{ name: 'author', portType: 'String', required: false, description: 'Entry author name' },
	],
	features: {
		isTrigger: true,
		triggerCategory: 'Polling',
	},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];
		
		if (!hasConfigValue('url', context.config)) {
			errors.push({ field: 'url', message: 'Feed URL is required', level: 'structural' });
		}
		
		return errors;
	},
};
