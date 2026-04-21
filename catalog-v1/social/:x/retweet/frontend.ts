import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Repeat } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const xRetweetNode: NodeTemplate = {
	type: 'XRetweet',
	label: 'X Retweet',
	description: 'Retweet a post on X/Twitter',
	icon: Repeat,
	color: '#000000',
	category: 'Utility',
	tags: ['x', 'twitter', 'retweet', 'repost'],
	fields: [],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: true, description: 'X config from XConfig node', configurable: false },
		{ name: 'authenticatedUserId', portType: 'String', required: true, description: 'Your X user ID (the account doing the retweeting)' },
		{ name: 'tweetId', portType: 'String', required: true, description: 'Tweet ID to retweet' },
	],
	defaultOutputs: [
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the retweet succeeded' },
	],
	features: {},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];
		if (!isInputConnected('config', context)) {
			errors.push({ port: 'config', message: 'X Config is required', level: 'structural' });
		} else {
			const t = getConnectedNodeType('config', context);
			if (t && t !== 'XConfig') errors.push({ port: 'config', message: `Expected XConfig, got ${t}`, level: 'structural' });
		}
		if (!isInputConnected('authenticatedUserId', context)) errors.push({ port: 'authenticatedUserId', message: 'Authenticated User ID is required', level: 'structural' });
		if (!isInputConnected('tweetId', context)) errors.push({ port: 'tweetId', message: 'Tweet ID is required', level: 'structural' });
		return errors;
	},
};
