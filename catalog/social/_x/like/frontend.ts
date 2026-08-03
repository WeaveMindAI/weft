import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { ThumbsUp } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const xLikeNode: NodeTemplate = {
	type: 'XLike',
	label: 'X Like',
	description: 'Like a post on X/Twitter',
	icon: ThumbsUp,
	color: '#000000',
	category: 'Utility',
	tags: ['x', 'twitter', 'like', 'favorite'],
	fields: [],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: true, description: 'X config from XConfig node', configurable: false },
		{ name: 'authenticatedUserId', portType: 'String', required: true, description: 'Your X user ID (the account doing the liking)' },
		{ name: 'tweetId', portType: 'String', required: true, description: 'Tweet ID to like' },
	],
	defaultOutputs: [
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the like succeeded' },
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
