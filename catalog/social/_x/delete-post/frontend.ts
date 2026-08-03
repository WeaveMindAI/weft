import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Trash2 } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const xDeletePostNode: NodeTemplate = {
	type: 'XDeletePost',
	label: 'X Delete Post',
	description: 'Delete a post on X/Twitter',
	icon: Trash2,
	color: '#000000',
	category: 'Utility',
	tags: ['x', 'twitter', 'delete', 'post', 'tweet'],
	fields: [],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: true, description: 'X config from XConfig node', configurable: false },
		{ name: 'tweetId', portType: 'String', required: true, description: 'Tweet ID to delete' },
	],
	defaultOutputs: [
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the deletion succeeded' },
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
		if (!isInputConnected('tweetId', context)) errors.push({ port: 'tweetId', message: 'Tweet ID is required', level: 'structural' });
		return errors;
	},
};
