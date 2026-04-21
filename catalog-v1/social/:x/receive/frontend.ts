import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { AtSign } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const xReceiveNode: NodeTemplate = {
	type: 'XReceive',
	label: 'X Receive',
	description: 'Triggers on new X (Twitter) posts matching a search query',
	icon: AtSign,
	color: '#000000',
	category: 'Triggers',
	tags: ['trigger', 'x', 'twitter', 'posts', 'mentions', 'polling'],
	fields: [
		{ key: 'query', label: 'Search Query', type: 'text', placeholder: 'e.g. @mybot, #mytag, from:someone' },
		{ key: 'pollIntervalSecs', label: 'Poll Interval (seconds)', type: 'number', placeholder: '30' },
	],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: true, description: 'X config from XConfig node', configurable: false },
	],
	defaultOutputs: [
		{ name: 'text', portType: 'String', required: false, description: 'Post text content' },
		{ name: 'authorUsername', portType: 'String', required: false, description: 'Username of the post author' },
		{ name: 'authorName', portType: 'String', required: false, description: 'Display name of the post author' },
		{ name: 'authorId', portType: 'String', required: false, description: 'User ID of the post author' },
		{ name: 'postId', portType: 'String', required: false, description: 'Unique post ID' },
		{ name: 'conversationId', portType: 'String', required: false, description: 'Conversation thread ID' },
		{ name: 'createdAt', portType: 'String', required: false, description: 'Post creation timestamp (ISO 8601)' },
		{ name: 'isReply', portType: 'Boolean', required: false, description: 'Whether the post is a reply' },
		{ name: 'isRetweet', portType: 'Boolean', required: false, description: 'Whether the post is a retweet' },
	],
	features: {
		isTrigger: true,
		triggerCategory: 'Polling',
	},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];

		if (!isInputConnected('config', context)) {
			errors.push({ port: 'config', message: 'X Config is required - connect an XConfig node', level: 'structural' });
		} else {
			const connectedType = getConnectedNodeType('config', context);
			if (connectedType && connectedType !== 'XConfig') {
				errors.push({ port: 'config', message: `Config should be connected to an XConfig node, not ${connectedType}`, level: 'structural' });
			}
		}

		return errors;
	},
};
