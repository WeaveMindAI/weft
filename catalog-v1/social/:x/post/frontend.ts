import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { AtSign } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const xPostNode: NodeTemplate = {
	type: 'XPost',
	label: 'X Post',
	description: 'Post a message or media to X (Twitter)',
	icon: AtSign,
	color: '#000000',
	category: 'Utility',
	tags: ['x', 'twitter', 'post', 'tweet', 'send', 'output'],
	fields: [],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: true, description: 'X config from XConfig node', configurable: false },
		{ name: 'text', portType: 'String', required: false, description: 'Post text content (max 280 characters, optional if media provided)' },
		{ name: 'replyToPostId', portType: 'String', required: false, description: 'Post ID to reply to (optional)' },
		{ name: 'media', portType: 'Media', required: false, description: 'Media object from Image/Video/Audio/Document node' },
	],
	defaultOutputs: [
		{ name: 'postId', portType: 'String', required: false, description: 'ID of the created post' },
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the post was created successfully' },
	],
	features: {
		oneOfRequired: [['text', 'media']],
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
		if (!isInputConnected('text', context) && !isInputConnected('media', context)) {
			errors.push({ port: 'text', message: 'Either text or media is required', level: 'structural' });
		}

		return errors;
	},
};
