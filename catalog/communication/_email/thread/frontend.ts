import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Mail } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const emailThreadNode: NodeTemplate = {
	type: 'EmailThread',
	label: 'Email Thread',
	description: 'Fetch the full email thread history from IMAP using message references',
	icon: Mail,
	color: '#2563eb',
	category: 'Data',
	tags: ['email', 'thread', 'history', 'imap', 'conversation', 'mail'],
	fields: [],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: true, description: 'Email config from EmailConfig node', configurable: false },
		{ name: 'threadId', portType: 'String', required: true, description: 'Thread ID (first message ID in the chain) to search for' },
		{ name: 'messageId', portType: 'String', required: false, description: 'Current message ID (excluded from results)' },
	],
	defaultOutputs: [
		{ name: 'senders', portType: 'List[String]', required: false, description: 'Sender addresses, oldest message first (e.g. "John <john@co.com>")' },
		{ name: 'subjects', portType: 'List[String]', required: false, description: 'Email subjects, oldest first' },
		{ name: 'bodies', portType: 'List[String]', required: false, description: 'Plain-text email bodies, oldest first' },
		{ name: 'dates', portType: 'List[String]', required: false, description: 'ISO 8601 timestamps, oldest first' },
		{ name: 'count', portType: 'Number', required: false, description: 'Number of messages in the thread' },
	],
	features: {
	},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];

		if (!isInputConnected('config', context)) {
			errors.push({ port: 'config', message: 'Email Config is required - connect an EmailConfig node', level: 'structural' });
		} else {
			const connectedType = getConnectedNodeType('config', context);
			if (connectedType && connectedType !== 'EmailConfig') {
				errors.push({ port: 'config', message: `Config should be connected to an EmailConfig node, not ${connectedType}`, level: 'structural' });
			}
		}
		if (!isInputConnected('threadId', context)) {
			errors.push({ port: 'threadId', message: 'Thread ID input is required', level: 'structural' });
		}

		return errors;
	},
};
