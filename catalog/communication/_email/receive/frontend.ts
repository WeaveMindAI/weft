import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Mail } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const emailReceiveNode: NodeTemplate = {
	type: 'EmailReceive',
	label: 'Email Receive',
	description: 'Polls an IMAP inbox for new emails and triggers on arrival',
	icon: Mail,
	color: '#2563eb',
	category: 'Triggers',
	tags: ['email', 'receive', 'imap', 'trigger', 'inbox', 'mail'],
	fields: [
		{ key: 'mailbox', label: 'Mailbox', type: 'text', placeholder: 'INBOX' },
		{ key: 'pollIntervalSecs', label: 'Poll Interval (seconds)', type: 'number', defaultValue: 60 },
	],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: true, description: 'Email config from EmailConfig node', configurable: false },
	],
	defaultOutputs: [
		{ name: 'from', portType: 'String', required: false, description: "Sender (e.g. 'Name <email@example.com>')" },
		{ name: 'to', portType: 'List[String]', required: false, description: 'Recipient email addresses' },
		{ name: 'subject', portType: 'String', required: false, description: 'Email subject line' },
		{ name: 'body', portType: 'String', required: false, description: 'Plain text body' },
		{ name: 'htmlBody', portType: 'String', required: false, description: 'HTML body (if available)' },
		{ name: 'cc', portType: 'List[String]', required: false, description: 'CC recipient email addresses' },
		{ name: 'bcc', portType: 'List[String]', required: false, description: 'BCC recipient email addresses' },
		{ name: 'replyTo', portType: 'String', required: false, description: 'Reply-To address' },
		{ name: 'date', portType: 'String', required: false, description: 'Date received (RFC 3339)' },
		{ name: 'messageId', portType: 'String', required: false, description: 'Unique message ID' },
		{ name: 'threadId', portType: 'String', required: false, description: 'Thread root message ID (for threading)' },
		{ name: 'inReplyTo', portType: 'String', required: false, description: 'Message ID this email replies to' },
		{ name: 'references', portType: 'List[String]', required: false, description: 'Full chain of message IDs in the thread' },
		{ name: 'hasAttachments', portType: 'Boolean', required: false, description: 'Whether the email has attachments' },
		{ name: 'attachmentCount', portType: 'Number', required: false, description: 'Number of attachments' },
	],
	features: {
		isTrigger: true,
		triggerCategory: 'Polling',
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

		return errors;
	},
};
