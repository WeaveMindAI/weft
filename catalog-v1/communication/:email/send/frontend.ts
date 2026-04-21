import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Mail } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const emailSendNode: NodeTemplate = {
	type: 'EmailSend',
	label: 'Email Send',
	description: 'Send an email via SMTP (supports file attachments)',
	icon: Mail,
	color: '#2563eb',
	category: 'Utility',
	tags: ['email', 'send', 'smtp', 'message', 'output', 'mail'],
	fields: [
		{ key: 'fromEmail', label: 'From Email (optional)', type: 'text', placeholder: 'you@gmail.com', description: 'Defaults to username if empty' },
		{ key: 'html', label: 'Send as HTML', type: 'checkbox' },
	],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: true, description: 'Email config from EmailConfig node', configurable: false },
		{ name: 'to', portType: 'List[String]', required: true, description: 'Recipient email addresses' },
		{ name: 'subject', portType: 'String', required: true, description: 'Email subject line' },
		{ name: 'body', portType: 'String', required: true, description: 'Email body (plain text or HTML)' },
		{ name: 'cc', portType: 'List[String]', required: false, description: 'CC recipient email addresses' },
		{ name: 'bcc', portType: 'List[String]', required: false, description: 'BCC recipient email addresses' },
		{ name: 'replyTo', portType: 'String', required: false, description: 'Reply-to email address' },
		{ name: 'inReplyTo', portType: 'String', required: false, description: 'Message ID to reply to (for threading)' },
		{ name: 'references', portType: 'List[String]', required: false, description: 'Chain of message IDs in the thread (for threading)' },
		{ name: 'media', portType: 'Media', required: false, description: 'Attachment from Image/Video/Audio/Document node' },
	],
	defaultOutputs: [
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the email was sent successfully' },
		{ name: 'messageId', portType: 'String', required: false, description: 'SMTP message ID if available' },
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
		if (!isInputConnected('to', context)) {
			errors.push({ port: 'to', message: 'Recipient email address is required', level: 'structural' });
		}
		if (!isInputConnected('subject', context)) {
			errors.push({ port: 'subject', message: 'Email subject is required', level: 'structural' });
		}
		if (!isInputConnected('body', context)) {
			errors.push({ port: 'body', message: 'Email body is required', level: 'structural' });
		}

		return errors;
	},
};
