import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Mail } from '@lucide/svelte';
import { hasConfigValue } from '$lib/validation';

export const emailConfigNode: NodeTemplate = {
	type: 'EmailConfig',
	label: 'Email Config',
	description: 'Email provider credentials and server settings (IMAP/SMTP)',
	icon: Mail,
	color: '#2563eb',
	category: 'Data',
	tags: ['email', 'config', 'imap', 'smtp', 'credentials', 'mail'],
	fields: [
		{ key: 'protocol', label: 'Protocol', type: 'select', options: ['imap', 'smtp'], defaultValue: 'imap' },
		{ key: 'host', label: 'Host', type: 'text', placeholder: 'imap.gmail.com' },
		{ key: 'port', label: 'Port', type: 'text', placeholder: '993' },
		{ key: 'security', label: 'Security', type: 'select', options: ['tls', 'starttls', 'none'], defaultValue: 'tls' },
		{ key: 'username', label: 'Username / Email', type: 'text', placeholder: 'you@gmail.com' },
		{ key: 'password', label: 'Password / App Password', type: 'password' },
		{ key: 'mailbox', label: 'Mailbox (IMAP only)', type: 'text', placeholder: 'INBOX' },
		{ key: 'tlsAcceptInvalid', label: 'Accept invalid TLS certificates', type: 'select', options: ['false', 'true'], defaultValue: 'false' },
	],
	defaultInputs: [],
	defaultOutputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: false, description: 'Email configuration object' },
	],
	setupGuide: [
		'This node stores your email server credentials. Connect its "config" output to any Email node.',
		'',
		'--- Gmail ---',
		'Gmail requires an App Password, not your regular Google password.',
		'1. Go to myaccount.google.com/apppasswords',
		'2. Enable 2-Step Verification if not already on',
		'3. Create an app password (select "Mail" or "Other")',
		'4. Use the 16-character password Google gives you',
		'IMAP: imap.gmail.com, port 993 (SSL)',
		'SMTP: smtp.gmail.com, port 587 (TLS)',
		'',
		'--- Outlook / Hotmail ---',
		'IMAP: imap-mail.outlook.com, port 993',
		'SMTP: smtp-mail.outlook.com, port 587',
		'Use your regular Microsoft account password.',
		'',
		'--- Yahoo Mail ---',
		'Requires an App Password (similar to Gmail).',
		'Go to login.yahoo.com > Account Security > Generate app password',
		'IMAP: imap.mail.yahoo.com, port 993',
		'SMTP: smtp.mail.yahoo.com, port 587',
		'',
		'--- iCloud Mail ---',
		'Requires an App-Specific Password.',
		'Go to appleid.apple.com > Sign-In and Security > App-Specific Passwords',
		'IMAP: imap.mail.me.com, port 993',
		'SMTP: smtp.mail.me.com, port 587',
		'',
		'--- Other providers (Proton, Fastmail, self-hosted, etc.) ---',
		'Search for "[your provider] IMAP SMTP settings" to find the host, port, and auth method.',
		'Some providers (like ProtonMail) require a Bridge app for IMAP/SMTP access.',
		'',
		'--- Security modes ---',
		'TLS (default): Direct encrypted connection. Use for port 993 (IMAP) or 465 (SMTP).',
		'STARTTLS: Starts plain, upgrades to encrypted. Use for port 143 (IMAP) or 587 (SMTP).',
		'None: No encryption (not recommended). Use only for trusted local/private servers.',
		'"Accept invalid TLS certificates" can be enabled for self-hosted servers with self-signed certs.',
	],
	features: {
	},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];

		if (!hasConfigValue('host', context.config)) {
			errors.push({ field: 'host', message: 'Host is required', level: 'structural' });
		}
		if (!hasConfigValue('username', context.config)) {
			errors.push({ field: 'username', message: 'Username is required', level: 'runtime' });
		}
		if (!hasConfigValue('password', context.config)) {
			errors.push({ field: 'password', message: 'Password is required', level: 'runtime' });
		}

		return errors;
	},
};
