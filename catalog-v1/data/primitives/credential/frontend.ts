import type { NodeTemplate } from '$lib/types';
import { KeyRound } from '@lucide/svelte';

export const CredentialNode: NodeTemplate = {
	type: 'Credential',
	label: 'Credential',
	description: 'Sensitive value (API key, token, secret, password). Automatically stripped when sharing or exporting.',
	isBase: true,
	icon: KeyRound,
	color: '#b45309',
	category: 'Data',
	tags: ['credential', 'secret', 'key', 'api', 'token', 'password', 'sensitive'],
	fields: [
		{ key: 'value', label: 'Secret Value', type: 'password', placeholder: 'Enter API key, token, password, or other secret...' },
	],
	defaultInputs: [],
	defaultOutputs: [
		{ name: 'value', portType: 'String', required: false, description: 'Secret value' },
	],
	features: {
	},
};
