import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { HardDriveDownload } from '@lucide/svelte';
import { isInputConnected } from '$lib/validation';

export const MemoryStoreNode: NodeTemplate = {
	type: 'MemoryStore',
	label: 'Memory Store',
	description: 'Store a key-value pair in a Key-Value Store. Connect the endpointUrl output of a Postgres Database node to specify which store to write to.',
	icon: HardDriveDownload,
	color: '#2563eb',
	category: 'Infrastructure',
	tags: ['infrastructure', 'memory', 'store', 'write', 'save', 'database', 'kv'],
	fields: [],
	defaultInputs: [
		{ name: 'endpointUrl', portType: 'String', required: true, description: 'Sidecar endpoint URL (connect from a Postgres Database node endpointUrl output)' },
		{ name: 'key', portType: 'String', required: true, description: 'Key to store the value under' },
		{ name: 'value', portType: 'T', required: true, description: 'Value to store' },
	],
	defaultOutputs: [
		{ name: 'stored', portType: 'Boolean', required: false, description: 'True if the value was stored successfully' },
		{ name: 'key', portType: 'String', required: false, description: 'The key the value was stored under' },
	],
	features: {},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];

		if (!isInputConnected('endpointUrl', context)) {
			errors.push({ port: 'endpointUrl', message: 'Connect a Postgres Database endpointUrl output to specify which store to write to', level: 'structural' });
		}
		if (!isInputConnected('key', context)) {
			errors.push({ port: 'key', message: 'Key input is required', level: 'structural' });
		}
		if (!isInputConnected('value', context)) {
			errors.push({ port: 'value', message: 'Value input is required', level: 'structural' });
		}

		return errors;
	},
};
