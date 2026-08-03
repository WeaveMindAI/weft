import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { HardDrive } from '@lucide/svelte';
import { isInputConnected } from '$lib/validation';

export const MemoryQueryNode: NodeTemplate = {
	type: 'MemoryQuery',
	label: 'Memory Query',
	description: 'Query data from a Key-Value Store by regex pattern. Connect the endpointUrl output of a Postgres Database node. Use "^mykey$" for exact key lookup, "user_.*" for prefix match, or ".*" for all keys.',
	icon: HardDrive,
	color: '#2563eb',
	category: 'Infrastructure',
	tags: ['infrastructure', 'memory', 'query', 'read', 'get', 'database', 'kv', 'regex', 'search', 'pattern'],
	fields: [],
	defaultInputs: [
		{ name: 'endpointUrl', portType: 'String', required: true, description: 'Sidecar endpoint URL (connect from a Postgres Database node endpointUrl output)' },
		{ name: 'pattern', portType: 'String', required: true, description: 'Regex pattern to match against keys (e.g. "^mykey$" for exact match, "user_.*" for prefix, ".*" for all)' },
	],
	defaultOutputs: [
		{ name: 'value', portType: 'Dict[String, T]', required: false, description: 'Dict of matching key-value pairs' },
		{ name: 'found', portType: 'Boolean', required: false, description: 'Whether any matching keys were found' },
		{ name: 'count', portType: 'Number', required: false, description: 'Number of matching keys' },
		{ name: 'keys', portType: 'List[String]', required: false, description: 'List of matching key names' },
	],
	features: {},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];

		if (!isInputConnected('endpointUrl', context)) {
			errors.push({ port: 'endpointUrl', message: 'Connect a Postgres Database endpointUrl output to specify which store to read from', level: 'structural' });
		}
		if (!isInputConnected('pattern', context)) {
			errors.push({ port: 'pattern', message: 'Pattern input is required', level: 'structural' });
		}

		return errors;
	},
};
