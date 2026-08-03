import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Trash2 } from '@lucide/svelte';
import { isInputConnected } from '$lib/validation';

export const MemoryDeleteNode: NodeTemplate = {
	type: 'MemoryDelete',
	label: 'Memory Delete',
	description: 'Delete keys from a Key-Value Store by regex pattern. Connect the endpointUrl output of a Postgres Database node. Use "^mykey$" to delete a single key, or "user_.*" to delete all matching keys.',
	icon: Trash2,
	color: '#2563eb',
	category: 'Infrastructure',
	tags: ['infrastructure', 'memory', 'delete', 'remove', 'database', 'kv', 'regex', 'pattern'],
	fields: [],
	defaultInputs: [
		{ name: 'endpointUrl', portType: 'String', required: true, description: 'Sidecar endpoint URL (connect from a Postgres Database node endpointUrl output)' },
		{ name: 'pattern', portType: 'String', required: true, description: 'Regex pattern to match keys to delete (e.g. "^mykey$" for exact match, "user_.*" for prefix match)' },
	],
	defaultOutputs: [
		{ name: 'deleted', portType: 'List[String]', required: false, description: 'List of deleted key names' },
		{ name: 'count', portType: 'Number', required: false, description: 'Number of keys deleted' },
	],
	features: {},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];

		if (!isInputConnected('endpointUrl', context)) {
			errors.push({ port: 'endpointUrl', message: 'Connect a Postgres Database endpointUrl output to specify which store to delete from', level: 'structural' });
		}
		if (!isInputConnected('pattern', context)) {
			errors.push({ port: 'pattern', message: 'Pattern input is required', level: 'structural' });
		}

		return errors;
	},
};
