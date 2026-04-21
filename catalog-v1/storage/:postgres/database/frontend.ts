import type { NodeTemplate } from '$lib/types';
import { Database } from '@lucide/svelte';

export const PostgresDatabaseNode: NodeTemplate = {
	type: 'PostgresDatabase',
	label: 'Postgres Database',
	description: 'Infrastructure node that provides durable key-value storage backed by PostgreSQL. Memory Store and Memory Query nodes use this to persist data across project executions.',
	icon: Database,
	color: '#2563eb',
	category: 'Infrastructure',
	tags: ['infrastructure', 'database', 'storage', 'persistent', 'kv', 'memory', 'key-value', 'postgres'],
	fields: [],
	defaultInputs: [],
	defaultOutputs: [
		{ name: 'instanceId', portType: 'String', required: false, description: 'Instance ID for referencing this infrastructure instance' },
		{ name: 'endpointUrl', portType: 'String', required: false, description: 'Sidecar action endpoint URL, connect to Memory Store / Memory Query / Memory Delete nodes' },
	],
	features: {
		isInfrastructure: true,
	},
};
