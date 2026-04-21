import type { NodeTemplate, NodeInstance, ValidationContext, ValidationError, DisplayDataContext } from '$lib/types';
import { Webhook } from '@lucide/svelte';

export const ApiPostNode: NodeTemplate = {
	type: 'ApiPost',
	label: 'API Endpoint (POST)',
	description: 'Triggers when an HTTP POST request is received. Add output ports to define the expected JSON body schema.',
	icon: Webhook,
	isBase: true,
	color: '#6366f1',
	category: 'Triggers',
	tags: ['trigger', 'api', 'webhook', 'http', 'post', 'endpoint'],
	fields: [
		{ key: 'apiKey', label: 'API Key (optional)', type: 'password', placeholder: 'Leave empty for no authentication' },
	],
	defaultInputs: [],
	defaultOutputs: [
		{ name: 'receivedAt', portType: 'String', required: false, description: 'Timestamp when the request was received (RFC 3339)' },
	],
	features: {
		isTrigger: true,
		triggerCategory: 'Webhook',
		canAddOutputPorts: true,
	},
	validate: (_context: ValidationContext): ValidationError[] => {
		return [];
	},
	getDisplayData: (node, context) => {
		if (!context.isProjectActive) return [];
		const triggerId = `${context.projectId}-${node.id}`;
		const url = `${context.apiBaseUrl}/api/v1/webhooks/${triggerId}`;
		return [
			{ type: 'text', label: 'Endpoint URL', data: url },
		];
	},
};
