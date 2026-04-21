import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Shield } from '@lucide/svelte';
import { isInputConnected } from '$lib/validation';

export const whatsappGroupAdminNode: NodeTemplate = {
	type: 'WhatsAppGroupAdmin',
	label: 'WhatsApp Group Admin',
	description: 'Promote or demote participants as group admins in WhatsApp',
	icon: Shield,
	color: '#25D366',
	category: 'Utility',
	tags: ['whatsapp', 'group', 'admin', 'promote', 'demote', 'moderation'],
	fields: [],
	defaultInputs: [
		{ name: 'endpointUrl', portType: 'String', required: true, description: 'Bridge endpoint URL from WhatsAppBridge node' },
		{ name: 'groupId', portType: 'String', required: true, description: 'Group JID' },
		{ name: 'participants', portType: 'List[String]', required: true, description: 'List of participant JIDs' },
		{ name: 'promote', portType: 'Boolean', required: true, description: 'true = promote to admin, false = demote from admin' },
	],
	defaultOutputs: [
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the operation succeeded' },
	],
	features: {},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];
		if (!isInputConnected('endpointUrl', context)) errors.push({ port: 'endpointUrl', message: 'Connect a WhatsAppBridge node', level: 'structural' });
		if (!isInputConnected('groupId', context)) errors.push({ port: 'groupId', message: 'Group ID is required', level: 'structural' });
		if (!isInputConnected('participants', context)) errors.push({ port: 'participants', message: 'Participants list is required', level: 'structural' });
		if (!isInputConnected('promote', context)) errors.push({ port: 'promote', message: 'Promote/demote flag is required (true = promote, false = demote)', level: 'structural' });
		return errors;
	},
};
