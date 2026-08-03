import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Smartphone } from '@lucide/svelte';
import { isInputConnected } from '$lib/validation';

export const whatsappCreateGroupNode: NodeTemplate = {
	type: 'WhatsAppCreateGroup',
	label: 'WhatsApp Create Group',
	description: 'Create a new WhatsApp group',
	icon: Smartphone,
	color: '#25D366',
	category: 'Utility',
	tags: ['whatsapp', 'group', 'create'],
	fields: [],
	defaultInputs: [
		{ name: 'endpointUrl', portType: 'String', required: true, description: 'Bridge endpoint URL from WhatsAppBridge node' },
		{ name: 'name', portType: 'String', required: true, description: 'Group name' },
		{ name: 'participants', portType: 'List[String]', required: true, description: 'List of participant JIDs' },
	],
	defaultOutputs: [
		{ name: 'groupId', portType: 'String', required: false, description: 'ID of the created group' },
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the group was created' },
	],
	features: {},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];
		if (!isInputConnected('endpointUrl', context)) errors.push({ port: 'endpointUrl', message: 'Connect a WhatsAppBridge node', level: 'structural' });
		if (!isInputConnected('name', context)) errors.push({ port: 'name', message: 'Group name is required', level: 'structural' });
		if (!isInputConnected('participants', context)) errors.push({ port: 'participants', message: 'Participants list is required', level: 'structural' });
		return errors;
	},
};
