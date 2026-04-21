import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { UserPlus } from '@lucide/svelte';
import { isInputConnected } from '$lib/validation';

export const whatsappGroupAddNode: NodeTemplate = {
	type: 'WhatsAppGroupAdd',
	label: 'WhatsApp Group Add',
	description: 'Add participants to a WhatsApp group',
	icon: UserPlus,
	color: '#25D366',
	category: 'Utility',
	tags: ['whatsapp', 'group', 'add', 'invite', 'participants'],
	fields: [],
	defaultInputs: [
		{ name: 'endpointUrl', portType: 'String', required: true, description: 'Bridge endpoint URL from WhatsAppBridge node' },
		{ name: 'groupId', portType: 'String', required: true, description: 'Group JID' },
		{ name: 'participants', portType: 'List[String]', required: true, description: 'List of participant JIDs to add' },
	],
	defaultOutputs: [
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether participants were added' },
	],
	features: {},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];
		if (!isInputConnected('endpointUrl', context)) errors.push({ port: 'endpointUrl', message: 'Connect a WhatsAppBridge node', level: 'structural' });
		if (!isInputConnected('groupId', context)) errors.push({ port: 'groupId', message: 'Group ID is required', level: 'structural' });
		if (!isInputConnected('participants', context)) errors.push({ port: 'participants', message: 'Participants list is required', level: 'structural' });
		return errors;
	},
};
