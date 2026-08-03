import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { UserX } from '@lucide/svelte';
import { isInputConnected } from '$lib/validation';

export const whatsappGroupKickNode: NodeTemplate = {
	type: 'WhatsAppGroupKick',
	label: 'WhatsApp Group Kick',
	description: 'Remove participants from a WhatsApp group',
	icon: UserX,
	color: '#25D366',
	category: 'Utility',
	tags: ['whatsapp', 'group', 'kick', 'remove', 'moderation'],
	fields: [],
	defaultInputs: [
		{ name: 'endpointUrl', portType: 'String', required: true, description: 'Bridge endpoint URL from WhatsAppBridge node' },
		{ name: 'groupId', portType: 'String', required: true, description: 'Group JID' },
		{ name: 'participants', portType: 'List[String]', required: true, description: 'List of participant JIDs to remove' },
	],
	defaultOutputs: [
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the kick succeeded' },
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
