import type { NodeTemplate, PortDefinition, ValidationContext, ValidationError } from '$lib/types';
import { PackageOpen } from '@lucide/svelte';
import { isInputConnected } from '$lib/validation';

export const UnpackNode: NodeTemplate = {
	type: 'Unpack',
	label: 'Unpack',
	description: 'Extracts fields from a Dict into individual outputs. Add output ports for each field to extract.',
	icon: PackageOpen,
	isBase: true,
	color: '#8b5cf6',
	category: 'Utility',
	tags: ['unpack', 'extract', 'split', 'dict', 'object'],
	fields: [],
	defaultInputs: [
		{ name: 'in', portType: 'Dict[String, T]', required: true, description: 'Dict to unpack' },
	],
	defaultOutputs: [],
	features: {
		canAddOutputPorts: true,
	},
	resolveTypes: (inputs: PortDefinition[], outputs: PortDefinition[]) => {
		if (outputs.length === 0) return {};
		const valueTypes = [...new Set(outputs.map(p => p.portType))];
		const valueType = valueTypes.length === 1 ? valueTypes[0] : valueTypes.join(' | ');
		return { inputs: { in: `Dict[String, ${valueType}]` } };
	},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];

		if (!isInputConnected('in', context)) {
			errors.push({ port: 'in', message: 'Dict input is required', level: 'structural' });
		}

		return errors;
	},
};
