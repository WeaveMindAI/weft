import type { NodeTemplate } from '$lib/types';
import { Play } from '@lucide/svelte';
import { port, buildSpecMap, deriveInputsFromFields, deriveOutputsFromFields, type FormFieldSpec, type FormFieldDef } from '$lib/utils/form-field-specs';
import { HUMAN_FORM_FIELD_SPECS } from '$lib/nodes/human-query';

export { type FormFieldDef };

export const HUMAN_TRIGGER_SPEC_MAP = buildSpecMap(HUMAN_FORM_FIELD_SPECS);

export function humanTriggerInputsFromConfig(config: Record<string, unknown>) {
	return deriveInputsFromFields((config.fields as FormFieldDef[] | undefined) ?? [], HUMAN_TRIGGER_SPEC_MAP);
}

export function humanTriggerOutputsFromConfig(config: Record<string, unknown>) {
	return deriveOutputsFromFields((config.fields as FormFieldDef[] | undefined) ?? [], HUMAN_TRIGGER_SPEC_MAP);
}

export const HumanTriggerNode: NodeTemplate = {
	type: 'HumanTrigger',
	label: 'Human Trigger',
	description: 'A trigger that fires when a human submits a form. The form appears in the browser extension under the Triggers tab. Each submission starts a new project execution with the form data as output.',
	icon: Play,
	isBase: true,
	color: '#c9873a',
	category: 'Flow',
	tags: ['flow', 'trigger', 'input', 'interactive', 'form', 'manual'],
	fields: [
		{ key: 'title', label: 'Trigger Title', type: 'text', placeholder: 'Submit a new task' },
		{ key: 'description', label: 'Description', type: 'text', placeholder: 'Optional context for the form' },
		{ key: 'fields', label: 'Form Fields', type: 'form_builder' as never, placeholder: '' },
	],
	defaultInputs: [
		{ name: 'context', portType: 'String', required: false },
	],
	defaultOutputs: [],
	features: {
		canAddInputPorts: false,
		canAddOutputPorts: false,
		hasFormSchema: true,
		isTrigger: true,
		triggerCategory: 'Manual',
	},
	formFieldSpecs: HUMAN_FORM_FIELD_SPECS,
};
