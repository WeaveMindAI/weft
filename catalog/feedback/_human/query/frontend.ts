import type { NodeTemplate } from '$lib/types';
import { UserCheck } from '@lucide/svelte';
import { port, buildSpecMap, deriveInputsFromFields, deriveOutputsFromFields, type FormFieldSpec, type FormFieldDef } from '$lib/utils/form-field-specs';

export { type FormFieldDef };

// Must stay in sync with form_field_specs() in backend.rs.
// When adding or modifying field types, update both files.
export const HUMAN_FORM_FIELD_SPECS: FormFieldSpec[] = [
	{
		fieldType: 'display',
		label: 'Display (read-only)',
		render: { component: 'readonly' },
		requiredConfig: ['key'],
		optionalConfig: [],
		addsInputs: [port('{key}', 'T_Auto')],
		addsOutputs: [],
	},
	{
		fieldType: 'display_image',
		label: 'Display image (read-only)',
		render: { component: 'image' },
		requiredConfig: ['key'],
		optionalConfig: [],
		addsInputs: [port('{key}', 'Image')],
		addsOutputs: [],
	},
	{
		fieldType: 'approve_reject',
		label: 'Approve / Reject',
		render: { component: 'buttons', source: 'static' },
		requiredConfig: ['key'],
		optionalConfig: ['approveLabel', 'rejectLabel'],
		addsInputs: [],
		addsOutputs: [
			port('{key}_approved', 'Boolean'),
			port('{key}_rejected', 'Boolean'),
		],
	},
	{
		fieldType: 'select',
		label: 'Select (single)',
		render: { component: 'select', source: 'static' },
		requiredConfig: ['key', 'options'],
		optionalConfig: [],
		addsInputs: [],
		addsOutputs: [port('{key}', 'String')],
	},
	{
		fieldType: 'multi_select',
		label: 'Multi-select (static)',
		render: { component: 'select', source: 'static', multiple: true },
		requiredConfig: ['key', 'options'],
		optionalConfig: [],
		addsInputs: [],
		addsOutputs: [port('{key}', 'List[String]')],
	},
	{
		fieldType: 'select_input',
		label: 'Select (from input)',
		render: { component: 'select', source: 'input' },
		requiredConfig: ['key'],
		optionalConfig: [],
		addsInputs: [port('{key}', 'List[String]')],
		addsOutputs: [port('{key}', 'String')],
	},
	{
		fieldType: 'multi_select_input',
		label: 'Multi-select (from input)',
		render: { component: 'select', source: 'input', multiple: true },
		requiredConfig: ['key'],
		optionalConfig: [],
		addsInputs: [port('{key}', 'List[String]')],
		addsOutputs: [port('{key}', 'List[String]')],
	},
	{
		fieldType: 'text_input',
		label: 'Text input',
		render: { component: 'text' },
		requiredConfig: ['key'],
		optionalConfig: [],
		addsInputs: [],
		addsOutputs: [port('{key}', 'String')],
	},
	{
		fieldType: 'textarea',
		label: 'Textarea',
		render: { component: 'textarea' },
		requiredConfig: ['key'],
		optionalConfig: [],
		addsInputs: [],
		addsOutputs: [port('{key}', 'String')],
	},
	{
		fieldType: 'editable_text_input',
		label: 'Editable text input (pre-filled from input)',
		render: { component: 'text', prefilled: true },
		requiredConfig: ['key'],
		optionalConfig: [],
		addsInputs: [port('{key}', 'String')],
		addsOutputs: [port('{key}', 'String')],
	},
	{
		fieldType: 'editable_textarea',
		label: 'Editable textarea (pre-filled from input)',
		render: { component: 'textarea', prefilled: true },
		requiredConfig: ['key'],
		optionalConfig: [],
		addsInputs: [port('{key}', 'String')],
		addsOutputs: [port('{key}', 'String')],
	},
];

export const HUMAN_FORM_SPEC_MAP = buildSpecMap(HUMAN_FORM_FIELD_SPECS);

export type FormFieldType = typeof HUMAN_FORM_FIELD_SPECS[number]['fieldType'];

export function humanInputsFromConfig(config: Record<string, unknown>) {
	return deriveInputsFromFields((config.fields as FormFieldDef[] | undefined) ?? [], HUMAN_FORM_SPEC_MAP);
}

export function humanOutputsFromConfig(config: Record<string, unknown>) {
	return deriveOutputsFromFields((config.fields as FormFieldDef[] | undefined) ?? [], HUMAN_FORM_SPEC_MAP);
}

export const HumanNode: NodeTemplate = {
	type: 'HumanQuery',
	label: 'Human',
	description: 'Pauses execution and shows a form to the user through their extension. Fields with input ports (display, editable_textarea, select_input, multi_select_input) can be marked required by adding "required":true to the field JSON. If any required field receives null, the entire node is skipped (same as required port behavior). Use this to prevent the form from showing when upstream data is missing.',
	isBase: true,
	icon: UserCheck,
	color: '#c9873a',
	category: 'Flow',
	tags: ['flow', 'approval', 'input', 'interactive', 'form'],
	fields: [
		{ key: 'title', label: 'Task Title', type: 'text', placeholder: 'Review this lead' },
		{ key: 'description', label: 'Description', type: 'text', placeholder: 'Optional context for the reviewer' },
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
	},
	formFieldSpecs: HUMAN_FORM_FIELD_SPECS,
};
