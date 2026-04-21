import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { File } from '@lucide/svelte';

export const documentNode: NodeTemplate = {
	type: 'Document',
	label: 'Document',
	description: 'Document/file data (pdf, docx, pptx, xlsx, csv, txt, zip, etc.)',
	icon: File,
	color: '#607D8B',
	category: 'Data',
	tags: ['document', 'file', 'media', 'pdf', 'docx', 'pptx', 'xlsx', 'csv', 'zip'],
	fields: [
		{ key: 'media', label: 'Document File', type: 'blob', accept: '*/*', placeholder: 'Paste file URL (https://...)' },
	],
	defaultInputs: [],
	defaultOutputs: [
		{ name: 'value', portType: 'Document', required: false, description: 'Document media object { url, mimeType, filename }' },
	],
	features: {},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];
		const ref = context.config?.media;
		if (!ref || typeof ref !== 'object' || !(ref as Record<string, unknown>).url) {
			errors.push({ field: 'media', message: 'Document file is required', level: 'runtime' });
		}
		return errors;
	},
};
