import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Music } from '@lucide/svelte';

export const audioNode: NodeTemplate = {
	type: 'Audio',
	label: 'Audio',
	description: 'Audio data (mp3, ogg, wav, flac, m4a, aac, opus)',
	icon: Music,
	color: '#FF9800',
	category: 'Data',
	tags: ['audio', 'media', 'music', 'mp3', 'ogg', 'wav', 'sound'],
	fields: [
		{ key: 'media', label: 'Audio File', type: 'blob', accept: 'audio/*', placeholder: 'Paste audio URL (https://...)' },
	],
	defaultInputs: [],
	defaultOutputs: [
		{ name: 'value', portType: 'Audio', required: false, description: 'Audio media object { url, mimeType, filename }' },
	],
	features: {},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];
		const ref = context.config?.media;
		if (!ref || typeof ref !== 'object' || !(ref as Record<string, unknown>).url) {
			errors.push({ field: 'media', message: 'Audio file is required', level: 'runtime' });
		}
		return errors;
	},
};
