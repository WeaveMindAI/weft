import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Video as VideoIcon } from '@lucide/svelte';

export const videoNode: NodeTemplate = {
	type: 'Video',
	label: 'Video',
	description: 'Video data (mp4, webm, mov, avi, mkv)',
	icon: VideoIcon,
	color: '#9C27B0',
	category: 'Data',
	tags: ['video', 'media', 'mp4', 'webm', 'mov'],
	fields: [
		{ key: 'media', label: 'Video File', type: 'blob', accept: 'video/*', placeholder: 'Paste video URL (https://...)' },
	],
	defaultInputs: [],
	defaultOutputs: [
		{ name: 'value', portType: 'Video', required: false, description: 'Video media object { url, mimeType, filename }' },
	],
	features: {},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];
		const ref = context.config?.media;
		if (!ref || typeof ref !== 'object' || !(ref as Record<string, unknown>).url) {
			errors.push({ field: 'media', message: 'Video file is required', level: 'runtime' });
		}
		return errors;
	},
};
