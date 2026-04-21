import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Image as ImageIcon } from '@lucide/svelte';

export const imageNode: NodeTemplate = {
	type: 'Image',
	label: 'Image',
	description: 'Image data (png, jpg, webp, gif, bmp, svg, tiff, heic, avif)',
	icon: ImageIcon,
	color: '#E91E63',
	category: 'Data',
	tags: ['image', 'media', 'photo', 'picture', 'png', 'jpg', 'webp', 'gif'],
	fields: [
		{ key: 'media', label: 'Image File', type: 'blob', accept: 'image/*', placeholder: 'Paste image URL (https://...)' },
	],
	defaultInputs: [],
	defaultOutputs: [
		{ name: 'value', portType: 'Image', required: false, description: 'Image media object { url, mimeType, filename }' },
	],
	features: {},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];
		const ref = context.config?.media;
		if (!ref || typeof ref !== 'object' || !(ref as Record<string, unknown>).url) {
			errors.push({ field: 'media', message: 'Image file is required', level: 'runtime' });
		}
		return errors;
	},
};
