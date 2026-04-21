import type { NodeTemplate } from '$lib/types';
import { StickyNote } from '@lucide/svelte';

export const AnnotationNode: NodeTemplate = {
	type: 'Annotation',
	label: 'Annotation',
	description: 'Add notes and documentation to your project with markdown support',
	icon: StickyNote,
	isBase: true,
	color: '#64748b',
	category: 'Utility',
	tags: ['note', 'comment', 'documentation', 'markdown', 'text', 'annotation'],
	fields: [
		{
			key: 'content',
			label: 'Content',
			type: 'textarea',
			placeholder: 'Write your notes here... (supports markdown)',
			defaultValue: '',
			description: 'Markdown content for the annotation',
		},
	],
	defaultInputs: [],
	defaultOutputs: [],
};
