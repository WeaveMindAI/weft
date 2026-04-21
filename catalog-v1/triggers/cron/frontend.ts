import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Clock } from '@lucide/svelte';
import { hasConfigValue } from '$lib/validation';

export const CronNode: NodeTemplate = {
	type: 'Cron',
	label: 'Cron Schedule',
	description: 'Triggers on a schedule defined by a cron expression',
	icon: Clock,
	isBase: true,
	color: '#7c6f9f',
	category: 'Triggers',
	tags: ['trigger', 'cron', 'schedule', 'timer', 'periodic'],
	fields: [
		{ key: 'cron', label: 'Cron Expression', type: 'text', placeholder: '0 0 * * * (every hour)' },
	],
	defaultInputs: [],
	defaultOutputs: [
		{ name: 'scheduledTime', portType: 'String', required: false, description: 'Scheduled trigger time (RFC 3339)' },
		{ name: 'actualTime', portType: 'String', required: false, description: 'Actual trigger time (RFC 3339)' },
	],
	features: {
		isTrigger: true,
		triggerCategory: 'Schedule',
	},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];
		
		if (!hasConfigValue('cron', context.config)) {
			errors.push({ field: 'cron', message: 'Cron expression is required', level: 'structural' });
		}
		
		return errors;
	},
};
