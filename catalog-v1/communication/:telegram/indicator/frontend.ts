import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Activity } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const telegramIndicatorNode: NodeTemplate = {
	type: 'TelegramIndicator',
	label: 'Telegram Indicator',
	description: 'Show a chat action indicator (typing, recording, uploading) for a configurable duration with jitter',
	icon: Activity,
	color: '#0088CC',
	category: 'Utility',
	tags: ['telegram', 'typing', 'recording', 'indicator', 'presence', 'uploading'],
	fields: [
		{ key: 'action', label: 'Action', type: 'select', options: ['typing', 'upload_photo', 'record_video', 'upload_video', 'record_voice', 'upload_voice', 'upload_document', 'choose_sticker', 'find_location', 'record_video_note', 'upload_video_note'], placeholder: 'typing', description: 'Which indicator to show' },
	],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: true, description: 'Telegram config from TelegramConfig node', configurable: false },
		{ name: 'chatId', portType: 'String', required: true, description: 'Chat ID to show indicator in' },
		{ name: 'durationMs', portType: 'Number', required: false, description: 'Total duration in ms (default 3000, max 300000)' },
		{ name: 'intervalMs', portType: 'Number', required: false, description: 'Re-send interval in ms (default 800, capped at decay ~5s)' },
		{ name: 'jitterMs', portType: 'Number', required: false, description: 'Random ± jitter in ms (default 300)' },
	],
	defaultOutputs: [
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the indicator was shown' },
	],
	features: {},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];
		if (!isInputConnected('config', context)) {
			errors.push({ port: 'config', message: 'Telegram Config is required', level: 'structural' });
		} else {
			const t = getConnectedNodeType('config', context);
			if (t && t !== 'TelegramConfig') errors.push({ port: 'config', message: `Expected TelegramConfig, got ${t}`, level: 'structural' });
		}
		if (!isInputConnected('chatId', context)) errors.push({ port: 'chatId', message: 'Chat ID is required', level: 'structural' });
		return errors;
	},
};
