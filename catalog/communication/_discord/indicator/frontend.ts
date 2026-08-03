import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Activity } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const discordIndicatorNode: NodeTemplate = {
	type: 'DiscordIndicator',
	label: 'Discord Indicator',
	description: 'Show a typing indicator in a Discord channel for a configurable duration with jitter',
	icon: Activity,
	color: '#5865F2',
	category: 'Utility',
	tags: ['discord', 'typing', 'indicator', 'presence'],
	fields: [],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: true, description: 'Discord config from DiscordConfig node', configurable: false },
		{ name: 'channelId', portType: 'String', required: true, description: 'Channel ID to show typing in' },
		{ name: 'durationMs', portType: 'Number', required: false, description: 'Total duration in ms (default 3000, max 300000)' },
		{ name: 'intervalMs', portType: 'Number', required: false, description: 'Re-send interval in ms (default 800, capped at decay ~10s)' },
		{ name: 'jitterMs', portType: 'Number', required: false, description: 'Random ± jitter in ms (default 300)' },
	],
	defaultOutputs: [
		{ name: 'success', portType: 'Boolean', required: false, description: 'Whether the indicator was shown' },
	],
	features: {},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];
		if (!isInputConnected('config', context)) {
			errors.push({ port: 'config', message: 'Discord Config is required', level: 'structural' });
		} else {
			const t = getConnectedNodeType('config', context);
			if (t && t !== 'DiscordConfig') errors.push({ port: 'config', message: `Expected DiscordConfig, got ${t}`, level: 'structural' });
		}
		if (!isInputConnected('channelId', context)) errors.push({ port: 'channelId', message: 'Channel ID is required', level: 'structural' });
		return errors;
	},
};
