import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { KeyRound } from '@lucide/svelte';
import { isApiKeyReady } from '$lib/validation';

export const ElevenLabsConfigNode: NodeTemplate = {
	type: 'ElevenLabsConfig',
	label: 'ElevenLabs Config',
	description: 'ElevenLabs API credentials. Connect its config output to ElevenLabsTranscribe.',
	icon: KeyRound,
	color: '#10b981',
	category: 'AI',
	tags: ['elevenlabs', 'config', 'credentials', 'api', 'audio', 'speech'],
	fields: [
		{ key: 'apiKey', label: 'API Key', type: 'api_key', provider: 'elevenlabs' },
	],
	defaultInputs: [],
	defaultOutputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: false, description: 'ElevenLabs configuration object (apiKey)' },
	],
	setupGuide: [
		'Get your ElevenLabs API key from https://elevenlabs.io (Settings → API Keys)',
		'Connect this node\'s config output to any ElevenLabs node',
	],
	features: {
	},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];
		if (!isApiKeyReady('apiKey', context.config)) {
			errors.push({ field: 'apiKey', message: 'Own key selected but not entered', level: 'runtime' });
		}
		return errors;
	},
};
