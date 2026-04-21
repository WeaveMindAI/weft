import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { BrainCircuit } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

export const LlmNode: NodeTemplate = {
	type: 'LlmInference',
	label: 'LLM',
	description: 'AI language model completion. Works out of the box with platform credits (default: anthropic/claude-sonnet-4.6). Optionally connect an LlmConfig node for BYOK or to override any parameter.',
	isBase: true,
	icon: BrainCircuit,
	color: '#7c6f9f',
	category: 'AI',
	tags: ['ai', 'gpt', 'claude', 'completion', 'chat', 'generate'],
	fields: [
		{ key: 'model', label: 'Model', type: 'text', placeholder: 'anthropic/claude-sonnet-4.6', description: 'OpenRouter model slug. Default: anthropic/claude-sonnet-4.6. Prefer cheaper models (anthropic/claude-haiku-4.5, openai/gpt-4o-mini) when the task allows.' },
		{ key: 'systemPrompt', label: 'System prompt', type: 'textarea', placeholder: '(empty)', description: 'Optional system prompt. Empty by default.' },
		{ key: 'temperature', label: 'Temperature', type: 'number', placeholder: '0.7', description: '0.0–2.0. Higher = more creative.' },
		{ key: 'maxTokens', label: 'Max tokens', type: 'number', placeholder: 'provider default' },
		{ key: 'topP', label: 'Top P', type: 'number', placeholder: '1.0' },
		{ key: 'frequencyPenalty', label: 'Frequency penalty', type: 'number', placeholder: '0.0' },
		{ key: 'presencePenalty', label: 'Presence penalty', type: 'number', placeholder: '0.0' },
		{ key: 'reasoning', label: 'Enable reasoning', type: 'checkbox', description: 'Use the model\'s reasoning/thinking mode if supported.' },
		{ key: 'reasoningEffort', label: 'Reasoning effort', type: 'select', options: ['low', 'medium', 'high'] },
		{ key: 'seed', label: 'Seed', type: 'number', placeholder: '(random)' },
		{ key: 'parseJson', label: 'Parse JSON', type: 'checkbox', description: 'Parse and repair JSON from the response (changes output to Dict).' },
	],
	defaultInputs: [
		{ name: 'prompt', portType: 'String', required: true, description: 'The prompt to send to the LLM' },
		{ name: 'systemPrompt', portType: 'String', required: false, description: 'Optional system prompt. Can be wired or set as a config field.' },
		{ name: 'config', portType: 'Dict[String, String | Number | Boolean]', required: false, description: 'Optional LlmConfig node. When wired, overrides this node\'s own config fields.', configurable: false },
	],
	defaultOutputs: [
		{ name: 'response', portType: 'MustOverride', required: false, description: 'LLM response. Declare type in Weft: String without parseJson, or Dict/JsonDict with parseJson. With parseJson, you can also add custom output ports to extract specific JSON keys directly (e.g. keywords: List[String]).' },
	],
	features: {
		canAddOutputPorts: true,
	},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];

		if (!isInputConnected('prompt', context)) {
			errors.push({ port: 'prompt', message: 'Prompt input is required', level: 'structural' });
		}

		const connectedConfigType = getConnectedNodeType('config', context);
		if (connectedConfigType && connectedConfigType !== 'LlmConfig') {
			errors.push({ port: 'config', message: `Config input should be connected to an LlmConfig node, not ${connectedConfigType}`, level: 'structural' });
		}

		return errors;
	},
};
