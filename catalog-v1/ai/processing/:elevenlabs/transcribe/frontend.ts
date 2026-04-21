import type { NodeTemplate, ValidationContext, ValidationError } from '$lib/types';
import { Mic } from '@lucide/svelte';
import { isInputConnected, getConnectedNodeType } from '$lib/validation';

const VALID_LANGUAGE_CODES = new Set([
	'afr', 'amh', 'ara', 'asm', 'ast', 'aze', 'bak', 'bas', 'bel', 'ben', 'bhr', 'bod',
	'bos', 'bre', 'bul', 'cat', 'ceb', 'ces', 'chv', 'ckb', 'cnh', 'cre', 'cym', 'dan',
	'dav', 'deu', 'div', 'dyu', 'ell', 'eng', 'epo', 'est', 'eus', 'fao', 'fas', 'fil',
	'fin', 'fra', 'fry', 'ful', 'gla', 'gle', 'glg', 'guj', 'hat', 'hau', 'heb', 'hin',
	'hrv', 'hsb', 'hun', 'hye', 'ibo', 'ina', 'ind', 'isl', 'ita', 'jav', 'jpn', 'kab',
	'kan', 'kas', 'kat', 'kaz', 'kea', 'khm', 'kin', 'kir', 'kln', 'kmr', 'kor', 'kur',
	'lao', 'lat', 'lav', 'lij', 'lin', 'lit', 'ltg', 'ltz', 'lug', 'luo', 'mal', 'mar',
	'mdf', 'mhr', 'mkd', 'mlg', 'mlt', 'mon', 'mri', 'mrj', 'msa', 'mya', 'myv', 'nan',
	'nep', 'nhi', 'nld', 'nor', 'nso', 'nya', 'oci', 'ori', 'orm', 'oss', 'pan', 'pol',
	'por', 'pus', 'quy', 'roh', 'ron', 'rus', 'sah', 'san', 'sat', 'sin', 'skr', 'slk',
	'slv', 'smo', 'sna', 'snd', 'som', 'sot', 'spa', 'sqi', 'srd', 'srp', 'sun', 'swa',
	'swe', 'tam', 'tat', 'tel', 'tgk', 'tha', 'tig', 'tir', 'tok', 'ton', 'tsn', 'tuk',
	'tur', 'twi', 'uig', 'ukr', 'umb', 'urd', 'uzb', 'vie', 'vot', 'vro', 'wol', 'xho',
	'yid', 'yor', 'yue', 'zgh', 'zho', 'zul', 'zza',
]);

export const elevenLabsTranscribeNode: NodeTemplate = {
	type: 'ElevenLabsTranscribe',
	label: 'ElevenLabs Transcribe',
	description: 'Transcribe audio to text using ElevenLabs Scribe v2.',
	icon: Mic,
	color: '#10b981',
	category: 'AI',
	tags: ['audio', 'transcription', 'speech', 'voice', 'elevenlabs', 'scribe'],
	fields: [
		{ key: 'diarize', label: 'Speaker Diarization', type: 'checkbox', description: 'Identify different speakers in the audio' },
		{
			key: 'language',
			label: 'Language',
			type: 'text',
			placeholder: 'Leave empty for auto-detect',
			description: 'ISO 639-3 code (e.g. "eng", "fra", "deu"). Leave empty for auto-detect (recommended).',
		},
	],
	defaultInputs: [
		{ name: 'config', portType: 'Dict[String, String]', required: false, description: 'Connect ElevenLabsConfig.config', configurable: false },
		{ name: 'audio', portType: 'Audio', required: true, description: 'Audio media object (URL passed directly to ElevenLabs)' },
	],
	defaultOutputs: [
		{ name: 'transcription', portType: 'String', required: false, description: 'Transcribed text' },
	],
	features: {
	},
	validate: (context: ValidationContext): ValidationError[] => {
		const errors: ValidationError[] = [];


		const connectedConfigType = getConnectedNodeType('config', context);
		if (connectedConfigType && connectedConfigType !== 'ElevenLabsConfig') {
			errors.push({ port: 'config', message: `Config should be connected to a ElevenLabsConfig node, not ${connectedConfigType}`, level: 'structural' });
		}
		if (!isInputConnected('audio', context)) {
			errors.push({ port: 'audio', message: 'Audio input is required', level: 'structural' });
		}

		const lang = context.config.language;
		if (lang && typeof lang === 'string' && lang.trim() !== '') {
			if (!VALID_LANGUAGE_CODES.has(lang.trim())) {
				errors.push({
					field: 'language',
					message: `Invalid language code "${lang}". Use an ISO 639-3 code (e.g. "eng", "fra") or leave empty for auto-detect.`,
					level: 'structural',
				});
			}
		}

		return errors;
	},
};
