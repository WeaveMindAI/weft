import { describe, expect, it } from 'vitest';
import { metadataPortSigs } from './metadata-ports';
import type { MetadataPort } from '../../../protocol';

const tool: MetadataPort = { name: 'tool', type: 'LlmTool', declaredType: 'LlmTool', value: { name: 'search' } };

describe('metadataPortSigs', () => {
	it('writes each metadata port as a header entry flagged metadata, in its written spelling', () => {
		expect(metadataPortSigs([tool, { name: 'extra', type: 'String' }])).toEqual([
			{ name: 'tool', required: true, portType: 'LlmTool', rendered: 'LlmTool', metadata: true },
			{ name: 'extra', required: true, portType: 'String', rendered: 'String', metadata: true },
		]);
	});
});
