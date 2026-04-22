import { describe, it, expect } from 'vitest';
import { updateNodePorts } from './weft-editor';

describe('updateNodePorts type editing', () => {
	it('should update port type in the weft code', () => {
		const code = `# Project: Test
# Description: Test

analyzer = LlmInference -> (response: String) {
  label: "Analyzer"
}`;
		const result = updateNodePorts(
			code,
			'analyzer',
			[{ name: 'prompt', portType: 'Number', required: true }],
			[{ name: 'response', portType: 'String' }],
		);
		expect(result).toContain('prompt: Number');
		expect(result).toContain('response: String');
	});

	it('should handle sequential type edits', () => {
		const code = `# Project: Test
# Description: Test

output = Debug {
  label: "Output"
}`;
		const step1 = updateNodePorts(
			code, 'output',
			[{ name: 'data', portType: 'String | Number', required: true }],
			[],
		);
		expect(step1).toContain('data: String | Number');

		const step2 = updateNodePorts(
			step1, 'output',
			[{ name: 'data', portType: 'Number', required: true }],
			[],
		);
		expect(step2).toContain('data: Number');
		expect(step2).not.toContain('String | Number');
	});

	it('should add port signature to a bare node', () => {
		const code = `# Project: Test
# Description: Test

node = Debug`;
		const result = updateNodePorts(
			code,
			'node',
			[{ name: 'data', portType: 'String', required: true }],
			[],
		);
		expect(result).toContain('data: String');
	});
});
