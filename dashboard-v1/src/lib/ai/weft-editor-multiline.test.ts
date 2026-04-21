import { describe, it, expect } from 'vitest';
import { updateNodeConfig } from './weft-editor';

describe('updateNodeConfig multiline', () => {
	it('should not duplicate multiline value on save', () => {
		const code = `input = Text {
  label: "Input"
  value: \`\`\`
AI is transforming how we work.
fwe
  \`\`\`
}`;
		const result = updateNodeConfig(code, 'input', 'value', 'AI is transforming how we work.\nfwe');
		const backtickCount = (result.match(/```/g) || []).length;
		expect(backtickCount).toBe(2);
	});
});
