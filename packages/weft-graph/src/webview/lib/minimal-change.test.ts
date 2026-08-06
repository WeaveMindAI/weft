import { describe, it, expect } from 'vitest';
import { minimalChange } from './minimal-change';

/** Applying the change spec must always reproduce the target string. */
function apply(oldValue: string, change: { from: number; to: number; insert: string }): string {
	return oldValue.slice(0, change.from) + change.insert + oldValue.slice(change.to);
}

describe('minimalChange', () => {
	it('returns null for equal strings', () => {
		expect(minimalChange('abc', 'abc')).toBeNull();
		expect(minimalChange('', '')).toBeNull();
	});

	it('empty to content inserts everything', () => {
		expect(minimalChange('', 'abc')).toEqual({ from: 0, to: 0, insert: 'abc' });
	});

	it('content to empty deletes everything', () => {
		expect(minimalChange('abc', '')).toEqual({ from: 0, to: 3, insert: '' });
	});

	it('middle insert touches only the gap', () => {
		expect(minimalChange('ac', 'abc')).toEqual({ from: 1, to: 1, insert: 'b' });
	});

	it('middle delete touches only the span', () => {
		expect(minimalChange('abc', 'ac')).toEqual({ from: 1, to: 2, insert: '' });
	});

	it('round-trips arbitrary edits', () => {
		const cases: Array<[string, string]> = [
			['hello world', 'hello brave world'],
			['aaa', 'aa'],
			['abcdef', 'abXYef'],
			['same-prefix-different', 'same-prefix-other'],
			['x', 'yxy'],
		];
		for (const [oldValue, newValue] of cases) {
			const change = minimalChange(oldValue, newValue);
			expect(change).not.toBeNull();
			expect(apply(oldValue, change!)).toBe(newValue);
		}
	});
});
