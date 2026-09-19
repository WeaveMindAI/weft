import { describe, it, expect } from 'vitest';
import { jsonTokens, valueSummary, valueText, isPlainText } from './value-display';

/** The rendered text, which must always be the JSON a parser accepts back. */
function render(value: unknown): string {
	return jsonTokens(value)
		.map((t) => t.text)
		.join('');
}

describe('jsonTokens', () => {
	it('renders exactly what JSON.stringify does at 2-space indent', () => {
		const cases: unknown[] = [
			null,
			true,
			42,
			'hi',
			[],
			{},
			[1, 2, 3],
			{ a: 1, b: 'two', c: null },
			{ nested: { list: [{ deep: true }], empty: {} } },
			['a', ['b', ['c']]],
		];
		for (const value of cases) {
			expect(render(value)).toBe(JSON.stringify(value, null, 2));
		}
	});

	it('colours keys apart from string values', () => {
		const tokens = jsonTokens({ port: 'value' });
		expect(tokens.find((t) => t.text === '"port"')?.kind).toBe('key');
		expect(tokens.find((t) => t.text === '"value"')?.kind).toBe('string');
	});

	it('tags each scalar with its own kind', () => {
		const kinds = (v: unknown) => jsonTokens(v).map((t) => t.kind);
		expect(kinds(1)).toEqual(['number']);
		expect(kinds(false)).toEqual(['boolean']);
		expect(kinds(null)).toEqual(['null']);
		expect(kinds('s')).toEqual(['string']);
	});

	// The reason tokens are walked rather than regexed: a value whose
	// TEXT looks like JSON must stay one string token.
	it('does not read structure out of a string that contains JSON', () => {
		const tokens = jsonTokens({ reply: '{"fake": [1, 2]}' });
		const strings = tokens.filter((t) => t.kind === 'string');
		expect(strings).toHaveLength(1);
		expect(strings[0].text).toBe(JSON.stringify('{"fake": [1, 2]}'));
		expect(render({ reply: '{"fake": [1, 2]}' })).toBe(
			JSON.stringify({ reply: '{"fake": [1, 2]}' }, null, 2),
		);
	});

	it('keeps newlines and quotes inside a string escaped', () => {
		expect(render({ s: 'a\n"b"' })).toBe(JSON.stringify({ s: 'a\n"b"' }, null, 2));
	});

	it('names a non-finite number instead of printing null', () => {
		expect(render(Number.NaN)).toBe('NaN');
		expect(render(Number.POSITIVE_INFINITY)).toBe('Infinity');
	});
});

describe('valueSummary', () => {
	it('counts what there is, pluralised', () => {
		expect(valueSummary('abc')).toBe('text · 3 chars');
		expect(valueSummary('a')).toBe('text · 1 char');
		expect(valueSummary([1])).toBe('array · 1 item');
		expect(valueSummary([1, 2])).toBe('array · 2 items');
		expect(valueSummary({ a: 1 })).toBe('object · 1 key');
		expect(valueSummary({ a: 1, b: 2 })).toBe('object · 2 keys');
	});

	it('groups a long count so it stays readable', () => {
		expect(valueSummary('x'.repeat(12345))).toBe('text · 12,345 chars');
	});

	it('names the scalars', () => {
		expect(valueSummary(null)).toBe('null');
		expect(valueSummary(1)).toBe('number');
		expect(valueSummary(true)).toBe('boolean');
		expect(valueSummary(undefined)).toBe('empty');
	});
});

describe('valueText', () => {
	it('hands back a string as itself, with no quotes or escapes', () => {
		expect(valueText('line one\nline two')).toBe('line one\nline two');
	});

	it('hands back anything else as its pretty JSON', () => {
		expect(valueText({ a: [1] })).toBe(JSON.stringify({ a: [1] }, null, 2));
	});
});

describe('isPlainText', () => {
	it('is true only for a string', () => {
		expect(isPlainText('x')).toBe(true);
		expect(isPlainText(1)).toBe(false);
		expect(isPlainText(null)).toBe(false);
		expect(isPlainText(['x'])).toBe(false);
	});
});
