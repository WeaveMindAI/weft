import { describe, expect, it } from 'vitest';
import { trimTrailingSlashes } from './url';

describe('trimTrailingSlashes', () => {
	it('drops every trailing slash and keeps the rest', () => {
		expect(trimTrailingSlashes('/weft///')).toBe('/weft');
		expect(trimTrailingSlashes('http://a/b')).toBe('http://a/b');
		expect(trimTrailingSlashes('///')).toBe('');
	});
	it('stays fast on a long run of slashes', () => {
		const long = '/'.repeat(100_000) + 'x';
		expect(trimTrailingSlashes(long)).toBe(long);
	});
});
