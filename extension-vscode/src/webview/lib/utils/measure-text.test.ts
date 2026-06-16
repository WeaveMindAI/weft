import { describe, it, expect } from 'vitest';
import { measureTextWidth, nodeLabelFont } from './measure-text';

// vitest's default environment is node (no document), so these exercise the
// non-DOM fallback path: measureTextWidth returns the coarse chars*6.5 estimate
// and nodeLabelFont uses a generic family. The DOM/canvas path is covered by the
// browser at runtime (jsdom would just return 0 widths, which tests nothing real).
describe('measureTextWidth (non-DOM fallback)', () => {
	it('falls back to a char-count estimate when no canvas is available', () => {
		expect(measureTextWidth('abcd', '10px sans-serif')).toBe(4 * 6.5);
		expect(measureTextWidth('', '10px sans-serif')).toBe(0);
	});

	it('caches by (font, text): same inputs return the same value', () => {
		const a = measureTextWidth('port_name', '10px sans-serif');
		const b = measureTextWidth('port_name', '10px sans-serif');
		expect(a).toBe(b);
	});
});

describe('nodeLabelFont', () => {
	it('builds a CSS font shorthand at the given px size', () => {
		expect(nodeLabelFont(10)).toMatch(/^10px /);
		expect(nodeLabelFont(11)).toMatch(/^11px /);
	});
});
