import { describe, it, expect } from 'vitest';
import { isValidFieldKey } from './form-field-specs';

// A field key becomes a weft PORT NAME (`{key}_approved`, ...), and the parser
// only accepts port names matching `[A-Za-z_][A-Za-z0-9_]*`. This guards the
// add-field form's key gate so a punctuation/space key can never emit an
// unparseable port (the `what_do_you_want?` -> `String?` bug).
describe('isValidFieldKey', () => {
	it('accepts legal bare identifiers', () => {
		for (const k of ['x', '_x', 'X1', 'what_do_you_want', 'a_b_2', '_', '_raw']) {
			expect(isValidFieldKey(k)).toBe(true);
		}
	});
	it('rejects anything outside the grammar', () => {
		for (const k of ['', 'what do you want', 'what?', '1abc', 'a-b', 'a.b', 'é']) {
			expect(isValidFieldKey(k)).toBe(false);
		}
	});
});
