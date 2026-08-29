import { describe, it, expect } from 'vitest';
import { buildSpecMap, entryPortCollisions, port, type PortEntryDef, type PortSpec } from './port-specs';
import { isValidFieldKey } from './port-specs';

// A field key becomes a weft PORT NAME (`{key}_approved`, ...), and the parser
// only accepts port names matching `[A-Za-z_][A-Za-z0-9_]*`. This guards the
// add-field form's key gate so a punctuation/space key can never emit an
// unparseable port (the `what_do_you_want?` -> `String?` bug).
describe('isValidFieldKey', () => {
	it('accepts legal bare identifiers', () => {
		for (const k of ['x', '_x', 'X1', 'what_do_you_want', 'a_b_2', '_', '_z9']) {
			expect(isValidFieldKey(k)).toBe(true);
		}
	});
	it('rejects anything outside the grammar', () => {
		for (const k of ['', 'what do you want', 'what?', '1abc', 'a-b', 'a.b', 'é']) {
			expect(isValidFieldKey(k)).toBe(false);
		}
	});
});

// Editing a form field keeps its name most of the time, so the name it
// already owns must not read as a clash with itself. Renaming it onto a
// neighbour's name still has to be caught.
describe('entryPortCollisions', () => {
	const approve: PortSpec = {
		kind: 'approve_reject',
		keyField: 'key',
		label: 'Approve / reject',
		addsInputs: [],
		addsOutputs: [port('{key}_approved', 'Boolean'), port('{key}_rejected', 'Boolean')],
	};
	const specMap = buildSpecMap([approve]);
	const entry = (key: string): PortEntryDef => ({ kind: 'approve_reject', key });

	it('finds nothing when the entry is alone', () => {
		expect(entryPortCollisions(entry('send'), [], specMap)).toEqual([]);
	});

	it('finds nothing when an edit keeps its own name', () => {
		const list = [entry('send'), entry('escalate')];
		const others = list.filter((_, i) => i !== 0);
		expect(entryPortCollisions(entry('send'), others, specMap)).toEqual([]);
	});

	it('names every port an edit would take from a neighbour', () => {
		const list = [entry('send'), entry('escalate')];
		const others = list.filter((_, i) => i !== 0);
		expect(entryPortCollisions(entry('escalate'), others, specMap)).toEqual([
			'escalate_approved',
			'escalate_rejected',
		]);
	});

	it('catches an added entry taking an existing name', () => {
		expect(entryPortCollisions(entry('send'), [entry('send')], specMap)).toEqual([
			'send_approved',
			'send_rejected',
		]);
	});
});
