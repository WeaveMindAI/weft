import { describe, it, expect } from 'vitest';
import { buildSpecMap, entryForSource, entryPortCollisions, hasValue, isEmptyChoiceSet, port, type PortEntryDef, type PortSpec, type SpecField } from './port-specs';
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
	const noReserved = { inputs: [], outputs: [] };

	it('finds nothing when the entry is alone', () => {
		expect(entryPortCollisions(entry('send'), [], specMap, noReserved)).toEqual([]);
	});

	it('finds nothing when an edit keeps its own name', () => {
		const list = [entry('send'), entry('escalate')];
		const others = list.filter((_, i) => i !== 0);
		expect(entryPortCollisions(entry('send'), others, specMap, noReserved)).toEqual([]);
	});

	it('names every port an edit would take from a neighbour', () => {
		const list = [entry('send'), entry('escalate')];
		const others = list.filter((_, i) => i !== 0);
		expect(entryPortCollisions(entry('escalate'), others, specMap, noReserved)).toEqual([
			'escalate_approved',
			'escalate_rejected',
		]);
	});

	it('catches an added entry taking an existing name', () => {
		expect(entryPortCollisions(entry('send'), [entry('send')], specMap, noReserved)).toEqual([
			'send_approved',
			'send_rejected',
		]);
	});

	it('refuses a name the node already owns on the SAME side only', () => {
		// The catalog's own ports and the instance's declared header
		// ports are reserved per side, matching the compiler's
		// duplicate-port rule: an input and an output may share a name.
		const reserved = { inputs: ['send_approved'], outputs: ['send_rejected'] };
		expect(entryPortCollisions(entry('send'), [], specMap, reserved)).toEqual(['send_rejected']);
	});

	it('_should_flow is reserved on the input side of every node', () => {
		const flow: PortSpec = {
			kind: 'flow',
			keyField: 'key',
			label: 'Flow',
			addsInputs: [port('{key}', 'Boolean')],
			addsOutputs: [],
		};
		const map = buildSpecMap([flow]);
		const bad: PortEntryDef = { kind: 'flow', key: '_should_flow' };
		expect(entryPortCollisions(bad, [], map, noReserved)).toEqual(['_should_flow']);
	});
});

describe('entryForSource', () => {
	const spec: PortSpec = {
		kind: 'text_input',
		keyField: 'key',
		label: 'Text input',
		fields: [
			{ key: 'label', label: 'Label' },
			{ key: 'placeholder', label: 'Placeholder' },
		],
		addsInputs: [],
		addsOutputs: [port('{key}', 'String')],
	};

	it('keeps only the values the author filled in', () => {
		expect(entryForSource({ kind: 'text_input', label: 'Your name' }, spec, 'name')).toEqual({
			kind: 'text_input',
			key: 'name',
			label: 'Your name',
		});
	});

	it('drops cleared values instead of writing null to source', () => {
		// A cleared control saves null (the editor's unset convention);
		// an entry in source says nothing for "not filled in".
		expect(
			entryForSource({ kind: 'text_input', label: null, placeholder: undefined }, spec, 'name'),
		).toEqual({ kind: 'text_input', key: 'name' });
	});

	it('keeps an empty string: matching "" is a value, not a gap', () => {
		expect(
			entryForSource({ kind: 'text_input', placeholder: '' }, spec, 'name'),
		).toEqual({ kind: 'text_input', key: 'name', placeholder: '' });
	});

	it('drops values left over from another kind', () => {
		expect(
			entryForSource({ kind: 'text_input', options: ['a', 'b'], label: 'Q' }, spec, 'name'),
		).toEqual({ kind: 'text_input', key: 'name', label: 'Q' });
	});
});

// The one filled-in predicate: what it accepts reaches the source, and
// what it rejects reads as missing for a required field. Exactly
// undefined and null are not values; an empty string and an empty list
// ARE values (the compiler accepts a case matching "" and an optional
// `[]`), matching the compiler's null-is-absent rule key for key.
describe('hasValue', () => {
	const field = { key: 'value', label: 'Value' };
	const draft = (value: unknown): PortEntryDef => ({ kind: 'equals', value });
	it('rejects unset and cleared', () => {
		expect(hasValue({ kind: 'equals' }, field)).toBe(false);
		expect(hasValue(draft(null), field)).toBe(false);
	});
	it('accepts real values, the empty string and empty list included', () => {
		expect(hasValue(draft(''), field)).toBe(true);
		expect(hasValue(draft('x'), field)).toBe(true);
		expect(hasValue(draft(0), field)).toBe(true);
		expect(hasValue(draft(false), field)).toBe(true);
		expect(hasValue(draft(['a']), field)).toBe(true);
		expect(hasValue(draft([]), field)).toBe(true);
	});
});

// The commit gate's second rule: an empty CHOICE SET reads as chosen
// nothing (the compiler refuses it on a required field the same way),
// while an empty list that is one VALUE being matched stays legitimate.
describe('isEmptyChoiceSet', () => {
	const draft = (field: SpecField, value: unknown): PortEntryDef => ({
		kind: 'in',
		[field.key]: value,
	});
	it('flags an empty list only where the list is the choice set', () => {
		const valueList = { key: 'value', label: 'Values', shape: 'valueList' } as SpecField;
		const typedList = {
			key: 'options',
			label: 'Options',
			shape: 'typed',
			valueType: 'List[String]',
		} as SpecField;
		const singleValue = { key: 'value', label: 'Value', shape: 'value' } as SpecField;
		expect(isEmptyChoiceSet(draft(valueList, []), valueList)).toBe(true);
		expect(isEmptyChoiceSet(draft(typedList, []), typedList)).toBe(true);
		// `equals: []` matches an empty list; a legitimate value.
		expect(isEmptyChoiceSet(draft(singleValue, []), singleValue)).toBe(false);
		expect(isEmptyChoiceSet(draft(valueList, ['a']), valueList)).toBe(false);
		expect(isEmptyChoiceSet(draft(valueList, null), valueList)).toBe(false);
		// A union is not a list, structurally: the Rust side matches
		// `WeftType::List(_)` and must agree, so no string-prefix test.
		const typedUnion = {
			key: 'value',
			label: 'Value',
			shape: 'typed',
			valueType: 'List[String] | String',
		} as SpecField;
		expect(isEmptyChoiceSet(draft(typedUnion, []), typedUnion)).toBe(false);
	});
});
