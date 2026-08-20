import { describe, it, expect } from 'vitest';
import {
	parseWeftType,
	weftTypeToString,
	weftTypeToWireString,
	isWeftTypeCompatible,
	isCompatible,
	weftTypesEqual,
	extractPrimitives,
	inferTypeFromValue,
	type WeftType,
} from '../types';

describe('parseWeftType', () => {
	it('parses primitives', () => {
		expect(parseWeftType('String')?.kind).toBe('primitive');
		expect(parseWeftType('Number')?.kind).toBe('primitive');
		expect(parseWeftType('Boolean')?.kind).toBe('primitive');
	});

	it('parses List[T]', () => {
		const t = parseWeftType('List[String]');
		expect(t?.kind).toBe('list');
		if (t?.kind === 'list') {
			expect(t.inner.kind).toBe('primitive');
		}
	});

	it('parses nested List[List[Number]]', () => {
		const t = parseWeftType('List[List[Number]]');
		expect(t?.kind).toBe('list');
		if (t?.kind === 'list') {
			expect(t.inner.kind).toBe('list');
		}
	});

	it('parses Dict[String, Number]', () => {
		const t = parseWeftType('Dict[String, Number]');
		expect(t?.kind).toBe('dict');
	});

	it('parses unions', () => {
		const t = parseWeftType('String | Number');
		expect(t?.kind).toBe('union');
		if (t?.kind === 'union') {
			expect(t.types).toHaveLength(2);
		}
	});

	it('parses complex union with parameterized types', () => {
		const t = parseWeftType('List[String] | Dict[String, Number]');
		expect(t?.kind).toBe('union');
		if (t?.kind === 'union') {
			expect(t.types[0].kind).toBe('list');
			expect(t.types[1].kind).toBe('dict');
		}
	});

	it('parses type variables', () => {
		expect(parseWeftType('T')?.kind).toBe('typevar');
		expect(parseWeftType('T1')?.kind).toBe('typevar');
		expect(parseWeftType('T42')?.kind).toBe('typevar');
	});

	it('parses MustOverride', () => {
		expect(parseWeftType('MustOverride')?.kind).toBe('must_override');
	});

	it('parses Access', () => {
		expect(parseWeftType('Access')?.kind).toBe('access');
	});

	it('parses a record with optional fields', () => {
		const t = parseWeftType('{ name: String, age: Number, nickname?: String }');
		expect(t).toEqual({
			kind: 'record',
			fields: [
				{ name: 'name', ty: { kind: 'primitive', value: 'String' }, optional: false },
				{ name: 'age', ty: { kind: 'primitive', value: 'Number' }, optional: false },
				{ name: 'nickname', ty: { kind: 'primitive', value: 'String' }, optional: true },
			],
		});
	});

	it('parses the self-contained named wire form', () => {
		const t = parseWeftType('Profile={ name: String }');
		expect(t).toEqual({
			kind: 'named',
			name: 'Profile',
			body: {
				kind: 'record',
				fields: [{ name: 'name', ty: { kind: 'primitive', value: 'String' }, optional: false }],
			},
		});
	});

	it('rejects a bare declared name (no registry frontend-side)', () => {
		expect(parseWeftType('Profile')).toBeNull();
	});

	it('rejects reserved names in the named wire form', () => {
		// Mirrors the backend's check_declarable_name: primitives,
		// container keywords, special types, and TypeVar shapes.
		for (const s of ['String=Number', 'Blob=String', 'List=String', 'Access=String', 'T1=String', 'T_Auto=String']) {
			expect(parseWeftType(s), s).toBeNull();
		}
	});

	it('never resolves an Object prototype key as a type or alias', () => {
		// The alias table is a Map precisely so 'toString' & co can
		// never resolve through the prototype chain; pin the crash
		// class shut (a plain-object table made `parseWeftType('toString')`
		// throw instead of returning null).
		for (const s of ['toString', 'constructor', 'valueOf', 'hasOwnProperty', '__proto__']) {
			expect(parseWeftType(s), s).toBeNull();
		}
		expect(isWeftTypeCompatible('toString', 'String')).toBe(false);
	});

	it('rejects a named body carrying a type variable', () => {
		// Mirrors the backend's wire-parser gate: a declared body must
		// be concrete, or two same-named types could carry different
		// bodies under name-only nominal compatibility.
		for (const s of ['MyBox=List[T]', 'MyBox=T_Auto', 'MyBox={a: T}', 'MyBox=MustOverride']) {
			expect(parseWeftType(s), s).toBeNull();
		}
	});

	it('parses type var inside List', () => {
		const t = parseWeftType('List[T]');
		expect(t?.kind).toBe('list');
		if (t?.kind === 'list') {
			expect(t.inner.kind).toBe('typevar');
		}
	});

	it('parses deeply nested type', () => {
		const t = parseWeftType('Dict[String, Dict[String, List[String] | String | Number | T1] | Number | String]');
		expect(t).not.toBeNull();
		expect(t?.kind).toBe('dict');
	});

	it('rejects bare List', () => {
		expect(parseWeftType('List')).toBeNull();
	});

	it('rejects bare Dict', () => {
		expect(parseWeftType('Dict')).toBeNull();
	});

	it('rejects Any', () => {
		expect(parseWeftType('Any')).toBeNull();
	});

	it('rejects Stack', () => {
		expect(parseWeftType('Stack[String]')).toBeNull();
	});

	it('rejects unknown types', () => {
		expect(parseWeftType('Foo')).toBeNull();
		expect(parseWeftType('int')).toBeNull();
	});

	it('rejects unclosed bracket', () => {
		expect(parseWeftType('List[String')).toBeNull();
	});

	it('rejects Dict with one param', () => {
		expect(parseWeftType('Dict[String]')).toBeNull();
	});

	it('rejects Dict with three params', () => {
		expect(parseWeftType('Dict[String, Number, Boolean]')).toBeNull();
	});

	it('rejects empty string', () => {
		expect(parseWeftType('')).toBeNull();
	});

	it('rejects whitespace only', () => {
		expect(parseWeftType('   ')).toBeNull();
	});
});

// ── Complex type parsing ────────────────────────────────────────────────────

describe('complex type parsing', () => {
	it('Dict[String, List[String]]', () => {
		const t = parseWeftType('Dict[String, List[String]]');
		expect(t?.kind).toBe('dict');
		if (t?.kind === 'dict') {
			expect(t.key.kind).toBe('primitive');
			expect(t.value.kind).toBe('list');
			if (t.value.kind === 'list') {
				expect(t.value.inner.kind).toBe('primitive');
			}
		}
	});

	it('List[Dict[String, Number]]', () => {
		const t = parseWeftType('List[Dict[String, Number]]');
		expect(t?.kind).toBe('list');
		if (t?.kind === 'list') {
			expect(t.inner.kind).toBe('dict');
		}
	});

	it('Dict[String, String | Number | Boolean]', () => {
		const t = parseWeftType('Dict[String, String | Number | Boolean]');
		expect(t?.kind).toBe('dict');
		if (t?.kind === 'dict') {
			expect(t.value.kind).toBe('union');
			if (t.value.kind === 'union') {
				expect(t.value.types).toHaveLength(3);
			}
		}
	});

	it('Dict[String, Dict[String, List[String] | Number] | String]', () => {
		const t = parseWeftType('Dict[String, Dict[String, List[String] | Number] | String]');
		expect(t).not.toBeNull();
		expect(t?.kind).toBe('dict');
		if (t?.kind === 'dict') {
			expect(t.value.kind).toBe('union');
		}
	});

	it('List[List[List[String]]]', () => {
		const t = parseWeftType('List[List[List[String]]]');
		expect(t?.kind).toBe('list');
		if (t?.kind === 'list') {
			expect(t.inner.kind).toBe('list');
			if (t.inner.kind === 'list') {
				expect(t.inner.inner.kind).toBe('list');
			}
		}
	});

	it('Dict[String, T]', () => {
		const t = parseWeftType('Dict[String, T]');
		expect(t?.kind).toBe('dict');
		if (t?.kind === 'dict') {
			expect(t.value.kind).toBe('typevar');
		}
	});

	it('T1 | T2', () => {
		const t = parseWeftType('T1 | T2');
		expect(t?.kind).toBe('union');
		if (t?.kind === 'union') {
			expect(t.types[0].kind).toBe('typevar');
			expect(t.types[1].kind).toBe('typevar');
		}
	});

	it('List[T] | Dict[String, T]', () => {
		const t = parseWeftType('List[T] | Dict[String, T]');
		expect(t?.kind).toBe('union');
		if (t?.kind === 'union') {
			expect(t.types[0].kind).toBe('list');
			expect(t.types[1].kind).toBe('dict');
		}
	});

	it('Dict[String, List[Dict[String, Number]] | String]', () => {
		const t = parseWeftType('Dict[String, List[Dict[String, Number]] | String]');
		expect(t).not.toBeNull();
		expect(t?.kind).toBe('dict');
		if (t?.kind === 'dict') {
			expect(t.value.kind).toBe('union');
			if (t.value.kind === 'union') {
				expect(t.value.types).toHaveLength(2);
				expect(t.value.types[0].kind).toBe('list');
				expect(t.value.types[1].kind).toBe('primitive');
			}
		}
	});

	it('Media alias expands to union', () => {
		const t = parseWeftType('Media');
		expect(t?.kind).toBe('union');
		if (t?.kind === 'union') {
			expect(t.types).toHaveLength(3); // Image | Video | Audio
		}
		const f = parseWeftType('File');
		if (f?.kind === 'union') {
			expect(f.types).toHaveLength(4); // Image | Video | Audio | Blob
		} else {
			throw new Error('File should be a union');
		}
	});

	it('String | Media expands Media in union', () => {
		const t = parseWeftType('String | Media');
		expect(t?.kind).toBe('union');
		if (t?.kind === 'union') {
			expect(t.types.length).toBe(4); // String + 3 media types
		}
	});

	it('List[Media]', () => {
		const t = parseWeftType('List[Media]');
		expect(t?.kind).toBe('list');
		if (t?.kind === 'list') {
			expect(t.inner.kind).toBe('union');
		}
	});

	it('List[Media | String]', () => {
		const t = parseWeftType('List[Media | String]');
		expect(t?.kind).toBe('list');
		if (t?.kind === 'list' && t.inner.kind === 'union') {
			expect(t.inner.types.length).toBe(4); // 3 media + String
		}
	});

	it('Dict[String, Media]', () => {
		const t = parseWeftType('Dict[String, Media]');
		expect(t?.kind).toBe('dict');
		if (t?.kind === 'dict') {
			expect(t.value.kind).toBe('union');
		}
	});

	it('Dict[String, List[Media | String]]', () => {
		const t = parseWeftType('Dict[String, List[Media | String]]');
		expect(t?.kind).toBe('dict');
		if (t?.kind === 'dict' && t.value.kind === 'list') {
			expect(t.value.inner.kind).toBe('union');
		}
	});

	it('alias roundtrip: Media/File render under their NAME and re-parse', () => {
		for (const name of ['Media', 'File']) {
			const t = parseWeftType(name)!;
			const s = weftTypeToString(t);
			expect(s).toBe(name);
			expect(parseWeftType(s)).toEqual(t);
		}
		// The structural spelling parses to the same type and renders under
		// the canonical name; a non-alias union stays structural.
		expect(weftTypeToString(parseWeftType('Audio | Image | Video')!)).toBe('Media');
		expect(weftTypeToString(parseWeftType('Image | Null')!)).toBe('Image | Null');
	});
});

describe('Null type', () => {
	it('parses Null', () => {
		const t = parseWeftType('Null');
		expect(t?.kind).toBe('primitive');
		if (t?.kind === 'primitive') expect(t.value).toBe('Null');
	});

	it('parses String | Null', () => {
		const t = parseWeftType('String | Null');
		expect(t?.kind).toBe('union');
		if (t?.kind === 'union') expect(t.types).toHaveLength(2);
	});

	it('parses List[Number | Null]', () => {
		const t = parseWeftType('List[Number | Null]');
		expect(t?.kind).toBe('list');
		if (t?.kind === 'list') expect(t.inner.kind).toBe('union');
	});

	it('parses Dict[String, String | Null]', () => {
		const t = parseWeftType('Dict[String, String | Null]');
		expect(t?.kind).toBe('dict');
	});

	it('parses List[Dict[String, Number | Null]]', () => {
		const t = parseWeftType('List[Dict[String, Number | Null]]');
		expect(t?.kind).toBe('list');
	});

	it('roundtrip: String | Null', () => {
		const t = parseWeftType('String | Null')!;
		const s = weftTypeToString(t);
		expect(parseWeftType(s)).toEqual(t);
	});

	// Compatibility
	it('String → String | Null: OK', () => {
		expect(isWeftTypeCompatible('String', 'String | Null')).toBe(true);
	});

	it('Null → String | Null: OK', () => {
		expect(isWeftTypeCompatible('Null', 'String | Null')).toBe(true);
	});

	it('String | Null → String: fails (Null not handled)', () => {
		expect(isWeftTypeCompatible('String | Null', 'String')).toBe(false);
	});

	it('String | Null → String | Null: OK', () => {
		expect(isWeftTypeCompatible('String | Null', 'String | Null')).toBe(true);
	});

	it('Null → String: fails', () => {
		expect(isWeftTypeCompatible('Null', 'String')).toBe(false);
	});

	it('Number → Number | Null: OK', () => {
		expect(isWeftTypeCompatible('Number', 'Number | Null')).toBe(true);
	});

	it('List[String] → List[String | Null]: OK (covariant)', () => {
		expect(isWeftTypeCompatible('List[String]', 'List[String | Null]')).toBe(true);
	});

	it('List[String | Null] → List[String]: fails', () => {
		expect(isWeftTypeCompatible('List[String | Null]', 'List[String]')).toBe(false);
	});

	it('Dict[String, Number] → Dict[String, Number | Null]: OK', () => {
		expect(isWeftTypeCompatible('Dict[String, Number]', 'Dict[String, Number | Null]')).toBe(true);
	});

	it('Dict[String, Number | Null] → Dict[String, Number]: fails', () => {
		expect(isWeftTypeCompatible('Dict[String, Number | Null]', 'Dict[String, Number]')).toBe(false);
	});

	it('nested: Dict[String, List[Number | Null]] compat', () => {
		expect(isWeftTypeCompatible(
			'Dict[String, List[Number]]',
			'Dict[String, List[Number | Null]]',
		)).toBe(true);
		expect(isWeftTypeCompatible(
			'Dict[String, List[Number | Null]]',
			'Dict[String, List[Number]]',
		)).toBe(false);
	});

	it('nested: List[Dict[String, String | Null]] compat', () => {
		expect(isWeftTypeCompatible(
			'List[Dict[String, String]]',
			'List[Dict[String, String | Null]]',
		)).toBe(true);
		expect(isWeftTypeCompatible(
			'List[Dict[String, String | Null]]',
			'List[Dict[String, String]]',
		)).toBe(false);
	});

	it('Media | Null', () => {
		const t = parseWeftType('Media | Null');
		expect(t?.kind).toBe('union');
		if (t?.kind === 'union') {
			expect(t.types.length).toBe(4); // 3 media + Null
		}
		expect(isWeftTypeCompatible('Image', 'Media | Null')).toBe(true);
		expect(isWeftTypeCompatible('Null', 'Media | Null')).toBe(true);
		expect(isWeftTypeCompatible('String', 'Media | Null')).toBe(false);
	});
});

describe('Media compatibility', () => {
	it('Image → Media: OK', () => {
		expect(isWeftTypeCompatible('Image', 'Media')).toBe(true);
	});

	it('Video → Media: OK', () => {
		expect(isWeftTypeCompatible('Video', 'Media')).toBe(true);
	});

	it('Audio → Media: OK', () => {
		expect(isWeftTypeCompatible('Audio', 'Media')).toBe(true);
	});

	it('Blob is NOT Media but IS File', () => {
		expect(isWeftTypeCompatible('Blob', 'Media')).toBe(false);
		expect(isWeftTypeCompatible('Blob', 'File')).toBe(true);
		expect(isWeftTypeCompatible('Image', 'File')).toBe(true);
	});

	it('String → Media: fails', () => {
		expect(isWeftTypeCompatible('String', 'Media')).toBe(false);
	});

	it('Number → Media: fails', () => {
		expect(isWeftTypeCompatible('Number', 'Media')).toBe(false);
	});

	it('Media → Image: fails (Video/Audio not handled)', () => {
		expect(isWeftTypeCompatible('Media', 'Image')).toBe(false);
	});

	it('Media → Media: OK', () => {
		expect(isWeftTypeCompatible('Media', 'Media')).toBe(true);
	});

	it('Image → Media | String: OK', () => {
		expect(isWeftTypeCompatible('Image', 'Media | String')).toBe(true);
	});

	it('String → Media | String: OK', () => {
		expect(isWeftTypeCompatible('String', 'Media | String')).toBe(true);
	});

	it('Number → Media | String: fails', () => {
		expect(isWeftTypeCompatible('Number', 'Media | String')).toBe(false);
	});

	it('List[Image] → List[Media]: OK (covariant)', () => {
		expect(isWeftTypeCompatible('List[Image]', 'List[Media]')).toBe(true);
	});

	it('List[Media] → List[Image]: fails', () => {
		expect(isWeftTypeCompatible('List[Media]', 'List[Image]')).toBe(false);
	});

	it('Dict[String, Image] → Dict[String, Media]: OK', () => {
		expect(isWeftTypeCompatible('Dict[String, Image]', 'Dict[String, Media]')).toBe(true);
	});

	it('Dict[String, Media] → Dict[String, Image]: fails', () => {
		expect(isWeftTypeCompatible('Dict[String, Media]', 'Dict[String, Image]')).toBe(false);
	});

	it('List[Media | String] → List[Media | String | Number]: OK', () => {
		expect(isWeftTypeCompatible('List[Media | String]', 'List[Media | String | Number]')).toBe(true);
	});

	it('List[Media | String | Number] → List[Media | String]: fails', () => {
		expect(isWeftTypeCompatible('List[Media | String | Number]', 'List[Media | String]')).toBe(false);
	});

	it('Dict[String, List[Media]] → Dict[String, List[Image]]: fails', () => {
		expect(isWeftTypeCompatible('Dict[String, List[Media]]', 'Dict[String, List[Image]]')).toBe(false);
	});

	it('Dict[String, List[Image]] → Dict[String, List[Media]]: OK', () => {
		expect(isWeftTypeCompatible('Dict[String, List[Image]]', 'Dict[String, List[Media]]')).toBe(true);
	});
});

describe('weftTypeToString roundtrip', () => {
	const cases = [
		'String',
		'List[String]',
		'List[List[Number]]',
		'Dict[String, Number]',
		'String | Number',
		'List[String] | Dict[String, Number]',
		'T',
		'T1',
		'List[T]',
		'MustOverride',
		'Access',
		'{ name: String, nickname?: String }',
	];

	for (const c of cases) {
		it(`roundtrips: ${c}`, () => {
			const parsed = parseWeftType(c);
			expect(parsed).not.toBeNull();
			const str = weftTypeToString(parsed!);
			const reparsed = parseWeftType(str);
			expect(reparsed).toEqual(parsed);
		});
	}
});

describe('isWeftTypeCompatible', () => {
	// Basic
	it('same type compatible', () => {
		expect(isWeftTypeCompatible('String', 'String')).toBe(true);
	});

	it('different primitives incompatible', () => {
		expect(isWeftTypeCompatible('String', 'Number')).toBe(false);
	});

	// Unions
	it('single into union compatible', () => {
		expect(isWeftTypeCompatible('String', 'String | Number')).toBe(true);
	});

	it('union into narrow incompatible', () => {
		expect(isWeftTypeCompatible('String | Number', 'String')).toBe(false);
	});

	it('same union compatible', () => {
		expect(isWeftTypeCompatible('String | Number', 'String | Number')).toBe(true);
	});

	it('subset union compatible', () => {
		expect(isWeftTypeCompatible('String | Number', 'String | Number | Boolean')).toBe(true);
	});

	it('superset union incompatible', () => {
		expect(isWeftTypeCompatible('String | Number | Boolean', 'String | Number')).toBe(false);
	});

	it('overlapping non-subset unions incompatible', () => {
		// String | Number → String | Boolean: Number not handled
		expect(isWeftTypeCompatible('String | Number', 'String | Boolean')).toBe(false);
	});

	// Lists
	it('same list compatible', () => {
		expect(isWeftTypeCompatible('List[String]', 'List[String]')).toBe(true);
	});

	it('different list inner incompatible', () => {
		expect(isWeftTypeCompatible('List[String]', 'List[Number]')).toBe(false);
	});

	// Dicts
	it('same dict compatible', () => {
		expect(isWeftTypeCompatible('Dict[String, Number]', 'Dict[String, Number]')).toBe(true);
	});

	it('different dict value incompatible', () => {
		expect(isWeftTypeCompatible('Dict[String, String]', 'Dict[String, Number]')).toBe(false);
	});

	// TypeVar/MustOverride always compatible (unresolved)
	it('TypeVar compatible with anything', () => {
		expect(isWeftTypeCompatible('T', 'String')).toBe(true);
		expect(isWeftTypeCompatible('String', 'T')).toBe(true);
	});

	it('MustOverride compatible with anything', () => {
		expect(isWeftTypeCompatible('MustOverride', 'String')).toBe(true);
		expect(isWeftTypeCompatible('String', 'MustOverride')).toBe(true);
	});

	// List compatibility. ('List into matching List' and 'element into
	// element' were exact duplicates of earlier cases (lines ~525 and
	// ~490) after a rename, deleted; these two are unique.)
	it('non-list into list incompatible', () => {
		expect(isWeftTypeCompatible('String', 'List[String]')).toBe(false);
	});

	it('wrong primitive types incompatible', () => {
		expect(isWeftTypeCompatible('Number', 'String')).toBe(false);
	});

	// Access connects only to Access.
	it('access into access compatible', () => {
		expect(isWeftTypeCompatible('Access', 'Access')).toBe(true);
	});
	it('access into anything else incompatible', () => {
		expect(isWeftTypeCompatible('Access', 'String')).toBe(false);
		expect(isWeftTypeCompatible('String', 'Access')).toBe(false);
	});

	// Named types are nominal; the mirror of the backend semantics.
	// SYNC assertions with crates/weft-core/src/weft_type.rs is_compatible.
	it('same-name named types compatible', () => {
		expect(isWeftTypeCompatible('Profile={ name: String }', 'Profile={ name: String }')).toBe(true);
	});
	it('different-name named types incompatible', () => {
		expect(isWeftTypeCompatible('Profile={ name: String }', 'Person={ name: String }')).toBe(false);
	});
	it('named decays into a structural target', () => {
		expect(isWeftTypeCompatible('Profile={ name: String }', 'JsonDict')).toBe(true);
		expect(isWeftTypeCompatible('Profile={ name: String }', '{ name: String }')).toBe(true);
	});
	it('nothing unnamed flows into a named target', () => {
		expect(isWeftTypeCompatible('JsonDict', 'Profile={ name: String }')).toBe(false);
		expect(isWeftTypeCompatible('{ name: String }', 'Profile={ name: String }')).toBe(false);
	});
	it('named matches its arm inside a union target', () => {
		expect(isWeftTypeCompatible('Profile={ name: String }', 'Profile={ name: String } | Null')).toBe(true);
	});

	// Record contract: strict unknown keys, optionality respected.
	it('record with an unknown key incompatible', () => {
		expect(isWeftTypeCompatible('{ name: String, extra: Number }', '{ name: String }')).toBe(false);
	});
	it('record missing an optional target field compatible', () => {
		expect(isWeftTypeCompatible('{ name: String }', '{ name: String, nickname?: String }')).toBe(true);
	});
	it('record missing a required target field incompatible', () => {
		expect(isWeftTypeCompatible('{ name: String }', '{ name: String, age: Number }')).toBe(false);
	});
	it('record forgets into JsonDict, never the reverse', () => {
		expect(isWeftTypeCompatible('{ name: String }', 'JsonDict')).toBe(true);
		expect(isWeftTypeCompatible('JsonDict', '{ name: String }')).toBe(false);
	});
});

describe('wire form round-trips', () => {
	// SYNC assertions with crates/weft-core/src/tests/weft_type_tests.rs named_union_body_wire_form_is_unambiguous
	it('a named union body parenthesizes and round-trips as one named type', () => {
		const t = parseWeftType('Kind=(String | Number)');
		expect(t).toEqual({
			kind: 'named',
			name: 'Kind',
			body: { kind: 'union', types: [
				{ kind: 'primitive', value: 'String' },
				{ kind: 'primitive', value: 'Number' },
			] },
		});
		expect(weftTypeToWireString(t!)).toBe('Kind=(String | Number)');
	});

	it('the un-parenthesized spelling is a union with a named member', () => {
		const t = parseWeftType('Kind=String | Number');
		expect(t).toEqual({
			kind: 'union',
			types: [
				{ kind: 'named', name: 'Kind', body: { kind: 'primitive', value: 'String' } },
				{ kind: 'primitive', value: 'Number' },
			],
		});
		expect(weftTypeToWireString(t!)).toBe('Kind=String | Number');
	});

	it('a record-bodied named type beside Null round-trips (braces protect)', () => {
		const wire = 'Profile={ name: String } | Null';
		const t = parseWeftType(wire);
		expect(t?.kind).toBe('union');
		expect(parseWeftType(weftTypeToWireString(t!))).toEqual(t);
	});

	it('paren grouping parses and nests', () => {
		expect(parseWeftType('(String | Number)')).toEqual(parseWeftType('String | Number'));
		expect(parseWeftType('((String))')).toEqual({ kind: 'primitive', value: 'String' });
	});

	it('unions drop Empty when another member remains', () => {
		expect(parseWeftType('String | Empty')).toEqual({ kind: 'primitive', value: 'String' });
		expect(parseWeftType('Empty | Number')).toEqual({ kind: 'primitive', value: 'Number' });
		expect(parseWeftType('Empty')).toEqual({ kind: 'primitive', value: 'Empty' });
	});
});

describe('runtime handle inference', () => {
	it('infers Bus and Access from their sentinel markers', () => {
		expect(inferTypeFromValue({ __weft_bus__: { id: 'a', mode: 'journaled' } })).toEqual({ kind: 'bus' });
		expect(inferTypeFromValue({ __weft_access__: {} })).toEqual({ kind: 'access' });
	});
});

describe('named display form', () => {
	// Display renders the BARE name (the authored form); only the wire
	// form (`Name=Body`) reparses frontend-side, by design: the editor
	// receives wire forms from the backend and shows bare names to the
	// user, and edit ops re-resolve bare names against the project
	// registry backend-side.
	it('renders a named type as its bare name', () => {
		const t = parseWeftType('Profile={ name: String }');
		expect(weftTypeToString(t!)).toBe('Profile');
	});
});

describe('extractPrimitives', () => {
	it('extracts from primitive', () => {
		expect(extractPrimitives(parseWeftType('String')!)).toEqual(['String']);
	});

	it('extracts from list', () => {
		expect(extractPrimitives(parseWeftType('List[Number]')!)).toEqual(['Number']);
	});

	it('extracts from union', () => {
		const prims = extractPrimitives(parseWeftType('String | Number')!);
		expect(prims).toContain('String');
		expect(prims).toContain('Number');
	});

	it('returns empty for TypeVar', () => {
		expect(extractPrimitives(parseWeftType('T')!)).toEqual([]);
	});

	it('returns empty for MustOverride', () => {
		expect(extractPrimitives(parseWeftType('MustOverride')!)).toEqual([]);
	});
});

// =========================================================================
// inferTypeFromValue
// =========================================================================

describe('inferTypeFromValue', () => {
	it('infers primitives', () => {
		expect(inferTypeFromValue('hello')).toEqual({ kind: 'primitive', value: 'String' });
		expect(inferTypeFromValue(42)).toEqual({ kind: 'primitive', value: 'Number' });
		expect(inferTypeFromValue(3.14)).toEqual({ kind: 'primitive', value: 'Number' });
		expect(inferTypeFromValue(true)).toEqual({ kind: 'primitive', value: 'Boolean' });
		expect(inferTypeFromValue(false)).toEqual({ kind: 'primitive', value: 'Boolean' });
		expect(inferTypeFromValue(null)).toEqual({ kind: 'primitive', value: 'Null' });
		expect(inferTypeFromValue(undefined)).toEqual({ kind: 'primitive', value: 'Null' });
	});

	it('infers empty array as List[Empty]', () => {
		const t = inferTypeFromValue([]);
		expect(t.kind).toBe('list');
		if (t.kind === 'list') {
			expect(t.inner).toEqual({ kind: 'primitive', value: 'Empty' });
		}
	});

	it('infers homogeneous array as List[Element]', () => {
		const t = inferTypeFromValue(['a', 'b', 'c']);
		expect(t.kind).toBe('list');
		if (t.kind === 'list') {
			expect(t.inner).toEqual({ kind: 'primitive', value: 'String' });
		}
	});

	it('infers heterogeneous array as List[Union]', () => {
		const t = inferTypeFromValue(['hello', 42]);
		expect(t.kind).toBe('list');
		if (t.kind === 'list') {
			expect(t.inner.kind).toBe('union');
			if (t.inner.kind === 'union') {
				expect(t.inner.types).toHaveLength(2);
			}
		}
	});

	it('infers empty object as Dict[String, Empty]', () => {
		const t = inferTypeFromValue({});
		expect(t.kind).toBe('dict');
		if (t.kind === 'dict') {
			expect(t.key).toEqual({ kind: 'primitive', value: 'String' });
			expect(t.value).toEqual({ kind: 'primitive', value: 'Empty' });
		}
	});

	it('infers object with homogeneous values', () => {
		const t = inferTypeFromValue({ a: 1, b: 2, c: 3 });
		expect(t.kind).toBe('dict');
		if (t.kind === 'dict') {
			expect(t.value).toEqual({ kind: 'primitive', value: 'Number' });
		}
	});

	it('infers object with heterogeneous values as Dict[String, Union]', () => {
		const t = inferTypeFromValue({ name: 'Alice', age: 30 });
		expect(t.kind).toBe('dict');
		if (t.kind === 'dict') {
			expect(t.value.kind).toBe('union');
		}
	});

	it('detects stored-file objects by their concrete marker', () => {
		// The marker IS the type; a bare {url, mimeType} (no marker) is NOT
		// a stored file, it types as a Dict.
		expect(inferTypeFromValue({ __weft_image__: { url: 'http://x/i', mimeType: 'image/png' } }))
			.toEqual({ kind: 'primitive', value: 'Image' });
		expect(inferTypeFromValue({ __weft_video__: { url: 'http://x/v', mimeType: 'video/mp4' } }))
			.toEqual({ kind: 'primitive', value: 'Video' });
		expect(inferTypeFromValue({ __weft_audio__: { url: 'http://x/a', mimeType: 'audio/wav' } }))
			.toEqual({ kind: 'primitive', value: 'Audio' });
		expect(inferTypeFromValue({ __weft_blob__: { key: 'exec/c/x', mimeType: 'application/pdf' } }))
			.toEqual({ kind: 'primitive', value: 'Blob' });
		// No marker -> not a stored file.
		expect(inferTypeFromValue({ url: 'http://x/i', mimeType: 'image/png' }).kind).toBe('dict');
	});

	it('does not detect a stored file without a handle', () => {
		const t = inferTypeFromValue({ mimeType: 'image/png', size: 1024 });
		expect(t.kind).toBe('dict');
	});

	it('infers nested structures', () => {
		const t = inferTypeFromValue({ vendors: [{ name: 'Acme', score: 92 }] });
		expect(t.kind).toBe('dict');
		if (t.kind === 'dict') {
			// value is List[Dict[String, String|Number]]
			expect(t.value.kind).toBe('list');
		}
	});
});

// =========================================================================
// Mock type compatibility (inferTypeFromValue + isCompatible)
// =========================================================================

describe('mock type compatibility', () => {
	function mockPortCompatible(value: unknown, portTypeStr: string): boolean {
		const inferred = inferTypeFromValue(value);
		const expected = parseWeftType(portTypeStr);
		if (!expected) return false;
		return isCompatible(inferred, expected);
	}

	it('string mock matches String port', () => {
		expect(mockPortCompatible('hello', 'String')).toBe(true);
	});

	it('number mock matches Number port', () => {
		expect(mockPortCompatible(42, 'Number')).toBe(true);
	});

	it('string mock does NOT match Number port', () => {
		expect(mockPortCompatible('hello', 'Number')).toBe(false);
	});

	it('number mock does NOT match String port', () => {
		expect(mockPortCompatible(42, 'String')).toBe(false);
	});

	it('string array matches List[String]', () => {
		expect(mockPortCompatible(['a', 'b'], 'List[String]')).toBe(true);
	});

	it('number array does NOT match List[String]', () => {
		expect(mockPortCompatible([1, 2], 'List[String]')).toBe(false);
	});

	it('mixed array does NOT match List[String]', () => {
		expect(mockPortCompatible(['a', 1], 'List[String]')).toBe(false);
	});

	it('string matches String | Number union', () => {
		expect(mockPortCompatible('hello', 'String | Number')).toBe(true);
	});

	it('boolean does NOT match String | Number union', () => {
		expect(mockPortCompatible(true, 'String | Number')).toBe(false);
	});

	it('any value matches TypeVar T', () => {
		expect(mockPortCompatible('anything', 'T')).toBe(true);
		expect(mockPortCompatible(42, 'T')).toBe(true);
		expect(mockPortCompatible([1, 2], 'T')).toBe(true);
	});

	it('any value matches MustOverride', () => {
		expect(mockPortCompatible('anything', 'MustOverride')).toBe(true);
	});

	it('nested dict matches Dict[String, Number]', () => {
		expect(mockPortCompatible({ a: 1, b: 2 }, 'Dict[String, Number]')).toBe(true);
	});

	it('nested dict with wrong value type fails', () => {
		expect(mockPortCompatible({ a: 'not a number' }, 'Dict[String, Number]')).toBe(false);
	});

	it('null array elements are type errors for List[String]', () => {
		// [null] infers as List[Null] which is not compatible with List[String]
		expect(mockPortCompatible([null], 'List[String]')).toBe(false);
	});

	it('empty array matches any List type (generic T)', () => {
		expect(mockPortCompatible([], 'List[String]')).toBe(true);
		expect(mockPortCompatible([], 'List[Number]')).toBe(true);
	});

	// Regression: frontend was missing typevar/MustOverride guards,
	// causing false-positive validation errors
	it('any value matches TypeVar T (regression)', () => {
		expect(mockPortCompatible('hello', 'T')).toBe(true);
		expect(mockPortCompatible(42, 'T')).toBe(true);
		expect(mockPortCompatible([1, 2], 'T')).toBe(true);
		expect(mockPortCompatible({ a: 1 }, 'T')).toBe(true);
		expect(mockPortCompatible(null, 'T')).toBe(true);
	});

	it('any value matches MustOverride (regression)', () => {
		expect(mockPortCompatible('anything', 'MustOverride')).toBe(true);
		expect(mockPortCompatible(42, 'MustOverride')).toBe(true);
		expect(mockPortCompatible([1], 'MustOverride')).toBe(true);
	});

	// Regression: Null at top level was type-checked instead of skipped,
	// producing "Null but port expects Boolean" errors.
	// Note: this test verifies the raw incompatibility. The actual skip
	// happens in validation.ts (top-level null -> continue).
	it('null is incompatible with Boolean at type level', () => {
		expect(mockPortCompatible(null, 'Boolean')).toBe(false);
	});

	it('null is incompatible with String at type level', () => {
		expect(mockPortCompatible(null, 'String')).toBe(false);
	});

	// Branching pattern: real-world mock with mixed null and values
	it('branching mock pattern validates correctly per-port', () => {
		const mock = { approved: true, rejected: null, notes: 'looks good' };
		const ports: [string, string][] = [
			['approved', 'Boolean'],
			['rejected', 'Boolean'],
			['notes', 'String'],
		];
		for (const [portName, portType] of ports) {
			const val = (mock as Record<string, unknown>)[portName];
			if (val === null) continue; // top-level null = port doesn't fire
			expect(mockPortCompatible(val, portType)).toBe(true);
		}
	});
});

// ── JsonDict tests ──────────────────────────────────────────────────

describe('JsonDict', () => {
	it('parses JsonDict', () => {
		const t = parseWeftType('JsonDict');
		expect(t?.kind).toBe('json_dict');
	});

	it('parses List[JsonDict]', () => {
		const t = parseWeftType('List[JsonDict]');
		expect(t?.kind).toBe('list');
		if (t?.kind === 'list') {
			expect(t.inner.kind).toBe('json_dict');
		}
	});

	it('displays as JsonDict', () => {
		const t = parseWeftType('JsonDict')!;
		expect(weftTypeToString(t)).toBe('JsonDict');
	});

	it('roundtrips List[JsonDict]', () => {
		const t = parseWeftType('List[JsonDict]')!;
		expect(weftTypeToString(t)).toBe('List[JsonDict]');
	});

	it('is compatible with Dict[String, V] in both directions', () => {
		const jd = parseWeftType('JsonDict')!;
		const dictStrNum = parseWeftType('Dict[String, Number]')!;
		const dictStrBool = parseWeftType('Dict[String, Boolean]')!;
		const dictStrNested = parseWeftType('Dict[String, Dict[String, Number]]')!;
		expect(isCompatible(jd, dictStrNum)).toBe(true);
		expect(isCompatible(jd, dictStrBool)).toBe(true);
		expect(isCompatible(jd, dictStrNested)).toBe(true);
		expect(isCompatible(dictStrNum, jd)).toBe(true);
		expect(isCompatible(dictStrBool, jd)).toBe(true);
		expect(isCompatible(dictStrNested, jd)).toBe(true);
		expect(isCompatible(jd, jd)).toBe(true);
	});

	it('is not compatible with non-dict types', () => {
		const jd = parseWeftType('JsonDict')!;
		expect(isCompatible(jd, parseWeftType('String')!)).toBe(false);
		expect(isCompatible(jd, parseWeftType('Number')!)).toBe(false);
		expect(isCompatible(jd, parseWeftType('List[String]')!)).toBe(false);
		expect(isCompatible(parseWeftType('String')!, jd)).toBe(false);
	});

	it('List[JsonDict] is compatible with List[Dict[String, V]]', () => {
		const listJd = parseWeftType('List[JsonDict]')!;
		const listDict = parseWeftType('List[Dict[String, Number]]')!;
		expect(isCompatible(listJd, listDict)).toBe(true);
		expect(isCompatible(listDict, listJd)).toBe(true);
	});

	it('JsonDict in union works', () => {
		const union = parseWeftType('JsonDict | String')!;
		expect(isCompatible(parseWeftType('Dict[String, Number]')!, union)).toBe(true);
		expect(isCompatible(parseWeftType('String')!, union)).toBe(true);
		expect(isCompatible(parseWeftType('Number')!, union)).toBe(false);
	});

	it('runtime dict value is compatible with JsonDict', () => {
		const jd = parseWeftType('JsonDict')!;
		const inferred = inferTypeFromValue({ name: 'test', nested: { a: 1 }, tags: ['x'] });
		expect(isCompatible(inferred, jd)).toBe(true);
	});

	it('runtime non-dict value is not compatible with JsonDict', () => {
		const jd = parseWeftType('JsonDict')!;
		expect(isCompatible(inferTypeFromValue('hello'), jd)).toBe(false);
		expect(isCompatible(inferTypeFromValue(42), jd)).toBe(false);
		expect(isCompatible(inferTypeFromValue([1, 2]), jd)).toBe(false);
	});
});

describe('inferTypeFromValue no typevars', () => {
	it('empty array infers List[Empty]', () => {
		const t = inferTypeFromValue([]);
		expect(weftTypeToString(t)).toBe('List[Empty]');
	});

	it('empty object infers Dict[String, Empty]', () => {
		const t = inferTypeFromValue({});
		expect(weftTypeToString(t)).toBe('Dict[String, Empty]');
	});

	it('empty list is compatible with any List[X]', () => {
		const empty = inferTypeFromValue([]);
		expect(isCompatible(empty, parseWeftType('List[String]')!)).toBe(true);
		expect(isCompatible(empty, parseWeftType('List[Number]')!)).toBe(true);
		expect(isCompatible(empty, parseWeftType('List[Boolean]')!)).toBe(true);
		expect(isCompatible(empty, parseWeftType('List[Dict[String, Number]]')!)).toBe(true);
		expect(isCompatible(empty, parseWeftType('List[JsonDict]')!)).toBe(true);
	});

	it('empty dict is compatible with any Dict[String, X]', () => {
		const empty = inferTypeFromValue({});
		expect(isCompatible(empty, parseWeftType('Dict[String, String]')!)).toBe(true);
		expect(isCompatible(empty, parseWeftType('Dict[String, Number]')!)).toBe(true);
		expect(isCompatible(empty, parseWeftType('JsonDict')!)).toBe(true);
	});

	it('null list elements are NOT compatible with typed list', () => {
		const nullList = inferTypeFromValue([null, null]);
		expect(isCompatible(nullList, parseWeftType('List[String]')!)).toBe(false);
		expect(isCompatible(nullList, parseWeftType('List[Null]')!)).toBe(true);
	});

	it('Empty only works as source, not target', () => {
		const empty: WeftType = { kind: 'primitive', value: 'Empty' };
		const str: WeftType = { kind: 'primitive', value: 'String' };
		// Empty → String: yes
		expect(isCompatible(empty, str)).toBe(true);
		// String → Empty: no
		expect(isCompatible(str, empty)).toBe(false);
	});

	it('nested empty arrays produce no typevars', () => {
		const t = inferTypeFromValue({ items: [], name: 'test' });
		const s = weftTypeToString(t);
		expect(s).not.toContain(' T');
		expect(s).not.toMatch(/T\]/);
	});

	it('union dedup is structural: field order and named bodies do not fork members', () => {
		// The backend's equality compares record fields as an unordered
		// set (a rendered-string compare kept both spellings of the
		// same record as separate members) and named types by name
		// alone (which even the display string got right by accident;
		// the record case is the one that regresses).
		const reordered = parseWeftType('{ a: String, b: Number } | { b: Number, a: String }');
		expect(reordered?.kind).toBe('record');
		const named = parseWeftType('K=String | K=Number');
		expect(named?.kind).toBe('named');
		// Direct equality checks too, for the two deliberate departures.
		const r1 = parseWeftType('{ a: String, b?: Number }')!;
		const r2 = parseWeftType('{ b?: Number, a: String }')!;
		expect(weftTypesEqual(r1, r2)).toBe(true);
		expect(weftTypesEqual(parseWeftType('K=String')!, parseWeftType('K=Number')!)).toBe(true);
		expect(weftTypesEqual(r1, parseWeftType('{ a: String, b: Number }')!)).toBe(false);
	});

	it('complex nested structure with empties produces no typevars', () => {
		const value = {
			users: [
				{ name: 'Alice', tags: [], meta: {} },
				{ name: 'Bob', tags: ['admin'], meta: { role: 'dev' } },
			],
			empty: [],
		};
		const t = inferTypeFromValue(value);
		const s = weftTypeToString(t);
		expect(s).not.toContain(' T');
		expect(s).not.toMatch(/T\]/);
	});
});
