import { describe, expect, it } from 'vitest';
import { describeOwnerTriggers, narrowingBound, TAKE_DOWN_DEFAULT, parseFileValue, tryParseWeftType, typeReferencesFile, type CatalogEntry, type NodeDefinition, type OutputSpec, type Widget } from './protocol';
import { declarationProblem, declarationRule } from './webview/lib/projection/header-ports';
import { NODE_TYPE_CONFIG, registerCatalog } from './webview/lib/nodes/index.svelte';

describe('TAKE_DOWN_DEFAULT', () => {
  it('destroys nothing: the door takes, what is held stays, the runs going are waited for', () => {
    expect(TAKE_DOWN_DEFAULT).toEqual({ door: 'take', held: 'keep', running: 'wait' });
  });
});

describe('describeOwnerTriggers', () => {
  it('names the program\'s own triggers as they are', () => {
    expect(describeOwnerTriggers([{ owner: { kind: 'shared' }, triggers: ['door', 'feed'] }])).toBe("'door', 'feed'");
  });

  it('names an instance\'s triggers with the instance, owners apart', () => {
    expect(
      describeOwnerTriggers([
        { owner: { kind: 'shared' }, triggers: ['door'] },
        { owner: { kind: 'instance', instance: 'ada' }, triggers: ['inbox'] },
      ]),
    ).toBe("'door'; 'inbox' of instance 'ada'");
  });
});

describe('typeReferencesFile', () => {
	it('matches file primitives and aliases, alone and in composites', () => {
		for (const t of ['Image', 'Video', 'Audio', 'Blob', 'Media', 'File', 'List[Image]', 'Image | Null', 'Dict[String, Audio]']) {
			expect(typeReferencesFile(t), t).toBe(true);
		}
	});

	it('rejects text types, including ones containing a file-kind substring', () => {
		for (const t of ['String', 'Number', 'JsonDict', 'Json', 'List[Json]', 'List[String]', 'ImageMeta', 'Filename']) {
			expect(typeReferencesFile(t), t).toBe(false);
		}
	});

	it('answers structurally from a dict value, and a dict keyed by a file is no type', () => {
		expect(typeReferencesFile('Dict[String, Image]')).toBe(true);
		expect(typeReferencesFile('Dict[Image, String]')).toBe(false);
	});

	it('rejects a stream whatever its element mentions, through chained aliases too', () => {
		// A generator port's value is a live handle, never a file; wire
		// aliases spell as `Name=Body` and chain.
		for (const t of ['Generator[Image]', 'Frames=Generator[Image]', 'F2=Frames=Generator[Image]']) {
			expect(typeReferencesFile(t), t).toBe(false);
		}
	});
});

// SYNC-covered peer: crates/weft-core/src/storage/mod.rs FileHandle::from_value
describe('parseFileValue', () => {
	const meta = { mimeType: 'image/png', sizeBytes: 8, filename: 'pic.png' };

	it('parses a key-backed file value from any concrete marker', () => {
		for (const marker of ['__weft_image__', '__weft_video__', '__weft_audio__', '__weft_blob__']) {
			const parsed = parseFileValue({ [marker]: { key: 'run/c1/f1', ...meta } });
			expect(parsed, marker).toEqual({ key: 'run/c1/f1', ...meta });
		}
	});

	it('parses a url-backed file value', () => {
		const parsed = parseFileValue({ __weft_image__: { url: 'https://x/pic.png', ...meta } });
		expect(parsed).toEqual({ url: 'https://x/pic.png', ...meta });
	});

	it('prefers key when both handles are present (bucket copy is authoritative)', () => {
		const parsed = parseFileValue({ __weft_image__: { key: 'k', url: 'https://x', ...meta } });
		expect(parsed?.key).toBe('k');
		expect(parsed?.url).toBeUndefined();
	});

	it('carries a key-backed value\'s link lifetime, and refuses one that is not whole seconds', () => {
		expect(parseFileValue({ __weft_image__: { key: 'k', linkSecs: 7200, ...meta } })).toEqual({ key: 'k', linkSecs: 7200, ...meta });
		expect(parseFileValue({ __weft_image__: { key: 'k', linkSecs: null, ...meta } })).toEqual({ key: 'k', ...meta });
		expect(parseFileValue({ __weft_image__: { key: 'k', linkSecs: 1.5, ...meta } })).toBeNull();
		expect(parseFileValue({ __weft_image__: { key: 'k', linkSecs: '60', ...meta } })).toBeNull();
		expect(parseFileValue({ __weft_image__: { key: 'k', linkSecs: -1, ...meta } })).toBeNull();
	});

	it('returns null for data-backed markers and non-file values', () => {
		expect(parseFileValue({ __weft_image__: { data: 'aGk=', ...meta } })).toBeNull();
		expect(parseFileValue({ __weft_image__: { key: '', ...meta } })).toBeNull();
		expect(parseFileValue({ notAMarker: { key: 'k', ...meta } })).toBeNull();
		expect(parseFileValue('run/c1/f1')).toBeNull();
		expect(parseFileValue(null)).toBeNull();
	});
});

describe('catalog wire fixture', () => {
	// Layer-2 wire-shape, the TS half: the exact JSON the backend's
	// `resolved()` metadata serializes (pinned by the Rust twin) must
	// satisfy `CatalogEntry`. A field the mirror requires but the
	// backend stopped sending (or renamed) fails this assignment at
	// compile time.
	// SYNC: catalog wire fixture <-> crates/weft-core/src/node.rs catalog_wire_tests
	it('the backend catalog payload satisfies CatalogEntry', () => {
		const fixture: CatalogEntry = {
			type: 'Fixture', label: 'Fixture', description: 'd',
			tags: ['a'], icon: 'Zap', color: '#123456',
			requiresInfra: true,
			inputs: [
				{ name: 'code', type: 'String', required: true, accepts: ['literal', 'wire'],
				  widget: { kind: 'code', language: 'python' } },
				{ name: 'pick', type: 'String', accepts: ['literal', 'wire'],
				  widget: { kind: 'select', options: ['a', 'b'] } },
				{ name: 'n', type: 'Number', accepts: ['literal', 'wire'],
				  widget: { kind: 'number', min: 0, max: 9, step: 1 } },
				{ name: 'grant', type: 'Access', accepts: ['literal'],
				  widget: { kind: 'access' },
				  requiresScopes: ['s.read'], requiresValues: ['host'] },
				{ name: 'sheet', type: 'String', accepts: ['literal', 'wire'],
				  widget: { kind: 'remote_select', access: 'grant',
				            sources: [{ kind: 'granted', from: 'sheets', label: 'label', value: 'id' }], dependsOn: ['pick'] } },
				{ name: 'img', type: 'Image', accepts: ['literal', 'wire'],
				  widget: { kind: 'file_drop', type: 'Image', accept: 'image/png' } },
			],
			outputs: [{ name: 'out', type: 'String' }],
			features: { oneOfRequired: [['code', 'img']], isTrigger: true,
			            canAddInputPorts: true,
			            showDebugPreview: true, liveEndpoint: 'web' },
			display: { kind: 'media', output: 'out' },
			portsFromConfig: {
				field: 'fields',
				matchInput: 'n',
				specs: [
					{ kind: 'text', keyField: 'key', label: 'Text',
					  render: { component: 'text_input', source: 'input', multiple: true },
					  // A metadata author may write a field as a bare name
					  // (`"label"`); the resolved metadata expands it into
					  // the whole declaration before the editor sees it.
					  fields: [
						{ key: 'label', label: 'Label', required: false,
						  shape: 'typed', valueType: 'String',
						  widget: { kind: 'text' } },
						{ key: 'options', label: 'Options', required: true,
						  shape: 'typed', valueType: 'List[String]',
						  widget: { kind: 'text_list' } },
						{ key: 'at_least', label: 'At least', shape: 'number',
						  widget: { kind: 'number' } },
					  ],
					  catchAll: true,
					  addsInputs: [],
					  addsOutputs: [{ nameTemplate: '{key}', portType: 'String' }] },
				],
			},
		};
		expect(fixture.type).toBe('Fixture');
	});

	it('every Widget kind is handled exhaustively', () => {
		// A new Rust Widget variant mirrored into the union breaks this
		// switch at compile time until someone routes it.
		const label = (w: Widget): string => {
			switch (w.kind) {
				case 'text': case 'textarea': case 'checkbox': case 'password':
				case 'entry_list': case 'text_list': case 'code': case 'number': case 'select':
				case 'multiselect': case 'access': case 'remote_select': case 'file_drop': case 'datetime':
					return w.kind;
				default: {
					const unhandled: never = w;
					return unhandled;
				}
			}
		};
		expect(label({ kind: 'text' })).toBe('text');
	});
});

describe('narrowable outputs and type parameters on the wire', () => {
	it('reads an output narrowing bound', () => {
		const route: OutputSpec[] = [
			{ name: 'answer', type: 'JsonDict', narrowable: true },
			{ name: 'body', type: 'JsonDict', narrowable: 'JsonDict | String | File' },
			{ name: 'status', type: 'Number' },
		];
		expect(route.map(narrowingBound)).toEqual(['JsonDict', 'JsonDict | String | File', null]);
	});

	it("holds a declaration to a catalog entry's bounds and to a node's resolved ones", () => {
		const entry = {
			type: 'KeepFile', label: 'Keep File', description: '', tags: [],
			inputs: [{ name: 'file', type: 'T', required: true }, { name: 'more', type: 'T2', required: true }],
			outputs: [],
			typeParams: { T: 'Media', T2: 'List[T]' },
		} as unknown as CatalogEntry;
		registerCatalog({ KeepFile: entry });
		const template = NODE_TYPE_CONFIG.KeepFile;
		const file = declarationRule('input', template.defaultInputs[0], template.typeParams);
		expect(declarationProblem(file, 'String')).toBe('`T` has to fit in Media, and what is connected makes it String');
		expect(declarationProblem(file, 'Image')).toBeNull();

		const node: Pick<NodeDefinition, 'typeParams'> = {
			typeParams: [{ name: 'T', bound: 'Media', resolved: 'Video' }, { name: 'T2', bound: 'List[T]' }],
		};
		const more = declarationRule('input', template.defaultInputs[1], node.typeParams);
		expect(declarationProblem(more, 'List[Video]')).toBeNull();
		expect(declarationProblem(more, 'List[Image]')).toBe('`T2` has to fit in List[Video], and what is connected makes it List[Image]');
	});
});

describe('tryParseWeftType leaves only declarable unknown names to the compiler', () => {
	const refusal = (s: string) => {
		const parsed = tryParseWeftType(s);
		if (parsed.ok) throw new Error(`${s} parsed`);
		return parsed;
	};

	it('an unknown declarable name is the compiler\'s, wherever it sits', () => {
		for (const s of ['Verdict', 'List[Verdict]', 'Verdict | Number', '{ a: Verdict, b?: Score }', 'Dict[String, Verdict]']) {
			expect(refusal(s).unknownName, s).toBe(true);
		}
		expect(refusal('Verdict | Score').error).toBe('`Verdict` is not a type');
	});

	// SYNC assertions with crates/weft-core/src/weft_type.rs parse_single_type
	it('a name no project can declare is refused outright', () => {
		for (const s of ['List', 'Dict', 'Generator', 'string', 'List[string]']) {
			expect(refusal(s).unknownName, s).toBe(false);
		}
		expect(refusal('List').error).toBe('`List` is not a type');
		expect(refusal('string').error).toBe('`string` is not a type');
	});

	it('a definite error elsewhere wins over an unknown name before it', () => {
		const key = refusal('Dict[Number, Verdict]');
		expect(key.unknownName).toBe(false);
		expect(key.error).toBe(
			"`Dict[Number, Verdict]`: a dict's keys are always text, so its key type is `String`. "
			+ 'To key by a Number, use `List[{key: Number, value: Verdict}]`',
		);
		const foo = refusal('Verdict | Foo[String]');
		expect(foo.unknownName).toBe(false);
		expect(foo.error).toBe('`Foo[...]` is not a type; the types that take one are List, Generator and Dict');
		const field = refusal('{a: Verdict, b}');
		expect(field.unknownName).toBe(false);
		expect(field.error).toBe('`{a: Verdict, b}`: the field `b` has no type (`name: Type`)');
	});

	it('a declared name that is no type name says why, like the compiler', () => {
		expect(refusal('verdict=String').error).toBe(
			'`verdict=String`: `verdict` is not a type name: a type name starts with an uppercase letter '
			+ 'and contains only letters, digits, and underscores',
		);
		expect(refusal('List=String').error).toBe('`List=String`: `List` is not a type name: `List` is reserved by the type language');
		expect(refusal('Media=String').error).toBe('`Media=String`: `Media` is not a type name: `Media` is reserved by the type language');
		expect(refusal('Json=String').error).toBe('`Json=String`: `Json` is not a type name: `Json` is reserved by the type language');
		expect(refusal('Kind=Verdict').unknownName).toBe(true);
	});
});
