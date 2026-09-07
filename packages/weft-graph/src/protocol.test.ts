import { describe, expect, it } from 'vitest';
import { parseFileValue, typeReferencesFile, type CatalogEntry, type Widget } from './protocol';

describe('typeReferencesFile', () => {
	it('matches file primitives and aliases, alone and in composites', () => {
		for (const t of ['Image', 'Video', 'Audio', 'Blob', 'Media', 'File', 'List[Image]', 'Image | Null', 'Dict[String, Audio]']) {
			expect(typeReferencesFile(t), t).toBe(true);
		}
	});

	it('rejects text types, including ones containing a file-kind substring', () => {
		for (const t of ['String', 'Number', 'JsonDict', 'List[String]', 'ImageMeta', 'Filename']) {
			expect(typeReferencesFile(t), t).toBe(false);
		}
	});

	it('answers structurally: a dict KEY is an index, never a stored value', () => {
		// Mirrors Rust's Dict(_, v) arm; the old token regex could not
		// tell the two positions apart.
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
			const parsed = parseFileValue({ [marker]: { key: 'exec/c1/f1', ...meta } });
			expect(parsed, marker).toEqual({ key: 'exec/c1/f1', ...meta });
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

	it('returns null for data-backed markers and non-file values', () => {
		expect(parseFileValue({ __weft_image__: { data: 'aGk=', ...meta } })).toBeNull();
		expect(parseFileValue({ __weft_image__: { key: '', ...meta } })).toBeNull();
		expect(parseFileValue({ notAMarker: { key: 'k', ...meta } })).toBeNull();
		expect(parseFileValue('exec/c1/f1')).toBeNull();
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
			requires_infra: true,
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
				            sources: [{ kind: 'granted', from: 'sheets', label: 'label', value: 'id' }], depends_on: ['pick'] } },
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
