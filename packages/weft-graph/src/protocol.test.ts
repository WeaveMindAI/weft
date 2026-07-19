import { describe, expect, it } from 'vitest';
import { parseFileValue, typeReferencesFile } from './protocol';

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
