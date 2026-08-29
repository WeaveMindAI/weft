import { describe, it, expect } from 'vitest';
import { fileRefsOf, formatConfigValue, parseConfigToken, type WeftFileRefValue } from './value-format';

/** format then parse must recover the original value (the two are documented as
 *  exact inverses). */
function roundTrip(value: unknown): unknown {
  return parseConfigToken(formatConfigValue(value));
}

describe('formatConfigValue / parseConfigToken round-trip', () => {
  it('round-trips scalars', () => {
    for (const v of ['hello', '', 'with "quotes"', 'back\\slash', true, false, 0, -3, 3.14, 1e21, 1e-7, -0.5]) {
      expect(roundTrip(v)).toEqual(v);
    }
  });

  it('round-trips a multi-line string as a heredoc', () => {
    const v = 'line one\nline two\nno fence inside';
    expect(roundTrip(v)).toEqual(v);
  });

  it('round-trips a multi-line string containing a fence via the escape', () => {
    // Mirrors the Rust edit-server: an inner ``` encodes as \``` and
    // round-trips; only the escape's own literal spelling is refused.
    const v = 'a\n```\nb';
    expect(formatConfigValue(v)).toBe('```\na\n\\```\nb\n```');
    expect(roundTrip(v)).toEqual(v);
    expect(() => formatConfigValue('a\n\\```\nb')).toThrow(/fence escape/);
  });

  it('round-trips indentation and trailing spaces verbatim', () => {
    const v = '    line1\n    line2   ';
    expect(roundTrip(v)).toEqual(v);
  });

  it('round-trips objects and arrays', () => {
    expect(roundTrip({ a: 1, b: ['x', 'y'], c: { d: true } })).toEqual({ a: 1, b: ['x', 'y'], c: { d: true } });
    expect(roundTrip([1, 2, { k: 'v' }])).toEqual([1, 2, { k: 'v' }]);
  });

  it('JSON values are compact when small, pretty when large (a size proxy)', () => {
    // Style only: the editor accepts both forms everywhere (the Rust edit test
    // set_config_writes_a_multiline_object_value_everywhere pins acceptance,
    // parser_multiline_object.rs pins the grammar). The proxy keeps small
    // values on one line and lets big ones (markers, form schemas) breathe.
    expect(formatConfigValue({ a: 1 })).not.toContain('\n');
    expect(formatConfigValue([1, 2, 3])).not.toContain('\n');
    const marker = {
      __weft_image__: { key: 'local/project/p/abc', mimeType: 'image/png', sizeBytes: 52, filename: 'x.png' },
    };
    expect(formatConfigValue(marker)).toContain('\n');
    // Both forms round-trip identically.
    expect(roundTrip(marker)).toEqual(marker);
    expect(roundTrip({ a: 1 })).toEqual({ a: 1 });
  });

  it('round-trips @file and @asset markers, including a path with a quote', () => {
    const ref: WeftFileRefValue = { __weftFileRef: { path: 'dir/file.txt', type: 'String', marker: 'file' } };
    expect(roundTrip(ref)).toEqual(ref);
    const typed: WeftFileRefValue = { __weftFileRef: { path: 'a/b.json', type: 'List[Number]', marker: 'file' } };
    expect(roundTrip(typed)).toEqual(typed);
    const quoted: WeftFileRefValue = { __weftFileRef: { path: 'weird "name".txt', type: 'String', marker: 'file' } };
    expect(roundTrip(quoted)).toEqual(quoted);
    const asset: WeftFileRefValue = { __weftFileRef: { path: 'assets/pic.png', type: 'Image', marker: 'asset' } };
    expect(formatConfigValue(asset)).toBe('@asset("assets/pic.png", Image)');
    expect(roundTrip(asset)).toEqual(asset);
  });
});

describe('formatConfigValue fails loudly on un-encodable values', () => {
  it('throws on null / undefined (unset is a removeConfig, not a token)', () => {
    expect(() => formatConfigValue(null)).toThrow(/unset/);
    expect(() => formatConfigValue(undefined)).toThrow(/unset/);
  });

  it('throws on non-finite numbers', () => {
    expect(() => formatConfigValue(NaN)).toThrow(/finite/);
    expect(() => formatConfigValue(Infinity)).toThrow(/finite/);
    expect(() => formatConfigValue(-Infinity)).toThrow(/finite/);
  });
});

describe('parseConfigToken rejects tokens formatConfigValue could not produce', () => {
  it('throws on an unterminated heredoc', () => {
    expect(() => parseConfigToken('```\nno closing fence')).toThrow(/heredoc/);
  });

  it('throws on a bare unquoted word', () => {
    expect(() => parseConfigToken('hello')).toThrow(/not a config value token/);
  });
});

/** A port that holds SEVERAL files (an email's attachments, the media on an
 *  LLM call). Its value is a list of refs, and its source is a list of
 *  markers: one per file, in the order they were added. */
describe('several files on one port', () => {
  const ref = (path: string, type = 'Image'): WeftFileRefValue => ({
    __weftFileRef: { path, type, marker: 'asset' },
  });

  it('writes one marker per file', () => {
    expect(formatConfigValue([ref('assets/a.png'), ref('assets/b.png')])).toBe(
      '[@asset("assets/a.png", Image), @asset("assets/b.png", Image)]',
    );
  });

  it('round-trips the list', () => {
    const files = [ref('assets/a.png'), ref('logo.svg'), ref('https://example.com/x.png')];
    expect(parseConfigToken(formatConfigValue(files))).toEqual(files);
  });

  it('round-trips a type carrying a comma', () => {
    const files = [ref('a.bin', 'Dict[String, Number]')];
    expect(parseConfigToken(formatConfigValue(files))).toEqual(files);
  });

  it('leaves an ordinary list alone', () => {
    expect(formatConfigValue(['a', 'b'])).toBe('["a","b"]');
    expect(parseConfigToken('["a","b"]')).toEqual(['a', 'b']);
  });

  it('reads the files a written value names, in either shape', () => {
    // The structural shape, what a file-backed config field carries.
    expect(fileRefsOf(ref('a.png'))).toEqual([ref('a.png')]);
    expect(fileRefsOf([ref('a.png'), ref('b.png')])).toEqual([ref('a.png'), ref('b.png')]);
    // The written shape, what a port literal carries.
    expect(fileRefsOf('@asset("a.png", Image)')).toEqual([ref('a.png')]);
    expect(fileRefsOf(['@asset("a.png", Image)', '@asset("b.png", Image)'])).toEqual([
      ref('a.png'),
      ref('b.png'),
    ]);
    // Anything else names no files.
    expect(fileRefsOf('hello')).toEqual([]);
    expect(fileRefsOf(['hello'])).toEqual([]);
    expect(fileRefsOf(null)).toEqual([]);
  });
});
