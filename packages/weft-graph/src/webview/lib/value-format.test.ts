import { describe, it, expect } from 'vitest';
import { formatConfigValue, parseConfigToken, type WeftFileRefValue } from './value-format';

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

  it('throws on a multi-line string containing a fence (the lexer cannot encode it)', () => {
    // Mirrors the Rust edit-server: a heredoc has no inner-fence escape.
    expect(() => formatConfigValue('a\n```\nb')).toThrow(/heredoc fence/);
  });

  it('round-trips objects and arrays', () => {
    expect(roundTrip({ a: 1, b: ['x', 'y'], c: { d: true } })).toEqual({ a: 1, b: ['x', 'y'], c: { d: true } });
    expect(roundTrip([1, 2, { k: 'v' }])).toEqual([1, 2, { k: 'v' }]);
  });

  it('round-trips an @file marker, including a path with a quote', () => {
    const ref: WeftFileRefValue = { __weftFileRef: { path: 'dir/file.txt', type: 'String' } };
    expect(roundTrip(ref)).toEqual(ref);
    const typed: WeftFileRefValue = { __weftFileRef: { path: 'a/b.json', type: 'List[Number]' } };
    expect(roundTrip(typed)).toEqual(typed);
    const quoted: WeftFileRefValue = { __weftFileRef: { path: 'weird "name".txt', type: 'String' } };
    expect(roundTrip(quoted)).toEqual(quoted);
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
