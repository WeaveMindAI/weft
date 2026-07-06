import { describe, it, expect } from 'vitest';
import { isLiveDataItem, signalDisplayToLiveItems } from './live-data';

describe('isLiveDataItem', () => {
  it('accepts a well-formed item', () => {
    expect(isLiveDataItem({ type: 'text', label: 'Path', data: '/hook' })).toBe(true);
    expect(isLiveDataItem({ type: 'progress', label: 'p', data: 42 })).toBe(true);
  });

  it('rejects non-objects and missing/mistyped fields', () => {
    expect(isLiveDataItem(null)).toBe(false);
    expect(isLiveDataItem('nope')).toBe(false);
    expect(isLiveDataItem({ label: 'x', data: 'y' })).toBe(false); // no type
    expect(isLiveDataItem({ type: 'nope', label: 'x', data: 'y' })).toBe(false); // bad type
    expect(isLiveDataItem({ type: 'text', label: 1, data: 'y' })).toBe(false); // label not string
    expect(isLiveDataItem({ type: 'text', label: 'x', data: {} })).toBe(false); // data not string|number
  });

  it('validates an optional action shape', () => {
    expect(
      isLiveDataItem({ type: 'text', label: 'x', data: 'y', action: { label: 'go', actionKind: 'k' } }),
    ).toBe(true);
    expect(isLiveDataItem({ type: 'text', label: 'x', data: 'y', action: { label: 'go' } })).toBe(
      false,
    );
  });
});

describe('signalDisplayToLiveItems', () => {
  it('renders a public-entry path (root when empty, leading slash normalized)', () => {
    expect(signalDisplayToLiveItems({ surface: { kind: 'public_entry', path: '' } })[0]).toEqual({
      type: 'text',
      label: 'Path',
      data: '/',
    });
    expect(signalDisplayToLiveItems({ surface: { kind: 'public_entry', path: '/hook' } })[0]).toEqual(
      { type: 'text', label: 'Path', data: '/hook' },
    );
  });

  it('renders auth mode (none vs api_key header)', () => {
    const none = signalDisplayToLiveItems({ auth: { kind: 'none' } });
    expect(none).toContainEqual({ type: 'text', label: 'Auth', data: 'public (no key)' });
    const keyed = signalDisplayToLiveItems({ auth: { kind: 'api_key', header_name: 'X-My-Key' } });
    expect(keyed).toContainEqual({ type: 'text', label: 'Auth header', data: 'X-My-Key' });
  });

  it('emits nothing for an empty body', () => {
    expect(signalDisplayToLiveItems({})).toEqual([]);
  });
});
